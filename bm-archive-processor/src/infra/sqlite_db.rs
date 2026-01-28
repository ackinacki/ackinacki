use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Ok;
use rusqlite::Connection;
use tracing::info;
use tracing::trace;

use crate::app::metrics::Metrics;
use crate::domain::traits::DbClient;
use crate::infra::sqlite_ddl::create_merge_query;
use crate::infra::sqlite_ddl::fetch_create_table_ddls;
use crate::infra::sqlite_ddl::fetch_index_ddls;

pub struct SqliteClient {
    tables: &'static [&'static str],
    metrics: Option<Metrics>,
}
impl SqliteClient {
    pub fn new(tables: &'static [&'static str], metrics: Option<Metrics>) -> Self {
        SqliteClient { tables, metrics }
    }
}
impl DbClient for SqliteClient {
    fn merge_daily_into_full(&self, src_db: &Path, target_db: &Path) -> anyhow::Result<()> {
        info!("running merge db {} into {} ...", src_db.display(), target_db.display());

        let mut conn = Connection::open(target_db)
            .with_context(|| format!("failed to open target db at {}", target_db.display()))?;

        conn.pragma_update(None, "synchronous", "OFF")?;
        conn.pragma_update(None, "temp_store", "MEMORY")?;
        conn.pragma_update(None, "cache_size", "-200000")?; // ~200MB
        conn.pragma_update(None, "foreign_keys", "OFF")?;

        let attach_sql = format!("ATTACH DATABASE '{}' AS src", escape_sqlite_path(src_db));
        trace!("attach_sql={attach_sql}");

        // generate merge query template
        let mut table_merge_queries = HashMap::new();
        for tbl in self.tables {
            let insert_table = tbl.to_string();
            let select_table = format!("src.{tbl}");
            let sql = create_merge_query(&conn, &insert_table, &select_table)?;
            table_merge_queries.insert(tbl.to_string(), sql);
        }

        let tx = conn.transaction()?;

        tx.execute_batch(&attach_sql)
            .with_context(|| format!("failed to attach {} as 'src'", src_db.display()))?;

        for table in self.tables {
            let merge_sql = table_merge_queries[*table].clone();
            trace!("merge_sql={merge_sql}");
            tx.execute(&merge_sql, [])
                .with_context(|| format!("failed to merge table `{table}`"))?;
        }

        tx.commit().context("failed to commit merged data")?;

        if let (Some(metrics), Some(ts_str)) =
            (&self.metrics, src_db.file_stem().and_then(|s| s.to_str()))
        {
            if let anyhow::Result::Ok(ts) = ts_str.parse::<u64>() {
                metrics.last_merged_timestamp.record(ts, &[]);
            }
        }

        let detach_sql = "DETACH DATABASE src";
        trace!("detach_sql={detach_sql}");
        conn.execute_batch(detach_sql).context("failed to DETACH src database")?;

        Ok(())
    }

    fn create_daily_db(&self, src_paths: &[PathBuf], dst_path: &Path) -> anyhow::Result<()> {
        if src_paths.is_empty() {
            bail!("input can't be empty!");
        }

        if dst_path.exists() {
            fs::remove_file(dst_path).with_context(|| {
                format!("failed to remove existing output db: {}", dst_path.display())
            })?;
        }

        // for DDL fetching
        let first_conn = Connection::open(&src_paths[0]).with_context(|| {
            format!("failed to open first input db: {}", src_paths[0].display())
        })?;

        let table_ddls = fetch_create_table_ddls(&first_conn, self.tables)?;
        if table_ddls.len() != self.tables.len() {
            return Err(anyhow!(
                "schema mismatch: some tables missing in first db (found {} of {})",
                table_ddls.len(),
                self.tables.len()
            ));
        }

        let index_ddls = fetch_index_ddls(&first_conn, self.tables)?;

        // create daily DB
        let mut out = Connection::open(dst_path)
            .with_context(|| format!("failed to create output db: {}", dst_path.display()))?;

        // speed optimization
        out.pragma_update(None, "journal_mode", "WAL")?;
        out.pragma_update(None, "synchronous", "OFF")?;
        out.pragma_update(None, "temp_store", "MEMORY")?;
        out.pragma_update(None, "cache_size", "-200000")?; // ~200MB
        out.pragma_update(None, "foreign_keys", "OFF")?;

        let tx = out.transaction()?;
        // init schema
        for ddl in &table_ddls {
            trace!("DDL exec: {ddl}");
            tx.execute_batch(ddl)
                .with_context(|| format!("failed to create table with DDL: {ddl}"))?;
        }
        tx.commit()?;

        // generate merge query template
        let mut table_merge_queries = HashMap::new();
        for table in self.tables {
            let select_table = format!("SOURCE_SCHEMA.{table}");
            let sql = create_merge_query(&out, table, &select_table).with_context(|| {
                format!("failed to create merge query: {table}, {select_table}")
            })?;
            table_merge_queries.insert(table.to_string(), sql);
        }

        // merge DBs: ATTACH + SELECT INSERT
        for (i, path) in src_paths.iter().enumerate() {
            let tx = out.transaction()?;
            let schema = format!("src{i}");
            let attach_sql =
                format!("ATTACH DATABASE '{}' AS {}", escape_sqlite_path(path), schema);
            trace!("attach_sql={attach_sql}");
            tx.execute_batch(&attach_sql)
                .with_context(|| format!("failed to attach {} as {}", path.display(), schema))?;

            for tbl in self.tables {
                let sql = table_merge_queries[*tbl].replace("SOURCE_SCHEMA", &schema);

                trace!("merge_sql={sql}");
                tx.execute(&sql, []).with_context(|| {
                    format!("merge failed for table `{}` from {}", tbl, path.display())
                })?;
            }

            tx.commit().context("failed to commit merged data")?;

            let detach_sql = format!("DETACH DATABASE {schema}");
            trace!("detach_sql={detach_sql}");
            out.execute_batch(&detach_sql).with_context(|| format!("failed to detach {schema}"))?;
        }

        // speed optimization: create indexes after data loaded
        let tx = out.transaction()?;

        for ddl in &index_ddls {
            tx.execute_batch(ddl)
                .with_context(|| format!("failed to create index with DDL: {ddl}"))?;
        }

        tx.commit()?;

        // todo: revise pragmas for read-only
        out.pragma_update(None, "foreign_keys", "ON")?;
        out.pragma_update(None, "synchronous", "FULL")?;
        Ok(())
    }
}

// Workaround for Sqlite `ATTACH`
pub fn escape_sqlite_path(p: &Path) -> String {
    let s = p.to_string_lossy().into_owned();
    s.replace('\'', "''")
}
