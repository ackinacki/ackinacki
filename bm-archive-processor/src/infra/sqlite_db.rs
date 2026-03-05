// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use migration_tool::DbInfo;
use migration_tool::DbMaintenanceOptions;
use migration_tool::MigrateTo;
use rusqlite::Connection;
use tracing::info;
use tracing::trace;

use crate::app::metrics::Metrics;
use crate::domain::traits::DbClient;
use crate::infra::sqlite_ddl::create_merge_query;

pub struct SqliteClient {
    tables: &'static [&'static str],
    metrics: Option<Metrics>,
}
impl SqliteClient {
    pub fn new(tables: &'static [&'static str], metrics: Option<Metrics>) -> Self {
        SqliteClient { tables, metrics }
    }

    /// Verifies that each attached `schema` table has no rows missing from the corresponding `main` table.
    fn verify_attached_tables(&self, out: &Connection, schema: &str) -> anyhow::Result<()> {
        for tbl in self.tables {
            let started_at = Instant::now();
            let verify_query = format!(
                "SELECT s.id FROM {}.{} s LEFT JOIN main.{} m ON m.id = s.id WHERE m.id IS NULL;",
                schema, tbl, tbl,
            );
            let mut stmt = out
                .prepare(&verify_query)
                .with_context(|| format!("failed to prepare verify query for table `{tbl}`"))?;
            let mut rows = stmt
                .query([])
                .with_context(|| format!("failed to execute verify query for table `{tbl}`"))?;
            let mut missing_ids = Vec::new();
            while let Some(row) = rows.next()? {
                missing_ids.push(row.get::<_, String>(0)?);
            }
            info!(
                "diff `{schema}.{tbl}` <-> main ({} ms): missing_ids={}, list={:?}",
                started_at.elapsed().as_millis(),
                missing_ids.len(),
                missing_ids
            );
        }

        Ok(())
    }

    /// Runs SQLite `PRAGMA quick_check` for the given schema and returns `true` when it reports `ok`.
    fn quick_check(conn: &Connection) -> anyhow::Result<bool> {
        let started_at = Instant::now();
        trace!("starting integrity quick check...");
        let result: String = conn.query_row("PRAGMA quick_check;", [], |row| row.get(0))?;
        info!("integrity check result ({} ms): {}", started_at.elapsed().as_millis(), result);
        Ok(result == "ok")
    }

    /// Applies schema migrations in-place to the given SQLite DB.
    fn migrate_db_to_latest(path: &Path) -> anyhow::Result<()> {
        let db_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let db_filename = path.file_name().ok_or_else(|| {
            anyhow!("invalid source db path (missing filename): {}", path.display())
        })?;

        let db_info = Box::leak(Box::new(DbInfo::new(db_filename)));
        let db_maintenance = migration_tool::DbMaintenance::new(db_info, db_dir);
        db_maintenance
            .migrate(MigrateTo::Latest, DbMaintenanceOptions { silent: true })
            .with_context(|| format!("failed to migrate source db: {}", path.display()))
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

        let started_at = Instant::now();
        for table in self.tables {
            let merge_sql = table_merge_queries[*table].clone();
            trace!("merge_sql={merge_sql}");
            tx.execute(&merge_sql, [])
                .with_context(|| format!("failed to merge table `{table}`"))?;
        }

        tx.commit().context("failed to commit merged data")?;

        info!(
            "[full db] merge iteration for {} took {} ms",
            src_db.display(),
            started_at.elapsed().as_millis()
        );

        self.verify_attached_tables(&conn, "src")?;

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

        // precondition: all source DBs must be at the latest schema before merging
        for src_path in src_paths {
            SqliteClient::migrate_db_to_latest(src_path)?;
        }

        let base_db = &src_paths[0];
        if dst_path.exists() && dst_path != base_db {
            fs::remove_file(dst_path).with_context(|| {
                format!("failed to remove existing output db: {}", dst_path.display())
            })?;
        }

        // daily DB is initialized by copying the first DB from the group
        let dst_dir = dst_path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(dst_dir).with_context(|| {
            format!("failed to create output db directory: {}", dst_dir.display())
        })?;

        if base_db != dst_path {
            fs::copy(base_db, dst_path).with_context(|| {
                format!("failed to copy base db {} -> {}", base_db.display(), dst_path.display())
            })?;
        }

        // merge remaining group DBs into copied daily DB
        let mut out = Connection::open(dst_path)
            .with_context(|| format!("failed to create output db: {}", dst_path.display()))?;

        // speed optimization
        out.pragma_update(None, "journal_mode", "WAL")?;
        out.pragma_update(None, "synchronous", "OFF")?;
        out.pragma_update(None, "temp_store", "MEMORY")?;
        out.pragma_update(None, "cache_size", "-200000")?; // ~200MB
        out.pragma_update(None, "foreign_keys", "OFF")?;

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
        for (i, path) in src_paths.iter().enumerate().skip(1) {
            let started_at = Instant::now();
            let tx = out.transaction()?;
            let schema = format!("src{i}");
            let attach_sql =
                format!("ATTACH DATABASE '{}' AS {}", escape_sqlite_path(path), schema);
            trace!("attach_sql={attach_sql}");
            tx.execute_batch(&attach_sql)
                .with_context(|| format!("failed to attach {} as {}", path.display(), schema))?;

            for tbl in self.tables {
                // TODO optimization?? starting from the second iteration, merge only diffs instead of performing full merge
                let sql = table_merge_queries[*tbl].replace("SOURCE_SCHEMA", &schema);

                trace!("merge_sql={sql}");
                tx.execute(&sql, []).with_context(|| {
                    format!("merge failed for table `{}` from {}", tbl, path.display())
                })?;
            }

            tx.commit().context("failed to commit merged data")?;

            info!(
                "[daily db] merge iteration {i} for {} took {} ms",
                path.display(),
                started_at.elapsed().as_millis()
            );

            self.verify_attached_tables(&out, &schema)?;

            let detach_sql = format!("DETACH DATABASE {schema}");
            trace!("detach_sql={detach_sql}");
            out.execute_batch(&detach_sql).with_context(|| format!("failed to detach {schema}"))?;
        }

        let _ = SqliteClient::quick_check(&out)
            .with_context(|| format!("file={}: integrity check failed", dst_path.display()))?;

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
