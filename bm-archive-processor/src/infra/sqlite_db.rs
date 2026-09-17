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
use rusqlite::types::ValueRef;
use rusqlite::Connection;
use rusqlite::Row;
use tracing::info;
use tracing::trace;

use crate::app::metrics::Metrics;
use crate::domain::models::BlockGap;
use crate::domain::traits::DbClient;
use crate::infra::sqlite_ddl::create_merge_query;

/// Fast-merge protocol: a v7 full DB takes v3..=v7 dailies as they are, since
/// the merge query reads its column list from the target schema and simply
/// ignores columns the target dropped (`blocks.boc`).
///
/// Deliberately a strict equality. Past v7 the generic rule applies: an older
/// daily is migrated up to the target version before the merge, and it is that
/// migrated file which is uploaded afterwards — so a daily crossing v7 this way
/// would lose `blocks.boc`. In steady state sources and target are on the same
/// version and no migration runs; the case only arises when re-merging pre-v7
/// archives, which no longer exist anywhere.
const FAST_MERGE_TARGET_VERSION: u32 = 7;
const FAST_MERGE_MIN_SOURCE_VERSION: u32 = 3;

pub struct SqliteClient {
    tables: &'static [&'static str],
    metrics: Option<Metrics>,
    paranoid: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum VerificationKey {
    Single(String),
    Composite(Vec<String>),
}

impl SqliteClient {
    pub fn new(tables: &'static [&'static str], metrics: Option<Metrics>, paranoid: bool) -> Self {
        SqliteClient { tables, metrics, paranoid }
    }

    fn detect_verification_key(conn: &Connection, table: &str) -> anyhow::Result<VerificationKey> {
        let pragma = format!("PRAGMA main.table_info({table})");
        let mut stmt = conn.prepare(&pragma)?;
        let columns: Vec<String> =
            stmt.query_map([], |row| row.get::<_, String>(1))?.collect::<Result<Vec<_>, _>>()?;

        let has_column = |name: &str| columns.iter().any(|col| col == name);
        if has_column("id") {
            return Ok(VerificationKey::Single("id".to_string()));
        }
        if has_column("block_id") && has_column("target_type") {
            return Ok(VerificationKey::Composite(vec![
                "block_id".to_string(),
                "target_type".to_string(),
            ]));
        }
        if has_column("block_id") {
            return Ok(VerificationKey::Single("block_id".to_string()));
        }

        anyhow::bail!(
            "can't verify table `{table}`: no supported key columns found (id | block_id | block_id+target_type)"
        );
    }

    fn value_ref_to_string(row: &Row<'_>, index: usize) -> anyhow::Result<String> {
        let value = row.get_ref(index)?;
        let text = match value {
            ValueRef::Null => "NULL".to_string(),
            ValueRef::Integer(v) => v.to_string(),
            ValueRef::Real(v) => v.to_string(),
            ValueRef::Text(v) => String::from_utf8_lossy(v).into_owned(),
            ValueRef::Blob(v) => format!("{v:?}"),
        };
        Ok(text)
    }

    fn build_verify_query(schema: &str, table: &str, key: &VerificationKey) -> String {
        match key {
            VerificationKey::Single(col) => format!(
                "SELECT s.{col} FROM {schema}.{table} s LEFT JOIN main.{table} m ON m.{col} = s.{col} WHERE m.{col} IS NULL;"
            ),
            VerificationKey::Composite(cols) => {
                let select_cols = cols
                    .iter()
                    .map(|col| format!("s.{col}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let join_on = cols
                    .iter()
                    .map(|col| format!("m.{col} = s.{col}"))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                let marker_col = &cols[0];
                format!(
                    "SELECT {select_cols} FROM {schema}.{table} s LEFT JOIN main.{table} m ON {join_on} WHERE m.{marker_col} IS NULL;"
                )
            }
        }
    }

    /// Verifies that each attached `schema` table has no rows missing from the corresponding `main` table.
    fn verify_attached_tables(&self, out: &Connection, schema: &str) -> anyhow::Result<()> {
        for tbl in self.tables {
            let started_at = Instant::now();
            let key = Self::detect_verification_key(out, tbl)?;
            let verify_query = Self::build_verify_query(schema, tbl, &key);
            let mut stmt = out
                .prepare(&verify_query)
                .with_context(|| format!("failed to prepare verify query for table `{tbl}`"))?;
            let mut rows = stmt
                .query([])
                .with_context(|| format!("failed to execute verify query for table `{tbl}`"))?;
            // Bounded on purpose: a real divergence can be millions of rows, and
            // collecting every key before the first log line is how this would
            // end in the OOM killer. The exact count is what matters — the
            // wrapper (run.sh) scrapes `missing_keys=N` off this line into the
            // bm_cold_ap_{table,source}_missing_keys metrics, so keep the format.
            const EXAMPLES: usize = 20;
            let mut missing = 0usize;
            let mut examples: Vec<String> = Vec::new();
            while let Some(row) = rows.next()? {
                missing += 1;
                if examples.len() >= EXAMPLES {
                    continue;
                }
                examples.push(match &key {
                    VerificationKey::Single(_) => Self::value_ref_to_string(row, 0)?,
                    VerificationKey::Composite(cols) => {
                        let mut parts = Vec::with_capacity(cols.len());
                        for idx in 0..cols.len() {
                            parts.push(Self::value_ref_to_string(row, idx)?);
                        }
                        parts.join(":")
                    }
                });
            }
            info!(
                "diff `{schema}.{tbl}` <-> main ({} ms): missing_keys={missing}, examples={examples:?}",
                started_at.elapsed().as_millis()
            );
            // Rows can go missing silently: the merge runs INSERT OR IGNORE, which
            // swallows CHECK and NOT NULL violations along with duplicates. By
            // default this only reports and the metrics scraped above raise the
            // alarm; under --paranoid it fails the group instead, before
            // --post-upload=delete gets a chance to remove the sources.
            //
            // Design decision, not an oversight: this runs after the source's
            // rows were committed into the daily. Verifying inside the
            // transaction and rolling back would only throw away hours of work
            // that a retry redoes and fails identically — a row INSERT OR IGNORE
            // drops is dropped deterministically. Failing here keeps the daily
            // and the sources on disk for an operator to look at, and the group
            // stays failed until the cause is fixed.
            if self.paranoid && missing > 0 {
                bail!(
                    "table `{tbl}`: {missing} row(s) from `{schema}` did not reach main, e.g. {:?}",
                    examples.iter().take(5).collect::<Vec<_>>()
                );
            }
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

    /// A `-wal`/`-wal2`/`-journal` file with content next to `db`, if any. An
    /// empty one, or a bare `-shm` index, carries no data and is ignored.
    fn live_sidecar(db: &Path) -> Option<PathBuf> {
        let name = db.file_name()?.to_owned();
        ["-wal", "-wal2", "-journal"].into_iter().find_map(|suffix| {
            let mut sidecar = name.clone();
            sidecar.push(suffix);
            let sidecar = db.with_file_name(sidecar);
            match fs::metadata(&sidecar) {
                Ok(meta) if meta.len() > 0 => Some(sidecar),
                _ => None,
            }
        })
    }

    fn read_db_version(path: &Path) -> anyhow::Result<u32> {
        let conn = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("failed to open db for version check: {}", path.display()))?;
        // Never default to 0 here: opening read-only is lazy, so a locked,
        // truncated or non-SQLite file would otherwise report version 0 and slip
        // past every version gate below straight into the merge.
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .with_context(|| format!("failed to read user_version from {}", path.display()))?;
        Ok(version)
    }

    fn migrate_db_to_version(path: &Path, version: u32) -> anyhow::Result<()> {
        let db_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let db_filename = path
            .file_name()
            .ok_or_else(|| anyhow!("invalid db path (missing filename): {}", path.display()))?;

        let db_info = Box::leak(Box::new(DbInfo::new(db_filename)));
        let db_maintenance = migration_tool::DbMaintenance::new(db_info, db_dir);
        db_maintenance
            .migrate(
                MigrateTo::Version(version),
                DbMaintenanceOptions { silent: true, ..Default::default() },
            )
            .with_context(|| {
                format!("failed to migrate db to version {version}: {}", path.display())
            })
    }
}

impl DbClient for SqliteClient {
    fn merge_daily_into_full(&self, src_db: &Path, target_db: &Path) -> anyhow::Result<()> {
        if !target_db.exists() {
            bail!("full DB does not exist: {}", target_db.display());
        }

        let src_version = SqliteClient::read_db_version(src_db)?;
        let target_version = SqliteClient::read_db_version(target_db)?;
        info!(
            "merge versions: daily={src_version}, target={target_version} ({} -> {})",
            src_db.display(),
            target_db.display()
        );

        if src_version > target_version {
            bail!("daily version {src_version} is newer than target version {target_version}");
        }

        if target_version == FAST_MERGE_TARGET_VERSION {
            if src_version < FAST_MERGE_MIN_SOURCE_VERSION {
                bail!(
                    "source version {src_version} is too old for fast merge into v{FAST_MERGE_TARGET_VERSION} (minimum: v{FAST_MERGE_MIN_SOURCE_VERSION})"
                );
            }
            info!("fast merge: daily v{src_version} -> full v{target_version}");
        } else if src_version < target_version {
            info!("migrating daily v{src_version} -> v{target_version} before merge");
            SqliteClient::migrate_db_to_version(src_db, target_version)?;
        }

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

    fn query_block_gaps(&self, db: &Path) -> anyhow::Result<Vec<BlockGap>> {
        let conn = Connection::open_with_flags(
            db,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("failed to open full db at {}", db.display()))?;

        let started_at = Instant::now();
        let mut stmt = conn.prepare(
            "WITH s AS (
                SELECT
                    seq_no,
                    LAG(seq_no) OVER (ORDER BY seq_no) AS prev_seq_no
                FROM blocks
            )
            SELECT
                s.prev_seq_no + 1 AS gap_start,
                b_prev.gen_utime AS gap_start_ts,
                s.seq_no - 1 AS gap_end,
                b_curr.gen_utime AS gap_end_ts,
                s.seq_no - s.prev_seq_no - 1 AS missing_count
            FROM s
            JOIN blocks b_prev ON b_prev.seq_no = s.prev_seq_no
            JOIN blocks b_curr ON b_curr.seq_no = s.seq_no
            WHERE s.prev_seq_no IS NOT NULL
              AND s.seq_no - s.prev_seq_no > 1
            ORDER BY gap_start",
        )?;

        let gaps = stmt
            .query_map([], |row| {
                Ok(BlockGap {
                    gap_start: row.get(0)?,
                    gap_start_ts: row.get(1)?,
                    gap_end: row.get(2)?,
                    gap_end_ts: row.get(3)?,
                    missing_count: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .context("failed to query block gaps")?;

        info!(
            "block gaps query took {} ms, found {} gaps",
            started_at.elapsed().as_millis(),
            gaps.len()
        );

        Ok(gaps)
    }

    fn create_daily_db(&self, src_paths: &[PathBuf], dst_path: &Path) -> anyhow::Result<()> {
        if src_paths.is_empty() {
            bail!("input can't be empty!");
        }

        let versions: Vec<u32> = src_paths
            .iter()
            .map(|p| SqliteClient::read_db_version(p))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let first_version = versions[0];
        if !versions.iter().all(|v| *v == first_version) {
            let details: Vec<_> = src_paths
                .iter()
                .zip(&versions)
                .map(|(p, v)| format!("{}=v{}", p.display(), v))
                .collect();
            bail!("source DB version mismatch: {}", details.join(", "));
        }
        info!("all {} source DBs at version {first_version}", src_paths.len());

        // Each source is expected to be self-contained: the pull copies `.db`
        // files only, and block-manager check-points an archive
        // (wal_checkpoint TRUNCATE) when it rotates it. Should a sidecar with
        // content ever turn up next to one, its tail is exactly what the byte
        // copy below would silently drop — and nothing downstream could tell,
        // since the copy is a perfectly valid database.
        for path in src_paths {
            if let Some(sidecar) = Self::live_sidecar(path) {
                bail!(
                    "source {} has a non-empty journal sidecar {}: refusing to copy an archive that was not check-pointed",
                    path.display(),
                    sidecar.display()
                );
            }
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
        // out.pragma_update(None, "journal_mode", "WAL")?;
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

        if self.paranoid {
            // `quick_check` reports corruption as Ok(false); discarding it would
            // make the whole (slow) check a no-op.
            anyhow::ensure!(
                SqliteClient::quick_check(&out).with_context(|| {
                    format!("file={}: failed to run integrity check", dst_path.display())
                })?,
                "file={}: integrity check reported corruption",
                dst_path.display()
            );
        }

        // The daily begins life as a byte copy of a BM archive, and block-manager
        // writes those in WAL2 — a non-standard extension that stamps format 3 in
        // the file header. Anything linked against stock SQLite (the trim hook, or
        // whoever restores this file from S3) then sees only "file is not a
        // database". Normalise it here, while we still hold a connection that
        // understands the format; there is nothing to check-point, since the
        // sidecars were never copied (and the guard above refuses a source that
        // has one with content).
        //
        // The pragma answers with the mode now in effect rather than failing,
        // so a refused switch has to be caught by looking at the answer.
        let mode: String = out
            .pragma_update_and_check(None, "journal_mode", "DELETE", |row| row.get(0))
            .with_context(|| format!("failed to normalise journal mode: {}", dst_path.display()))?;
        anyhow::ensure!(
            mode.eq_ignore_ascii_case("delete"),
            "file={}: journal_mode=DELETE refused, database still reports {mode:?}",
            dst_path.display()
        );

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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rusqlite::Connection;

    use super::SqliteClient;
    use super::VerificationKey;
    use super::FAST_MERGE_MIN_SOURCE_VERSION;
    use super::FAST_MERGE_TARGET_VERSION;
    use crate::domain::traits::DbClient;

    fn create_test_db(dir: &std::path::Path, name: &str, version: u32) -> PathBuf {
        let path = dir.join(name);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE accounts (id TEXT PRIMARY KEY, data BLOB);
             CREATE TABLE blocks (id TEXT PRIMARY KEY, seq_no INTEGER);
             CREATE TABLE messages (id TEXT PRIMARY KEY, body BLOB);
             CREATE TABLE transactions (id TEXT PRIMARY KEY, data BLOB);
             CREATE TABLE bk_set_updates (block_id TEXT PRIMARY KEY, chain_order TEXT);
             CREATE TABLE attestations (block_id TEXT, target_type INTEGER, UNIQUE(block_id, target_type));",
        )
        .unwrap();
        conn.pragma_update(None, "user_version", version).unwrap();
        drop(conn);
        path
    }

    #[test]
    fn read_db_version_returns_correct_value() -> anyhow::Result<()> {
        let dir = testdir::testdir!();
        let path = create_test_db(&dir, "test.db", 5);
        assert_eq!(SqliteClient::read_db_version(&path)?, 5);
        Ok(())
    }

    #[test]
    fn create_daily_db_fails_on_version_mismatch() {
        let dir = testdir::testdir!();
        let db1 = create_test_db(&dir, "a.db", 5);
        let db2 = create_test_db(&dir, "b.db", 7);
        let dst = dir.join("daily.db");

        let client = SqliteClient::new(
            &["accounts", "blocks", "messages", "transactions", "bk_set_updates", "attestations"],
            None,
            false,
        );
        let result = client.create_daily_db(&[db1, db2], &dst);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("version mismatch"));
    }

    #[test]
    fn create_daily_db_succeeds_with_same_versions() -> anyhow::Result<()> {
        let dir = testdir::testdir!();
        let db1 = create_test_db(&dir, "a.db", 5);
        let db2 = create_test_db(&dir, "b.db", 5);
        let dst = dir.join("daily.db");

        let client = SqliteClient::new(
            &["accounts", "blocks", "messages", "transactions", "bk_set_updates", "attestations"],
            None,
            false,
        );
        client.create_daily_db(&[db1, db2], &dst)?;
        assert_eq!(SqliteClient::read_db_version(&dst)?, 5);
        Ok(())
    }

    #[test]
    fn merge_daily_into_full_fails_when_target_missing() {
        let dir = testdir::testdir!();
        let daily = create_test_db(&dir, "daily.db", 5);
        let full = dir.join("nonexistent.db");

        let client = SqliteClient::new(
            &["accounts", "blocks", "messages", "transactions", "bk_set_updates", "attestations"],
            None,
            false,
        );
        let result = client.merge_daily_into_full(&daily, &full);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn merge_daily_into_full_fast_merge_rejects_old_source() {
        let dir = testdir::testdir!();
        let daily = create_test_db(&dir, "daily.db", 2);
        let full = create_test_db(&dir, "full.db", FAST_MERGE_TARGET_VERSION);

        let client = SqliteClient::new(
            &["accounts", "blocks", "messages", "transactions", "bk_set_updates", "attestations"],
            None,
            false,
        );
        let result = client.merge_daily_into_full(&daily, &full);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too old for fast merge"));
    }

    #[test]
    fn merge_daily_into_full_fast_merge_succeeds() -> anyhow::Result<()> {
        let dir = testdir::testdir!();
        let daily = create_test_db(&dir, "daily.db", FAST_MERGE_MIN_SOURCE_VERSION);
        let full = create_test_db(&dir, "full.db", FAST_MERGE_TARGET_VERSION);

        let conn = Connection::open(&daily)?;
        conn.execute("INSERT INTO accounts (id, data) VALUES ('acc1', X'DEAD')", [])?;
        drop(conn);

        let client = SqliteClient::new(
            &["accounts", "blocks", "messages", "transactions", "bk_set_updates", "attestations"],
            None,
            false,
        );
        client.merge_daily_into_full(&daily, &full)?;

        let conn = Connection::open(&full)?;
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM accounts", [], |r| r.get(0))?;
        assert_eq!(count, 1);
        Ok(())
    }

    #[test]
    fn merge_daily_into_full_rejects_newer_source() {
        let dir = testdir::testdir!();
        let daily = create_test_db(&dir, "daily.db", 8);
        let full = create_test_db(&dir, "full.db", 5);

        let client = SqliteClient::new(
            &["accounts", "blocks", "messages", "transactions", "bk_set_updates", "attestations"],
            None,
            false,
        );
        let result = client.merge_daily_into_full(&daily, &full);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("newer than target"));
    }

    #[test]
    fn fast_merge_v5_into_v7_ignores_boc_column() -> anyhow::Result<()> {
        let dir = testdir::testdir!();

        // v5 blocks schema: has boc column
        let daily_path = dir.join("daily.db");
        let daily_conn = Connection::open(&daily_path)?;
        daily_conn.execute_batch(
            "CREATE TABLE accounts (id TEXT PRIMARY KEY, data BLOB);
             CREATE TABLE messages (id TEXT PRIMARY KEY, body BLOB);
             CREATE TABLE transactions (id TEXT PRIMARY KEY, block_id TEXT NOT NULL, chain_order TEXT NOT NULL);
             CREATE TABLE bk_set_updates (block_id TEXT PRIMARY KEY, chain_order TEXT NOT NULL);
             CREATE TABLE attestations (block_id TEXT NOT NULL, target_type INTEGER NOT NULL, source_block_id TEXT, source_chain_order TEXT, UNIQUE(block_id, target_type));
             CREATE TABLE blocks (
                 id TEXT NOT NULL UNIQUE,
                 status INTEGER NOT NULL,
                 seq_no INTEGER NOT NULL,
                 parent TEXT NOT NULL,
                 thread_id TEXT,
                 gen_utime INTEGER,
                 chain_order TEXT,
                 boc BLOB,
                 height BLOB,
                 envelope_hash BLOB
             );",
        )?;
        daily_conn.execute(
            "INSERT INTO blocks (id, status, seq_no, parent, thread_id, gen_utime, chain_order, boc, height, envelope_hash)
             VALUES ('blk1', 1, 100, 'p0', 't1', 1000, 'co1', X'DEADBEEF', X'0000000000000001', X'AABB')",
            [],
        )?;
        daily_conn.execute("INSERT INTO accounts (id, data) VALUES ('acc1', X'1234')", [])?;
        daily_conn.pragma_update(None, "user_version", 5)?;
        drop(daily_conn);

        // v7 blocks schema: no boc column
        let full_path = dir.join("full.db");
        let full_conn = Connection::open(&full_path)?;
        full_conn.execute_batch(
            "CREATE TABLE accounts (id TEXT PRIMARY KEY, data BLOB);
             CREATE TABLE messages (id TEXT PRIMARY KEY, body BLOB);
             CREATE TABLE transactions (id TEXT PRIMARY KEY, block_id TEXT NOT NULL, chain_order TEXT NOT NULL);
             CREATE TABLE bk_set_updates (block_id TEXT PRIMARY KEY, chain_order TEXT NOT NULL);
             CREATE TABLE attestations (block_id TEXT NOT NULL, target_type INTEGER NOT NULL, source_block_id TEXT, source_chain_order TEXT, UNIQUE(block_id, target_type));
             CREATE TABLE blocks (
                 id TEXT NOT NULL UNIQUE,
                 status INTEGER NOT NULL,
                 seq_no INTEGER NOT NULL,
                 parent TEXT NOT NULL,
                 thread_id TEXT,
                 gen_utime INTEGER,
                 chain_order TEXT,
                 height BLOB,
                 envelope_hash BLOB
             );",
        )?;
        full_conn.pragma_update(None, "user_version", 7)?;
        drop(full_conn);

        let client = SqliteClient::new(
            &["accounts", "blocks", "messages", "transactions", "bk_set_updates", "attestations"],
            None,
            false,
        );
        client.merge_daily_into_full(&daily_path, &full_path)?;

        let conn = Connection::open(&full_path)?;
        let (id, seq_no, chain_order): (String, i64, String) = conn.query_row(
            "SELECT id, seq_no, chain_order FROM blocks WHERE id = 'blk1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        assert_eq!(id, "blk1");
        assert_eq!(seq_no, 100);
        assert_eq!(chain_order, "co1");

        // boc column must not exist in target
        let has_boc: bool = conn.query_row(
            "SELECT COUNT(*) > 0 FROM pragma_table_info('blocks') WHERE name = 'boc'",
            [],
            |r| r.get(0),
        )?;
        assert!(!has_boc);

        // accounts also merged
        let acc_count: i64 = conn.query_row("SELECT COUNT(*) FROM accounts", [], |r| r.get(0))?;
        assert_eq!(acc_count, 1);

        Ok(())
    }

    #[test]
    fn detect_verification_key_uses_id_when_present() -> anyhow::Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute("CREATE TABLE accounts (id TEXT PRIMARY KEY, data BLOB)", [])?;

        let key = SqliteClient::detect_verification_key(&conn, "accounts")?;
        assert_eq!(key, VerificationKey::Single("id".to_string()));
        Ok(())
    }

    #[test]
    fn detect_verification_key_uses_composite_for_attestations() -> anyhow::Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute(
            "CREATE TABLE attestations (
                block_id TEXT NOT NULL,
                target_type INTEGER NOT NULL,
                source_chain_order TEXT NOT NULL,
                UNIQUE(block_id, target_type)
            )",
            [],
        )?;

        let key = SqliteClient::detect_verification_key(&conn, "attestations")?;
        assert_eq!(
            key,
            VerificationKey::Composite(vec!["block_id".to_string(), "target_type".to_string()])
        );
        Ok(())
    }

    #[test]
    fn detect_verification_key_uses_block_id_when_id_missing() -> anyhow::Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute(
            "CREATE TABLE bk_set_updates (block_id TEXT PRIMARY KEY, chain_order TEXT NOT NULL)",
            [],
        )?;

        let key = SqliteClient::detect_verification_key(&conn, "bk_set_updates")?;
        assert_eq!(key, VerificationKey::Single("block_id".to_string()));
        Ok(())
    }

    #[test]
    fn read_db_version_propagates_errors_instead_of_reporting_zero() {
        let dir = testdir::testdir!();
        let bogus = dir.join("not-a-db.db");
        std::fs::write(&bogus, b"this is definitely not a SQLite database").unwrap();

        // Reporting v0 here would sail past every version gate in the merge.
        let err = SqliteClient::read_db_version(&bogus).unwrap_err();
        assert!(
            err.to_string().contains("user_version"),
            "expected a version-read error, got: {err}"
        );
    }

    #[test]
    fn paranoid_merge_fails_when_rows_do_not_reach_the_target() -> anyhow::Result<()> {
        let dir = testdir::testdir!();
        // INSERT OR IGNORE drops CHECK violations as quietly as duplicates, so a
        // stricter target loses rows without any error from the merge itself.
        let target = dir.join("full.db");
        let conn = Connection::open(&target)?;
        conn.execute_batch(
            "CREATE TABLE accounts (id TEXT PRIMARY KEY);
             CREATE TABLE blocks (id TEXT PRIMARY KEY, seq_no INTEGER CHECK (seq_no > 0));
             CREATE TABLE messages (id TEXT PRIMARY KEY);
             CREATE TABLE transactions (id TEXT PRIMARY KEY);
             CREATE TABLE bk_set_updates (block_id TEXT PRIMARY KEY);
             CREATE TABLE attestations (block_id TEXT, target_type INTEGER, UNIQUE(block_id, target_type));",
        )?;
        conn.pragma_update(None, "user_version", 8)?;
        drop(conn);

        let daily = dir.join("daily.db");
        let conn = Connection::open(&daily)?;
        conn.execute_batch(
            "CREATE TABLE accounts (id TEXT PRIMARY KEY);
             CREATE TABLE blocks (id TEXT PRIMARY KEY, seq_no INTEGER);
             CREATE TABLE messages (id TEXT PRIMARY KEY);
             CREATE TABLE transactions (id TEXT PRIMARY KEY);
             CREATE TABLE bk_set_updates (block_id TEXT PRIMARY KEY);
             CREATE TABLE attestations (block_id TEXT, target_type INTEGER, UNIQUE(block_id, target_type));",
        )?;
        conn.pragma_update(None, "user_version", 8)?;
        conn.execute("INSERT INTO blocks (id, seq_no) VALUES ('ok', 1)", [])?;
        conn.execute("INSERT INTO blocks (id, seq_no) VALUES ('rejected', -1)", [])?;
        drop(conn);

        let quiet = SqliteClient::new(
            &["accounts", "blocks", "messages", "transactions", "bk_set_updates", "attestations"],
            None,
            false,
        );
        quiet.merge_daily_into_full(&daily, &target)?;

        let out = Connection::open(&target)?;
        let landed: i64 = out.query_row("SELECT count(*) FROM blocks", [], |r| r.get(0))?;
        assert_eq!(landed, 1, "the CHECK violation should have been dropped silently");
        out.execute("DELETE FROM blocks", [])?;
        drop(out);

        let strict = SqliteClient::new(
            &["accounts", "blocks", "messages", "transactions", "bk_set_updates", "attestations"],
            None,
            true,
        );
        let err = strict.merge_daily_into_full(&daily, &target).unwrap_err();
        assert!(
            err.to_string().contains("did not reach main"),
            "paranoid merge should report the loss, got: {err}"
        );
        Ok(())
    }

    #[test]
    fn create_daily_db_normalises_the_journal_mode() -> anyhow::Result<()> {
        let dir = testdir::testdir!();
        let src = create_test_db(&dir, "src.db", 8);
        // The vendored SQLite build understands WAL2, so stand in with the real
        // thing: header bytes 18-19 read 3/3, which stock SQLite rejects.
        let conn = Connection::open(&src)?;
        conn.execute_batch("PRAGMA journal_mode=WAL2")?;
        drop(conn);
        let header = std::fs::read(&src)?;
        assert_eq!(&header[18..20], &[3, 3], "fixture must really be WAL2");

        let dst = dir.join("daily.db");
        let client = SqliteClient::new(
            &["accounts", "blocks", "messages", "transactions", "bk_set_updates", "attestations"],
            None,
            false,
        );
        client.create_daily_db(&[src], &dst)?;

        let out = Connection::open(&dst)?;
        let mode: String = out.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        assert_eq!(mode, "delete", "daily must not inherit the archive's journal mode");
        let header = std::fs::read(&dst)?;
        assert_eq!(&header[18..20], &[1, 1], "stock SQLite must see a rollback-journal header");
        Ok(())
    }

    #[test]
    fn create_daily_db_refuses_a_source_with_a_live_wal_sidecar() -> anyhow::Result<()> {
        let dir = testdir::testdir!();
        let src = create_test_db(&dir, "src.db", 8);
        // A rotated archive block-manager failed to check-point: the byte copy
        // would be a valid database missing the WAL tail, and no later check
        // could tell.
        std::fs::write(dir.join("src.db-wal"), b"frames")?;

        let client = SqliteClient::new(
            &["accounts", "blocks", "messages", "transactions", "bk_set_updates", "attestations"],
            None,
            false,
        );
        let err =
            client.create_daily_db(std::slice::from_ref(&src), &dir.join("daily.db")).unwrap_err();
        assert!(err.to_string().contains("sidecar"), "unexpected error: {err}");
        assert!(!dir.join("daily.db").exists(), "nothing may be copied");

        // An empty sidecar carries nothing and must not block the run.
        std::fs::write(dir.join("src.db-wal"), b"")?;
        client.create_daily_db(&[src], &dir.join("daily.db"))?;
        Ok(())
    }
}
