// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::ops::Deref;
use std::ops::DerefMut;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use arc_swap::ArcSwap;
use opentelemetry::metrics::Histogram;
use sqlx::pool::PoolConnection;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqliteConnection;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::QueryBuilder;
use sqlx::Sqlite;
use sqlx::SqlitePool;

use crate::defaults;
use crate::defaults::DEFAULT_SQLITE_QUERY_TIMEOUT_SECS;
use crate::defaults::THREAD_ZERO;
use crate::metrics::GqlServerMetrics;

/// Build PRAGMAs applied to every new SQLite connection for read-optimized workloads.
pub fn build_read_pragmas(mmap_size: i64, cache_size: i64) -> String {
    format!(
        "PRAGMA mmap_size = {mmap_size};\
         PRAGMA cache_size = {cache_size};\
         PRAGMA temp_store = MEMORY;\
         PRAGMA query_only = ON;"
    )
}

/// Number of SQLite virtual-machine instructions between progress handler
/// invocations. Lower values give more precise timeout enforcement but add
/// overhead; 1000 is a good balance for second-scale timeouts.
const PROGRESS_HANDLER_OPS: i32 = 1000;

pub struct DBConnector {
    pool: ArcSwap<SqlitePool>,
    attachments: ArcSwap<HashMap<String, String>>,
    /// `ATTACH` URI of the history proofs store, already escaped as an SQL
    /// literal. `None` when there is no store next to the main DB.
    history_proofs_db: ArcSwap<Option<String>>,
    /// The history proofs store is attached to the connections of the current
    /// pool (its ATTACH did not fail). Checked when the pool is recreated.
    history_store_attached: ArcSwap<bool>,
    db_path: PathBuf,
    metrics: ArcSwap<Option<GqlServerMetrics>>,
    query_timeout: ArcSwap<Duration>,
    acquire_timeout: ArcSwap<Duration>,
    max_connections: ArcSwap<u32>,
    max_attached_db: ArcSwap<u16>,
    sqlite_mmap_size: ArcSwap<i64>,
    sqlite_cache_size: ArcSwap<i64>,
}

pub struct TimedConnection {
    conn: PoolConnection<Sqlite>,
    start: Instant,
    histogram: Option<Histogram<u64>>,
    /// Shared flag with the progress handler callback.
    /// Set to `false` on Drop so the handler won't interrupt pool-internal
    /// operations (e.g. ping) after the connection is returned.
    timeout_active: Arc<AtomicBool>,
    /// Set to `true` by the progress handler when it actually interrupts a query.
    pub(crate) interrupted: Arc<AtomicBool>,
    sql: Option<String>,
}

impl Deref for TimedConnection {
    type Target = SqliteConnection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl DerefMut for TimedConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl TimedConnection {
    pub fn set_sql(&mut self, sql: impl Into<String>) {
        self.sql = Some(sql.into());
    }
}

/// Map a database error to a GraphQL-friendly error.
/// Replaces `SQLITE_INTERRUPT` with a user-facing "Request timeout" message
/// and adds an `extensions.code = "TIMEOUT"` field.
pub fn map_db_error(err: async_graphql::Error) -> async_graphql::Error {
    use async_graphql::ErrorExtensions;

    if err.message.contains("interrupted") {
        async_graphql::Error::new("Request timeout").extend_with(|_, e| e.set("code", "TIMEOUT"))
    } else {
        err
    }
}

impl Drop for TimedConnection {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;
        let sql = self.sql.as_deref().unwrap_or("<unknown>");
        if let Some(h) = &self.histogram {
            h.record(elapsed_ms, &[]);
        }
        if self.interrupted.load(Ordering::Acquire) {
            tracing::warn!(elapsed_ms, sql, "SQLite query interrupted: exceeded timeout");
        } else {
            tracing::debug!(elapsed_ms, sql, "SQLite query completed");
        }
        // Deactivate the progress handler so pool-internal operations
        // (ping, after_release) won't be interrupted by a stale deadline.
        self.timeout_active.store(false, Ordering::Release);
    }
}

impl DBConnector {
    pub fn new(pool: SqlitePool, db_path: PathBuf, max_connections: u32) -> Arc<Self> {
        Arc::new(Self {
            pool: ArcSwap::new(Arc::new(pool)),
            attachments: ArcSwap::new(Arc::new(HashMap::new())),
            history_proofs_db: ArcSwap::new(Arc::new(None)),
            history_store_attached: ArcSwap::new(Arc::new(false)),
            db_path,
            metrics: ArcSwap::new(Arc::new(None)),
            query_timeout: ArcSwap::new(Arc::new(Duration::from_secs(
                DEFAULT_SQLITE_QUERY_TIMEOUT_SECS,
            ))),
            acquire_timeout: ArcSwap::new(Arc::new(Duration::from_secs(
                defaults::DEFAULT_ACQUIRE_TIMEOUT_SECS,
            ))),
            max_connections: ArcSwap::new(Arc::new(max_connections)),
            max_attached_db: ArcSwap::new(Arc::new(defaults::MAX_ATTACHED_DB)),
            sqlite_mmap_size: ArcSwap::new(Arc::new(defaults::DEFAULT_SQLITE_MMAP_SIZE)),
            sqlite_cache_size: ArcSwap::new(Arc::new(defaults::DEFAULT_SQLITE_CACHE_SIZE)),
        })
    }

    pub fn set_query_timeout(&self, timeout: Duration) {
        self.query_timeout.store(Arc::new(timeout));
    }

    pub async fn set_acquire_timeout(&self, timeout: Duration) -> anyhow::Result<()> {
        let current = **self.acquire_timeout.load();
        if current == timeout {
            return Ok(());
        }
        self.acquire_timeout.store(Arc::new(timeout));
        let new_pool = self.create_pool_with_attachments().await?;
        self.replace_pool(new_pool);
        tracing::info!("Recreated pool with acquire_timeout={}s", timeout.as_secs());
        Ok(())
    }

    pub async fn set_sqlite_pragmas(&self, mmap_size: i64, cache_size: i64) -> anyhow::Result<()> {
        let current_mmap = **self.sqlite_mmap_size.load();
        let current_cache = **self.sqlite_cache_size.load();
        if current_mmap == mmap_size && current_cache == cache_size {
            return Ok(());
        }
        self.sqlite_mmap_size.store(Arc::new(mmap_size));
        self.sqlite_cache_size.store(Arc::new(cache_size));
        let new_pool = self.create_pool_with_attachments().await?;
        self.replace_pool(new_pool);
        tracing::info!(mmap_size, cache_size, "Recreated pool with updated SQLite PRAGMAs");
        Ok(())
    }

    pub fn set_max_attached_db(&self, max: u16) {
        self.max_attached_db.store(Arc::new(max));
    }

    pub fn max_attached_db(&self) -> u16 {
        **self.max_attached_db.load()
    }

    /// Sets the history proofs store to attach to every pooled connection as
    /// `HISTORY_PROOFS_DB_ALIAS`. Takes effect when the pool is next
    /// recreated, i.e. on `update_attachments`.
    pub fn set_history_proofs_db(&self, path: Option<&Path>) {
        let attach_uri =
            path.map(|path| escape_literal(&format!("file:{}?mode=ro", path.display())));
        self.history_proofs_db.store(Arc::new(attach_uri));
    }

    pub fn has_history_proofs_db(&self) -> bool {
        self.history_proofs_db.load().is_some()
    }

    /// The store's ATTACH worked on a new connection of the current pool when
    /// the pool was created. Reads of the store check this, not
    /// `has_history_proofs_db`: a failed ATTACH, or the old pool before the
    /// file's first attach, would fail them with "no such table" instead of a
    /// miss. Connections opened later attach on their own; one whose ATTACH
    /// fails still returns that error.
    pub fn history_store_attached(&self) -> bool {
        **self.history_store_attached.load()
    }

    /// Probes the store on a connection of `pool`: attached, with its four
    /// tables. `false` without a store, or when the ATTACH or the query failed.
    async fn probe_history_store(&self, pool: &SqlitePool) -> bool {
        if !self.has_history_proofs_db() {
            return false;
        }
        let sql = format!(
            "SELECT name FROM \"{}\".sqlite_master WHERE type = 'table' \
             AND name IN ({})",
            defaults::HISTORY_PROOFS_DB_ALIAS,
            HISTORY_TABLES.map(|table| format!("'{table}'")).join(", ")
        );
        let tables: Result<Vec<(String,)>, sqlx::Error> =
            QueryBuilder::new(sql).build_query_as().fetch_all(pool).await;
        let tables: Vec<String> = match tables {
            Ok(rows) => rows.into_iter().map(|(name,)| name).collect(),
            Err(err) => {
                tracing::error!(
                    "the history proofs store is not attached, gql-server serves without it: {err}"
                );
                return false;
            }
        };
        let missing = missing_history_tables(&tables);
        if !missing.is_empty() {
            tracing::error!(
                "the history proofs store has no table {}, gql-server serves without it",
                missing.join(", ")
            );
            return false;
        }
        true
    }

    /// Replaces the pool and the store flag probed on it. The flag turns off
    /// before the swap and on after it, so a query does not see a flag that
    /// its pool does not back.
    async fn replace_pool_and_probe(&self, new_pool: SqlitePool) {
        let attached = self.probe_history_store(&new_pool).await;
        if !attached {
            self.history_store_attached.store(Arc::new(false));
        }
        self.replace_pool(new_pool);
        self.history_store_attached.store(Arc::new(attached));
    }

    pub async fn set_max_connections(&self, max_conn: u32) -> anyhow::Result<()> {
        let current = **self.max_connections.load();
        if current == max_conn {
            return Ok(());
        }
        self.max_connections.store(Arc::new(max_conn));
        let new_pool = self.create_pool_with_attachments().await?;
        self.replace_pool_and_probe(new_pool).await;
        tracing::info!("Recreated pool with max_connections={max_conn}");
        Ok(())
    }

    pub fn set_metrics(&self, metrics: GqlServerMetrics) {
        self.metrics.store(Arc::new(Some(metrics)));
    }

    pub async fn update_attachments(&self, attachments: Vec<String>) -> anyhow::Result<()> {
        let mut new_attachments = HashMap::new();
        for (index, path) in attachments.iter().enumerate() {
            let attach_name = Path::new(path)
                .file_stem()
                .and_then(|stem| stem.to_str())
                .map(|stem| stem.to_owned())
                .unwrap_or_else(|| format!("archive{index}"));

            if is_system_db(&attach_name) {
                continue;
            }

            let escaped_name = escape_identifier(&attach_name);
            let escaped_path = escape_literal(&format!("file:{}?mode=ro", path));
            new_attachments.insert(escaped_name, escaped_path);
        }

        self.attachments.store(Arc::new(new_attachments));

        let new_pool = self.create_pool_with_attachments().await?;
        self.replace_pool_and_probe(new_pool).await;

        Ok(())
    }

    fn replace_pool(&self, new_pool: SqlitePool) {
        let old_pool = self.pool.swap(Arc::new(new_pool));
        tokio::spawn(async move {
            let pool = (*old_pool).clone();
            pool.close().await;
        });
    }

    async fn create_pool_with_attachments(&self) -> anyhow::Result<SqlitePool> {
        let db_path_str = self.db_path.display().to_string();
        let connect_string = format!("{db_path_str}?mode=ro");

        let connect_options =
            SqliteConnectOptions::from_str(&connect_string)?.create_if_missing(false);

        let attachments = self.attachments.load_full().clone();
        let history_proofs_db = self.history_proofs_db.load_full();
        let max_conn = **self.max_connections.load();
        let acquire_timeout = **self.acquire_timeout.load();
        let pragmas =
            build_read_pragmas(**self.sqlite_mmap_size.load(), **self.sqlite_cache_size.load());

        tracing::info!(pragmas = pragmas.as_str(), "Recreating pool with SQLite read PRAGMAs");

        let pool = SqlitePoolOptions::new()
            .max_connections(max_conn)
            .acquire_timeout(acquire_timeout)
            .after_connect(move |conn, _meta| {
                let attachments = attachments.clone();
                let history_proofs_db = history_proofs_db.clone();
                let pragmas = pragmas.clone();
                Box::pin(async move {
                    sqlx::raw_sql(sqlx::AssertSqlSafe(pragmas.clone())).execute(&mut *conn).await?;
                    // TODO: remove after verifying PRAGMAs in production
                    tracing::info!("Applied SQLite read PRAGMAs to new connection");
                    // Attached before the archives and outside
                    // `attached_db_names()`: it is not an archive and no
                    // UNION over archives must see it.
                    if let Some(path) = &*history_proofs_db {
                        let sql = format!(
                            "ATTACH DATABASE '{}' AS '{}'",
                            path,
                            defaults::HISTORY_PROOFS_DB_ALIAS
                        );
                        tracing::trace!(sql);

                        if let Err(err) = QueryBuilder::new(sql).build().execute(&mut *conn).await {
                            tracing::error!("ATTACH of the history proofs store failed: {err}");
                        }
                    }
                    for (attach_name, path) in attachments.iter() {
                        let sql = format!("ATTACH DATABASE '{}' AS '{}'", path, attach_name);
                        tracing::trace!(sql);

                        if let Err(err) = QueryBuilder::new(sql).build().execute(&mut *conn).await {
                            tracing::error!("ATTACH failed: {err}");
                        }
                    }
                    Ok(())
                })
            })
            .connect_with(connect_options)
            .await?;

        Ok(pool)
    }

    pub async fn get_connection(&self) -> anyhow::Result<TimedConnection> {
        let pool = self.pool.load_full();
        let mut conn = pool.acquire().await?;
        let histogram =
            self.metrics.load().as_ref().as_ref().map(|m| m.sqlite_query_duration.clone());

        let timeout_active = Arc::new(AtomicBool::new(true));
        let interrupted = Arc::new(AtomicBool::new(false));
        let handler_active = Arc::clone(&timeout_active);
        let handler_interrupted = Arc::clone(&interrupted);
        let deadline = Instant::now() + *self.query_timeout.load_full();

        let mut handle = conn.lock_handle().await?;
        handle.set_progress_handler(PROGRESS_HANDLER_OPS, move || {
            let should_interrupt =
                handler_active.load(Ordering::Acquire) && Instant::now() > deadline;
            if should_interrupt {
                handler_interrupted.store(true, Ordering::Release);
            }
            // Return `false` to interrupt, `true` to continue.
            !should_interrupt
        });
        drop(handle);

        Ok(TimedConnection {
            conn,
            start: Instant::now(),
            histogram,
            timeout_active,
            interrupted,
            sql: None,
        })
    }

    pub fn pool_size(&self) -> u32 {
        self.pool.load().size()
    }

    pub fn pool_idle(&self) -> usize {
        self.pool.load().num_idle()
    }

    /// The lowest thread-0 height served from the main DB, the archives and
    /// the history proofs store, with that block's `gen_utime`.
    pub async fn oldest_thread0(&self) -> anyhow::Result<Option<(u64, Option<i64>)>> {
        let mut oldest: Option<(u64, Option<i64>)> = None;
        let mut keep = |height: u64, gen_utime: Option<i64>| {
            if oldest.is_none_or(|(current, _)| height < current) {
                oldest = Some((height, gen_utime));
            }
        };
        let union_sql = self
            .attached_db_names()
            .into_iter()
            .map(|name| {
                format!(
                    "SELECT * FROM (SELECT height, gen_utime FROM \"{name}\".blocks \
                     WHERE thread_id = '{THREAD_ZERO}' ORDER BY height LIMIT 1)"
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let mut conn = self.get_connection().await?;
        conn.set_sql(&union_sql);
        let rows: Vec<(Option<Vec<u8>>, Option<i64>)> =
            sqlx::QueryBuilder::new(union_sql).build_query_as().fetch_all(&mut *conn).await?;
        for (height, gen_utime) in rows {
            if let Some(height) = height.and_then(|h| <[u8; 8]>::try_from(h.as_slice()).ok()) {
                keep(u64::from_be_bytes(height), gen_utime);
            }
        }
        if self.history_store_attached() {
            let sql = format!(
                "SELECT height, gen_utime FROM \"{}\".blocks ORDER BY height LIMIT 1",
                defaults::HISTORY_PROOFS_DB_ALIAS
            );
            conn.set_sql(&sql);
            let row: Option<(i64, Option<i64>)> =
                sqlx::QueryBuilder::new(sql).build_query_as().fetch_optional(&mut *conn).await?;
            if let Some((height, gen_utime)) = row {
                keep(height as u64, gen_utime);
            }
        }
        Ok(oldest)
    }

    pub fn attached_db_names(&self) -> Vec<String> {
        let mut list: Vec<String> = self.attachments.load_full().keys().cloned().collect();

        list.push("main".to_string());
        list
    }

    /// Attached archives newest first, from a single snapshot of the attachments, without the
    /// oldest one: the one most likely to be cut off by retention. Archives are named
    /// `bm-archive-<unix_ts>.db` and attached under their file stem; a stem whose suffix after
    /// the last `-` is not a number counts as `0`. `main` is not included.
    pub fn attached_db_names_except_oldest(&self) -> Vec<String> {
        let attachments = self.attachments.load_full();
        let mut names: Vec<(u64, &String)> =
            attachments.keys().map(|name| (archive_timestamp(name), name)).collect();
        names.sort_unstable();
        names.into_iter().skip(1).rev().map(|(_, name)| name.clone()).collect()
    }
}

/// Suffix after the last `-` of an attached archive name (`bm-archive-<unix_ts>`); `0` when it
/// is not a number.
fn archive_timestamp(name: &str) -> u64 {
    name.rsplit('-').next().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0)
}

fn is_system_db(name: &str) -> bool {
    matches!(name, "main" | "temp") || name == defaults::HISTORY_PROOFS_DB_ALIAS
}

fn escape_identifier(value: &str) -> String {
    value.replace('"', "\"\"")
}

fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// The tables of the history proofs store that gql-server reads.
const HISTORY_TABLES: [&str; 4] = ["blocks", "events", "bk_set_updates", "attestations"];

/// The tables of `HISTORY_TABLES` that are not in `found`.
fn missing_history_tables(found: &[String]) -> Vec<&'static str> {
    HISTORY_TABLES.into_iter().filter(|table| !found.iter().any(|name| name == table)).collect()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use sqlx::Row;

    use super::DBConnector;
    use crate::defaults;
    use crate::schema::db::build_read_pragmas;
    use crate::web;

    /// The tables gql-server requires are the ones block-manager creates.
    #[test]
    fn history_tables_are_in_the_store_schema() {
        let dir = testdir::testdir!();
        let store = database::sqlite::history_proofs::HistoryProofsStore::open(&dir).unwrap();
        let tables: Vec<String> = store
            .connection()
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(super::missing_history_tables(&tables), Vec::<&str>::new(), "{tables:?}");
    }

    #[test]
    fn missing_history_tables_names_each_absent_table() {
        let found = ["attestations".to_string(), "blocks".to_string()];
        assert_eq!(super::missing_history_tables(&found), vec!["events", "bk_set_updates"]);
    }

    async fn setup_connector() -> (Arc<DBConnector>, std::path::PathBuf) {
        let root = PathBuf::from("./tests/fixtures/popitgame");
        let db_path = root.join("bm-archive.db");
        let pool = web::open_db(
            db_path.clone(),
            15,
            Duration::from_secs(defaults::DEFAULT_ACQUIRE_TIMEOUT_SECS),
            build_read_pragmas(
                defaults::DEFAULT_SQLITE_MMAP_SIZE,
                defaults::DEFAULT_SQLITE_CACHE_SIZE,
            ),
        )
        .await
        .expect("open `main` DB");
        (DBConnector::new(pool, db_path, defaults::MAX_POOL_CONNECTIONS), root)
    }

    async fn setup_connector_with_timeout(
        timeout: Duration,
    ) -> (Arc<DBConnector>, std::path::PathBuf) {
        let (connector, root) = setup_connector().await;
        connector.set_query_timeout(timeout);
        (connector, root)
    }

    #[tokio::test]
    async fn query_timeout_interrupts_long_query() {
        let (connector, _root) = setup_connector_with_timeout(Duration::ZERO).await;

        let mut conn = connector.get_connection().await.expect("get conn");

        // Recursive CTE generates enough VM instructions to trigger the
        // progress handler even with a zero-duration deadline.
        let heavy_sql = "WITH RECURSIVE cnt(x) AS \
            (VALUES(1) UNION ALL SELECT x+1 FROM cnt WHERE x < 1000000) \
            SELECT COUNT(*) FROM cnt";
        conn.set_sql(heavy_sql);

        let result = sqlx::query(heavy_sql).fetch_one(&mut *conn).await;

        assert!(result.is_err(), "query should be interrupted by timeout");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("interrupted") || err.contains("Interrupt"),
            "error should mention interrupt, got: {err}"
        );
        assert!(conn.interrupted.load(Ordering::Acquire), "interrupted flag should be set");
    }

    #[tokio::test]
    async fn query_succeeds_within_timeout() {
        let (connector, _root) = setup_connector_with_timeout(Duration::from_secs(10)).await;

        let mut conn = connector.get_connection().await.expect("get conn");
        conn.set_sql("SELECT COUNT(*) FROM messages");

        let row = sqlx::query("SELECT COUNT(*) FROM messages")
            .fetch_one(&mut *conn)
            .await
            .expect("query should succeed within timeout");

        let count: i64 = row.get(0);
        assert!(count > 0, "should have rows");
        assert!(!conn.interrupted.load(Ordering::Acquire), "interrupted flag should not be set");
    }

    #[tokio::test]
    async fn timeout_deactivated_after_drop() {
        let (connector, _root) = setup_connector_with_timeout(Duration::ZERO).await;

        {
            let mut conn = connector.get_connection().await.expect("get conn");
            let _ = sqlx::query("SELECT id FROM messages").fetch_all(&mut *conn).await;
            // conn dropped here — timeout_active set to false
        }

        // Next connection from the same pool should work fine
        let (connector2, _) = setup_connector_with_timeout(Duration::from_secs(10)).await;
        let mut conn2 = connector2.get_connection().await.expect("get conn");
        let result = sqlx::query("SELECT COUNT(*) FROM messages").fetch_one(&mut *conn2).await;
        assert!(result.is_ok(), "query on fresh connection should succeed");
    }

    #[tokio::test]
    async fn attach_new_dbs() {
        // crate::helpers::init_tracing();
        let (connector, root) = setup_connector().await;

        let archive1 = root.join("bm-archive-1.db");
        let archive2 = root.join("bm-archive-2.db");
        let archive3 = root.join("bm-archive-3.db");

        let attachments = vec![archive3.to_string_lossy().into_owned()];

        connector.update_attachments(attachments).await.expect("attach new dbs");

        let mut names = connector.attached_db_names();
        names.sort();
        assert_eq!(names, vec!["bm-archive-3".to_string(), "main".to_string()]);

        let attachments = vec![
            archive3.to_string_lossy().into_owned(),
            archive2.to_string_lossy().into_owned(),
            archive1.to_string_lossy().into_owned(),
        ];

        connector.update_attachments(attachments).await.expect("attach new dbs");

        let mut names = connector.attached_db_names();
        names.sort();
        assert_eq!(
            names,
            vec![
                "bm-archive-1".to_string(),
                "bm-archive-2".to_string(),
                "bm-archive-3".to_string(),
                "main".to_string()
            ]
        );

        let _conn = connector.get_connection().await.expect("get conn with attachments");
    }

    #[tokio::test]
    async fn read_pragmas_applied_to_connection() {
        let (connector, _root) = setup_connector().await;
        let mut conn = connector.get_connection().await.expect("get conn");

        let row: (i64,) = sqlx::query_as("PRAGMA query_only")
            .fetch_one(&mut *conn)
            .await
            .expect("PRAGMA query_only");
        assert_eq!(row.0, 1, "query_only should be ON");

        let row: (i64,) = sqlx::query_as("PRAGMA cache_size")
            .fetch_one(&mut *conn)
            .await
            .expect("PRAGMA cache_size");
        assert!(row.0 < 0, "cache_size should be negative (KB mode), got {}", row.0);

        let row: (i64,) = sqlx::query_as("PRAGMA temp_store")
            .fetch_one(&mut *conn)
            .await
            .expect("PRAGMA temp_store");
        assert_eq!(row.0, 2, "temp_store should be MEMORY (2)");

        let row: (i64,) = sqlx::query_as("PRAGMA mmap_size")
            .fetch_one(&mut *conn)
            .await
            .expect("PRAGMA mmap_size");
        assert!(row.0 > 0, "mmap_size should be positive, got {}", row.0);
    }

    #[tokio::test]
    async fn new_stores_custom_max_connections() {
        let root = PathBuf::from("./tests/fixtures/popitgame");
        let db_path = root.join("bm-archive.db");
        let pool = web::open_db(
            db_path.clone(),
            30,
            Duration::from_secs(defaults::DEFAULT_ACQUIRE_TIMEOUT_SECS),
            build_read_pragmas(
                defaults::DEFAULT_SQLITE_MMAP_SIZE,
                defaults::DEFAULT_SQLITE_CACHE_SIZE,
            ),
        )
        .await
        .expect("open DB");
        let connector = DBConnector::new(pool, db_path, 30);

        assert_eq!(**connector.max_connections.load(), 30);
    }

    #[tokio::test]
    async fn set_max_connections_recreates_pool() {
        let (connector, _root) = setup_connector().await;
        assert_eq!(**connector.max_connections.load(), defaults::MAX_POOL_CONNECTIONS);

        connector.set_max_connections(25).await.expect("set max connections");
        assert_eq!(**connector.max_connections.load(), 25);

        // Same value should be a no-op (no error, value unchanged).
        connector.set_max_connections(25).await.expect("set same value");
        assert_eq!(**connector.max_connections.load(), 25);
    }

    #[tokio::test]
    async fn set_acquire_timeout_recreates_pool() {
        let (connector, _root) = setup_connector().await;
        let default_timeout = Duration::from_secs(defaults::DEFAULT_ACQUIRE_TIMEOUT_SECS);
        assert_eq!(**connector.acquire_timeout.load(), default_timeout);

        let new_timeout = Duration::from_secs(20);
        connector.set_acquire_timeout(new_timeout).await.expect("set acquire timeout");
        assert_eq!(**connector.acquire_timeout.load(), new_timeout);

        // Same value should be a no-op.
        connector.set_acquire_timeout(new_timeout).await.expect("set same value");
        assert_eq!(**connector.acquire_timeout.load(), new_timeout);
    }

    #[tokio::test]
    async fn attach_dbs_skips_system_names() {
        let (connector, root) = setup_connector().await;

        let main_db = root.join("main.db");
        let temp_db = root.join("temp.db");
        let normal_db = root.join("bm-archive-1.db");

        let attachments = vec![
            main_db.to_string_lossy().into_owned(),
            temp_db.to_string_lossy().into_owned(),
            normal_db.to_string_lossy().into_owned(),
        ];

        connector.update_attachments(attachments).await.expect("attach dbs");

        let mut names = connector.attached_db_names();
        names.sort();
        assert_eq!(names, vec!["bm-archive-1".to_string(), "main".to_string()]);
    }

    #[tokio::test]
    async fn oldest_thread0_covers_file() {
        use database::sqlite::history_proofs::HistoryProofsStore;
        use database::sqlite::history_proofs::HistoryRow;
        use migration_tool::DbInfo;
        use migration_tool::DbMaintenance;
        use migration_tool::DbMaintenanceOptions;
        use migration_tool::MigrateTo;

        // The library and the binary target both run this test and share the
        // directory, so every statement has to tolerate a second run.
        let root = testdir::testdir!();
        let open = |db_dir: &std::path::Path| {
            let maintenance = DbMaintenance::new(&DbInfo::BM_ARCHIVE, db_dir);
            maintenance
                .migrate(
                    MigrateTo::Latest,
                    DbMaintenanceOptions { silent: true, ..Default::default() },
                )
                .expect("migrate the main DB");
            maintenance.path
        };
        let main_db = open(&root.join("main"));
        let empty_db = open(&root.join("empty"));
        {
            let conn = rusqlite::Connection::open(&main_db).unwrap();
            let thread_zero = "0".repeat(68);
            for (id, thread, height) in [
                ("a", thread_zero.as_str(), 700u64),
                ("b", thread_zero.as_str(), 500),
                ("c", "01", 10),
            ] {
                conn.execute(
                    "INSERT OR IGNORE INTO blocks (id, status, seq_no, parent, thread_id, height, gen_utime)
                     VALUES (?1, 2, ?2, 'p', ?3, ?4, 1800000000)",
                    rusqlite::params![id, height as i64, thread, height.to_be_bytes().to_vec()],
                )
                .unwrap();
            }
        }
        let store_path = root.join("store").join(defaults::HISTORY_PROOFS_DB_FILE);
        std::fs::create_dir_all(store_path.parent().unwrap()).unwrap();
        {
            let mut store =
                HistoryProofsStore::open_file(&store_path, Duration::from_secs(5), true).unwrap();
            for height in [100u64, 300] {
                let row = HistoryRow {
                    height,
                    block_id: [height as u8; 32],
                    envelope_hash: [0; 32],
                    tracked_root: [0; 32],
                    tracked_hashes: None,
                    history_proofs: None,
                    block_merkle_leaves: None,
                    gen_utime: Some(1_700_000_000 + height as i64),
                };
                store.insert_block(&row, &[]).unwrap();
            }
        }
        let connector = |db: PathBuf| async move {
            let pool = web::open_db(
                db.clone(),
                2,
                Duration::from_secs(defaults::DEFAULT_ACQUIRE_TIMEOUT_SECS),
                build_read_pragmas(
                    defaults::DEFAULT_SQLITE_MMAP_SIZE,
                    defaults::DEFAULT_SQLITE_CACHE_SIZE,
                ),
            )
            .await
            .expect("open `main` DB");
            DBConnector::new(pool, db, 2)
        };

        let main = connector(main_db.clone()).await;
        assert_eq!(main.oldest_thread0().await.unwrap(), Some((500, Some(1_800_000_000))));
        main.set_history_proofs_db(Some(store_path.as_path()));
        main.update_attachments(vec![]).await.unwrap();
        assert_eq!(main.oldest_thread0().await.unwrap(), Some((100, Some(1_700_000_100))));

        let empty = connector(empty_db).await;
        assert_eq!(empty.oldest_thread0().await.unwrap(), None);
    }

    #[tokio::test]
    async fn attaches_history_proofs_db_next_to_nine_archives() {
        // The library and the binary target both run this test and share the
        // directory, so every statement has to tolerate a second run.
        let root = testdir::testdir!();
        let main_db = root.join("bm-archive.db");
        rusqlite::Connection::open(&main_db)
            .and_then(|conn| conn.execute_batch("CREATE TABLE IF NOT EXISTS t (x)"))
            .expect("create main DB");

        let mut archives = Vec::new();
        for ts in 1..=9 {
            let path = root.join(format!("bm-archive-{ts}.db"));
            rusqlite::Connection::open(&path)
                .and_then(|conn| conn.execute_batch("CREATE TABLE IF NOT EXISTS t (x)"))
                .expect("create archive DB");
            archives.push(path.to_string_lossy().into_owned());
        }

        let history_db = root.join(defaults::HISTORY_PROOFS_DB_FILE);
        rusqlite::Connection::open(&history_db)
            .and_then(|conn| {
                conn.execute_batch(
                    "CREATE TABLE IF NOT EXISTS history_proofs (
                        thread_id TEXT NOT NULL,
                        height BLOB NOT NULL,
                        block_id TEXT NOT NULL,
                        gen_utime INTEGER,
                        history_proofs BLOB NOT NULL,
                        PRIMARY KEY (thread_id, height)
                    ) WITHOUT ROWID",
                )
            })
            .expect("create history proofs store");

        let pool = web::open_db(
            main_db.clone(),
            2,
            Duration::from_secs(defaults::DEFAULT_ACQUIRE_TIMEOUT_SECS),
            build_read_pragmas(
                defaults::DEFAULT_SQLITE_MMAP_SIZE,
                defaults::DEFAULT_SQLITE_CACHE_SIZE,
            ),
        )
        .await
        .expect("open `main` DB");
        let connector = DBConnector::new(pool, main_db, 2);
        assert!(!connector.has_history_proofs_db());

        connector.set_history_proofs_db(Some(history_db.as_path()));
        connector.update_attachments(archives).await.expect("attach archives");
        assert!(connector.has_history_proofs_db());

        // Nine archives and `main`; the store is not an archive.
        let names = connector.attached_db_names();
        assert_eq!(names.len(), 10);
        assert!(!names.iter().any(|name| name == defaults::HISTORY_PROOFS_DB_ALIAS));

        let mut conn = connector.get_connection().await.expect("get conn");
        let attached: Vec<String> = sqlx::query("PRAGMA database_list")
            .fetch_all(&mut *conn)
            .await
            .expect("database_list")
            .iter()
            .map(|row| row.get::<String, _>(1))
            .collect();
        for name in &names {
            assert!(attached.contains(name), "{name} is not attached: {attached:?}");
        }
        assert!(
            attached.iter().any(|name| name == defaults::HISTORY_PROOFS_DB_ALIAS),
            "the history proofs store is not attached: {attached:?}"
        );

        let rows: i64 = sqlx::query("SELECT COUNT(*) FROM \"history_proofs_db\".history_proofs")
            .fetch_one(&mut *conn)
            .await
            .expect("query the history proofs store")
            .get(0);
        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn history_proofs_db_can_be_unset() {
        let (connector, root) = setup_connector().await;

        connector.set_history_proofs_db(Some(root.join("bm-archive-1.db").as_path()));
        assert!(connector.has_history_proofs_db());

        connector.set_history_proofs_db(None);
        assert!(!connector.has_history_proofs_db());
    }
}
