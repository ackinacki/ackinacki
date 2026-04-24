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

    pub async fn set_max_connections(&self, max_conn: u32) -> anyhow::Result<()> {
        let current = **self.max_connections.load();
        if current == max_conn {
            return Ok(());
        }
        self.max_connections.store(Arc::new(max_conn));
        let new_pool = self.create_pool_with_attachments().await?;
        self.replace_pool(new_pool);
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
        self.replace_pool(new_pool);

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
                let pragmas = pragmas.clone();
                Box::pin(async move {
                    sqlx::raw_sql(sqlx::AssertSqlSafe(pragmas.clone())).execute(&mut *conn).await?;
                    // TODO: remove after verifying PRAGMAs in production
                    tracing::info!("Applied SQLite read PRAGMAs to new connection");
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

    pub fn attached_db_names(&self) -> Vec<String> {
        let mut list: Vec<String> = self.attachments.load_full().keys().cloned().collect();

        list.push("main".to_string());
        list
    }
}

fn is_system_db(name: &str) -> bool {
    matches!(name, "main" | "temp")
}

fn escape_identifier(value: &str) -> String {
    value.replace('"', "\"\"")
}

fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
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
            let _ = sqlx::query("SELECT * FROM messages").fetch_all(&mut *conn).await;
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
}
