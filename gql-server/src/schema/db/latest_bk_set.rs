// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use sqlx::Row;

use crate::schema::db::DBConnector;

/// BK set snapshot maintained by block-manager in `latest_bk_set`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LatestBkSetSnapshot {
    pub thread_id: String,
    pub seq_no: u64,
    pub block_id: String,
    pub height: u64,
    pub chain_order: String,
    pub updated_at: u64,
    /// `(signer_index, BLS public key)` ordered by `signer_index`.
    pub signers: Vec<(u32, Vec<u8>)>,
}

/// Picks the probe error to report: the first one that was interrupted (so the client sees
/// `TIMEOUT`, see `map_db_error`), otherwise the first error.
fn select_failure(failures: &[(String, String)]) -> Option<&(String, String)> {
    failures.iter().find(|(_, text)| text.contains("interrupted")).or_else(|| failures.first())
}

/// Loads the latest BK set snapshot.
///
/// Returns `None` when there is no usable snapshot: the table is missing (BM
/// without migration 011), it is empty, its rows disagree on the anchor, the
/// stored values are malformed, or the anchor block is outside the history window
/// served by this node and every probe succeeded. The window excludes the oldest
/// attached archive, which may have been cut off by retention. When the anchor is
/// not found and at least one probe failed, an error is returned instead.
pub async fn load_latest_bk_set(db: &DBConnector) -> anyhow::Result<Option<LatestBkSetSnapshot>> {
    let mut conn = db.get_connection().await?;

    let sql = "SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND name = 'latest_bk_set'";
    conn.set_sql(sql);
    let has_table: Option<i64> = sqlx::query_scalar(sql).fetch_optional(&mut *conn).await?;
    if has_table.is_none() {
        tracing::debug!("latest_bk_set: no such table in main (BM archive without migration 011)");
        return Ok(None);
    }

    let sql = "SELECT signer_index, pubkey, thread_id, seq_no, block_id, height, chain_order, \
         updated_at FROM main.latest_bk_set ORDER BY signer_index";
    conn.set_sql(sql);
    let rows = sqlx::query(sql).fetch_all(&mut *conn).await?;

    let mut snapshot: Option<LatestBkSetSnapshot> = None;
    for row in rows {
        let signer_index = u32::try_from(row.try_get::<i64, _>("signer_index")?);
        let seq_no = u64::try_from(row.try_get::<i64, _>("seq_no")?);
        let updated_at = u64::try_from(row.try_get::<i64, _>("updated_at")?);
        let height: [u8; 8] = match row.try_get::<Vec<u8>, _>("height")?.try_into() {
            Ok(height) => height,
            Err(_) => {
                tracing::warn!("latest_bk_set: height is not 8 bytes");
                return Ok(None);
            }
        };
        let (Ok(signer_index), Ok(seq_no), Ok(updated_at)) = (signer_index, seq_no, updated_at)
        else {
            tracing::warn!("latest_bk_set: negative integer column");
            return Ok(None);
        };
        let pubkey: Vec<u8> = row.try_get("pubkey")?;
        let thread_id: String = row.try_get("thread_id")?;
        let block_id: String = row.try_get("block_id")?;
        let chain_order: String = row.try_get("chain_order")?;
        let height = u64::from_be_bytes(height);

        match &mut snapshot {
            None => {
                snapshot = Some(LatestBkSetSnapshot {
                    thread_id,
                    seq_no,
                    block_id,
                    height,
                    chain_order,
                    updated_at,
                    signers: vec![(signer_index, pubkey)],
                });
            }
            Some(first) => {
                if first.thread_id != thread_id
                    || first.seq_no != seq_no
                    || first.block_id != block_id
                    || first.height != height
                    || first.chain_order != chain_order
                    || first.updated_at != updated_at
                {
                    tracing::warn!("latest_bk_set: rows disagree on the anchor block");
                    return Ok(None);
                }
                first.signers.push((signer_index, pubkey));
            }
        }
    }
    let Some(snapshot) = snapshot else {
        tracing::debug!("latest_bk_set: table is empty");
        return Ok(None);
    };

    // `main` first: the anchor is normally there, and attached archives can be large cold files.
    let names = std::iter::once("main".to_string()).chain(db.attached_db_names_except_oldest());
    let mut failures: Vec<(String, String)> = Vec::new();
    for name in names {
        let sql = format!("SELECT 1 FROM \"{name}\".blocks WHERE id = ? LIMIT 1");
        conn.set_sql(sql.clone());
        match sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql))
            .bind(&snapshot.block_id)
            .fetch_optional(&mut *conn)
            .await
        {
            Ok(Some(_)) => return Ok(Some(snapshot)),
            Ok(None) => {}
            Err(err) => {
                tracing::debug!(db = %name, "latest_bk_set: anchor lookup failed: {err}");
                failures.push((name, err.to_string()));
            }
        }
    }
    if let Some((failed_db, err)) = select_failure(&failures) {
        // The anchor may sit in a DB we could not probe: report an error, not a false `null`.
        tracing::warn!(
            block_id = %snapshot.block_id,
            db = %failed_db,
            error = %err,
            failed_dbs = %failures.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>().join(","),
            "latest_bk_set: anchor not found and {} DB probe(s) failed",
            failures.len()
        );
        // The sqlx text goes into the message itself: callers format with `to_string()`, which
        // drops inner causes, and `map_db_error` must still see "interrupted".
        return Err(anyhow::anyhow!("latest_bk_set: anchor lookup failed: {err}"));
    }
    tracing::debug!(
        block_id = %snapshot.block_id,
        "latest_bk_set: anchor block is outside the served history window"
    );
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use migration_tool::DbInfo;
    use migration_tool::DbMaintenance;
    use migration_tool::DbMaintenanceOptions;
    use migration_tool::MigrateTo;
    use rusqlite::params;
    use rusqlite::Connection;
    use testdir::testdir;

    use super::load_latest_bk_set;
    use crate::defaults;
    use crate::schema::db::DBConnector;
    use crate::web;

    const ANCHOR: &str = "anchor-block";

    #[test]
    fn select_failure_prefers_the_interrupted_probe() {
        use super::select_failure;
        let f = |n: &str, t: &str| (n.to_string(), t.to_string());
        assert!(select_failure(&[]).is_none());
        let only = [f("main", "disk I/O error")];
        assert_eq!(select_failure(&only).unwrap().1, "disk I/O error");
        let mixed = [f("a", "no such table"), f("b", "interrupted"), f("c", "disk I/O error")];
        assert_eq!(select_failure(&mixed).unwrap().0, "b");
        let none_interrupted = [f("a", "first"), f("b", "second")];
        assert_eq!(select_failure(&none_interrupted).unwrap().0, "a");
    }

    /// Fresh directory per call: `testdir!()` is shared by the lib and bin test
    /// targets, which would otherwise trip over each other's database files.
    fn unique_dir() -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let dir = testdir!().join(format!("{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create test dir");
        dir
    }

    /// Creates `<dir>/<file>` with the BM archive schema at `target`.
    fn make_db(dir: &Path, file: &str, target: MigrateTo) -> PathBuf {
        let info: &'static DbInfo = Box::leak(Box::new(DbInfo::new(file)));
        let maintenance = DbMaintenance::new(info, dir);
        maintenance
            .migrate(target, DbMaintenanceOptions { silent: true, ..Default::default() })
            .expect("migrate");
        maintenance.path
    }

    fn insert_block(db: &Path, id: &str) {
        let conn = Connection::open(db).expect("open db");
        conn.execute(
            "INSERT INTO blocks (id, status, seq_no, parent) VALUES (?1, 0, 1, 'parent')",
            params![id],
        )
        .expect("insert block");
    }

    fn insert_signer(db: &Path, index: i64, pubkey: [u8; 48], block_id: &str, height: &[u8]) {
        let conn = Connection::open(db).expect("open db");
        conn.execute(
            "INSERT INTO latest_bk_set (signer_index, pubkey, thread_id, seq_no, block_id, \
             height, chain_order, updated_at) VALUES (?1, ?2, 'thread-A', 42, ?3, ?4, 'order-1', 777)",
            params![index, pubkey.to_vec(), block_id, height],
        )
        .expect("insert signer");
    }

    async fn connector(main: &Path, archives: &[&Path]) -> Arc<DBConnector> {
        let pool = web::open_db(
            main.to_path_buf(),
            15,
            std::time::Duration::from_secs(defaults::DEFAULT_ACQUIRE_TIMEOUT_SECS),
            crate::schema::db::build_read_pragmas(
                defaults::DEFAULT_SQLITE_MMAP_SIZE,
                defaults::DEFAULT_SQLITE_CACHE_SIZE,
            ),
        )
        .await
        .expect("open main db");
        let db = DBConnector::new(pool, main.to_path_buf(), defaults::MAX_POOL_CONNECTIONS);
        db.update_attachments(archives.iter().map(|p| p.to_string_lossy().into_owned()).collect())
            .await
            .expect("attach archives");
        db
    }

    /// main (with latest_bk_set) + bm-archive-100 (oldest) + bm-archive-200.
    fn setup() -> (PathBuf, PathBuf, PathBuf) {
        let dir = unique_dir();
        let main = make_db(&dir, "bm-archive.db", MigrateTo::Latest);
        let old = make_db(&dir, "bm-archive-100.db", MigrateTo::Latest);
        let new = make_db(&dir, "bm-archive-200.db", MigrateTo::Latest);
        (main, old, new)
    }

    #[tokio::test]
    async fn attached_db_names_except_oldest_is_newest_first() {
        let dir = unique_dir();
        let main = make_db(&dir, "bm-archive.db", MigrateTo::Latest);
        let a100 = make_db(&dir, "bm-archive-100.db", MigrateTo::Latest);
        let a200 = make_db(&dir, "bm-archive-200.db", MigrateTo::Latest);
        let a300 = make_db(&dir, "bm-archive-300.db", MigrateTo::Latest);

        let db = connector(&main, &[&a200, &a100, &a300]).await;
        assert_eq!(db.attached_db_names_except_oldest(), vec!["bm-archive-300", "bm-archive-200"]);

        let db = connector(&main, &[&a100]).await;
        assert!(db.attached_db_names_except_oldest().is_empty());

        let db = connector(&main, &[]).await;
        assert!(db.attached_db_names_except_oldest().is_empty());

        // Unparseable suffix counts as 0 and is therefore the oldest.
        let odd = make_db(&dir, "archive0.db", MigrateTo::Latest);
        let db = connector(&main, &[&a200, &odd, &a100]).await;
        assert_eq!(db.attached_db_names_except_oldest(), vec!["bm-archive-200", "bm-archive-100"]);

        // Suffixes compare as numbers: as strings "100" < "99" would make -100 the oldest.
        let a99 = make_db(&dir, "bm-archive-99.db", MigrateTo::Latest);
        let db = connector(&main, &[&a100, &a99]).await;
        assert_eq!(db.attached_db_names_except_oldest(), vec!["bm-archive-100"]);
    }

    #[tokio::test]
    async fn load_returns_none_without_table() {
        let dir = unique_dir();
        let main = make_db(&dir, "bm-archive.db", MigrateTo::Version(10));
        let db = connector(&main, &[]).await;
        assert!(load_latest_bk_set(&db).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn load_returns_none_for_empty_table() {
        let (main, old, new) = setup();
        let db = connector(&main, &[&old, &new]).await;
        assert!(load_latest_bk_set(&db).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn load_returns_snapshot_when_anchor_in_main() {
        let (main, old, new) = setup();
        insert_block(&main, ANCHOR);
        let height = 0x0102_0304_0506_0708u64.to_be_bytes();
        insert_signer(&main, 5, [5; 48], ANCHOR, &height);
        insert_signer(&main, 1, [1; 48], ANCHOR, &height);
        insert_signer(&main, 3, [3; 48], ANCHOR, &height);

        let db = connector(&main, &[&old, &new]).await;
        let snap = load_latest_bk_set(&db).await.unwrap().expect("snapshot");
        assert_eq!(snap.thread_id, "thread-A");
        assert_eq!(snap.seq_no, 42);
        assert_eq!(snap.block_id, ANCHOR);
        assert_eq!(snap.height, 0x0102_0304_0506_0708);
        assert_eq!(snap.chain_order, "order-1");
        assert_eq!(snap.updated_at, 777);
        assert_eq!(snap.signers, vec![(1, vec![1u8; 48]), (3, vec![3u8; 48]), (5, vec![5u8; 48])]);
    }

    #[tokio::test]
    async fn load_returns_snapshot_when_anchor_in_newer_archive() {
        let (main, old, new) = setup();
        insert_block(&new, ANCHOR);
        insert_signer(&main, 0, [9; 48], ANCHOR, &7u64.to_be_bytes());

        let db = connector(&main, &[&old, &new]).await;
        let snap = load_latest_bk_set(&db).await.unwrap().expect("snapshot");
        assert_eq!(snap.height, 7);
    }

    #[tokio::test]
    async fn load_returns_none_when_anchor_only_in_oldest_archive() {
        let (main, old, new) = setup();
        insert_block(&old, ANCHOR);
        insert_signer(&main, 0, [9; 48], ANCHOR, &7u64.to_be_bytes());

        let db = connector(&main, &[&old, &new]).await;
        assert!(load_latest_bk_set(&db).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn load_returns_none_when_anchor_missing() {
        let (main, old, new) = setup();
        insert_block(&main, "some-other-block");
        insert_signer(&main, 0, [9; 48], ANCHOR, &7u64.to_be_bytes());

        let db = connector(&main, &[&old, &new]).await;
        assert!(load_latest_bk_set(&db).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn load_returns_none_on_inconsistent_anchor() {
        let (main, old, new) = setup();
        insert_block(&main, ANCHOR);
        insert_block(&main, "other-anchor");
        insert_signer(&main, 0, [1; 48], ANCHOR, &7u64.to_be_bytes());
        insert_signer(&main, 1, [2; 48], "other-anchor", &7u64.to_be_bytes());

        let db = connector(&main, &[&old, &new]).await;
        assert!(load_latest_bk_set(&db).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn load_returns_snapshot_when_a_probe_fails_but_the_anchor_is_found_elsewhere() {
        let (main, old, new) = setup();
        insert_signer(&main, 0, [9; 48], ANCHOR, &7u64.to_be_bytes());
        insert_block(&new, ANCHOR);
        // Probed before `-200` (newest first), has no `blocks` table.
        let broken = main.with_file_name("bm-archive-300.db");
        Connection::open(&broken).expect("create empty archive");

        let db = connector(&main, &[&old, &new, &broken]).await;
        let snap = load_latest_bk_set(&db).await.unwrap().expect("snapshot");
        assert_eq!(snap.block_id, ANCHOR);
    }

    #[tokio::test]
    async fn load_returns_error_when_probe_fails() {
        let (main, old, new) = setup();
        insert_block(&main, "some-other-block");
        insert_signer(&main, 0, [9; 48], ANCHOR, &7u64.to_be_bytes());
        // A newer archive without a `blocks` table: its probe fails.
        let broken = main.with_file_name("bm-archive-300.db");
        Connection::open(&broken).expect("create empty archive");

        let db = connector(&main, &[&old, &new, &broken]).await;
        let err = load_latest_bk_set(&db).await.expect_err("probe error");
        let message = err.to_string();
        assert!(message.contains("no such table"), "{message}");

        // An interrupted probe reaches the resolver's `map_db_error` as TIMEOUT.
        let interrupted =
            async_graphql::Error::new("latest_bk_set: anchor lookup failed: interrupted");
        let mapped = crate::schema::db::connector::map_db_error(interrupted);
        assert_eq!(mapped.message, "Request timeout");
    }
}
