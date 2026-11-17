// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use anyhow::Context;
use rusqlite::params;

/// Snapshot of the BK set of thread 0, anchored to a block `(thread_id, seq_no)`.
#[derive(Clone, Debug, PartialEq)]
pub struct ArchLatestBkSet {
    pub thread_id: String,
    pub seq_no: u64,
    pub block_id: String,
    pub height: [u8; 8],
    pub chain_order: String,
    pub updated_at: u64,
    pub signers: Vec<(u16, [u8; 48])>,
}

/// Fields of the anchor block taken from the `blocks` table.
#[derive(Clone, Debug, PartialEq)]
pub struct AnchorBlock {
    pub block_id: String,
    pub height: [u8; 8],
    pub chain_order: String,
    pub block_merkle_leaves: Option<Vec<u8>>,
}

/// Replaces the whole `latest_bk_set` table with the given snapshot in one transaction.
pub fn replace_latest_bk_set(
    conn: &mut rusqlite::Connection,
    item: &ArchLatestBkSet,
) -> anyhow::Result<()> {
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM latest_bk_set", [])?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO latest_bk_set (
                signer_index, pubkey, thread_id, seq_no, block_id, height, chain_order, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        for (signer_index, pubkey) in &item.signers {
            stmt.execute(params![
                signer_index,
                pubkey.as_slice(),
                item.thread_id,
                i64::try_from(item.seq_no).context("seq_no does not fit into i64")?,
                item.block_id,
                item.height.as_slice(),
                item.chain_order,
                i64::try_from(item.updated_at).context("updated_at does not fit into i64")?,
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// Result of the anchor block lookup.
#[derive(Clone, Debug, PartialEq)]
pub enum AnchorLookup {
    Found(AnchorBlock),
    /// No block with this `(thread_id, seq_no)` is stored yet.
    Missing,
    /// The block can never serve as an anchor; the reason is for logs.
    Invalid(String),
}

/// Looks up the anchor block. Only `blocks.id` is unique, so more than one row for
/// `(thread_id, seq_no)` is an ambiguous anchor, as is a row without a valid `height` or
/// `chain_order` and a `seq_no` that does not fit into the `INTEGER` column: all `Invalid`.
pub fn find_anchor_block(
    conn: &rusqlite::Connection,
    thread_id: &str,
    seq_no: u64,
) -> anyhow::Result<AnchorLookup> {
    let Ok(seq_no_db) = i64::try_from(seq_no) else {
        return Ok(AnchorLookup::Invalid(format!("seq_no {seq_no} does not fit into i64")));
    };
    let mut stmt = conn.prepare_cached(
        "SELECT id, height, chain_order, block_merkle_leaves
         FROM blocks WHERE thread_id = ?1 AND seq_no = ?2 LIMIT 2",
    )?;
    let rows = stmt
        .query_map(params![thread_id, seq_no_db], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<Vec<u8>>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<Vec<u8>>>(3)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut rows = rows.into_iter();
    let Some((block_id, height, chain_order, block_merkle_leaves)) = rows.next() else {
        return Ok(AnchorLookup::Missing);
    };
    if rows.next().is_some() {
        return Ok(AnchorLookup::Invalid(format!(
            "more than one block with seq_no {seq_no} in the thread"
        )));
    }
    let Some(height) = height else {
        return Ok(AnchorLookup::Invalid(format!("block {block_id} has NULL height")));
    };
    let Ok(height) = <[u8; 8]>::try_from(height.as_slice()) else {
        return Ok(AnchorLookup::Invalid(format!(
            "block {block_id} height is {} bytes, expected 8",
            height.len()
        )));
    };
    let Some(chain_order) = chain_order else {
        return Ok(AnchorLookup::Invalid(format!("block {block_id} has NULL chain_order")));
    };
    Ok(AnchorLookup::Found(AnchorBlock { block_id, height, chain_order, block_merkle_leaves }))
}

/// Reads the whole snapshot back. An empty table is `None`; rows that disagree on the anchor
/// fields, or carry a malformed `pubkey`, `height` or number, are an error.
pub fn load_latest_bk_set(conn: &rusqlite::Connection) -> anyhow::Result<Option<ArchLatestBkSet>> {
    let mut stmt = conn.prepare_cached(
        "SELECT signer_index, pubkey, thread_id, seq_no, block_id, height, chain_order, updated_at
         FROM latest_bk_set ORDER BY signer_index",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Vec<u8>>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i64>(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut snapshot: Option<ArchLatestBkSet> = None;
    for (signer_index, pubkey, thread_id, seq_no, block_id, height, chain_order, updated_at) in rows
    {
        let signer_index =
            u16::try_from(signer_index).context("latest_bk_set.signer_index is out of range")?;
        let pubkey = <[u8; 48]>::try_from(pubkey.as_slice()).map_err(|_| {
            anyhow::anyhow!("latest_bk_set.pubkey is {} bytes, expected 48", pubkey.len())
        })?;
        let height = <[u8; 8]>::try_from(height.as_slice()).map_err(|_| {
            anyhow::anyhow!("latest_bk_set.height is {} bytes, expected 8", height.len())
        })?;
        let seq_no = u64::try_from(seq_no).context("latest_bk_set.seq_no is negative")?;
        let updated_at =
            u64::try_from(updated_at).context("latest_bk_set.updated_at is negative")?;
        match snapshot.as_mut() {
            None => {
                snapshot = Some(ArchLatestBkSet {
                    thread_id,
                    seq_no,
                    block_id,
                    height,
                    chain_order,
                    updated_at,
                    signers: vec![(signer_index, pubkey)],
                });
            }
            Some(s) => {
                anyhow::ensure!(
                    s.thread_id == thread_id
                        && s.seq_no == seq_no
                        && s.block_id == block_id
                        && s.height == height
                        && s.chain_order == chain_order
                        && s.updated_at == updated_at,
                    "latest_bk_set rows disagree on the anchor fields"
                );
                s.signers.push((signer_index, pubkey));
            }
        }
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;
    use std::time::Duration;
    use std::time::Instant;

    use migration_tool::DbInfo;
    use migration_tool::DbMaintenance;
    use migration_tool::DbMaintenanceOptions;
    use migration_tool::MigrateTo;
    use rusqlite::params;
    use rusqlite::Connection;
    use testdir::testdir;

    use super::find_anchor_block;
    use super::load_latest_bk_set;
    use super::replace_latest_bk_set;
    use super::AnchorBlock;
    use super::AnchorLookup;
    use super::ArchLatestBkSet;
    use crate::sqlite::sqlite_helper::SqliteHelper;
    use crate::sqlite::sqlite_helper::SqliteHelperConfig;
    use crate::sqlite::sqlite_helper::SQLITE_EMPTY_DB;

    fn migrate(dir: &Path) -> anyhow::Result<PathBuf> {
        migrate_to(dir, MigrateTo::Latest)
    }

    fn migrate_to(dir: &Path, target: MigrateTo) -> anyhow::Result<PathBuf> {
        let db_maintenance = DbMaintenance::new(&DbInfo::BM_ARCHIVE, dir);
        db_maintenance
            .migrate(target, DbMaintenanceOptions { silent: true, ..Default::default() })?;
        Ok(db_maintenance.path)
    }

    fn snapshot(seq_no: u64, signers: usize) -> ArchLatestBkSet {
        ArchLatestBkSet {
            thread_id: "thread-0".to_string(),
            seq_no,
            block_id: format!("block-{seq_no}"),
            height: seq_no.to_be_bytes(),
            chain_order: format!("order-{seq_no}"),
            updated_at: 1000 + seq_no,
            signers: (0..signers).map(|i| (i as u16, [i as u8 + 1; 48])).collect(),
        }
    }

    fn insert_block(
        conn: &Connection,
        id: &str,
        thread_id: &str,
        seq_no: u64,
        height: Option<Vec<u8>>,
        chain_order: Option<&str>,
        leaves: Option<Vec<u8>>,
    ) -> anyhow::Result<()> {
        conn.execute(
            "INSERT INTO blocks (id, status, seq_no, parent, thread_id, height, chain_order, \
             block_merkle_leaves) VALUES (?1, 0, ?2, 'parent', ?3, ?4, ?5, ?6)",
            params![id, seq_no as i64, thread_id, height, chain_order, leaves],
        )?;
        Ok(())
    }

    #[test]
    fn replace_keeps_only_the_latest_snapshot() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut conn = Connection::open(migrate(&dir)?)?;
        replace_latest_bk_set(&mut conn, &snapshot(10, 3))?;
        let b = snapshot(11, 2);
        replace_latest_bk_set(&mut conn, &b)?;

        let mut stmt = conn.prepare(
            "SELECT signer_index, pubkey, thread_id, seq_no, block_id, height, chain_order, \
             updated_at FROM latest_bk_set ORDER BY signer_index",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, u16>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, u64>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, Vec<u8>>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, u64>(7)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(rows.len(), 2);
        for (row, (idx, key)) in rows.iter().zip(&b.signers) {
            assert_eq!(row.0, *idx);
            assert_eq!(row.1, key.to_vec());
            assert_eq!(row.2, b.thread_id);
            assert_eq!(row.3, b.seq_no);
            assert_eq!(row.4, b.block_id);
            assert_eq!(row.5, b.height.to_vec());
            assert_eq!(row.6, b.chain_order);
            assert_eq!(row.7, b.updated_at);
        }
        Ok(())
    }

    #[test]
    fn find_anchor_block_returns_block_fields() -> anyhow::Result<()> {
        let dir = testdir!();
        let conn = Connection::open(migrate(&dir)?)?;
        let height = 77_u64.to_be_bytes();
        insert_block(
            &conn,
            "b1",
            "t0",
            5,
            Some(height.to_vec()),
            Some("co"),
            Some(vec![0x50; 512]),
        )?;
        assert_eq!(
            find_anchor_block(&conn, "t0", 5)?,
            AnchorLookup::Found(AnchorBlock {
                block_id: "b1".to_string(),
                height,
                chain_order: "co".to_string(),
                block_merkle_leaves: Some(vec![0x50; 512]),
            })
        );
        Ok(())
    }

    #[test]
    fn find_anchor_block_misses_other_seq_no_or_thread() -> anyhow::Result<()> {
        let dir = testdir!();
        let conn = Connection::open(migrate(&dir)?)?;
        insert_block(&conn, "b1", "t0", 5, Some(vec![0; 8]), Some("co"), None)?;
        assert_eq!(find_anchor_block(&conn, "t0", 6)?, AnchorLookup::Missing);
        assert_eq!(find_anchor_block(&conn, "t1", 5)?, AnchorLookup::Missing);
        assert!(matches!(find_anchor_block(&conn, "t0", 5)?, AnchorLookup::Found(_)));
        Ok(())
    }

    #[test]
    fn find_anchor_block_reports_missing_and_invalid() -> anyhow::Result<()> {
        let dir = testdir!();
        let conn = Connection::open(migrate(&dir)?)?;
        assert_eq!(find_anchor_block(&conn, "t0", 1)?, AnchorLookup::Missing);

        insert_block(&conn, "b1", "t0", 5, Some(vec![0; 8]), Some("co1"), None)?;
        insert_block(&conn, "b2", "t0", 5, Some(vec![1; 8]), Some("co2"), None)?;
        assert!(matches!(find_anchor_block(&conn, "t0", 5)?, AnchorLookup::Invalid(_)));

        insert_block(&conn, "b3", "t0", 6, Some(vec![0; 8]), None, None)?;
        assert!(matches!(find_anchor_block(&conn, "t0", 6)?, AnchorLookup::Invalid(_)));

        insert_block(&conn, "b4", "t0", 7, None, Some("co"), None)?;
        assert!(matches!(find_anchor_block(&conn, "t0", 7)?, AnchorLookup::Invalid(_)));

        // A non-8-byte `height` is rejected by the schema's CHECK, so it cannot be inserted.

        assert!(matches!(find_anchor_block(&conn, "t0", u64::MAX)?, AnchorLookup::Invalid(_)));
        Ok(())
    }

    #[test]
    fn load_round_trips_the_snapshot() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut conn = Connection::open(migrate(&dir)?)?;
        let snap = snapshot(42, 3);
        replace_latest_bk_set(&mut conn, &snap)?;
        assert_eq!(load_latest_bk_set(&conn)?, Some(snap));
        Ok(())
    }

    #[test]
    fn load_of_an_empty_table_is_none() -> anyhow::Result<()> {
        let dir = testdir!();
        let conn = Connection::open(migrate(&dir)?)?;
        assert_eq!(load_latest_bk_set(&conn)?, None);
        Ok(())
    }

    #[test]
    fn load_rejects_inconsistent_or_malformed_rows() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut conn = Connection::open(migrate(&dir)?)?;
        replace_latest_bk_set(&mut conn, &snapshot(42, 2))?;
        conn.execute("UPDATE latest_bk_set SET seq_no = 43 WHERE signer_index = 1", [])?;
        assert!(load_latest_bk_set(&conn).is_err());

        replace_latest_bk_set(&mut conn, &snapshot(42, 2))?;
        conn.execute("UPDATE latest_bk_set SET block_id = 'other' WHERE signer_index = 1", [])?;
        assert!(load_latest_bk_set(&conn).is_err());

        // A malformed `pubkey` or `height` is rejected by the schema's CHECK, so it cannot be
        // inserted; the length checks in the loader are a second line of defence.
        Ok(())
    }

    /// Prepares `bm-schema.db` and `bm-archive.db` (a copy) in `dir` and opens a helper on it.
    fn open_helper(dir: &Path) -> anyhow::Result<(SqliteHelper, std::thread::JoinHandle<()>)> {
        open_helper_at(dir, MigrateTo::Latest)
    }

    /// Like `open_helper`, with the schema migrated only up to `target`.
    fn open_helper_at(
        dir: &Path,
        target: MigrateTo,
    ) -> anyhow::Result<(SqliteHelper, std::thread::JoinHandle<()>)> {
        let schema = migrate_to(dir, target)?;
        assert_eq!(schema.file_name().unwrap(), SQLITE_EMPTY_DB);
        std::fs::copy(&schema, dir.join("bm-archive.db"))?;
        SqliteHelper::from_config(SqliteHelperConfig::new(dir.to_path_buf(), None))
    }

    /// Waits for the writer thread, which processes records asynchronously.
    fn wait_until(what: &str, cond: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !cond() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn failed_write_is_counted_and_not_confirmed() -> anyhow::Result<()> {
        let dir = testdir!();
        // Schema before `011-latest_bk_set`: the table does not exist.
        let (helper, handle) = open_helper_at(&dir, MigrateTo::Version(10))?;
        assert_eq!(helper.latest_bk_set_writes(), Default::default());

        helper.put_latest_bk_set(snapshot(10, 2))?;
        wait_until("the failed write", || helper.latest_bk_set_writes().failures == 1);
        assert_eq!(helper.latest_bk_set_writes().written_seq_no, 0);

        drop(helper);
        handle.join().unwrap();
        Ok(())
    }

    #[test]
    fn successful_write_is_confirmed_and_survives_later_failure() -> anyhow::Result<()> {
        let dir = testdir!();
        let (helper, handle) = open_helper(&dir)?;
        let clone = helper.clone();

        helper.put_latest_bk_set(snapshot(10, 2))?;
        wait_until("the write", || clone.latest_bk_set_writes().written_seq_no == 10);
        assert_eq!(clone.latest_bk_set_writes().failures, 0);

        // The table disappears, as after a rotation onto a `bm-schema.db` without 011.
        helper.conn.lock().as_ref().unwrap().execute("DROP TABLE latest_bk_set", [])?;
        helper.put_latest_bk_set(snapshot(11, 2))?;
        wait_until("the failed write", || clone.latest_bk_set_writes().failures == 1);
        assert_eq!(clone.latest_bk_set_writes().written_seq_no, 10);

        drop(helper);
        drop(clone);
        handle.join().unwrap();
        Ok(())
    }

    #[test]
    fn find_anchor_block_without_connection_is_an_error() -> anyhow::Result<()> {
        let dir = testdir!();
        let (helper, handle) = open_helper(&dir)?;
        {
            let guard = helper.conn.lock();
            insert_block(
                guard.as_ref().unwrap(),
                "b1",
                "t0",
                5,
                Some(vec![0; 8]),
                Some("co"),
                None,
            )?;
        }
        assert!(matches!(helper.find_anchor_block("t0", 5)?, AnchorLookup::Found(_)));
        assert_eq!(helper.load_latest_bk_set()?, None);
        helper.reset_connecton();
        assert!(helper.find_anchor_block("t0", 5).is_err());
        assert_eq!(helper.load_latest_bk_set()?, None);
        drop(helper);
        handle.join().unwrap();
        Ok(())
    }

    #[test]
    fn rotation_count_increments_and_is_shared_by_clones() -> anyhow::Result<()> {
        let dir = testdir!();
        let (mut helper, handle) = open_helper(&dir)?;
        let clone = helper.clone();
        assert_eq!(helper.rotation_count(), 0);
        assert_eq!(clone.rotation_count(), 0);

        let started = Instant::now();
        helper.rotate_db_file()?;
        assert!(started.elapsed() >= Duration::from_secs(2));

        assert_eq!(helper.rotation_count(), 1);
        assert_eq!(clone.rotation_count(), 1);

        drop(helper);
        drop(clone);
        handle.join().unwrap();
        Ok(())
    }
}
