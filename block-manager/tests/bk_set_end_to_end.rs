// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
//! Cross-crate checks of `latest_bk_set`: the node's `/v2/bk_set` body is accepted by BM's
//! validation, and the poller keeps the table filled across a DB file rotation on a real SQLite.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use block_manager::application::services::bk_set_poller::AdvanceReason;
use block_manager::application::services::bk_set_poller::Effect;
use block_manager::application::services::bk_set_poller::FetchedBkSet;
use block_manager::application::services::bk_set_poller::Poller;
use block_manager::application::services::bk_set_poller::PollerState;
use block_manager::domain::latest_bk_set::check_commitment;
use block_manager::domain::latest_bk_set::parse_response;
use block_manager::domain::latest_bk_set::validate;
use block_manager::domain::latest_bk_set::BkSetRejection;
use database::sqlite::sqlite_helper::SqliteHelper;
use database::sqlite::sqlite_helper::SqliteHelperConfig;
use migration_tool::DbInfo;
use migration_tool::DbMaintenance;
use migration_tool::DbMaintenanceOptions;
use migration_tool::MigrateTo;
use node::block_keeper_system::BlockKeeperData;
use node::block_keeper_system::BlockKeeperSet;
use node::bls::gosh_bls::PubKey;
use node::repository::repository_impl::BkSetUpdate;
use node::versioning::ProtocolVersionSupport;
use node_types::AccountIdentifier;
use node_types::ThreadIdentifier;
use rusqlite::params;
use rusqlite::Connection;
use testdir::testdir;

fn pubkey(i: u16) -> [u8; 48] {
    let mut ikm = [0x33u8; 32];
    ikm[..2].copy_from_slice(&i.to_le_bytes());
    gosh_blst::min_pk::SecretKey::key_gen(&ikm, &[]).unwrap().sk_to_pk().to_bytes()
}

/// Signer indices are deliberately not in order.
fn bk_set() -> BlockKeeperSet {
    bk_set_of(&[5, 0, 2, 17])
}

fn bk_set_of(indices: &[u16]) -> BlockKeeperSet {
    let mut set = BlockKeeperSet::new();
    for &i in indices {
        let mut owner = [0u8; 32];
        owner[..2].copy_from_slice(&i.to_le_bytes());
        set.insert(
            i,
            BlockKeeperData {
                pubkey: PubKey::from(pubkey(i)),
                epoch_finish_seq_no: None,
                wait_step: 0,
                status: node::block_keeper_system::BlockKeeperStatus::Active,
                address: String::new(),
                stake: Default::default(),
                owner_address: AccountIdentifier::new(owner),
                signer_index: i,
                owner_pubkey: [0; 32],
                protocol_support: ProtocolVersionSupport::from_str("1.0.0").unwrap(),
            },
        );
    }
    set
}

/// `blocks.block_merkle_leaves` with the producer's commitment in the old BK set leaf (L2).
fn leaves_for(set: &BlockKeeperSet) -> Vec<u8> {
    let mut leaves = [[0u8; 32]; node::types::BLOCK_MERKLE_LEAF_COUNT];
    leaves[node::types::BLOCK_MERKLE_OLD_BK_SET_LEAF_INDEX] = set.poseidon_commitment().unwrap();
    leaves.concat()
}

fn update(seq_no: u32, set: &BlockKeeperSet) -> BkSetUpdate {
    BkSetUpdate {
        seq_no,
        thread_id: ThreadIdentifier::default(),
        current: Some(Arc::new(set.clone())),
        future: None,
        added_nodes: Default::default(),
    }
}

fn body(seq_no: u32, set: &BlockKeeperSet) -> Vec<u8> {
    serde_json::to_vec(&http_server::ApiExactBkSet::from(&update(seq_no, set))).unwrap()
}

#[test]
fn node_bk_set_round_trips_through_bm() {
    let set = bk_set();
    let resp = parse_response(&body(1234, &set)).unwrap();
    let validated = validate(resp, None).unwrap();
    assert_eq!(validated.seq_no(), 1234);
    assert_eq!(
        validated.clone().into_parts().2,
        vec![(0, pubkey(0)), (2, pubkey(2)), (5, pubkey(5)), (17, pubkey(17))]
    );
    assert_eq!(check_commitment(&validated, Some(&leaves_for(&set))), Ok(()));

    // A different set does not match the block's commitment.
    let other = bk_set_of(&[5, 0, 2]);
    assert_eq!(
        check_commitment(&validated, Some(&leaves_for(&other))),
        Err(BkSetRejection::CommitmentMismatch)
    );
}

fn wait_until(what: &str, cond: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn snapshot_rows(dir: &std::path::Path) -> Vec<(u16, Vec<u8>, u64, String)> {
    let conn = Connection::open(dir.join("bm-archive.db")).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT signer_index, pubkey, seq_no, block_id FROM latest_bk_set \
             ORDER BY signer_index",
        )
        .unwrap();
    stmt.query_map([], |r| {
        Ok((r.get::<_, i64>(0)? as u16, r.get(1)?, r.get::<_, i64>(2)? as u64, r.get(3)?))
    })
    .unwrap()
    .map(Result::unwrap)
    .collect()
}

#[test]
fn rotation_rewrites_snapshot_on_real_sqlite() -> anyhow::Result<()> {
    let dir = testdir!();
    let maintenance = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &dir);
    maintenance
        .migrate(MigrateTo::Latest, DbMaintenanceOptions { silent: true, ..Default::default() })?;
    std::fs::copy(&maintenance.path, dir.join("bm-archive.db"))?;

    let set = bk_set();
    let thread_id = hex::encode(ThreadIdentifier::default());
    let seq_no = 700u64;
    {
        let conn = Connection::open(dir.join("bm-archive.db"))?;
        conn.execute(
            "INSERT INTO blocks (id, status, seq_no, parent, thread_id, height, chain_order, \
             block_merkle_leaves) VALUES ('anchor-700', 0, ?1, 'parent', ?2, ?3, 'order-700', ?4)",
            params![seq_no as i64, thread_id, seq_no.to_be_bytes().to_vec(), leaves_for(&set)],
        )?;
    }

    let (mut helper, writer) =
        SqliteHelper::from_config(SqliteHelperConfig::new(dir.to_path_buf(), None))?;
    let clone = helper.clone();
    let mut state = PollerState::with_capacity(10, None);

    let now = Instant::now();
    state.on_response(parse_response(&body(seq_no as u32, &set))?, "bk-a:8600", now).unwrap();
    let report = state.resolve_once(&helper, 1000, now)?;
    assert_eq!(report.written, Some(seq_no));
    wait_until("the first write", || clone.latest_bk_set_writes().written_seq_no == seq_no);
    let rows = snapshot_rows(&dir);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], (0, pubkey(0).to_vec(), seq_no, "anchor-700".to_string()));

    // The archive file is replaced by an empty one (>= 2 s): the snapshot must come back.
    helper.rotate_db_file()?;
    assert_eq!(helper.rotation_count(), 1);
    assert!(snapshot_rows(&dir).is_empty());
    let report = state.resolve_once(&helper, 1001, now)?;
    assert_eq!(report.written, None);
    assert_eq!(report.rewritten, Some(seq_no));
    wait_until("the rewrite", || snapshot_rows(&dir).len() == 4);
    assert_eq!(snapshot_rows(&dir)[3], (17, pubkey(17).to_vec(), seq_no, "anchor-700".to_string()));

    // Nothing more to do on the next tick.
    assert_eq!(state.resolve_once(&helper, 1002, now)?.rewritten, None);

    drop(helper);
    drop(clone);
    writer.join().unwrap();
    Ok(())
}

#[test]
fn restart_restores_snapshot_and_rewrites_after_rotation() -> anyhow::Result<()> {
    const ENDPOINT: &str = "bk-a:8600";
    let dir = testdir!();
    let maintenance = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &dir);
    maintenance
        .migrate(MigrateTo::Latest, DbMaintenanceOptions { silent: true, ..Default::default() })?;
    std::fs::copy(&maintenance.path, dir.join("bm-archive.db"))?;

    let set = bk_set();
    let thread_id = hex::encode(ThreadIdentifier::default());
    let seq_no = 800u64;
    {
        let conn = Connection::open(dir.join("bm-archive.db"))?;
        conn.execute(
            "INSERT INTO blocks (id, status, seq_no, parent, thread_id, height, chain_order, \
             block_merkle_leaves) VALUES ('anchor-800', 0, ?1, 'parent', ?2, ?3, 'order-800', ?4)",
            params![seq_no as i64, thread_id, seq_no.to_be_bytes().to_vec(), leaves_for(&set)],
        )?;
    }
    let (helper, writer) =
        SqliteHelper::from_config(SqliteHelperConfig::new(dir.to_path_buf(), None))?;
    let clone = helper.clone();
    let endpoints = vec![ENDPOINT.to_string()];
    let interval = Duration::from_secs(60);
    let fetched = |seq_no: u32| FetchedBkSet {
        idx: 0,
        endpoint: ENDPOINT.to_string(),
        failed_over: false,
        response: parse_response(&body(seq_no, &set)).unwrap(),
    };

    // First run: the snapshot is written through the real writer thread.
    let (mut poller, effects) = Poller::start(&helper, interval);
    assert!(effects.is_empty());
    assert!(poller.on_fetch(Ok(fetched(seq_no as u32)), Instant::now()).is_empty());
    poller.on_tick(&helper, &endpoints, Instant::now());
    wait_until("the first write", || clone.latest_bk_set_writes().written_seq_no == seq_no);
    assert_eq!(snapshot_rows(&dir).len(), 4);
    drop(poller);

    // Restart: the whole helper goes away, the writer thread is joined, and a new helper opens
    // the same directory.
    drop(helper);
    drop(clone);
    writer.join().unwrap();
    let (mut helper, writer) =
        SqliteHelper::from_config(SqliteHelperConfig::new(dir.to_path_buf(), None))?;
    let clone = helper.clone();

    // The stored snapshot is restored and reported at once.
    let (mut poller, effects) = Poller::start(&helper, interval);
    assert_eq!(effects, vec![Effect::Gauge { seq_no, thread_id: thread_id.clone() }]);
    // A lagging BK does not roll the restored snapshot back.
    // Without the restored snapshot the first answer would be queued as a candidate, so the
    // third would be only the second not-newer answer and would not advance (and `last`
    // would be `seq_no - 5`).
    for _ in 0..2 {
        assert!(poller.on_fetch(Ok(fetched(seq_no as u32 - 5)), Instant::now()).is_empty());
    }
    assert_eq!(
        poller.on_fetch(Ok(fetched(seq_no as u32 - 5)), Instant::now()),
        vec![Effect::Advance {
            from: ENDPOINT.to_string(),
            reason: AdvanceReason::NotNewer { got: seq_no - 5, last: seq_no },
        }]
    );

    // The archive file is replaced by an empty one (>= 2 s): the next tick rewrites it.
    helper.rotate_db_file()?;
    assert!(snapshot_rows(&dir).is_empty());
    poller.on_tick(&helper, &endpoints, Instant::now());
    wait_until("the rewrite", || snapshot_rows(&dir).len() == 4);
    assert_eq!(snapshot_rows(&dir)[0], (0, pubkey(0).to_vec(), seq_no, "anchor-800".to_string()));

    drop(poller);
    drop(helper);
    drop(clone);
    writer.join().unwrap();
    Ok(())
}
