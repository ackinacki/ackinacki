// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SendError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;

use parking_lot::Mutex;
use rusqlite::OpenFlags;

use super::history_bk_set::BkSetUpdateWrite;
use super::history_bk_set::MismatchKind;
use super::history_proofs::decode_hex32;
use super::history_proofs::events_of;
use super::history_proofs::BlockWrite;
use super::history_proofs::EventCounts;
use super::history_proofs::EventFilters;
use super::history_proofs::EventKind;
use super::history_proofs::HistoryProofsCounters;
use super::history_proofs::HistoryProofsStore;
use super::history_proofs::StoreResult;
use super::history_proofs::Written;
use super::history_proofs::ATTESTATION_WAIT_BLOCKS;
use super::history_proofs::HISTORY_PROOFS_DB;
use super::history_proofs::THREAD_ZERO_HEX;
use super::latest_bk_set::replace_latest_bk_set;
use super::AnchorLookup;
use super::ArchAccount;
use super::ArchAttestation;
use super::ArchBkSetUpdate;
use super::ArchBlock;
use super::ArchLatestBkSet;
use super::ArchMessage;
use super::ArchTransaction;
use super::FlatTransaction;
use crate::documents_db::DBStoredRecord;
use crate::documents_db::DocumentsDb;

pub const SQLITE_DATA_DIR: &str = "./data";
pub const SQLITE_EMPTY_DB: &str = "bm-schema.db";
pub const SQLITE_NEXT_DB: &str = "bm-archive-next.db";
const BLOCK_MERKLE_LEAF_COUNT: usize = 16;
const BLOCK_MERKLE_HASH_SIZE: usize = 32;
const BLOCK_MERKLE_LEAVES_SIZE: usize = BLOCK_MERKLE_LEAF_COUNT * BLOCK_MERKLE_HASH_SIZE;

fn default_db_file() -> PathBuf {
    "bm-archive.db".into()
}

#[derive(Clone)]
pub struct SqliteHelperConfig {
    pub data_dir: PathBuf,
    pub db_file: PathBuf,
    /// Which ext-out messages of thread-0 blocks go to the history proofs store.
    pub event_filters: EventFilters,
}

impl SqliteHelperConfig {
    pub fn new(data_dir: PathBuf, db_file: Option<PathBuf>) -> Self {
        SqliteHelperConfig {
            data_dir,
            db_file: db_file.unwrap_or_else(default_db_file),
            event_filters: EventFilters::default(),
        }
    }
}

pub struct SqliteHelperContext {
    pub config: SqliteHelperConfig,
    pub conn: Arc<Mutex<Option<rusqlite::Connection>>>,
    pub latest_bk_set_writes: LatestBkSetWrites,
    pub history_proofs: HistoryProofsStore,
    pub history: HistoryWriterState,
}

/// History store bookkeeping of the writer thread.
#[derive(Default)]
pub struct HistoryWriterState {
    pub counters: HistoryProofsCounters,
    /// Stored blocks whose attestation has not arrived yet: height -> block id.
    awaiting_attestation: BTreeMap<u64, [u8; 32]>,
}

/// Outcome of `latest_bk_set` writes in the writer thread, shared by all clones of the helper.
/// A record dropped because the connection is absent counts as neither a write nor a failure.
#[derive(Clone, Default)]
pub struct LatestBkSetWrites {
    written_seq_no: Arc<AtomicU64>,
    failures: Arc<AtomicU64>,
}

/// A reading of `LatestBkSetWrites`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LatestBkSetWriteCounters {
    /// `seq_no` of the last snapshot written successfully; 0 when there is none yet.
    pub written_seq_no: u64,
    pub failures: u64,
}

impl LatestBkSetWrites {
    pub fn get(&self) -> LatestBkSetWriteCounters {
        LatestBkSetWriteCounters {
            written_seq_no: self.written_seq_no.load(Ordering::SeqCst),
            failures: self.failures.load(Ordering::SeqCst),
        }
    }

    fn record(&self, seq_no: u64, ok: bool) {
        if ok {
            self.written_seq_no.store(seq_no, Ordering::SeqCst);
        } else {
            self.failures.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[derive(Clone)]
struct DBFiles {
    empty: PathBuf,
    next: PathBuf,
    work: PathBuf,
}

#[derive(Clone)]
pub struct SqliteHelper {
    record_sender: Sender<DBStoredRecord>,
    pub config: SqliteHelperConfig,
    pub conn: Arc<parking_lot::Mutex<Option<rusqlite::Connection>>>,
    db_files: DBFiles,
    rotations: Arc<AtomicU64>,
    latest_bk_set_writes: LatestBkSetWrites,
    history_counters: HistoryProofsCounters,
}

impl SqliteHelper {
    /// Queues a `latest_bk_set` snapshot for the writer thread. Like every other record, it is
    /// dropped if the connection is absent (during `rotate_db_file`). The outcome of the write
    /// is visible through `latest_bk_set_writes`.
    pub fn put_latest_bk_set(&self, item: ArchLatestBkSet) -> anyhow::Result<()> {
        self.record_sender
            .send(DBStoredRecord::LatestBkSet(Box::new(item)))
            .map_err(|_| anyhow::anyhow!("sqlite writer is gone"))
    }

    /// Looks up the anchor block; an error when the connection is `None`.
    pub fn find_anchor_block(&self, thread_id: &str, seq_no: u64) -> anyhow::Result<AnchorLookup> {
        let guard = self.conn.lock();
        let Some(conn) = guard.as_ref() else {
            anyhow::bail!("no database connection");
        };
        super::latest_bk_set::find_anchor_block(conn, thread_id, seq_no)
    }

    /// The snapshot stored in `latest_bk_set`; `Ok(None)` when the table is empty or the
    /// connection is `None`.
    pub fn load_latest_bk_set(&self) -> anyhow::Result<Option<ArchLatestBkSet>> {
        let guard = self.conn.lock();
        let Some(conn) = guard.as_ref() else {
            return Ok(None);
        };
        super::latest_bk_set::load_latest_bk_set(conn)
    }

    /// Number of completed `rotate_db_file` reconnects, shared by all clones.
    pub fn rotation_count(&self) -> u64 {
        self.rotations.load(Ordering::SeqCst)
    }

    /// Outcome of `latest_bk_set` writes in the writer thread, shared by all clones.
    pub fn latest_bk_set_writes(&self) -> LatestBkSetWriteCounters {
        self.latest_bk_set_writes.get()
    }

    /// Outcomes of the history proofs store writes.
    pub fn history_counters(&self) -> HistoryProofsCounters {
        self.history_counters.clone()
    }

    pub fn reset_connecton(&self) {
        let mut guard = self.conn.lock();
        *guard = None;
    }

    pub fn set_connection(&self, conn: rusqlite::Connection) {
        let mut guard = self.conn.lock();
        *guard = Some(conn);
    }

    pub fn from_config(
        config: SqliteHelperConfig,
    ) -> anyhow::Result<(Self, thread::JoinHandle<()>)> {
        let db_path = config.data_dir.clone().join(config.db_file.clone());

        let (record_sender, record_receiver) = channel::<DBStoredRecord>();
        let conn = Arc::new(Mutex::new(Some(Self::create_connection(db_path.clone())?)));
        let latest_bk_set_writes = LatestBkSetWrites::default();
        let history_proofs = HistoryProofsStore::open(&config.data_dir)?;
        let history = HistoryWriterState::default();
        let history_counters = history.counters.clone();
        let mut context = SqliteHelperContext {
            config: config.clone(),
            conn: conn.clone(),
            latest_bk_set_writes: latest_bk_set_writes.clone(),
            history_proofs,
            history,
        };
        let writer_join_handle = thread::Builder::new()
            .name("sqlite".to_string())
            .spawn(move || Self::put_records_worker(record_receiver, &mut context))?;

        let db_files = DBFiles {
            empty: config.data_dir.join(SQLITE_EMPTY_DB),
            next: config.data_dir.join(SQLITE_NEXT_DB),
            work: config.data_dir.join(&config.db_file),
        };
        Ok((
            SqliteHelper {
                record_sender,
                config,
                conn,
                db_files,
                rotations: Arc::new(AtomicU64::new(0)),
                latest_bk_set_writes,
                history_counters,
            },
            writer_join_handle,
        ))
    }

    fn create_connection(db_path: PathBuf) -> anyhow::Result<rusqlite::Connection> {
        tracing::trace!("create_connection: {db_path:?}");
        let conn = rusqlite::Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| {
            tracing::debug!("Failed to opendb file: {e}");
            anyhow::format_err!("{e}")
        })?;

        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL2;
            PRAGMA synchronous = NORMAL;
            PRAGMA wal_autocheckpoint = 1000;
            PRAGMA wal_checkpoint(TRUNCATE);
            PRAGMA temp_store = MEMORY;
            PRAGMA mmap_size = 30000000000;
            PRAGMA page_size = 4096;
        ",
        )?;

        print_sqlite_info(&conn)?;

        Ok(conn)
    }

    pub fn create_connection_ro(db_path: PathBuf) -> anyhow::Result<rusqlite::Connection> {
        tracing::trace!("create_connection: {db_path:?}");
        let conn = rusqlite::Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        Ok(conn)
    }

    // This function is idempotent and can be repeated as many times as needed.
    pub fn shutdown(&mut self) -> anyhow::Result<()> {
        self.reset_connecton();
        std::thread::sleep(std::time::Duration::from_secs(2));

        let db_path = self.config.data_dir.clone().join(self.config.db_file.clone());

        let new_conn = rusqlite::Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        new_conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))?;
        Ok(())
    }

    // This function is NOT idempotent (because it moves db files), so it contains retries inside
    pub fn rotate_db_file(&mut self) -> anyhow::Result<()> {
        self.reset_connecton();
        std::thread::sleep(std::time::Duration::from_secs(2));

        // prepare an empty DB file with the applied schema
        std::fs::copy(&self.db_files.empty, &self.db_files.next)?;
        let timestamp = chrono::Utc::now().timestamp();
        let archived_path = self.config.data_dir.join(format!("bm-archive-{timestamp}.db"));

        // move DB files (db, wal, shm)
        rename_with_suffixes(&self.db_files.work, &archived_path)?;
        std::fs::rename(&self.db_files.next, &self.db_files.work)?;
        tracing::info!(target: "sqlite", "Database file created");

        self.set_connection(Self::create_connection(self.db_files.work.clone())?);
        self.rotations.fetch_add(1, Ordering::SeqCst);

        // sync wal
        let archive = rusqlite::Connection::open_with_flags(
            archived_path.clone(),
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        // Ten hardcoded retries
        for _ in 0..10 {
            if let Err(err) = archive.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())) {
                tracing::error!(target: "sqlite", "Can't truncate database {err:?}");
                std::thread::sleep(std::time::Duration::from_secs(1));
            } else {
                tracing::info!(target: "sqlite", "Database file rotated to: {:?}", archived_path);
                return Ok(());
            }
        }
        Err(anyhow::anyhow!("Database was not truncated"))
    }

    fn put_records_worker(receiver: Receiver<DBStoredRecord>, context: &mut SqliteHelperContext) {
        for record in receiver {
            let result = match record {
                DBStoredRecord::Block(ref block) => Self::store_block(context, block),
                DBStoredRecord::Transactions(ref transactions) => {
                    Self::store_transactions(context, transactions.to_vec())
                }
                DBStoredRecord::Accounts(ref accounts) => {
                    Self::store_accounts(context, accounts.to_vec())
                }
                DBStoredRecord::Messages(ref messages) => {
                    Self::store_messages(context, messages.to_vec())
                }
                DBStoredRecord::BkSetUpdates(ref updates) => {
                    Self::store_bk_set_updates(context, updates.to_vec())
                }
                DBStoredRecord::Attestations(ref attestations) => {
                    Self::store_attestations(context, attestations.to_vec())
                }
                DBStoredRecord::LatestBkSet(ref item) => Self::store_latest_bk_set(context, item),
            };

            if let Err(err) = result {
                tracing::error!(target: "sqlite", "Error store object(s) into sqlite: {err}");
                tracing::error!(target: "sqlite", "bad object: {:?}", record);

                if let DBStoredRecord::Block(_) = record {
                    panic!("This error is fatal, thread exiting")
                };
            }
        }
        tracing::debug!(target: "sqlite", "receiver dropped");
    }

    fn store_latest_bk_set(
        context: &mut SqliteHelperContext,
        item: &ArchLatestBkSet,
    ) -> anyhow::Result<()> {
        let mut guarded = context.conn.lock();
        let Some(conn) = guarded.as_mut() else {
            return Ok(());
        };
        let result = replace_latest_bk_set(conn, item);
        context.latest_bk_set_writes.record(item.seq_no, result.is_ok());
        result
    }

    fn store_accounts(
        context: &mut SqliteHelperContext,
        accounts: Vec<ArchAccount>,
    ) -> anyhow::Result<()> {
        let cnt_accounts = accounts.len();

        let mut guarded = context.conn.lock();
        let Some(tx) = guarded.as_mut() else {
            return Ok(());
        };
        let tx = tx.transaction()?;

        let now_batched = std::time::Instant::now();
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO accounts (
                id, workchain_id, boc, init_code_hash, last_paid, bits, cells, public_cells,
                last_trans_lt, last_trans_hash, balance, code, code_hash, data, data_hash, acc_type,
                last_trans_chain_order, dapp_id, balance_other
                ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,   ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19
                ) ON CONFLICT(id) DO UPDATE SET
                boc=excluded.boc,
                init_code_hash=excluded.init_code_hash,
                last_paid=excluded.last_paid,
                bits=excluded.bits,
                cells=excluded.cells,
                public_cells=excluded.public_cells,
                last_trans_lt=excluded.last_trans_lt,
                last_trans_hash=excluded.last_trans_hash,
                balance=excluded.balance,
                code=excluded.code,
                code_hash=excluded.code_hash,
                data=excluded.data,
                data_hash=excluded.data_hash,
                acc_type=excluded.acc_type,
                last_trans_chain_order=excluded.last_trans_chain_order,
                dapp_id=excluded.dapp_id,
                balance_other=excluded.balance_other",
            )?;

            for acc in accounts.into_iter() {
                let params = rusqlite::params![
                    acc.id,
                    acc.workchain_id,
                    acc.boc,
                    acc.init_code_hash,
                    acc.last_paid,
                    acc.bits,
                    acc.cells,
                    acc.public_cells,
                    acc.last_trans_lt,
                    acc.last_trans_hash,
                    acc.balance,
                    acc.code,
                    acc.code_hash,
                    acc.data,
                    acc.data_hash,
                    acc.acc_type,
                    acc.last_trans_chain_order,
                    acc.dapp_id,
                    acc.balance_other,
                ];
                if let Err(err) = stmt.execute(params) {
                    tracing::error!("inner_archive_account(): failed to store account: {err}")
                }
            }
        }
        tracing::debug!(target: "sqlite", "TIME: batched {} account(s) {}ms", cnt_accounts, now_batched.elapsed().as_millis());

        let now_committed = std::time::Instant::now();
        tx.commit()?;
        tracing::debug!(target: "sqlite", "TIME: committed {} account(s) {}ms", cnt_accounts, now_committed.elapsed().as_millis());

        Ok(())
    }

    /// Writes the history proofs store row of a block with its bridge events.
    /// Only SQLite errors fail; data problems are logged and counted.
    fn store_history_row(
        context: &mut SqliteHelperContext,
        block: &ArchBlock,
    ) -> anyhow::Result<()> {
        let counters = &context.history.counters;
        let add = |counter: &AtomicU64, n: u64| counter.fetch_add(n, Ordering::Relaxed);
        let height = u64::from_be_bytes(block.height);
        let thread_zero = block.thread_id.as_deref() == Some(THREAD_ZERO_HEX);
        if thread_zero && block.seq_no as u64 != height {
            tracing::error!(
                target: "sqlite",
                "thread-0 block {} has seq_no {} at height {height}",
                block.id,
                block.seq_no
            );
            add(&counters.seq_no_mismatch, 1);
        }
        let (events, skipped) = if thread_zero {
            events_of(block, &context.config.event_filters)
        } else {
            (vec![], EventCounts::default())
        };
        add(&counters.withdrawal_events_skipped, skipped.withdrawal);
        add(&counters.voucher_events_skipped, skipped.voucher);
        match context.history_proofs.store_arch_block(block, &events)? {
            StoreResult::NotThreadZero => {}
            StoreResult::Rejected(err) => {
                tracing::error!(
                    target: "sqlite",
                    "block {} at height {height} is not stored in {HISTORY_PROOFS_DB}: {err}",
                    block.id
                );
                add(&counters.rejected, 1);
            }
            StoreResult::Stored(BlockWrite::Inserted(written)) => {
                Self::count_written(counters, &block.id, height, &written);
                Self::log_skipped_events(counters, &block.id, height, &written);
                add(&counters.rows_written, 1);
                Self::count_events(counters, &written.events_inserted);
                // A stored row has a valid hex id.
                if let Some(block_id) = decode_hex32(&block.id) {
                    context.history.awaiting_attestation.insert(height, block_id);
                }
                let awaiting = &mut context.history.awaiting_attestation;
                while let Some((&oldest, _)) = awaiting.first_key_value() {
                    if oldest + ATTESTATION_WAIT_BLOCKS >= height {
                        break;
                    }
                    awaiting.pop_first();
                    tracing::warn!(
                        target: "sqlite",
                        "no attestation for thread-0 block at height {oldest} within {ATTESTATION_WAIT_BLOCKS} blocks"
                    );
                    add(&counters.attestation_missing, 1);
                }
            }
            StoreResult::Stored(BlockWrite::Duplicate(written)) => {
                Self::count_written(counters, &block.id, height, &written);
                // A block sent again counts all its skipped events again, also
                // the ones its first write counted: after a change of the
                // event flags it can carry new ones, and the alert must not
                // miss them. Its events stored by this write count too.
                Self::log_skipped_events(counters, &block.id, height, &written);
                Self::count_events(counters, &written.events_inserted);
                add(&counters.duplicates, 1);
            }
            StoreResult::Stored(BlockWrite::Conflict { existing_height, existing_block_id }) => {
                tracing::error!(
                    target: "sqlite",
                    "block {} at height {height} conflicts with block {} at height {existing_height} in {HISTORY_PROOFS_DB}; the stored row is kept",
                    block.id,
                    hex::encode(existing_block_id)
                );
                add(&counters.conflicts, 1);
            }
            StoreResult::Stored(BlockWrite::BkSetUpdateConflict { height }) => {
                tracing::error!(
                    target: "sqlite",
                    "block {} at height {height} is not stored in {HISTORY_PROOFS_DB}: another BK set update holds its height or block id",
                    block.id
                );
                add(&counters.conflicts, 1);
            }
            StoreResult::Stored(BlockWrite::BkSetUpdateRejected(reason)) => {
                tracing::error!(
                    target: "sqlite",
                    "block {} at height {height} is not stored in {HISTORY_PROOFS_DB}: {reason}",
                    block.id
                );
                add(&counters.rejected, 1);
            }
        }
        Ok(())
    }

    /// Logs and counts the events of a stored block that are not stored: the
    /// file holds their message at another height. Nothing else records them:
    /// the counter starts from zero when block-manager restarts.
    fn log_skipped_events(
        counters: &HistoryProofsCounters,
        block_id: &str,
        height: u64,
        w: &Written,
    ) {
        for (kind, msg_hash, stored_at) in &w.skipped_events {
            let conflicts = match kind {
                EventKind::Withdrawal => &counters.withdrawal_event_conflicts,
                EventKind::Voucher => &counters.voucher_event_conflicts,
            };
            conflicts.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                target: "sqlite",
                "{} event {} of block {block_id} at height {height} is not stored in {HISTORY_PROOFS_DB}: the event at height {stored_at} holds the message, so the data is damaged or of another chain",
                kind.as_str(),
                hex::encode(msg_hash)
            );
        }
    }

    fn count_events(counters: &HistoryProofsCounters, inserted: &EventCounts) {
        counters.withdrawal_events.fetch_add(inserted.withdrawal, Ordering::Relaxed);
        counters.voucher_events.fetch_add(inserted.voucher, Ordering::Relaxed);
    }

    /// Counts the BK set update written with a stored block, and counts and
    /// logs its attestations. Its skipped events go to `log_skipped_events`.
    fn count_written(counters: &HistoryProofsCounters, block_id: &str, height: u64, w: &Written) {
        let add = |counter: &AtomicU64, n: u64| counter.fetch_add(n, Ordering::Relaxed);
        if w.bk_set_update == Some(BkSetUpdateWrite::Inserted) {
            add(&counters.bk_set_updates_written, 1);
        }
        let attestations = &w.attestations;
        add(&counters.attestations_written, attestations.inserted);
        add(&counters.attestations_replaced, attestations.replaced);
        for (kind, reason) in &attestations.mismatches {
            tracing::error!(
                target: "sqlite",
                "attestation carried in block {block_id} at height {height} is not stored in {HISTORY_PROOFS_DB}: {reason}"
            );
            match kind {
                // Counted by `check_attestations`.
                MismatchKind::EnvelopeHash => {}
                MismatchKind::Parent | MismatchKind::BadBlockId | MismatchKind::OtherBlock => {
                    add(&counters.attestations_rejected, 1);
                }
            }
        }
        for reason in &attestations.detached {
            tracing::error!(
                target: "sqlite",
                "attestation {reason}: deleted from {HISTORY_PROOFS_DB} when block {block_id} at height {height} was stored"
            );
            add(&counters.attestations_rejected, 1);
        }
    }

    /// Compares attestations with the stored rows of the blocks they attest.
    /// A diagnostic: it names the block when an L1 window does not match.
    fn check_attestations(context: &mut SqliteHelperContext, attestations: &[ArchAttestation]) {
        for attestation in attestations {
            let Some(block_id) = decode_hex32(&attestation.block_id) else {
                continue;
            };
            let row = match context.history_proofs.row_by_block_id(&block_id) {
                Ok(Some(row)) => row,
                Ok(None) => continue,
                Err(err) => {
                    tracing::warn!(target: "sqlite", "attestation check skipped: {err}");
                    continue;
                }
            };
            if row.envelope_hash != attestation.envelope_hash {
                tracing::error!(
                    target: "sqlite",
                    "attestation of block {} at height {} has envelope_hash {}, {HISTORY_PROOFS_DB} has {}",
                    attestation.block_id,
                    row.height,
                    hex::encode(attestation.envelope_hash),
                    hex::encode(row.envelope_hash)
                );
                context.history.counters.attestation_mismatch.fetch_add(1, Ordering::Relaxed);
            }
            if context.history.awaiting_attestation.get(&row.height) == Some(&block_id) {
                context.history.awaiting_attestation.remove(&row.height);
            }
        }
    }

    fn store_block(context: &mut SqliteHelperContext, block: &ArchBlock) -> anyhow::Result<()> {
        // Written first and on its own connection: the history proofs store
        // must get the row even while the archive connection is closed for
        // rotation. An error here fails the block, which is fatal.
        Self::store_history_row(context, block)?;

        let mut guarded = context.conn.lock();
        let Some(tx) = guarded.as_mut() else {
            return Ok(());
        };
        let tx = tx.transaction()?;

        let now = std::time::Instant::now();
        {
            let result = if !cfg!(feature = "store_events_only") {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO blocks (
                        id,status,seq_no,parent,aggregated_signature,signature_occurrences,
                        share_state_resource_address,global_id,version,after_merge,before_split,after_split,
                        want_split,want_merge,key_block,flags,shard,workchain_id,gen_utime,gen_utime_ms_part,
                        start_lt,end_lt,gen_validator_list_hash_short,gen_catchain_seqno,min_ref_mc_seqno,
                        prev_key_block_seqno,gen_software_version,gen_software_capabilities,file_hash,
                        root_hash,prev_ref_seq_no,prev_ref_end_lt,prev_ref_file_hash,prev_ref_root_hash,
                        prev_alt_ref_seq_no,prev_alt_ref_end_lt,prev_alt_ref_file_hash,prev_alt_ref_root_hash,
                        in_msgs,out_msgs,data,chain_order,tr_count,thread_id,producer_id,height,envelope_hash,
                        tracked_ext_out_messages_root,tracked_ext_out_message_hashes,block_merkle_leaves,history_proofs,proof_block_refs
                    ) VALUES (
                        ?1,?2,?3,?4,?5,?6,   ?7,?8,?9,   ?10,?11,?12,?13,?14,?15,
                        ?16,?17,?18,?19,?20,?21,?22,   ?23,?24,?25,   ?26,?27,?28,   ?29,?30,
                        ?31,?32,?33,?34,   ?35,?36,?37,?38,   ?39,?40,?41,?42,?43,?44,?45,  ?46,?47,
                        ?48,?49,?50,?51,?52
                    )
                    ON CONFLICT(id) DO UPDATE SET
                        aggregated_signature=excluded.aggregated_signature,
                        signature_occurrences=excluded.signature_occurrences,
                        status=excluded.status"
                )?;

                let prev_ref = block.prev_ref.clone().unwrap_or_default();
                let prev_alt_ref = block.prev_alt_ref.clone().unwrap_or_default();
                let block_data = zstd_compress(&block.data);
                let block_merkle_leaves = (block.block_merkle_leaves.len()
                    == BLOCK_MERKLE_LEAVES_SIZE)
                    .then_some(block.block_merkle_leaves.as_slice());
                let history_proofs =
                    (!block.history_proofs.is_empty()).then_some(block.history_proofs.as_slice());
                let tracked_ext_out_message_hashes =
                    (!block.tracked_ext_out_message_hashes.is_empty())
                        .then_some(block.tracked_ext_out_message_hashes.as_slice());
                let proof_block_refs = (!block.proof_block_refs.is_empty())
                    .then_some(block.proof_block_refs.as_slice());
                let params = rusqlite::params![
                    block.id,
                    block.status,
                    block.seq_no,
                    block.parent,
                    block.aggregated_signature,
                    block.signature_occurrences,
                    block.share_state_resource_address,
                    block.global_id,
                    block.version,
                    block.after_merge,
                    block.before_split,
                    block.after_split,
                    block.want_split,
                    block.want_merge,
                    block.key_block,
                    block.flags,
                    block.shard,
                    block.workchain_id,
                    block.gen_utime,
                    block.gen_utime_ms_part,
                    block.start_lt,
                    block.end_lt,
                    block.gen_validator_list_hash_short,
                    block.gen_catchain_seqno,
                    block.min_ref_mc_seqno,
                    block.prev_key_block_seqno,
                    block.gen_software_version,
                    block.gen_software_capabilities,
                    block.file_hash,
                    block.root_hash,
                    prev_ref.seq_no,
                    prev_ref.end_lt,
                    prev_ref.file_hash,
                    prev_ref.root_hash,
                    prev_alt_ref.seq_no,
                    prev_alt_ref.end_lt,
                    prev_alt_ref.file_hash,
                    prev_alt_ref.root_hash,
                    block.in_msgs,
                    block.out_msgs,
                    block_data,
                    block.chain_order,
                    block.tr_count,
                    block.thread_id,
                    block.producer_id,
                    block.height,
                    block.envelope_hash,
                    block.tracked_ext_out_messages_root,
                    tracked_ext_out_message_hashes,
                    block_merkle_leaves,
                    history_proofs,
                    proof_block_refs,
                ];

                stmt.execute(params)
            } else {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO blocks (
                        id,status,seq_no,parent,producer_id,thread_id,height,gen_utime,chain_order,envelope_hash
                    ) VALUES (
                        ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10
                    )",
                )?;

                let params = rusqlite::params![
                    block.id,
                    block.status,
                    block.seq_no,
                    block.parent,
                    block.producer_id,
                    block.thread_id,
                    block.height,
                    block.gen_utime,
                    block.chain_order,
                    block.envelope_hash,
                ];

                stmt.execute(params)
            };

            if let Err(err) = result {
                tracing::error!("store_block(): failed to store block: {err}")
            }
        }
        tracing::debug!(target: "sqlite", "TIME: batched ({}:{}) block {}ms", block.seq_no, block.id, now.elapsed().as_millis());

        let now_committed = std::time::Instant::now();
        tx.commit()?;
        tracing::debug!(target: "sqlite", "TIME: committed ({}:{}) block {}ms", block.seq_no, block.id, now_committed.elapsed().as_millis());

        Ok(())
    }

    fn store_bk_set_updates(
        context: &mut SqliteHelperContext,
        updates: Vec<ArchBkSetUpdate>,
    ) -> anyhow::Result<()> {
        let cnt_updates = updates.len();

        let mut guarded = context.conn.lock();
        let Some(tx) = guarded.as_mut() else {
            return Ok(());
        };
        let tx = tx.transaction()?;

        let now_batched = std::time::Instant::now();
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO bk_set_updates (
                    block_id, thread_id, height, chain_order, bk_set_update
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5
                ) ON CONFLICT(block_id) DO UPDATE SET
                    thread_id=excluded.thread_id,
                    height=excluded.height,
                    chain_order=excluded.chain_order,
                    bk_set_update=excluded.bk_set_update",
            )?;

            for update in updates.into_iter() {
                let params = rusqlite::params![
                    update.block_id,
                    update.thread_id,
                    update.height,
                    update.chain_order,
                    update.bk_set_update,
                ];
                if let Err(err) = stmt.execute(params) {
                    tracing::error!("store_bk_set_updates(): failed to store bk set update: {err}");
                }
            }
        }
        tracing::debug!(
            target: "sqlite",
            "TIME: batched {} bk_set_update(s) {}ms",
            cnt_updates,
            now_batched.elapsed().as_millis()
        );

        let now_committed = std::time::Instant::now();
        tx.commit()?;
        tracing::debug!(
            target: "sqlite",
            "TIME: committed {} bk_set_update(s) {}ms",
            cnt_updates,
            now_committed.elapsed().as_millis()
        );

        Ok(())
    }

    fn store_attestations(
        context: &mut SqliteHelperContext,
        attestations: Vec<ArchAttestation>,
    ) -> anyhow::Result<()> {
        let cnt_attestations = attestations.len();
        // Before the archive connection check: rotation must not skip it.
        Self::check_attestations(context, &attestations);

        let mut guarded = context.conn.lock();
        let Some(tx) = guarded.as_mut() else {
            return Ok(());
        };
        let tx = tx.transaction()?;

        let now_batched = std::time::Instant::now();
        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO attestations (
                    block_id, parent_block_id, envelope_hash, target_type, aggregated_signature,
                    signature_occurrences, source_block_id, source_chain_order
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8
                ) ON CONFLICT(block_id, target_type) DO UPDATE SET
                    parent_block_id=excluded.parent_block_id,
                    envelope_hash=excluded.envelope_hash,
                    aggregated_signature=excluded.aggregated_signature,
                    signature_occurrences=excluded.signature_occurrences,
                    source_block_id=excluded.source_block_id,
                    source_chain_order=excluded.source_chain_order",
            )?;

            for attestation in attestations.into_iter() {
                let params = rusqlite::params![
                    attestation.block_id,
                    attestation.parent_block_id,
                    attestation.envelope_hash,
                    attestation.target_type,
                    attestation.aggregated_signature,
                    attestation.signature_occurrences,
                    attestation.source_block_id,
                    attestation.source_chain_order,
                ];
                if let Err(err) = stmt.execute(params) {
                    tracing::error!("store_attestations(): failed to store attestation: {err}");
                }
            }
        }
        tracing::debug!(
            target: "sqlite",
            "TIME: batched {} attestation(s) {}ms",
            cnt_attestations,
            now_batched.elapsed().as_millis()
        );

        let now_committed = std::time::Instant::now();
        tx.commit()?;
        tracing::debug!(
            target: "sqlite",
            "TIME: committed {} attestation(s) {}ms",
            cnt_attestations,
            now_committed.elapsed().as_millis()
        );

        Ok(())
    }

    fn store_messages(
        context: &mut SqliteHelperContext,
        messages: Vec<ArchMessage>,
    ) -> anyhow::Result<()> {
        let cnt_messages = messages.len();

        let mut guarded = context.conn.lock();
        let Some(tx) = guarded.as_mut() else {
            return Ok(());
        };
        let tx = tx.transaction()?;

        let now_batched = std::time::Instant::now();
        {
            if !cfg!(feature = "store_events_only") {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO messages (
                    id, boc, status, msg_type, src, src_workchain_id, dst, dst_workchain_id,
                    fwd_fee, bounce, bounced, value, created_lt, created_at,
                    dst_chain_order, src_chain_order, transaction_id, proof, src_dapp_id,
                    code, code_hash, data, data_hash, body, body_hash, value_other, msg_chain_order
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,   ?9, ?10, ?11, ?12, ?13, ?14,   ?15, ?16, ?17, ?18,
                    ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27
                ) ON CONFLICT(id) DO UPDATE SET
                    dst_chain_order=excluded.dst_chain_order",
                )?;

                for msg in messages.into_iter() {
                    let params = rusqlite::params![
                        msg.id,
                        msg.boc,
                        msg.status,
                        msg.msg_type,
                        msg.src,
                        msg.src_workchain_id,
                        msg.dst,
                        msg.dst_workchain_id,
                        msg.fwd_fee,
                        msg.bounce,
                        msg.bounced,
                        msg.value,
                        msg.created_lt,
                        msg.created_at,
                        msg.dst_chain_order,
                        msg.src_chain_order,
                        msg.transaction_id,
                        msg.proof,
                        msg.src_dapp_id,
                        msg.code,
                        msg.code_hash,
                        msg.data,
                        msg.data_hash,
                        msg.body,
                        msg.body_hash,
                        msg.value_other,
                        msg.msg_chain_order,
                    ];
                    if let Err(err) = stmt.execute(params) {
                        tracing::error!("store_messages(): failed to store message: {err}")
                    }
                }
            } else {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO messages (
                    id, boc, body, status, msg_type, src, dst,
                    value, created_lt, created_at,
                    dst_chain_order, src_chain_order, src_dapp_id, value_other,
                    msg_chain_order
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,   ?9, ?10, ?11, ?12, ?13,  ?14, ?15
                ) ON CONFLICT(id) DO UPDATE SET
                    dst_chain_order=excluded.dst_chain_order",
                )?;

                for msg in messages.into_iter() {
                    let params = rusqlite::params![
                        msg.id,
                        msg.boc,
                        msg.body,
                        msg.status,
                        msg.msg_type,
                        msg.src,
                        msg.dst,
                        msg.value,
                        msg.created_lt,
                        msg.created_at,
                        msg.dst_chain_order,
                        msg.src_chain_order,
                        msg.src_dapp_id,
                        msg.value_other,
                        msg.msg_chain_order,
                    ];
                    if let Err(err) = stmt.execute(params) {
                        tracing::error!("store_messages(): failed to store message: {err}")
                    }
                }
            }
        }
        tracing::debug!(target: "sqlite", "TIME: batched {} message(s) {}ms", cnt_messages, now_batched.elapsed().as_millis());

        let now_committed = std::time::Instant::now();
        tx.commit()?;
        tracing::debug!(target: "sqlite", "TIME: committed {} message(s) {}ms", cnt_messages, now_committed.elapsed().as_millis());
        Ok(())
    }

    fn store_transactions(
        context: &mut SqliteHelperContext,
        transactions: Vec<ArchTransaction>,
    ) -> anyhow::Result<()> {
        let cnt_transactions = transactions.len();
        let mut guarded = context.conn.lock();
        let Some(tx) = guarded.as_mut() else {
            return Ok(());
        };
        let tx = tx.transaction()?;

        let now_batched = std::time::Instant::now();
        {
            let mut stmt = tx.prepare_cached("INSERT INTO transactions (
                id, block_id, boc, status, storage_fees_collected, storage_status_change,
                credit, compute_success, compute_msg_state_used, compute_account_activated, compute_gas_fees,
                compute_gas_used, compute_gas_limit, compute_mode, compute_exit_code, compute_vm_steps,
                compute_vm_init_state_hash, compute_vm_final_state_hash, compute_type, action_success,
                action_valid, action_no_funds, action_status_change, action_result_code, action_tot_actions,
                action_spec_actions, action_skipped_actions, action_msgs_created, action_list_hash,
                action_tot_msg_size_cells, action_tot_msg_size_bits, credit_first, aborted, destroyed,
                tr_type, lt, prev_trans_hash, prev_trans_lt, now, outmsg_cnt, orig_status, end_status,
                in_msg, out_msgs, account_addr, workchain_id, total_fees, total_fees_other,
                balance_delta, balance_delta_other, old_hash, new_hash, chain_order
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6,   ?7, ?8, ?9, ?10, ?11,   ?12, ?13, ?14, ?15, ?16,
                ?17, ?18, ?19, ?20,   ?21, ?22, ?23, ?24, ?25,   ?26, ?27, ?28, ?29,
                ?30, ?31, ?32, ?33, ?34,   ?35, ?36, ?37, ?38, ?39, ?40, ?41, ?42,
                ?43, ?44, ?45, ?46, ?47, ?48, ?49,   ?50, ?51, ?52, ?53
            ) ON CONFLICT(id) DO NOTHING")?;

            for trx in
                transactions.into_iter().map(<ArchTransaction as Into<FlatTransaction>>::into)
            {
                let trx_boc = zstd_compress(&trx.boc);
                let params = rusqlite::params![
                    trx.id,
                    trx.block_id,
                    trx_boc,
                    trx.status,
                    trx.storage_fees_collected,
                    trx.storage_status_change,
                    trx.credit,
                    trx.compute_success,
                    trx.compute_msg_state_used,
                    trx.compute_account_activated,
                    trx.compute_gas_fees,
                    trx.compute_gas_used,
                    trx.compute_gas_limit,
                    trx.compute_mode,
                    trx.compute_exit_code,
                    trx.compute_vm_steps,
                    trx.compute_vm_init_state_hash,
                    trx.compute_vm_final_state_hash,
                    trx.compute_type,
                    trx.action_success,
                    trx.action_valid,
                    trx.action_no_funds,
                    trx.action_status_change,
                    trx.action_result_code,
                    trx.action_tot_actions,
                    trx.action_spec_actions,
                    trx.action_skipped_actions,
                    trx.action_msgs_created,
                    trx.action_list_hash,
                    trx.action_tot_msg_size_cells,
                    trx.action_tot_msg_size_bits,
                    trx.credit_first,
                    trx.aborted,
                    trx.destroyed,
                    trx.tr_type,
                    trx.lt,
                    trx.prev_trans_hash,
                    trx.prev_trans_lt,
                    trx.now,
                    trx.outmsg_cnt,
                    trx.orig_status,
                    trx.end_status,
                    trx.in_msg,
                    trx.out_msgs,
                    trx.account_addr,
                    trx.workchain_id,
                    Some(trx.total_fees),
                    trx.total_fees_other,
                    trx.balance_delta,
                    trx.balance_delta_other,
                    trx.old_hash,
                    trx.new_hash,
                    trx.chain_order,
                ];

                if let Err(err) = stmt.execute(params) {
                    tracing::error!("store_transactions(): failed to store transaction: {err}")
                }
            }
        }
        tracing::debug!(target: "sqlite", "TIME: batched {} transaction(s) {}ms", cnt_transactions, now_batched.elapsed().as_millis());

        let now_committed = std::time::Instant::now();
        tracing::debug!(target: "sqlite", "TIME: commiting...");
        match tx.commit() {
            Ok(_) => {
                tracing::debug!(target: "sqlite", "TIME: committed {} transaction(s) {}ms", cnt_transactions, now_committed.elapsed().as_millis())
            }
            Err(e) => tracing::error!("transactions commit error: {e}"),
        }
        tracing::debug!(target: "sqlite", "TIME: commit complete");

        Ok(())
    }
}

impl DocumentsDb for SqliteHelper {
    fn put_block(&self, item: ArchBlock) -> anyhow::Result<()> {
        if let Err(SendError(DBStoredRecord::Block(item))) =
            self.record_sender.send(DBStoredRecord::Block(Box::new(item)))
        {
            tracing::error!(target: "node", "Error sending block {}:", item.id);
        };

        Ok(())
    }

    fn put_accounts(&self, items: Vec<ArchAccount>) -> anyhow::Result<()> {
        if !cfg!(feature = "store_events_only") {
            if let Err(SendError(DBStoredRecord::Accounts(items))) =
                self.record_sender.send(DBStoredRecord::Accounts(items))
            {
                tracing::error!(target: "node", "Error sending accounts {}:", items.len());
            };
        }

        Ok(())
    }

    fn put_messages(&self, items: Vec<ArchMessage>) -> anyhow::Result<()> {
        if let Err(SendError(DBStoredRecord::Messages(items))) =
            self.record_sender.send(DBStoredRecord::Messages(items))
        {
            tracing::error!(target: "node", "Error sending arch_messages {}:", items.len());
        };

        Ok(())
    }

    fn put_transactions(&self, items: Vec<ArchTransaction>) -> anyhow::Result<()> {
        if !cfg!(feature = "store_events_only") {
            if let Err(SendError(DBStoredRecord::Transactions(items))) =
                self.record_sender.send(DBStoredRecord::Transactions(items))
            {
                tracing::error!(target: "node", "Error sending transactions {}:", items.len());
            };
        }

        Ok(())
    }

    fn put_bk_set_updates(&self, items: Vec<ArchBkSetUpdate>) -> anyhow::Result<()> {
        if let Err(SendError(DBStoredRecord::BkSetUpdates(items))) =
            self.record_sender.send(DBStoredRecord::BkSetUpdates(items))
        {
            tracing::error!(target: "node", "Error sending bk set updates {}:", items.len());
        };

        Ok(())
    }

    fn put_attestations(&self, items: Vec<ArchAttestation>) -> anyhow::Result<()> {
        if let Err(SendError(DBStoredRecord::Attestations(items))) =
            self.record_sender.send(DBStoredRecord::Attestations(items))
        {
            tracing::error!(target: "node", "Error sending attestations {}:", items.len());
        };

        Ok(())
    }

    fn has_delivery_problems(&self) -> bool {
        false
    }
}

/// zstd-compress a BLOB before persisting it. Used for `blocks.data` and
/// `transactions.boc`; readers (e.g. `gql-server`) must zstd-decode.
///
/// Compression must never lose a block. If zstd fails for any reason we log
/// the error and fall back to the original bytes so the row still lands in
/// SQLite. Readers tolerate this — the matching `decompress_blob` returns
/// raw bytes when zstd decode fails.
fn zstd_compress(bytes: &[u8]) -> Vec<u8> {
    if bytes.is_empty() {
        return Vec::new();
    }
    match zstd::encode_all(bytes, 3) {
        Ok(compressed) => compressed,
        Err(err) => {
            tracing::error!(
                target: "sqlite",
                "zstd compression failed ({} bytes), storing raw: {err}",
                bytes.len()
            );
            bytes.to_vec()
        }
    }
}

pub fn unprefix_opt_u64str(value: Option<String>) -> Option<String> {
    match value {
        Some(v) => {
            let mut chars = v.chars();
            chars.next();
            Some(chars.as_str().to_string())
        }
        None => None,
    }
}

pub fn unprefix_opt_u128str(value: Option<String>) -> Option<String> {
    match value {
        Some(v) => {
            let mut chars = v.chars();
            chars.next();
            chars.next();
            Some(chars.as_str().to_string())
        }
        None => None,
    }
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.file_name().unwrap().to_os_string();
    name.push(suffix);
    path.with_file_name(name)
}

fn rename_with_suffixes(work: &Path, archived: &Path) -> std::io::Result<()> {
    std::fs::rename(work, archived)?;

    let wal_src = append_suffix(work, "-wal");
    let wal_dst = append_suffix(archived, "-wal");
    if wal_src.exists() {
        std::fs::rename(wal_src, wal_dst)?;
    }

    let wal2_src = append_suffix(work, "-wal2");
    let wal2_dst = append_suffix(archived, "-wal2");
    if wal2_src.exists() {
        std::fs::rename(wal2_src, wal2_dst)?;
    }

    let shm_src = append_suffix(work, "-shm");
    let shm_dst = append_suffix(archived, "-shm");
    if shm_src.exists() {
        std::fs::rename(shm_src, shm_dst)?;
    }

    Ok(())
}

pub fn print_sqlite_info(conn: &rusqlite::Connection) -> anyhow::Result<()> {
    let journal_mode: String = conn.query_row("PRAGMA journal_mode;", [], |row| row.get(0))?;
    println!("journal_mode       = {journal_mode}");

    let synchronous: i32 = conn.query_row("PRAGMA synchronous;", [], |row| row.get(0))?;
    println!("synchronous        = {synchronous} (0=OFF, 1=NORMAL, 2=FULL, 3=EXTRA)");

    let wal_autocheckpoint: i32 =
        conn.query_row("PRAGMA wal_autocheckpoint;", [], |row| row.get(0))?;
    println!("wal_autocheckpoint = {wal_autocheckpoint} pages");

    let page_size: i32 = conn.query_row("PRAGMA page_size;", [], |row| row.get(0))?;
    println!("page_size          = {page_size} bytes");

    let cache_size: i32 = conn.query_row("PRAGMA cache_size;", [], |row| row.get(0))?;
    println!("cache_size         = {cache_size} pages");

    let foreign_keys: i32 = conn.query_row("PRAGMA foreign_keys;", [], |row| row.get(0))?;
    println!("foreign_keys       = {}", if foreign_keys == 1 { "ON" } else { "OFF" });

    let user_version: i32 = conn.query_row("PRAGMA user_version;", [], |row| row.get(0))?;
    println!("user_version       = {user_version}");

    let schema_version: i32 = conn.query_row("PRAGMA schema_version;", [], |row| row.get(0))?;
    println!("schema_version     = {schema_version}");

    let (log_size, frames, checkpointed): (i32, i32, i32) =
        conn.query_row("PRAGMA wal_checkpoint(PASSIVE);", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    println!(
        "WAL checkpoint     = log_size={log_size}, frames={frames}, checkpointed={checkpointed}"
    );

    Ok(())
}

#[cfg(test)]
mod test {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::Instant;

    use migration_tool::DbInfo;
    use migration_tool::DbMaintenance;
    use migration_tool::DbMaintenanceOptions;
    use migration_tool::MigrateTo;
    use parking_lot::Mutex;
    use rusqlite::Connection;
    use rusqlite::OpenFlags;
    use testdir::testdir;

    use super::SqliteHelper;
    use super::SqliteHelperConfig;
    use super::SqliteHelperContext;
    use super::BLOCK_MERKLE_LEAVES_SIZE;
    use crate::documents_db::DocumentsDb;
    use crate::sqlite::block::ExtBlkRef;
    use crate::sqlite::history_bk_set::tests as bk_set_tests;
    use crate::sqlite::history_proofs::tests as history_proofs_tests;
    use crate::sqlite::history_proofs::EventFilter;
    use crate::sqlite::history_proofs::HistoryProofsStore;
    use crate::sqlite::history_proofs::HISTORY_PROOFS_DB;
    use crate::sqlite::sqlite_helper::print_sqlite_info;
    use crate::sqlite::ArchAttestation;
    use crate::sqlite::ArchBlock;
    use crate::sqlite::ArchLatestBkSet;
    use crate::sqlite::ArchMessage;
    use crate::sqlite::ArchTransaction;

    #[test]
    fn test_sqlite_features() -> anyhow::Result<()> {
        let db_path = "./dummy.db";
        let conn = Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;

        let mode: String = conn.query_row("PRAGMA journal_mode = wal2;", [], |row| row.get(0))?;
        assert_eq!(mode, "wal2");

        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS foo (id INTEGER PRIMARY KEY, name TEXT)",
            [],
        )?;

        let expected_name = "Alice";
        conn.execute("BEGIN CONCURRENT", [])?;
        conn.execute("INSERT INTO foo (name) VALUES (?1)", [expected_name])?;
        conn.execute("COMMIT", [])?;

        let mut stmt = conn.prepare("SELECT id, name FROM foo")?;
        let (id, name) =
            stmt.query_one([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))?;
        assert_eq!((id, name), (1, expected_name.to_string()));

        print_sqlite_info(&conn)?;

        Ok(())
    }

    #[test]
    fn store_block_insert_matches_columns_and_params() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let db_maintenance = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &db_dir);
        db_maintenance.migrate(
            MigrateTo::Latest,
            DbMaintenanceOptions { silent: true, ..Default::default() },
        )?;
        let conn = Connection::open(&db_maintenance.path)?;

        let conn = Arc::new(Mutex::new(Some(conn)));
        let mut context = SqliteHelperContext {
            config: SqliteHelperConfig::new(PathBuf::new(), None),
            conn: conn.clone(),
            latest_bk_set_writes: Default::default(),
            history_proofs: HistoryProofsStore::open(&db_dir)?,
            history: Default::default(),
        };

        let block_merkle_leaves = vec![0x50; BLOCK_MERKLE_LEAVES_SIZE];
        let mut history_proofs = vec![0x01, 0x07];
        history_proofs.extend_from_slice(&[0x62; 32]);
        let proof_block_refs = vec![0x00, 0x00, 0x00, 0x01, 0x77];
        let tracked_ext_out_messages_root = [0x31; 32];
        let tracked_ext_out_message_hashes = vec![0x41, 0x42, 0x43];
        let envelope_hash = [0x21; 32];
        let height = 123_u64.to_be_bytes();

        let block = ArchBlock {
            id: "block-1".to_string(),
            status: 2,
            seq_no: 42,
            parent: "parent-block".to_string(),
            file_hash: Some("file-hash".to_string()),
            root_hash: Some("root-hash".to_string()),
            prev_ref: Some(ExtBlkRef {
                seq_no: Some(41),
                end_lt: Some("100".to_string()),
                file_hash: Some("prev-file".to_string()),
                root_hash: Some("prev-root".to_string()),
            }),
            prev_alt_ref: Some(ExtBlkRef {
                seq_no: Some(40),
                end_lt: Some("90".to_string()),
                file_hash: Some("prev-alt-file".to_string()),
                root_hash: Some("prev-alt-root".to_string()),
            }),
            data: vec![0x01, 0x02],
            chain_order: Some("chain-order".to_string()),
            tr_count: Some(7),
            thread_id: Some("thread-id".to_string()),
            producer_id: Some("producer-id".to_string()),
            height,
            envelope_hash,
            tracked_ext_out_messages_root,
            tracked_ext_out_message_hashes: tracked_ext_out_message_hashes.clone(),
            block_merkle_leaves: block_merkle_leaves.clone(),
            history_proofs: history_proofs.clone(),
            proof_block_refs: proof_block_refs.clone(),
            ..Default::default()
        };

        SqliteHelper::store_block(&mut context, &block)?;

        let guard = conn.lock();
        let db = guard.as_ref().expect("connection is present");
        let row = db.query_row(
            "SELECT id, status, seq_no, parent, prev_ref_seq_no, prev_ref_file_hash,
                    chain_order, tr_count, thread_id, producer_id, height, envelope_hash,
                    tracked_ext_out_messages_root, tracked_ext_out_message_hashes,
                    block_merkle_leaves, history_proofs, proof_block_refs
             FROM blocks WHERE id = 'block-1'",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, u8>(1)?,
                    row.get::<_, u32>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, Vec<u8>>(10)?,
                    row.get::<_, Vec<u8>>(11)?,
                    row.get::<_, Vec<u8>>(12)?,
                    row.get::<_, Vec<u8>>(13)?,
                    row.get::<_, Vec<u8>>(14)?,
                    row.get::<_, Vec<u8>>(15)?,
                    row.get::<_, Vec<u8>>(16)?,
                ))
            },
        )?;

        assert_eq!(row.0, "block-1");
        assert_eq!(row.1, 2);
        assert_eq!(row.2, 42);
        assert_eq!(row.3, "parent-block");
        assert_eq!(row.4, 41);
        assert_eq!(row.5, "prev-file");
        assert_eq!(row.6, "chain-order");
        assert_eq!(row.7, 7);
        assert_eq!(row.8, "thread-id");
        assert_eq!(row.9, "producer-id");
        assert_eq!(row.10, height);
        assert_eq!(row.11, envelope_hash);
        assert_eq!(row.12, tracked_ext_out_messages_root);
        assert_eq!(row.13, tracked_ext_out_message_hashes);
        assert_eq!(row.14, block_merkle_leaves);
        assert_eq!(row.15, history_proofs);
        assert_eq!(row.16, proof_block_refs);

        Ok(())
    }

    #[test]
    fn store_transactions_insert_matches_columns_and_params() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let db_maintenance = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &db_dir);
        db_maintenance.migrate(
            MigrateTo::Latest,
            DbMaintenanceOptions { silent: true, ..Default::default() },
        )?;
        let conn = Arc::new(Mutex::new(Some(Connection::open(&db_maintenance.path)?)));
        let mut context = SqliteHelperContext {
            config: SqliteHelperConfig::new(PathBuf::new(), None),
            conn: conn.clone(),
            latest_bk_set_writes: Default::default(),
            history_proofs: HistoryProofsStore::open(&db_dir)?,
            history: Default::default(),
        };

        let balance_delta_other = r#"[{"currency":1,"value":"-300"}]"#.to_string();
        let transaction = ArchTransaction {
            id: "trx-1".to_string(),
            account_addr: "0:aa".to_string(),
            total_fees: "2a".to_string(),
            balance_delta: "-1000".to_string(),
            balance_delta_other: Some(balance_delta_other.clone()),
            // A transaction that collected no extra currency fees stores NULL.
            total_fees_other: None,
            ..Default::default()
        };

        SqliteHelper::store_transactions(&mut context, vec![transaction])?;

        let guard = conn.lock();
        let db = guard.as_ref().expect("connection is present");
        let row = db.query_row(
            "SELECT account_addr, total_fees, total_fees_other, balance_delta, balance_delta_other
             FROM transactions WHERE id = 'trx-1'",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )?;

        assert_eq!(row.0, "0:aa");
        assert_eq!(row.1, "2a");
        assert_eq!(row.2, None);
        assert_eq!(row.3, "-1000");
        assert_eq!(row.4, Some(balance_delta_other));

        Ok(())
    }

    #[test]
    fn dropped_latest_bk_set_counts_as_neither_write_nor_failure() -> anyhow::Result<()> {
        let mut context = SqliteHelperContext {
            config: SqliteHelperConfig::new(PathBuf::new(), None),
            conn: Arc::new(Mutex::new(None)),
            latest_bk_set_writes: Default::default(),
            history_proofs: HistoryProofsStore::open(&testdir!())?,
            history: Default::default(),
        };
        let item = ArchLatestBkSet {
            thread_id: "thread-0".to_string(),
            seq_no: 10,
            block_id: "block-10".to_string(),
            height: [0; 8],
            chain_order: "order-10".to_string(),
            updated_at: 1,
            signers: vec![(0, [1; 48])],
        };
        SqliteHelper::store_latest_bk_set(&mut context, &item)?;
        assert_eq!(context.latest_bk_set_writes.get(), Default::default());
        Ok(())
    }

    /// A thread-0 block; layer boundaries get the layer roots their height needs.
    fn thread_zero_block(height: u64) -> ArchBlock {
        let layers = proof_data::layers::required_layers(height);
        let mut block =
            history_proofs_tests::arch_block(height, history_proofs_tests::proofs(layers));
        block.status = 2;
        block.parent = "parent-block".to_string();
        block
    }

    fn block_id(height: u64) -> Vec<u8> {
        history_proofs_tests::id(height).to_vec()
    }

    fn history_block_ids(db_dir: &Path) -> anyhow::Result<Vec<Vec<u8>>> {
        let conn = Connection::open(db_dir.join(HISTORY_PROOFS_DB))?;
        let mut stmt = conn.prepare("SELECT block_id FROM blocks ORDER BY height")?;
        let mut ids = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            ids.push(row.get(0)?);
        }
        Ok(ids)
    }

    fn wait_for_history_rows(db_dir: &Path, expected: usize) -> anyhow::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if history_block_ids(db_dir)?.len() >= expected {
                return Ok(());
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "history proofs store did not reach {expected} rows"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    #[test]
    fn store_block_writes_the_history_proofs_row() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let db_maintenance = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &db_dir);
        db_maintenance.migrate(
            MigrateTo::Latest,
            DbMaintenanceOptions { silent: true, ..Default::default() },
        )?;
        let conn = Arc::new(Mutex::new(Some(Connection::open(&db_maintenance.path)?)));
        let mut context = SqliteHelperContext {
            config: SqliteHelperConfig::new(PathBuf::new(), None),
            conn: conn.clone(),
            latest_bk_set_writes: Default::default(),
            history_proofs: HistoryProofsStore::open(&db_dir)?,
            history: Default::default(),
        };

        SqliteHelper::store_block(&mut context, &thread_zero_block(127))?;
        SqliteHelper::store_block(&mut context, &thread_zero_block(128))?;

        // Every thread-0 block gets a row, not only layer boundaries.
        assert_eq!(history_block_ids(&db_dir)?, vec![block_id(127), block_id(128)]);
        let archived: i64 = conn.lock().as_ref().expect("connection is present").query_row(
            "SELECT COUNT(*) FROM blocks WHERE id = ?1",
            [hex::encode(block_id(128))],
            |row| row.get(0),
        )?;
        assert_eq!(archived, 1);
        Ok(())
    }

    #[test]
    fn store_block_writes_history_proofs_while_the_archive_connection_is_closed(
    ) -> anyhow::Result<()> {
        let db_dir = testdir!();
        // `rotate_db_file` drops the archive connection for a moment. Blocks
        // that arrive meanwhile are skipped by the archive but must still
        // reach the history proofs store.
        let mut context = SqliteHelperContext {
            config: SqliteHelperConfig::new(PathBuf::new(), None),
            conn: Arc::new(Mutex::new(None)),
            latest_bk_set_writes: Default::default(),
            history_proofs: HistoryProofsStore::open(&db_dir)?,
            history: Default::default(),
        };

        SqliteHelper::store_block(&mut context, &thread_zero_block(128))?;

        assert_eq!(history_block_ids(&db_dir)?, vec![block_id(128)]);
        Ok(())
    }

    #[test]
    fn store_block_fails_when_the_history_proofs_store_stays_locked() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = SqliteHelperContext {
            config: SqliteHelperConfig::new(PathBuf::new(), None),
            conn: Arc::new(Mutex::new(None)),
            latest_bk_set_writes: Default::default(),
            history_proofs: HistoryProofsStore::open_with_busy_timeout(
                &db_dir,
                Duration::from_millis(100),
            )?,
            history: Default::default(),
        };
        let locker = Connection::open(db_dir.join(HISTORY_PROOFS_DB))?;
        locker.execute_batch("BEGIN IMMEDIATE")?;

        let result = SqliteHelper::store_block(&mut context, &thread_zero_block(128));

        locker.execute_batch("ROLLBACK")?;
        // put_records_worker logs the error with `{err}`: that line alone has
        // to name the file, or the operator goes looking at bm-archive.db.
        let message =
            result.expect_err("a write that cannot get the lock must fail the block").to_string();
        assert!(message.contains("locked"), "unexpected error: {message}");
        assert!(message.contains(HISTORY_PROOFS_DB), "the error does not name the file: {message}");
        Ok(())
    }

    fn history_context(db_dir: &Path) -> anyhow::Result<SqliteHelperContext> {
        Ok(SqliteHelperContext {
            config: SqliteHelperConfig::new(PathBuf::new(), None),
            conn: Arc::new(Mutex::new(None)),
            latest_bk_set_writes: Default::default(),
            history_proofs: HistoryProofsStore::open(db_dir)?,
            history: Default::default(),
        })
    }

    fn bridge_event_message(seed: u8) -> ArchMessage {
        let filter = EventFilter::withdrawal_default();
        history_proofs_tests::ext_out(&filter.src(), &filter.dst(), seed)
    }

    fn attestation(height: u64, envelope_hash: [u8; 32]) -> ArchAttestation {
        ArchAttestation {
            block_id: hex::encode(history_proofs_tests::id(height)),
            envelope_hash,
            ..Default::default()
        }
    }

    /// A message comes in one block only. The block is written, the event is
    /// not stored a second time, and it counts as a bridge event conflict,
    /// also when the block is sent again: missing one is worse for the alert
    /// than counting one twice.
    #[test]
    fn store_block_counts_an_event_stored_at_another_height() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        let mut first = thread_zero_block(10);
        first.ext_out_messages = vec![bridge_event_message(1)];
        SqliteHelper::store_block(&mut context, &first)?;
        let mut second = thread_zero_block(11);
        second.ext_out_messages = vec![bridge_event_message(1), bridge_event_message(2)];
        SqliteHelper::store_block(&mut context, &second)?;

        assert_eq!(history_block_ids(&db_dir)?, vec![block_id(10), block_id(11)]);
        assert_eq!(
            context.history_proofs.events_in_range(0, 20)?,
            vec![history_proofs_tests::event(10, 1), history_proofs_tests::event(11, 2)]
        );
        let counters = context.history.counters.snapshot();
        assert_eq!((counters.rows_written, counters.withdrawal_events), (2, 2));
        assert_eq!((counters.withdrawal_event_conflicts, counters.conflicts), (1, 0));

        SqliteHelper::store_block(&mut context, &second)?;
        let counters = context.history.counters.snapshot();
        assert_eq!((counters.duplicates, counters.withdrawal_event_conflicts), (1, 2));
        assert_eq!(
            context.history_proofs.events_in_range(0, 20)?,
            vec![history_proofs_tests::event(10, 1), history_proofs_tests::event(11, 2)]
        );
        Ok(())
    }

    /// A block sent again with an event it did not carry before (after a
    /// change of the event flags) stores the event, and it counts.
    #[test]
    fn store_block_counts_events_added_to_a_stored_row() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        let mut block = thread_zero_block(10);
        block.ext_out_messages = vec![bridge_event_message(1)];
        SqliteHelper::store_block(&mut context, &block)?;
        block.ext_out_messages = vec![bridge_event_message(1), bridge_event_message(2)];
        SqliteHelper::store_block(&mut context, &block)?;
        SqliteHelper::store_block(&mut context, &block)?;

        assert_eq!(
            context.history_proofs.events_in_range(10, 10)?,
            vec![history_proofs_tests::event(10, 1), history_proofs_tests::event(10, 2)]
        );
        let counters = context.history.counters.snapshot();
        assert_eq!((counters.rows_written, counters.duplicates), (1, 2));
        assert_eq!(counters.withdrawal_events, 2);
        Ok(())
    }

    #[test]
    fn store_block_writes_history_row_and_events() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        let mut block = thread_zero_block(10);
        block.ext_out_messages = vec![bridge_event_message(1)];
        SqliteHelper::store_block(&mut context, &block)?;

        assert_eq!(history_block_ids(&db_dir)?, vec![block_id(10)]);
        assert_eq!(
            context.history_proofs.events_in_range(10, 10)?,
            vec![history_proofs_tests::event(10, 1)]
        );
        let counters = context.history.counters.snapshot();
        assert_eq!(counters.rows_written, 1);
        assert_eq!(counters.withdrawal_events, 1);
        Ok(())
    }

    #[test]
    fn event_without_transaction_id_is_skipped_not_rejected() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        let mut block = thread_zero_block(10);
        let mut event = bridge_event_message(1);
        event.transaction_id = None;
        block.ext_out_messages = vec![event];
        SqliteHelper::store_block(&mut context, &block)?;

        assert_eq!(history_block_ids(&db_dir)?, vec![block_id(10)]);
        let counters = context.history.counters.snapshot();
        assert_eq!(counters.withdrawal_events_skipped, 1);
        assert_eq!(counters.withdrawal_events, 0);
        assert_eq!(counters.rejected, 0);
        Ok(())
    }

    /// Vouchers count apart from withdrawals: stored, skipped for a body that
    /// is not `VoucherGenerated`, and not stored a second time at another height.
    #[test]
    fn store_block_counts_events_per_kind() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        let mut first = thread_zero_block(10);
        first.ext_out_messages = vec![
            bridge_event_message(1),
            history_proofs_tests::archived_voucher(
                history_proofs_tests::voucher_boc([0x5c; 32], 2),
                2,
            ),
            history_proofs_tests::archived_voucher(
                history_proofs_tests::voucher_boc_with_body(
                    Some(history_proofs_tests::voucher_body(0x1234_5678, [0x5c; 32])),
                    true,
                    3,
                ),
                3,
            ),
        ];
        SqliteHelper::store_block(&mut context, &first)?;
        let mut second = thread_zero_block(11);
        second.ext_out_messages = vec![history_proofs_tests::archived_voucher(
            history_proofs_tests::voucher_boc([0x5c; 32], 2),
            2,
        )];
        SqliteHelper::store_block(&mut context, &second)?;

        assert_eq!(
            context.history_proofs.events_in_range(0, 20)?,
            vec![history_proofs_tests::event(10, 1), history_proofs_tests::voucher(10, 2)]
        );
        let c = context.history.counters.snapshot();
        assert_eq!((c.withdrawal_events, c.voucher_events), (1, 1));
        assert_eq!((c.withdrawal_events_skipped, c.voucher_events_skipped), (0, 1));
        assert_eq!((c.withdrawal_event_conflicts, c.voucher_event_conflicts), (0, 1));
        Ok(())
    }

    fn table_rows(db_dir: &Path, table: &str) -> anyhow::Result<i64> {
        let conn = Connection::open(db_dir.join(HISTORY_PROOFS_DB))?;
        Ok(conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?)
    }

    /// Thread-0 rotation block: leaves (from `arch_block`) and its BK set update.
    fn rotation_block(height: u64) -> ArchBlock {
        let mut block = thread_zero_block(height);
        block.bk_set_update = Some(bk_set_tests::update(height));
        block
    }

    #[test]
    fn store_block_writes_bk_set_update_and_counts() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        SqliteHelper::store_block(&mut context, &thread_zero_block(1999))?;
        SqliteHelper::store_block(&mut context, &rotation_block(2000))?;
        let mut carrier = thread_zero_block(2001);
        carrier.attestations = vec![bk_set_tests::attestation(2000, 0, 2001, "00002001")];
        SqliteHelper::store_block(&mut context, &carrier)?;

        let counters = context.history.counters.snapshot();
        assert_eq!(counters.bk_set_updates_written, 1);
        assert_eq!(counters.attestations_written, 1);
        assert_eq!(counters.attestations_rejected, 0);
        assert_eq!(table_rows(&db_dir, "bk_set_updates")?, 1);
        assert_eq!(table_rows(&db_dir, "attestations")?, 1);

        let mut wrong_parent = bk_set_tests::attestation(2000, 1, 2002, "00002002");
        wrong_parent.parent_block_id = hex::encode(history_proofs_tests::id(9999));
        let mut carrier = thread_zero_block(2002);
        carrier.attestations = vec![wrong_parent];
        SqliteHelper::store_block(&mut context, &carrier)?;
        assert_eq!(context.history.counters.snapshot().attestations_rejected, 1);

        // Counted by `bm_history_attestation_mismatch` when the archive gets it.
        let mut wrong_envelope = bk_set_tests::attestation(2000, 1, 2003, "00002003");
        wrong_envelope.envelope_hash = [0xee; 32];
        let mut carrier = thread_zero_block(2003);
        carrier.attestations = vec![wrong_envelope];
        SqliteHelper::store_block(&mut context, &carrier)?;
        let counters = context.history.counters.snapshot();
        assert_eq!(counters.attestations_rejected, 1);
        assert_eq!(counters.attestations_written, 1);
        assert_eq!(table_rows(&db_dir, "attestations")?, 1);
        Ok(())
    }

    #[test]
    fn resent_carrier_moves_no_attestation_counter() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        SqliteHelper::store_block(&mut context, &thread_zero_block(1999))?;
        SqliteHelper::store_block(&mut context, &rotation_block(2000))?;
        let mut carrier = thread_zero_block(2001);
        carrier.attestations = vec![bk_set_tests::attestation(2000, 0, 2001, "00002001")];
        SqliteHelper::store_block(&mut context, &carrier)?;
        let before = context.history.counters.snapshot();

        // The node sends the block again after a block-manager restart.
        SqliteHelper::store_block(&mut context, &carrier)?;
        let after = context.history.counters.snapshot();
        assert_eq!(after.duplicates, before.duplicates + 1);
        assert_eq!(after.attestations_written, before.attestations_written);
        assert_eq!(after.attestations_replaced, before.attestations_replaced);
        assert_eq!(after.attestations_rejected, before.attestations_rejected);
        assert_eq!(table_rows(&db_dir, "attestations")?, 1);
        Ok(())
    }

    #[test]
    fn store_block_writes_bk_set_update_while_archive_connection_is_closed() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = SqliteHelperContext {
            config: SqliteHelperConfig::new(PathBuf::new(), None),
            conn: Arc::new(Mutex::new(None)),
            latest_bk_set_writes: Default::default(),
            history_proofs: HistoryProofsStore::open(&db_dir)?,
            history: Default::default(),
        };

        SqliteHelper::store_block(&mut context, &rotation_block(2000))?;

        assert_eq!(history_block_ids(&db_dir)?, vec![block_id(2000)]);
        assert_eq!(table_rows(&db_dir, "bk_set_updates")?, 1);
        assert_eq!(context.history.counters.snapshot().bk_set_updates_written, 1);
        Ok(())
    }

    #[test]
    fn rejected_rotation_block_counts_as_rejected() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        let mut block = rotation_block(2000);
        block.bk_set_update.as_mut().expect("rotation block").block_id =
            hex::encode(history_proofs_tests::id(2001));
        SqliteHelper::store_block(&mut context, &block)?;

        let counters = context.history.counters.snapshot();
        assert_eq!(counters.rejected, 1);
        assert_eq!(counters.rows_written, 0);
        assert_eq!(counters.bk_set_updates_written, 0);
        assert!(!context.history.awaiting_attestation.contains_key(&2000));
        assert!(history_block_ids(&db_dir)?.is_empty());
        assert_eq!(table_rows(&db_dir, "bk_set_updates")?, 0);
        Ok(())
    }

    #[test]
    fn store_block_counts_conflict_and_keeps_row() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        SqliteHelper::store_block(&mut context, &thread_zero_block(10))?;
        SqliteHelper::store_block(&mut context, &thread_zero_block(10))?;
        let mut other = thread_zero_block(10);
        other.envelope_hash = [0xee; 32];
        SqliteHelper::store_block(&mut context, &other)?;

        let counters = context.history.counters.snapshot();
        assert_eq!((counters.rows_written, counters.duplicates, counters.conflicts), (1, 1, 1));
        assert_eq!(context.history_proofs.row(10)?.map(|row| row.envelope_hash), Some([0xe0; 32]));
        Ok(())
    }

    #[test]
    fn store_block_counts_seq_no_mismatch_and_writes() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        let mut block = thread_zero_block(10);
        block.seq_no = 11;
        SqliteHelper::store_block(&mut context, &block)?;
        // Other threads have their own seq_no.
        let mut other_thread = thread_zero_block(12);
        other_thread.thread_id = Some("01".repeat(34));
        other_thread.seq_no = 1;
        SqliteHelper::store_block(&mut context, &other_thread)?;

        let counters = context.history.counters.snapshot();
        assert_eq!(counters.seq_no_mismatch, 1);
        assert_eq!(counters.rows_written, 1);
        assert_eq!(history_block_ids(&db_dir)?, vec![block_id(10)]);
        Ok(())
    }

    #[test]
    fn store_block_rejects_layer_mismatch() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        let mut block = thread_zero_block(128);
        block.history_proofs = vec![0];
        SqliteHelper::store_block(&mut context, &block)?;

        assert_eq!(context.history.counters.snapshot().rejected, 1);
        assert!(history_block_ids(&db_dir)?.is_empty());
        Ok(())
    }

    #[test]
    fn attestation_mismatch_is_counted() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        SqliteHelper::store_block(&mut context, &thread_zero_block(10))?;
        SqliteHelper::store_block(&mut context, &thread_zero_block(11))?;
        SqliteHelper::store_attestations(
            &mut context,
            vec![
                attestation(10, [0xe0; 32]),
                attestation(11, [0xee; 32]),
                attestation(99, [0; 32]),
            ],
        )?;

        let counters = context.history.counters.snapshot();
        assert_eq!(counters.attestation_mismatch, 1);
        assert_eq!(counters.attestation_missing, 0);
        Ok(())
    }

    #[test]
    fn attestation_missing_after_wait() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        for height in 1..=258 {
            SqliteHelper::store_block(&mut context, &thread_zero_block(height))?;
        }
        assert_eq!(context.history.counters.snapshot().attestation_missing, 1);
        Ok(())
    }

    #[test]
    fn attestation_after_window_boundary_is_matched() -> anyhow::Result<()> {
        let db_dir = testdir!();
        let mut context = history_context(&db_dir)?;
        SqliteHelper::store_block(&mut context, &thread_zero_block(255))?;
        SqliteHelper::store_block(&mut context, &thread_zero_block(256))?;
        SqliteHelper::store_attestations(&mut context, vec![attestation(255, [0xe0; 32])])?;
        for height in 257..=600 {
            SqliteHelper::store_block(&mut context, &thread_zero_block(height))?;
            SqliteHelper::store_attestations(
                &mut context,
                vec![attestation(height - 1, [0xe0; 32])],
            )?;
        }

        let counters = context.history.counters.snapshot();
        assert_eq!(counters.attestation_missing, 0);
        assert_eq!(counters.attestation_mismatch, 0);
        Ok(())
    }

    #[test]
    fn history_proofs_rows_survive_rotation() -> anyhow::Result<()> {
        let db_dir = testdir!();
        // `bm-schema.db` is the empty schema that rotation copies into place.
        let db_maintenance = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &db_dir);
        db_maintenance.migrate(
            MigrateTo::Latest,
            DbMaintenanceOptions { silent: true, ..Default::default() },
        )?;
        std::fs::copy(&db_maintenance.path, db_dir.join("bm-archive.db"))?;

        let (mut helper, writer) =
            SqliteHelper::from_config(SqliteHelperConfig::new(db_dir.clone(), None))?;
        helper.put_block(thread_zero_block(128))?;
        wait_for_history_rows(&db_dir, 1)?;

        helper.rotate_db_file()?;
        helper.put_block(thread_zero_block(256))?;
        wait_for_history_rows(&db_dir, 2)?;

        drop(helper);
        writer.join().expect("writer thread");

        assert_eq!(history_block_ids(&db_dir)?, vec![block_id(128), block_id(256)]);
        // Reopening keeps the rows.
        let (helper, writer) =
            SqliteHelper::from_config(SqliteHelperConfig::new(db_dir.clone(), None))?;
        drop(helper);
        writer.join().expect("writer thread");
        assert_eq!(history_block_ids(&db_dir)?, vec![block_id(128), block_id(256)]);
        let rotated = std::fs::read_dir(&db_dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                name.starts_with("bm-archive-")
                    && name.ends_with(".db")
                    && name != "bm-archive-next.db"
            })
            .count();
        assert_eq!(rotated, 1, "rotation must leave the history proofs store alone");
        Ok(())
    }
}
