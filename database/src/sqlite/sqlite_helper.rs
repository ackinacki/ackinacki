// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::SendError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;

use parking_lot::Mutex;
use rusqlite::OpenFlags;

use super::ArchAccount;
use super::ArchBlock;
use super::ArchMessage;
use super::ArchTransaction;
use super::FlatTransaction;
use crate::documents_db::DBStoredRecord;
use crate::documents_db::DocumentsDb;

pub const SQLITE_DATA_DIR: &str = "./data";
pub const SQLITE_EMPTY_DB: &str = "bm-schema.db";
pub const SQLITE_NEXT_DB: &str = "bm-archive-next.db";

fn default_db_file() -> PathBuf {
    "bm-archive.db".into()
}

#[derive(Clone)]
pub struct SqliteHelperConfig {
    pub data_dir: PathBuf,
    pub db_file: PathBuf,
}

impl SqliteHelperConfig {
    pub fn new(data_dir: PathBuf, db_file: Option<PathBuf>) -> Self {
        SqliteHelperConfig { data_dir, db_file: db_file.unwrap_or_else(default_db_file) }
    }
}

pub struct SqliteHelperContext {
    pub config: SqliteHelperConfig,
    pub conn: Arc<Mutex<Option<rusqlite::Connection>>>,
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
}

impl SqliteHelper {
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
        let mut context = SqliteHelperContext { config: config.clone(), conn: conn.clone() };
        let writer_join_handle = thread::Builder::new()
            .name("sqlite".to_string())
            .spawn(move || Self::put_records_worker(record_receiver, &mut context))?;

        let db_files = DBFiles {
            empty: config.data_dir.join(SQLITE_EMPTY_DB),
            next: config.data_dir.join(SQLITE_NEXT_DB),
            work: config.data_dir.join(&config.db_file),
        };
        Ok((SqliteHelper { record_sender, config, conn, db_files }, writer_join_handle))
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
                DBStoredRecord::Block(ref block) => Self::store_block(context, block.clone()),
                DBStoredRecord::Transactions(ref transactions) => {
                    Self::store_transactions(context, transactions.to_vec())
                }
                DBStoredRecord::Accounts(ref accounts) => {
                    Self::store_accounts(context, accounts.to_vec())
                }
                DBStoredRecord::Messages(ref messages) => {
                    Self::store_messages(context, messages.to_vec())
                }
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

    fn store_block(context: &mut SqliteHelperContext, block: Box<ArchBlock>) -> anyhow::Result<()> {
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
                        prev_key_block_seqno,gen_software_version,gen_software_capabilities,boc,file_hash,
                        root_hash,prev_ref_seq_no,prev_ref_end_lt,prev_ref_file_hash,prev_ref_root_hash,
                        prev_alt_ref_seq_no,prev_alt_ref_end_lt,prev_alt_ref_file_hash,prev_alt_ref_root_hash,
                        in_msgs,out_msgs,data,chain_order,tr_count,thread_id,producer_id
                    ) VALUES (
                        ?1,?2,?3,?4,?5,?6,   ?7,?8,?9,   ?10,?11,?12,?13,?14,?15,
                        ?16,?17,?18,?19,?20,?21,?22,   ?23,?24,?25,   ?26,?27,?28,   ?29,?30,?31,
                        ?32,?33,?34,?35,   ?36,?37,?38,?39,   ?40,?41,?42,?43,?44,?45,?46
                    )
                    ON CONFLICT(id) DO UPDATE SET
                        aggregated_signature=excluded.aggregated_signature,
                        signature_occurrences=excluded.signature_occurrences,
                        status=excluded.status"
                )?;

                let prev_ref = block.prev_ref.unwrap_or_default();
                let prev_alt_ref = block.prev_alt_ref.unwrap_or_default();
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
                    block.boc,
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
                    block.data,
                    block.chain_order,
                    block.tr_count,
                    block.thread_id,
                    block.producer_id,
                ];

                stmt.execute(params)
            } else {
                let mut stmt = tx.prepare_cached(
                    "INSERT INTO blocks (
                        id,status,seq_no,parent,producer_id,thread_id,gen_utime,chain_order,boc
                    ) VALUES (
                        ?1,?2,?3,?4,?5,?6,?7,?8,?9
                    )",
                )?;

                let params = rusqlite::params![
                    block.id,
                    block.status,
                    block.seq_no,
                    block.parent,
                    block.producer_id,
                    block.thread_id,
                    block.gen_utime,
                    block.chain_order,
                    block.boc,
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
                in_msg, out_msgs, account_addr, workchain_id, total_fees, balance_delta, old_hash,
                new_hash, chain_order
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6,   ?7, ?8, ?9, ?10, ?11,   ?12, ?13, ?14, ?15, ?16,
                ?17, ?18, ?19, ?20,   ?21, ?22, ?23, ?24, ?25,   ?26, ?27, ?28, ?29,
                ?30, ?31, ?32, ?33, ?34,   ?35, ?36, ?37, ?38, ?39, ?40, ?41, ?42,
                ?43, ?44, ?45, ?46, ?47, ?48, ?49,   ?50, ?51
            ) ON CONFLICT(id) DO NOTHING")?;

            for trx in
                transactions.into_iter().map(<ArchTransaction as Into<FlatTransaction>>::into)
            {
                let params = rusqlite::params![
                    trx.id,
                    trx.block_id,
                    trx.boc,
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
                    trx.balance_delta,
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

    fn has_delivery_problems(&self) -> bool {
        false
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
    use rusqlite::Connection;
    use rusqlite::OpenFlags;

    use crate::sqlite::sqlite_helper::print_sqlite_info;

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
}
