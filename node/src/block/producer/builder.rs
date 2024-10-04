// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;

use serde::Serialize;
use tvm_block::Account;
use tvm_block::AccountStatus;
use tvm_block::AddSub;
use tvm_block::Augmentation;
use tvm_block::BlkPrevInfo;
use tvm_block::Block;
use tvm_block::BlockExtra;
use tvm_block::BlockInfo;
use tvm_block::ComputeSkipReason;
use tvm_block::CopyleftRewards;
use tvm_block::CurrencyCollection;
use tvm_block::Deserializable;
use tvm_block::EnqueuedMsg;
use tvm_block::GetRepresentationHash;
use tvm_block::Grams;
use tvm_block::HashUpdate;
use tvm_block::HashmapAugType;
use tvm_block::InMsg;
use tvm_block::InMsgDescr;
use tvm_block::MerkleUpdate;
use tvm_block::Message;
use tvm_block::MsgEnvelope;
use tvm_block::OutMsg;
use tvm_block::OutMsgDescr;
use tvm_block::OutMsgQueue;
use tvm_block::OutMsgQueueInfo;
use tvm_block::OutMsgQueueKey;
use tvm_block::Serializable;
use tvm_block::ShardAccount;
use tvm_block::ShardAccountBlocks;
use tvm_block::ShardAccounts;
use tvm_block::ShardIdent;
use tvm_block::ShardStateUnsplit;
use tvm_block::StateInit;
use tvm_block::TrComputePhase;
use tvm_block::TrComputePhaseVm;
use tvm_block::Transaction;
use tvm_block::TransactionDescr;
use tvm_block::TransactionDescrOrdinary;
use tvm_block::UnixTime32;
use tvm_block::ValueFlow;
use tvm_executor::BlockchainConfig;
use tvm_executor::ExecuteParams;
use tvm_executor::ExecutorError;
use tvm_executor::OrdinaryTransactionExecutor;
use tvm_executor::TransactionExecutor;
use tvm_types::AccountId;
use tvm_types::Cell;
use tvm_types::HashmapRemover;
use tvm_types::HashmapType;
use tvm_types::SliceData;
use tvm_types::UInt256;
use tvm_types::UsageTree;
use tvm_vm::executor::Engine;
use tvm_vm::executor::EngineTraceInfo;
use tvm_vm::executor::EngineTraceInfoType;
use tvm_vm::stack::Stack;
use tvm_vm::stack::StackItem;

use crate::block::producer::errors::verify_error;
use crate::block::producer::errors::BP_DID_NOT_PROCESS_ALL_MESSAGES_FROM_PREVIOUS_BLOCK;
use crate::block_keeper_system::epoch::create_epoch_touch_message;
use crate::block_keeper_system::epoch::decode_epoch_data;
use crate::block_keeper_system::BlockKeeperData;
use crate::block_keeper_system::BlockKeeperSetChange;
use crate::config::Config;
use crate::creditconfig::abi::DAPP_CONFIG_TVC;
use crate::creditconfig::dappconfig::calculate_dapp_config_address;
use crate::creditconfig::dappconfig::create_config_touch_message;
use crate::creditconfig::dappconfig::decode_dapp_config_data;
use crate::creditconfig::dappconfig::get_available_credit_from_config;
use crate::creditconfig::dappconfig::recalculate_config;
use crate::creditconfig::DappConfig;

pub struct PreparedBlock {
    pub block: Block,
    pub remain_fees: Grams,
    pub state: ShardStateUnsplit,
    pub state_cell: Cell,
    pub is_empty: bool,
    pub transaction_traces: HashMap<UInt256, Vec<EngineTraceInfoData>>,
    pub active_threads: Vec<(Cell, ActiveThread)>,
    pub tx_cnt: usize,
    pub block_keeper_set_changes: Vec<BlockKeeperSetChange>,
}

pub struct ThreadResult {
    transaction: Transaction,
    lt: u64,
    trace: Option<Vec<EngineTraceInfoData>>,
    account_root: Cell,
    account_id: AccountId,
    minted_shell: u128,
    dapp_id: Option<UInt256>,
}

pub struct ActiveThread {
    thread: JoinHandle<anyhow::Result<ThreadResult>>,
    message: Message,
    vm_execution_is_block_related: Arc<Mutex<bool>>,
    block_production_was_finished: Arc<Mutex<bool>>,
}

#[derive(Clone, Default, Debug, Serialize)]
pub struct EngineTraceInfoData {
    pub info_type: String,
    pub step: u32, // number of executable command
    pub cmd_str: String,
    pub stack: Vec<String>,
    pub gas_used: String,
    pub gas_cmd: String,
    pub cmd_code_rem_bits: u32,
    pub cmd_code_hex: String,
    pub cmd_code_cell_hash: String,
    pub cmd_code_offset: u32,
}

impl From<&tvm_vm::executor::EngineTraceInfo<'_>> for EngineTraceInfoData {
    fn from(info: &tvm_vm::executor::EngineTraceInfo) -> Self {
        let cmd_code_rem_bits = info.cmd_code.remaining_bits() as u32;
        let cmd_code_hex = info.cmd_code.to_hex_string();
        let cmd_code_cell_hash = info.cmd_code.cell().repr_hash().to_hex_string();
        let cmd_code_offset = info.cmd_code.pos() as u32;

        Self {
            info_type: format!("{:#?}", info.info_type),
            step: info.step,
            cmd_str: info.cmd_str.clone(),
            stack: info.stack.storage.iter().map(|s| s.to_string()).collect(),
            gas_used: info.gas_used.to_string(),
            gas_cmd: info.gas_cmd.to_string(),
            cmd_code_rem_bits,
            cmd_code_hex,
            cmd_code_cell_hash,
            cmd_code_offset,
        }
    }
}

type MessageIndex = u128;

/// BlockBuilder structure
#[derive(Default)]
pub struct BlockBuilder {
    shard_state: Arc<ShardStateUnsplit>,
    accounts: ShardAccounts,
    pub block_info: BlockInfo,
    rand_seed: UInt256,
    new_messages: BTreeMap<MessageIndex, (Message, Cell)>, /* Mapping of messages generated
                                                            * while execution */
    from_prev_blk: CurrencyCollection,
    pub(crate) in_msg_descr: InMsgDescr,
    pub(crate) out_msg_descr: OutMsgDescr,
    out_queue_info: OutMsgQueueInfo,
    block_gas_limit: u64,
    pub(crate) account_blocks: ShardAccountBlocks,
    total_gas_used: u64,
    total_message_processed: usize,
    start_lt: u64,
    end_lt: u64, // biggest logical time of all messages
    copyleft_rewards: CopyleftRewards,
    transaction_traces: HashMap<UInt256, Vec<EngineTraceInfoData>>,

    control_rx_stop: Option<Receiver<()>>,
    parallelization_level: usize,
    tx_cnt: usize,

    usage_tree: UsageTree,
    initial_shard_state_root: Cell,
    node_config: Config,
    block_keeper_set_changes: Vec<BlockKeeperSetChange>,
    pub base_config_stateinit: StateInit,
    pub dapp_credit_map: HashMap<UInt256, DappConfig>,
    pub dapp_minted_map: HashMap<UInt256, u128>,
}

impl BlockBuilder {
    #[allow(clippy::too_many_arguments)]
    /// Initialize BlockBuilder
    pub fn with_params(
        shard_state_root_cell: Cell,
        prev_ref: BlkPrevInfo,
        time: u32,
        block_gas_limit: u64,
        rand_seed: Option<UInt256>,
        control_rx_stop: Option<Receiver<()>>,
        node_config: Config,
    ) -> anyhow::Result<Self> {
        // Replace shard state cell with Usage Cell to track merkle tree visits
        let usage_tree = UsageTree::with_params(shard_state_root_cell.clone(), true);
        let usage_cell = usage_tree.root_cell();
        let shard_state = Arc::new(
            ShardStateUnsplit::construct_from_cell(usage_cell)
                .map_err(|e| anyhow::format_err!("Failed to construct shard state {e}"))?,
        );
        let accounts = shard_state.read_accounts().map_err(|e| anyhow::format_err!("{e}"))?;
        let out_queue_info =
            shard_state.read_out_msg_queue_info().map_err(|e| anyhow::format_err!("{e}"))?;
        let seq_no = shard_state.seq_no() + 1;

        let start_lt = prev_ref.prev1().map_or(0, |p| p.end_lt) + 1;
        let rand_seed = rand_seed.unwrap_or(UInt256::rand()); // we don't need strict randomization like real node

        let mut block_info = BlockInfo::default();
        block_info.set_shard(shard_state.shard().clone());
        block_info.set_seq_no(seq_no).unwrap();
        block_info.set_prev_stuff(false, &prev_ref).unwrap();
        block_info.set_gen_utime(UnixTime32::new(time));
        block_info.set_start_lt(start_lt);

        let parallelization_level = node_config.local.parallelization_level;

        let base_config_stateinit = StateInit::construct_from_bytes(DAPP_CONFIG_TVC)
            .map_err(|e| anyhow::format_err!("{e}"))?;

        Ok(BlockBuilder {
            shard_state,
            out_queue_info,
            from_prev_blk: accounts.full_balance().clone(),
            accounts,
            block_info,
            rand_seed,
            start_lt,
            end_lt: start_lt + 1,
            block_gas_limit,
            control_rx_stop,
            usage_tree,
            initial_shard_state_root: shard_state_root_cell,
            node_config,
            parallelization_level,
            block_keeper_set_changes: vec![],
            base_config_stateinit,
            ..Default::default()
        })
    }

    /// Shard ident
    fn shard_ident(&self) -> &ShardIdent {
        self.shard_state.shard()
    }

    fn out_msg_key(&self, prefix: u64, hash: UInt256) -> OutMsgQueueKey {
        OutMsgQueueKey::with_workchain_id_and_prefix(
            self.shard_ident().workchain_id(),
            prefix,
            hash,
        )
    }

    fn try_prepare_transaction(
        executor: &OrdinaryTransactionExecutor,
        acc_root: &mut Cell,
        msg: &Message,
        last_trans_hash: UInt256,
        last_trans_lt: u64,
        execute_params: ExecuteParams,
        trace: Arc<lockfree::queue::Queue<EngineTraceInfoData>>,
    ) -> anyhow::Result<(Transaction, u64, Option<Vec<EngineTraceInfoData>>, u128)> {
        let lt = execute_params.last_tr_lt.clone();
        let last_lt = lt.load(Ordering::Relaxed);
        let block_unixtime = execute_params.block_unixtime;
        let vm_execution_is_block_related = execute_params.vm_execution_is_block_related.clone();

        let start = std::time::Instant::now();
        let result = executor.execute_with_libs_and_params(Some(msg), acc_root, execute_params);
        log::trace!(target: "builder", "Execution result {:?}", result);
        log::trace!(target: "builder", "Execution time {} ms", start.elapsed().as_millis());
        log::trace!(
            target: "builder",
            "vm_execution_is_block_related: {}",
            vm_execution_is_block_related.lock().unwrap()
        );
        match result {
            Ok((mut transaction, minted_shell)) => {
                let trace = if transaction
                    .read_description()
                    .map_err(|e| anyhow::format_err!("{e}"))?
                    .is_aborted()
                {
                    Some(trace.pop_iter().collect::<Vec<EngineTraceInfoData>>())
                        .filter(|trace| !trace.is_empty())
                } else {
                    None
                };
                transaction.set_prev_trans_hash(last_trans_hash);
                transaction.set_prev_trans_lt(last_trans_lt);
                Ok((transaction, lt.load(Ordering::Relaxed), trace, minted_shell))
            }
            Err(err) => {
                let old_hash = acc_root.repr_hash();
                let mut account = Account::construct_from_cell(acc_root.clone())
                    .map_err(|e| anyhow::format_err!("{e}"))?;
                let lt = std::cmp::max(
                    account.last_tr_time().unwrap_or(0),
                    std::cmp::max(last_lt, msg.lt().unwrap_or(0) + 1),
                );
                account.set_last_tr_time(lt);
                *acc_root = account.serialize().map_err(|e| anyhow::format_err!("{e}"))?;
                let mut transaction = Transaction::with_account_and_message(&account, msg, lt)
                    .map_err(|e| anyhow::format_err!("{e}"))?;
                transaction.set_now(block_unixtime);
                let mut description =
                    TransactionDescrOrdinary { aborted: true, ..Default::default() };
                match err.downcast_ref::<ExecutorError>() {
                    Some(ExecutorError::NoAcceptError(error, arg)) => {
                        let mut vm_phase = TrComputePhaseVm {
                            success: false,
                            exit_code: *error,
                            ..Default::default()
                        };
                        if let Some(item) = arg {
                            vm_phase.exit_arg = match item
                                .as_integer()
                                .and_then(|value| value.into(i32::MIN..=i32::MAX))
                            {
                                Err(_) | Ok(0) => None,
                                Ok(exit_arg) => Some(exit_arg),
                            };
                        }
                        description.compute_ph = TrComputePhase::Vm(vm_phase);
                    }
                    Some(ExecutorError::NoFundsToImportMsg) => {
                        description.compute_ph = if account.is_none() {
                            TrComputePhase::skipped(ComputeSkipReason::NoState)
                        } else {
                            TrComputePhase::skipped(ComputeSkipReason::NoGas)
                        };
                    }
                    Some(ExecutorError::ExtMsgComputeSkipped(reason)) => {
                        description.compute_ph = TrComputePhase::skipped(reason.clone());
                    }
                    _ => return Err(err).map_err(|e| anyhow::format_err!("{e}"))?,
                }
                transaction
                    .write_description(&TransactionDescr::Ordinary(description))
                    .map_err(|e| anyhow::format_err!("{e}"))?;
                let state_update = HashUpdate::with_hashes(old_hash, acc_root.repr_hash());
                transaction
                    .write_state_update(&state_update)
                    .map_err(|e| anyhow::format_err!("{e}"))?;
                let trace = Some(trace.pop_iter().collect::<Vec<EngineTraceInfoData>>())
                    .filter(|trace| !trace.is_empty());
                transaction.set_prev_trans_hash(last_trans_hash);
                transaction.set_prev_trans_lt(last_trans_lt);
                Ok((transaction, lt, trace, 0))
            }
        }
    }

    fn after_transaction(&mut self, thread_result: ThreadResult) -> anyhow::Result<()> {
        let transaction = thread_result.transaction;
        let max_lt = thread_result.lt;
        let trace = thread_result.trace;
        let acc_root = thread_result.account_root;
        let acc_id = thread_result.account_id;
        self.tx_cnt += 1;

        if let Some(gas_used) = transaction.gas_used() {
            self.total_gas_used += gas_used;
        }
        log::trace!(
            target: "builder",
            "Transaction {:?} {}",
            transaction.hash(),
            transaction.read_description().map_err(|e| anyhow::format_err!("{e}"))?.is_aborted()
        );

        self.end_lt = std::cmp::max(self.end_lt, max_lt);

        let tr_cell = transaction.serialize().map_err(|e| anyhow::format_err!("{e}"))?;
        if let Some(trace) = trace {
            self.transaction_traces.insert(tr_cell.repr_hash(), trace);
        }

        log::trace!(target: "builder", "Transaction ID {:x}", tr_cell.repr_hash());
        log::trace!(
            target: "builder",
            "Transaction aborted: {}",
            transaction.read_description().map_err(|e| anyhow::format_err!("{e}"))?.is_aborted()
        );

        let acc = Account::construct_from_cell(acc_root.clone())
            .map_err(|e| anyhow::format_err!("{e}"))?;

        if let Some(acc_code_hash) = acc.get_code_hash() {
            let code_hash_str = acc_code_hash.as_hex_string();
            if code_hash_str == self.node_config.global.block_keeper_epoch_code_hash {
                log::info!(target: "builder", "after_transaction tx statuses: {:?} {:?}", transaction.orig_status, transaction.end_status);
                if transaction.orig_status == AccountStatus::AccStateNonexist {
                    log::info!(target: "builder", "Epoch contract was deployed");
                    if let Some((id, block_keeper_data)) = decode_epoch_data(&acc)
                        .map_err(|e| anyhow::format_err!("Failed to decode epoch data: {e}"))?
                    {
                        self.block_keeper_set_changes
                            .push(BlockKeeperSetChange::BlockKeeperAdded((id, block_keeper_data)));
                    }
                }
            }
        }

        let shard_acc = ShardAccount::with_account_root(
            acc_root,
            tr_cell.repr_hash(),
            transaction.logical_time(),
        );
        log::trace!(target: "builder", "Update account data: {}", acc_id.to_hex_string());
        let data = shard_acc.write_to_new_cell().map_err(|e| anyhow::format_err!("{e}"))?;
        self.accounts
            .set_builder_serialized(
                acc_id.clone(),
                &data,
                &acc.aug().map_err(|e| anyhow::format_err!("{e}"))?,
            )
            .map_err(|e| anyhow::format_err!("{e}"))?;

        if let Err(err) = self.add_raw_transaction(transaction, tr_cell) {
            log::warn!(target: "builder", "Error append transaction {:?}", err);
            // TODO log error, write to transaction DB about error
        }
        if let Some(dapp_id) = thread_result.dapp_id.clone() {
            if thread_result.minted_shell != 0 {
                if let Some(data) = self.dapp_minted_map.get_mut(&dapp_id) {
                    *data = data.saturating_add(thread_result.minted_shell);
                } else {
                    self.dapp_minted_map.insert(dapp_id, thread_result.minted_shell);
                }
            }
        }
        Ok(())
    }

    fn get_available_credit(
        &mut self,
        acc_id: AccountId,
    ) -> anyhow::Result<(i128, Option<UInt256>)> {
        let mut available_credit = 0;
        let mut dapp_id_opt = None;
        if let Some(acc) = self.accounts.account(&acc_id).map_err(|e| anyhow::format_err!("{e}"))? {
            let dest_dapp_id = acc
                .read_account()
                .map_err(|e| anyhow::format_err!("Failed to get account data: {e}"))?
                .get_dapp_id()
                .cloned();
            if let Some(dapp_id) = dest_dapp_id {
                dapp_id_opt = Some(dapp_id.clone());
                if !self.dapp_credit_map.contains_key(&dapp_id.clone()) {
                    let addr = calculate_dapp_config_address(
                        dapp_id.clone(),
                        self.base_config_stateinit.clone(),
                    )
                    .map_err(|e| anyhow::format_err!("{e}"))?;
                    let acc_id = AccountId::from(addr);
                    if let Some(acc) = self
                        .accounts
                        .account(&AccountId::from(acc_id))
                        .map_err(|e| anyhow::format_err!("{e}"))?
                    {
                        let acc_d = Account::construct_from_cell(acc.account_cell().clone())
                            .map_err(|e| anyhow::format_err!("{e}"))?;
                        let data = decode_dapp_config_data(&acc_d).unwrap();

                        if let Some(configdata) = data {
                            let updated_configdata = recalculate_config(
                                configdata.clone(),
                                u128::from(self.block_info.seq_no()),
                            );
                            available_credit = get_available_credit_from_config(
                                configdata.clone(),
                                u128::from(self.block_info.seq_no()),
                            );
                            self.dapp_credit_map.insert(dapp_id, updated_configdata);
                        }
                    }
                } else {
                    available_credit = get_available_credit_from_config(
                        self.dapp_credit_map[&dapp_id].clone(),
                        u128::from(self.block_info.seq_no()),
                    );
                }
            }
        }
        Ok((available_credit, dapp_id_opt))
    }

    fn execute(
        &mut self,
        message: Message,
        blockchain_config: &BlockchainConfig,
        acc_id: &AccountId,
        lt: Option<u64>,
        block_unixtime: u32,
        block_lt: u64,
    ) -> anyhow::Result<ActiveThread> {
        log::debug!(target: "builder", "Start msg execution: {:?}", message.hash());
        let start = std::time::Instant::now();
        let (available_credit, dapp_id_opt) =
            self.get_available_credit(acc_id.clone()).map_err(|e| anyhow::format_err!("{e}"))?;
        log::debug!(target: "builder", "Execute available credit: {}", available_credit);
        self.total_message_processed += 1;
        log::debug!(target: "builder", "Read account: {}", acc_id.to_hex_string());
        let shard_acc = match self
            .accounts
            .account(acc_id)
            .map_err(|e| anyhow::format_err!("{e}"))?
        {
            Some(acc) => acc,
            None => {
                log::warn!(target: "builder", "Failed to get account, return default: {}", acc_id.to_hex_string());
                ShardAccount::default()
            }
        };
        log::trace!(target: "builder", "Execute: read account time {} ms", start.elapsed().as_millis());
        let mut acc_root = shard_acc.account_cell();
        let executor = OrdinaryTransactionExecutor::new((*blockchain_config).clone());

        let last_lt = lt.unwrap_or(std::cmp::max(self.start_lt, shard_acc.last_trans_lt() + 1));
        let lt = Arc::new(AtomicU64::new(last_lt));
        let trace = Arc::new(lockfree::queue::Queue::new());
        // let trace_copy = trace.clone();
        // let callback = move |engine: &tvm_vm::executor::Engine,
        //                      info: &tvm_vm::executor::EngineTraceInfo| {
        //     trace_copy.push(EngineTraceInfoData::from(info));
        //     simple_trace_callback(engine, info);
        // };
        let vm_execution_is_block_related = Arc::new(Mutex::new(false));
        let mut src_dapp_id: Option<UInt256> = None;
        if let Some(header) = message.int_header() {
            src_dapp_id = header.src_dapp_id().clone();
        }
        let execute_params = ExecuteParams {
            block_unixtime,
            block_lt,
            last_tr_lt: Arc::clone(&lt),
            seed_block: self.rand_seed.clone(),
            debug: false,
            signature_id: self.shard_state.global_id(),
            trace_callback: None, //  Some(Arc::new(callback)),
            vm_execution_is_block_related: vm_execution_is_block_related.clone(),
            seq_no: self.block_info.seq_no(),
            src_dapp_id: src_dapp_id.clone(),
            available_credit,
            ..Default::default()
        };

        let acc_id = acc_id.clone();

        {
            let account_start = std::time::Instant::now();
            if let Ok(account) = Account::construct_from_cell(acc_root.clone()) {
                if let Some(code_hash) = account.get_code_hash() {
                    let code_hash_str = code_hash.to_hex_string();
                    log::trace!(target: "builder", "Start acc code hash: {}", code_hash_str);
                    if code_hash_str == self.node_config.global.block_keeper_epoch_code_hash {
                        log::trace!(target: "builder", "Message src: {:?}, dst: {:?}", message.src(), message.dst());
                        if message.src() == message.dst() {
                            log::trace!(target: "builder", "Epoch destroy message");
                            if let Some((id, block_keeper_data)) = decode_epoch_data(&account)
                                .map_err(|e| {
                                    anyhow::format_err!("Failed to decode epoch data: {e}")
                                })?
                            {
                                self.block_keeper_set_changes.push(
                                    BlockKeeperSetChange::BlockKeeperRemoved((
                                        id,
                                        block_keeper_data,
                                    )),
                                );
                            }
                        }
                    }
                }
                log::trace!(target: "builder", "Start acc code hash elapsed: {}", account_start.elapsed().as_millis());
            }
        }
        let message_clone = message.clone();
        let thread =
            std::thread::Builder::new().name("Message execution".to_string()).spawn(move || {
                log::debug!(target: "builder", "Executing message {}", message.hash().unwrap().to_hex_string());
                let res = Self::try_prepare_transaction(
                    &executor,
                    &mut acc_root,
                    &message,
                    shard_acc.last_trans_hash().clone(),
                    shard_acc.last_trans_lt(),
                    execute_params,
                    trace
                );
                log::trace!(target: "builder", "Execute: total time {} ms, available_credit {}, result with minted {:?}", start.elapsed().as_millis(), available_credit, res);
                res.map(|(tx, lt, trace, minted_shell)| ThreadResult {
                    transaction: tx,
                    lt,
                    trace,
                    account_root: acc_root,
                    account_id: acc_id,
                    minted_shell,
                    dapp_id: dapp_id_opt
                })
            })?;

        Ok(ActiveThread {
            thread,
            message: message_clone,
            vm_execution_is_block_related,
            block_production_was_finished: Arc::new(Mutex::new(false)),
        })
    }

    fn is_limits_reached(&self) -> bool {
        if let Some(rx) = &self.control_rx_stop {
            if rx.try_recv().is_ok() {
                log::info!(target: "builder", "block builder received stop");
                return true;
            }
        }
        self.total_gas_used > self.block_gas_limit
    }

    pub fn build_block(
        mut self,
        mut queue: VecDeque<Message>,
        blockchain_config: &BlockchainConfig,
        mut active_threads: Vec<(Cell, ActiveThread)>,
        mut epoch_block_keeper_data: Vec<BlockKeeperData>,
        check_internal_messages: Option<HashSet<UInt256>>,
    ) -> anyhow::Result<(PreparedBlock, usize)> {
        let mut processed_ext_messages_cnt = 0;
        log::info!(target: "builder", "Start build of block: {}", self.block_info.seq_no());
        log::info!(target: "builder", "Build block with parallelization_level: {}", self.parallelization_level);
        let mut block_full = false;
        let out_queue = self.out_queue_info.out_queue().clone();
        let msg_count = out_queue.len().map_err(|e| anyhow::format_err!("{e}"))?;
        log::info!(target: "builder", "out_queue.len={}, queue.len={}, active_threads.len={}", msg_count, queue.len(), active_threads.len());
        let (block_unixtime, block_lt) = self.at_and_lt();
        self.execute_epoch_messages(
            &mut epoch_block_keeper_data,
            blockchain_config,
            block_unixtime,
            block_lt,
        )
        .map_err(|e| anyhow::format_err!("{e}"))?;

        // first import internal messages
        let mut sorted = Vec::with_capacity(msg_count);
        let mut active_int_destinations = HashSet::new();
        let verify_block_contains_missing_messages_from_prev_state = false;

        for out in out_queue.iter() {
            let (key, mut slice) = out.map_err(|e| anyhow::format_err!("{e}"))?;
            let key = key.into_cell().map_err(|e| anyhow::format_err!("{e}"))?;
            let (enqueued_message, create_lt) =
                OutMsgQueue::value_aug(&mut slice).map_err(|e| anyhow::format_err!("{e}"))?;
            let message = enqueued_message
                .read_out_msg()
                .map_err(|e| anyhow::format_err!("{e}"))?
                .read_message()
                .map_err(|e| anyhow::format_err!("{e}"))?;
            if let Some(msg_set) = &check_internal_messages {
                if !msg_set.contains(&message.hash().unwrap()) {
                    // verify_block_contains_missing_messages_from_prev_state = true;
                    continue;
                }
            }
            if let Some(acc_id) = message.int_dst_account_id() {
                // leave only messages with internal destination address
                // key is not matter for one shard
                if active_threads.iter().any(|(k, _active_thread)| k == &key) {
                    active_int_destinations.insert(acc_id.clone());
                } else {
                    sorted.push((key, message, acc_id, create_lt));
                }
            } else {
                self.out_queue_info
                    .out_queue_mut()
                    .remove(SliceData::load_cell(key).map_err(|e| anyhow::format_err!("{e}"))?)
                    .map_err(|e| anyhow::format_err!("{e}"))?;
            }
        }
        // Sort by message time
        sorted.sort_by(|a, b| a.3.cmp(&b.3));
        let start = std::time::Instant::now();
        // if there are some internal messages start parallel execution
        log::info!(target: "builder", "Internal messages execution start, messages cnt: {}", sorted.len());
        if !sorted.is_empty() || !active_threads.is_empty() {
            // Start first message execution separately because we must wait for it to
            // finish
            let mut first_thread = None;
            let mut first_key = None;

            for i in 0..sorted.len() {
                let acc_id = sorted.get(i).unwrap().2.clone();
                if !active_int_destinations.contains(&acc_id) {
                    let (key, first_message, first_acc_id, _) = sorted.remove(i);
                    log::trace!(target: "builder", "First int message: {:?} to {:?}, key: {}", first_message.hash().unwrap(), first_acc_id.to_hex_string(), key.repr_hash().to_hex_string());
                    first_thread = Some(self.execute(
                        first_message,
                        blockchain_config,
                        &first_acc_id,
                        None,
                        block_unixtime,
                        block_lt,
                    )?);
                    first_key = Some(key);
                    // self.out_queue_info
                    //     .out_queue_mut()
                    //     .remove(SliceData::load_cell(first_key.take().unwrap())?)?;
                    active_int_destinations.insert(first_acc_id);
                    break;
                }
            }

            loop {
                // If active pool is not full add threads
                if active_threads.len() < self.parallelization_level {
                    for i in 0..sorted.len() {
                        let (_, _, id, _) = &sorted[i];
                        if !active_int_destinations.contains(id) {
                            let (key, message, acc_id, _) = sorted.remove(i);
                            log::trace!(target: "builder", "Parallel int message: {:?} to {:?}, key {}", message.hash().unwrap(), acc_id.to_hex_string(), key.repr_hash().to_hex_string());
                            let thread = self.execute(
                                message,
                                blockchain_config,
                                &acc_id,
                                None,
                                block_unixtime,
                                block_lt,
                            )?;
                            active_threads.push((key.clone(), thread));
                            // self.out_queue_info.out_queue_mut().remove(SliceData::load_cell(key)?
                            // )?;
                            active_int_destinations.insert(acc_id);
                            break;
                        }
                    }
                }
                // Check the first thread finalization
                let first_finished =
                    first_thread.as_ref().map(|thread| thread.thread.is_finished());
                if let Some(true) = first_finished {
                    log::trace!(target: "builder", "First int message finished, key: {}", first_key.as_ref().unwrap().repr_hash().to_hex_string());
                    let thread_result =
                        first_thread.take().unwrap().thread.join().map_err(|_| {
                            anyhow::format_err!("Failed to execute transaction in parallel")
                        })??;
                    let acc_id = thread_result.account_id.clone();
                    self.after_transaction(thread_result)?;
                    active_int_destinations.remove(&acc_id);
                    self.out_queue_info
                        .out_queue_mut()
                        .remove(
                            SliceData::load_cell(first_key.take().unwrap())
                                .map_err(|e| anyhow::format_err!("{e}"))?,
                        )
                        .map_err(|e| anyhow::format_err!("{e}"))?;
                }
                // Check active threads
                let mut i = 0;
                while i < active_threads.len() {
                    if active_threads[i].1.thread.is_finished() {
                        let (key, thread) = active_threads.remove(i);
                        let (vm_execution_is_block_related, block_production_was_finished) = (
                            thread.vm_execution_is_block_related.lock().unwrap(),
                            thread.block_production_was_finished.lock().unwrap(),
                        );
                        let thread_result = thread.thread.join().map_err(|_| {
                            anyhow::format_err!("Failed to execute transaction in parallel")
                        })??;
                        log::trace!(target: "builder", "Thread with dapp_id and mintedshell {:?} {:?} {:?}", thread_result.dapp_id.clone(), thread_result.minted_shell.clone(), thread_result.transaction.clone());
                        let acc_id = thread_result.account_id.clone();
                        if *vm_execution_is_block_related && *block_production_was_finished {
                            log::trace!(target: "builder", "parallel int message finished dest: {}, key: {}, but tx was block related so result is not used", acc_id.to_hex_string(), key.repr_hash().to_hex_string());
                            sorted.push((key, thread.message, acc_id.clone(), 0));
                        } else {
                            log::trace!(target: "builder", "parallel int message finished dest: {}, key: {}", acc_id.to_hex_string(), key.repr_hash().to_hex_string());
                            self.after_transaction(thread_result)?;
                            self.out_queue_info
                                .out_queue_mut()
                                .remove(
                                    SliceData::load_cell(key)
                                        .map_err(|e| anyhow::format_err!("{e}"))?,
                                )
                                .map_err(|e| anyhow::format_err!("{e}"))?;
                        }
                        active_int_destinations.remove(&acc_id);
                    } else {
                        i += 1;
                    }
                }
                if self.is_limits_reached() {
                    log::debug!(target: "builder", "Internal messages stop was set because block is full");
                    block_full = true;
                }
                // If first message was processed and block should be finalized, break the loop
                if block_full && first_thread.is_none() {
                    log::debug!(target: "builder", "Internal messages stop because block is full");
                    break;
                }
                if sorted.is_empty() && active_threads.is_empty() && first_thread.is_none() {
                    log::debug!(target: "builder", "Internal messages stop");
                    break;
                }
            }
        }
        log::info!(target: "builder", "Internal messages execution time {} ms", start.elapsed().as_millis());

        let start = std::time::Instant::now();
        // second import external messages
        if !block_full {
            let mut active_destinations = HashSet::new();
            let mut active_ext_threads = VecDeque::new();
            loop {
                // If active pool is not full add threads
                if active_ext_threads.len() < self.parallelization_level {
                    while !queue.is_empty() {
                        if active_ext_threads.len() == self.parallelization_level {
                            break;
                        }
                        if let Some(acc_id) = queue[0].int_dst_account_id() {
                            if !active_destinations.contains(&acc_id) {
                                let msg = queue.pop_front().unwrap();
                                log::trace!(target: "builder", "Parallel ext message: {:?} to {:?}", msg.hash().unwrap(), acc_id.to_hex_string());
                                let thread = self.execute(
                                    msg,
                                    blockchain_config,
                                    &acc_id,
                                    None,
                                    block_unixtime,
                                    block_lt,
                                )?;
                                active_ext_threads.push_back(thread);
                                active_destinations.insert(acc_id);
                            } else {
                                break;
                            }
                        } else {
                            log::warn!(
                                target: "builder",
                                "Found external msg with not valid internal destination: {:?}",
                                queue.pop_front().unwrap()
                            );
                        }
                    }
                }

                while !active_ext_threads.is_empty() {
                    if active_ext_threads.front().unwrap().thread.is_finished() {
                        let thread = active_ext_threads.pop_front().unwrap();
                        let thread_result = thread.thread.join().map_err(|_| {
                            anyhow::format_err!("Failed to execute transaction in parallel")
                        })??;
                        log::trace!(target: "builder", "Thread with dapp_id and mintedshell {:?} {:?} {:?}", thread_result.dapp_id.clone(), thread_result.minted_shell.clone(), thread_result.transaction.clone());
                        let acc_id = thread_result.account_id.clone();
                        log::trace!(target: "builder", "parallel ext message finished dest: {}", acc_id.to_hex_string());
                        self.after_transaction(thread_result)?;
                        active_destinations.remove(&acc_id);
                        processed_ext_messages_cnt += 1;
                    } else {
                        break;
                    }
                }

                if self.is_limits_reached() {
                    block_full = true;
                    log::debug!(target: "builder", "Ext messages stop because block is full");
                    break;
                }
                if queue.is_empty() && active_ext_threads.is_empty() {
                    log::debug!(target: "builder", "Ext messages stop");
                    break;
                }
            }
        }
        log::info!(target: "builder", "External messages execution time {} ms", start.elapsed().as_millis());

        // process new messages in parallel if block is not full
        log::info!(target: "builder", "Start new messages execution");
        // for (index, (message, _)) in &self.new_messages {
        //     log::trace!(target: "builder", "New message: {:?} {:?}", index,
        // message.hash().unwrap()); }
        let start = std::time::Instant::now();
        if !block_full {
            let mut active_destinations = HashMap::new();
            loop {
                let mut there_are_no_new_messages_for_verify_block = true;
                if active_threads.len() < self.parallelization_level {
                    let mut next_message = None;
                    for (index, (message, _tr_cell)) in &self.new_messages {
                        // TODO: need to add check if dst account belongs to this shard or be ext
                        // see process int msgs
                        if let Some(msg_set) = &check_internal_messages {
                            if verify_block_contains_missing_messages_from_prev_state {
                                return Err(verify_error(
                                    BP_DID_NOT_PROCESS_ALL_MESSAGES_FROM_PREVIOUS_BLOCK,
                                ));
                            }
                            if !msg_set.contains(&message.hash().unwrap()) {
                                continue;
                            }
                        }
                        there_are_no_new_messages_for_verify_block = false;
                        let acc_id = message.int_dst_account_id().unwrap_or_default();
                        if !active_destinations.contains_key(&acc_id) {
                            let info = message
                                .int_header()
                                .ok_or_else(|| anyhow::format_err!("message is not internal"))?;
                            let fwd_fee = info.fwd_fee();
                            let msg_cell =
                                message.serialize().map_err(|e| anyhow::format_err!("{e}"))?;
                            let _env = MsgEnvelope::with_message_and_fee(message, *fwd_fee)
                                .map_err(|e| anyhow::format_err!("{e}"))?;
                            let prefix = acc_id
                                .clone()
                                .get_next_u64()
                                .map_err(|e| anyhow::format_err!("{e}"))?;
                            let key = self.out_msg_key(prefix, msg_cell.repr_hash());
                            next_message = Some((message.clone(), acc_id, *index, key));
                            break;
                        };
                    }
                    if let Some((message, acc_id, index, key)) = next_message {
                        let key = key
                            .write_to_new_cell()
                            .map_err(|e| anyhow::format_err!("{e}"))?
                            .into_cell()
                            .map_err(|e| anyhow::format_err!("{e}"))?;
                        log::trace!(target: "builder", "Parallel new message: {:?} to {:?}, key: {}", message.hash().unwrap(), acc_id.to_hex_string(), key.repr_hash().to_hex_string());
                        let thread = self.execute(
                            message,
                            blockchain_config,
                            &acc_id,
                            None,
                            block_unixtime,
                            block_lt,
                        )?;
                        active_threads.push((key, thread));
                        active_destinations.insert(acc_id, index);
                    }
                }

                // Stop building verify block
                if check_internal_messages.is_some()
                    && there_are_no_new_messages_for_verify_block
                    && active_threads.is_empty()
                {
                    log::debug!(target: "builder", "Stop building verify block");
                    break;
                }

                // Check active threads
                let mut i = 0;
                while i < active_threads.len() {
                    if active_threads[i].1.thread.is_finished() {
                        let (key, thread) = active_threads.remove(i);
                        let thread_result = thread.thread.join().map_err(|_| {
                            anyhow::format_err!("Failed to execute transaction in parallel")
                        })??;
                        log::trace!(target: "builder", "Thread with dapp_id and mintedshell {:?} {:?} {:?}", thread_result.dapp_id.clone(), thread_result.minted_shell.clone(), thread_result.transaction.clone());
                        let acc_id = thread_result.account_id.clone();
                        log::trace!(target: "builder", "parallel int message finished dest: {}, key {}", acc_id.to_hex_string(), key.repr_hash().to_hex_string());
                        self.after_transaction(thread_result)?;
                        let index = active_destinations.remove(&acc_id).unwrap();
                        log::trace!(target: "builder", "remove new message: {}", index);
                        self.new_messages.remove(&index);
                    } else {
                        i += 1;
                    }
                }

                if self.is_limits_reached() {
                    log::debug!(target: "builder", "New messages stop because block is full");
                    break;
                }
                if self.new_messages.is_empty() && active_threads.is_empty() {
                    log::debug!(target: "builder", "New messages stop");
                    break;
                }
            }
        }
        log::info!(target: "builder", "New messages execution time {} ms", start.elapsed().as_millis());
        self.execute_dapp_config_messages(blockchain_config, block_unixtime, block_lt)
            .map_err(|e| anyhow::format_err!("{e}"))?;

        let remain_fees =
            self.in_msg_descr.root_extra().fees_collected + self.account_blocks.root_extra().grams;
        // save new messages
        let start = std::time::Instant::now();
        if !self.new_messages.is_empty() {
            log::info!(target: "builder", "save new messages cnt {:?}", self.new_messages.len());
            for (message, tr_cell) in std::mem::take(&mut self.new_messages).into_values() {
                let info = message
                    .int_header()
                    .ok_or_else(|| anyhow::format_err!("message is not internal"))?;
                let fwd_fee = info.fwd_fee();
                let msg_cell = message.serialize().map_err(|e| anyhow::format_err!("{e}"))?;
                // TODO: use it when interface is merged
                let env = MsgEnvelope::with_message_and_fee(&message, *fwd_fee)
                    .map_err(|e| anyhow::format_err!("{e}"))?;
                let acc_id = message.int_dst_account_id().unwrap_or_default();
                let enq = EnqueuedMsg::with_param(info.created_lt, &env)
                    .map_err(|e| anyhow::format_err!("{e}"))?;
                let prefix =
                    acc_id.clone().get_next_u64().map_err(|e| anyhow::format_err!("{e}"))?;
                let key = self.out_msg_key(prefix, msg_cell.repr_hash());
                self.out_queue_info
                    .out_queue_mut()
                    .set(&key, &enq, &enq.aug().map_err(|e| anyhow::format_err!("{e}"))?)
                    .map_err(|e| anyhow::format_err!("{e}"))?;
                let out_msg = OutMsg::new(enq.out_msg_cell(), tr_cell);
                self.out_msg_descr
                    .set(
                        &msg_cell.repr_hash(),
                        &out_msg,
                        &out_msg.aug().map_err(|e| anyhow::format_err!("{e}"))?,
                    )
                    .map_err(|e| anyhow::format_err!("{e}"))?;
            }
            self.new_messages.clear();
        }
        log::info!(target: "builder", "New messages save time {} ms", start.elapsed().as_millis());

        for active_thread in &active_threads {
            let mut value = active_thread.1.block_production_was_finished.lock().unwrap();
            *value = true;
        }

        log::info!(target: "builder", "in messages queue len={}", queue.len());
        let transaction_traces = std::mem::take(&mut self.transaction_traces);
        let tx_cnt = self.tx_cnt;
        let block_keeper_set_changes = self.block_keeper_set_changes.clone();
        let (block, new_shard_state, new_shard_state_cell) =
            self.finish_block().map_err(|e| anyhow::format_err!("{e}"))?;
        Ok((
            PreparedBlock {
                block,
                state: new_shard_state,
                state_cell: new_shard_state_cell,
                is_empty: false,
                transaction_traces,
                active_threads,
                tx_cnt,
                remain_fees,
                block_keeper_set_changes,
            },
            processed_ext_messages_cnt,
        ))
    }

    pub fn execute_epoch_messages(
        &mut self,
        epoch_block_keeper_data: &mut Vec<BlockKeeperData>,
        blockchain_config: &BlockchainConfig,
        block_unixtime: u32,
        block_lt: u64,
    ) -> anyhow::Result<()> {
        // execute epoch messages
        let mut active_destinations = HashSet::new();
        let mut active_ext_threads = VecDeque::new();
        loop {
            // If active pool is not full add threads
            if active_ext_threads.len() < self.parallelization_level {
                while !epoch_block_keeper_data.is_empty() {
                    if active_ext_threads.len() == self.parallelization_level {
                        break;
                    }
                    let msg = create_epoch_touch_message(
                        &epoch_block_keeper_data[0],
                        self.block_info.gen_utime().as_u32(),
                    )?;
                    if let Some(acc_id) = msg.int_dst_account_id() {
                        if !active_destinations.contains(&acc_id) {
                            epoch_block_keeper_data.remove(0);
                            log::trace!(target: "builder", "Parallel epoch message: {:?} to {:?}", msg.hash().unwrap(), acc_id.to_hex_string());
                            let thread = self.execute(
                                msg,
                                blockchain_config,
                                &acc_id,
                                None,
                                block_unixtime,
                                block_lt,
                            )?;
                            active_ext_threads.push_back(thread);
                            active_destinations.insert(acc_id);
                        } else {
                            break;
                        }
                    } else {
                        epoch_block_keeper_data.remove(0);
                        log::warn!(
                            target: "builder",
                            "Found epoch msg with not valid internal destination: {:?}",
                            msg
                        );
                    }
                }
            }

            while !active_ext_threads.is_empty() {
                if active_ext_threads.front().unwrap().thread.is_finished() {
                    let thread = active_ext_threads.pop_front().unwrap();
                    let thread_result = thread.thread.join().map_err(|_| {
                        anyhow::format_err!("Failed to execute transaction in parallel")
                    })??;
                    let acc_id = thread_result.account_id.clone();
                    log::trace!(target: "builder", "parallel epoch message finished dest: {}", acc_id.to_hex_string());
                    self.after_transaction(thread_result)?;
                    active_destinations.remove(&acc_id);
                } else {
                    break;
                }
            }

            if epoch_block_keeper_data.is_empty() && active_ext_threads.is_empty() {
                log::debug!(target: "builder", "Epoch messages stop");
                break;
            }
        }
        Ok(())
    }

    pub fn execute_dapp_config_messages(
        &mut self,
        blockchain_config: &BlockchainConfig,
        block_unixtime: u32,
        block_lt: u64,
    ) -> anyhow::Result<()> {
        log::trace!(target: "builder", "map of minted shell {:?}", self.dapp_minted_map);
        let mut config_messages: Vec<Message> = Vec::new();
        // execute DappConfig messages
        for (key, value) in self.dapp_minted_map.clone() {
            if value == 0 {
                continue;
            }
            let addr =
                calculate_dapp_config_address(key.clone(), self.base_config_stateinit.clone())
                    .map_err(|e| anyhow::format_err!("{e}"))?;
            let dapp_config = self.dapp_credit_map.get(&key).expect("need to exist dapp_id");
            if dapp_config.is_unlimit {
                continue;
            }
            let message = create_config_touch_message(
                dapp_config.clone(),
                value,
                addr,
                self.block_info.gen_utime().as_u32(),
            )
            .map_err(|e| anyhow::format_err!("{e}"))?;
            config_messages.push(message);
        }
        let mut active_destinations = HashSet::new();
        let mut active_ext_threads = VecDeque::new();
        loop {
            if active_ext_threads.len() < self.parallelization_level {
                while !config_messages.is_empty() {
                    if active_ext_threads.len() == self.parallelization_level {
                        break;
                    }
                    if let Some(acc_id) = config_messages[0].int_dst_account_id() {
                        if !active_destinations.contains(&acc_id) {
                            let msg = config_messages.remove(0);
                            log::trace!(target: "builder", "Parallel config message: {:?} to {:?}", msg.hash().unwrap(), acc_id.to_hex_string());
                            let thread = self.execute(
                                msg,
                                blockchain_config,
                                &acc_id,
                                None,
                                block_unixtime,
                                block_lt,
                            )?;
                            active_ext_threads.push_back(thread);
                            active_destinations.insert(acc_id);
                        } else {
                            break;
                        }
                    } else {
                        log::warn!(
                            target: "builder",
                            "Found dapp config msg with not valid internal destination: {:?}",
                            config_messages.remove(0)
                        );
                    }
                }
            }

            while !active_ext_threads.is_empty() {
                if active_ext_threads.front().unwrap().thread.is_finished() {
                    let thread = active_ext_threads.pop_front().unwrap();
                    let thread_result = thread.thread.join().map_err(|_| {
                        anyhow::format_err!("Failed to execute transaction in parallel")
                    })??;
                    let acc_id = thread_result.account_id.clone();
                    log::trace!(target: "builder", "parallel epoch message finished dest: {}", acc_id.to_hex_string());
                    self.after_transaction(thread_result)?;
                    active_destinations.remove(&acc_id);
                } else {
                    break;
                }
            }

            if config_messages.is_empty() && active_ext_threads.is_empty() {
                log::debug!(target: "builder", "Dapp Config messages stop");
                break;
            }
        }
        Ok(())
    }

    /// Add transaction to block
    pub fn add_raw_transaction(
        &mut self,
        transaction: Transaction,
        tr_cell: Cell,
    ) -> anyhow::Result<()> {
        log::debug!(
            target: "builder",
            "Inserting transaction {} {}",
            transaction.account_id().to_hex_string(),
            transaction.hash().unwrap().to_hex_string()
        );

        self.account_blocks
            .add_serialized_transaction(&transaction, &tr_cell)
            .map_err(|e| anyhow::format_err!("{e}"))?;
        if let Some(copyleft_reward) = transaction.copyleft_reward() {
            self.copyleft_rewards
                .add_copyleft_reward(&copyleft_reward.address, &copyleft_reward.reward)
                .map_err(|e| anyhow::format_err!("{e}"))?;
        }

        if let Some(msg_cell) = transaction.in_msg_cell() {
            let msg = Message::construct_from_cell(msg_cell.clone())
                .map_err(|e| anyhow::format_err!("{e}"))?;
            let in_msg = if let Some(hdr) = msg.int_header() {
                let fee = hdr.fwd_fee();
                let env = MsgEnvelope::with_message_and_fee(&msg, *fee)
                    .map_err(|e| anyhow::format_err!("{e}"))?;
                InMsg::immediate(
                    env.serialize().map_err(|e| anyhow::format_err!("{e}"))?,
                    tr_cell.clone(),
                    *fee,
                )
            } else {
                InMsg::external(msg_cell.clone(), tr_cell.clone())
            };

            log::debug!(target: "builder", "Add in message to in_msg_descr: ");
            self.in_msg_descr
                .set(
                    &msg_cell.repr_hash(),
                    &in_msg,
                    &in_msg.aug().map_err(|e| anyhow::format_err!("{e}"))?,
                )
                .map_err(|e| anyhow::format_err!("{e}"))?;
        }
        // let mut src_dapp_id = None;
        // if let Some(acc) = self
        //     .accounts
        //     .account(transaction.account_id())
        //     .map_err(|e| anyhow::format_err!("{e}"))?
        // {
        //     src_dapp_id = acc
        //         .read_account()
        //         .map_err(|e| anyhow::format_err!("Failed to get account data: {e}"))?
        //         .get_dapp_id()
        //         .cloned();
        // }
        transaction
            .iterate_out_msgs(|msg| {
                if msg.int_header().is_some() {
                    // msg.int_header_mut()
                    //     .expect("Message int header")
                    //     .set_src_dapp_id(src_dapp_id.clone());
                    // We use message lt as a mapping key to have them sorted and have unique keys
                    // But messages from different account can have equal lt so add hash to index
                    let mut n_index = ((msg.lt().unwrap() as u128) << 64)
                        + (msg.hash().unwrap().first_u64() as u128);
                    loop {
                        if self.new_messages.contains_key(&n_index) {
                            n_index += 1;
                        } else {
                            break;
                        }
                    }
                    log::debug!(
                        target: "builder",
                        "Inserting new message with {:?}",
                        msg
                    );
                    self.new_messages.insert(n_index, (msg, tr_cell.clone()));
                } else {
                    //////////////TODO
                    let msg_cell = msg.serialize()?;
                    let out_msg = OutMsg::external(msg_cell.clone(), tr_cell.clone());
                    log::debug!(
                        target: "builder",
                        "Inserting new message with {:?}",
                        msg
                    );
                    self.out_msg_descr.set(&msg_cell.repr_hash(), &out_msg, &out_msg.aug()?)?;
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("{e}"))?;
        Ok(())
    }

    /// Get UNIX time and Logical Time of the current block
    pub fn at_and_lt(&self) -> (u32, u64) {
        (self.block_info.gen_utime().as_u32(), self.start_lt)
    }

    /// Complete the construction of the block and return it.
    /// returns generated block and new shard state bag (and transaction count)
    fn finish_block(mut self) -> anyhow::Result<(Block, ShardStateUnsplit, Cell)> {
        log::trace!(target: "builder", "finish_block");
        let mut new_shard_state = self.shard_state.deref().clone();
        log::info!(target: "builder", "finish block: seq_no: {:?}", self.block_info.seq_no());
        new_shard_state.set_seq_no(self.block_info.seq_no());
        new_shard_state.write_accounts(&self.accounts).map_err(|e| anyhow::format_err!("{e}"))?;
        new_shard_state
            .write_out_msg_queue_info(&self.out_queue_info)
            .map_err(|e| anyhow::format_err!("{e}"))?;
        log::info!(
            target: "builder",
            "finish block new_shard_state hash: {:?}",
            new_shard_state.hash().unwrap().to_hex_string()
        );

        log::info!(target: "builder", "finish_block: write_out_msg_queue");
        let mut block_extra = BlockExtra::default();
        block_extra
            .write_in_msg_descr(&self.in_msg_descr)
            .map_err(|e| anyhow::format_err!("{e}"))?;
        block_extra
            .write_out_msg_descr(&self.out_msg_descr)
            .map_err(|e| anyhow::format_err!("{e}"))?;
        block_extra
            .write_account_blocks(&self.account_blocks)
            .map_err(|e| anyhow::format_err!("{e}"))?;
        block_extra.rand_seed = self.rand_seed;
        log::info!(target: "builder", "finish_block: prepare block extra");

        let mut value_flow = ValueFlow {
            fees_collected: self.account_blocks.root_extra().clone(),
            imported: self.in_msg_descr.root_extra().value_imported.clone(),
            exported: self.out_msg_descr.root_extra().clone(),
            from_prev_blk: self.from_prev_blk,
            to_next_blk: self.accounts.full_balance().clone(),
            copyleft_rewards: self.copyleft_rewards,
            ..Default::default()
        };
        value_flow
            .fees_collected
            .grams
            .add(&self.in_msg_descr.root_extra().fees_collected)
            .map_err(|e| anyhow::format_err!("{e}"))?;

        log::info!(target: "builder", "finish_block: prepare value flow");

        let new_ss_root = new_shard_state.serialize().map_err(|e| anyhow::format_err!("{e}"))?;
        log::info!(target: "builder", "finish_block: serialize new state: {}", new_ss_root.repr_hash().to_hex_string());
        let old_ss_root = self.initial_shard_state_root;
        log::info!(target: "builder", "finish_block: got old state: {}", old_ss_root.repr_hash().to_hex_string());
        log::trace!(target: "builder", "finish_block: usage tree root: {:?}", self.usage_tree.root_cell());
        log::trace!(target: "builder", "finish_block: usage tree set: {:?}", self.usage_tree.build_visited_set());
        let update_time = std::time::Instant::now();
        let state_update =
            MerkleUpdate::create_fast(&old_ss_root, &new_ss_root, |h| self.usage_tree.contains(h))
                .map_err(|e| anyhow::format_err!("{e}"))?;
        log::info!(target: "builder", "finish_block: prepare merkle update: {}ms", update_time.elapsed().as_millis());
        self.block_info.set_end_lt(self.end_lt.max(self.start_lt + 1));

        let block = Block::with_params(
            self.shard_state.global_id(),
            self.block_info,
            value_flow,
            state_update,
            block_extra,
        )
        .map_err(|e| anyhow::format_err!("{e}"))?;
        log::info!(target: "builder", "Finish block: {:?}", block.hash().unwrap().to_hex_string());

        Ok((block, new_shard_state, new_ss_root))
    }
}

pub fn simple_trace_callback(engine: &Engine, info: &EngineTraceInfo) {
    if info.info_type == EngineTraceInfoType::Dump {
        log::info!(target: "tvm_op", "{}", info.cmd_str);
    } else if info.info_type == EngineTraceInfoType::Start {
        if engine.trace_bit(Engine::TRACE_CTRLS) {
            log::trace!(target: "tvm", "{}", engine.dump_ctrls(true));
        }
        if engine.trace_bit(Engine::TRACE_STACK) {
            log::info!(target: "tvm", " [ {} ] \n", dump_stack_result(info.stack));
        }
        if engine.trace_bit(Engine::TRACE_GAS) {
            log::info!(target: "tvm", "gas - {}\n", info.gas_used);
        }
    } else if info.info_type == EngineTraceInfoType::Exception {
        if engine.trace_bit(Engine::TRACE_CODE) {
            log::info!(target: "tvm", "{} ({}) BAD_CODE: {}\n", info.step, info.gas_cmd, info.cmd_str);
        }
        if engine.trace_bit(Engine::TRACE_STACK) {
            log::info!(target: "tvm", " [ {} ] \n", dump_stack_result(info.stack));
        }
        if engine.trace_bit(Engine::TRACE_CTRLS) {
            log::trace!(target: "tvm", "{}", engine.dump_ctrls(true));
        }
        if engine.trace_bit(Engine::TRACE_GAS) {
            log::info!(target: "tvm", "gas - {}\n", info.gas_used);
        }
    } else if info.has_cmd() {
        if engine.trace_bit(Engine::TRACE_CODE) {
            log::info!(target: "tvm", "{}\n", info.cmd_str);
        }
        if engine.trace_bit(Engine::TRACE_STACK) {
            log::info!(target: "tvm", " [ {} ] \n", dump_stack_result(info.stack));
        }
        if engine.trace_bit(Engine::TRACE_CTRLS) {
            log::trace!(target: "tvm", "{}", engine.dump_ctrls(true));
        }
        if engine.trace_bit(Engine::TRACE_GAS) {
            log::info!(target: "tvm", "gas - {}\n", info.gas_used);
        }
    }
}

fn dump_stack_result(stack: &Stack) -> String {
    lazy_static::lazy_static!(
        static ref PREV_STACK: Mutex<Stack> = Mutex::new(Stack::new());
    );
    let mut prev_stack = PREV_STACK.lock().unwrap();
    let mut result = String::new();
    let mut iter = prev_stack.iter();
    let mut same = false;
    for item in stack.iter() {
        if let Some(prev) = iter.next() {
            if prev == item {
                same = true;
                continue;
            }
            while iter.next().is_some() {}
        }
        if same {
            same = false;
            result = "--\"-- ".to_string();
        }
        let string = match item {
            StackItem::None => "N".to_string(),
            StackItem::Integer(data) => match data.bitsize() {
                Ok(0..=230) => data.to_string(),
                Ok(bitsize) => format!("I{}", bitsize),
                Err(err) => err.to_string(),
            },
            StackItem::Cell(data) => {
                format!("C{}-{}", data.bit_length(), data.references_count())
            }
            StackItem::Continuation(data) => format!("T{}", data.code().remaining_bits() / 8),
            StackItem::Builder(data) => {
                format!("B{}-{}", data.length_in_bits(), data.references().len())
            }
            StackItem::Slice(data) => {
                format!("S{}-{}", data.remaining_bits(), data.remaining_references())
            }
            StackItem::Tuple(data) => match data.len() {
                0 => "[]".to_string(),
                len => format!("[@{}]", len),
            },
        };
        result += &string;
        result += " ";
    }
    *prev_stack = stack.clone();
    result
}
