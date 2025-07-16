// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use account_inbox::iter::iterator::MessagesRangeIterator;
use anyhow::ensure;
use http_server::ExtMsgFeedback;
use http_server::ExtMsgFeedbackList;
use http_server::FeedbackError;
use http_server::FeedbackErrorCode;
use indexset::BTreeMap;
use telemetry_utils::mpsc::instrumented_channel;
use telemetry_utils::mpsc::InstrumentedReceiver;
use tracing::instrument;
use tracing::trace_span;
use tvm_block::Account;
use tvm_block::AccountStatus;
use tvm_block::AddSub;
use tvm_block::Augmentation;
use tvm_block::BlkPrevInfo;
use tvm_block::Block;
use tvm_block::BlockExtra;
use tvm_block::BlockInfo;
use tvm_block::CommonMsgInfo;
use tvm_block::ComputeSkipReason;
use tvm_block::Deserializable;
use tvm_block::EnqueuedMsg;
use tvm_block::ExtBlkRef;
use tvm_block::GetRepresentationHash;
use tvm_block::HashUpdate;
use tvm_block::HashmapAugType;
use tvm_block::InMsg;
use tvm_block::MerkleUpdate;
use tvm_block::Message;
use tvm_block::MsgEnvelope;
use tvm_block::OutMsg;
use tvm_block::OutMsgQueueKey;
use tvm_block::Serializable;
use tvm_block::ShardAccount;
use tvm_block::ShardIdent;
use tvm_block::ShardStateUnsplit;
use tvm_block::StateInit;
use tvm_block::TrComputePhase;
use tvm_block::TrComputePhaseVm;
use tvm_block::Transaction;
use tvm_block::TransactionDescr;
use tvm_block::TransactionDescrOrdinary;
use tvm_block::ValueFlow;
use tvm_executor::BlockchainConfig;
use tvm_executor::ExecuteParams;
use tvm_executor::ExecutorError;
use tvm_executor::OrdinaryTransactionExecutor;
use tvm_executor::TransactionExecutor;
use tvm_types::AccountId;
use tvm_types::Cell;
use tvm_types::UInt256;
use tvm_types::UsageTree;
use tvm_vm::executor::Engine;
use tvm_vm::executor::EngineTraceInfo;

use super::ActiveThread;
use super::BlockBuilder;
use super::EngineTraceInfoData;
use super::PreparedBlock;
use super::ThreadResult;
use crate::block::postprocessing::postprocess;
use crate::block::producer::builder::trace::simple_trace_callback;
use crate::block::producer::errors::verify_error;
use crate::block::producer::errors::BP_DID_NOT_PROCESS_ALL_MESSAGES_FROM_PREVIOUS_BLOCK;
use crate::block::producer::execution_time::ExecutionTimeLimits;
use crate::block_keeper_system::epoch::decode_epoch_data;
use crate::block_keeper_system::epoch::decode_preepoch_data;
use crate::block_keeper_system::BlockKeeperSetChange;
use crate::creditconfig::abi::DAPP_CONFIG_TVC;
use crate::creditconfig::abi::DAPP_ROOT_ADDR;
use crate::creditconfig::dappconfig::calculate_dapp_config_address;
use crate::creditconfig::dappconfig::decode_dapp_config_data;
use crate::creditconfig::dappconfig::decode_message_config;
use crate::creditconfig::dappconfig::get_available_balance_from_config;
use crate::external_messages::Stamp;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::message_storage::MessageDurableStorage;
use crate::repository::accounts::AccountsRepository;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::types::account::WrappedAccount;
use crate::types::thread_message_queue::account_messages_iterator::AccountMessagesIterator;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AccountAddress;
use crate::types::AccountRouting;
use crate::types::BlockEndLT;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::DAppIdentifier;
use crate::types::ThreadIdentifier;

impl BlockBuilder {
    /// Initialize BlockBuilder
    #[allow(clippy::too_many_arguments)]
    pub fn with_params(
        thread_id: ThreadIdentifier,
        mut initial_optimistic_state: OptimisticStateImpl,
        gen_utime_ms: u64,
        block_gas_limit: u64,
        rand_seed: Option<UInt256>,
        control_rx_stop: Option<InstrumentedReceiver<()>>,
        accounts_repository: AccountsRepository,
        block_keeper_epoch_code_hash: String,
        block_keeper_preepoch_code_hash: String,
        parallelization_level: usize,
        produced_internal_messages_to_other_threads: HashMap<
            AccountRouting,
            Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
        >,
        metrics: Option<BlockProductionMetrics>,
    ) -> anyhow::Result<Self> {
        // Replace shard state cell with Usage Cell to track merkle tree visits
        let usage_tree =
            UsageTree::with_params(initial_optimistic_state.get_shard_state_as_cell(), true);
        let usage_cell = usage_tree.root_cell();
        let shard_state = Arc::new(
            ShardStateUnsplit::construct_from_cell(usage_cell)
                .map_err(|e| anyhow::format_err!("Failed to construct shard state {e}"))?,
        );
        let accounts = shard_state
            .read_accounts()
            .map_err(|e| anyhow::format_err!("Failed to read accounts: {e}"))?;
        // let out_queue_info = shard_state
        //     .read_out_msg_queue_info()
        //     .map_err(|e| anyhow::format_err!("Failed to read out msgs queue: {e}"))?;
        let seq_no = shard_state.seq_no() + 1;

        let prev_block_info = initial_optimistic_state.get_block_info();
        let start_lt = prev_block_info.prev1().map_or(0, |p| p.end_lt) + 1;
        let rand_seed = rand_seed.unwrap_or(UInt256::rand()); // we don't need strict randomization like real node

        let mut block_info = BlockInfo::default();
        block_info.set_shard(shard_state.shard().clone());
        block_info.set_seq_no(seq_no).unwrap();
        block_info.set_prev_stuff(false, prev_block_info).unwrap();
        block_info.set_gen_utime_ms(gen_utime_ms);
        block_info.set_start_lt(start_lt);

        let base_config_stateinit = StateInit::construct_from_bytes(DAPP_CONFIG_TVC)
            .map_err(|e| anyhow::format_err!("Failed to construct DAPP config tvc: {e}"))?;

        let dapp_id_table = initial_optimistic_state.get_dapp_id_table().clone();

        let builder = BlockBuilder {
            thread_id,
            shard_state,
            // out_queue_info, // TODO: Change to ThreadMessageQueueState, get it from Optimistic
            from_prev_blk: accounts.full_balance().clone(),
            in_msg_descr: Default::default(),
            initial_accounts: accounts.clone(),
            accounts,
            block_info,
            rand_seed,
            start_lt,
            end_lt: start_lt + 1,
            copyleft_rewards: Default::default(),
            block_gas_limit,
            account_blocks: Default::default(),
            total_gas_used: 0,
            control_rx_stop,
            usage_tree,
            initial_optimistic_state,
            block_keeper_epoch_code_hash,
            block_keeper_preepoch_code_hash,
            parallelization_level,
            block_keeper_set_changes: vec![],
            base_config_stateinit,
            dapp_credit_map: Default::default(),
            new_messages: Default::default(),
            out_msg_descr: Default::default(),
            transaction_traces: Default::default(),
            tx_cnt: 0,
            dapp_minted_map: Default::default(),
            dapp_id_table,
            accounts_repository,
            consumed_internal_messages: Default::default(),
            produced_internal_messages_to_the_current_thread: Default::default(),
            produced_internal_messages_to_other_threads,
            accounts_that_changed_their_dapp_id: Default::default(),
            metrics,
            is_stop_requested: false,
        };

        Ok(builder)
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
    ) -> anyhow::Result<(Transaction, u64, Option<Vec<EngineTraceInfoData>>, u128, bool)> {
        let lt = execute_params.last_tr_lt.clone();
        let last_lt = lt.load(Ordering::Relaxed);
        let block_unixtime = execute_params.block_unixtime;
        let vm_execution_is_block_related = execute_params.vm_execution_is_block_related.clone();

        #[cfg(feature = "timing")]
        let start = std::time::Instant::now();
        tracing::trace!(target: "builder", "execute_with_libs_and_params: {} {msg:?}", msg.hash().unwrap().to_hex_string());
        let mut is_ext_message = msg.is_inbound_external();
        let result = executor.execute_with_libs_and_params(Some(msg), acc_root, execute_params);
        tracing::trace!(target: "builder", "Execution result {:?}", result);
        #[cfg(feature = "timing")]
        tracing::trace!(target: "builder", "Execution time {} ms", start.elapsed().as_millis());
        tracing::trace!(target: "builder",
            "vm_execution_is_block_related: {}",
            vm_execution_is_block_related.lock().unwrap()
        );
        match result {
            Ok((mut transaction, minted_shell)) => {
                is_ext_message = false;
                let trace = if transaction
                    .read_description()
                    .map_err(|e| {
                        anyhow::format_err!("Failed to read transaction description: {e}")
                    })?
                    .is_aborted()
                {
                    Some(trace.pop_iter().collect::<Vec<EngineTraceInfoData>>())
                        .filter(|trace| !trace.is_empty())
                } else {
                    None
                };
                transaction.set_prev_trans_hash(last_trans_hash);
                transaction.set_prev_trans_lt(last_trans_lt);
                Ok((transaction, lt.load(Ordering::Relaxed), trace, minted_shell, is_ext_message))
            }
            Err(err) => {
                if let Some(ExecutorError::TerminationDeadlineReached) = err.downcast_ref() {
                    anyhow::bail!(ExecutorError::TerminationDeadlineReached);
                }
                let old_hash = acc_root.repr_hash();
                let mut account = Account::construct_from_cell(acc_root.clone())
                    .map_err(|e| anyhow::format_err!("Failed to construct account: {e}"))?;
                let lt = std::cmp::max(
                    account.last_tr_time().unwrap_or(0),
                    std::cmp::max(last_lt, msg.lt().unwrap_or(0) + 1),
                );
                account.set_last_tr_time(lt);
                *acc_root = account
                    .serialize()
                    .map_err(|e| anyhow::format_err!("Failed to serialize account: {e}"))?;
                let mut transaction = Transaction::with_account_and_message(&account, msg, lt)
                    .map_err(|e| anyhow::format_err!("Failed to create transaction: {e}"))?;
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
                    _ => {
                        return Err(err).map_err(|e| anyhow::format_err!("Execution error: {e}"))?
                    }
                }
                transaction
                    .write_description(&TransactionDescr::Ordinary(description))
                    .map_err(|e| anyhow::format_err!("Failed to write tx description: {e}"))?;
                let state_update = HashUpdate::with_hashes(old_hash, acc_root.repr_hash());
                transaction
                    .write_state_update(&state_update)
                    .map_err(|e| anyhow::format_err!("Failed to write tx state update: {e}"))?;
                let trace = Some(trace.pop_iter().collect::<Vec<EngineTraceInfoData>>())
                    .filter(|trace| !trace.is_empty());
                transaction.set_prev_trans_hash(last_trans_hash);
                transaction.set_prev_trans_lt(last_trans_lt);
                Ok((transaction, lt, trace, 0, is_ext_message))
            }
        }
    }

    pub(super) fn after_transaction(&mut self, thread_result: ThreadResult) -> anyhow::Result<()> {
        let transaction = thread_result.transaction;
        let is_tx_aborted = transaction
            .read_description()
            .map_err(|e| anyhow::format_err!("Failed to read tx description: {e}"))?
            .is_aborted();

        if is_tx_aborted {
            // This metric counts ALL aborted transactions.
            self.metrics.as_ref().inspect(|m| m.report_tx_aborted(&self.thread_id));

            if thread_result.in_msg_is_ext {
                // This metric counts only external aborted transactions
                self.metrics.as_ref().inspect(|m| m.report_ext_tx_aborted(&self.thread_id));
                tracing::trace!(target: "builder", "Ext message was aborted, do not process resulting tx");
                return Ok(());
            }
        }

        let max_lt = thread_result.lt;
        let trace = thread_result.trace;
        let acc_root = thread_result.account_root;
        let acc_id = thread_result.account_id;
        self.tx_cnt += 1;

        if let Some(gas_used) = transaction.gas_used() {
            self.total_gas_used += gas_used;
        }
        tracing::trace!(target: "builder",
            "Transaction {:?} {}",
            transaction.hash(),
            is_tx_aborted,
        );

        self.end_lt = std::cmp::max(self.end_lt, max_lt);

        let tr_cell = transaction
            .serialize()
            .map_err(|e| anyhow::format_err!("Failed to serialize tx: {e}"))?;
        if let Some(trace) = trace {
            self.transaction_traces.insert(tr_cell.repr_hash(), trace);
        }

        tracing::trace!(target: "builder", "Transaction ID {:x}", tr_cell.repr_hash());
        tracing::trace!(target: "builder",
            "Transaction aborted: {}",
            transaction.read_description().map_err(|e| anyhow::format_err!("Failed to read tx description: {e}"))?.is_aborted()
        );

        let acc = Account::construct_from_cell(acc_root.clone())
            .map_err(|e| anyhow::format_err!("Failed to construct account: {e}"))?;

        if let Some(acc_code_hash) = acc.get_code_hash() {
            let code_hash_str = acc_code_hash.as_hex_string();
            if code_hash_str == self.block_keeper_epoch_code_hash
                || code_hash_str == self.block_keeper_preepoch_code_hash
            {
                tracing::info!(target: "builder", "after_transaction tx statuses: {:?} {:?}", transaction.orig_status, transaction.end_status);
                if transaction.orig_status == AccountStatus::AccStateNonexist {
                    if code_hash_str == self.block_keeper_epoch_code_hash {
                        tracing::info!(target: "builder", "Epoch contract was deployed");
                        if let Some((id, block_keeper_data)) = decode_epoch_data(&acc)
                            .map_err(|e| anyhow::format_err!("Failed to decode epoch data: {e}"))?
                        {
                            self.block_keeper_set_changes.push(
                                BlockKeeperSetChange::BlockKeeperAdded((id, block_keeper_data)),
                            );
                        } else {
                            anyhow::bail!("Failed to decode epoch contract");
                        }
                    }
                    if code_hash_str == self.block_keeper_preepoch_code_hash {
                        tracing::info!(target: "builder", "PreEpoch contract was deployed");
                        if let Some((id, block_keeper_data)) =
                            decode_preepoch_data(&acc).map_err(|e| {
                                anyhow::format_err!("Failed to decode preepoch data: {e}")
                            })?
                        {
                            self.block_keeper_set_changes.push(
                                BlockKeeperSetChange::FutureBlockKeeperAdded((
                                    id,
                                    block_keeper_data,
                                )),
                            );
                        } else {
                            anyhow::bail!("Failed to decode preepoch contract");
                        }
                    }
                }
            }
        }

        let acc_id_uint = UInt256::try_from(acc_id.clone())
            .map_err(|e| anyhow::format_err!("Failed to convert account ID: {e}"))?;
        let account_address = AccountAddress(acc_id.clone());
        // let initial_account_routing = AccountRouting(thread_result.initial_dapp_id.clone().unwrap_or(DAppIdentifier(account_address.clone())), account_address.clone());
        if acc.is_none() {
            tracing::trace!(target: "builder", "Remove account from shard state: {}", acc_id.to_hex_string());
            self.accounts
                .remove(&acc_id_uint)
                .map_err(|e| anyhow::format_err!("Failed to remove account: {e}"))?;
            self.dapp_id_table.insert(account_address.clone(), (None, BlockEndLT(self.end_lt)));
            let account_routing =
                AccountRouting(DAppIdentifier(account_address.clone()), account_address.clone());
            self.accounts_that_changed_their_dapp_id
                .entry(acc_id.clone())
                .or_default()
                .push((account_routing, None));
        } else {
            let mut result_dapp_id = thread_result.initial_dapp_id.clone();
            if transaction.end_status == AccountStatus::AccStateActive
                && (transaction.orig_status == AccountStatus::AccStateNonexist
                    || transaction.orig_status == AccountStatus::AccStateUninit)
            {
                if let Some(header) = thread_result.in_msg.int_header() {
                    result_dapp_id = header
                        .src_dapp_id
                        .as_ref()
                        .map(|dapp_id| DAppIdentifier(AccountAddress(dapp_id.into())));
                } else {
                    result_dapp_id = Some(DAppIdentifier(AccountAddress(acc_id.clone())));
                }
            };

            let shard_acc = ShardAccount::with_account_root(
                acc_root,
                tr_cell.repr_hash(),
                transaction.logical_time(),
                result_dapp_id
                    .clone()
                    .map(|dapp_id| dapp_id.0 .0.try_into())
                    .transpose()
                    .map_err(|e| anyhow::format_err!("Failed to convert dapp_id: {e}"))?,
            );

            if result_dapp_id != thread_result.initial_dapp_id {
                tracing::trace!(target: "builder", "Update dapp id table for {:?}: {:?}", acc_id, result_dapp_id);
                self.dapp_id_table.insert(
                    AccountAddress(acc_id.clone()),
                    (result_dapp_id.clone(), BlockEndLT(self.end_lt)),
                );
                let new_dapp_id = result_dapp_id.expect(
                    "DApp Id has changed and account was not deleted, It should not be None",
                );
                let new_account_routing = AccountRouting(new_dapp_id, account_address.clone());
                self.accounts_that_changed_their_dapp_id.entry(acc_id.clone()).or_default().push((
                    new_account_routing,
                    Some(WrappedAccount {
                        account: shard_acc.clone(),
                        account_id: acc_id_uint.clone(),
                        aug: shard_acc
                            .aug()
                            .map_err(|e| anyhow::format_err!("Failed to get account aug: {e}"))?,
                    }),
                ));
            } else if self.accounts_that_changed_their_dapp_id.contains_key(&acc_id) {
                let history = self.accounts_that_changed_their_dapp_id.get_mut(&acc_id).unwrap();
                let prev_routing = history.last().expect("Can't be empty").0.clone();
                history.push((
                    prev_routing,
                    Some(WrappedAccount {
                        account: shard_acc.clone(),
                        account_id: acc_id_uint.clone(),
                        aug: shard_acc
                            .aug()
                            .map_err(|e| anyhow::format_err!("Failed to get account aug: {e}"))?,
                    }),
                ));
            }
            tracing::trace!(target: "builder", "Update account data: {}", acc_id.to_hex_string());
            self.accounts
                .insert(&acc_id_uint, &shard_acc)
                .map_err(|e| anyhow::format_err!("Failed to save account: {e}"))?;
        }
        if let Err(err) = self.add_raw_transaction(transaction, tr_cell) {
            tracing::warn!(target: "builder", "Error append transaction {:?}", err);
            // TODO log error, write to transaction DB about error
        }

        if let Some(dapp_id) = thread_result.initial_dapp_id.clone() {
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

    fn get_available_balance(
        &mut self,
        acc_id: AccountId,
    ) -> anyhow::Result<(i128, Option<DAppIdentifier>)> {
        let mut available_balance = 0;
        let account_address = AccountAddress(acc_id);
        // TODO use dapp_id_table here
        let dapp_id_opt = if let Some((dapp_id, _)) = self.dapp_id_table.get(&account_address) {
            dapp_id.clone()
        } else {
            None
        };
        if let Some(dapp_id) = dapp_id_opt.clone() {
            if !self.dapp_credit_map.contains_key(&dapp_id.clone()) {
                let addr = calculate_dapp_config_address(
                    dapp_id.clone(),
                    self.base_config_stateinit.clone(),
                )
                .map_err(|e| anyhow::format_err!("Failed to calculate dapp config address: {e}"))?;
                let acc_id = AccountId::from(addr);
                if let Some(acc) = self.get_account(&AccountId::from(acc_id))? {
                    let acc_d = acc
                        .read_account()
                        .map_err(|e| anyhow::format_err!("Failed to construct account: {e}"))?
                        .as_struct()
                        .map_err(|e| anyhow::format_err!("Failed to construct account: {e}"))?;
                    let data = decode_dapp_config_data(&acc_d)?;

                    if let Some(configdata) = data {
                        available_balance = get_available_balance_from_config(configdata.clone());
                        self.dapp_credit_map.insert(dapp_id, configdata);
                    }
                }
            } else {
                available_balance =
                    get_available_balance_from_config(self.dapp_credit_map[&dapp_id].clone());
            }
        }

        Ok((available_balance, dapp_id_opt))
    }

    fn get_account(&mut self, acc_id: &AccountId) -> anyhow::Result<Option<ShardAccount>> {
        match self
            .accounts
            .account(acc_id)
            .map_err(|e| anyhow::format_err!("Failed to get account: {e}"))?
        {
            Some(mut acc) => {
                if acc.is_external() {
                    tracing::trace!(target: "builder", "account is external {}", acc_id.to_hex_string());
                    let acc_id = acc_id
                        .clone()
                        .try_into()
                        .map_err(|e| anyhow::format_err!("Failed to convert address: {e}"))?;
                    let root = match self.initial_optimistic_state.cached_accounts.get(&acc_id) {
                        Some((_, acc_root)) => acc_root.clone(),
                        None => self.accounts_repository.load_account(
                            &acc_id,
                            acc.last_trans_hash(),
                            acc.last_trans_lt(),
                        )?,
                    };
                    if root.repr_hash() != acc.account_cell().repr_hash() {
                        return Err(anyhow::format_err!("External account cell hash mismatch"));
                    }
                    acc.set_account_cell(root);
                    self.accounts
                        .insert(&acc_id, &acc)
                        .map_err(|e| anyhow::format_err!("Failed to save account: {e}"))?;
                    self.initial_accounts
                        .insert(&acc_id, &acc)
                        .map_err(|e| anyhow::format_err!("Failed to save initial account: {e}"))?;
                }
                Ok(Some(acc))
            }
            None => Ok(None),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn execute(
        &mut self,
        message: Message,
        blockchain_config: &BlockchainConfig,
        acc_id: &AccountId,
        block_unixtime: u32,
        block_lt: u64,
        check_messages_map: &mut Option<HashMap<AccountId, BTreeMap<u64, UInt256>>>,
        time_limits: &ExecutionTimeLimits,
    ) -> anyhow::Result<ActiveThread> {
        let message_hash = message.hash().unwrap();
        tracing::debug!(target: "builder", "Start msg execution: {:?}", message_hash);
        #[cfg(feature = "timing")]
        let start = std::time::Instant::now();
        let shard_acc = self.get_account(acc_id)?;
        let (available_balance, dapp_id_opt) = self
            .get_available_balance(acc_id.clone())
            .map_err(|e| anyhow::format_err!("Failed to get available credit: {e}"))?;
        tracing::debug!(target: "builder", "Execute available credit: {}", available_balance);
        tracing::debug!(target: "builder", "Read account: {}", acc_id.to_hex_string());
        let shard_acc = shard_acc.unwrap_or_default();
        #[cfg(feature = "timing")]
        tracing::trace!(target: "builder", "Execute: read account time {} ms", start.elapsed().as_millis());
        let mut acc_root = shard_acc.account_cell();
        let executor = OrdinaryTransactionExecutor::new((*blockchain_config).clone());

        let mut last_lt = std::cmp::max(self.end_lt, shard_acc.last_trans_lt() + 1);
        if let Some(check_messages_map) = check_messages_map.as_mut() {
            let Some(messages) = check_messages_map.get_mut(acc_id) else {
                anyhow::bail!("Unexpected message destination for verify block");
            };
            let Some((lt, next_message_hash)) = messages.pop_first() else {
                anyhow::bail!("Unexpected message for verify block");
            };
            ensure!(next_message_hash == message_hash, "Wrong message order for verify block");
            last_lt = lt;
        }
        let lt = Arc::new(AtomicU64::new(last_lt));
        let trace = Arc::new(lockfree::queue::Queue::new());
        let vm_execution_is_block_related = Arc::new(Mutex::new(false));
        let acc_id = acc_id.clone();

        {
            #[cfg(feature = "timing")]
            let account_start = std::time::Instant::now();
            if let Ok(account) = Account::construct_from_cell(acc_root.clone()) {
                if let Some(code_hash) = account.get_code_hash() {
                    let code_hash_str = code_hash.to_hex_string();
                    tracing::trace!(target: "builder", "Start acc code hash: {}", code_hash_str);
                    // Note: we assume that epoch contract can't be deployed by any other way than by the block keeper system
                    if code_hash_str == self.block_keeper_epoch_code_hash {
                        tracing::trace!(target: "builder", "Message src: {:?}, dst: {:?}", message.src(), message.dst());
                        if message.src() == message.dst() {
                            tracing::trace!(target: "builder", "Epoch destroy message");
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
                            } else {
                                anyhow::bail!("Failed to decode epoch contract");
                            }
                        }
                    }
                }
                #[cfg(feature = "timing")]
                tracing::trace!(target: "builder", "Start acc code hash elapsed: {}", account_start.elapsed().as_millis());
            }
        }

        let mut is_same_thread = false;
        if let Some(acc_id) = message.get_int_src_account_id() {
            is_same_thread =
                self.initial_optimistic_state.does_account_belong_to_the_state(&acc_id)?;
        }
        if message.ext_in_header().is_some() {
            is_same_thread = true;
        }
        let termination_deadline = time_limits.block_deadline();
        let execution_timeout = time_limits.get_message_timeout(&message_hash);
        let execute_params = if cfg!(feature = "tvm_tracing") {
            let trace_copy = trace.clone();
            let callback = move |engine: &Engine, info: &EngineTraceInfo| {
                trace_copy.push(EngineTraceInfoData::from(info));
                simple_trace_callback(engine, info);
            };
            ExecuteParams {
                block_unixtime,
                block_lt,
                last_tr_lt: Arc::clone(&lt),
                seed_block: self.rand_seed.clone(),
                debug: true,
                signature_id: self.shard_state.global_id(),
                trace_callback: Some(Arc::new(callback)),
                vm_execution_is_block_related: vm_execution_is_block_related.clone(),
                seq_no: self.block_info.seq_no(),
                dapp_id: shard_acc.get_dapp_id().cloned(),
                available_credit: available_balance,
                is_same_thread_id: is_same_thread,
                termination_deadline,
                execution_timeout,
                wasm_binary_root_path: "./config/wasm".to_owned(),
                ..Default::default()
            }
        } else {
            ExecuteParams {
                block_unixtime,
                block_lt,
                last_tr_lt: Arc::clone(&lt),
                seed_block: self.rand_seed.clone(),
                debug: false,
                signature_id: self.shard_state.global_id(),
                trace_callback: None,
                vm_execution_is_block_related: vm_execution_is_block_related.clone(),
                seq_no: self.block_info.seq_no(),
                dapp_id: shard_acc.get_dapp_id().cloned(),
                available_credit: available_balance,
                is_same_thread_id: is_same_thread,
                termination_deadline,
                execution_timeout,
                wasm_binary_root_path: "./config/wasm".to_owned(),
                ..Default::default()
            }
        };

        let message_clone = message.clone();
        let (result_tx, result_rx) = instrumented_channel::<anyhow::Result<ThreadResult>>(
            self.metrics.clone(),
            crate::helper::metrics::PRODUCE_THREAD_RESULT_CHANNEL,
        );
        rayon::spawn(move || {
            tracing::debug!(target: "builder", "Executing message {} {:?}", message_hash.to_hex_string(), message);
            let res = Self::try_prepare_transaction(
                &executor,
                &mut acc_root,
                &message,
                shard_acc.last_trans_hash().clone(),
                shard_acc.last_trans_lt(),
                execute_params,
                trace,
            );
            #[cfg(feature = "timing")]
            tracing::trace!(target: "builder", "Execute: total time {} ms, available_balance {}, result with minted {:?}", start.elapsed().as_millis(), available_balance, res);
            let _ = result_tx.send(res.map(|(tx, lt, trace, minted_shell, is_ext_message)| {
                ThreadResult {
                    transaction: tx,
                    lt,
                    trace,
                    account_root: acc_root,
                    account_id: acc_id,
                    minted_shell,
                    initial_dapp_id: dapp_id_opt,
                    in_msg_is_ext: is_ext_message,
                    in_msg: message,
                }
            }));
        });

        Ok(ActiveThread {
            result_rx,
            message: message_clone,
            vm_execution_is_block_related,
            block_production_was_finished: Arc::new(Mutex::new(false)),
        })
    }

    fn execute_internal_messages(
        &mut self,
        blockchain_config: &BlockchainConfig,
        check_messages_map: &mut Option<HashMap<AccountId, BTreeMap<u64, UInt256>>>,
        _white_list_of_slashing_messages_hashes: HashSet<UInt256>, // TODO: check usage
        message_queue: ThreadMessageQueueState,
        message_db: MessageDurableStorage,
        time_limits: &ExecutionTimeLimits,
    ) -> anyhow::Result<bool> {
        let (block_unixtime, block_lt) = self.at_and_lt();
        // let out_queue = self.out_queue_info.out_queue().clone();
        // let msg_count = out_queue
        //     .len()
        //     .map_err(|e| anyhow::format_err!("Failed to get msgs queue length: {e}"))?;
        let mut block_full = false;
        // let (
        //     mut sorted,
        //     mut active_int_destinations,
        //     verify_block_contains_missing_messages_from_prev_state,
        // ) = trace_span!("sort internal messages").in_scope(|| {
        //     // Internal messages should be executed in the order right. Sort them by create lt.
        //     let mut sorted = Vec::with_capacity(msg_count);
        //     let mut active_int_destinations = HashSet::new();
        //
        //     // TODO: this flag is unused, fix it
        //     let verify_block_contains_missing_messages_from_prev_state = false;
        //
        //     // TODO: check if iteration can be reworked to remove key management. iter_slices can be possibly used to prevent extra conversions
        //     for out in out_queue.iter() {
        //         let (key, mut slice) = out.map_err(|e| {
        //             anyhow::format_err!("Failed to get data from out msgs queue: {e}")
        //         })?;
        //         let key = key
        //             .into_cell()
        //             .map_err(|e| anyhow::format_err!("Failed to serialize msg key: {e}"))?;
        //         let (enqueued_message, create_lt) = OutMsgQueue::value_aug(&mut slice)
        //             .map_err(|e| anyhow::format_err!("Failed to calculate msg aug: {e}"))?;
        //         let message = enqueued_message
        //             .read_out_msg()
        //             .map_err(|e| anyhow::format_err!("Failed to read enqueued message: {e}"))?
        //             .read_message()
        //             .map_err(|e| anyhow::format_err!("Failed to read message: {e}"))?;
        //
        //         // Check out messages from previous state
        //         if let Some(acc_id) = message.int_dst_account_id() {
        //             // leave only messages with internal destination address from this thread
        //             if active_threads.iter().any(|(k, _active_thread)| k == &key) {
        //                 active_int_destinations.insert(acc_id.clone());
        //                 continue;
        //             }
        //             let does_belong_to_the_current_state =
        //                 self.initial_optimistic_state.does_account_belong_to_the_state(&acc_id)?;
        //             if does_belong_to_the_current_state {
        //                 if let Some(msg_set) = check_messages_map {
        //                     if !msg_set.contains_key(&message.hash().unwrap()) {
        //                         // TODO: check this flag
        //                         // verify_block_contains_missing_messages_from_prev_state = true;
        //                         tracing::trace!(target: "builder",
        //                             "Skip message for verify block: {}",
        //                             message.hash().unwrap().to_hex_string()
        //                         );
        //                         continue;
        //                     }
        //                 }
        //                 sorted.push((key, message, acc_id, create_lt));
        //             } else {
        //                 let message_hash = message.hash().unwrap();
        //                 tracing::trace!(target: "builder",
        //                     "Remove message from state: {}",
        //                     message_hash.to_hex_string()
        //                 );
        //                 // If message destination doesn't belong to the current thread, remove it from the state
        //                 self.out_queue_info
        //                     .out_queue_mut()
        //                     .remove(
        //                         SliceData::load_cell(key)
        //                             .map_err(|e| anyhow::format_err!("Failed to load key: {e}"))?,
        //                     )
        //                     .map_err(|e| {
        //                         anyhow::format_err!("Failed to remove message from queue: {e}")
        //                     })?;
        //                 if white_list_of_slashing_messages_hashes.contains(&message_hash) {
        //                     let msg_cell = message.serialize().map_err(|e| {
        //                         anyhow::format_err!("Failed to serialize message: {e}")
        //                     })?;
        //                     let out_msg = OutMsg::external(msg_cell.clone(), Cell::default());
        //                     tracing::debug!(
        //                         target: "builder",
        //                         "Inserting new message with {:?}",
        //                         message
        //                     );
        //                     self.out_msg_descr
        //                         .set(
        //                             &msg_cell.repr_hash(),
        //                             &out_msg,
        //                             &out_msg.aug().map_err(|e| {
        //                                 anyhow::format_err!("Failed to get out message aug: {e}")
        //                             })?,
        //                         )
        //                         .map_err(|e| {
        //                             anyhow::format_err!("Failed to set out msg descr: {e}")
        //                         })?;
        //                 }
        //             }
        //         } else {
        //             tracing::trace!(target: "builder",
        //                 "Remove ext message from state: {}",
        //                 message.hash().unwrap().to_hex_string()
        //             );
        //             // If message destination is not internal, remove it from the state
        //             self.out_queue_info
        //                 .out_queue_mut()
        //                 .remove(
        //                     SliceData::load_cell(key)
        //                         .map_err(|e| anyhow::format_err!("Failed to load key: {e}"))?,
        //                 )
        //                 .map_err(|e| {
        //                     anyhow::format_err!("Failed to remove message from queue: {e}")
        //                 })?;
        //         }
        //     }
        //     // Sort internal messages by creation time
        //     sorted.sort_by(|a, b| a.3.cmp(&b.3));
        //
        //     tracing::Span::current().record("sorted.len", sorted.len() as i64);
        //     Ok::<_, anyhow::Error>((
        //         sorted,
        //         active_int_destinations,
        //         verify_block_contains_missing_messages_from_prev_state,
        //     ))
        // })?;

        trace_span!("internal messages execution")
            .in_scope(|| {
                #[cfg(feature = "timing")]
                let start = std::time::Instant::now();

                let mut active_threads = vec![];
                let mut executed_int_messages_cnt = 0;
                // if there are any internal messages start parallel execution
               /* let queue: crate::types::thread_message_queue::ThreadMessageQueueState = match is_high_priority {
                    false => self.initial_optimistic_state.messages.clone(),
                    true => self.initial_optimistic_state.high_priority_messages.clone(),
                };*/
                let mut internal_messages_iter = message_queue.iter(&message_db);

                let active_int_destinations = parking_lot::Mutex::new(HashSet::new());
                let mut started_accounts: HashMap<AccountId, MessagesRangeIterator<MessageIdentifier, Arc<WrappedMessage>, MessageDurableStorage>> = HashMap::new();

                let mut get_next_int_message = |check_messages_map: &Option<HashMap<AccountId, BTreeMap<u64, UInt256>>>| {
                Ok::<_, anyhow::Error>(loop {
                    {
                        if let Some(checker) = check_messages_map.as_ref() {
                            if checker.is_empty() {
                                break None;
                            }
                        }

                        let mut guarded = active_int_destinations.lock();
                        let started_keys: Vec<AccountId> = started_accounts.keys().cloned().collect();
                        for acc_id in started_keys {
                            if !guarded.contains(&acc_id) {
                                let next_message = loop {
                                    let value = started_accounts.get_mut(&acc_id).unwrap();
                                    match value.next() {
                                        Some(Ok((message, key))) => {
                                            tracing::trace!(target: "builder", "get_next_int_message: Got message from queue iter: {:?}", message);
                                            let Some(acc_id) = message.message.int_dst_account_id() else {
                                                // TODO: We expect only messages with internal destination, skip it
                                                // check if it should be deleted from out_queue_info
                                                tracing::trace!(target: "builder", "get_next_int_message: skip ext: {:?}", message);
                                                continue;
                                            };
                                            if let Some(checker) = check_messages_map.as_ref() {
                                                if let Some(acc_messages) = checker.get(&acc_id) {
                                                    let Some((_, next_message)) = acc_messages.first_key_value() else {
                                                        // For verify block we assume iter returns messages in the same order
                                                        break None;
                                                    };
                                                    ensure!(*next_message == message.message.hash().unwrap(), "Wrong int messages order");
                                                } else {
                                                    // For verify block we assume iter returns messages in the same order
                                                    break None;
                                                }
                                            }
                                            tracing::trace!(target: "builder", "get_next_int_message: Got message from queue iter: {:?} {:?}", acc_id, message);
                                            // TODO: check that message destination belongs to this thread
                                            guarded.insert(acc_id.clone());

                                            tracing::trace!(target: "builder", "get_next_int_message: return: {:?}", message);
                                            break Some((message.message.clone(), key));
                                        }
                                        Some(Err(_)) => {
                                            // TODO: we have received an error while fetching a message, skip it for now, but it needs check
                                            // TODO: print error, it needs some traits
                                            tracing::warn!("Failed to get next internal message");
                                            break None;
                                        }
                                        None => {
                                            tracing::trace!(target: "builder", "internal message queue is empty");
                                            break None;
                                        }
                                    }
                                };
                                if next_message.is_none() {
                                    // Account iter is empty
                                    started_accounts.remove(&acc_id);
                                } else {
                                    return Ok(next_message);
                                }
                            }
                        }
                    }
                    match internal_messages_iter.next() {
                        Some(mut account_msgs_iter) => {
                            match account_msgs_iter.next() {
                                Some(Ok((message, key))) => {
                                    tracing::trace!(target: "builder", "get_next_int_message: Got message from queue iter: {:?}", message);
                                    let Some(acc_id) = message.message.int_dst_account_id() else {
                                        // TODO: We expect only messages with internal destination, skip it
                                        // check if it should be deleted from out_queue_info
                                        tracing::trace!(target: "builder", "get_next_int_message: skip ext: {:?}", message);
                                        continue;
                                    };
                                    if let Some(checker) = check_messages_map.as_ref() {
                                        if let Some(acc_messages) = checker.get(&acc_id) {
                                            let Some((_, next_message)) = acc_messages.first_key_value() else {
                                                // For verify block we assume iter returns messages in the same order
                                                continue;
                                            };
                                            ensure!(*next_message == message.message.hash().unwrap(), "Wrong int messages order");
                                        } else {
                                            // For verify block we assume iter returns messages in the same order
                                            continue;
                                        }
                                    }
                                    tracing::trace!(target: "builder", "get_next_int_message: Got message from queue iter: {:?} {:?}", acc_id, message);
                                    // TODO: check that message destination belongs to this thread
                                    let mut guarded = active_int_destinations.lock();
                                    if guarded.contains(&acc_id) {
                                        // TODO: for now assume that accounts should not repeat it iter
                                        unreachable!("Iter should not return the same account several times");
                                        // TODO: skip account that is already executed
                                        // tracing::trace!(target: "builder", "get_next_int_message: Skip due to account busy: {:?} {:?}", first_acc_id, first_message);
                                        // skipped_messages.entry(acc_id).or_default().push((first_message.message.clone(), key));
                                        // continue;
                                    }
                                    guarded.insert(acc_id.clone());
                                    started_accounts.insert(acc_id, account_msgs_iter);

                                    tracing::trace!(target: "builder", "get_next_int_message: return: {:?}", message);
                                    break Some((message.message.clone(), key));
                                }
                                Some(Err(_)) => {
                                    // TODO: we have received an error while fetching a message, skip it for now, but it needs check
                                    // TODO: print error, it needs some traits
                                    tracing::warn!("Failed to get next internal message");
                                    continue;
                                }
                                None => {
                                    tracing::trace!(target: "builder", "get_next_int_message: account iter has no new messages");
                                    break None;
                                }
                            }
                        },
                        None => {
                            tracing::trace!(target: "builder", "get_next_int_message: iter has no new accounts");
                            break None;
                        }
                    }
                })};

                // Start first message execution separately because we must wait for it to finish
                let mut first_thread_and_key = match get_next_int_message(check_messages_map)? {
                    Some((message, key)) => {
                        executed_int_messages_cnt += 1;
                        let first_acc_id = message.int_dst_account_id().expect("Failed to get int_dst_account_id");
                        let first_thread = self.execute(
                            message,
                            blockchain_config,
                            &first_acc_id,
                            block_unixtime,
                            block_lt,
                            check_messages_map,
                            time_limits,
                        )?;
                        Some((first_thread, key))
                    },
                    None => None
                };

                tracing::info!(target: "builder", "Internal messages execution start");
                if first_thread_and_key.is_some() {
                    loop {
                        // If active pool is not full add threads
                        let mut message_queue_is_empty = false;
                        while active_threads.len() < self.parallelization_level {
                            let thread_and_key = match get_next_int_message(check_messages_map)? {
                                Some((message, key)) => {
                                    executed_int_messages_cnt += 1;
                                    let acc_id = message.int_dst_account_id().expect("Failed to get int_dst_account_id");
                                    let thread = self.execute(
                                        message,
                                        blockchain_config,
                                        &acc_id,
                                        block_unixtime,
                                        block_lt,
                                        check_messages_map,
                                        time_limits
                                    )?;
                                    Some((thread, key))
                                },
                                None => None
                            };
                            if let Some((thread, key)) = thread_and_key {
                                active_threads.push((key, thread));
                                continue;
                            } else if active_threads.is_empty() && first_thread_and_key.is_none() {
                                message_queue_is_empty = true;
                            }
                            break;
                        }
                        // Check first thread finalization
                        let first_finished =
                            first_thread_and_key.as_ref().and_then(|(thread, _)| thread.result_rx.try_recv().ok());
                        if let Some(thread_result) = first_finished {
                            let (_, first_key) = first_thread_and_key.take().unwrap();
                            tracing::trace!(target: "builder", "First int message finished, key: {}", first_key.inner().hash.to_hex_string());
                            let thread_result = thread_result.map_err(|_| {
                                    anyhow::format_err!("Failed to execute transaction in parallel")
                                })?;
                            let acc_id = thread_result.account_id.clone();

                            self.after_transaction(thread_result)?;
                            active_int_destinations.lock().remove(&acc_id);
                            //
                            // self.out_queue_info
                            //     .out_queue_mut()
                            //     .remove(
                            //         SliceData::load_builder(first_key.inner().write_to_new_cell().map_err(|e| anyhow::format_err!("Failed to serialize key: {e}"))?).map_err(|e| anyhow::format_err!("Failed to serialize key: {e}"))?,
                            //     )
                            //     .map_err(|e| {
                            //         anyhow::format_err!("Failed to remove message from queue: {e}")
                            //     })?;
                        }
                        // Check active threads
                        let mut i = 0;
                        while i < active_threads.len() {
                            if let Ok(thread_result) = active_threads[i].1.result_rx.try_recv() {
                                let (key, _) = active_threads.remove(i);
                                // let (vm_execution_is_block_related, block_production_was_finished) = (
                                //     thread.vm_execution_is_block_related.lock().unwrap(),
                                //     thread.block_production_was_finished.lock().unwrap(),
                                // );
                                let thread_result = thread_result.map_err(|_| {
                                    anyhow::format_err!("Failed to execute transaction in parallel")
                                })?;
                                tracing::trace!(target: "builder", "Thread with dapp_id and minted shell {:?} {:?} {:?}", thread_result.initial_dapp_id, thread_result.minted_shell, thread_result.transaction);
                                let acc_id = thread_result.account_id.clone();
                                // if *vm_execution_is_block_related && *block_production_was_finished {
                                //     tracing::trace!(target: "builder", "parallel int message finished dest: {}, key: {}, but tx was block related so result is not used", acc_id.to_hex_string(), key.repr_hash().to_hex_string());
                                //     Insert message to the head of message queue for not to break initial messages order to one account
                                //     sorted.insert(0,(key, thread.message, acc_id.clone(), 0));
                                    // sorted.push((key, thread.message, acc_id.clone(), 0));
                                // } else {
                                    tracing::trace!(target: "builder", "parallel int message finished dest: {}, key: {}", acc_id.to_hex_string(), key.inner().hash.to_hex_string());
                                    self.after_transaction(thread_result)?;
                                    // self.out_queue_info
                                    //     .out_queue_mut()
                                    //     .remove(
                                    //         SliceData::load_builder(key.inner().write_to_new_cell().map_err(|e| anyhow::format_err!("Failed to serialize key: {e}"))?).map_err(|e| anyhow::format_err!("Failed to serialize key: {e}"))?,
                                    //     )
                                    //     .map_err(|e| {
                                    //         anyhow::format_err!("Failed to remove message from queue: {e}")
                                    //     })?;
                                // }
                                active_int_destinations.lock().remove(&acc_id);
                            } else {
                                i += 1;
                            }
                        }
                        if check_messages_map.is_none() && self.is_limits_reached() {
                            tracing::debug!(target: "builder", "Internal messages stop was set because block is full");
                            block_full = true;
                        }
                        // If first message was processed and block should be finalized, break the loop
                        if block_full && first_thread_and_key.is_none() {
                            tracing::debug!(target: "builder", "Internal messages stop because block is full");
                            break;
                        }
                        if message_queue_is_empty && active_threads.is_empty() && first_thread_and_key.is_none() {
                            tracing::debug!(target: "builder", "Internal messages stop because there is no internal messages left");
                            break;
                        }
                    }
                }
                tracing::info!(target: "builder", "Internal messages execution: executed_int_messages_cnt={}", executed_int_messages_cnt);
                #[cfg(feature = "timing")]
                tracing::info!(target: "builder", "Internal messages execution time {} ms", start.elapsed().as_millis());
                Ok::<_, anyhow::Error>(())
            })?;
        Ok(block_full)
    }

    #[instrument(skip_all)]
    #[allow(clippy::too_many_arguments)]
    pub fn build_block(
        mut self,
        ext_messages_queue: HashMap<AccountId, VecDeque<(Stamp, Message)>>,
        blockchain_config: &BlockchainConfig,
        mut active_threads: Vec<(Cell, ActiveThread)>,
        mut check_messages_map: Option<HashMap<AccountId, BTreeMap<u64, UInt256>>>,
        white_list_of_slashing_messages_hashes: HashSet<UInt256>,
        message_db: MessageDurableStorage,
        time_limits: &ExecutionTimeLimits,
    ) -> anyhow::Result<(PreparedBlock, Vec<Stamp>, ExtMsgFeedbackList)> {
        let _ =
            tracing::span!(tracing::Level::INFO, "build_block", seq_no = self.block_info.seq_no());
        active_threads.clear();
        tracing::info!(target: "builder", "Start build of block: {} for {:?}", self.block_info.seq_no(), self.thread_id);
        tracing::info!(target: "builder", "ext_messages_queue.len={}, active_threads.len={}, check_messages_map.len={:?}", queue_len(&ext_messages_queue), active_threads.len(), check_messages_map.as_ref().map(|map| map.len()));

        let (block_unixtime, block_lt) = self.at_and_lt();

        // TODO: this flag is unused, fix it
        let verify_block_contains_missing_messages_from_prev_state = false;

        // Second step: Take outbound internal messages from previous state, execute internal
        // messages that have destination in the current state and remove others from state.

        let mut block_full = self.execute_all_internal_messages(
            blockchain_config,
            &mut check_messages_map,
            white_list_of_slashing_messages_hashes,
            message_db.clone(),
            time_limits,
        )?;

        // Third step: execute external messages if block is not full
        let (ext_message_feedbacks, processed_stamps, unprocessed_ext_msgs_cnt) =
            if !block_full && !ext_messages_queue.is_empty() {
                let (feedbacks, processed_stamps, is_full, unprocessed) = self
                    .execute_external_messages(
                        blockchain_config,
                        ext_messages_queue,
                        block_unixtime,
                        block_lt,
                        &mut check_messages_map,
                        time_limits,
                    )?;
                block_full = is_full;
                (feedbacks, processed_stamps, unprocessed)
            } else {
                (ExtMsgFeedbackList::new(), vec![], queue_len(&ext_messages_queue))
            };

        #[cfg(feature = "timing")]
        let start = std::time::Instant::now();

        trace_span!("execute new messages", messages.count = self.new_messages.len() as i64).in_scope(||{
        // Fourth step: execute new messages if block is not full
        tracing::info!(target: "builder", "Start new messages execution");

        if !block_full {
            let mut active_destinations = HashMap::new();
            loop {
                let mut there_are_no_new_messages_for_verify_block = true;

                while active_threads.len() < self.parallelization_level {
                    let next_message = self.retrieve_next_message(
                        &active_destinations,
                        &mut check_messages_map,
                        &mut there_are_no_new_messages_for_verify_block,
                        verify_block_contains_missing_messages_from_prev_state,
                    )?;

                    let Some((message, acc_id, index, key)) = next_message else {
                        break;
                    };

                    let key = key
                        .write_to_new_cell()
                        .map_err(|e| anyhow::format_err!("Failed to serialize key: {e}"))?
                        .into_cell()
                        .map_err(|e| anyhow::format_err!("Failed to serialize key: {e}"))?;

                    tracing::trace!(target: "builder", "Parallel new message: {:?} to {:?}, key: {}", message.hash().unwrap(), acc_id.to_hex_string(), key.repr_hash().to_hex_string());

                    let thread = self.execute(
                        message.clone(),
                        blockchain_config,
                        &acc_id,
                        block_unixtime,
                        block_lt,
                        &mut check_messages_map,
                        time_limits,
                    )?;
                    active_threads.push((key, thread));
                    active_destinations.insert(acc_id, index);
                }

                // Stop building verify block
                if check_messages_map.is_some()
                    && there_are_no_new_messages_for_verify_block
                    && active_threads.is_empty()
                {
                    tracing::info!(target: "builder", "Stop building verify block");
                    break;
                }

                // Check active threads
                self.process_completed_new_message_threads(&mut active_threads, &mut active_destinations)?;

                if check_messages_map.is_none() && self.is_limits_reached() {
                    tracing::info!(target: "builder", "New messages stop because block is full");
                    break;
                }

                if self.new_messages.is_empty() && active_threads.is_empty() {
                    tracing::info!(target: "builder", "New messages stop");
                    break;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
        })?;

        #[cfg(feature = "timing")]
        tracing::info!(target: "builder", "New messages execution time {} ms", start.elapsed().as_millis());
        self.execute_dapp_config_messages(
            blockchain_config,
            block_unixtime,
            block_lt,
            &mut check_messages_map,
        )
        .map_err(|e| anyhow::format_err!("Failed to execute dapp config messages: {e}"))?;

        // trace_span!("save new messages", messages.len = self.new_messages.len() as i64).in_scope(||{
        // save new messages
        // #[cfg(feature = "timing")]
        // let start = std::time::Instant::now();
        // if !self.new_messages.is_empty() {
        //     tracing::info!(target: "builder", "save new messages cnt {:?}", self.new_messages.len());
        //     for (message, _tr_cell) in std::mem::take(&mut self.new_messages).into_values() {
        //         let info = message
        //             .int_header()
        //             .ok_or_else(|| anyhow::format_err!("message is not internal"))?;
        //         let fwd_fee = info.fwd_fee();
        //         let msg_cell = message
        //             .serialize()
        //             .map_err(|e| anyhow::format_err!("Failed to serialize message: {e}"))?;
        //         // TODO: use it when interface is merged
        //         let env = MsgEnvelope::with_message_and_fee(&message, *fwd_fee)
        //             .map_err(|e| anyhow::format_err!("Failed to envelope message: {e}"))?;
        //         let acc_id = message.int_dst_account_id().unwrap_or_default();
        //         let enq = EnqueuedMsg::with_param(info.created_lt, &env)
        //             .map_err(|e| anyhow::format_err!("Failed to enqueue message: {e}"))?;
        //         let prefix = acc_id
        //             .clone()
        //             .get_next_u64()
        //             .map_err(|e| anyhow::format_err!("Failed to calculate acc prefix: {e}"))?;
        //         let key = self.out_msg_key(prefix, msg_cell.repr_hash());
        //
        //         // self.out_queue_info
        //         //     .out_queue_mut()
        //         //     .set(
        //         //         &key,
        //         //         &enq,
        //         //         &enq.aug()
        //         //             .map_err(|e| anyhow::format_err!("Failed to get msg aug: {e}"))?,
        //         //     )
        //         //     .map_err(|e| anyhow::format_err!("Failed to save msg to queue: {e}"))?;
        //         // Add new message to block out msg descr only if it was generated while execution, not imported from other threads
        //         // if let Some(tr_cell) = tr_cell {
        //         //     let out_msg = OutMsg::new(enq.out_msg_cell(), tr_cell);
        //         //     self.out_msg_descr
        //         //         .set(
        //         //             &msg_cell.repr_hash(),
        //         //             &out_msg,
        //         //             &out_msg
        //         //                 .aug()
        //         //                 .map_err(|e| anyhow::format_err!("Failed to get msg aug: {e}"))?,
        //         //         )
        //         //         .map_err(|e| anyhow::format_err!("Failed to set msg to out descr: {e}"))?;
        //         // }
        //     }
        //     self.new_messages.clear();
        // }
        // #[cfg(feature = "timing")]
        // tracing::info!(target: "builder", "New messages save time {} ms", start.elapsed().as_millis());
        //     Ok::<_, anyhow::Error>(())
        // })?;

        tracing::info!(target: "ext_messages", "unprocessed/processed/feedbacks={}/{}/{}", unprocessed_ext_msgs_cnt, processed_stamps.len(), ext_message_feedbacks.0.len());

        let prepared_block = self.finish_and_prepare_block(active_threads, message_db)?;

        Ok((prepared_block, processed_stamps, ext_message_feedbacks))
    }

    /// Add transaction to block
    fn add_raw_transaction(
        &mut self,
        transaction: Transaction,
        tr_cell: Cell,
    ) -> anyhow::Result<()> {
        tracing::debug!(
            target: "builder",
            "Inserting transaction {} {}",
            transaction.account_id().to_hex_string(),
            transaction.hash().unwrap().to_hex_string()
        );

        self.account_blocks
            .add_serialized_transaction(&transaction, &tr_cell)
            .map_err(|e| anyhow::format_err!("Failed to add serialized tx: {e}"))?;
        if let Some(copyleft_reward) = transaction.copyleft_reward() {
            self.copyleft_rewards
                .add_copyleft_reward(&copyleft_reward.address, &copyleft_reward.reward)
                .map_err(|e| anyhow::format_err!("Failed to add copyleft reward: {e}"))?;
        }

        if let Some(msg_cell) = transaction.in_msg_cell() {
            let msg = Message::construct_from_cell(msg_cell.clone())
                .map_err(|e| anyhow::format_err!("Failed to construct message: {e}"))?;
            let in_msg = if let Some(hdr) = msg.int_header() {
                let fee = hdr.fwd_fee();
                let env = MsgEnvelope::with_message_and_fee(&msg, *fee)
                    .map_err(|e| anyhow::format_err!("Failed to envelope message: {e}"))?;

                let acc_id = hdr.dst.address();

                let wrapped_message = WrappedMessage { message: msg.clone() };
                let entry = self
                    .consumed_internal_messages
                    .entry(AccountAddress(acc_id.clone()))
                    .or_default();
                entry.insert(MessageIdentifier::from(&wrapped_message));

                InMsg::immediate(
                    env.serialize().map_err(|e| {
                        anyhow::format_err!("Failed to serialize msg envelope: {e}")
                    })?,
                    tr_cell.clone(),
                    *fee,
                )
            } else {
                InMsg::external(msg_cell.clone(), tr_cell.clone())
            };

            tracing::debug!(target: "builder", "Add in message to in_msg_descr: {}", msg_cell.repr_hash().to_hex_string());
            self.in_msg_descr
                .set(
                    &msg_cell.repr_hash(),
                    &in_msg,
                    &in_msg.aug().map_err(|e| anyhow::format_err!("Failed to get msg aug: {e}"))?,
                )
                .map_err(|e| anyhow::format_err!("Failed to add in msg descr: {e}"))?;
        }
        transaction
            .iterate_out_msgs(|mut msg| {
                if msg.int_header().is_some() {
                    // NOTE: Special hack for DAPP config contracts. DAPP Root deploys dapp config
                    // contract, and to deploy dapp config contract dapp to the same dapp, we init
                    // deploy message src dapp id.
                    let body_opt = msg.clone().body();
                    if let Some(data) = msg.int_header_mut() {
                        if let Ok(address) = data.src() {
                            if address.address() == AccountId::from_string(DAPP_ROOT_ADDR).unwrap()
                            {
                                if let Some(body) = body_opt {
                                    if let Ok(Some(dapp)) = decode_message_config(body) {
                                        data.set_src_dapp_id(Some(dapp));
                                    }
                                }
                            }
                        }
                    }

                    let dest_account_id = msg
                        .int_dst_account_id()
                        .expect("Internal message must have valid internal destination");
                    let info = msg.int_header().unwrap();
                    let fwd_fee = info.fwd_fee();
                    let msg_cell = msg.serialize()?;
                    let env = MsgEnvelope::with_message_and_fee(&msg, *fwd_fee)?;
                    let enq = EnqueuedMsg::with_param(info.created_lt, &env)?;
                    let out_msg = OutMsg::new(enq.out_msg_cell(), tr_cell.clone());

                    self.out_msg_descr.set(&msg_cell.repr_hash(), &out_msg, &out_msg.aug()?)?;

                    let destination_routing = self
                        .initial_optimistic_state
                        .get_account_routing(&dest_account_id)
                        .map_err(|_e| {
                            tvm_types::error!(
                                "Failed to check account routing for new message destination"
                            )
                        })?;
                    if self
                        .initial_optimistic_state
                        .does_routing_belong_to_the_state(&destination_routing)
                        .map_err(|_e| {
                            tvm_types::error!(
                                "Failed to check account routing for new message destination"
                            )
                        })?
                    {
                        // If message destination matches current thread, save it in cache to possibly execute in the current block
                        self.add_new_internal_message_to_the_current_thread(
                            msg,
                            Some(tr_cell.clone()),
                        );
                    } else {
                        // If internal message destination doesn't match current thread, save it directly to the out msg descr of the block
                        tracing::trace!(target: "builder",
                            "New message for another thread: {}",
                            msg.hash().unwrap().to_hex_string()
                        );
                        let wrapped_message = WrappedMessage { message: msg.clone() };
                        let entry = self
                            .produced_internal_messages_to_other_threads
                            .entry(destination_routing)
                            .or_default();
                        entry.push((
                            MessageIdentifier::from(&wrapped_message),
                            Arc::new(wrapped_message),
                        ));
                    }
                } else {
                    // If message is external, save it directly to the out msg descr of the block
                    let msg_cell = msg.serialize()?;
                    let out_msg = OutMsg::external(msg_cell.clone(), tr_cell.clone());
                    tracing::debug!(
                        target: "builder",
                        "Inserting new ext out message with {} {:?}",
                        msg.hash().unwrap().to_hex_string(),
                        msg
                    );
                    self.out_msg_descr.set(&msg_cell.repr_hash(), &out_msg, &out_msg.aug()?)?;
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iterate out msgs: {e}"))?;
        Ok(())
    }

    /// Get UNIX time and Logical Time of the current block
    fn at_and_lt(&self) -> (u32, u64) {
        (self.block_info.gen_utime().as_u32(), self.start_lt)
    }

    /// Complete the construction of the block and return it.
    /// returns generated block and new shard state bag (and transaction count)
    #[instrument(skip_all)]
    fn finish_block(
        mut self,
        message_db: MessageDurableStorage,
    ) -> anyhow::Result<(
        Block,
        OptimisticStateImpl,
        CrossThreadRefData,
        HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)>,
    )> {
        tracing::trace!(target: "builder", "finish_block");
        let mut new_shard_state = self.shard_state.deref().clone();
        tracing::info!(target: "builder", "finish block: seq_no: {:?}", self.block_info.seq_no());
        tracing::info!(target: "builder", "finish block: tx_cnt: {}", self.tx_cnt);
        let _ = tracing::span!(
            tracing::Level::INFO,
            "finish_block",
            seq_no = self.block_info.seq_no(),
            tx_cnt = self.tx_cnt
        );
        new_shard_state.set_seq_no(self.block_info.seq_no());
        trace_span!("write accounts").in_scope(|| {
            new_shard_state.write_accounts(&self.accounts).map_err(|e| {
                anyhow::format_err!("Failed to write accounts to new shard state: {e}")
            })
        })?;
        // new_shard_state
        //     .write_out_msg_queue_info(&self.out_queue_info)
        //     .map_err(|e| anyhow::format_err!("Failed to write out msg queue info: {e}"))?;
        tracing::info!(
            target: "builder",
            "finish block new_shard_state hash: {:?}",
            new_shard_state.hash().unwrap().to_hex_string()
        );

        let block_extra = trace_span!("block extra").in_scope(|| {
            let mut block_extra = BlockExtra::default();
            block_extra
                .write_in_msg_descr(&self.in_msg_descr)
                .map_err(|e| anyhow::format_err!("Failed to write in msg descr: {e}"))?;
            block_extra
                .write_out_msg_descr(&self.out_msg_descr)
                .map_err(|e| anyhow::format_err!("Failed to write out msg descr: {e}"))?;
            block_extra
                .write_account_blocks(&self.account_blocks)
                .map_err(|e| anyhow::format_err!("Failed to write account blocks: {e}"))?;
            block_extra.rand_seed = self.rand_seed;
            tracing::info!(target: "builder", "finish_block: prepare block extra");
            Ok::<BlockExtra, anyhow::Error>(block_extra)
        })?;
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
            .map_err(|e| anyhow::format_err!("Failed to add fees: {e}"))?;

        tracing::info!(target: "builder", "finish_block: prepare value flow");

        let (new_ss_root, state_update) = trace_span!("generate state update").in_scope(||{
            let new_ss_root = new_shard_state
                .serialize()
                .map_err(|e| anyhow::format_err!("Failed to serialize shard state: {e}"))?;
            tracing::info!(target: "builder", "finish_block: serialize new state: {}", new_ss_root.repr_hash().to_hex_string());
            let mut old_ss = self.initial_optimistic_state.get_shard_state().deref().clone();
            old_ss.write_accounts(&self.initial_accounts).map_err(|e| {
                anyhow::format_err!("Failed to write accounts to old shard state: {e}")
            })?;
            let old_ss_root = old_ss
                .serialize()
                .map_err(|e| anyhow::format_err!("Failed to serialize old shard state: {e}"))?;
            tracing::info!(target: "builder", "finish_block: got old state: {}", old_ss_root.repr_hash().to_hex_string());
            tracing::trace!(target: "builder", "finish_block: usage tree root: {:?}", self.usage_tree.root_cell());
            tracing::trace!(target: "builder", "finish_block: usage tree set: {:?}", self.usage_tree.build_visited_set());
            #[cfg(feature = "timing")]
            let update_time = std::time::Instant::now();
            let state_update =
                // MerkleUpdate::create_fast(&old_ss_root, &new_ss_root, |h| self.usage_tree.contains(h))
                MerkleUpdate::create(&old_ss_root, &new_ss_root)
                    .map_err(|e| anyhow::format_err!("Failed to create merkle update: {e}"))?;
            #[cfg(feature = "timing")]
            tracing::info!(target: "builder", "finish_block: prepare merkle update: {}ms", update_time.elapsed().as_millis());
            Ok::<(Cell, MerkleUpdate), anyhow::Error>((new_ss_root, state_update))
        })?;

        self.block_info.set_end_lt(self.end_lt.max(self.start_lt + 1));

        let block_info = self.block_info.clone();

        let block = Block::with_params(
            self.shard_state.global_id(),
            self.block_info,
            value_flow,
            state_update,
            block_extra,
        )
        .map_err(|e| anyhow::format_err!("Failed to construct block: {e}"))?;

        let span = tracing::span!(tracing::Level::INFO, "serialize and write boc");
        let span_guard = span.enter();
        let cell = block.serialize().unwrap();
        let root_hash = cell.repr_hash();

        let serialized_block = tvm_types::write_boc(&cell).unwrap();
        drop(span_guard);

        let file_hash = UInt256::calc_file_hash(&serialized_block);
        let prev_block_info = BlkPrevInfo::Block {
            prev: ExtBlkRef {
                end_lt: block_info.end_lt(),
                seq_no: block_info.seq_no(),
                root_hash,
                file_hash,
            },
        };
        let block_id: BlockIdentifier = block.hash().expect("Failed to calculate block id").into();
        let mut new_thread_refs = self.initial_optimistic_state.thread_refs_state.clone();
        let current_thread_id = *self.initial_optimistic_state.get_thread_id();
        let current_thread_last_block =
            (current_thread_id, block_id.clone(), BlockSeqNo::from(block_info.seq_no()));
        new_thread_refs.update(current_thread_id, current_thread_last_block.clone());

        let mut changed_dapp_ids: HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)> =
            HashMap::new();
        let accounts_that_changed_their_dapp_id: HashMap<AccountRouting, Option<WrappedAccount>> =
            HashMap::from_iter(
                self.accounts_that_changed_their_dapp_id
                    .values()
                    .map(|v| v.last().unwrap().clone()),
            );
        for (routing, account) in &accounts_that_changed_their_dapp_id {
            let new_dapp = if account.is_none() { None } else { Some(routing.0.clone()) };
            changed_dapp_ids.insert(routing.1.clone(), (new_dapp, BlockEndLT(self.end_lt)));
        }

        let mut changed_accounts = HashSet::new();
        self.account_blocks
            .iterate_with_keys(|addr, _| {
                changed_accounts.insert(addr);
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iterate account blocks: {e}"))?;
        let thread_id = *self.initial_optimistic_state.get_thread_id();
        let threads_table = self.initial_optimistic_state.get_produced_threads_table().clone();
        let (new_state, cross_thread_ref_data) = postprocess(
            self.initial_optimistic_state,
            self.consumed_internal_messages.clone(),
            self.produced_internal_messages_to_the_current_thread.clone(),
            accounts_that_changed_their_dapp_id.clone(),
            block_id,
            BlockSeqNo::from(block_info.seq_no()),
            (new_shard_state, new_ss_root).into(),
            self.produced_internal_messages_to_other_threads.clone(),
            self.dapp_id_table.clone(),
            prev_block_info.into(),
            thread_id,
            threads_table,
            changed_dapp_ids.clone(),
            changed_accounts,
            self.accounts_repository,
            message_db.clone(),
        )?;

        tracing::info!(target: "builder", "Finish block: {:?}", block.hash().unwrap().to_hex_string());
        Ok((block, new_state, cross_thread_ref_data, changed_dapp_ids))
    }

    // TODO: remove Option from tr_cell arg
    fn add_new_internal_message_to_the_current_thread(
        &mut self,
        message: Message,
        tr_cell: Option<Cell>,
    ) {
        // TODO: this approach works, but looks bad and needs refactoring
        // We use message lt as a mapping key to have them sorted and have unique keys
        // But messages from different account can have equal lt so add hash to index
        let mut n_index =
            ((message.lt().unwrap() as u128) << 64) + (message.hash().unwrap().first_u64() as u128);
        loop {
            if self.new_messages.contains_key(&n_index) {
                n_index += 1;
            } else {
                break;
            }
        }
        let wrapped_message = WrappedMessage { message: message.clone() };
        tracing::debug!(
            "Add message to produced_internal_messages_to_the_current_thread: {}",
            wrapped_message.message.hash().unwrap().to_hex_string()
        );
        let entry = self
            .produced_internal_messages_to_the_current_thread
            .entry(AccountAddress(message.dst().expect("must be set").address().clone()))
            .or_default();
        entry.push((MessageIdentifier::from(&wrapped_message), Arc::new(wrapped_message)));
        tracing::debug!(
            target: "builder",
            "Inserting new message with {} {:?}",
            message.hash().unwrap().to_hex_string(),
            message
        );
        self.new_messages.insert(n_index, (message, tr_cell));
    }
}

// Decompose BlockBuilder::build_block
impl BlockBuilder {
    fn execute_all_internal_messages(
        &mut self,
        blockchain_config: &BlockchainConfig,
        check_messages_map: &mut Option<HashMap<AccountId, BTreeMap<u64, UInt256>>>,
        white_list_of_slashing_messages_hashes: HashSet<UInt256>,
        message_db: MessageDurableStorage,
        time_limits: &ExecutionTimeLimits,
    ) -> anyhow::Result<bool> {
        let mut block_full = self.execute_internal_messages(
            blockchain_config,
            check_messages_map,
            white_list_of_slashing_messages_hashes.clone(),
            self.initial_optimistic_state.high_priority_messages.clone(),
            message_db.clone(),
            time_limits,
        )?;

        if !block_full {
            block_full = self.execute_internal_messages(
                blockchain_config,
                check_messages_map,
                white_list_of_slashing_messages_hashes,
                self.initial_optimistic_state.messages.clone(),
                message_db,
                time_limits,
            )?;
        }

        Ok(block_full)
    }

    fn process_completed_new_message_threads(
        &mut self,
        active_threads: &mut Vec<(Cell, ActiveThread)>,
        active_destinations: &mut HashMap<AccountId, u128>,
    ) -> anyhow::Result<()> {
        let mut i = 0;
        while i < active_threads.len() {
            if let Ok(thread_result) = active_threads[i].1.result_rx.try_recv() {
                let (key, _d) = active_threads.remove(i);
                let thread_result = thread_result.map_err(|_| {
                    anyhow::format_err!("Failed to execute transaction in parallel")
                })?;

                tracing::trace!(target: "builder", "Thread with dapp_id and mintedshell {:?} {:?} {:?}", thread_result.initial_dapp_id, thread_result.minted_shell, thread_result.transaction);

                let acc_id = thread_result.account_id.clone();
                tracing::trace!(target: "builder", "parallel new message finished dest: {}, key {}", acc_id.to_hex_string(), key.repr_hash().to_hex_string());
                self.after_transaction(thread_result)?;
                let index = active_destinations.remove(&acc_id).unwrap();
                tracing::trace!(target: "builder", "remove new message: {}", index);
                self.new_messages.remove(&index);
            } else {
                i += 1;
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    fn finish_and_prepare_block(
        mut self,
        active_threads: Vec<(Cell, ActiveThread)>,
        message_db: MessageDurableStorage,
    ) -> anyhow::Result<PreparedBlock> {
        for active_thread in &active_threads {
            let mut value = active_thread.1.block_production_was_finished.lock().unwrap();
            *value = true;
        }

        let remain_fees =
            self.in_msg_descr.root_extra().fees_collected + self.account_blocks.root_extra().grams;

        let transaction_traces = std::mem::take(&mut self.transaction_traces);
        let tx_cnt = self.tx_cnt;
        let block_keeper_set_changes = self.block_keeper_set_changes.clone();

        let (block, new_state, cross_thread_ref_data, changed_dapp_ids) = self
            .finish_block(message_db)
            .map_err(|e| anyhow::format_err!("Failed to finish block: {e}"))?;

        let span = tracing::span!(tracing::Level::INFO, "prepare block struct");
        let span_guard = span.enter();
        let prepared_block = PreparedBlock {
            block,
            state: new_state,
            is_empty: false,
            transaction_traces,
            active_threads,
            tx_cnt,
            remain_fees,
            block_keeper_set_changes,
            cross_thread_ref_data,
            changed_dapp_ids,
        };
        drop(span_guard);

        Ok(prepared_block)
    }

    fn retrieve_next_message(
        &mut self,
        active_destinations: &HashMap<AccountId, u128>,
        check_messages_map: &mut Option<HashMap<AccountId, BTreeMap<u64, UInt256>>>,
        there_are_no_new_messages_for_verify_block: &mut bool,
        verify_block_contains_missing_messages_from_prev_state: bool,
    ) -> anyhow::Result<Option<(Message, AccountId, u128, OutMsgQueueKey)>> {
        for (index, (message, _tr_cell)) in &self.new_messages {
            let acc_id = message.int_dst_account_id().unwrap_or_default();
            if let Some(msg_set) = &check_messages_map {
                if verify_block_contains_missing_messages_from_prev_state {
                    return Err(verify_error(BP_DID_NOT_PROCESS_ALL_MESSAGES_FROM_PREVIOUS_BLOCK));
                }
                let Some(messages) = msg_set.get(&acc_id) else {
                    continue;
                };
                let Some((_, next_message)) = messages.first_key_value() else {
                    continue;
                };
                if next_message != &message.hash().unwrap() {
                    // tracing::info!(target: "builder", "Skip new message for verify block: {} {:?}", message.hash().unwrap().to_hex_string(), message);
                    continue;
                }
            }
            *there_are_no_new_messages_for_verify_block = false;

            if !self.initial_optimistic_state.does_account_belong_to_the_state(&acc_id)? {
                // TODO: message is skipped, but it can prevent loop from stop
                // need to save it to out msg descr and remove from new messages
                continue;
            }

            if !active_destinations.contains_key(&acc_id) {
                let msg_cell = message
                    .serialize()
                    .map_err(|e| anyhow::format_err!("Failed to serialize message: {e}"))?;
                let prefix = acc_id
                    .clone()
                    .get_next_u64()
                    .map_err(|e| anyhow::format_err!("Failed to calculate acc prefix: {e}"))?;
                // This key is used for active threads moved to the next block to remove message from out msg queue
                let key = self.out_msg_key(prefix, msg_cell.repr_hash());

                return Ok(Some((message.clone(), acc_id, *index, key)));
            };
        }

        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    fn fill_ext_msg_threads_pool(
        &mut self,
        ext_messages_queue: &mut HashMap<AccountId, VecDeque<(Stamp, Message)>>,
        active_ext_threads: &mut VecDeque<(Stamp, ActiveThread)>,
        active_destinations: &mut HashSet<AccountId>,
        ext_message_feedbacks: &mut ExtMsgFeedbackList,
        processed_stamps: &mut Vec<Stamp>,
        blockchain_config: &BlockchainConfig,
        block_unixtime: u32,
        block_lt: u64,
        check_messages_map: &mut Option<HashMap<AccountId, BTreeMap<u64, UInt256>>>,
        time_limits: &ExecutionTimeLimits,
    ) -> anyhow::Result<()> {
        if active_ext_threads.len() >= self.parallelization_level || ext_messages_queue.is_empty() {
            return Ok(());
        }

        let span = tracing::span!(
            tracing::Level::INFO,
            "ext messages filling thread pool",
            ext_queue_size = queue_len(ext_messages_queue),
            ext_queue_dst = ext_messages_queue.keys().len(),
            ext_active_threads = active_ext_threads.len(),
        );
        let span_guard = span.enter();

        for (acc_id, _queue) in ext_messages_queue.clone().into_iter() {
            if self.is_limits_reached() || active_ext_threads.len() >= self.parallelization_level {
                break;
            }

            if active_destinations.contains(&acc_id) {
                continue;
            }

            if !self.initial_optimistic_state.does_account_belong_to_the_state(&acc_id)? {
                if let Some(mut q) = ext_messages_queue.remove(&acc_id) {
                    // If message destination doesn't belong to the current thread, remove it from the queue
                    let acc_thread =
                        self.initial_optimistic_state.get_thread_for_account(&acc_id).ok();
                    tracing::debug!(target: "ext_messages", "thread mismatch for <dst:{}>. skipped", acc_id.to_hex_string());

                    while let Some((stamp, msg)) = q.pop_front() {
                        processed_stamps.push(stamp);
                        ext_message_feedbacks
                            .push(create_thread_mismatch_feedback(msg, acc_thread)?);
                    }
                }
                continue;
            }

            // used in tests/ext_messages/process_in_parallel.py
            tracing::debug!(target: "ext_messages", "fill threads: active_ext_threads={}, ext_messages_queue={}", active_ext_threads.len(), queue_len(ext_messages_queue));

            if let Some(q) = ext_messages_queue.get_mut(&acc_id) {
                if let Some((stamp, msg)) = q.pop_front() {
                    anyhow::ensure!(msg.int_header().is_none());
                    tracing::trace!(
                        target: "ext_messages",
                        "Parallel ext message: {:?} to {:?}",
                        msg.hash().unwrap(),
                        acc_id.to_hex_string()
                    );

                    let exec_span = tracing::span!(tracing::Level::INFO, "execute ext message");
                    let span_guard = exec_span.enter();
                    let thread = self.execute(
                        msg.clone(),
                        blockchain_config,
                        &acc_id,
                        block_unixtime,
                        block_lt,
                        check_messages_map,
                        time_limits,
                    )?;
                    drop(span_guard);

                    active_ext_threads.push_back((stamp.clone(), thread));
                    active_destinations.insert(acc_id.clone());
                    processed_stamps.push(stamp);

                    if q.is_empty() {
                        ext_messages_queue.remove(&acc_id);
                    }
                }
            }
        }

        drop(span_guard);

        Ok(())
    }

    fn process_completed_ext_msg_threads(
        &mut self,
        active_ext_threads: &mut VecDeque<(Stamp, ActiveThread)>,
        active_destinations: &mut HashSet<AccountId>,
        ext_message_feedbacks: &mut ExtMsgFeedbackList,
    ) -> anyhow::Result<()> {
        let span = tracing::span!(
            tracing::Level::INFO,
            "ext messages processing completed thread pool",
            active_ext_threads = active_ext_threads.len()
        );
        let span_guard = span.enter();

        let mut i = 0;

        while i < active_ext_threads.len() {
            let ready = {
                let (_, thread) = &active_ext_threads[i];
                match thread.result_rx.try_recv() {
                    Ok(result) => Some(result),
                    Err(std::sync::mpsc::TryRecvError::Empty) => None,
                    Err(err) => {
                        tracing::trace!(target: "ext_messages", "Error receiving thread result: {err:?}");
                        return Err(anyhow::anyhow!("Error receiving thread result: {:?}", err));
                    }
                }
            };

            let Some(thread_result) = ready else {
                i += 1;
                continue;
            };

            // used in tests/ext_messages/process_in_parallel.py
            tracing::trace!(target: "ext_messages", "process completed: active_ext_threads={}", active_ext_threads.len());

            let (_, thread) = active_ext_threads.remove(i).unwrap();
            let thread_result = thread_result
                .map_err(|_| anyhow::anyhow!("Failed to execute transaction in parallel"))?;

            tracing::trace!(target: "builder", "Thread with dapp_id and minted shell {:?} {:?} {:?}", thread_result.initial_dapp_id, thread_result.minted_shell, thread_result.transaction);
            let acc_id = thread_result.account_id.clone();
            tracing::trace!(target: "ext_messages", "parallel ext message finished dest: {}", acc_id.to_hex_string());

            let feedback = create_feedback(
                thread.message,
                Some(thread_result.transaction.clone()),
                self.initial_optimistic_state.get_thread_for_account(&acc_id).ok(),
                None,
            )?;

            self.after_transaction(thread_result)?;
            active_destinations.remove(&acc_id);
            ext_message_feedbacks.push(feedback);
        }

        drop(span_guard);

        Ok(())
    }

    fn execute_external_messages(
        &mut self,
        blockchain_config: &BlockchainConfig,
        mut ext_messages_queue: HashMap<AccountId, VecDeque<(Stamp, Message)>>,
        block_unixtime: u32,
        block_lt: u64,
        check_messages_map: &mut Option<HashMap<AccountId, BTreeMap<u64, UInt256>>>,
        time_limits: &ExecutionTimeLimits,
    ) -> anyhow::Result<(ExtMsgFeedbackList, Vec<Stamp>, bool, usize)> {
        let incoming_queue_len: usize = queue_len(&ext_messages_queue);

        let span = tracing::span!(
            tracing::Level::INFO,
            "external messages execution",
            queue_size = incoming_queue_len
        );
        let span_guard = span.enter();

        // #[cfg(feature = "timing")]
        let start = std::time::Instant::now();

        let mut ext_message_feedbacks = ExtMsgFeedbackList::new();
        let mut active_destinations = HashSet::new();
        let mut active_ext_threads = VecDeque::new();
        let mut block_full = false;
        let mut processed_stamps = vec![];
        if check_messages_map.is_none() && self.is_limits_reached() {
            // Don't even enter prcessing external messages.
            return Ok((ext_message_feedbacks, processed_stamps, true, incoming_queue_len));
        }

        loop {
            self.fill_ext_msg_threads_pool(
                &mut ext_messages_queue,
                &mut active_ext_threads,
                &mut active_destinations,
                &mut ext_message_feedbacks,
                &mut processed_stamps,
                blockchain_config,
                block_unixtime,
                block_lt,
                check_messages_map,
                time_limits,
            )?;

            self.process_completed_ext_msg_threads(
                &mut active_ext_threads,
                &mut active_destinations,
                &mut ext_message_feedbacks,
            )?;

            let span = tracing::span!(tracing::Level::INFO, "is_limits_reached");
            let span_guard = span.enter();
            if check_messages_map.is_none() && self.is_limits_reached() {
                block_full = true;
                tracing::debug!(target: "ext_messages", "Ext messages stop because block is full");
                break;
            }
            drop(span_guard);

            if ext_messages_queue.is_empty() && active_ext_threads.is_empty() {
                tracing::debug!(target: "ext_messages", "Ext messages stop");
                break;
            }
        }

        drop(span_guard);

        tracing::debug!(target: "ext_messages", "processed per block (total/processed): {}/{}", incoming_queue_len, processed_stamps.len());

        // #[cfg(feature = "timing")]
        tracing::info!(target: "builder", "External messages execution time {} ms", start.elapsed().as_millis());

        tracing::Span::current().record("messages.count", processed_stamps.len() as i64);

        Ok((ext_message_feedbacks, processed_stamps, block_full, queue_len(&ext_messages_queue)))
    }
}

fn create_feedback(
    message: Message,
    transaction: Option<Transaction>,
    thread_id: Option<ThreadIdentifier>,
    error: Option<FeedbackError>,
) -> anyhow::Result<ExtMsgFeedback> {
    let hash = message.hash().map_err(|e| anyhow::format_err!("{e}"))?;
    let message_hash = hash.to_hex_string();

    let mut feedback = ExtMsgFeedback {
        message_hash,
        thread_id: thread_id.map(|thread_id| thread_id.into()),
        error,
        ..Default::default()
    };

    if let Some(t) = transaction {
        feedback.tx_hash = Some(t.hash().map_err(|e| anyhow::format_err!("{e}"))?.to_hex_string());

        let tr_desc = t.read_description().map_err(|e| anyhow::format_err!("{e}"))?;
        feedback.aborted = tr_desc.is_aborted();

        if let Some(TrComputePhase::Vm(compute)) = tr_desc.compute_phase_ref() {
            feedback.exit_code = compute.exit_code;
            if compute.exit_code != 0 {
                feedback.error = Some(FeedbackError {
                    code: FeedbackErrorCode::TvmError,
                    message: Some(
                        "Failed to execute the message. Error occurred during the compute phase."
                            .to_owned(),
                    ),
                });
            }
        } else if let Some(TrComputePhase::Skipped(skipped)) = tr_desc.compute_phase_ref() {
            let reason = match skipped.reason {
                ComputeSkipReason::NoState => "The account doesn't have a state",
                ComputeSkipReason::BadState => "The account has an invalid state",
                ComputeSkipReason::NoGas => "The account has an empty balance",
                ComputeSkipReason::Suspended => "The account is suspended",
            };
            feedback.error = Some(FeedbackError {
                code: FeedbackErrorCode::ComputeSkipped,
                message: Some(reason.to_string()),
            });
            // Exit code to indicate a reason of skipping compute phase
            feedback.exit_code = skipped.reason.clone() as i32;
        }
        let mut ext_out_msgs = vec![];
        let _ = t.out_msgs.iterate(|out_msg| {
            let header = out_msg.0.header().clone();
            if let CommonMsgInfo::ExtOutMsgInfo(_) = header {
                if let Some(body) = out_msg.0.body() {
                    ext_out_msgs.push(body);
                }
            }
            Ok(true)
        });
        feedback.ext_out_msgs = ext_out_msgs;
    }

    tracing::trace!(target: "builder", "Constructed feedback: {}", feedback);

    Ok(feedback)
}

fn create_thread_mismatch_feedback(
    msg: Message,
    acc_thread: Option<ThreadIdentifier>,
) -> anyhow::Result<ExtMsgFeedback> {
    tracing::warn!(
        target: "builder",
        "Found external msg with internal destination that doesn't match current thread: {:?}. Correct thread: {:?}",
        msg,
        acc_thread,
    );
    create_feedback(
        msg,
        None,
        acc_thread,
        Some(FeedbackError {
            code: FeedbackErrorCode::ThreadMismatch,
            message: Some("Internal processing error: thread mismatch".to_string()),
        }),
    )
}

pub fn create_queue_overflow_feedback(
    msg: Message,
    thread_id: &ThreadIdentifier,
) -> anyhow::Result<ExtMsgFeedback> {
    tracing::warn!(
        target: "builder",
        "External msg is rejected in case of queue overflow: {:?}",
        msg
    );

    create_feedback(
        msg,
        None,
        Some(*thread_id),
        Some(FeedbackError {
            code: FeedbackErrorCode::QueueOverflow,
            message: Some(
                "Message queue is full. Please try to send the message later.".to_string(),
            ),
        }),
    )
}

fn queue_len(map: &HashMap<AccountId, VecDeque<(Stamp, Message)>>) -> usize {
    map.values().map(|queue| queue.len()).sum()
}
