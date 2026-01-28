// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;

use serde::Serialize;
use telemetry_utils::mpsc::InstrumentedReceiver;
use thiserror::Error;
use tvm_block::Account;
use tvm_block::Block;
use tvm_block::BlockInfo;
use tvm_block::CopyleftRewards;
use tvm_block::Grams;
use tvm_block::InMsgDescr;
use tvm_block::Message;
use tvm_block::OutMsgDescr;
use tvm_block::ShardAccountBlocks;
use tvm_block::ShardAccounts;
use tvm_block::ShardStateUnsplit;
use tvm_block::StateInit;
use tvm_block::Transaction;
use tvm_types::Cell;
use tvm_types::UInt256;
use tvm_types::UsageTree;
use typed_builder::TypedBuilder;

use crate::block::producer::wasm::WasmNodeCache;
use crate::block_keeper_system::BlockKeeperSetChange;
use crate::creditconfig::DappConfig;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::repository::accounts::AccountsRepository;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::types::account::WrappedAccount;
use crate::types::AccountAddress;
use crate::types::AccountRouting;
use crate::types::DAppIdentifier;
use crate::types::ThreadIdentifier;

pub mod build_actions;
mod engine_version;
pub use engine_version::get_engine_version;
pub mod special_messages;
mod test;
pub mod trace;

pub struct PreparedBlock {
    pub block: Block,
    pub remain_fees: Grams,
    pub state: OptimisticStateImpl,
    pub is_empty: bool,
    // TODO: check if we can get rid of it
    // pub transaction_traces: HashMap<UInt256, Vec<EngineTraceInfoData>>,
    pub active_threads: Vec<(Cell, ActiveThread)>,
    pub tx_cnt: usize,
    pub block_keeper_set_changes: Vec<BlockKeeperSetChange>,
    pub cross_thread_ref_data: CrossThreadRefData,
    #[cfg(feature = "monitor-accounts-number")]
    pub accounts_number_diff: i64,
}

pub struct ThreadResult {
    pub transaction: Transaction,
    pub lt: u64,
    // pub trace: Option<Vec<EngineTraceInfoData>>,
    pub account_root: Cell,
    pub account_id: AccountAddress,
    pub minted_shell: i128,
    pub initial_dapp_id: Option<DAppIdentifier>,
    pub initial_code_hash: Option<UInt256>,
    // Note: initial_account field is initialized only for epoch contracts
    pub initial_account: Option<Account>,
    pub in_msg_is_ext: bool,
    pub in_msg: Message,
}

pub struct ActiveThread {
    pub result_rx: InstrumentedReceiver<anyhow::Result<ThreadResult>>,
    pub message: Message,
    pub vm_execution_is_block_related: Arc<Mutex<bool>>,
    pub block_production_was_finished: Arc<Mutex<bool>>,
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

type MessageIndex = u128;

/// BlockBuilder structure
#[derive(TypedBuilder)]
pub struct BlockBuilder {
    pub(crate) thread_id: ThreadIdentifier,
    pub(crate) shard_state: Arc<ShardStateUnsplit>,
    pub(crate) accounts: ShardAccounts,
    pub(crate) initial_accounts: ShardAccounts,
    pub(crate) block_info: BlockInfo,
    pub(crate) rand_seed: UInt256,
    // Mapping of messages generated while execution
    #[builder(default)]
    pub(crate) new_messages: BTreeMap<MessageIndex, (Message, Option<Cell>)>,
    #[builder(default)]
    pub(crate) in_msg_descr: InMsgDescr,
    #[builder(default)]
    pub(crate) out_msg_descr: OutMsgDescr,
    // pub(crate) out_queue_info: OutMsgQueueInfo,
    pub(crate) block_gas_limit: u64,
    #[builder(default)]
    pub(crate) account_blocks: ShardAccountBlocks,
    #[builder(default)]
    pub(crate) total_gas_used: u64,
    pub(crate) start_lt: u64,
    pub(crate) end_lt: u64, // biggest logical time of all messages
    #[builder(default)]
    pub(crate) copyleft_rewards: CopyleftRewards,
    // pub(crate) transaction_traces: HashMap<UInt256, Vec<EngineTraceInfoData>>,
    control_rx_stop: Option<InstrumentedReceiver<()>>,
    is_stop_requested: bool,

    pub(crate) parallelization_level: usize,
    #[builder(default)]
    pub(crate) tx_cnt: usize,

    pub(crate) usage_tree: UsageTree,
    pub(crate) initial_optimistic_state: OptimisticStateImpl,
    pub(crate) block_keeper_epoch_code_hash: String,
    pub(crate) block_keeper_preepoch_code_hash: String,
    #[builder(default)]
    pub(crate) block_keeper_set_changes: Vec<BlockKeeperSetChange>,
    pub(crate) base_config_stateinit: StateInit,
    #[builder(default)]
    pub(crate) dapp_credit_map: HashMap<DAppIdentifier, DappConfig>,
    #[builder(default)]
    pub(crate) dapp_minted_map: HashMap<DAppIdentifier, i128>,
    pub(crate) accounts_repository: AccountsRepository,

    // part used to update local state
    #[builder(default)]
    pub(crate) consumed_internal_messages: HashMap<AccountAddress, HashSet<MessageIdentifier>>,
    #[builder(default)]
    pub(crate) produced_internal_messages_to_the_current_thread:
        HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,

    // part to create cross thread ref data
    pub(crate) produced_internal_messages_to_other_threads:
        HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    #[builder(default)]
    pub(crate) accounts_that_changed_their_dapp_id:
        HashMap<AccountAddress, Vec<(AccountRouting, Option<WrappedAccount>)>>,
    metrics: Option<BlockProductionMetrics>,

    // cached resources used for wasm execution
    pub(crate) wasm_cache: WasmNodeCache,

    #[cfg(feature = "monitor-accounts-number")]
    pub(crate) accounts_number_diff: i64,

    is_verifier: bool,
}

impl BlockBuilder {
    fn should_stop_production(&mut self) -> bool {
        if self.is_stop_requested {
            return true;
        }
        if self.is_verifier {
            // Note: while building a verify block we don't check gas limit, only time deadline
            return false;
        }
        if let Some(rx) = &self.control_rx_stop {
            if rx.try_recv().is_ok() {
                tracing::debug!(target: "builder", "block builder received stop");
                tracing::event!(tracing::Level::INFO, "block builder received stop");
                self.is_stop_requested = true;
                return true;
            }
        }
        if self.total_gas_used > self.block_gas_limit {
            tracing::debug!(target: "builder", "block builder gas limit reached");
            tracing::event!(tracing::Level::INFO, "block builder gas limit reached");
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum ExecuteError {
    #[error("Account was moved to another thread, move internal message to the out queue")]
    AccountWasMovedRerouteInternalMessage,
    #[error("Account was moved to another thread, ignore external message")]
    AccountWasMovedIgnoreExternalMessage,
    #[error("Dest account does not match thread state")]
    WrongDestinationThread(AccountRouting),
}
