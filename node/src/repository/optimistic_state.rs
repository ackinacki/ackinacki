// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use tvm_block::Deserializable;
use tvm_block::HashmapAugType;
use tvm_block::ShardStateUnsplit;
use tvm_types::AccountId;
use tvm_types::Cell;
use typed_builder::TypedBuilder;

use super::optimistic_shard_state::OptimisticShardState;
use super::repository_impl::RepositoryImpl;
use crate::block::keeper::process::prepare_prev_block_info;
use crate::bls::GoshBLS;
use crate::message::Message;
use crate::message::WrappedMessage;
use crate::multithreading::account::get_account_routing_for_account;
use crate::multithreading::shard_state_operations::split_shard_state_based_on_threads_table;
use crate::types::AccountAddress;
use crate::types::AccountRouting;
use crate::types::AckiNackiBlock;
use crate::types::BlockEndLT;
use crate::types::BlockIdentifier;
use crate::types::BlockInfo;
use crate::types::BlockSeqNo;
use crate::types::DAppIdentifier;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

pub trait OptimisticState: Send + Clone {
    type Cell;
    type Message: Message;
    type ShardState;

    fn get_remaining_ext_messages(
        &self,
        repository: &RepositoryImpl,
    ) -> anyhow::Result<Vec<Self::Message>>;
    fn get_block_seq_no(&self) -> &BlockSeqNo;
    fn get_block_id(&self) -> &BlockIdentifier;
    fn get_shard_state(&mut self) -> Self::ShardState;
    fn get_shard_state_as_cell(&mut self) -> Self::Cell;
    fn get_block_info(&self) -> &BlockInfo;
    fn serialize_into_buf(&mut self) -> anyhow::Result<Vec<u8>>;
    fn apply_block(&mut self, block_candidate: &AckiNackiBlock<GoshBLS>) -> anyhow::Result<()>;
    fn get_thread_id(&self) -> &ThreadIdentifier;
    fn get_threads_table(&self) -> &ThreadsTable;
    fn split_state_for_mask(
        self,
        threads_table: ThreadsTable,
        thread_id: ThreadIdentifier,
    ) -> anyhow::Result<(Self, Self)>;
    fn does_account_belong_to_the_state(&self, account_id: &AccountId) -> anyhow::Result<bool>;
    fn get_dapp_id_table(&self) -> &HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)>;
    fn merge_threads_table(&mut self, another_state: &Self) -> anyhow::Result<()>;
    fn merge_dapp_id_tables(&mut self, another_state: &Self) -> anyhow::Result<()>;
    fn add_unprocessed_messages(&mut self, messages: Vec<Self::Message>);
    fn get_messages_for_another_thread(
        &self,
        another_state: &Self,
    ) -> anyhow::Result<Vec<Self::Message>>;
    fn does_state_has_messages_to_other_threads(&self) -> anyhow::Result<bool>;
}

#[serde_as]
#[derive(Clone, TypedBuilder, Serialize, Deserialize)]
pub struct OptimisticStateImpl {
    pub(crate) block_seq_no: BlockSeqNo,
    pub(crate) block_id: BlockIdentifier,
    #[builder(setter(into))]
    pub(crate) shard_state: OptimisticShardState,
    pub(crate) last_processed_external_message_index: u32,

    #[builder(setter(into))]
    pub block_info: BlockInfo,
    pub threads_table: ThreadsTable,
    pub thread_id: ThreadIdentifier,
    // Value is a tuple (Option<DAppIdentifier>, <end_lt of the block it was changed in>)
    // TODO: we must clear this table after account was removed from all threads and finalized
    pub dapp_id_table: HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)>,
}

impl Debug for OptimisticStateImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("block_seq_no", &self.block_seq_no)
            .field("block_id", &self.block_id)
            .field("shard_state", &self.shard_state)
            .field(
                "last_processed_external_message_index",
                &self.last_processed_external_message_index,
            )
            .field("block_info", &self.block_info)
            .field("threads_table", &self.threads_table)
            .field("thread_id", &self.thread_id)
            .field("dapp_id_table", &self.dapp_id_table)
            .finish()
    }
}

impl Default for OptimisticStateImpl {
    fn default() -> Self {
        Self::zero()
    }
}

impl OptimisticStateImpl {
    pub fn deserialize_from_buf(
        data: &[u8],
        block_id: BlockIdentifier,
        // block_seq_no: BlockSeqNo,
    ) -> anyhow::Result<Self> {
        let mut state: Self = bincode::deserialize(data)?;
        // TODO: why?
        state.block_id = block_id;
        Ok(state)
    }

    pub fn zero() -> Self {
        Self {
            block_seq_no: BlockSeqNo::default(),
            block_id: BlockIdentifier::default(),
            shard_state: OptimisticShardState::default(),
            last_processed_external_message_index: 0,
            block_info: BlockInfo::default(),
            threads_table: ThreadsTable::default(),
            thread_id: ThreadIdentifier::default(),
            dapp_id_table: HashMap::new(),
        }
    }

    pub fn set_shard_state(&mut self, new_state: <Self as OptimisticState>::ShardState) {
        self.shard_state = OptimisticShardState::from(new_state);
    }

    #[cfg(test)]
    pub fn stub(_id: BlockIdentifier) -> Self {
        Self::zero()
    }
}

impl OptimisticState for OptimisticStateImpl {
    type Cell = Cell;
    type Message = WrappedMessage;
    type ShardState = Arc<ShardStateUnsplit>;

    fn get_dapp_id_table(&self) -> &HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)> {
        &self.dapp_id_table
    }

    fn get_remaining_ext_messages(
        &self,
        repository: &RepositoryImpl,
    ) -> anyhow::Result<Vec<Self::Message>> {
        let ext_messages = repository.load_ext_messages_queue()?;
        if ext_messages.queue.is_empty() {
            return Ok(vec![]);
        }
        tracing::trace!(
            "last_index: {} ext_msgs: {:?}",
            self.last_processed_external_message_index,
            ext_messages.queue.len()
        );
        let mut res = vec![];
        for message in ext_messages.queue {
            if message.index > self.last_processed_external_message_index {
                res.push(message.message)
            }
        }
        tracing::trace!("get_remaining_ext_messages result: {}", res.len());
        Ok(res)
    }

    fn get_block_seq_no(&self) -> &BlockSeqNo {
        &self.block_seq_no
    }

    fn get_block_id(&self) -> &BlockIdentifier {
        &self.block_id
    }

    fn get_shard_state(&mut self) -> Self::ShardState {
        self.shard_state.into_shard_state()
    }

    fn get_shard_state_as_cell(&mut self) -> Self::Cell {
        self.shard_state.into_cell()
    }

    fn get_block_info(&self) -> &BlockInfo {
        &self.block_info
    }

    fn serialize_into_buf(&mut self) -> anyhow::Result<Vec<u8>> {
        let buffer: Vec<u8> = bincode::serialize(self)?;
        Ok(buffer)
    }

    fn apply_block(&mut self, block_candidate: &AckiNackiBlock<GoshBLS>) -> anyhow::Result<()> {
        let block_id = block_candidate.identifier();
        tracing::trace!("Applying block: {:?}", block_id);
        tracing::trace!("Check parent: {:?} ?= {:?}", self.block_id, block_candidate.parent());
        assert_eq!(
            self.block_id,
            block_candidate.parent(),
            "Tried to apply block that is not child"
        );
        #[cfg(feature = "timing")]
        let start = std::time::Instant::now();
        let last_processed_messages_index = self.last_processed_external_message_index
            + block_candidate.processed_ext_messages_cnt() as u32;
        // if block_candidate.tx_cnt() == 0 {
        //     tracing::trace!("no txns, add seq_no and block info");
        //     let mut prev_state = (*self.get_shard_state()).clone();
        //     let block_info = prepare_prev_block_info(block_candidate);
        //     prev_state.set_seq_no(prev_state.seq_no() + 1);
        //
        //     self.block_id = block_candidate.identifier();
        //     self.shard_state = OptimisticShardState::from(prev_state);
        //     self.block_info = block_info;
        //     self.last_processed_external_message_index = last_processed_messages_index;
        // } else {
        let prev_state = self.get_shard_state_as_cell();
        tracing::trace!("Applying block");
        tracing::trace!(target: "node", "apply_block: Old state hash: {:?}", prev_state.repr_hash());
        let state_update = block_candidate
            .tvm_block()
            .read_state_update()
            .map_err(|e| anyhow::format_err!("Failed to read block state update: {e}"))?;
        tracing::trace!("Applying block loaded state update");
        #[cfg(feature = "timing")]
        let apply_timer = std::time::Instant::now();
        let new_state = state_update
            .apply_for(&prev_state)
            .map_err(|e| anyhow::format_err!("Failed to apply state update: {e}"))?;
        #[cfg(feature = "timing")]
        tracing::trace!(target: "node", "apply_block: update has taken {}ms", apply_timer.elapsed().as_millis());
        tracing::trace!(target: "node", "apply_block: New state hash: {:?}", new_state.repr_hash());

        let block_info = prepare_prev_block_info(block_candidate);

        self.block_id = block_candidate.identifier();
        self.shard_state = OptimisticShardState::from(new_state);
        self.block_info = block_info;
        self.last_processed_external_message_index = last_processed_messages_index;
        // }
        #[cfg(feature = "timing")]
        tracing::trace!("Apply block {block_id:?} time: {} ms", start.elapsed().as_millis());
        Ok(())
    }

    fn get_thread_id(&self) -> &ThreadIdentifier {
        &self.thread_id
    }

    fn get_threads_table(&self) -> &ThreadsTable {
        &self.threads_table
    }

    fn split_state_for_mask(
        mut self,
        threads_table: ThreadsTable,
        thread_id_b: ThreadIdentifier,
    ) -> anyhow::Result<(Self, Self)> {
        let unsplit_state = self.get_shard_state();
        let (state_a, state_b) = split_shard_state_based_on_threads_table(
            unsplit_state,
            &threads_table,
            self.thread_id,
            thread_id_b,
        )?;
        Ok((
            OptimisticStateImpl {
                block_seq_no: self.block_seq_no,
                block_id: self.block_id.clone(),
                shard_state: OptimisticShardState::from(state_a),
                block_info: self.block_info.clone(),
                last_processed_external_message_index: self.last_processed_external_message_index,
                threads_table: threads_table.clone(),
                thread_id: self.thread_id,
                dapp_id_table: self.dapp_id_table.clone(),
            },
            OptimisticStateImpl {
                block_seq_no: self.block_seq_no,
                block_id: self.block_id.clone(),
                shard_state: OptimisticShardState::from(state_b),
                block_info: self.block_info.clone(),
                last_processed_external_message_index: self.last_processed_external_message_index,
                threads_table: threads_table.clone(),
                thread_id: thread_id_b,
                dapp_id_table: self.dapp_id_table.clone(),
            },
        ))
    }

    fn does_account_belong_to_the_state(&self, account_id: &AccountId) -> anyhow::Result<bool> {
        let account_address = AccountAddress(account_id.clone());
        let account_routing = if let Some(dapp_id) = self.dapp_id_table.get(&account_address) {
            match &dapp_id.0 {
                Some(dapp_id) => AccountRouting(dapp_id.clone(), account_address.clone()),
                None => {
                    AccountRouting(DAppIdentifier(account_address.clone()), account_address.clone())
                }
            }
        } else {
            // TODO: We cant have a mut ref in this function, so have to decode shard state
            let shard_state = if let Some(state) = self.shard_state.shard_state.clone() {
                state
            } else {
                assert!(self.shard_state.shard_state_cell.is_some());
                let cell = self.shard_state.shard_state_cell.clone().unwrap();
                Arc::new(
                    ShardStateUnsplit::construct_from_cell(cell)
                        .expect("Failed to deserialize shard state from cell"),
                )
            };
            let accounts = shard_state
                .read_accounts()
                .map_err(|e| anyhow::format_err!("Failed to read shard state accounts: {e}"))?;
            if let Some(shard_account) = accounts
                .account(account_id)
                .map_err(|e| anyhow::format_err!("Failed to read shard account: {e}"))?
            {
                let account = shard_account
                    .read_account()
                    .map_err(|e| anyhow::format_err!("Failed to read account: {e}"))?;
                get_account_routing_for_account(&account)
            } else {
                AccountRouting(DAppIdentifier(account_address.clone()), account_address)
            }
        };
        Ok(self.threads_table.is_match(account_routing, self.thread_id))
    }

    fn merge_threads_table(&mut self, another_state: &Self) -> anyhow::Result<()> {
        self.threads_table.merge(another_state.get_threads_table())
    }

    fn merge_dapp_id_tables(&mut self, another_state: &Self) -> anyhow::Result<()> {
        // TODO: need to think of how to merge dapp id tables, because accounts can be deleted and created in both threads
        // Possible solution is to store tuple (Option<Value>, timestamp) as a value and compare timestamps on merge.
        for (account_address, (dapp_id, lt)) in another_state.get_dapp_id_table() {
            self.dapp_id_table
                .entry(account_address.clone())
                .and_modify(|data| {
                    if data.1 < *lt {
                        *data = (dapp_id.clone(), lt.clone())
                    }
                })
                .or_insert((dapp_id.clone(), lt.clone()));
        }
        Ok(())
    }

    fn add_unprocessed_messages(&mut self, _messages: Vec<Self::Message>) {
        todo!()
    }

    fn get_messages_for_another_thread(
        &self,
        another_state: &Self,
    ) -> anyhow::Result<Vec<Self::Message>> {
        // TODO: We cant have a mut ref in this function, so have to decode shard state
        let shard_state = if let Some(state) = self.shard_state.shard_state.clone() {
            state
        } else {
            assert!(self.shard_state.shard_state_cell.is_some());
            let cell = self.shard_state.shard_state_cell.clone().unwrap();
            Arc::new(
                ShardStateUnsplit::construct_from_cell(cell)
                    .expect("Failed to deserialize shard state from cell"),
            )
        };
        let out_msg_queue_info = shard_state
            .read_out_msg_queue_info()
            .map_err(|e| anyhow::format_err!("Failed to read out msg queue: {e}"))?;
        let mut filtered_messages = vec![];
        out_msg_queue_info
            .out_queue()
            .iterate_objects(|enq_message| {
                let message = enq_message.read_out_msg()?.read_message()?;
                if let Some(dest_account_id) = message.int_dst_account_id() {
                    if another_state
                        .does_account_belong_to_the_state(&dest_account_id)
                        .map_err(|e| tvm_types::error!("{}", e))?
                    {
                        filtered_messages.push(WrappedMessage { message });
                    }
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iterate state out messages: {e}"))?;

        Ok(filtered_messages)
    }

    fn does_state_has_messages_to_other_threads(&self) -> anyhow::Result<bool> {
        // TODO: We cant have a mut ref in this function, so have to decode shard state
        let shard_state = if let Some(state) = self.shard_state.shard_state.clone() {
            state
        } else {
            assert!(self.shard_state.shard_state_cell.is_some());
            let cell = self.shard_state.shard_state_cell.clone().unwrap();
            Arc::new(
                ShardStateUnsplit::construct_from_cell(cell)
                    .expect("Failed to deserialize shard state from cell"),
            )
        };
        let out_msg_queue_info = shard_state
            .read_out_msg_queue_info()
            .map_err(|e| anyhow::format_err!("Failed to read out msg queue: {e}"))?;
        let mut result = false;

        // TODO: refactor this part for not to iterate the whole map
        out_msg_queue_info
            .out_queue()
            .iterate_objects(|enq_message| {
                let message = enq_message.read_out_msg()?.read_message()?;
                if let Some(dest_account_id) = message.int_dst_account_id() {
                    if !self
                        .does_account_belong_to_the_state(&dest_account_id)
                        .map_err(|e| tvm_types::error!("{}", e))?
                    {
                        result = true;
                    }
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iterate state out messages: {e}"))?;

        Ok(result)
    }
}
