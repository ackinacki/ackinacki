// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use tracing::instrument;
use tvm_block::Augmentation;
use tvm_block::Deserializable;
use tvm_block::EnqueuedMsg;
use tvm_block::GetRepresentationHash;
use tvm_block::HashmapAugType;
use tvm_block::MsgEnvelope;
use tvm_block::OutMsgQueueKey;
use tvm_block::Serializable;
use tvm_block::ShardStateUnsplit;
use tvm_types::AccountId;
use tvm_types::Cell;
use tvm_types::UInt256;
use typed_builder::TypedBuilder;

use super::accounts::AccountsRepository;
use super::optimistic_shard_state::OptimisticShardState;
use crate::block::postprocessing::postprocess;
use crate::block::verify::prepare_prev_block_info;
use crate::block_keeper_system::wallet_config::create_wallet_slash_message;
use crate::block_keeper_system::BlockKeeperSlashData;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::message::identifier::MessageIdentifier;
use crate::message::Message;
use crate::message::WrappedMessage;
use crate::message_storage::MessageDurableStorage;
use crate::multithreading::cross_thread_messaging::thread_references_state::ThreadReferencesState;
use crate::multithreading::shard_state_operations::crop_shard_state_based_on_threads_table;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::shared_services::SharedServices;
use crate::repository::CrossThreadRefData;
use crate::repository::CrossThreadRefDataRead;
use crate::types::thread_message_queue::ThreadMessageQueueState;
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
use crate::utilities::FixedSizeHashSet;

pub trait OptimisticState: Send + Clone {
    type Cell;
    type Message: Message;
    type ShardState;

    fn get_share_stare_refs(&self) -> HashMap<ThreadIdentifier, BlockIdentifier>;
    fn get_block_seq_no(&self) -> &BlockSeqNo;
    fn get_block_id(&self) -> &BlockIdentifier;
    fn get_shard_state(&mut self) -> Self::ShardState;
    fn get_shard_state_as_cell(&mut self) -> Self::Cell;
    fn get_block_info(&self) -> &BlockInfo;
    fn serialize_into_buf(self) -> anyhow::Result<Vec<u8>>;
    fn apply_block(
        &mut self,
        block_candidate: &AckiNackiBlock,
        shared_services: &SharedServices,
        block_state_repo: BlockStateRepository,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        accounts_repo: AccountsRepository,
        message_db: MessageDurableStorage,
    ) -> anyhow::Result<(
        CrossThreadRefData,
        HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    )>;
    fn get_thread_id(&self) -> &ThreadIdentifier;
    fn get_produced_threads_table(&self) -> &ThreadsTable;
    fn set_produced_threads_table(&mut self, table: ThreadsTable);
    fn crop(
        &mut self,
        thread_identifier: &ThreadIdentifier,
        threads_table: &ThreadsTable,
        message_db: MessageDurableStorage,
    ) -> anyhow::Result<()>;
    fn get_account_routing<T>(&mut self, account_id: &T) -> anyhow::Result<AccountRouting>
    where
        T: Clone + Into<AccountAddress>;
    fn get_thread_for_account(
        &mut self,
        account_id: &AccountId,
    ) -> anyhow::Result<ThreadIdentifier>;
    fn does_routing_belong_to_the_state(
        &mut self,
        account_routing: &AccountRouting,
    ) -> anyhow::Result<bool>;
    fn does_account_belong_to_the_state(&mut self, account_id: &AccountId) -> anyhow::Result<bool>;
    fn get_dapp_id_table(&self) -> &HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)>;
    fn merge_dapp_id_tables(&mut self, another_state: &DAppIdTable) -> anyhow::Result<()>;
    fn get_internal_message_queue_length(&mut self) -> usize;
    fn does_state_has_messages_to_other_threads(&mut self) -> anyhow::Result<bool>;
    fn add_slashing_messages(
        &mut self,
        slashing_messages: Vec<Arc<Self::Message>>,
    ) -> anyhow::Result<()>;
    fn get_thread_refs(&self) -> &ThreadReferencesState;
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CrossThreadMessageData {
    // ID of the block that produced message
    pub block_id: BlockIdentifier,
    // Source account routing (used for crop/split)
    pub src_account_routing: AccountRouting,
    // Destination account routing (can be used to track message)
    pub dest_account_routing: AccountRouting,
}

pub type DAppIdTable = HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)>;

#[serde_as]
#[derive(Clone, TypedBuilder, Serialize, Deserialize)]
pub struct OptimisticStateImpl {
    pub(crate) block_seq_no: BlockSeqNo,
    pub(crate) block_id: BlockIdentifier,
    #[builder(setter(into))]
    pub(crate) shard_state: OptimisticShardState,
    pub(crate) messages: ThreadMessageQueueState,
    pub(crate) high_priority_messages: ThreadMessageQueueState,

    #[builder(setter(into))]
    pub block_info: BlockInfo,
    pub threads_table: ThreadsTable,
    pub thread_id: ThreadIdentifier,
    // Value is a tuple (Option<DAppIdentifier>, <end_lt of the block it was changed in>)
    // TODO: we must clear this table after account was removed from all threads and finalized.
    // TODO: LT usage can be ambiguous because lt in different threads can change with different speed.
    pub dapp_id_table: DAppIdTable,
    pub thread_refs_state: ThreadReferencesState,

    pub cropped: Option<(ThreadIdentifier, ThreadsTable)>,

    #[serde(skip)]
    pub changed_accounts: HashMap<UInt256, BlockSeqNo>,
    #[serde(skip)]
    pub cached_accounts: HashMap<UInt256, (BlockSeqNo, Cell)>,
}

impl Debug for OptimisticStateImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("")
            .field("block_seq_no", &self.block_seq_no)
            .field("block_id", &self.block_id)
            .field("shard_state", &self.shard_state)
            .field("block_info", &self.block_info)
            .field("threads_table", &self.threads_table)
            .field("thread_id", &self.thread_id)
            // .field("dapp_id_table", &self.dapp_id_table)
            .field("cropped", &self.cropped)
            .field("thread_refs_state", &self.thread_refs_state)
            .finish()
    }
}

impl Default for OptimisticStateImpl {
    fn default() -> Self {
        Self::zero()
    }
}

impl OptimisticStateImpl {
    pub fn deserialize_from_buf(data: &[u8]) -> anyhow::Result<Self> {
        let state: Self = bincode::deserialize(data)?;
        Ok(state)
    }

    pub fn zero() -> Self {
        Self {
            block_seq_no: BlockSeqNo::default(),
            block_id: BlockIdentifier::default(),
            shard_state: OptimisticShardState::default(),
            block_info: BlockInfo::default(),
            threads_table: ThreadsTable::default(),
            thread_id: ThreadIdentifier::default(),
            dapp_id_table: HashMap::new(),
            thread_refs_state: ThreadReferencesState::builder()
                .all_thread_refs(HashMap::from_iter(vec![(
                    ThreadIdentifier::default(),
                    (
                        ThreadIdentifier::default(),
                        BlockIdentifier::default(),
                        BlockSeqNo::default(),
                    )
                        .into(),
                )]))
                .build(),
            cropped: None,
            messages: ThreadMessageQueueState::empty(),
            high_priority_messages: ThreadMessageQueueState::empty(),
            changed_accounts: Default::default(),
            cached_accounts: Default::default(),
        }
    }

    pub fn set_shard_state(&mut self, new_state: <Self as OptimisticState>::ShardState) {
        self.shard_state = OptimisticShardState::from(new_state);
    }

    #[cfg(test)]
    pub fn stub(_id: BlockIdentifier) -> Self {
        Self::zero()
    }

    fn load_changed_accounts(
        &self,
        state: &mut ShardStateUnsplit,
        block: &tvm_block::Block,
        accounts_repo: AccountsRepository,
    ) -> anyhow::Result<HashSet<UInt256>> {
        let mut accounts = state
            .read_accounts()
            .map_err(|e| anyhow::format_err!("Failed to read shard state accounts: {e}"))?;
        let extra = block
            .read_extra()
            .map_err(|e| anyhow::format_err!("Failed to read block extra: {e}"))?;
        let block_accounts = extra
            .read_account_blocks()
            .map_err(|e| anyhow::format_err!("Failed to read account blocks: {e}"))?;
        let mut changed_accounts = HashSet::new();
        block_accounts.iterate_slices_with_keys(|account_id, _| {
            if let Some(mut shard_acc) = accounts.account(&(&account_id).into())? {
                if shard_acc.is_external() {
                    let acc_root = match self.cached_accounts.get(&account_id) {
                        Some((_, cell)) => cell.clone(),
                        None => accounts_repo.load_account(&account_id, shard_acc.last_trans_hash(), shard_acc.last_trans_lt()).map_err(|err| tvm_types::error!("{}", err))?
                    };
                    if acc_root.repr_hash() != shard_acc.account_cell().repr_hash() {
                        return Err(tvm_types::error!("External account {account_id} cell hash mismatch: required: {}, actual: {}", acc_root.repr_hash(), shard_acc.account_cell().repr_hash()));
                    }
                    shard_acc.set_account_cell(acc_root);
                    accounts.insert(&account_id, &shard_acc)?;
                }
            }
            changed_accounts.insert(account_id);
            Ok(true)
        }).map_err(|e| anyhow::format_err!("Failed to iterate changed accounts: {e}"))?;
        state
            .write_accounts(&accounts)
            .map_err(|e| anyhow::format_err!("Failed to write shard state accounts: {e}"))?;
        Ok(changed_accounts)
    }
}

impl OptimisticState for OptimisticStateImpl {
    type Cell = Cell;
    type Message = WrappedMessage;
    type ShardState = Arc<ShardStateUnsplit>;

    fn get_share_stare_refs(&self) -> HashMap<ThreadIdentifier, BlockIdentifier> {
        let mut thread_refs: HashMap<ThreadIdentifier, BlockIdentifier> = self
            .thread_refs_state
            .all_thread_refs()
            .iter()
            .map(|(thread_id, ref_block)| (*thread_id, ref_block.block_identifier.clone()))
            .collect();
        thread_refs.insert(self.thread_id, self.block_id.clone());
        thread_refs
    }

    fn get_dapp_id_table(&self) -> &HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)> {
        &self.dapp_id_table
    }

    fn get_internal_message_queue_length(&mut self) -> usize {
        self.messages.length()
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

    fn serialize_into_buf(self) -> anyhow::Result<Vec<u8>> {
        let buffer: Vec<u8> = bincode::serialize(&self)?;
        Ok(buffer)
    }

    fn apply_block(
        &mut self,
        block_candidate: &AckiNackiBlock,
        shared_services: &SharedServices,
        block_state_repo: BlockStateRepository,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        accounts_repo: AccountsRepository,
        message_db: MessageDurableStorage,
    ) -> anyhow::Result<(
        CrossThreadRefData,
        HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    )> {
        // TODO: Critical. Add refs. support + notes
        // Note: the way we store state only on some blocks will not work
        // since it will require other states to be restored on own restore
        // and it can chain forever.

        let block_id = block_candidate.identifier();
        tracing::trace!("Applying block: {:?}", block_id);
        tracing::trace!("Check parent: {:?} ?= {:?}", self.block_id, block_candidate.parent());
        assert_eq!(
            self.block_id,
            block_candidate.parent(),
            "Tried to apply block that is not child"
        );

        let block_nack = block_candidate.get_common_section().nacks.clone();
        let mut wrapped_slash_messages = vec![];
        for nack in block_nack.iter() {
            tracing::trace!("push nack into slash {:?}", nack);
            let reason = nack.data().reason.clone();
            if let Some((id, bls_key, addr)) = reason.get_node_data(block_state_repo.clone()) {
                let epoch_nack_data = BlockKeeperSlashData {
                    node_id: id,
                    bls_pubkey: bls_key,
                    addr: addr.0,
                    slash_type: 0,
                };
                let msg = create_wallet_slash_message(&epoch_nack_data)?;
                let wrapped_message = WrappedMessage { message: msg.clone() };
                wrapped_slash_messages.push(Arc::new(wrapped_message));
            }
        }
        let start = std::time::Instant::now();
        // let _ = self.last_processed_external_message_index
        //     + block_candidate.processed_ext_messages_cnt() as u32;
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
        // DEBUG
        // tracing::trace!(
        // "Preprocessing state for thread {}. before: {:?}",
        // &block_candidate.get_common_section().thread_id,
        // self.get_shard_state_as_cell(),
        // );
        let mut shared_services = shared_services.clone();
        tracing::trace!(
            "apply_block: {:?}, refs: {:?}",
            block_candidate.identifier(),
            block_candidate.get_common_section().refs
        );
        let (refs, cross_thread_ref_data_repo) = shared_services.exec(|e| {
            let mut refs = vec![];
            for block_id in &block_candidate.get_common_section().refs {
                let state = e
                    .cross_thread_ref_data_service
                    .get_cross_thread_ref_data(block_id)
                    .expect("Failed to load ref state");
                refs.push(state);
            }
            (refs, e.cross_thread_ref_data_service.clone())
        });

        let preprocessing_result = crate::block::preprocessing::preprocess(
            self.clone(),
            refs.iter(),
            &block_candidate.get_common_section().thread_id,
            &cross_thread_ref_data_repo,
            wrapped_slash_messages,
            Vec::new(),
            message_db.clone(),
        )?;
        // todo!("Use this to init outbox {:?}", forwarded_messages);
        *self = preprocessing_result.state;
        let mut prev_state = self.get_shard_state().deref().clone();
        let changed = self.load_changed_accounts(
            &mut prev_state,
            block_candidate.tvm_block(),
            accounts_repo.clone(),
        )?;
        let prev_state = prev_state
            .serialize()
            .map_err(|e| anyhow::format_err!("Failed to serialize state: {e}"))?;
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
        let shard_state = Arc::new(
            ShardStateUnsplit::construct_from_cell(new_state.clone())
                .expect("Failed to deserialize shard state from cell"),
        );
        let (
            consumed_internal_messages,
            produced_internal_messages_to_the_current_thread,
            accounts_that_changed_their_dapp_id,
            produced_internal_messages_to_other_threads,
        ) = block_candidate.get_data_for_postprocessing(self, shard_state.clone())?;

        let old_dapp_id_table = self.dapp_id_table.clone();

        let mut all_added_messages = preprocessing_result.settled_messages;
        all_added_messages.extend(produced_internal_messages_to_the_current_thread.clone());
        all_added_messages.extend(HashMap::<
            AccountAddress,
            Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
        >::from_iter(
            produced_internal_messages_to_other_threads
                .iter()
                .map(|(k, v)| (k.1.clone(), v.clone())),
        ));

        let threads_table = if let Some(table) = &block_candidate.get_common_section().threads_table
        {
            table.clone()
        } else {
            self.threads_table.clone()
        };

        let (mut new_state, cross_thread_ref_data) = postprocess(
            self.clone(),
            consumed_internal_messages,
            produced_internal_messages_to_the_current_thread,
            accounts_that_changed_their_dapp_id,
            block_candidate.identifier(),
            block_candidate.seq_no(),
            (shard_state, new_state).into(),
            produced_internal_messages_to_other_threads,
            old_dapp_id_table,
            block_info,
            block_candidate.get_common_section().thread_id,
            threads_table,
            block_candidate.get_common_section().changed_dapp_ids.clone(),
            changed,
            accounts_repo,
            message_db.clone(),
        )?;
        // merge with update from block
        new_state.merge_dapp_id_tables(&block_candidate.get_common_section().changed_dapp_ids)?;
        *self = new_state;

        let nacks = block_candidate.get_common_section().clone().nacks;
        let mut nack_set_cache_in = nack_set_cache.lock();
        for nack in nacks {
            if let Ok(nack_hash) = nack.data().clone().reason.get_hash_nack() {
                if !nack_set_cache_in.contains(&nack_hash) {
                    nack_set_cache_in.insert(nack_hash.clone());
                }
            }
        }
        drop(nack_set_cache_in);
        #[cfg(feature = "timing")]
        tracing::trace!("Apply block {block_id:?} time: {} ms", start.elapsed().as_millis());

        shared_services.metrics.inspect(|m| {
            m.report_block_apply_time(
                start.elapsed().as_millis() as u64,
                &block_candidate.get_common_section().thread_id,
            );
        });

        Ok((cross_thread_ref_data, all_added_messages))
    }

    fn get_thread_id(&self) -> &ThreadIdentifier {
        &self.thread_id
    }

    fn get_produced_threads_table(&self) -> &ThreadsTable {
        &self.threads_table
    }

    fn set_produced_threads_table(&mut self, table: ThreadsTable) {
        if let Some(crop_state) = &self.cropped {
            if crop_state.1 != table {
                self.cropped = None;
            }
        }
        self.threads_table = table;
    }

    // TODO: need to crop self.messages
    #[instrument(skip_all)]
    fn crop(
        &mut self,
        thread_identifier: &ThreadIdentifier,
        threads_table: &ThreadsTable,
        message_db: MessageDurableStorage,
    ) -> anyhow::Result<()> {
        let crop_state = Some((*thread_identifier, threads_table.clone()));
        if crop_state == self.cropped {
            return Ok(());
        }
        let optimization_skip_shard_accounts_crop = false;
        let initial_state = self.get_shard_state();
        let mut removed_accounts = vec![];
        // Get all message destinations
        let mut message_destinations_that_do_not_exist = self.messages.destinations().to_set();
        // crop_shard_state_based_on_threads_table will remove all existing account from message_destinations_that_do_not_exist
        let filtered_state = crop_shard_state_based_on_threads_table(
            initial_state,
            threads_table,
            *thread_identifier,
            &self.dapp_id_table,
            self.block_id.clone(),
            optimization_skip_shard_accounts_crop,
            &mut removed_accounts,
            |acc_id| {
                message_destinations_that_do_not_exist.remove(&acc_id.clone().into());
            },
        )?;

        for account_address in message_destinations_that_do_not_exist {
            let account_routing =
                AccountRouting(DAppIdentifier(account_address.clone()), account_address.clone());
            if !threads_table.is_match(&account_routing, *thread_identifier) {
                removed_accounts.push(account_address);
            }
        }

        tracing::trace!(
            "Cropping by thread: {thread_identifier:?}. table: {threads_table:?}. Number of removed accounts: {}",
            removed_accounts.len()
        );

        self.shard_state = OptimisticShardState::from(filtered_state);
        self.messages = ThreadMessageQueueState::build_next()
            .with_initial_state(self.messages.clone())
            .with_consumed_messages(HashMap::new())
            .with_removed_accounts(removed_accounts)
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(HashMap::new())
            .with_db(message_db.clone())
            .build()?;

        self.cropped = crop_state;
        self.threads_table = threads_table.clone();
        self.thread_id = *thread_identifier;
        Ok(())
    }

    // TODO: can't return error
    fn get_account_routing<T>(&mut self, account_id: &T) -> anyhow::Result<AccountRouting>
    where
        T: Into<AccountAddress> + Clone,
    {
        let account_address = account_id.clone().into();
        Ok(if let Some(dapp_id) = self.dapp_id_table.get(&account_address) {
            match &dapp_id.0 {
                Some(dapp_id) => AccountRouting(dapp_id.clone(), account_address.clone()),
                None => {
                    AccountRouting(DAppIdentifier(account_address.clone()), account_address.clone())
                }
            }
        } else {
            AccountRouting(DAppIdentifier(account_address.clone()), account_address.clone())
        })
    }

    fn get_thread_for_account(
        &mut self,
        account_id: &AccountId,
    ) -> anyhow::Result<ThreadIdentifier> {
        let account_routing = self.get_account_routing(account_id)?;
        Ok(self.threads_table.find_match(&account_routing))
    }

    fn does_routing_belong_to_the_state(
        &mut self,
        account_routing: &AccountRouting,
    ) -> anyhow::Result<bool> {
        Ok(self.threads_table.is_match(account_routing, self.thread_id))
    }

    fn does_account_belong_to_the_state(&mut self, account_id: &AccountId) -> anyhow::Result<bool> {
        let account_routing = self.get_account_routing(account_id)?;
        Ok(self.threads_table.is_match(&account_routing, self.thread_id))
    }

    fn merge_dapp_id_tables(&mut self, another_state: &DAppIdTable) -> anyhow::Result<()> {
        // TODO: need to think of how to merge dapp id tables, because accounts can be deleted and created in both threads
        // Possible solution is to store tuple (Option<Value>, timestamp) as a value and compare timestamps on merge.
        for (account_address, (dapp_id, lt)) in another_state {
            self.dapp_id_table
                .entry(account_address.clone())
                .and_modify(|data| {
                    if data.1 < *lt {
                        *data = (dapp_id.clone(), lt.clone())
                    }
                })
                .or_insert_with(|| (dapp_id.clone(), lt.clone()));
        }
        Ok(())
    }

    fn does_state_has_messages_to_other_threads(&mut self) -> anyhow::Result<bool> {
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

    fn add_slashing_messages(
        &mut self,
        slashing_messages: Vec<Arc<Self::Message>>,
    ) -> anyhow::Result<()> {
        let mut shard_state = self.get_shard_state().deref().clone();
        let mut out_queue_info = shard_state
            .read_out_msg_queue_info()
            .map_err(|e| anyhow::format_err!("Failed to read out msg queue: {e}"))?;
        for (index, message) in slashing_messages.into_iter().enumerate() {
            let msg = message.message.clone();
            let info = msg.int_header().unwrap();
            let fwd_fee = info.fwd_fee();
            let msg_cell = msg
                .serialize()
                .map_err(|e| anyhow::format_err!("Failed to serialize message: {e}"))?;
            let env = MsgEnvelope::with_message_and_fee(&msg, *fwd_fee)
                .map_err(|e| anyhow::format_err!("Failed to create message envelope: {e}"))?;
            // Note: replace message created_lt to process slashing messages first
            let enq = EnqueuedMsg::with_param(index as u64 + 1, &env)
                .map_err(|e| anyhow::format_err!("Failed to make enqueued message: {e}"))?;
            let prefix = msg
                .int_dst_account_id()
                .unwrap()
                .clone()
                .get_next_u64()
                .map_err(|e| anyhow::format_err!("Failed to generate message prefix: {e}"))?;
            let key = OutMsgQueueKey::with_workchain_id_and_prefix(
                shard_state.shard().workchain_id(),
                prefix,
                msg_cell.repr_hash(),
            );
            tracing::trace!(
                "OptimisticState: add slashing message: {index} {} {}",
                msg.hash().unwrap().to_hex_string(),
                key.hash.to_hex_string()
            );
            out_queue_info
                .out_queue_mut()
                .set(
                    &key,
                    &enq,
                    &enq.aug()
                        .map_err(|e| anyhow::format_err!("Failed to generate message aug: {e}"))?,
                )
                .map_err(|e| anyhow::format_err!("Failed to put message to out queue: {e}"))?;
        }
        shard_state
            .write_out_msg_queue_info(&out_queue_info)
            .map_err(|e| anyhow::format_err!("Failed to put message to out queue: {e}"))?;
        self.set_shard_state(Arc::new(shard_state));
        Ok(())
    }

    fn get_thread_refs(&self) -> &ThreadReferencesState {
        &self.thread_refs_state
    }
}
