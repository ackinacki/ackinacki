// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use database::documents_db::SerializedItem;
use parking_lot::Mutex;
use tvm_block::ShardStateUnsplit;
use tvm_types::AccountId;
use tvm_types::UInt256;

use super::accounts::AccountsRepository;
use super::repository_impl::RepositoryMetadata;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::message::identifier::MessageIdentifier;
#[cfg(test)]
use crate::message::message_stub::MessageStub;
use crate::message::WrappedMessage;
use crate::message_storage::MessageDurableStorage;
use crate::multithreading::cross_thread_messaging::thread_references_state::ThreadReferencesState;
use crate::node::associated_types::AttestationData;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::shared_services::SharedServices;
use crate::node::NodeIdentifier;
use crate::repository::optimistic_state::DAppIdTable;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;
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

#[cfg(test)]
#[derive(Clone)]
pub struct OptimisticStateStub {
    _key: u64,
    _acc: u16,
    _state: tvm_block::Account,
}

#[cfg(test)]
impl OptimisticState for OptimisticStateStub {
    type Cell = ();
    type Message = MessageStub;
    type ShardState = ShardStateUnsplit;

    fn get_share_stare_refs(&self) -> HashMap<ThreadIdentifier, BlockIdentifier> {
        todo!()
    }

    fn get_block_seq_no(&self) -> &BlockSeqNo {
        todo!()
    }

    fn get_block_id(&self) -> &BlockIdentifier {
        todo!()
    }

    fn serialize_into_buf(self) -> anyhow::Result<Vec<u8>> {
        todo!()
    }

    fn get_shard_state(&mut self) -> Self::ShardState {
        todo!()
    }

    fn get_block_info(&self) -> &BlockInfo {
        todo!()
    }

    fn get_shard_state_as_cell(&mut self) -> Self::Cell {
        todo!()
    }

    fn apply_block(
        &mut self,
        _block_candidate: &AckiNackiBlock,
        _shared_services: &SharedServices,
        _block_state_repo: BlockStateRepository,
        _nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        _accounts_repo: AccountsRepository,
        _message_db: MessageDurableStorage,
    ) -> anyhow::Result<(
        CrossThreadRefData,
        HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    )> {
        todo!()
    }

    fn get_thread_id(&self) -> &ThreadIdentifier {
        todo!()
    }

    fn get_produced_threads_table(&self) -> &ThreadsTable {
        todo!()
    }

    fn set_produced_threads_table(&mut self, _table: ThreadsTable) {
        todo!()
    }

    fn crop(
        &mut self,
        _thread_identifier: &ThreadIdentifier,
        _threads_table: &ThreadsTable,
        _message_db: MessageDurableStorage,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn get_account_routing<T: Into<AccountAddress> + Clone>(
        &mut self,
        _account_id: &T,
    ) -> anyhow::Result<AccountRouting> {
        todo!()
    }

    fn get_thread_for_account(
        &mut self,
        _account_id: &AccountId,
    ) -> anyhow::Result<ThreadIdentifier> {
        todo!()
    }

    fn does_routing_belong_to_the_state(
        &mut self,
        _account_routing: &AccountRouting,
    ) -> anyhow::Result<bool> {
        todo!()
    }

    fn does_account_belong_to_the_state(
        &mut self,
        _account_id: &AccountId,
    ) -> anyhow::Result<bool> {
        todo!()
    }

    fn get_dapp_id_table(&self) -> &HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)> {
        todo!()
    }

    fn merge_dapp_id_tables(&mut self, _another_state: &DAppIdTable) -> anyhow::Result<()> {
        todo!()
    }

    fn get_internal_message_queue_length(&mut self) -> usize {
        todo!()
    }

    fn does_state_has_messages_to_other_threads(&mut self) -> anyhow::Result<bool> {
        todo!()
    }

    // fn add_messages_from_ref(
    // &mut self,
    // _cross_thread_ref: &CrossThreadRefData,
    // ) -> anyhow::Result<()> {
    // todo!()
    // }
    fn add_slashing_messages(
        &mut self,
        _slashing_messages: Vec<Arc<Self::Message>>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    // fn add_accounts_from_ref(
    // &mut self,
    // _cross_thread_ref: &CrossThreadRefData,
    // ) -> anyhow::Result<()> {
    // todo!()
    // }

    fn get_thread_refs(&self) -> &ThreadReferencesState {
        todo!()
    }
}

#[cfg(test)]
pub struct RepositoryStub {
    _storage: HashMap<BlockIdentifier, Envelope<GoshBLS, AckiNackiBlock>>,
    optimistic_state: HashMap<BlockIdentifier, <Self as Repository>::OptimisticState>,
}

#[cfg(test)]
impl Default for RepositoryStub {
    fn default() -> Self {
        Self::new()
    }
}

impl RepositoryStub {
    pub fn new() -> Self {
        Self { _storage: HashMap::new(), optimistic_state: HashMap::new() }
    }
}

#[cfg(test)]
impl From<Vec<u8>> for OptimisticStateStub {
    fn from(_value: Vec<u8>) -> Self {
        todo!()
    }
}

#[cfg(test)]
impl From<OptimisticStateStub> for Vec<u8> {
    fn from(_val: OptimisticStateStub) -> Self {
        todo!()
    }
}

#[cfg(test)]
impl Repository for RepositoryStub {
    type Attestation = Envelope<GoshBLS, AttestationData>;
    type BLS = GoshBLS;
    type CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>;
    type EnvelopeSignerIndex = u16;
    type NodeIdentifier = NodeIdentifier;
    type OptimisticState = OptimisticStateStub;
    type StateSnapshot = OptimisticStateStub;

    fn get_message_db(&self) -> MessageDurableStorage {
        todo!()
    }

    fn has_thread_metadata(&self, _thread_id: &ThreadIdentifier) -> bool {
        todo!()
    }

    fn load_sent_attestations(
        &self,
    ) -> anyhow::Result<HashMap<ThreadIdentifier, Vec<(BlockSeqNo, Self::Attestation)>>> {
        todo!()
    }

    fn get_block(
        &self,
        _identifier: &BlockIdentifier,
    ) -> anyhow::Result<Option<Arc<Self::CandidateBlock>>> {
        todo!();
    }

    fn get_block_from_repo_or_archive(
        &self,
        _block_id: &BlockIdentifier,
    ) -> anyhow::Result<Arc<<Self as Repository>::CandidateBlock>> {
        todo!()
    }

    fn get_block_from_repo_or_archive_by_seq_no(
        &self,
        _block_seq_no: &BlockSeqNo,
        _thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<<Self as Repository>::CandidateBlock>> {
        todo!()
    }

    fn init_thread(
        &mut self,
        _thread_id: &ThreadIdentifier,
        _parent_block_id: &BlockIdentifier,
    ) -> anyhow::Result<()> {
        todo!();
    }

    fn select_thread_last_finalized_block(
        &self,
        _thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Option<(BlockIdentifier, BlockSeqNo)>> {
        todo!();
    }

    fn is_block_suspicious(&self, _block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>> {
        todo!()
    }

    fn mark_block_as_finalized(
        &mut self,
        _block: &Self::CandidateBlock,
        _block_state: BlockState,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_block_finalized(&self, _block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>> {
        Ok(None)
    }

    fn get_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
        _thread_id: &ThreadIdentifier,
        _min_seq_no: Option<OptimisticStateStub>,
    ) -> anyhow::Result<Option<OptimisticStateStub>> {
        Ok(self.optimistic_state.get(block_id).map(|s| s.to_owned()))
    }

    fn get_full_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
        _thread_id: &ThreadIdentifier,
        _min_state: Option<Self::OptimisticState>,
    ) -> anyhow::Result<Option<Self::OptimisticState>> {
        Ok(self.optimistic_state.get(block_id).map(|s| s.to_owned()))
    }

    fn erase_block_and_optimistic_state(
        &self,
        _block_id: &BlockIdentifier,
        _thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        todo!();
    }

    fn erase_block(
        &self,
        _block_id: &BlockIdentifier,
        _thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        todo!()
    }

    // fn set_optimistic_as_is(&self, _optimistic: Self::OptimisticState) ->
    // anyhow::Result<()> { todo!()
    // }

    fn is_block_already_applied(&self, _block_id: &BlockIdentifier) -> anyhow::Result<bool> {
        todo!()
    }

    fn set_state_from_snapshot(
        &mut self,
        _snapshot: Self::StateSnapshot,
        _thread_id: &ThreadIdentifier,
        _skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn sync_accounts_from_state(
        &mut self,
        _shard_state: Arc<ShardStateUnsplit>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn save_account_diffs(
        &self,
        _block_id: BlockIdentifier,
        _accounts: HashMap<String, SerializedItem>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn store_optimistic<T: Into<Self::OptimisticState>>(&self, _state: T) -> anyhow::Result<()> {
        todo!()
    }

    fn store_optimistic_in_cache<T: Into<Self::OptimisticState>>(
        &self,
        _state: T,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn get_latest_block_id_with_producer_group_change(
        &self,
        _thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<BlockIdentifier> {
        todo!()
    }

    fn clear_verification_markers(
        &self,
        _starting_block_id: &BlockSeqNo,
        _thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn get_zero_state_for_thread(
        &self,
        _thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Self::OptimisticState> {
        todo!()
    }

    fn add_thread_buffer(&self, _thread_id: ThreadIdentifier) -> Arc<Mutex<Vec<BlockIdentifier>>> {
        todo!()
    }

    fn get_all_metadata(&self) -> RepositoryMetadata {
        todo!()
    }

    fn last_finalized_optimistic_state(
        &self,
        _thread_id: &ThreadIdentifier,
    ) -> Option<Self::OptimisticState> {
        todo!()
    }
}
