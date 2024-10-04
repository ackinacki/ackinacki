// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;

use tvm_block::BlkPrevInfo;
use tvm_block::ShardStateUnsplit;

use super::repository_impl::RepositoryImpl;
#[cfg(test)]
use crate::account::MockAccount;
use crate::block::block_stub::BlockIdentifierStub;
use crate::block::Block;
use crate::block::MockBlockStruct;
use crate::block::WrappedUInt256;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::database::documents_db::SerializedItem;
#[cfg(test)]
use crate::message::message_stub::MessageStub;
use crate::node::associated_types::AttestationData;
use crate::node::NodeIdentifier;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::BlockIdentifierFor;
use crate::repository::BlockSeqNoFor;
use crate::repository::Repository;

#[cfg(test)]
#[derive(Clone)]
pub struct OptimisticStateStub {
    _key: u64,
    _acc: u16,
    _state: MockAccount,
}

#[cfg(test)]
impl OptimisticState for OptimisticStateStub {
    type Block = ();
    type BlockIdentifier = u64;
    type BlockInfo = BlkPrevInfo;
    type Cell = ();
    type Message = MessageStub;
    type ShardState = ShardStateUnsplit;

    fn get_remaining_ext_messages(
        &self,
        _repository: &RepositoryImpl,
    ) -> anyhow::Result<Vec<Self::Message>> {
        todo!()
    }

    fn get_block_id(&self) -> &Self::BlockIdentifier {
        todo!()
    }

    fn get_shard_state(&mut self) -> Self::ShardState {
        todo!()
    }

    fn get_block_info(&self) -> Self::BlockInfo {
        todo!()
    }

    fn get_shard_state_as_cell(&mut self) -> Self::Cell {
        todo!()
    }

    fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        todo!()
    }

    fn apply_block<T: Into<Self::Block>>(&mut self, _block_candidate: T) -> anyhow::Result<()> {
        todo!()
    }
}

#[cfg(test)]
pub struct RepositoryStub {
    _storage: HashMap<BlockIdentifierFor<Self>, Envelope<GoshBLS, MockBlockStruct>>,
    optimistic_state: HashMap<BlockIdentifierFor<Self>, <Self as Repository>::OptimisticState>,
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
    type Attestation = Envelope<GoshBLS, AttestationData<BlockIdentifierStub, u64>>;
    type BLS = GoshBLS;
    type Block = MockBlockStruct;
    type CandidateBlock = Envelope<GoshBLS, MockBlockStruct>;
    type EnvelopeSignerIndex = u16;
    type NodeIdentifier = NodeIdentifier;
    type OptimisticState = OptimisticStateStub;
    type StateSnapshot = OptimisticStateStub;
    type ThreadIdentifier = u64;

    fn dump_sent_attestations(
        &self,
        _data: HashMap<
            Self::ThreadIdentifier,
            Vec<(<Self::Block as Block>::BlockSeqNo, Self::Attestation)>,
        >,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn load_sent_attestations(
        &self,
    ) -> anyhow::Result<
        HashMap<
            Self::ThreadIdentifier,
            Vec<(<Self::Block as Block>::BlockSeqNo, Self::Attestation)>,
        >,
    > {
        todo!()
    }

    fn get_block(
        &self,
        _identifier: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<Self::CandidateBlock>> {
        todo!();
    }

    fn get_block_from_repo_or_archive(
        &self,
        _block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<<Self as Repository>::CandidateBlock> {
        todo!()
    }

    fn get_block_from_repo_or_archive_by_seq_no(
        &self,
        _block_seq_no: &BlockSeqNoFor<Self>,
    ) -> anyhow::Result<Vec<<Self as Repository>::CandidateBlock>> {
        todo!()
    }

    fn list_blocks_with_seq_no(
        &self,
        _seq_no: &BlockSeqNoFor<Self>,
    ) -> anyhow::Result<Vec<Envelope<GoshBLS, MockBlockStruct>>> {
        Ok(vec![])
    }

    fn select_thread_last_finalized_block(
        &self,
        _thread_id: &Self::ThreadIdentifier,
    ) -> anyhow::Result<(BlockIdentifierFor<Self>, BlockSeqNoFor<Self>)> {
        todo!();
    }

    fn select_thread_last_main_candidate_block(
        &self,
        _thread_id: &Self::ThreadIdentifier,
    ) -> anyhow::Result<(BlockIdentifierFor<Self>, BlockSeqNoFor<Self>)> {
        todo!();
    }

    fn mark_block_as_accepted_as_main_candidate(
        &self,
        _block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_block_accepted_as_main_candidate(
        &self,
        _block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<bool>> {
        Ok(None)
    }

    fn mark_block_as_finalized(
        &mut self,
        _block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_block_finalized(
        &self,
        _block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<bool>> {
        Ok(None)
    }

    fn get_optimistic_state(
        &self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<OptimisticStateStub>> {
        Ok(self.optimistic_state.get(block_id).map(|s| s.to_owned()))
    }

    fn is_optimistic_state_present(&self, _block_id: &BlockIdentifierFor<Self>) -> bool {
        todo!()
    }

    fn store_block<T: Into<Self::CandidateBlock>>(&self, _block: T) -> anyhow::Result<()> {
        Ok(())
    }

    fn erase_block_and_optimistic_state(
        &self,
        _block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<()> {
        todo!();
    }

    fn erase_block(&self, _block_id: &BlockIdentifierFor<Self>) -> anyhow::Result<()> {
        todo!()
    }

    // fn set_optimistic_as_is(&self, _optimistic: Self::OptimisticState) ->
    // anyhow::Result<()> { todo!()
    // }

    fn list_stored_thread_finalized_blocks(
        &self,
        _thread_id: &Self::ThreadIdentifier,
    ) -> anyhow::Result<Vec<(BlockIdentifierFor<Self>, BlockSeqNoFor<Self>)>> {
        todo!();
    }

    fn delete_external_messages(&self, _count: usize) -> anyhow::Result<()> {
        todo!()
    }

    fn add_external_message<T>(&mut self, _messages: Vec<T>) -> anyhow::Result<()>
    where
        T: Into<<Self::OptimisticState as OptimisticState>::Message>,
    {
        todo!()
    }

    fn mark_block_as_verified(&self, _block_id: &BlockIdentifierFor<Self>) -> anyhow::Result<()> {
        todo!()
    }

    fn is_block_verified(&self, _block_id: &BlockIdentifierFor<Self>) -> anyhow::Result<bool> {
        todo!()
    }

    fn take_state_snapshot(
        &self,
        _block_id: &BlockIdentifierFor<Self>,
        _block_producer_groups: HashMap<Self::ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        _block_keeper_set: BlockKeeperSet,
    ) -> anyhow::Result<Self::StateSnapshot> {
        todo!()
    }

    fn convert_state_data_to_snapshot(
        &self,
        _serialized_state: Vec<u8>,
        _block_producer_groups: HashMap<Self::ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        _block_keeper_set: BlockKeeperSet,
    ) -> anyhow::Result<Self::StateSnapshot> {
        todo!()
    }

    fn set_state_from_snapshot(
        &mut self,
        _block_id: &BlockIdentifierFor<Self>,
        _snapshot: Self::StateSnapshot,
    ) -> anyhow::Result<(HashMap<Self::ThreadIdentifier, Vec<Self::NodeIdentifier>>, BlockKeeperSet)>
    {
        todo!()
    }

    fn save_account_diffs(
        &self,
        _block_id: WrappedUInt256,
        _accounts: HashMap<String, SerializedItem>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn dump_verification_data<
        TBlock: Into<Self::CandidateBlock>,
        TState: Into<Self::OptimisticState>,
    >(
        &self,
        _prev_block: TBlock,
        _prev_state: TState,
        _cur_block: TBlock,
        _failed_block: TBlock,
        _failed_state: TState,
        _incoming_state: TState,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn last_stored_block_by_seq_no(&self) -> anyhow::Result<BlockSeqNoFor<Self>> {
        todo!()
    }

    fn store_optimistic<T: Into<Self::OptimisticState>>(
        &mut self,
        _state: T,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn get_block_id_by_seq_no(
        &self,
        _block_seq_no: &BlockSeqNoFor<Self>,
    ) -> Vec<BlockIdentifierFor<Self>> {
        todo!()
    }

    fn get_latest_block_id_with_producer_group_change(
        &self,
        _thread_id: &Self::ThreadIdentifier,
    ) -> anyhow::Result<BlockIdentifierFor<Self>> {
        todo!()
    }

    fn clear_ext_messages_queue_by_time(&self) -> anyhow::Result<()> {
        todo!()
    }

    fn saved_optimistic_states(
        &self,
    ) -> BTreeMap<<Self::Block as Block>::BlockSeqNo, <Self::Block as Block>::BlockIdentifier> {
        todo!()
    }

    fn clear_verification_markers(
        &self,
        _starting_block_id: &BlockSeqNoFor<Self>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn get_finalized_state(&self) -> Self::OptimisticState {
        todo!()
    }

    fn get_finalized_state_as_mut(&mut self) -> &mut Self::OptimisticState {
        todo!()
    }
}
