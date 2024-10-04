// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

use crate::block::Block;
use crate::block::WrappedUInt256;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::BLSSignatureScheme;
use crate::database::documents_db::SerializedItem;
use crate::node::associated_types::AttestationData;
use crate::repository::optimistic_state::OptimisticState;

pub mod optimistic_state;
pub mod repository_impl;

#[cfg(test)]
pub mod stub_repository;

pub(crate) type BlockIdentifierFor<Repo> = <<Repo as Repository>::Block as Block>::BlockIdentifier;
pub(crate) type BlockSeqNoFor<Repo> = <<Repo as Repository>::Block as Block>::BlockSeqNo;

pub trait Repository {
    type BLS: BLSSignatureScheme;
    type Block: Block + Serialize + for<'a> Deserialize<'a>;
    type CandidateBlock: BLSSignedEnvelope<
        SignerIndex = Self::EnvelopeSignerIndex,
        BLS = Self::BLS,
        Data = Self::Block,
    >;
    type EnvelopeSignerIndex;
    type NodeIdentifier;
    type OptimisticState: OptimisticState;
    type Attestation: BLSSignedEnvelope<
        SignerIndex = Self::EnvelopeSignerIndex,
        BLS = Self::BLS,
        Data = AttestationData<
            <Self::Block as Block>::BlockIdentifier,
            <Self::Block as Block>::BlockSeqNo,
        >,
    >;
    type StateSnapshot: From<Vec<u8>> + Into<Vec<u8>>;
    type ThreadIdentifier;

    fn get_block(
        &self,
        identifier: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<Self::CandidateBlock>>;

    fn get_block_from_repo_or_archive(
        &self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<<Self as Repository>::CandidateBlock>;

    fn get_block_from_repo_or_archive_by_seq_no(
        &self,
        block_seq_no: &BlockSeqNoFor<Self>,
    ) -> anyhow::Result<Vec<<Self as Repository>::CandidateBlock>>;

    fn list_blocks_with_seq_no(
        &self,
        seq_no: &BlockSeqNoFor<Self>,
    ) -> anyhow::Result<Vec<Self::CandidateBlock>>;

    fn clear_verification_markers(
        &self,
        starting_block_id: &BlockSeqNoFor<Self>,
    ) -> anyhow::Result<()>;

    fn select_thread_last_finalized_block(
        &self,
        thread_id: &Self::ThreadIdentifier,
    ) -> anyhow::Result<(BlockIdentifierFor<Self>, BlockSeqNoFor<Self>)>;

    fn select_thread_last_main_candidate_block(
        &self,
        thread_id: &Self::ThreadIdentifier,
    ) -> anyhow::Result<(BlockIdentifierFor<Self>, BlockSeqNoFor<Self>)>;

    fn mark_block_as_accepted_as_main_candidate(
        &self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<()>;

    fn is_block_accepted_as_main_candidate(
        &self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<bool>>;

    fn mark_block_as_finalized(
        &mut self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<()>;

    fn is_block_finalized(
        &self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<bool>>;

    fn get_optimistic_state(
        &self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<Self::OptimisticState>>;

    fn is_optimistic_state_present(&self, block_id: &BlockIdentifierFor<Self>) -> bool;

    fn store_block<T: Into<Self::CandidateBlock>>(&self, block: T) -> anyhow::Result<()>;

    fn erase_block_and_optimistic_state(
        &self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<()>;

    fn erase_block(&self, block_id: &BlockIdentifierFor<Self>) -> anyhow::Result<()>;

    fn list_stored_thread_finalized_blocks(
        &self,
        thread_id: &Self::ThreadIdentifier,
    ) -> anyhow::Result<Vec<(BlockIdentifierFor<Self>, BlockSeqNoFor<Self>)>>;

    fn delete_external_messages(&self, count: usize) -> anyhow::Result<()>;

    fn add_external_message<T>(&mut self, messages: Vec<T>) -> anyhow::Result<()>
    where
        T: Into<<Self::OptimisticState as OptimisticState>::Message>;

    fn mark_block_as_verified(&self, block_id: &BlockIdentifierFor<Self>) -> anyhow::Result<()>;

    fn is_block_verified(&self, block_id: &BlockIdentifierFor<Self>) -> anyhow::Result<bool>;

    fn take_state_snapshot(
        &self,
        block_id: &BlockIdentifierFor<Self>,
        block_producer_groups: HashMap<Self::ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        block_keeper_set: BlockKeeperSet,
    ) -> anyhow::Result<Self::StateSnapshot>;

    fn convert_state_data_to_snapshot(
        &self,
        serialized_state: Vec<u8>,
        block_producer_groups: HashMap<Self::ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        block_keeper_set: BlockKeeperSet,
    ) -> anyhow::Result<Self::StateSnapshot>;

    fn set_state_from_snapshot(
        &mut self,
        block_id: &BlockIdentifierFor<Self>,
        snapshot: Self::StateSnapshot,
    ) -> anyhow::Result<(HashMap<Self::ThreadIdentifier, Vec<Self::NodeIdentifier>>, BlockKeeperSet)>;

    fn save_account_diffs(
        &self,
        block_id: WrappedUInt256,
        accounts: HashMap<String, SerializedItem>,
    ) -> anyhow::Result<()>;

    fn dump_verification_data<
        TBlock: Into<Self::CandidateBlock>,
        TState: Into<Self::OptimisticState>,
    >(
        &self,
        prev_block: TBlock,
        prev_state: TState,
        cur_block: TBlock,
        failed_block: TBlock,
        failed_state: TState,
        incoming_state: TState,
    ) -> anyhow::Result<()>;

    fn last_stored_block_by_seq_no(&self) -> anyhow::Result<BlockSeqNoFor<Self>>;

    fn store_optimistic<T: Into<Self::OptimisticState>>(&mut self, state: T) -> anyhow::Result<()>;

    fn get_block_id_by_seq_no(
        &self,
        block_seq_no: &BlockSeqNoFor<Self>,
    ) -> Vec<BlockIdentifierFor<Self>>;

    fn get_latest_block_id_with_producer_group_change(
        &self,
        thread_id: &Self::ThreadIdentifier,
    ) -> anyhow::Result<BlockIdentifierFor<Self>>;

    fn clear_ext_messages_queue_by_time(&self) -> anyhow::Result<()>;

    fn dump_sent_attestations(
        &self,
        data: HashMap<
            Self::ThreadIdentifier,
            Vec<(<Self::Block as Block>::BlockSeqNo, Self::Attestation)>,
        >,
    ) -> anyhow::Result<()>;

    fn load_sent_attestations(
        &self,
    ) -> anyhow::Result<
        HashMap<
            Self::ThreadIdentifier,
            Vec<(<Self::Block as Block>::BlockSeqNo, Self::Attestation)>,
        >,
    >;

    fn saved_optimistic_states(
        &self,
    ) -> BTreeMap<<Self::Block as Block>::BlockSeqNo, <Self::Block as Block>::BlockIdentifier>;

    fn get_finalized_state(&self) -> Self::OptimisticState;

    fn get_finalized_state_as_mut(&mut self) -> &mut Self::OptimisticState;
}
