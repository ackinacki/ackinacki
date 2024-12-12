// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use database::documents_db::SerializedItem;
use tvm_block::ShardStateUnsplit;

use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::repository::optimistic_state::OptimisticState;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

pub mod optimistic_shard_state;
pub mod optimistic_state;
pub mod repository_impl;
mod tvm_cell_serde;

#[cfg(test)]
pub mod stub_repository;

pub trait Repository {
    type BLS: BLSSignatureScheme;
    type CandidateBlock: BLSSignedEnvelope<
        SignerIndex = Self::EnvelopeSignerIndex,
        BLS = Self::BLS,
        Data = AckiNackiBlock<Self::BLS>,
    >;
    type EnvelopeSignerIndex;
    type NodeIdentifier;
    type OptimisticState: OptimisticState;
    type Attestation: BLSSignedEnvelope<
        SignerIndex = Self::EnvelopeSignerIndex,
        BLS = Self::BLS,
        Data = AttestationData,
    >;
    type StateSnapshot: From<Vec<u8>> + Into<Vec<u8>>;

    fn get_block(
        &self,
        identifier: &BlockIdentifier,
    ) -> anyhow::Result<Option<Self::CandidateBlock>>;

    fn get_block_from_repo_or_archive(
        &self,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<<Self as Repository>::CandidateBlock>;

    fn get_block_from_repo_or_archive_by_seq_no(
        &self,
        block_seq_no: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<<Self as Repository>::CandidateBlock>>;

    fn list_blocks_with_seq_no(
        &self,
        seq_no: &BlockSeqNo,
        thread_identifier: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<Self::CandidateBlock>>;

    fn clear_verification_markers(
        &self,
        starting_block_id: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()>;

    fn select_thread_last_finalized_block(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<(BlockIdentifier, BlockSeqNo)>;

    fn select_thread_last_main_candidate_block(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<(BlockIdentifier, BlockSeqNo)>;

    fn mark_block_as_accepted_as_main_candidate(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()>;

    fn is_block_accepted_as_main_candidate(
        &self,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<Option<bool>>;

    fn mark_block_as_finalized(
        &mut self,
        block: &<Self as Repository>::CandidateBlock,
    ) -> anyhow::Result<()>;

    fn is_block_finalized(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>>;

    fn get_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<Option<Self::OptimisticState>>;

    fn is_optimistic_state_present(&self, block_id: &BlockIdentifier) -> bool;

    fn is_candidate_block_can_be_applied(&self, block: &Self::CandidateBlock) -> bool;

    fn store_block<T: Into<Self::CandidateBlock>>(&self, block: T) -> anyhow::Result<()>;

    fn erase_block_and_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()>;

    fn erase_block(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()>;

    fn list_stored_thread_finalized_blocks(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<(BlockIdentifier, BlockSeqNo)>>;

    fn delete_external_messages(&self, count: usize) -> anyhow::Result<()>;

    fn add_external_message<T>(&mut self, messages: Vec<T>) -> anyhow::Result<()>
    where
        T: Into<<Self::OptimisticState as OptimisticState>::Message>;

    fn mark_block_as_verified(&self, block_id: &BlockIdentifier) -> anyhow::Result<()>;

    fn is_block_verified(&self, block_id: &BlockIdentifier) -> anyhow::Result<bool>;

    fn take_state_snapshot(
        &self,
        block_id: &BlockIdentifier,
        block_producer_groups: HashMap<ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        block_keeper_set: HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>,
    ) -> anyhow::Result<Self::StateSnapshot>;

    fn convert_state_data_to_snapshot(
        &self,
        serialized_state: Vec<u8>,
        block_producer_groups: HashMap<ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        block_keeper_set: HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>,
    ) -> anyhow::Result<Self::StateSnapshot>;

    fn set_state_from_snapshot(
        &mut self,
        block_id: &BlockIdentifier,
        snapshot: Self::StateSnapshot,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<(
        HashMap<ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>,
    )>;

    fn sync_accounts_from_state(
        &mut self,
        shard_state: Arc<ShardStateUnsplit>,
    ) -> anyhow::Result<()>;

    fn save_account_diffs(
        &self,
        block_id: BlockIdentifier,
        accounts: HashMap<String, SerializedItem>,
    ) -> anyhow::Result<()>;

    fn last_stored_block_by_seq_no(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<BlockSeqNo>;

    fn store_optimistic<T: Into<Self::OptimisticState>>(&mut self, state: T) -> anyhow::Result<()>;

    fn get_block_id_by_seq_no(
        &self,
        block_seq_no: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<BlockIdentifier>>;

    fn get_latest_block_id_with_producer_group_change(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<BlockIdentifier>;

    fn clear_ext_messages_queue_by_time(&self) -> anyhow::Result<()>;

    fn dump_sent_attestations(
        &self,
        data: HashMap<ThreadIdentifier, Vec<(BlockSeqNo, Self::Attestation)>>,
    ) -> anyhow::Result<()>;

    fn load_sent_attestations(
        &self,
    ) -> anyhow::Result<HashMap<ThreadIdentifier, Vec<(BlockSeqNo, Self::Attestation)>>>;

    fn get_zero_state_for_thread(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Self::OptimisticState>;

    fn list_finalized_states(
        &self,
    ) -> impl Iterator<Item = (&'_ ThreadIdentifier, &'_ Self::OptimisticState)>;
}
