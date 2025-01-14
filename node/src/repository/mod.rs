// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use database::documents_db::SerializedItem;
use parking_lot::Mutex;
use tvm_block::ShardStateUnsplit;

use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::repository::optimistic_state::OptimisticState;
use crate::types::block_keeper_ring::BlockKeeperRing;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

mod cross_thread_ref_data;
pub mod cross_thread_ref_repository;
pub mod optimistic_shard_state;
pub mod optimistic_state;
pub mod repository_impl;
mod tvm_cell_serde;
pub use cross_thread_ref_data::CrossThreadRefData;
pub use cross_thread_ref_repository::CrossThreadRefDataRead;
pub use cross_thread_ref_repository::CrossThreadRefDataRepository;
use tvm_types::UInt256;

use crate::message::WrappedMessage;
use crate::node::block_state::repository::BlockState;
use crate::repository::repository_impl::RepositoryMetadata;
use crate::utilities::FixedSizeHashSet;

#[cfg(test)]
pub mod stub_repository;

pub trait Repository {
    type BLS: BLSSignatureScheme;
    type CandidateBlock: BLSSignedEnvelope<
        SignerIndex = Self::EnvelopeSignerIndex,
        BLS = Self::BLS,
        Data = AckiNackiBlock,
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

    // TODO: Critical! This method must be removed. No algorithm will be correct
    // once merge logic is added.
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

    fn has_thread_metadata(&self, thread_id: &ThreadIdentifier) -> bool;

    fn prepare_thread_sync(
        &mut self,
        thread_id: &ThreadIdentifier,
        known_finalized_block_id: &BlockIdentifier,
        known_finalized_block_seq_no: &BlockSeqNo,
    ) -> anyhow::Result<()>;

    fn init_thread(
        &mut self,
        thread_id: &ThreadIdentifier,
        parent_block_id: &BlockIdentifier,
        block_keeper_sets: BlockKeeperRing,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
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

    fn is_block_suspicious(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>>;

    fn mark_block_as_finalized(
        &mut self,
        block: &<Self as Repository>::CandidateBlock,
        block_keeper_sets: BlockKeeperRing,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        block_state: BlockState,
    ) -> anyhow::Result<()>;

    fn is_block_finalized(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>>;

    fn get_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
        block_keeper_sets: BlockKeeperRing,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
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

    fn delete_external_messages(
        &self,
        count: usize,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()>;

    fn add_external_message<T>(
        &mut self,
        messages: Vec<T>,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()>
    where
        T: Into<WrappedMessage>;

    fn is_block_already_applied(&self, block_id: &BlockIdentifier) -> anyhow::Result<bool>;

    fn mark_block_as_processed(&self, block_id: &BlockIdentifier) -> anyhow::Result<()>;

    fn is_block_processed(&self, block_id: &BlockIdentifier) -> anyhow::Result<bool>;

    fn set_state_from_snapshot(
        &mut self,
        block_id: &BlockIdentifier,
        snapshot: Self::StateSnapshot,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<(
        HashMap<ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        BTreeMap<BlockSeqNo, BlockKeeperSet>,
        Vec<CrossThreadRefData>,
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

    fn clear_ext_messages_queue_by_time(&self, thread_id: &ThreadIdentifier) -> anyhow::Result<()>;

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

    fn add_thread_buffer(&self, thread_id: ThreadIdentifier) -> Arc<Mutex<Vec<BlockIdentifier>>>;

    fn list_finalized_states(
        &self,
    ) -> impl Iterator<Item = (&'_ ThreadIdentifier, &'_ Self::OptimisticState)>;

    fn get_all_metadata(&self) -> RepositoryMetadata;
}
