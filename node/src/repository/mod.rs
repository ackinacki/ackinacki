// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use database::documents_db::SerializedItem;
use parking_lot::Mutex;
use tvm_block::ShardStateUnsplit;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::BLSSignatureScheme;
use crate::message_storage::MessageDurableStorage;
use crate::node::associated_types::AttestationData;
use crate::repository::optimistic_state::OptimisticState;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

pub mod accounts;
mod cross_thread_ref_data;
pub mod cross_thread_ref_repository;
pub mod optimistic_shard_state;
pub mod optimistic_state;
pub mod repository_impl;
mod tvm_cell_serde;
pub use cross_thread_ref_data::CrossThreadRefData;
pub use cross_thread_ref_repository::CrossThreadRefDataRead;
pub use cross_thread_ref_repository::CrossThreadRefDataRepository;

use crate::message::WrappedMessage;
use crate::node::block_state::repository::BlockState;
use crate::repository::repository_impl::RepositoryMetadata;

#[cfg(test)]
pub mod stub_repository;

#[derive(thiserror::Error, Debug)]
pub enum RepositoryError {
    #[error("Failed to load optimistic state: no appropriate state was found during depth search because of block count limit reached")]
    DepthSearchBlockCountLimitReached,
    #[error("Failed to load optimistic state: no appropriate state was found during depth search because of min state limit reached")]
    DepthSearchMinStateLimitReached,
    #[error("{0}")]
    BlockNotFound(String),
}

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
    ) -> anyhow::Result<Option<Arc<Self::CandidateBlock>>>;

    fn get_block_from_repo_or_archive(
        &self,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<Arc<<Self as Repository>::CandidateBlock>>;

    fn get_block_from_repo_or_archive_by_seq_no(
        &self,
        block_seq_no: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<<Self as Repository>::CandidateBlock>>;

    fn last_finalized_optimistic_state(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> Option<Self::OptimisticState>;

    fn clear_verification_markers(
        &self,
        starting_block_id: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()>;

    fn has_thread_metadata(&self, thread_id: &ThreadIdentifier) -> bool;

    fn init_thread(
        &mut self,
        thread_id: &ThreadIdentifier,
        parent_block_id: &BlockIdentifier,
    ) -> anyhow::Result<()>;

    fn select_thread_last_finalized_block(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Option<(BlockIdentifier, BlockSeqNo)>>;

    fn is_block_suspicious(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>>;

    fn mark_block_as_finalized(
        &mut self,
        block: &<Self as Repository>::CandidateBlock,
        block_state: BlockState,
    ) -> anyhow::Result<()>;

    fn is_block_finalized(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>>;

    fn get_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        min_state: Option<Self::OptimisticState>,
    ) -> anyhow::Result<Option<Self::OptimisticState>>;

    fn get_full_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        min_state: Option<Self::OptimisticState>,
    ) -> anyhow::Result<Option<Self::OptimisticState>>;

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

    fn is_block_already_applied(&self, block_id: &BlockIdentifier) -> anyhow::Result<bool>;

    fn set_state_from_snapshot(
        &mut self,
        snapshot: Self::StateSnapshot,
        thread_id: &ThreadIdentifier,
        skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
    ) -> anyhow::Result<()>;

    fn sync_accounts_from_state(
        &mut self,
        shard_state: Arc<ShardStateUnsplit>,
    ) -> anyhow::Result<()>;

    fn save_account_diffs(
        &self,
        block_id: BlockIdentifier,
        accounts: HashMap<String, SerializedItem>,
    ) -> anyhow::Result<()>;

    fn store_optimistic<T: Into<Self::OptimisticState>>(&self, state: T) -> anyhow::Result<()>;

    fn store_optimistic_in_cache<T: Into<Self::OptimisticState>>(
        &self,
        state: T,
    ) -> anyhow::Result<()>;

    fn get_latest_block_id_with_producer_group_change(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<BlockIdentifier>;

    fn load_sent_attestations(
        &self,
    ) -> anyhow::Result<HashMap<ThreadIdentifier, Vec<(BlockSeqNo, Self::Attestation)>>>;

    fn get_zero_state_for_thread(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Self::OptimisticState>;

    fn add_thread_buffer(&self, thread_id: ThreadIdentifier) -> Arc<Mutex<Vec<BlockIdentifier>>>;

    fn get_all_metadata(&self) -> RepositoryMetadata;

    fn get_message_db(&self) -> MessageDurableStorage;
}
