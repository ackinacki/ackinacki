// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::sync::Arc;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use telemetry_utils::mpsc::InstrumentedSender;

use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::BlockHeight;
use crate::types::BlockSeqNo;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SaveStateForSharingStatus {
    Spawned,
    SkippedBusy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum SyncSnapshotAnchor {
    Height(BlockHeight),
    SeqNo(BlockSeqNo),
}

impl SyncSnapshotAnchor {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Height(_) => "height",
            Self::SeqNo(_) => "seq_no",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncSnapshotRequest {
    pub address: BTreeMap<ThreadIdentifier, BlockIdentifier>,
    pub anchor: SyncSnapshotAnchor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SyncSnapshotLoaded {
    pub address: BTreeMap<ThreadIdentifier, BlockIdentifier>,
    pub anchor: SyncSnapshotAnchor,
}

pub trait StateSyncService {
    type Repository: Repository;
    // An assumption here:
    // We expect to have an implementation that will be able
    // to convert or reuse TVM hashes to address IPFS resources:
    // ref: https://github.com/multiformats/rust-multihash
    // In the later implementation this function should be modified:
    // - to take state hash that is created internally by the node
    // - accept a data storage as a parameters to be able to take state directly
    //   from there.
    // - use data storage to snapshot state and then publish it on ipfs.
    /// Spawn a worker that writes a snapshot file for the given block.
    /// The implementation must reserve worker capacity first and only then
    /// call `request_snapshot_pin(anchor)` to avoid pin leaks when a save is
    /// skipped due to concurrency limits. The returned guard must stay armed
    /// until the worker successfully acquires the pin itself.
    fn save_state_for_sharing(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        anchor: account_state::AnchorBlockRef,
        min_state: Option<Arc<OptimisticStateImpl>>,
        finalizing_block_id: BlockIdentifier,
    ) -> anyhow::Result<SaveStateForSharingStatus>;

    fn add_load_state_task(
        &mut self,
        resource_address: BTreeMap<ThreadIdentifier, BlockIdentifier>,
        block_seq_no: BlockSeqNo,
        repository: RepositoryImpl,
        output: InstrumentedSender<anyhow::Result<SyncSnapshotLoaded>>,
    ) -> anyhow::Result<()>;

    fn add_load_state_task_with_height(
        &mut self,
        resource_address: BTreeMap<ThreadIdentifier, BlockIdentifier>,
        block_height: BlockHeight,
        repository: RepositoryImpl,
        output: InstrumentedSender<anyhow::Result<SyncSnapshotLoaded>>,
    ) -> anyhow::Result<()>;

    fn is_load_thread_available(&self) -> bool;

    fn clear_load_state_tasks(&mut self);

    fn flush(&self) -> anyhow::Result<()>;
}
