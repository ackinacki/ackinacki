//! Repository handoff for verified partitioned imports. Network format selection
//! remains paired with the publisher; this is not a separate activation switch.
use super::snapshot_consumption::StagedPartitionedSnapshot;
#[cfg(test)]
use super::snapshot_http_import::download_and_stage_snapshot;
#[cfg(test)]
use super::snapshot_http_import::SnapshotHttpImport;
use super::snapshot_partitioned::PrimaryMetadata;
#[cfg(test)]
use super::snapshot_partitioned::PrimaryThreadSnapshot;
use super::*;

/// Keep provisional archive cleanup armed until controls have been published.
/// In particular, an error writing the recovery record must not leak staging.
pub(super) struct StagedImportGuard {
    repository: ThreadAccountsRepository,
    staged: Option<account_state::StagedArchiveEpoch>,
}

impl StagedImportGuard {
    pub(super) fn new(
        repository: ThreadAccountsRepository,
        staged: Option<account_state::StagedArchiveEpoch>,
    ) -> Self {
        Self { repository, staged }
    }

    pub(super) fn disarm(&mut self) {
        self.staged = None;
    }
}

impl Drop for StagedImportGuard {
    fn drop(&mut self) {
        if let Some(staged) = self.staged.take() {
            self.repository.abort_staged_import(staged);
        }
    }
}

impl RepositoryImpl {
    /// The worker must serialize imports, supply trusted-primary verification
    /// and use a clone of this repository's account handle in the request.
    /// No defaults or protocol activation decisions are made here.
    #[cfg(test)]
    pub(crate) fn download_and_install_partitioned_snapshot<C, V>(
        &mut self,
        request: SnapshotHttpImport<'_>,
        cur_thread_id: &ThreadIdentifier,
        verify_primary: V,
        should_cancel: &C,
    ) -> anyhow::Result<SnapshotImportStatus>
    where
        C: Fn() -> bool + Sync,
        V: FnMut(&PrimaryThreadSnapshot) -> anyhow::Result<()>,
    {
        anyhow::ensure!(
            std::ptr::eq(request.repository, self.thread_accounts_repository.durable_map_repo()),
            "snapshot staging repository does not match installation repository"
        );
        let staged = download_and_stage_snapshot(request, verify_primary, should_cancel)?;
        self.install_partitioned_snapshot(staged, cur_thread_id, should_cancel)
    }

    pub(crate) fn install_partitioned_snapshot<C: Fn() -> bool>(
        &mut self,
        staged: StagedPartitionedSnapshot<'_>,
        cur_thread_id: &ThreadIdentifier,
        should_cancel: &C,
    ) -> anyhow::Result<SnapshotImportStatus> {
        anyhow::ensure!(
            staged.belongs_to(self.thread_accounts_repository.durable_map_repo()),
            "snapshot staging repository does not match installation repository"
        );
        // Serialize recovery state while the original staged owner is still
        // armed. Canonical account bodies are not serialized again here.
        let optimistic_state = bincode::serialize(staged.state())?;
        anyhow::ensure!(!should_cancel(), "snapshot installation cancelled");
        let (primary, state, epoch) = staged.into_parts();
        let prefix = primary.metadata.into_install_prefix(optimistic_state);
        self.install_staged_snapshot(
            prefix,
            state,
            Some(epoch),
            cur_thread_id,
            Arc::new(Mutex::new(HashSet::new())),
            should_cancel,
        )
    }
}

impl PrimaryMetadata {
    /// Reuse the existing local recovery schema, not the monolithic wire codec.
    /// Message bodies already live in the reconstructed queues; no message DB
    /// extraction or temporary account-body segment is needed.
    fn into_install_prefix(self, optimistic_state: Vec<u8>) -> StreamedThreadSnapshot {
        StreamedThreadSnapshot {
            optimistic_state,
            ancestor_blocks_data: self.ancestor_blocks_data,
            db_messages: Vec::new(),
            finalized_block: self.finalized_block,
            bk_set: self.bk_set,
            descendant_bk_set: self.descendant_bk_set,
            future_bk_set: self.future_bk_set,
            descendant_future_bk_set: self.descendant_future_bk_set,
            finalized_block_stats: self.finalized_block_stats,
            attestation_target: self.attestation_target,
            producer_selector: self.producer_selector,
            block_height: self.block_height,
            prefinalization_proof: self.prefinalization_proof,
            ancestor_blocks_finalization_checkpoints: self.ancestor_blocks_finalization_checkpoints,
            finalizes_blocks: self.finalizes_blocks,
            parent_ancestor_blocks_finalization_checkpoints: self
                .parent_ancestor_blocks_finalization_checkpoints,
            block_protocol_version_state: self.block_protocol_version_state,
            history_data_snapshot: self.history_data_snapshot,
            history_cursor: self.history_cursor,
            durable_snapshot_header: DurableSnapshotHeader { tag: 0, len: 0 },
            finalization_chain: self.finalization_chain,
        }
    }
}

#[cfg(test)]
#[path = "snapshot_installation_tests.rs"]
mod tests;
