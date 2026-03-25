use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use parking_lot::Mutex;
use typed_builder::TypedBuilder;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::helper::get_temp_file_path;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::services::block_processor::service::MAX_ATTESTATION_TARGET_BETA;
use crate::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BLOCK_STATISTICS_INITIAL_WINDOW_SIZE;
use crate::node::services::sync::snapshot_compression::COMPRESSED_SNAPSHOT_MAGIC;
use crate::node::services::sync::snapshot_compression::ZSTD_COMPRESSION_LEVEL;
use crate::node::shared_services::SharedServices;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::AncestorBlockData;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::repository_impl::ThreadSnapshot;
use crate::repository::repository_impl::ThreadSnapshotHeader;
use crate::repository::CrossThreadRefData;
use crate::repository::CrossThreadRefDataRead;
use crate::repository::Repository;
use crate::storage::MessageDurableStorage;
#[cfg(feature = "history_proofs")]
use crate::types::history_proof::take_history_data_snapshot;
use crate::types::thread_message_queue::account_messages_iterator::AccountMessagesIterator;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

impl AllowGuardedMut for Vec<JoinHandle<anyhow::Result<()>>> {}

/// Maximum number of concurrent file-saving threads.
const MAX_CONCURRENT_SAVE_THREADS: usize = 2;

/// RAII guard that decrements an atomic counter on drop.
struct ActiveThreadGuard(Arc<AtomicUsize>);
impl Drop for ActiveThreadGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

#[derive(Clone, TypedBuilder)]
pub struct FileSavingService {
    root_path: PathBuf,
    #[builder(default = Arc::new(Mutex::new(Vec::new())))]
    threads: Arc<Mutex<Vec<JoinHandle<anyhow::Result<()>>>>>,
    #[builder(default = Arc::new(AtomicUsize::new(0)))]
    active_thread_count: Arc<AtomicUsize>,
    repository: RepositoryImpl,
    block_state_repository: BlockStateRepository,
    shared_services: SharedServices,
    message_db: MessageDurableStorage,
}

fn get_ancestor_blocks_data(
    starting_block_id: &BlockIdentifier,
    block_state_repository: &BlockStateRepository,
    shared_services: &mut SharedServices,
) -> anyhow::Result<Vec<AncestorBlockData>> {
    let mut history = vec![];
    let history_length = std::cmp::max(
        std::cmp::max(
            2 * MAX_ATTESTATION_TARGET_BETA + MAX_ATTESTATION_TARGET_BETA,
            2 * MAX_ATTESTATION_TARGET_BETA + 2,
        ),
        BLOCK_STATISTICS_INITIAL_WINDOW_SIZE,
    ) + 1;
    let mut cursor = *starting_block_id;
    for _ in 0..history_length {
        if cursor == BlockIdentifier::default() {
            break;
        }
        let cross_thread_ref_data =
            shared_services.exec(|e| -> anyhow::Result<CrossThreadRefData> {
                e.cross_thread_ref_data_service.get_cross_thread_ref_data(&cursor)
            })?;
        let block_state = block_state_repository.get(&cursor)?;
        let (
            Some(block_seq_no),
            Some(thread_identifier),
            Some(bk_set),
            Some(future_bk_set),
            Some(envelope_hash),
            Some(parent_block_identifier),
            Some(block_version_state),
        ) = block_state.guarded(|e| {
            (
                *e.block_seq_no(),
                *e.thread_identifier(),
                e.bk_set().clone(),
                e.future_bk_set().clone(),
                e.envelope_hash().clone(),
                *e.parent_block_identifier(),
                e.block_version_state().clone(),
            )
        })
        else {
            anyhow::bail!("Failed to get ancestor block data");
        };
        history.push(
            AncestorBlockData::builder()
                .block_identifier(cursor)
                .block_seq_no(block_seq_no)
                .thread_identifier(thread_identifier)
                .cross_thread_ref_data(cross_thread_ref_data)
                .bk_set(bk_set.deref().clone())
                .future_bk_set(future_bk_set.deref().clone())
                .envelope_hash(envelope_hash)
                .parent_block_identifier(parent_block_identifier)
                .block_version_state(block_version_state)
                .build(),
        );
        cursor = parent_block_identifier;
    }
    Ok(history)
}

impl Drop for FileSavingService {
    fn drop(&mut self) {
        // Only join threads when this is the last clone (Arc has a single owner).
        let Some(threads_mutex) = Arc::get_mut(&mut self.threads) else {
            return;
        };
        let threads = threads_mutex.get_mut();
        while let Some(handle) = threads.pop() {
            if let Err(e) = handle.join() {
                tracing::error!(target: "node", "File saving thread panicked on drop: {:?}", e);
            }
        }
    }
}

impl FileSavingService {
    pub fn flush(&self) -> anyhow::Result<()> {
        self.threads.guarded_mut(|threads| {
            let mut i = 0;
            while i < threads.len() {
                if threads[i].is_finished() {
                    let handle = threads.swap_remove(i);
                    match handle
                        .join()
                        .map_err(|e| anyhow::format_err!("Thread panicked: {:?}", e))
                        .and_then(|r| r)
                    {
                        Ok(()) => {}
                        Err(e) => {
                            tracing::error!(target: "node", "File saving thread failed: {:?}", e)
                        }
                    }
                    // Don't increment i: swap_remove placed the last element at position i
                } else {
                    i += 1;
                }
            }
        });
        Ok(())
    }

    /// Allow saving only if the number of active save threads drops below the limit.
    fn is_slot_available(&self) -> anyhow::Result<bool> {
        self.flush()?;
        if self.active_thread_count.load(Ordering::Acquire) < MAX_CONCURRENT_SAVE_THREADS {
            return Ok(true);
        }
        Ok(false)
    }

    pub fn save_object(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        min_state: Option<Arc<OptimisticStateImpl>>,
        path: PathBuf,
        finalizing_block_id: BlockIdentifier,
    ) -> anyhow::Result<()> {
        self.flush()?;
        if !self.is_slot_available()? {
            tracing::trace!(target: "node", "Saving state to {}", path.display());
            tracing::trace!(target: "monit", "There is no available slot for saving thread. Skip it");
            return Ok(());
        }

        let path = self.root_path.join(path);
        let parent_dir = self.root_path.clone();
        let message_db = self.message_db.clone();
        let mut shared_services = self.shared_services.clone();
        let block_state_repository = self.block_state_repository.clone();
        let repository = self.repository.clone();
        let block_id = *block_id;
        let thread_id = *thread_id;
        // Capture the finalized block synchronously before spawning the thread.
        // The block is guaranteed to be in FinalizedBlockStorage at this call site,
        // but may be evicted by the time the background thread runs (race condition).

        tracing::trace!(
            "save_object: block_id={block_id:?}, finalizing_block_id={finalizing_block_id:?}"
        );
        let finalized_block = self
            .repository
            .get_finalized_block(&block_id)?
            .ok_or_else(|| anyhow::format_err!("Failed to get block: {:?}", block_id))?;
        // Collect the finalization chain synchronously before spawning the thread.
        // Blocks may be evicted from FinalizedBlockStorage by the time the background thread runs.
        let finalization_chain = Self::collect_finalization_chain(
            &block_id,
            &finalizing_block_id,
            &thread_id,
            &self.repository,
        )
        .unwrap_or_else(|e| {
            tracing::warn!("Failed to collect finalization chain: {e}");
            vec![]
        });
        tracing::trace!("save_object: finalization_chain len={}", finalization_chain.len());
        #[cfg(feature = "history_proofs")]
        let history_proof_data = self.repository.get_history_proof_data();
        let active_count = Arc::clone(&self.active_thread_count);
        active_count.fetch_add(1, Ordering::Release);
        let thread = std::thread::Builder::new()
            .name(format!("Saving state: {}", path.display()))
            .spawn(move || {
                let _guard = ActiveThreadGuard(active_count);
                tracing::trace!(target: "node", "Saving state to {}", path.display());
                if std::fs::exists(&path)? {
                    tracing::trace!(target: "node", "File {} already exists, skip saving.", path.display());
                    return Ok(());
                }
                // Use the no-cache variant so we don't pollute the optimistic_state
                // cache with entries that are only needed for serialization.
                let state = repository
                    .get_full_optimistic_state_no_cache(&block_id, &thread_id, min_state)?
                    .ok_or(anyhow::format_err!("Failed to get full optimistic state"))?;

                // When the Arc has a single strong reference (no cache entry),
                // unwrap_or_clone will move the data without cloning.
                let state = Arc::unwrap_or_clone(state);
                let db_messages = state
                    .messages
                    .iter(&message_db)
                    .map(|range| range.remaining_messages_from_db().unwrap_or_default())
                    .collect();
                // Export durable state snapshot
                let durable_state_snapshot = match repository
                    .thread_accounts_repository()
                    .export_durable_snapshot(&state.shard_state.0)
                {
                    Ok(snapshot) => match bincode::serialize(&snapshot) {
                        Ok(bytes) => Some(bytes),
                        Err(e) => {
                            tracing::warn!("Failed to serialize durable snapshot: {e}");
                            None
                        }
                    },
                    Err(e) => {
                        tracing::warn!("Failed to export durable snapshot: {e}");
                        None
                    }
                };

                let serialized_state = bincode::serialize(&state)?;
                // Drop the heavy optimistic state now that it's serialized.
                drop(state);

                let ancestor_blocks_data = get_ancestor_blocks_data(
                    &block_id,
                    &block_state_repository,
                    &mut shared_services,
                )?;
                let block_state = block_state_repository.get(&block_id)?;
                let (
                    Some(bk_set),
                    Some(finalized_block_stats),
                    Some(attestation_target),
                    Some(producer_selector),
                    Some(block_height),
                    Some(prefinalization_proof),
                    Some(future_bk_set),
                    Some(descendant_bk_set),
                    Some(descendant_future_bk_set),
                    Some(ancestor_blocks_finalization_checkpoints),
                    Some(finalizes_blocks),
                    Some(parent_id),
                    Some(block_protocol_version_state),
                ) = block_state.guarded(|e| {
                    (
                        e.bk_set().clone(),
                        e.block_stats().clone(),
                        *e.attestation_target(),
                        e.producer_selector_data().clone(),
                        *e.block_height(),
                        e.prefinalization_proof().clone(),
                        e.future_bk_set().clone(),
                        e.descendant_bk_set().clone(),
                        e.descendant_future_bk_set().clone(),
                        e.ancestor_blocks_finalization_checkpoints().clone(),
                        e.finalizes_blocks().clone(),
                        *e.parent_block_identifier(),
                        e.block_version_state().clone(),
                    )
                })
                else {
                    anyhow::bail!("Failed to get block data for sync");
                };
                let parent_block_state = block_state_repository.get(&parent_id)?;
                let Some(parent_ancestor_blocks_finalization_checkpoints) = parent_block_state
                    .guarded(|e| e.ancestor_blocks_finalization_checkpoints().clone())
                else {
                    anyhow::bail!("Failed to get parent block data for sync");
                };
                #[cfg(feature = "history_proofs")]
                let history_snapshot = take_history_data_snapshot(history_proof_data);
                let builder = ThreadSnapshot::builder()
                    .optimistic_state(serialized_state)
                    .ancestor_blocks_data(ancestor_blocks_data)
                    .db_messages(db_messages)
                    .finalized_block(finalized_block.deref().clone())
                    .bk_set(bk_set.deref().clone())
                    .future_bk_set(future_bk_set.deref().clone())
                    .finalized_block_stats(finalized_block_stats)
                    .attestation_target(attestation_target)
                    .producer_selector(producer_selector)
                    .block_height(block_height)
                    .prefinalization_proof(prefinalization_proof)
                    .descendant_bk_set(descendant_bk_set.deref().clone())
                    .descendant_future_bk_set(descendant_future_bk_set.deref().clone())
                    .ancestor_blocks_finalization_checkpoints(
                        ancestor_blocks_finalization_checkpoints,
                    )
                    .finalizes_blocks(finalizes_blocks)
                    .parent_ancestor_blocks_finalization_checkpoints(
                        parent_ancestor_blocks_finalization_checkpoints,
                    )
                    .block_protocol_version_state(block_protocol_version_state)
                    .durable_state_snapshot(durable_state_snapshot)
                    .finalization_chain(finalization_chain);
                #[cfg(feature = "history_proofs")]
                let builder = builder.history_data_snapshot(history_snapshot);
                let shared_thread_state = builder.build();
                // Drop the Arc<Envelope> now that its data has been cloned into the snapshot.
                drop(finalized_block);

                // Extract lightweight header before dropping the snapshot
                let header = ThreadSnapshotHeader {
                    block_id: shared_thread_state.finalized_block().data().identifier(),
                    thread_id: *shared_thread_state.finalized_block().data().common_section().thread_id(),
                    seq_no: shared_thread_state.finalized_block().data().seq_no(),
                    round: *shared_thread_state.finalized_block().data().common_section().round(),
                };

                let bytes = bincode::serialize(&shared_thread_state)?;
                // Drop the ThreadSnapshot now that it's serialized.
                drop(shared_thread_state);

                // Compress with zstd and prepend magic header
                let compressed = zstd::encode_all(bytes.as_slice(), ZSTD_COMPRESSION_LEVEL)?;
                let mut bytes = Vec::with_capacity(COMPRESSED_SNAPSHOT_MAGIC.len() + compressed.len());
                bytes.extend_from_slice(COMPRESSED_SNAPSHOT_MAGIC);
                bytes.extend_from_slice(&compressed);

                let tmp_file_path = get_temp_file_path(&parent_dir);
                std::fs::write(tmp_file_path.clone(), bytes)?;
                std::fs::rename(tmp_file_path, &path)?;
                tracing::trace!(target: "node", "Successfully saved state to {}", path.display());

                // Write lightweight header sidecar for fast startup scanning
                let header_path = RepositoryImpl::snapshot_header_path(&path);
                if !std::fs::exists(&header_path)? {
                    let header_bytes = bincode::serialize(&header)?;
                    let tmp_header_path = get_temp_file_path(&parent_dir);
                    std::fs::write(&tmp_header_path, header_bytes)?;
                    std::fs::rename(tmp_header_path, &header_path)?;
                }

                Ok(())
            })?;
        self.threads.guarded_mut(|threads| {
            threads.push(thread);
        });
        Ok(())
    }

    /// Collects the ordered chain of blocks from `finalized_block_id` (exclusive)
    /// to `finalizing_block_id` (inclusive) by following parent links.
    ///
    /// Returns blocks in ascending order: [B+1, B+2, ..., P].
    fn collect_finalization_chain(
        finalized_block_id: &BlockIdentifier,
        finalizing_block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        repository: &RepositoryImpl,
    ) -> anyhow::Result<Vec<crate::bls::envelope::Envelope<crate::types::AckiNackiBlock>>> {
        use crate::bls::envelope::BLSSignedEnvelope;

        if finalized_block_id == finalizing_block_id {
            return Ok(vec![]);
        }

        let mut chain = vec![];
        let mut cursor = *finalizing_block_id;

        loop {
            // TODO: Can't load finalizing block
            let block =
                repository.get_block_from_repo_or_archive(&cursor, thread_id).map_err(|e| {
                    anyhow::format_err!(
                        "collect_finalization_chain: block {cursor:?} not found: {e}"
                    )
                })?;

            let parent_id = block.data().parent();

            chain.push(block.as_ref().clone());

            if parent_id == *finalized_block_id || cursor == *finalized_block_id {
                break;
            }
            if parent_id == BlockIdentifier::default() {
                break;
            }
            cursor = parent_id;
        }

        chain.reverse();
        Ok(chain)
    }
}
