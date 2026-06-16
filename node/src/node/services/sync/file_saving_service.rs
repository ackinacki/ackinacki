use std::io::Write;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use parking_lot::Mutex;
use tempfile::NamedTempFile;
use typed_builder::TypedBuilder;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::helper::get_temp_file_path;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::services::block_processor::service::MAX_ATTESTATION_TARGET_BETA;
use crate::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BLOCK_STATISTICS_INITIAL_WINDOW_SIZE;
use crate::node::services::sync::snapshot_compression::COMPRESSED_SNAPSHOT_MAGIC;
use crate::node::services::sync::snapshot_compression::ZSTD_COMPRESSION_LEVEL;
use crate::node::services::sync::state_sync_service_trait::SaveStateForSharingStatus;
use crate::node::shared_services::SharedServices;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::get_process_rss_bytes;
use crate::repository::repository_impl::write_streamed_thread_snapshot_to_writer;
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
/// Set to 1 so only one snapshot runs at a time — if a snapshot is already
/// in progress when `save_object` is called, it's skipped.
const MAX_CONCURRENT_SAVE_THREADS: usize = 1;
const SNAPSHOT_CANCEL_CHECK_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
struct SnapshotCancelToken(Arc<AtomicBool>);

impl SnapshotCancelToken {
    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    fn check(&self) -> anyhow::Result<()> {
        if self.is_cancelled() {
            anyhow::bail!("snapshot save cancelled");
        }
        Ok(())
    }
}

struct CancelCheckedWriter<W> {
    inner: W,
    cancel: SnapshotCancelToken,
    bytes_until_check: usize,
}

impl<W> CancelCheckedWriter<W> {
    fn new(inner: W, cancel: SnapshotCancelToken) -> Self {
        Self { inner, cancel, bytes_until_check: SNAPSHOT_CANCEL_CHECK_BYTES }
    }
}

impl<W: Write> Write for CancelCheckedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.cancel.check().map_err(|e| std::io::Error::new(std::io::ErrorKind::Interrupted, e))?;
        let written = self.inner.write(buf)?;
        self.bytes_until_check = self.bytes_until_check.saturating_sub(written);
        if self.bytes_until_check == 0 {
            self.cancel
                .check()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Interrupted, e))?;
            self.bytes_until_check = SNAPSHOT_CANCEL_CHECK_BYTES;
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.cancel.check().map_err(|e| std::io::Error::new(std::io::ErrorKind::Interrupted, e))?;
        self.inner.flush()
    }
}

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
    #[builder(default = SnapshotCancelToken(Arc::new(AtomicBool::new(false))))]
    cancel_token: SnapshotCancelToken,
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

enum SnapshotPinAcquireOutcome<P> {
    Acquired(P),
    SkipSnapshot,
}

fn spawn_named_worker<T, F, S>(name: String, spawner: S, worker: F) -> std::io::Result<T>
where
    F: FnOnce() -> anyhow::Result<()> + Send + 'static,
    S: FnOnce(std::thread::Builder, F) -> std::io::Result<T>,
{
    spawner(std::thread::Builder::new().name(name), worker)
}

fn acquire_snapshot_pin_for_worker<P, F>(
    thread_id: ThreadIdentifier,
    block_id: BlockIdentifier,
    timeout: std::time::Duration,
    cancel: &SnapshotCancelToken,
    acquire_pin: F,
) -> SnapshotPinAcquireOutcome<P>
where
    F: Fn(ThreadIdentifier, BlockIdentifier, std::time::Duration) -> Option<P>,
{
    tracing::info!(
        target: "monit",
        "snapshot[{block_id:?}]: pin was requested earlier; waiting for boundary up to {:?}",
        timeout,
    );
    if cancel.is_cancelled() {
        tracing::info!(
            target: "monit",
            "snapshot[{block_id:?}]: pin acquire cancelled; skipping atomically",
        );
        return SnapshotPinAcquireOutcome::SkipSnapshot;
    }
    let pin = acquire_pin(thread_id, block_id, timeout);
    let Some(pin) = pin else {
        tracing::warn!(
            target: "monit",
            "snapshot[{block_id:?}]: pin not acquired (anchor mismatch or timeout); skipping atomically",
        );
        return SnapshotPinAcquireOutcome::SkipSnapshot;
    };
    tracing::info!(target: "monit", "snapshot[{block_id:?}]: pin acquired");
    SnapshotPinAcquireOutcome::Acquired(pin)
}

#[cfg(test)]
fn write_snapshot_file<F>(
    snapshot_path: &Path,
    parent_dir: &Path,
    header: &ThreadSnapshotHeader,
    write_snapshot: F,
) -> anyhow::Result<()>
where
    F: FnOnce(&mut dyn Write) -> anyhow::Result<()>,
{
    let tmp_file_path = get_temp_file_path(parent_dir);
    let result = (|| -> anyhow::Result<()> {
        let mut tmp_file = std::fs::File::create(&tmp_file_path)?;
        tmp_file.write_all(COMPRESSED_SNAPSHOT_MAGIC)?;
        let mut encoder = zstd::Encoder::new(tmp_file, ZSTD_COMPRESSION_LEVEL)?;
        write_snapshot(&mut encoder)?;
        let tmp_file = encoder.finish()?;
        tmp_file.sync_all()?;
        std::fs::rename(&tmp_file_path, snapshot_path)?;

        let header_path = RepositoryImpl::snapshot_header_path(snapshot_path);
        if !std::fs::exists(&header_path)? {
            let header_bytes = bincode::serialize(header)?;
            let tmp_header_path = get_temp_file_path(parent_dir);
            std::fs::write(&tmp_header_path, header_bytes)?;
            std::fs::rename(tmp_header_path, &header_path)?;
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_file_path);
    }

    result
}

fn write_snapshot_file_checked<F>(
    snapshot_path: &Path,
    parent_dir: &Path,
    header: &ThreadSnapshotHeader,
    cancel: &SnapshotCancelToken,
    write_snapshot: F,
) -> anyhow::Result<()>
where
    F: FnOnce(&mut dyn Write) -> anyhow::Result<()>,
{
    let tmp_file_path = get_temp_file_path(parent_dir);
    let mut tmp_header_path = None;
    let result = (|| -> anyhow::Result<()> {
        cancel.check()?;
        let mut tmp_file = std::fs::File::create(&tmp_file_path)?;
        tmp_file.write_all(COMPRESSED_SNAPSHOT_MAGIC)?;
        let mut encoder = zstd::Encoder::new(tmp_file, ZSTD_COMPRESSION_LEVEL)?;
        {
            let mut checked = CancelCheckedWriter::new(&mut encoder, cancel.clone());
            write_snapshot(&mut checked)?;
            checked.flush()?;
        }
        cancel.check()?;
        let tmp_file = encoder.finish()?;
        cancel.check()?;
        tmp_file.sync_all()?;
        cancel.check()?;
        std::fs::rename(&tmp_file_path, snapshot_path)?;

        let header_path = RepositoryImpl::snapshot_header_path(snapshot_path);
        if !std::fs::exists(&header_path)? {
            cancel.check()?;
            let header_bytes = bincode::serialize(header)?;
            let header_tmp = get_temp_file_path(parent_dir);
            tmp_header_path = Some(header_tmp.clone());
            std::fs::write(&header_tmp, header_bytes)?;
            cancel.check()?;
            std::fs::rename(&header_tmp, &header_path)?;
            tmp_header_path = None;
        }
        Ok(())
    })();

    if result.is_err() {
        let _ = std::fs::remove_file(&tmp_file_path);
        if let Some(tmp_header_path) = tmp_header_path {
            let _ = std::fs::remove_file(tmp_header_path);
        }
    }

    result
}

#[cfg(test)]
fn export_durable_and_write_snapshot<FExport, FWrite>(
    snapshot_path: &Path,
    parent_dir: &Path,
    header: &ThreadSnapshotHeader,
    export_durable_snapshot: FExport,
    write_snapshot: FWrite,
) -> anyhow::Result<()>
where
    FExport: FnOnce() -> anyhow::Result<Option<NamedTempFile>>,
    FWrite: FnOnce(Option<&Path>, &mut dyn Write) -> anyhow::Result<()>,
{
    let durable_snapshot = export_durable_snapshot()?;
    write_snapshot_file(snapshot_path, parent_dir, header, |writer| {
        write_snapshot(durable_snapshot.as_ref().map(|temp| temp.path()), writer)
    })
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

    pub fn shutdown_snapshot_workers(&self, timeout: Duration) -> anyhow::Result<()> {
        self.cancel_token.cancel();
        let deadline = Instant::now() + timeout;
        loop {
            self.flush()?;
            let active = self.active_thread_count.load(Ordering::Acquire);
            if active == 0 {
                tracing::info!(target: "monit", "snapshot workers stopped");
                return Ok(());
            }
            if Instant::now() >= deadline {
                let handles = self.threads.guarded(|threads| threads.len());
                tracing::warn!(
                    target: "monit",
                    "timed out waiting for snapshot workers to stop: active={active}, handles={handles}",
                );
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn wait_snapshot_workers(&self) -> anyhow::Result<()> {
        tracing::info!(target: "monit", "waiting for snapshot workers to finish");
        loop {
            self.flush()?;
            let active = self.active_thread_count.load(Ordering::Acquire);
            if active == 0 {
                tracing::info!(target: "monit", "snapshot workers finished");
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn save_object(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        anchor: account_state::AnchorBlockRef,
        min_state: Option<Arc<OptimisticStateImpl>>,
        path: PathBuf,
        finalizing_block_id: BlockIdentifier,
    ) -> anyhow::Result<SaveStateForSharingStatus> {
        if self.cancel_token.is_cancelled() {
            tracing::trace!(
                target: "monit",
                "Snapshot shutdown requested, skipping save for {}",
                path.display(),
            );
            return Ok(SaveStateForSharingStatus::SkippedBusy);
        }
        // Atomically claim a slot. If another snapshot is in progress, skip.
        // Using compare_exchange to avoid TOCTOU races.
        let mut current = self.active_thread_count.load(Ordering::Acquire);
        loop {
            if current >= MAX_CONCURRENT_SAVE_THREADS {
                tracing::trace!(
                    target: "monit",
                    "Snapshot already in progress, skipping save for {}",
                    path.display(),
                );
                return Ok(SaveStateForSharingStatus::SkippedBusy);
            }
            match self.active_thread_count.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        // From here on, we must release the slot on any early return.
        // The ActiveThreadGuard in the spawned thread handles the normal path;
        // we need to handle errors before the thread is successfully spawned.
        let slot_guard = ActiveThreadGuard(Arc::clone(&self.active_thread_count));

        // Reap any finished threads (cheap — just joins handles that are already done).
        self.flush()?;
        // Request pin only after slot reservation succeeds so skipped saves
        // cannot leave a stale Requested/BoundaryReached pin behind.
        let pin_request_guard =
            self.repository.thread_accounts_repository().request_snapshot_pin(anchor);

        let path = self.root_path.join(path);
        let parent_dir = self.root_path.clone();
        let message_db = self.message_db.clone();
        let mut shared_services = self.shared_services.clone();
        let block_state_repository = self.block_state_repository.clone();
        let repository = self.repository.clone();
        let block_id = *block_id;
        let thread_id = *thread_id;
        // The snapshot pin is acquired by the spawned worker via
        // `acquire_snapshot_pin` with a long timeout — finalize never
        // blocks on it. The worker drops the pin on completion or
        // panic.
        //
        // Block lookups (`get_finalized_block`, finalization-chain walk)
        // are deferred to the worker so the finalize thread does no
        // disk I/O or Envelope clones. The blocks were just finalized
        // and live in `FinalizedBlockStorage`; if any have been evicted
        // by the time the worker runs, the chain walk falls back to an
        // empty chain (with a warn log) and the snapshot proceeds
        // without that block range — atomic skip if a critical lookup
        // (`get_finalized_block` for `block_id`) fails.
        tracing::trace!(
            "save_object: block_id={block_id:?}, finalizing_block_id={finalizing_block_id:?}"
        );
        #[cfg(feature = "history_proofs")]
        let history_proof_data = self.repository.get_history_proof_data();
        // Transfer slot ownership to the spawned thread. The slot was already
        // claimed by the compare_exchange above; we forget slot_guard on
        // successful spawn so ownership passes to the thread's ActiveThreadGuard.
        let active_count = Arc::clone(&self.active_thread_count);
        let cancel = self.cancel_token.clone();
        let thread_result = spawn_named_worker(
            format!("Saving state: {}", path.display()),
            |builder, worker| builder.spawn(worker),
            move || {
                let _guard = ActiveThreadGuard(active_count);
                let mut pin_request_guard = pin_request_guard;
                tracing::trace!(target: "node", "Saving state to {}", path.display());
                cancel.check()?;
                if std::fs::exists(&path)? {
                    tracing::trace!(target: "node", "File {} already exists, skip saving.", path.display());
                    return Ok(());
                }
                let snapshot_creation_start = std::time::Instant::now();
                let mb = |b: u64| b as f64 / (1024.0 * 1024.0);
                let rss_start = get_process_rss_bytes();
                tracing::info!(target: "mem", "snapshot[{block_id:?}]: START rss_mb={:.1}", mb(rss_start));

                cancel.check()?;
                let finalized_block = repository
                    .get_finalized_block(&block_id)?
                    .ok_or_else(|| anyhow::format_err!("Failed to get block: {:?}", block_id))?;
                let finalization_chain_arcs = Self::collect_finalization_chain_arcs(
                    &block_id,
                    &finalizing_block_id,
                    &thread_id,
                    &repository,
                )
                .unwrap_or_else(|e| {
                    tracing::warn!("Failed to collect finalization chain: {e}");
                    vec![]
                });
                tracing::trace!(
                    "save_object: finalization_chain len={}",
                    finalization_chain_arcs.len()
                );

                // Use the no-cache variant so we don't pollute the optimistic_state
                // cache with entries that are only needed for serialization.
                cancel.check()?;
                let state = repository
                    .get_full_optimistic_state_no_cache(&block_id, &thread_id, min_state)?
                    .ok_or(anyhow::format_err!("Failed to get full optimistic state"))?;
                let rss_after_get = get_process_rss_bytes();
                tracing::info!(
                    target: "mem",
                    "snapshot[{block_id:?}]: AFTER get_full_state rss_mb={:.1} delta={:.1}",
                    mb(rss_after_get),
                    mb(rss_after_get.saturating_sub(rss_start)),
                );

                // When the Arc has a single strong reference (no cache entry),
                // unwrap_or_clone will move the data without cloning.
                let state_arc_count = Arc::strong_count(&state);
                let state = Arc::unwrap_or_clone(state);
                tracing::info!(
                    target: "mem",
                    "snapshot[{block_id:?}]: unwrapped state Arc, prev_strong_count={state_arc_count}",
                );
                cancel.check()?;
                let db_messages = state
                    .messages
                    .iter(&message_db)
                    .map(|range| range.remaining_messages_from_db().unwrap_or_default())
                    .collect::<Vec<_>>();
                let db_messages_total: usize = db_messages.iter().map(|v: &Vec<_>| v.len()).sum();
                let rss_after_db_msgs = get_process_rss_bytes();
                tracing::info!(
                    target: "mem",
                    "snapshot[{block_id:?}]: AFTER db_messages count={db_messages_total} rss_mb={:.1}",
                    mb(rss_after_db_msgs),
                );

                let block_state = block_state_repository.get(&block_id)?;
                const SLOW_PATH_PIN_TIMEOUT: std::time::Duration =
                    std::time::Duration::from_secs(15 * 60);
                // Acquire the snapshot pin on this worker thread.
                // `request_snapshot_pin` was called on the accumulator
                // in `mark_block_as_finalized` (synchronously with
                // the alignment-hook'd `push_transition` that ships
                // the boundary batch), so the accumulator is in
                // `Requested` when we get here — modulo update-loop
                // backlog. We wait long enough to span the worst-
                // observed update-loop batch apply time (~9 min per
                // batch); if the boundary still isn't reached after
                // this window, skip atomically.
                let _pin = match acquire_snapshot_pin_for_worker(
                    thread_id,
                    block_id,
                    SLOW_PATH_PIN_TIMEOUT,
                    &cancel,
                    |thread_id, block_id, timeout| {
                        repository.thread_accounts_repository().acquire_snapshot_pin_cancellable(
                            thread_id,
                            block_id,
                            timeout,
                            || cancel.is_cancelled(),
                        )
                    },
                ) {
                    SnapshotPinAcquireOutcome::Acquired(pin) => {
                        pin_request_guard.disarm();
                        tracing::info!(
                            target: "mem",
                            "snapshot[{block_id:?}]: pin acquired rss_mb={:.1}",
                            mb(get_process_rss_bytes()),
                        );
                        pin
                    }
                    SnapshotPinAcquireOutcome::SkipSnapshot => return Ok(()),
                };
                cancel.check()?;
                let mut temp = NamedTempFile::new_in(&parent_dir).map_err(|e| {
                    tracing::error!(
                        target: "node",
                        ?block_id,
                        ?thread_id,
                        path = %path.display(),
                        "Failed to create temp durable snapshot file: {e}",
                    );
                    anyhow::anyhow!("Failed to create temp durable snapshot file: {e}")
                })?;
                // Atomic skip: if the export's resolution chain
                // fails on any routing, it returns Err and the temp
                // file is dropped (NamedTempFile cleans up on
                // drop). The final snapshot path is never touched.
                repository
                    .thread_accounts_repository()
                    .export_durable_snapshot_to_writer_checked(
                        &state.shard_state.0,
                        temp.as_file_mut(),
                        &|| cancel.is_cancelled(),
                    )
                    .map_err(|e| {
                        tracing::warn!(
                            target: "monit",
                            ?block_id,
                            ?thread_id,
                            "snapshot export aborted (atomic skip): {e}",
                        );
                        anyhow::anyhow!("Failed to export durable snapshot: {e}")
                    })?;
                let rss_after_export = get_process_rss_bytes();
                tracing::info!(
                    target: "mem",
                    "snapshot[{block_id:?}]: AFTER export_durable_snapshot rss_mb={:.1}",
                    mb(rss_after_export),
                );
                drop(_pin);
                tracing::info!(
                    target: "mem",
                    "snapshot[{block_id:?}]: snapshot released pin after durable export rss_mb={:.1}",
                    mb(get_process_rss_bytes()),
                );
                cancel.check()?;
                temp.as_file_mut().sync_all()?;
                let durable_state_snapshot = Some(temp);
                cancel.check()?;
                let serialized_state = bincode::serialize(&state)?;
                let serialized_state_bytes = serialized_state.len();
                // Drop the heavy optimistic state now that it's serialized.
                drop(state);
                let rss_after_serialize_state = get_process_rss_bytes();
                tracing::info!(
                    target: "mem",
                    "snapshot[{block_id:?}]: AFTER serialize+drop(state) bytes={serialized_state_bytes} rss_mb={:.1}",
                    mb(rss_after_serialize_state),
                );

                cancel.check()?;
                let ancestor_blocks_data = get_ancestor_blocks_data(
                    &block_id,
                    &block_state_repository,
                    &mut shared_services,
                )?;
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
                    .durable_state_snapshot(None)
                    .finalization_chain(
                        finalization_chain_arcs.iter().map(|b| b.as_ref().clone()).collect(),
                    );
                drop(finalization_chain_arcs);
                #[cfg(feature = "history_proofs")]
                let builder = builder.history_data_snapshot(history_snapshot);
                let shared_thread_state = builder.build();
                // Drop the Arc<Envelope> now that its data has been cloned into the snapshot.
                drop(finalized_block);

                // Extract lightweight header before dropping the snapshot
                let header = ThreadSnapshotHeader {
                    block_id: shared_thread_state.finalized_block().data().identifier(),
                    thread_id: *shared_thread_state
                        .finalized_block()
                        .data()
                        .common_section()
                        .thread_id(),
                    seq_no: shared_thread_state.finalized_block().data().seq_no(),
                    round: *shared_thread_state.finalized_block().data().common_section().round(),
                };

                cancel.check()?;
                write_snapshot_file_checked(&path, &parent_dir, &header, &cancel, |writer| {
                    write_streamed_thread_snapshot_to_writer(
                        &shared_thread_state,
                        durable_state_snapshot.as_ref().map(|temp| temp.path()),
                        &mut *writer,
                    )
                })?;
                drop(shared_thread_state);
                let rss_after_write = get_process_rss_bytes();
                tracing::info!(
                    target: "mem",
                    "snapshot[{block_id:?}]: AFTER streamed snapshot write rss_mb={:.1}",
                    mb(rss_after_write),
                );
                let snapshot_creation_elapsed =
                    snapshot_creation_start.elapsed().as_millis() as u64;
                if let Some(m) = repository.get_metrics() {
                    m.report_snapshot_creation_time(snapshot_creation_elapsed, &thread_id);
                }
                let rss_end = get_process_rss_bytes();
                tracing::info!(
                    target: "mem",
                    "snapshot[{block_id:?}]: END elapsed_ms={snapshot_creation_elapsed} \
                     rss_mb={:.1} delta_total={:.1}",
                    mb(rss_end),
                    mb(rss_end.saturating_sub(rss_start)),
                );
                tracing::trace!(target: "node", "Successfully saved state to {}", path.display());

                // Sample RSS at intervals after the snapshot completes to detect
                // whether memory is reclaimed (allocator returns to OS) or retained.
                // We've dropped all snapshot-local allocations by this point.
                for delay_ms in [0u64, 1000, 5000, 15000] {
                    if delay_ms > 0 {
                        let until = Instant::now() + Duration::from_millis(delay_ms);
                        while Instant::now() < until {
                            if cancel.is_cancelled() {
                                return Ok(());
                            }
                            std::thread::sleep(
                                Duration::from_millis(100)
                                    .min(until.saturating_duration_since(Instant::now())),
                            );
                        }
                    }
                    let rss = get_process_rss_bytes();
                    tracing::info!(
                        target: "mem",
                        "snapshot[{block_id:?}]: post_snapshot t+{delay_ms}ms rss_mb={:.1} delta_from_start={:.1}",
                        mb(rss),
                        mb(rss.saturating_sub(rss_start)),
                    );
                }
                Ok(())
            },
        );
        match thread_result {
            Ok(handle) => {
                // Thread has taken ownership of the slot via its own ActiveThreadGuard.
                std::mem::forget(slot_guard);
                self.threads.guarded_mut(|threads| {
                    threads.push(handle);
                });
                Ok(SaveStateForSharingStatus::Spawned)
            }
            Err(e) => {
                // Thread failed to spawn — slot_guard will release the slot on drop.
                drop(slot_guard);
                Err(anyhow::anyhow!("Failed to spawn save thread: {e}"))
            }
        }
    }

    /// Collects the ordered chain of blocks from `finalized_block_id` (exclusive)
    /// to `finalizing_block_id` (inclusive) by following parent links.
    ///
    /// Returns blocks in ascending order: [B+1, B+2, ..., P]. The returned
    /// `Arc`s keep each block alive across the spawn boundary even if
    /// `FinalizedBlockStorage` evicts its own caches; the worker clones
    /// the `Envelope`s once when building the snapshot so the heavy copy
    /// happens off the finalize thread.
    fn collect_finalization_chain_arcs(
        finalized_block_id: &BlockIdentifier,
        finalizing_block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        repository: &RepositoryImpl,
    ) -> anyhow::Result<Vec<Arc<crate::bls::envelope::Envelope<crate::types::AckiNackiBlock>>>>
    {
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

            chain.push(block);

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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use node_types::BlockIdentifier;
    use node_types::ThreadIdentifier;

    use super::acquire_snapshot_pin_for_worker;
    use super::export_durable_and_write_snapshot;
    use super::spawn_named_worker;
    use super::SnapshotCancelToken;
    use super::SnapshotPinAcquireOutcome;
    use super::ThreadSnapshotHeader;
    use crate::repository::repository_impl::RepositoryImpl;

    struct TestPinGuard(Arc<AtomicUsize>);

    impl Drop for TestPinGuard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn durable_export_failure_does_not_publish_snapshot_or_header() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let snapshot_path = dir.path().join("snapshot.bin");
        let header = ThreadSnapshotHeader {
            block_id: BlockIdentifier::default(),
            thread_id: ThreadIdentifier::default(),
            seq_no: Default::default(),
            round: 0,
        };
        let mut writer_called = false;

        let err = export_durable_and_write_snapshot(
            &snapshot_path,
            dir.path(),
            &header,
            || anyhow::bail!("durable export failed"),
            |_durable_snapshot_path, _writer| {
                writer_called = true;
                Ok(())
            },
        )
        .unwrap_err();

        assert!(err.to_string().contains("durable export failed"));
        assert!(!writer_called);
        assert!(!snapshot_path.exists());
        assert!(!RepositoryImpl::snapshot_header_path(&snapshot_path).exists());
        Ok(())
    }

    #[test]
    fn acquire_snapshot_pin_returns_guard_and_releases_on_drop() {
        let acquire_calls = Arc::new(AtomicUsize::new(0));
        let drop_calls = Arc::new(AtomicUsize::new(0));
        let acquire_calls_clone = Arc::clone(&acquire_calls);
        let drop_calls_clone = Arc::clone(&drop_calls);
        let cancel = SnapshotCancelToken(Arc::new(AtomicBool::new(false)));

        let outcome = acquire_snapshot_pin_for_worker(
            ThreadIdentifier::default(),
            BlockIdentifier::default(),
            std::time::Duration::from_secs(1),
            &cancel,
            move |_thread_id, _block_id, _timeout| {
                acquire_calls_clone.fetch_add(1, Ordering::Relaxed);
                Some(TestPinGuard(Arc::clone(&drop_calls_clone)))
            },
        );

        let guard = match outcome {
            SnapshotPinAcquireOutcome::Acquired(guard) => guard,
            SnapshotPinAcquireOutcome::SkipSnapshot => panic!("expected pin acquisition"),
        };
        assert_eq!(acquire_calls.load(Ordering::Relaxed), 1);
        assert_eq!(drop_calls.load(Ordering::Relaxed), 0);
        drop(guard);
        assert_eq!(drop_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn streamed_snapshot_path_keeps_pin_until_guard_drop() {
        let drop_calls = Arc::new(AtomicUsize::new(0));
        let drop_calls_clone = Arc::clone(&drop_calls);
        let cancel = SnapshotCancelToken(Arc::new(AtomicBool::new(false)));

        let outcome = acquire_snapshot_pin_for_worker(
            ThreadIdentifier::default(),
            BlockIdentifier::default(),
            std::time::Duration::from_secs(1),
            &cancel,
            move |_thread_id, _block_id, _timeout| {
                Some(TestPinGuard(Arc::clone(&drop_calls_clone)))
            },
        );

        let guard = match outcome {
            SnapshotPinAcquireOutcome::Acquired(guard) => guard,
            SnapshotPinAcquireOutcome::SkipSnapshot => panic!("streamed snapshot path skipped pin"),
        };
        assert_eq!(drop_calls.load(Ordering::Relaxed), 0);
        drop(guard);
        assert_eq!(drop_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn snapshot_pin_timeout_skips_atomically() {
        let acquire_calls = Arc::new(AtomicUsize::new(0));
        let acquire_calls_clone = Arc::clone(&acquire_calls);
        let cancel = SnapshotCancelToken(Arc::new(AtomicBool::new(false)));

        let outcome = acquire_snapshot_pin_for_worker(
            ThreadIdentifier::default(),
            BlockIdentifier::default(),
            std::time::Duration::from_millis(1),
            &cancel,
            move |_thread_id, _block_id, _timeout| {
                acquire_calls_clone.fetch_add(1, Ordering::Relaxed);
                None::<TestPinGuard>
            },
        );

        assert!(matches!(outcome, SnapshotPinAcquireOutcome::SkipSnapshot));
        assert_eq!(acquire_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn spawn_failure_drops_captured_guard() {
        let drop_calls = Arc::new(AtomicUsize::new(0));
        let captured_guard = TestPinGuard(Arc::clone(&drop_calls));

        let result: std::io::Result<std::thread::JoinHandle<anyhow::Result<()>>> =
            spawn_named_worker(
                "test-worker".to_string(),
                |_builder, worker| {
                    drop(worker);
                    Err(std::io::Error::other("spawn failed"))
                },
                move || {
                    let _guard = captured_guard;
                    Ok(())
                },
            );

        assert!(result.is_err());
        assert_eq!(drop_calls.load(Ordering::Relaxed), 1);
    }
}
