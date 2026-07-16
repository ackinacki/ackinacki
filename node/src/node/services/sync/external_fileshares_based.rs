// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use http_server::ApiBkSet;
use network::topology::NetTopology;
use node_types::AccountIdentifier;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use parking_lot::Mutex;
use parking_lot::RwLock;
use telemetry_utils::mpsc::InstrumentedSender;
use tokio::sync::watch::Receiver;
use url::Url;

use crate::helper::SHUTDOWN_FLAG;
use crate::node::services::sync::snapshot_compression::COMPRESSED_SNAPSHOT_MAGIC;
use crate::node::services::sync::state_sync_service_trait::SaveStateForSharingStatus;
use crate::node::services::sync::FileSavingService;
use crate::node::services::sync::StateSyncService;
use crate::node::services::sync::SyncSnapshotAnchor;
use crate::node::services::sync::SyncSnapshotLoaded;
use crate::node::services::sync::SyncSnapshotRequest;
use crate::node::services::sync::SyncSnapshotSkipped;
use crate::node::unprocessed_blocks_collection::UnfinalizedCutoff;
use crate::node::NodeIdentifier;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::repository_impl::SnapshotImportDecision;
use crate::repository::repository_impl::SnapshotImportStatus;
use crate::repository::Repository;
use crate::services::blob_sync::external_fileshares_based::DownloadError;
use crate::services::blob_sync::external_fileshares_based::ServiceInterface;
use crate::services::blob_sync::BlobSyncService;
use crate::types::BlockHeight;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::thread_spawn_critical::SpawnCritical;

/// Maximum number of pending state-sync candidates kept in memory. New
/// candidates from incoming height-aware sync messages are pushed to the
/// front; once the deque is at capacity, the oldest is evicted.
const LOAD_CANDIDATES_CAPACITY: usize = 5;
const COMPLETED_CANDIDATES_CAPACITY: usize = 32;
const FAILED_CANDIDATES_CAPACITY: usize = 32;
const FAILED_CANDIDATE_BACKOFF: Duration = Duration::from_secs(10);

/// Per-candidate download budget.
///
/// `MAX_TRIES` and `RETRY_TIMEOUT` together bound how long we keep
/// retrying on transient HTTP failures (e.g. persistent 404s while a
/// peer's snapshot file is still being uploaded): with 2 tries and 2s
/// between cycles, a 404'ing candidate is rejected in ~4 seconds and
/// the worker moves to the next one.
///
/// `DEADLINE` is the wall-clock cap on a *successful* download. State
/// snapshot files can be many GB, so we allow up to an hour for the
/// transfer to complete.
const PER_CANDIDATE_MAX_TRIES: u8 = 2;
const PER_CANDIDATE_RETRY_TIMEOUT: Duration = Duration::from_secs(2);
const PER_CANDIDATE_DEADLINE: Duration = Duration::from_secs(60 * 60);

/// Sleep between worker iterations when the candidate set is empty or every
/// candidate failed in the previous pass.
const WORKER_IDLE_SLEEP: Duration = Duration::from_millis(500);
const WORKER_RETRY_SLEEP: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotDownloadStatus {
    Imported,
    Skipped(SnapshotImportDecision),
}

#[derive(Default)]
struct SnapshotCandidateProgress {
    imported_threads: usize,
    skipped_threads: usize,
    last_skip: Option<SnapshotImportDecision>,
    terminal_skip: Option<SnapshotImportDecision>,
}

impl SnapshotCandidateProgress {
    fn record_thread_status(
        &mut self,
        checker: &mut BTreeMap<ThreadIdentifier, BlockIdentifier>,
        anchor_thread: ThreadIdentifier,
        thread_id: ThreadIdentifier,
        status: SnapshotImportStatus,
    ) {
        match status {
            SnapshotImportStatus::Imported => {
                self.imported_threads += 1;
                checker.remove(&thread_id);
            }
            SnapshotImportStatus::Skipped(decision) => {
                self.skipped_threads += 1;
                self.last_skip = Some(decision);
                if thread_id == anchor_thread {
                    self.terminal_skip = Some(decision);
                    checker.clear();
                } else {
                    checker.remove(&thread_id);
                }
            }
        }
    }

    fn finish_status(&self) -> SnapshotDownloadStatus {
        if let Some(decision) = self.terminal_skip {
            return SnapshotDownloadStatus::Skipped(decision);
        }
        if self.imported_threads > 0 {
            return SnapshotDownloadStatus::Imported;
        }
        if let Some(decision) = self.last_skip {
            return SnapshotDownloadStatus::Skipped(decision);
        }
        SnapshotDownloadStatus::Imported
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct CandidateKey(BTreeMap<ThreadIdentifier, BlockIdentifier>);

impl From<&BTreeMap<ThreadIdentifier, BlockIdentifier>> for CandidateKey {
    fn from(address: &BTreeMap<ThreadIdentifier, BlockIdentifier>) -> Self {
        Self(address.clone())
    }
}

impl From<&SyncSnapshotRequest> for CandidateKey {
    fn from(request: &SyncSnapshotRequest) -> Self {
        Self::from(&request.address)
    }
}

/// State machine for state-sync candidates. Candidates are keyed by snapshot
/// resource address.
struct LoadCandidates {
    inner: RwLock<LoadCandidatesInner>,
}

struct LoadCandidatesInner {
    pending: VecDeque<SyncSnapshotRequest>,
    in_progress: HashMap<CandidateKey, SyncSnapshotRequest>,
    completed: HashSet<CandidateKey>,
    completed_order: VecDeque<CandidateKey>,
    failed_recently: HashMap<CandidateKey, Instant>,
    failed_order: VecDeque<CandidateKey>,
}

#[derive(Clone, Debug)]
struct ClaimedSnapshot {
    request: SyncSnapshotRequest,
    prune_decision: PruneDecision,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PruneDecision {
    PruneNow,
    Defer { pending_count: usize, in_progress_count: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LoadCandidatePushResult {
    Pushed,
    Duplicate,
    InProgress,
    Completed,
    FailedBackoff,
    StaleHeight,
}

impl LoadCandidatePushResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pushed => "pushed",
            Self::Duplicate => "duplicate",
            Self::InProgress => "in progress",
            Self::Completed => "completed",
            Self::FailedBackoff => "failed backoff",
            Self::StaleHeight => "stale height",
        }
    }
}

impl LoadCandidates {
    fn new() -> Self {
        Self {
            inner: RwLock::new(LoadCandidatesInner {
                pending: VecDeque::with_capacity(LOAD_CANDIDATES_CAPACITY),
                in_progress: HashMap::new(),
                completed: HashSet::new(),
                completed_order: VecDeque::with_capacity(COMPLETED_CANDIDATES_CAPACITY),
                failed_recently: HashMap::new(),
                failed_order: VecDeque::with_capacity(FAILED_CANDIDATES_CAPACITY),
            }),
        }
    }

    fn push_or_upgrade(&self, request: SyncSnapshotRequest) -> LoadCandidatePushResult {
        let key = CandidateKey::from(&request);
        let mut q = self.inner.write();
        if q.completed.contains(&key) {
            tracing::debug!(
                target: "node",
                "snapshot candidate ignored: already completed, anchor_kind={}, address={:?}",
                request.anchor.kind(),
                request.address,
            );
            return LoadCandidatePushResult::Completed;
        }
        if q.failed_recently
            .get(&key)
            .map(|failed_at| failed_at.elapsed() < FAILED_CANDIDATE_BACKOFF)
            .unwrap_or(false)
        {
            tracing::debug!(
                target: "node",
                "snapshot candidate ignored: failed recently, anchor_kind={}, address={:?}",
                request.anchor.kind(),
                request.address,
            );
            return LoadCandidatePushResult::FailedBackoff;
        }
        if let Some(in_progress) = q.in_progress.get_mut(&key) {
            tracing::debug!(
                target: "node",
                "snapshot candidate ignored: already in progress, anchor_kind={}, address={:?}",
                in_progress.anchor.kind(),
                in_progress.address,
            );
            return LoadCandidatePushResult::InProgress;
        }
        if let Some(pending) =
            q.pending.iter_mut().find(|candidate| CandidateKey::from(&**candidate) == key)
        {
            tracing::debug!(
                target: "node",
                "duplicate snapshot candidate ignored: anchor_kind={}, address={:?}",
                pending.anchor.kind(),
                pending.address,
            );
            return LoadCandidatePushResult::Duplicate;
        }
        if let Some(latest_height) = q
            .pending
            .iter()
            .map(|candidate| match candidate.anchor {
                SyncSnapshotAnchor::Height(height) => height,
            })
            .next()
        {
            let SyncSnapshotAnchor::Height(height) = request.anchor;
            let is_newer = latest_height
                .signed_distance_to(&height)
                .map(|distance| distance > 0)
                .unwrap_or(false);
            if !is_newer {
                tracing::debug!(
                    target: "node",
                    "stale snapshot candidate dropped: height={height:?}, latest={latest_height:?}",
                );
                return LoadCandidatePushResult::StaleHeight;
            }
        }
        q.pending.push_front(request);
        while q.pending.len() > LOAD_CANDIDATES_CAPACITY {
            if let Some(stale) = q.pending.pop_back() {
                tracing::debug!(
                    target: "node",
                    "stale snapshot candidate dropped by capacity: anchor_kind={}, address={:?}",
                    stale.anchor.kind(),
                    stale.address,
                );
            }
        }
        q.failed_recently.remove(&key);
        LoadCandidatePushResult::Pushed
    }

    fn claim_next(&self) -> Option<ClaimedSnapshot> {
        let mut q = self.inner.write();
        while let Some(candidate) = q.pending.pop_front() {
            let key = CandidateKey::from(&candidate);
            if q.completed.contains(&key) || q.in_progress.contains_key(&key) {
                continue;
            }
            if q.failed_recently
                .get(&key)
                .map(|failed_at| failed_at.elapsed() < FAILED_CANDIDATE_BACKOFF)
                .unwrap_or(false)
            {
                continue;
            }
            q.in_progress.insert(key.clone(), candidate.clone());
            let pending_count =
                q.pending.iter().filter(|pending| CandidateKey::from(*pending) != key).count();
            let in_progress_count =
                q.in_progress.keys().filter(|in_progress_key| **in_progress_key != key).count();
            let prune_decision = if pending_count == 0 && in_progress_count == 0 {
                PruneDecision::PruneNow
            } else {
                PruneDecision::Defer { pending_count, in_progress_count }
            };
            tracing::debug!(
                target: "node",
                "snapshot candidate claimed: anchor_kind={}, address={:?}",
                candidate.anchor.kind(),
                candidate.address,
            );
            return Some(ClaimedSnapshot { request: candidate, prune_decision });
        }
        None
    }

    fn mark_success(&self, request: &SyncSnapshotRequest) {
        let key = CandidateKey::from(request);
        let mut q = self.inner.write();
        q.in_progress.remove(&key);
        q.pending.retain(|candidate| CandidateKey::from(candidate) != key);
        q.failed_recently.remove(&key);
        q.completed.insert(key.clone());
        q.completed_order.retain(|existing| existing != &key);
        q.completed_order.push_back(key.clone());
        while q.completed_order.len() > COMPLETED_CANDIDATES_CAPACITY {
            if let Some(evicted) = q.completed_order.pop_front() {
                q.completed.remove(&evicted);
            }
        }
        drop(q);
        self.clear_older_than(request.anchor);
        tracing::info!(
            target: "node",
            "snapshot candidate completed: anchor_kind={}, address={:?}",
            request.anchor.kind(),
            request.address,
        );
    }

    fn mark_failure(&self, request: &SyncSnapshotRequest, error: &anyhow::Error) {
        let key = CandidateKey::from(request);
        let mut q = self.inner.write();
        q.in_progress.remove(&key);
        q.failed_recently.insert(key.clone(), Instant::now());
        q.failed_order.retain(|existing| existing != &key);
        q.failed_order.push_back(key.clone());
        while q.failed_order.len() > FAILED_CANDIDATES_CAPACITY {
            if let Some(evicted) = q.failed_order.pop_front() {
                q.failed_recently.remove(&evicted);
            }
        }
        tracing::debug!(
            target: "node",
            "snapshot candidate failed: anchor_kind={}, address={:?}, error={error}",
            request.anchor.kind(),
            request.address,
        );
    }

    fn return_to_the_pool(&self, request: &SyncSnapshotRequest, error: &anyhow::Error) {
        let key = CandidateKey::from(request);
        let mut q = self.inner.write();
        q.in_progress.remove(&key);
        q.pending.push_back(request.clone());
        tracing::debug!(
            target: "node",
            "snapshot candidate failed, but returned to the pool: anchor_kind={}, address={:?}, error={error},",
            request.anchor.kind(),
            request.address,
        );
    }

    fn clear_older_than(&self, anchor: SyncSnapshotAnchor) {
        let mut q = self.inner.write();
        let before = q.pending.len();
        q.pending.retain(|candidate| !is_older_than(candidate.anchor, anchor));
        let dropped = before.saturating_sub(q.pending.len());
        if dropped > 0 {
            tracing::debug!(
                target: "node",
                "stale snapshot candidate(s) dropped after success: dropped={dropped}, anchor_kind={}",
                anchor.kind(),
            );
        }
    }

    fn len(&self) -> usize {
        let inner = self.inner.read();
        inner.pending.len() + inner.in_progress.len()
    }

    fn clear_pending(&self) -> usize {
        let mut q = self.inner.write();
        let dropped = q.pending.len();
        q.pending.clear();
        dropped
    }

    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.inner.read().pending.len()
    }

    #[cfg(test)]
    fn completed_len(&self) -> usize {
        self.inner.read().completed.len()
    }
}

fn is_older_than(candidate: SyncSnapshotAnchor, anchor: SyncSnapshotAnchor) -> bool {
    match (candidate, anchor) {
        (SyncSnapshotAnchor::Height(candidate), SyncSnapshotAnchor::Height(anchor)) => {
            candidate.signed_distance_to(&anchor).map(|distance| distance > 0).unwrap_or(false)
        }
    }
}

#[derive(Clone)]
pub struct ExternalFileSharesBased {
    pub static_storages: Vec<url::Url>,
    pub max_download_tries: u8,
    pub retry_download_timeout: std::time::Duration,
    pub download_deadline_timeout: std::time::Duration,
    blob_sync: ServiceInterface,
    file_saving_service: FileSavingService,
    /// Bounded queue of state-sync candidates received via sync finalized messages.
    /// Shared with the long-lived worker.
    candidates: Arc<LoadCandidates>,
    /// Long-lived load worker. Spawned lazily on first
    /// `add_load_state_task` call. Replaced on completion (success or
    /// shutdown) so a fresh synchronization session can spawn a new one.
    worker_thread: Arc<Mutex<Option<JoinHandle<()>>>>,
    net_topology_rx: Receiver<NetTopology<NodeIdentifier>>,
    bk_set_rx: tokio::sync::watch::Receiver<ApiBkSet>,
}

impl AllowGuardedMut for Option<JoinHandle<()>> {}

impl ExternalFileSharesBased {
    pub fn new(
        blob_sync: ServiceInterface,
        file_saving_service: FileSavingService,
        net_topology_rx: Receiver<NetTopology<NodeIdentifier>>,
        bk_set_rx: tokio::sync::watch::Receiver<ApiBkSet>,
    ) -> Self {
        // TODO: move to config
        Self {
            static_storages: vec![],
            max_download_tries: 30,
            retry_download_timeout: Duration::from_secs(2),
            download_deadline_timeout: Duration::from_secs(600),
            blob_sync,
            file_saving_service,
            candidates: Arc::new(LoadCandidates::new()),
            worker_thread: Arc::new(Mutex::new(None)),
            net_topology_rx,
            bk_set_rx,
        }
    }

    /// Spawn the long-lived worker if it isn't already running.
    fn ensure_worker_running(
        &self,
        repository: RepositoryImpl,
        output: InstrumentedSender<anyhow::Result<SyncSnapshotLoaded>>,
    ) -> anyhow::Result<()> {
        let mut slot = self.worker_thread.lock();
        if let Some(handle) = slot.as_ref() {
            if !handle.is_finished() {
                // Already running.
                return Ok(());
            }
            // Reap the finished handle.
            if let Some(h) = slot.take() {
                let _ = h.join();
            }
        }
        let candidates = Arc::clone(&self.candidates);
        let blob_sync = self.blob_sync.clone();
        let static_storages = self.static_storages.clone();
        let bk_set_rx = self.bk_set_rx.clone();
        let net_topology_rx = self.net_topology_rx.clone();
        let metrics = repository.get_metrics().cloned();

        let handle = std::thread::Builder::new()
            .name("State load worker".to_string())
            .spawn_critical(move || -> anyhow::Result<()> {
                run_load_worker(
                    candidates,
                    output,
                    blob_sync,
                    repository,
                    static_storages,
                    bk_set_rx,
                    net_topology_rx,
                    metrics,
                );
                Ok(())
            })?;
        *slot = Some(handle);
        Ok(())
    }
}

impl StateSyncService for ExternalFileSharesBased {
    type Repository = RepositoryImpl;

    fn save_state_for_sharing(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        anchor: account_state::AnchorBlockRef,
        min_state: Option<Arc<OptimisticStateImpl>>,
        finalizing_block_id: BlockIdentifier,
    ) -> anyhow::Result<SaveStateForSharingStatus> {
        tracing::trace!("save_state_for_sharing: {:?}", block_id);
        let file_name = PathBuf::from(block_id.to_string());
        self.file_saving_service.save_object(
            block_id,
            thread_id,
            anchor,
            min_state,
            file_name,
            finalizing_block_id,
        )
    }

    fn add_load_state_task_with_height(
        &mut self,
        resource_address: BTreeMap<ThreadIdentifier, BlockIdentifier>,
        block_height: BlockHeight,
        repository: RepositoryImpl,
        output: InstrumentedSender<anyhow::Result<SyncSnapshotLoaded>>,
    ) -> anyhow::Result<()> {
        let request = SyncSnapshotRequest {
            address: resource_address,
            anchor: SyncSnapshotAnchor::Height(block_height),
        };
        let push_result = self.candidates.push_or_upgrade(request);
        tracing::trace!(
            target: "node",
            "add_load_state_task_with_height: {} height-aware candidate (queue size = {})",
            push_result.as_str(),
            self.candidates.len(),
        );
        // Spawn (or re-spawn) the worker if it isn't running. This is
        // idempotent — repeated calls during a single sync session
        // observe the same worker.
        self.ensure_worker_running(repository, output)?;
        Ok(())
    }

    /// Always returns `true`. Per the new design, `synchronization.rs`
    /// must keep broadcasting `NodeJoining` regardless of whether a
    /// download is in progress; the bounded candidate queue absorbs the
    /// resulting `SyncFinalized` responses, and the worker iterates
    /// candidates newest-first.
    fn is_load_thread_available(&self) -> bool {
        true
    }

    fn clear_load_state_tasks(&mut self) {
        let dropped = self.candidates.clear_pending();
        if dropped > 0 {
            tracing::debug!(
                target: "node",
                "cleared pending state-sync candidates: dropped={dropped}",
            );
        }
    }

    fn flush(&self) -> anyhow::Result<()> {
        self.file_saving_service.flush()
    }

    fn shutdown_snapshot_workers(&self, timeout: std::time::Duration) -> anyhow::Result<()> {
        self.file_saving_service.shutdown_snapshot_workers(timeout)
    }

    fn wait_snapshot_workers(&self) -> anyhow::Result<()> {
        self.file_saving_service.wait_snapshot_workers()
    }
}

/// Long-lived load worker. Iterates the candidate deque newest-first,
/// trying each with a short download budget. On the first success,
/// reports the result through `output` and exits. If every candidate
/// fails, sleeps briefly and re-snapshots — the deque may have new
/// entries by the next pass thanks to ongoing `NodeJoining` broadcasts.
#[allow(clippy::too_many_arguments)]
fn run_load_worker(
    candidates: Arc<LoadCandidates>,
    output: InstrumentedSender<anyhow::Result<SyncSnapshotLoaded>>,
    mut blob_sync: ServiceInterface,
    repository: RepositoryImpl,
    static_storages: Vec<Url>,
    bk_set_rx: tokio::sync::watch::Receiver<ApiBkSet>,
    net_topology_rx: Receiver<NetTopology<NodeIdentifier>>,
    metrics: Option<crate::helper::metrics::BlockProductionMetrics>,
) {
    loop {
        if SHUTDOWN_FLAG.get() == Some(&true) {
            return;
        }
        let Some(claimed) = candidates.claim_next() else {
            std::thread::sleep(WORKER_IDLE_SLEEP);
            continue;
        };
        if SHUTDOWN_FLAG.get() == Some(&true) {
            return;
        }
        let candidate = claimed.request;
        match decide_snapshot_import_for_request(&repository, &candidate) {
            Ok(SnapshotImportDecision::ImportNeeded { distance_from_last_finalized }) => {
                tracing::debug!(
                    target: "monit",
                    ?candidate,
                    ?distance_from_last_finalized,
                    "Snapshot candidate requires import",
                );
            }
            Ok(SnapshotImportDecision::AlreadyCovered { distance_from_last_finalized }) => {
                tracing::info!(
                    target: "node",
                    ?candidate,
                    distance_from_last_finalized,
                    "Skipping snapshot download because local finalized state already covers it",
                );
                finish_skipped_candidate(
                    &candidates,
                    &output,
                    &candidate,
                    SnapshotImportDecision::AlreadyCovered { distance_from_last_finalized },
                );
                return;
            }
            Ok(SnapshotImportDecision::TooClose { distance_from_last_finalized }) => {
                tracing::info!(
                    target: "node",
                    ?candidate,
                    distance_from_last_finalized,
                    "Skipping snapshot download because candidate is too close to local finalized state",
                );
                finish_skipped_candidate(
                    &candidates,
                    &output,
                    &candidate,
                    SnapshotImportDecision::TooClose { distance_from_last_finalized },
                );
                return;
            }
            Ok(SnapshotImportDecision::Invalidated) => {
                let error = anyhow::anyhow!("snapshot candidate is invalidated: {candidate:?}");
                candidates.mark_failure(&candidate, &error);
                std::thread::sleep(WORKER_RETRY_SLEEP);
                continue;
            }
            Err(error) => {
                candidates.mark_failure(&candidate, &error);
                std::thread::sleep(WORKER_RETRY_SLEEP);
                continue;
            }
        }
        match claimed.prune_decision {
            PruneDecision::PruneNow => {
                tracing::info!(
                    target: "node",
                    "Pruning unfinalized blocks before snapshot download: anchor={:?}, candidate={:?}",
                    candidate.anchor,
                    candidate.address,
                );
                prune_unfinalized_before_snapshot(&repository, &candidate);
            }
            PruneDecision::Defer { pending_count, in_progress_count } => {
                tracing::debug!(
                    target: "monit",
                    "Deferring pruning before snapshot download because other snapshot candidates are pending/in progress: candidate={:?}, pending_count={pending_count}, in_progress_count={in_progress_count}",
                    candidate,
                );
            }
        }
        let urls = resolve_urls_for_attempt(&static_storages, &bk_set_rx, &net_topology_rx);
        tracing::trace!(
            target: "node",
            "Load worker: processing candidate anchor_kind={} address={:?} urls={}",
            candidate.anchor.kind(),
            candidate.address,
            urls.len(),
        );
        match try_download_candidate(
            &candidate,
            &mut blob_sync,
            &repository,
            urls,
            metrics.as_ref(),
        ) {
            Ok(SnapshotDownloadStatus::Imported) => {
                candidates.mark_success(&candidate);
                tracing::info!(
                    target: "node",
                    "Pruning unfinalized blocks after accepting loaded snapshot: anchor={:?}, loaded={:?}",
                    candidate.anchor,
                    candidate.address,
                );
                prune_unfinalized_before_snapshot(&repository, &candidate);
                let _ = output.send(Ok(SyncSnapshotLoaded {
                    address: candidate.address.clone(),
                    anchor: candidate.anchor,
                }));
                return;
            }
            Ok(SnapshotDownloadStatus::Skipped(decision)) => {
                tracing::info!(
                    target: "node",
                    ?candidate,
                    ?decision,
                    "Snapshot download completed but import was skipped",
                );
                finish_skipped_candidate(&candidates, &output, &candidate, decision);
                return;
            }
            Err(e) => {
                let mut mark_failure = true;
                if let Some(download_err) = e.downcast_ref::<DownloadError>() {
                    if *download_err == DownloadError::MaxTriesExceeded {
                        tracing::warn!("Download error {:?}, skip error", e);
                        mark_failure = false;
                    }
                }
                if mark_failure {
                    candidates.mark_failure(&candidate, &e);
                } else {
                    candidates.return_to_the_pool(&candidate, &e);
                }
                std::thread::sleep(WORKER_RETRY_SLEEP);
            }
        }
    }
}

fn finish_skipped_candidate(
    candidates: &LoadCandidates,
    output: &InstrumentedSender<anyhow::Result<SyncSnapshotLoaded>>,
    candidate: &SyncSnapshotRequest,
    decision: SnapshotImportDecision,
) {
    candidates.mark_success(candidate);
    let _ = output.send(Err(SyncSnapshotSkipped {
        request: candidate.clone(),
        reason: format!("decision={decision:?}"),
    }
    .into()));
}

fn decide_snapshot_import_for_request(
    repository: &RepositoryImpl,
    candidate: &SyncSnapshotRequest,
) -> anyhow::Result<SnapshotImportDecision> {
    match candidate.anchor {
        SyncSnapshotAnchor::Height(block_height) => {
            let thread_id = *block_height.thread_identifier();
            let block_id = candidate.address.get(&thread_id).ok_or_else(|| {
                anyhow::anyhow!(
                    "snapshot candidate address is missing anchor thread {thread_id:?}: {candidate:?}"
                )
            })?;
            repository.decide_snapshot_import(&thread_id, block_id, &block_height)
        }
    }
}

fn prune_unfinalized_before_snapshot(repository: &RepositoryImpl, candidate: &SyncSnapshotRequest) {
    let collections = repository.unfinalized_blocks();
    match candidate.anchor {
        SyncSnapshotAnchor::Height(height) => {
            let thread_id = height.thread_identifier();
            let pruned = collections.guarded(|map| {
                map.get(thread_id)
                    .map(|collection| {
                        collection.prune_before_snapshot(UnfinalizedCutoff::Height(height))
                    })
                    .unwrap_or_default()
            });
            tracing::info!(
                target: "node",
                "unfinalized blocks pruned before snapshot download: pruned={pruned}, anchor_kind=height, thread_id={thread_id:?}, height={height:?}",
            );
        }
    }
}

fn resolve_urls_for_attempt(
    static_storages: &[Url],
    bk_set_rx: &tokio::sync::watch::Receiver<ApiBkSet>,
    net_topology_rx: &Receiver<NetTopology<NodeIdentifier>>,
) -> Vec<Url> {
    let current_bk_set_node_ids = bk_set_rx
        .borrow()
        .current
        .iter()
        .map(|bk| NodeIdentifier::from(AccountIdentifier::new(bk.owner_address.0)))
        .collect::<HashSet<_>>();
    let mut services = HashSet::<Url>::from_iter(static_storages.iter().cloned());
    for (node_id, peers) in net_topology_rx.borrow().peer_resolver() {
        if current_bk_set_node_ids.contains(node_id) {
            for peer in peers {
                if let Some(base_url) = &peer.bk_api_url_for_storage_sync {
                    if let Ok(url) = base_url.join("v2/storage/") {
                        services.insert(url);
                    }
                }
            }
        }
    }
    Vec::from_iter(services)
}

/// Synchronously try one candidate. Dispatches `load_blob` for every
/// `(thread_id, block_id)` in the candidate's address and waits until
/// they all complete or any one fails. A skipped non-anchor thread is
/// treated as completed for this candidate; the anchor thread decides whether
/// the whole candidate can be skipped. Uses short retry budgets so the worker
/// can move to the next candidate quickly on persistent 404s.
fn try_download_candidate(
    candidate: &SyncSnapshotRequest,
    blob_sync: &mut ServiceInterface,
    repository: &RepositoryImpl,
    external_blob_share_services: Vec<Url>,
    metrics: Option<&crate::helper::metrics::BlockProductionMetrics>,
) -> anyhow::Result<SnapshotDownloadStatus> {
    let address = candidate.address.clone();
    let SyncSnapshotAnchor::Height(anchor_height) = candidate.anchor;
    let anchor_thread = *anchor_height.thread_identifier();
    let checker = Arc::new(Mutex::new(address.clone()));
    let last_error = Arc::new(Mutex::new(None::<anyhow::Error>));
    let progress = Arc::new(Mutex::new(SnapshotCandidateProgress::default()));
    let repo = Arc::new(Mutex::new(repository.clone()));

    if let Some(m) = metrics {
        m.report_state_request();
    }

    for (thread_id, block_id) in &address {
        let checker_clone = checker.clone();
        let checker_clone2 = checker.clone();
        let last_error_clone = last_error.clone();
        let progress_clone = progress.clone();
        let repo_clone = repo.clone();
        let metrics_on_error = metrics.cloned();
        let urls = external_blob_share_services.clone();
        let thread_id_copy = *thread_id;
        let block_id_copy = *block_id;

        blob_sync.load_blob(
            block_id.to_string(),
            urls,
            PER_CANDIDATE_MAX_TRIES,
            Some(PER_CANDIDATE_RETRY_TIMEOUT),
            Some(Instant::now() + PER_CANDIDATE_DEADLINE),
            move |e| -> anyhow::Result<()> {
                let mut header = [0u8; COMPRESSED_SNAPSHOT_MAGIC.len()];
                match e.read_exact(&mut header) {
                    Ok(()) => {
                        let res = if &header == COMPRESSED_SNAPSHOT_MAGIC {
                            tracing::trace!(
                                "load worker: read COMPRESSED_SNAPSHOT_MAGIC for {thread_id_copy:?}",
                            );
                            match zstd::Decoder::new(e) {
                                Ok(mut decoder) => {
                                    repo_clone.lock().set_state_from_snapshot_reader_checked(
                                        &mut decoder,
                                        &thread_id_copy,
                                        Arc::new(Mutex::new(HashSet::new())),
                                    )
                                }
                                Err(e) => Err(e.into()),
                            }
                        } else {
                            tracing::trace!(
                                "load worker: read non-compressed snapshot for {thread_id_copy:?}",
                            );
                            let mut reader = std::io::Cursor::new(header).chain(e);
                            repo_clone.lock().set_state_from_snapshot_reader_checked(
                                &mut reader,
                                &thread_id_copy,
                                Arc::new(Mutex::new(HashSet::new())),
                            )
                        };
                        tracing::trace!(
                            "load worker: for {thread_id_copy:?} res={res:?}",
                        );
                        let status = res?;
                        match status {
                            SnapshotImportStatus::Imported => {
                                tracing::trace!(
                                    "load worker: done for {thread_id_copy:?} (block={block_id_copy:?})",
                                );
                            }
                            SnapshotImportStatus::Skipped(decision) => {
                                tracing::info!(
                                    target: "node",
                                    ?decision,
                                    "load worker: snapshot import skipped for {thread_id_copy:?} (block={block_id_copy:?})",
                                );
                            }
                        }
                        let mut checker = checker_clone.lock();
                        progress_clone.lock().record_thread_status(
                            &mut checker,
                            anchor_thread,
                            thread_id_copy,
                            status,
                        );
                        Ok(())
                    }
                    Err(e) => Err(e.into()),
                }
            },
            move |e| {
                tracing::trace!(
                    target: "node",
                    "load worker: load_blob failed for {block_id_copy:?}: {e}",
                );
                *last_error_clone.lock() = Some(e);
                checker_clone2.lock().clear();
                if let Some(m) = metrics_on_error {
                    m.report_error("load_state_error");
                }
            },
        )?;
    }

    // Block this worker iteration until all dispatched downloads finish
    // (checker drains) or any one fails (on_error clears the checker
    // and sets last_error).
    loop {
        if SHUTDOWN_FLAG.get() == Some(&true) {
            return Err(anyhow::anyhow!("shutdown"));
        }
        let is_empty = { checker.lock().is_empty() };
        if is_empty {
            if let Some(err) = last_error.lock().take() {
                return Err(err);
            }
            return Ok(progress.lock().finish_status());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use node_types::BlockIdentifier;
    use node_types::ThreadIdentifier;

    use super::LoadCandidatePushResult;
    use super::LoadCandidates;
    use super::PruneDecision;
    use super::SnapshotCandidateProgress;
    use super::SnapshotDownloadStatus;
    use super::LOAD_CANDIDATES_CAPACITY;
    use crate::node::services::sync::SyncSnapshotAnchor;
    use crate::node::services::sync::SyncSnapshotRequest;
    use crate::repository::repository_impl::SnapshotImportDecision;
    use crate::repository::repository_impl::SnapshotImportStatus;
    use crate::types::BlockHeight;

    fn block_id(seed: u8) -> BlockIdentifier {
        BlockIdentifier::new([seed; 32])
    }

    fn thread_id() -> ThreadIdentifier {
        ThreadIdentifier::new(&block_id(u8::MAX), 7)
    }

    fn candidate_with_height(address_seed: u8, height: u64) -> SyncSnapshotRequest {
        let thread_identifier = thread_id();
        SyncSnapshotRequest {
            address: BTreeMap::from([(thread_identifier, block_id(address_seed))]),
            anchor: SyncSnapshotAnchor::Height(
                BlockHeight::builder().thread_identifier(thread_identifier).height(height).build(),
            ),
        }
    }

    fn import_needed() -> SnapshotImportDecision {
        SnapshotImportDecision::ImportNeeded { distance_from_last_finalized: Some(10) }
    }

    #[test]
    fn non_anchor_skip_does_not_skip_candidate_if_anchor_imports() {
        let anchor_thread = thread_id();
        let other_thread = ThreadIdentifier::new(&block_id(42), 3);
        let mut checker =
            BTreeMap::from([(anchor_thread, block_id(1)), (other_thread, block_id(2))]);
        let mut progress = SnapshotCandidateProgress::default();

        progress.record_thread_status(
            &mut checker,
            anchor_thread,
            other_thread,
            SnapshotImportStatus::Skipped(import_needed()),
        );

        assert!(checker.contains_key(&anchor_thread));
        assert!(!checker.contains_key(&other_thread));

        progress.record_thread_status(
            &mut checker,
            anchor_thread,
            anchor_thread,
            SnapshotImportStatus::Imported,
        );

        assert!(checker.is_empty());
        assert_eq!(progress.finish_status(), SnapshotDownloadStatus::Imported);
    }

    #[test]
    fn anchor_skip_skips_candidate() {
        let anchor_thread = thread_id();
        let other_thread = ThreadIdentifier::new(&block_id(42), 3);
        let mut checker =
            BTreeMap::from([(anchor_thread, block_id(1)), (other_thread, block_id(2))]);
        let mut progress = SnapshotCandidateProgress::default();
        let decision = SnapshotImportDecision::TooClose { distance_from_last_finalized: 2 };

        progress.record_thread_status(
            &mut checker,
            anchor_thread,
            anchor_thread,
            SnapshotImportStatus::Skipped(decision),
        );

        assert!(checker.is_empty());
        assert_eq!(progress.finish_status(), SnapshotDownloadStatus::Skipped(decision));
    }

    #[test]
    fn all_skipped_without_import_returns_skipped() {
        let anchor_thread = thread_id();
        let other_thread = ThreadIdentifier::new(&block_id(42), 3);
        let mut checker = BTreeMap::from([(other_thread, block_id(2))]);
        let mut progress = SnapshotCandidateProgress::default();
        let decision = SnapshotImportDecision::AlreadyCovered { distance_from_last_finalized: 0 };

        progress.record_thread_status(
            &mut checker,
            anchor_thread,
            other_thread,
            SnapshotImportStatus::Skipped(decision),
        );

        assert!(checker.is_empty());
        assert_eq!(progress.finish_status(), SnapshotDownloadStatus::Skipped(decision));
    }

    #[test]
    fn push_adds_first_candidate() {
        let candidates = LoadCandidates::new();

        assert_eq!(
            candidates.push_or_upgrade(candidate_with_height(1, 10)),
            LoadCandidatePushResult::Pushed
        );
        assert_eq!(candidates.len(), 1);
    }

    #[test]
    fn push_rejects_duplicate_address() {
        let candidates = LoadCandidates::new();
        let initial = candidate_with_height(1, 10);
        let duplicate = SyncSnapshotRequest {
            address: initial.address.clone(),
            anchor: SyncSnapshotAnchor::Height(
                BlockHeight::builder().thread_identifier(thread_id()).height(11).build(),
            ),
        };

        assert_eq!(candidates.push_or_upgrade(initial), LoadCandidatePushResult::Pushed);
        assert_eq!(candidates.push_or_upgrade(duplicate), LoadCandidatePushResult::Duplicate);
        assert_eq!(candidates.len(), 1);
    }

    #[test]
    fn push_rejects_smaller_or_equal_block_height() {
        let candidates = LoadCandidates::new();

        assert_eq!(
            candidates.push_or_upgrade(candidate_with_height(1, 10)),
            LoadCandidatePushResult::Pushed
        );
        assert_eq!(
            candidates.push_or_upgrade(candidate_with_height(2, 9)),
            LoadCandidatePushResult::StaleHeight
        );
        assert_eq!(
            candidates.push_or_upgrade(candidate_with_height(3, 10)),
            LoadCandidatePushResult::StaleHeight
        );
        assert_eq!(candidates.len(), 1);
    }

    #[test]
    fn push_adds_greater_block_height() {
        let candidates = LoadCandidates::new();

        assert_eq!(
            candidates.push_or_upgrade(candidate_with_height(1, 10)),
            LoadCandidatePushResult::Pushed
        );
        assert_eq!(
            candidates.push_or_upgrade(candidate_with_height(2, 11)),
            LoadCandidatePushResult::Pushed
        );

        let first = candidates.claim_next().unwrap();
        assert!(matches!(
            first.request.anchor,
            SyncSnapshotAnchor::Height(height) if *height.height() == 11
        ));
    }

    #[test]
    fn push_preserves_capacity_limit() {
        let candidates = LoadCandidates::new();

        for height in 1..=(LOAD_CANDIDATES_CAPACITY as u64 + 1) {
            assert_eq!(
                candidates.push_or_upgrade(candidate_with_height(height as u8, height)),
                LoadCandidatePushResult::Pushed
            );
        }

        let mut heights = vec![];
        while let Some(candidate) = candidates.claim_next() {
            let SyncSnapshotAnchor::Height(height) = candidate.request.anchor;
            heights.push(*height.height());
        }
        let expected_heights: Vec<u64> =
            (2..=(LOAD_CANDIDATES_CAPACITY as u64 + 1)).rev().collect();

        assert_eq!(heights.len(), LOAD_CANDIDATES_CAPACITY);
        assert_eq!(heights, expected_heights);
    }

    #[test]
    fn completed_candidate_is_not_queued_again() {
        let candidates = LoadCandidates::new();
        let request = candidate_with_height(1, 10);

        assert_eq!(candidates.push_or_upgrade(request.clone()), LoadCandidatePushResult::Pushed);
        let claimed = candidates.claim_next().unwrap().request;
        candidates.mark_success(&claimed);

        assert_eq!(candidates.completed_len(), 1);
        assert_eq!(candidates.push_or_upgrade(request), LoadCandidatePushResult::Completed);
        assert_eq!(candidates.pending_len(), 0);
    }

    #[test]
    fn failed_candidate_requires_new_signal_after_backoff() {
        let candidates = LoadCandidates::new();
        let request = candidate_with_height(1, 10);

        assert_eq!(candidates.push_or_upgrade(request.clone()), LoadCandidatePushResult::Pushed);
        let claimed = candidates.claim_next().unwrap().request;
        candidates.mark_failure(&claimed, &anyhow::anyhow!("test failure"));

        assert!(candidates.claim_next().is_none());
        assert_eq!(candidates.push_or_upgrade(request), LoadCandidatePushResult::FailedBackoff);
    }

    #[test]
    fn claim_single_candidate_prunes_now() {
        let candidates = LoadCandidates::new();
        assert_eq!(
            candidates.push_or_upgrade(candidate_with_height(1, 10)),
            LoadCandidatePushResult::Pushed
        );

        let claimed = candidates.claim_next().unwrap();
        assert_eq!(claimed.prune_decision, PruneDecision::PruneNow);
    }

    #[test]
    fn claim_multiple_pending_defers_pruning() {
        let candidates = LoadCandidates::new();
        assert_eq!(
            candidates.push_or_upgrade(candidate_with_height(1, 10)),
            LoadCandidatePushResult::Pushed
        );
        assert_eq!(
            candidates.push_or_upgrade(candidate_with_height(2, 11)),
            LoadCandidatePushResult::Pushed
        );

        let claimed = candidates.claim_next().unwrap();
        assert!(matches!(
            claimed.prune_decision,
            PruneDecision::Defer { pending_count: 1, in_progress_count: 0 }
        ));
    }

    #[test]
    fn pending_plus_in_progress_defers_pruning() {
        let candidates = LoadCandidates::new();
        assert_eq!(
            candidates.push_or_upgrade(candidate_with_height(1, 10)),
            LoadCandidatePushResult::Pushed
        );
        assert_eq!(
            candidates.push_or_upgrade(candidate_with_height(2, 11)),
            LoadCandidatePushResult::Pushed
        );
        assert_eq!(
            candidates.push_or_upgrade(candidate_with_height(3, 12)),
            LoadCandidatePushResult::Pushed
        );

        let _first = candidates.claim_next().unwrap();
        let second = candidates.claim_next().unwrap();
        assert!(matches!(
            second.prune_decision,
            PruneDecision::Defer { pending_count: 1, in_progress_count: 1 }
        ));
    }
}
