//! Thread-snapshot download actor. Only this task mutates scheduling/trust
//! state. Worker commands carry a common response sender, so select! can service
//! all four transfers without polling a collection of one-shot receivers.
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Seek;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinSet;
use url::Url;

use super::super::snapshot_consumption::decode_blob_with_transport;
use super::super::snapshot_consumption::SnapshotImportLimits;
use super::super::snapshot_http_import::SnapshotHttpImport;
use super::super::snapshot_manifest::ThreadSnapshotManifest;
use super::super::snapshot_partitioned::read_primary_snapshot_checked;
use super::super::snapshot_partitioned::PrimaryThreadSnapshot;
use super::super::snapshot_sources::SnapshotSourcePlan;
use super::partition_downloader::BlobDownloadLimits;
use super::partition_downloader::DownloadAdmission;
use super::partition_downloader::PartitionDownloadResponse;
use super::partition_downloader::PartitionDownloadState;
use super::partition_downloader::PartitionDownloader;
use super::partition_downloader::PartitionDownloaderCommand;
use super::partition_downloader::TransferError;
use crate::node::NodeIdentifier;

pub(super) enum ThreadSnapshotDownloaderCommand {
    Start {
        thread_id: ThreadIdentifier,
        block_id: BlockIdentifier,
        known_sources: Vec<(NodeIdentifier, Url)>,
        fallback_sources: Vec<Url>,
        events: mpsc::UnboundedSender<ThreadSnapshotDownloaderEvent>,
        reply: oneshot::Sender<StartResponse>,
    },
    /// The importer has closed its reader. Accept/reject changes only cache
    /// lifetime and provider trust, never the worker-slot scheduler.
    BlobProcessed {
        blob_id: [u8; 32],
        accepted: bool,
        reply: oneshot::Sender<BlobProcessedResponse>,
    },
    Stop {
        reply: oneshot::Sender<StopResponse>,
    },
}
/// Startup acknowledgement, not a snapshot-import completion notification.
pub(super) struct StartResponse;
/// Sent after cancelling and joining every worker, never before.
pub(super) struct StopResponse;

/// Readiness notifications contain no transport state or acknowledgement
/// channels. Each variant states exactly which codec the consumer must use.
#[allow(clippy::large_enum_variant)] // One primary is moved, never broadcast.
#[expect(
    clippy::enum_variant_names,
    reason = "These events intentionally describe readiness, not commands"
)]
pub(super) enum ThreadSnapshotDownloaderEvent {
    PrimaryReady(PrimaryThreadSnapshot),
    AccountsReady { root: [u8; 32], path: PathBuf },
    NormalMessagesReady { root: [u8; 32], path: PathBuf },
    HighPriorityMessagesReady { root: [u8; 32], path: PathBuf },
}

/// Cache acceptance/rejection has completed before this command response.
pub(super) struct BlobProcessedResponse {
    pub result: anyhow::Result<()>,
}

pub(super) struct ThreadSnapshotDownloader {
    directory: PathBuf,
    limits: BlobDownloadLimits,
    import_limits: SnapshotImportLimits,
    state: SnapshotDownloadState,
    cancellation: watch::Receiver<bool>,
    cancel_import: watch::Sender<bool>,
    seed: Option<PrimaryThreadSnapshot>,
    primary_sources: Vec<Url>,
    primary_id: [u8; 32],
    thread_id: ThreadIdentifier,
    primary_ready: bool,
    events: Option<mpsc::UnboundedSender<ThreadSnapshotDownloaderEvent>>,
    known: Vec<(NodeIdentifier, Url)>,
    fallbacks: Vec<Url>,
    source_plan: Option<SnapshotSourcePlan>,
    last_failure: Option<anyhow::Error>,
}

impl ThreadSnapshotDownloader {
    pub fn new(
        request: &SnapshotHttpImport<'_>,
        seed: Option<PrimaryThreadSnapshot>,
        primary_sources: Vec<Url>,
        cancellation: watch::Sender<bool>,
    ) -> Self {
        Self {
            directory: request.cache_directory.to_owned(),
            limits: request.download_limits,
            import_limits: request.import_limits,
            state: SnapshotDownloadState::new(request.fetch_limits.concurrent_downloads),
            cancellation: cancellation.subscribe(),
            cancel_import: cancellation,
            seed,
            primary_sources,
            primary_id: *request.expected.1.as_array(),
            thread_id: request.expected.0,
            primary_ready: false,
            events: None,
            known: Vec::new(),
            fallbacks: Vec::new(),
            source_plan: None,
            last_failure: None,
        }
    }

    pub async fn run(
        mut self,
        mut commands: mpsc::Receiver<ThreadSnapshotDownloaderCommand>,
    ) -> anyhow::Result<()> {
        let Some(ThreadSnapshotDownloaderCommand::Start {
            thread_id,
            block_id,
            known_sources,
            fallback_sources,
            events,
            reply,
        }) = commands.recv().await
        else {
            anyhow::bail!("snapshot downloader requires Start");
        };
        anyhow::ensure!(
            self.primary_id == *block_id.as_array() && self.thread_id == thread_id,
            "snapshot downloader identity mismatch"
        );
        self.known = known_sources;
        self.fallbacks = fallback_sources;
        self.events = Some(events);
        for base in self.known.iter().map(|(_, url)| url).chain(&self.fallbacks) {
            let url = base.join(&hex::encode(self.primary_id))?;
            if !self.primary_sources.contains(&url) {
                self.primary_sources.push(url);
            }
        }
        self.state.add(self.primary_id, None, std::mem::take(&mut self.primary_sources))?;
        let _ = reply.send(StartResponse);
        let (responses, mut completed) = mpsc::channel(self.state.slots.len());
        let mut workers = JoinSet::new();
        let mut mailboxes = Vec::new();
        for slot in 0..self.state.slots.len() {
            let (sender, mut receiver) = mpsc::channel(1);
            let mut worker = PartitionDownloader::new(&self.directory, self.limits)?;
            // Physical slots are reused across blobs/retries: no per-blob time
            // series, and concurrent blockchain threads cannot overwrite them.
            worker.activity_slot = format!("{}:{slot}", self.thread_id);
            workers.spawn(async move {
                while let Some(command) = receiver.recv().await {
                    worker.run(command).await;
                }
            });
            mailboxes.push(sender);
        }
        let mut stop_reply = None;
        let mut enforce_download_deadline = true;
        let result = async {
            if let Some(primary) = self.seed.take() { self.deliver_primary(primary)?; }
            loop {
                self.schedule(&mailboxes, &responses).await?;
                tokio::select! {
                    biased;
                    command = commands.recv() => match command {
                        Some(ThreadSnapshotDownloaderCommand::Stop { reply }) => { stop_reply = Some(reply); break; },
                        Some(ThreadSnapshotDownloaderCommand::BlobProcessed { blob_id, accepted, reply }) => {
                            let processed = self.process_blob(blob_id, accepted).await;
                            let outcome = processed.as_ref().map(|_| ()).map_err(|error| anyhow::anyhow!("{error:#}"));
                            let _ = reply.send(BlobProcessedResponse { result: outcome });
                            processed?;
                            if !accepted { break; }
                        },
                        Some(ThreadSnapshotDownloaderCommand::Start { .. }) => anyhow::bail!("snapshot downloader already started"),
                        None => break,
                    },
                    response = completed.recv() => if let Some(response) = response { self.finish_attempt(response).await?; },
                    _ = self.cancellation.changed() => break,
                    _ = tokio::time::sleep_until(self.limits.deadline.into()), if enforce_download_deadline => {
                        // The actor outlives transport: it keeps verified files
                        // leased until the importer accepts/rejects them. A slow
                        // importer must not lose its staged state just because
                        // those acknowledgements take longer than downloading.
                        anyhow::ensure!(self.primary_ready && self.state.partitions.values().all(|part| part.ready),
                            "snapshot download deadline exceeded");
                        // Check once at expiry, not on every scheduler tick or
                        // importer acknowledgement (large manifests are bounded
                        // but may still contain many thousands of partitions).
                        enforce_download_deadline = false;
                    },
                    exited = workers.join_next(), if !workers.is_empty() => anyhow::bail!("snapshot worker exited unexpectedly: {exited:?}"),
                    // Only retry/disk-admission timers are polled. HTTP replies,
                    // cancellation and command delivery wake select! immediately.
                    _ = tokio::time::sleep(Duration::from_millis(25)) => {},
                }
                if *self.cancellation.borrow() { break; }
            }
            Ok(())
        }.await;
        if result.is_err() {
            let _ = self.cancel_import.send(true);
        }
        for slot in self.state.slots.iter().flatten() {
            let _ = slot.cancel.send(true);
        }
        mailboxes.clear();
        drop(completed); // A cancelled worker must not block reporting its final result.
        self.events.take();
        while let Some(joined) = workers.join_next().await {
            if let Err(error) = joined {
                tracing::warn!("snapshot worker failed during shutdown: {error}");
            }
        }
        if let Some(reply) = stop_reply {
            let _ = reply.send(StopResponse);
        }
        if let Err(error) = &result {
            tracing::warn!("thread snapshot download stopped: {error:#}");
        }
        result
    }

    fn deliver_primary(&mut self, primary: PrimaryThreadSnapshot) -> anyhow::Result<()> {
        primary.manifest.validate_root_limits(self.import_limits.roots_per_list)?;
        let plan = SnapshotSourcePlan::new(
            &primary.manifest,
            &primary.metadata.bk_set,
            self.known.iter().cloned(),
        )?;
        self.state.manifest = primary.manifest.clone();
        for index in 0..plan.len() {
            let id = plan.blob_id(index).expect("manifest index");
            self.state.add(id, Some(index), Vec::new())?;
        }
        self.source_plan = Some(plan);
        self.state.partitions.get_mut(&self.primary_id).expect("primary entry").ready = true;
        if let Some(mut admission) =
            self.state.partitions.get_mut(&self.primary_id).expect("primary").admission.take()
        {
            admission.admitted();
        }
        self.primary_ready = true;
        self.events
            .as_ref()
            .expect("started")
            .send(ThreadSnapshotDownloaderEvent::PrimaryReady(primary))
            .map_err(|_| anyhow::anyhow!("snapshot importer stopped"))?;
        Ok(())
    }

    /// Rank endpoints lazily for a dispatched partition. Materializing P*N URLs
    /// in the manifest state would dwarf the bounded root lists on large plans.
    fn partition_sources(&self, part: &PartitionState) -> anyhow::Result<Vec<Url>> {
        let Some(index) = part.manifest_index else {
            return Ok(part.sources.clone());
        };
        let plan = self.source_plan.as_ref().expect("decoded primary source plan");
        let path = plan.partition_path(&self.primary_id, index);
        let mut sources = plan
            .partition_sources(index)?
            .into_iter()
            .map(|url| Ok(url.join(&path)?))
            .collect::<anyhow::Result<Vec<_>>>()?;
        for base in &self.fallbacks {
            let source = base.join(&path)?;
            if !sources.contains(&source) {
                sources.push(source);
            }
        }
        Ok(sources)
    }

    async fn schedule(
        &mut self,
        workers: &[mpsc::Sender<PartitionDownloaderCommand>],
        replies: &mpsc::Sender<PartitionDownloadResponse>,
    ) -> anyhow::Result<()> {
        if *self.cancellation.borrow() {
            return Ok(());
        }
        for (slot, worker) in workers.iter().enumerate() {
            if self.state.slots[slot].is_some() {
                continue;
            }
            let mut selected = None;
            for id in &self.state.order {
                let part = &self.state.partitions[id];
                if part.ready || part.active_task.is_some() {
                    continue;
                }
                if *id != self.primary_id && !self.primary_ready {
                    continue;
                }
                let source = if !part.cache_checked {
                    None
                } else {
                    let sources = self.partition_sources(part)?;
                    let best = sources
                        .iter()
                        .enumerate()
                        .filter_map(|(rank, url)| {
                            let provider = self.state.sources.get(&source_base(url).ok()?)?;
                            if provider.banned.is_some()
                                || part.attempts.get(url).copied().unwrap_or(0)
                                    >= u32::from(self.limits.rounds)
                            {
                                return None;
                            }
                            // Prefer any available endpoint over one still on
                            // publication cooldown, without penalizing that
                            // provider for other partitions in the snapshot.
                            let cooling = part
                                .not_published
                                .get(url)
                                .is_some_and(|(_, until)| Instant::now() < *until);
                            Some(((cooling, provider.penalty, rank), url.clone()))
                        })
                        .min_by_key(|(rank, _)| *rank);
                    if best.is_none() {
                        return Err(self.last_failure.take().unwrap_or_else(|| {
                            anyhow::anyhow!("no usable snapshot partition source")
                        }));
                    }
                    let (rank, url) = best.expect("usable source");
                    if rank.0 {
                        // Preserve a worker for this admission-chain head's
                        // retry, just as for transient network backoff below.
                        if part.has_pending_admission() {
                            break;
                        }
                        continue;
                    }
                    Some(url)
                };
                if Instant::now() < part.retry_after {
                    // Successors can occupy workers while waiting for this
                    // blob's disk admission. Keep a worker free for its retry;
                    // otherwise all four can wait behind an unscheduled task.
                    // Once admitted, payload retries impose no such barrier.
                    if part.has_pending_admission() {
                        break;
                    }
                    continue;
                }
                selected = Some((*id, source));
                break;
            }
            let Some((id, source)) = selected else { continue };
            let source_etag = source
                .as_ref()
                .and_then(|url| self.state.sources[&source_base(url).ok()?].etags.get(&id))
                .cloned();
            let part = self.state.partitions.get_mut(&id).expect("selected partition");
            let best_etag = source_etag
                .filter(|etag| part.etags.get(etag).is_some_and(|state| state.download.is_some()))
                .or_else(|| {
                    part.etags
                        .iter()
                        .filter_map(|(etag, state)| {
                            state
                                .download
                                .as_ref()
                                .map(|download| (download.verified_chunks, etag.clone()))
                        })
                        .max_by_key(|(chunks, _)| *chunks)
                        .map(|(_, etag)| etag)
                });
            let download = best_etag
                .as_ref()
                .and_then(|etag| part.etags.get_mut(etag).and_then(|state| state.download.take()))
                .or_else(|| part.initial.take());
            let retained_bytes = part
                .etags
                .values()
                .filter_map(|etag| etag.download.as_ref())
                .map(PartitionDownloadState::bytes)
                .sum();
            let previous_chunks = download.as_ref().map_or(0, |state| state.verified_chunks);
            let had_map = download.as_ref().is_some_and(PartitionDownloadState::has_verified_map);
            let expected_etag = download
                .as_ref()
                .filter(|state| !state.etag.is_empty())
                .map(|state| state.etag.clone());
            if let Some(source) = &source {
                *part.attempts.entry(source.clone()).or_default() += 1;
            }
            let task_id = self.state.next_task_id;
            self.state.next_task_id += 1;
            let (cancel, cancellation) = watch::channel(false);
            part.active_task = Some(task_id);
            self.state.slots[slot] = Some(ActiveTask {
                task_id,
                blob_id: id,
                source: source.clone(),
                previous_chunks,
                had_map,
                expected_etag,
                cancel,
            });
            worker
                .send(PartitionDownloaderCommand::Download {
                    task_id,
                    blob_id: id,
                    source,
                    state: download,
                    retained_bytes,
                    cancel: cancellation,
                    admission: part.admission.take(),
                    reply: replies.clone(),
                })
                .await
                .map_err(|_| anyhow::anyhow!("snapshot worker mailbox closed"))?;
        }
        Ok(())
    }

    async fn finish_attempt(&mut self, response: PartitionDownloadResponse) -> anyhow::Result<()> {
        let slot = self
            .state
            .slots
            .iter()
            .position(|slot| slot.as_ref().is_some_and(|task| task.task_id == response.task_id))
            .ok_or_else(|| anyhow::anyhow!("unknown snapshot download task"))?;
        // Release the worker before notifying the importer. A ready file holds
        // its disk lease but never a network slot.
        let task = self.state.slots[slot].take().expect("active task");
        let id = task.blob_id;
        self.state.partitions.get_mut(&id).expect("partition").active_task = None;
        let mut download = response.state;
        let observed = match &response.result {
            Err(TransferError::EtagChanged(etag)) => Some(etag.clone()),
            _ => response.source_etag,
        };
        let mut ban = None;
        if let Some(url) = &task.source {
            let base = source_base(url)?;
            let provider = self.state.sources.get_mut(&base).expect("provider");
            provider.bytes_received += response.bytes_received;
            provider.elapsed += response.elapsed;
            let previous = provider.etags.get(&id).cloned();
            if let Some(etag) = &observed {
                if previous.as_ref().is_some_and(|old| old != etag) {
                    ban = Some((base.clone(), "source changed ETag".to_owned()));
                }
                provider.etags.insert(id, etag.clone());
            }
            match &response.result {
                Err(TransferError::Invalid(error)) => {
                    ban = Some((base, format!("invalid chunk envelope: {error:#}")))
                }
                Err(TransferError::EtagChanged(_)) if task.expected_etag.is_none() => {
                    ban = Some((base, "source changed ETag within an attempt".into()))
                }
                Err(TransferError::Network(_)) => {
                    provider.penalty += 1;
                    provider.failures += 1;
                }
                _ => {}
            }
        }
        let completed = response.result.is_ok()
            && task.source.as_ref().is_none_or(|url| {
                source_base(url).is_ok_and(|base| self.state.sources[&base].banned.is_none())
            });
        let part = self.state.partitions.get_mut(&id).expect("partition");
        part.cache_checked = true;
        if let Some(state) = download.take() {
            if state.etag.is_empty() {
                part.initial = Some(state);
            } else {
                let etag = part.etags.entry(state.etag.clone()).or_default();
                if let Some(source) = &task.source {
                    let provider = source_base(source)?;
                    if (!task.had_map && state.has_verified_map())
                        || state.verified_chunks > task.previous_chunks
                    {
                        etag.sources.insert(provider.clone());
                    }
                    // One terminal report records the whole newly verified range.
                    etag.chunk_sources.resize(state.verified_chunks, provider);
                    debug_assert!(state.verified_chunks >= task.previous_chunks);
                }
                etag.bytes_received += response.bytes_received;
                etag.elapsed += response.elapsed;
                if response.result.is_err()
                    && !matches!(response.result, Err(TransferError::NotPublished))
                {
                    etag.failures += 1;
                }
                etag.download = Some(state);
            }
        }
        match response.result {
            Ok(()) => {}
            Err(TransferError::Local(error)) => return Err(error),
            Err(TransferError::NotPublished) => {
                let url = task.source.as_ref().expect("HTTP 404 has a source");
                // Do not exhaust the transport-error budget while an honest
                // publisher is still building this blob. Cool down this pair
                // only; the snapshot deadline still bounds total waiting.
                *part.attempts.get_mut(url).expect("attempt") -= 1;
                let (misses, until) =
                    part.not_published.entry(url.clone()).or_insert((0, Instant::now()));
                let delay = self
                    .limits
                    .retry_delay
                    .max(Duration::from_millis(1))
                    .saturating_mul(1 << (*misses).min(4))
                    .min(Duration::from_secs(15));
                *misses = misses.saturating_add(1);
                *until = Instant::now() + delay;
            }
            Err(TransferError::Network(error)) => {
                tracing::debug!(source = ?task.source, blob = %hex::encode(id), "retrying snapshot transfer: {error:#}");
                self.last_failure = Some(error);
                part.retry_after = Instant::now() + self.limits.retry_delay;
            }
            Err(TransferError::Capacity) => {
                if let Some(url) = &task.source {
                    *part.attempts.get_mut(url).expect("attempt") -= 1;
                }
                part.retry_after = Instant::now() + Duration::from_millis(25);
                // Keep each logical blob's admitted reservation: releasing it
                // could allow later-phase files to starve an earlier resume.
                // Only alternate representations of THIS blob are expendable.
                for (etag, representation) in &mut part.etags {
                    if Some(etag.as_str()) != observed.as_deref() {
                        if let Some(mut state) = representation.download.take() {
                            if let Some(admission) = state.take_pending_admission() {
                                part.admission = Some(admission);
                            }
                        }
                    }
                }
            }
            Err(TransferError::EtagChanged(observed)) => {
                // Another provider may legitimately encode the same logical
                // partition differently. Start its representation separately;
                // only a provider changing its OWN prior ETag is a ban.
                let part = self.state.partitions.get_mut(&id).expect("partition");
                if ban.is_none() {
                    let alternative = part
                        .etags
                        .values_mut()
                        .find_map(|entry| entry.download.as_mut())
                        .map(PartitionDownloadState::new_representation);
                    if let Some(mut alternative) = alternative {
                        alternative.etag = observed.clone();
                        part.etags.entry(observed).or_default().download = Some(alternative);
                    }
                    if let Some(url) = &task.source {
                        *part.attempts.get_mut(url).expect("attempt") -= 1;
                    }
                }
            }
            Err(TransferError::Invalid(error)) => self.last_failure = Some(error),
            Err(TransferError::Cancelled | TransferError::CacheMiss) => {}
        }
        if let Some((source, reason)) = ban {
            self.ban(&source, reason)?;
        }
        self.discard_banned_partials();
        self.resize_reservations()?;
        if completed {
            let part = self.state.partitions.get_mut(&id).expect("partition");
            let etag = part
                .etags
                .iter()
                .find_map(|(etag, entry)| {
                    entry.download.as_ref().filter(|state| state.complete).map(|_| etag.clone())
                })
                .ok_or_else(|| anyhow::anyhow!("completed transfer has no verified file"))?;
            if let Some(index) = part.manifest_index {
                let path = part.etags[&etag].download.as_ref().expect("file").path();
                part.ready = true;
                let manifest = &self.state.manifest;
                let event = if let Some(root) = manifest.account_partition_roots.get(index) {
                    ThreadSnapshotDownloaderEvent::AccountsReady { root: *root, path }
                } else {
                    let index = index - manifest.account_partition_roots.len();
                    if let Some(root) = manifest.normal_message_partition_roots.get(index) {
                        ThreadSnapshotDownloaderEvent::NormalMessagesReady { root: *root, path }
                    } else {
                        let index = index - manifest.normal_message_partition_roots.len();
                        ThreadSnapshotDownloaderEvent::HighPriorityMessagesReady {
                            root: manifest.high_priority_message_partition_roots[index],
                            path,
                        }
                    }
                };
                self.events
                    .as_ref()
                    .expect("started")
                    .send(event)
                    .map_err(|_| anyhow::anyhow!("snapshot importer stopped"))?;
            } else {
                let mut state = part
                    .etags
                    .get_mut(&etag)
                    .expect("primary representation")
                    .download
                    .take()
                    .expect("file");
                let limits = self.import_limits;
                let cancel = self.cancellation.clone();
                let (state, primary) = tokio::task::spawn_blocking(move || {
                    let primary =
                        state.file_mut().rewind().map_err(anyhow::Error::from).and_then(|_| {
                            decode_blob_with_transport(
                                state.file_mut(),
                                &|| *cancel.borrow(),
                                true,
                                |reader| {
                                    read_primary_snapshot_checked(
                                        reader,
                                        limits.roots_per_list,
                                        &|| *cancel.borrow(),
                                    )
                                },
                            )
                        });
                    (state, primary)
                })
                .await?;
                self.state
                    .partitions
                    .get_mut(&id)
                    .expect("primary")
                    .etags
                    .entry(etag)
                    .or_default()
                    .download = Some(state);
                self.deliver_primary(primary?)?;
            }
        }
        Ok(())
    }

    fn ban(&mut self, source: &Url, reason: String) -> anyhow::Result<()> {
        tracing::warn!(%source, %reason, "snapshot source banned");
        self.state.sources.get_mut(source).expect("provider").banned = Some(reason);
        for slot in self.state.slots.iter().flatten() {
            if slot
                .source
                .as_ref()
                .is_some_and(|url| source_base(url).as_ref().is_ok_and(|base| base == source))
            {
                let _ = slot.cancel.send(true);
            }
        }
        self.discard_banned_partials();
        Ok(())
    }

    /// A cancelled task can reply after its provider was banned elsewhere.
    /// Recheck ownership on EVERY reply, not only at the instant of the ban.
    fn discard_banned_partials(&mut self) {
        for part in self.state.partitions.values_mut() {
            if part.ready || part.active_task.is_some() {
                continue;
            }
            for etag in part.etags.values_mut() {
                if !etag.sources.is_empty()
                    && etag.sources.iter().all(|source| self.state.sources[source].banned.is_some())
                {
                    if let Some(mut state) = etag.download.take() {
                        if let Some(admission) = state.take_pending_admission() {
                            part.admission = Some(admission);
                        }
                    }
                }
            }
        }
    }

    fn resize_reservations(&self) -> anyhow::Result<()> {
        for part in self.state.partitions.values() {
            if part.active_task.is_some() {
                continue;
            }
            let states: Vec<_> = part
                .etags
                .values()
                .filter_map(|entry| entry.download.as_ref())
                .chain(part.initial.as_ref())
                .collect();
            if let Some(state) = states.first() {
                state.resize_reservation(states.iter().map(|state| state.bytes()).sum())?;
            }
        }
        Ok(())
    }

    async fn process_blob(&mut self, blob_id: [u8; 32], accepted: bool) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state.partitions.get(&blob_id).is_some_and(|part| part.ready && !part.accepted),
            "snapshot blob is not awaiting import"
        );
        if accepted {
            self.accept_blob(blob_id).await
        } else {
            let part = self
                .state
                .partitions
                .get_mut(&blob_id)
                .ok_or_else(|| anyhow::anyhow!("unknown imported partition"))?;
            let providers: BTreeSet<_> = part
                .etags
                .values()
                .filter(|etag| etag.download.as_ref().is_some_and(|download| download.complete))
                .flat_map(|etag| etag.sources.iter().cloned())
                .collect();
            for etag in part.etags.values_mut() {
                if let Some(download) = etag.download.take() {
                    download.reject()?;
                }
            }
            for provider in providers {
                self.ban(&provider, "snapshot blob failed logical validation".into())?;
            }
            Ok(())
        }
    }

    async fn accept_blob(&mut self, id: [u8; 32]) -> anyhow::Result<()> {
        let part = self.state.partitions.get_mut(&id).expect("accepted partition");
        let mut ready = None;
        for etag in part.etags.values_mut() {
            if let Some(state) = etag.download.take() {
                if state.complete {
                    ready = Some(state);
                }
            }
        }
        part.initial.take();
        if let Some(ready) = ready {
            tokio::task::spawn_blocking(move || ready.accept()).await??;
        }
        part.accepted = true;
        Ok(())
    }
}

// The orchestrator's exclusively owned bookkeeping; workers never borrow it.
//
// ETags identify wire representations, not logical partitions. Several ETags
// can therefore coexist for one manifest root. Chunk ownership is recorded only
// for verified chunks, using the interval advanced by a completed attempt.
// Metadata survives discarding an unusable partial representation. Source bans
// are snapshot-wide; an ordinary network failure only lowers source priority.
#[derive(Default)]
struct SourceState {
    penalty: u64,
    banned: Option<String>,
    bytes_received: u64,
    elapsed: Duration,
    failures: u64,
    etags: BTreeMap<[u8; 32], String>,
}

#[derive(Default)]
struct EtagState {
    download: Option<PartitionDownloadState>,
    chunk_sources: Vec<Url>,
    /// Includes the map supplier even when an attempt verified no payload chunk.
    sources: BTreeSet<Url>,
    bytes_received: u64,
    elapsed: Duration,
    failures: u64,
}

struct PartitionState {
    /// None denotes primary; otherwise position in the flattened manifest.
    manifest_index: Option<usize>,
    admission: Option<DownloadAdmission>,
    /// Explicit primary sources only. Partition preferences are ranked lazily
    /// from their manifest index and the frozen source plan, not stored P*N times.
    sources: Vec<Url>,
    etags: BTreeMap<String, EtagState>,
    initial: Option<PartitionDownloadState>,
    attempts: BTreeMap<Url, u32>,
    /// HTTP 404 backoff is per blob/endpoint, not a snapshot-wide source penalty.
    not_published: BTreeMap<Url, (u32, Instant)>,
    cache_checked: bool,
    ready: bool,
    accepted: bool,
    active_task: Option<u64>,
    retry_after: std::time::Instant,
}

impl PartitionState {
    fn has_pending_admission(&self) -> bool {
        self.admission.is_some()
            || self.initial.as_ref().is_some_and(PartitionDownloadState::has_pending_admission)
            || self.etags.values().any(|representation| {
                representation
                    .download
                    .as_ref()
                    .is_some_and(PartitionDownloadState::has_pending_admission)
            })
    }

    fn new(manifest_index: Option<usize>, sources: Vec<Url>) -> Self {
        Self {
            manifest_index,
            admission: None,
            sources,
            etags: BTreeMap::new(),
            initial: None,
            attempts: BTreeMap::new(),
            not_published: BTreeMap::new(),
            cache_checked: false,
            ready: false,
            accepted: false,
            active_task: None,
            retry_after: std::time::Instant::now(),
        }
    }
}

struct ActiveTask {
    task_id: u64,
    blob_id: [u8; 32],
    source: Option<Url>,
    previous_chunks: usize,
    had_map: bool,
    expected_etag: Option<String>,
    cancel: watch::Sender<bool>,
}

/// One instance per thread-snapshot attempt. No Arc<Mutex<_>> around this state:
/// the orchestrator processes commands/replies serially and transfers owned
/// per-representation state to workers through their command channels.
struct SnapshotDownloadState {
    manifest: ThreadSnapshotManifest,
    sources: BTreeMap<Url, SourceState>,
    partitions: BTreeMap<[u8; 32], PartitionState>,
    order: Vec<[u8; 32]>,
    slots: Vec<Option<ActiveTask>>,
    next_task_id: u64,
    next_admission: Option<oneshot::Receiver<()>>,
}

impl SnapshotDownloadState {
    fn new(slots: usize) -> Self {
        Self {
            manifest: ThreadSnapshotManifest::default(),
            sources: BTreeMap::new(),
            partitions: BTreeMap::new(),
            order: Vec::new(),
            slots: (0..slots).map(|_| None).collect(),
            next_task_id: 0,
            next_admission: None,
        }
    }

    fn add(
        &mut self,
        id: [u8; 32],
        manifest_index: Option<usize>,
        sources: Vec<Url>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(!self.partitions.contains_key(&id), "duplicate snapshot blob");
        for source in &sources {
            self.sources.entry(source_base(source)?).or_default();
        }
        let (admission, following) = DownloadAdmission::next(self.next_admission.take());
        self.next_admission = Some(following);
        let mut part = PartitionState::new(manifest_index, sources);
        part.admission = Some(admission);
        self.partitions.insert(id, part);
        self.order.push(id);
        Ok(())
    }
}

/// A resource URL's directory identifies its provider across all blob IDs.
fn source_base(source: &Url) -> anyhow::Result<Url> {
    let nested = source.path().rsplit_once('/').is_some_and(|(parent, _)| {
        parent
            .rsplit('/')
            .next()
            .and_then(|name| name.strip_suffix(".partitions"))
            .is_some_and(snapshot_storage::valid_id)
    });
    Ok(source.join(if nested { "../" } else { "./" })?)
}

#[cfg(test)]
#[path = "thread_snapshot_downloader_tests.rs"]
mod tests;
