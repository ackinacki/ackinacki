//! Logical import actor. Only transport-verified primary/partition readiness
//! reaches this mailbox. The archive and semantic validation belong exclusively
//! to the importer; no HTTP worker can call an import callback.
use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use super::super::snapshot_consumption::stage_verified_snapshot;
use super::super::snapshot_consumption::SnapshotPartitionSource;
use super::super::snapshot_consumption::StagedPartitionedSnapshot;
use super::super::snapshot_http_import::SnapshotHttpImport;
use super::super::snapshot_partitioned::PrimaryThreadSnapshot;
use super::thread_snapshot_downloader::ThreadSnapshotDownloader;
use super::thread_snapshot_downloader::ThreadSnapshotDownloaderCommand;
use super::thread_snapshot_downloader::ThreadSnapshotDownloaderEvent;

/// Compatibility boundary for the existing synchronous snapshot-service
/// callback. The importer runs as the root Tokio future, the coordinator and
/// HTTP workers as spawned tasks. Only VM decoding/staged database writes run
/// in spawn_blocking, never on a Tokio scheduler thread.
///
/// The runtime's format dispatcher may supply an already decoded primary: it
/// must distinguish legacy streams before invoking this partitioned pipeline.
/// That seed is still handed through PrimaryReady and validated exactly once;
/// the cold download path instead fetches/decodes primary in the coordinator.
pub(crate) fn import_snapshot<'a, C, V>(
    request: SnapshotHttpImport<'a>,
    primary: Option<PrimaryThreadSnapshot>,
    primary_sources: Vec<url::Url>,
    mut verify_primary: V,
    cancel: &C,
) -> anyhow::Result<StagedPartitionedSnapshot<'a>>
where
    C: Fn() -> bool + Sync,
    V: FnMut(&PrimaryThreadSnapshot) -> anyhow::Result<()>,
{
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("snapshot-async")
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let (cancel_tx, cancellation) = watch::channel(cancel());
        let (done, mut finished) = watch::channel(false);
        // Even unwinding stops network awaits; do not rely on a Stop command
        // queued behind work to interrupt an in-flight transfer.
        let _cancel_on_drop = CancelOnDrop(cancel_tx.clone());
        let monitor = async {
            loop {
                tokio::select! {
                    _ = finished.changed() => break,
                    _ = tokio::time::sleep(Duration::from_millis(25)) => {
                        if cancel() { let _ = cancel_tx.send(true); break; }
                    }
                }
            }
        };
        let import = async {
            let _finish = CancelOnDrop(done.clone());
            let (commands, incoming) = mpsc::channel(2);
            let (notifications, ready) = mpsc::unbounded_channel();
            let (reply, started) = oneshot::channel();
            let downloader = ThreadSnapshotDownloader::new(
                &request,
                primary,
                primary_sources,
                cancel_tx.clone(),
            );
            let coordinator = tokio::spawn(downloader.run(incoming));
            commands
                .send(ThreadSnapshotDownloaderCommand::Start {
                    thread_id: request.expected.0,
                    block_id: request.expected.1,
                    known_sources: request.known_sources.to_vec(),
                    fallback_sources: request.fallback_sources.to_vec(),
                    events: notifications,
                    reply,
                })
                .await
                .map_err(|_| anyhow::anyhow!("snapshot downloader did not start"))?;
            let result = async {
                started.await?;
                let mut importer = ThreadSnapshotImporter {
                    incoming: ready,
                    downloader: commands.clone(),
                    cancellation: cancellation.clone(),
                    ready: BTreeMap::new(),
                    handle: tokio::runtime::Handle::current(),
                };
                let primary_event = importer.receive().await?;
                let ThreadSnapshotDownloaderEvent::PrimaryReady(primary) = primary_event else {
                    anyhow::bail!("partition arrived before snapshot primary");
                };
                let verified = super::super::snapshot_consumption::check_primary_identity(
                    &primary,
                    request.expected,
                )
                .and_then(|_| verify_primary(&primary));
                importer.processed(*request.expected.1.as_array(), verified.is_ok()).await?;
                verified?;
                let repository = request.repository.clone();
                let limits = request.import_limits;
                let staged = tokio::task::spawn_blocking(move || {
                    let cancelled = importer.cancellation.clone();
                    stage_verified_snapshot(primary, &repository, limits, &mut importer, &|| {
                        *cancelled.borrow()
                    })
                    .map(StagedPartitionedSnapshot::into_owned)
                })
                .await??;
                Ok(staged.with_repository(request.repository))
            }
            .await;
            let (reply, stopped) = oneshot::channel();
            let _ = commands.send(ThreadSnapshotDownloaderCommand::Stop { reply }).await;
            let _ = stopped.await;
            let stopped = coordinator.await?;
            let _ = done.send(true);
            match (result, stopped) {
                (Err(_), Err(error)) => Err(error),
                (result, _) => result,
            }
        };
        let (_, result) = tokio::join!(monitor, import);
        complete_import(result, cancel)
    })
}

/// The watch channel is only a periodically refreshed view of the caller's
/// cancellation flag. Import and actor shutdown can finish between two polls,
/// stopping the monitor before it observes cancellation. Recheck the original
/// source at the successful handoff boundary, after all actors have joined.
/// Returning an error here drops the still-private staged epoch and aborts both
/// archive copies; no cancelled snapshot escapes to the installation workflow.
fn complete_import<'a>(
    result: anyhow::Result<StagedPartitionedSnapshot<'a>>,
    cancel: &impl Fn() -> bool,
) -> anyhow::Result<StagedPartitionedSnapshot<'a>> {
    let staged = result?;
    anyhow::ensure!(!cancel(), "snapshot import cancelled");
    Ok(staged)
}

#[cfg(test)]
#[path = "thread_snapshot_importer_tests.rs"]
mod tests;

struct CancelOnDrop(watch::Sender<bool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        let _ = self.0.send(true);
    }
}

/// This actor's owned mailbox moves into spawn_blocking for the staged import.
/// It buffers paths for future phases, never decoded messages or account bodies.
struct ThreadSnapshotImporter {
    incoming: mpsc::UnboundedReceiver<ThreadSnapshotDownloaderEvent>,
    cancellation: watch::Receiver<bool>,
    ready: BTreeMap<[u8; 32], PathBuf>,
    downloader: mpsc::Sender<ThreadSnapshotDownloaderCommand>,
    handle: tokio::runtime::Handle,
}

impl ThreadSnapshotImporter {
    /// Cache lifetime is an explicit command, not a reply attached to a readiness
    /// event. Await its completion after closing the reader so eviction is safe.
    async fn processed(&self, blob_id: [u8; 32], accepted: bool) -> anyhow::Result<()> {
        let (reply, completed) = oneshot::channel();
        self.downloader
            .send(ThreadSnapshotDownloaderCommand::BlobProcessed { blob_id, accepted, reply })
            .await
            .map_err(|_| anyhow::anyhow!("snapshot downloader stopped before cache handoff"))?;
        completed.await?.result
    }

    async fn receive(&mut self) -> anyhow::Result<ThreadSnapshotDownloaderEvent> {
        anyhow::ensure!(!*self.cancellation.borrow(), "snapshot import cancelled");
        tokio::select! {
            biased;
            _ = self.cancellation.changed() => anyhow::bail!("snapshot import cancelled"),
            command = self.incoming.recv() => command.ok_or_else(|| anyhow::anyhow!("snapshot downloader stopped before import completed")),
        }
    }
}

impl SnapshotPartitionSource for &mut ThreadSnapshotImporter {
    fn chunks_verified(&self) -> bool {
        // Readiness is emitted only after the downloader verified all chunks
        // (or reverified a cache hit). The cache lease keeps the file immutable
        // and retained until this importer acknowledges semantic validation.
        true
    }

    fn read_partition<T>(
        &mut self,
        id: [u8; 32],
        read: impl FnOnce(&mut dyn Read) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        self.read_any_partition(&BTreeMap::from([(id, 0)]), |_, file| read(file))
            .map(|(_, value)| value)
    }

    fn read_any_partition<T>(
        &mut self,
        pending: &BTreeMap<[u8; 32], usize>,
        read: impl FnOnce(usize, &mut dyn Read) -> anyhow::Result<T>,
    ) -> anyhow::Result<([u8; 32], T)> {
        anyhow::ensure!(!pending.is_empty(), "no snapshot partitions requested");
        loop {
            anyhow::ensure!(!*self.cancellation.borrow(), "snapshot import cancelled");
            if let Some(id) = self.ready.keys().find(|id| pending.contains_key(*id)).copied() {
                let path = self.ready.remove(&id).expect("ready partition");
                let result = (|| {
                    let mut file = std::fs::File::open(path)?;
                    let result = read(pending[&id], &mut file)?;
                    anyhow::ensure!(!*self.cancellation.borrow(), "snapshot import cancelled");
                    Ok(result)
                })();
                // File is closed before acknowledgement, so immediate GC after
                // acceptance is safe. Cancellation is not a provider violation.
                if !*self.cancellation.borrow() {
                    self.handle.block_on(self.processed(id, result.is_ok()))?;
                }
                return result.map(|value| (id, value));
            }
            let handle = self.handle.clone();
            let (id, path) = match handle.block_on(self.receive())? {
                ThreadSnapshotDownloaderEvent::AccountsReady { root, path } =>
                    (super::super::snapshot_manifest::account_partition_blob_id(&root), path),
                ThreadSnapshotDownloaderEvent::NormalMessagesReady { root, path } =>
                    (super::super::snapshot_manifest::message_partition_blob_id(crate::types::thread_message_queue::partition_codec::MessageQueueKind::Normal, &root), path),
                ThreadSnapshotDownloaderEvent::HighPriorityMessagesReady { root, path } =>
                    (super::super::snapshot_manifest::message_partition_blob_id(crate::types::thread_message_queue::partition_codec::MessageQueueKind::HighPriority, &root), path),
                ThreadSnapshotDownloaderEvent::PrimaryReady(_) => anyhow::bail!("duplicate primary readiness notification"),
            };
            anyhow::ensure!(
                self.ready.insert(id, path).is_none(),
                "duplicate partition readiness notification"
            );
        }
    }
}
