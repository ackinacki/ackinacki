//! A single-source, single-attempt transport actor.
//!
//! The caller owns scheduling and trust decisions. This worker never retries,
//! switches providers or imports records. It returns its resume state even on
//! failure: only hash-verified whole chunks advance that state. HTTP awaits are
//! interruptible; a Stop command cannot otherwise interrupt an in-flight body.
use std::fs::File;
use std::future::Future;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use reqwest::header::ACCEPT_ENCODING;
use reqwest::header::CONTENT_ENCODING;
use reqwest::header::CONTENT_RANGE;
use reqwest::header::ETAG;
use reqwest::header::IF_RANGE;
use reqwest::header::RANGE;
use snapshot_storage::chunks::ChunkMapMetadata;
use snapshot_storage::chunks::CHUNK_BYTES;
use snapshot_storage::chunks::FOOTER_BYTES;
use snapshot_storage::chunks::MAGIC;
use snapshot_storage::chunks::{self};
use tempfile::NamedTempFile;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;

use crate::services::blob_sync::external_fileshares_based::download_cache::CacheLease;
use crate::services::blob_sync::external_fileshares_based::download_cache::DownloadCache;
use crate::services::blob_sync::external_fileshares_based::download_cache::TEMP_PREFIX;

#[derive(Clone, Copy)]
pub(crate) struct BlobDownloadLimits {
    pub rounds: u8,
    pub connect_timeout: Duration,
    /// Time to receive response headers, not a limit on downloading the body.
    pub request_timeout: Duration,
    pub retry_delay: Duration,
    pub deadline: Instant,
}

/// A sustained low rate returns a retryable failure, never a trust violation.
/// A generous window tolerates both network jitter and a busy serving node.
const RATE_WINDOW: Duration = Duration::from_secs(10);
/// Independent of the rate window: tolerate a temporary pause without imposing
/// a total duration on a large, steadily progressing partition transfer.
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const MIN_BYTES_PER_SECOND: u64 = 64 * 1024;

pub(super) enum PartitionDownloaderCommand {
    Download {
        task_id: u64,
        blob_id: [u8; 32],
        /// None probes the verified local cache without selecting a provider.
        source: Option<url::Url>,
        state: Option<PartitionDownloadState>,
        /// Other wire representations share the same logical cache reservation.
        retained_bytes: u64,
        cancel: watch::Receiver<bool>,
        admission: Option<DownloadAdmission>,
        reply: mpsc::Sender<PartitionDownloadResponse>,
    },
}

/// One terminal response per attempt, even on failure. All four workers reply
/// to the same coordinator mailbox; the task ID identifies the active slot.
pub(super) struct PartitionDownloadResponse {
    pub task_id: u64,
    pub state: Option<PartitionDownloadState>,
    pub result: Result<(), TransferError>,
    pub bytes_received: u64,
    pub elapsed: Duration,
    /// Observed at this source, never inferred from a saved representation.
    pub source_etag: Option<String>,
}

/// Worker-local mutable attempt data, never a second public command protocol.
struct DownloadTask {
    blob_id: [u8; 32],
    source: Option<url::Url>,
    state: Option<PartitionDownloadState>,
    retained_bytes: u64,
    cancel: watch::Receiver<bool>,
    admission: Option<DownloadAdmission>,
    source_etag: Option<String>,
}

/// Admission is a one-way chain of disk reservations, NOT transfer/import
/// completion. Once a blob reserves its bytes and validates its map, the next
/// reservation may proceed while this payload is still downloading. This
/// prevents ready later-phase files from filling the budget ahead of accounts.
/// The token moves with resume state across failed attempts before admission.
pub(super) struct DownloadAdmission {
    preceding: Option<oneshot::Receiver<()>>,
    following: Option<oneshot::Sender<()>>,
}

impl DownloadAdmission {
    pub fn next(preceding: Option<oneshot::Receiver<()>>) -> (Self, oneshot::Receiver<()>) {
        let (following, next) = oneshot::channel();
        (Self { preceding, following: Some(following) }, next)
    }

    pub fn admitted(&mut self) {
        if let Some(following) = self.following.take() {
            let _ = following.send(());
        }
    }
}

#[derive(Debug)]
pub(super) enum TransferError {
    CacheMiss,
    /// Publication is asynchronous; HTTP 404 is not a failed provider.
    NotPublished,
    Capacity,
    Cancelled,
    Network(anyhow::Error),
    Local(anyhow::Error),
    Invalid(anyhow::Error),
    EtagChanged(String),
}

enum DownloadFile {
    Cached(File),
    Private(NamedTempFile),
}

/// Ownership moves between the coordinator and exactly one worker. Different
/// ETags never share a file, but share the logical blob's exclusive cache lease.
/// The private file is sparse while incomplete; its entire length is reserved
/// against the cache budget, and it is never exposed before all chunks verify.
pub(super) struct PartitionDownloadState {
    file: Option<DownloadFile>,
    pub etag: String,
    map: Option<ChunkMapMetadata>,
    hashes: Vec<[u8; 32]>,
    pub verified_chunks: usize,
    pub complete: bool,
    reserved_bytes: u64,
    admission: Option<DownloadAdmission>,
    // File must be dropped before the last lease releases its disk reservation.
    lease: Arc<CacheLease>,
}

impl PartitionDownloadState {
    fn empty(lease: Arc<CacheLease>) -> Self {
        Self {
            file: None,
            etag: String::new(),
            map: None,
            hashes: Vec::new(),
            verified_chunks: 0,
            complete: false,
            reserved_bytes: 0,
            admission: None,
            lease,
        }
    }

    pub fn alternative(&self) -> Self {
        Self::empty(self.lease.clone())
    }

    pub fn new_representation(&mut self) -> Self {
        let mut next = self.alternative();
        next.admission = self.take_pending_admission();
        next
    }

    pub fn has_verified_map(&self) -> bool {
        self.map.is_some()
    }

    pub fn has_pending_admission(&self) -> bool {
        self.admission.as_ref().is_some_and(|admission| admission.following.is_some())
    }

    pub fn resize_reservation(&self, bytes: u64) -> anyhow::Result<()> {
        anyhow::ensure!(self.lease.try_resize(bytes)?, "cannot shrink snapshot reservation");
        Ok(())
    }

    pub fn bytes(&self) -> u64 {
        self.reserved_bytes
    }

    /// Preserve an ungranted admission token when discarding an untrusted wire
    /// representation, otherwise the next worker could wait on a lost sender.
    pub fn take_pending_admission(&mut self) -> Option<DownloadAdmission> {
        if self.has_pending_admission() {
            self.admission.take()
        } else {
            None
        }
    }

    pub fn path(&self) -> PathBuf {
        match self.file.as_ref().expect("complete download has a file") {
            DownloadFile::Cached(_) => self.lease.path(),
            DownloadFile::Private(file) => file.path().to_owned(),
        }
    }

    pub fn file_mut(&mut self) -> &mut File {
        match self.file.as_mut().expect("download has a file") {
            DownloadFile::Cached(file) => file,
            DownloadFile::Private(file) => file.as_file_mut(),
        }
    }

    /// The importer has closed its reader before acknowledging. Persist only
    /// after logical validation, while the lease still excludes cache eviction.
    pub fn accept(mut self) -> anyhow::Result<()> {
        anyhow::ensure!(self.complete, "cannot accept an incomplete partition");
        if let Some(DownloadFile::Private(file)) = self.file.take() {
            file.as_file().sync_all()?;
            file.persist_noclobber(self.lease.path()).map_err(|error| error.error)?;
            File::open(self.lease.path().parent().expect("cache directory"))?.sync_all()?;
        }
        self.lease.accepted()
    }

    pub fn reject(self) -> anyhow::Result<()> {
        self.lease.invalidate()
    }
}

pub(super) struct PartitionDownloader {
    client: reqwest::Client,
    directory: PathBuf,
    limits: BlobDownloadLimits,
    pub activity_slot: String,
}

impl PartitionDownloader {
    pub fn new(directory: &Path, limits: BlobDownloadLimits) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(limits.connect_timeout);
        // Do not set Client::timeout: reqwest applies it to the entire body,
        // which would exhaust retries on large partitions from healthy peers.
        // Only acceptance builds may route peer requests through an isolated
        // source. Ordinary nodes must continue ignoring ambient HTTP proxies.
        #[cfg(feature = "test_upgrade")]
        let client = match std::env::var("TEST_SNAPSHOT_DOWNLOAD_PROXY") {
            Ok(url) => client.proxy(reqwest::Proxy::all(url)?),
            Err(_) => client,
        };
        let client = client.build()?;
        Ok(Self {
            client,
            directory: directory.to_owned(),
            limits,
            activity_slot: "standalone".into(),
        })
    }

    /// One command produces exactly one reply, including cancellation/failure.
    /// No callback can run the importer on this worker's thread.
    pub async fn run(&self, command: PartitionDownloaderCommand) {
        let PartitionDownloaderCommand::Download {
            task_id,
            blob_id,
            source,
            state,
            retained_bytes,
            cancel,
            admission,
            reply,
        } = command;
        let mut task = DownloadTask {
            blob_id,
            source,
            state,
            retained_bytes,
            cancel,
            admission,
            source_etag: None,
        };
        let started = Instant::now();
        let mut bytes = 0;
        let result = self.attempt(&mut task, &mut bytes).await;
        // A closed receiver means the coordinator cancelled this task. Dropping
        // the unsent response also drops its private file and cache reservation.
        let _ = reply
            .send(PartitionDownloadResponse {
                task_id,
                state: task.state,
                result,
                bytes_received: bytes,
                elapsed: started.elapsed(),
                source_etag: task.source_etag,
            })
            .await;
    }

    async fn attempt(
        &self,
        task: &mut DownloadTask,
        received: &mut u64,
    ) -> Result<(), TransferError> {
        if *task.cancel.borrow() || Instant::now() >= self.limits.deadline {
            return Err(TransferError::Cancelled);
        }
        let admission = task
            .state
            .as_mut()
            .and_then(|state| state.admission.as_mut())
            .or(task.admission.as_mut());
        if let Some(admission) = admission {
            if let Some(preceding) = admission.preceding.as_mut() {
                let _activity = crate::helper::metrics::ActivityMetric::parallel(
                    "snapshot_download",
                    "wait_disk_admission",
                    &self.activity_slot,
                )
                .enter();
                self.wait(&mut task.cancel, preceding).await?;
                admission.preceding = None;
            }
        }
        if task.state.is_none() {
            // Cache recovery can scan a large existing file or wait for a lease;
            // neither operation belongs on a Tokio scheduler thread.
            let directory = self.directory.clone();
            let id = task.blob_id;
            let limits = self.limits;
            let cancellation = task.cancel.clone();
            let mut state = tokio::task::spawn_blocking(move || {
                probe_cache(&directory, id, limits, cancellation)
            })
            .await
            .map_err(|error| TransferError::Local(error.into()))??;
            state.admission = task.admission.take();
            let complete = state.complete;
            if complete {
                if let Some(admission) = &mut state.admission {
                    admission.admitted();
                }
            }
            task.state = Some(state);
            if complete {
                return Ok(());
            }
        }
        let source = task.source.as_ref().ok_or(TransferError::CacheMiss)?;
        let state = task.state.as_mut().expect("initialized state");
        let cancel = &mut task.cancel;
        if state.map.is_none() {
            let response = self
                .headers(
                    cancel,
                    self.client.head(source.clone()).header(ACCEPT_ENCODING, "identity").send(),
                )
                .await?;
            if !response.status().is_success() {
                if response.status() == reqwest::StatusCode::NOT_FOUND {
                    return Err(TransferError::NotPublished);
                }
                return Err(TransferError::Network(anyhow::anyhow!(
                    "HEAD {}: {}",
                    source,
                    response.status()
                )));
            }
            let etag = strong_etag(&response).map_err(TransferError::Invalid)?;
            task.source_etag = Some(etag.clone());
            if !state.etag.is_empty() && state.etag != etag {
                return Err(TransferError::EtagChanged(etag));
            }
            state.etag = etag;
            let offset = integer_header(&response, chunks::MAP_OFFSET_HEADER)
                .map_err(TransferError::Invalid)?;
            let length = integer_header(&response, chunks::MAP_LENGTH_HEADER)
                .map_err(TransferError::Invalid)?;
            // HEAD's body is empty: Response::content_length() describes that
            // body, not necessarily the representation's Content-Length header.
            let total =
                integer_header(&response, "content-length").map_err(TransferError::Invalid)?;
            if offset <= 4
                || !(12..=(12 + 32 * chunks::MAX_CHUNKS) as u64).contains(&length)
                || offset.checked_add(length).and_then(|n| n.checked_add(FOOTER_BYTES as u64))
                    != Some(total)
            {
                return Err(TransferError::Invalid(anyhow::anyhow!("invalid chunk map bounds")));
            }
            let budget = state.lease.budget();
            if total > budget {
                return Err(TransferError::Local(anyhow::anyhow!(
                    "snapshot blob exceeds download cache budget"
                )));
            }
            // A single oversized representation cannot be downloaded. Several
            // individually valid ETags exceeding the budget together are only
            // disk pressure: Capacity lets the coordinator drop this blob's old
            // alternatives and retry without exhausting the healthy source.
            let reserved = total
                .checked_add(task.retained_bytes)
                .filter(|bytes| *bytes <= budget)
                .ok_or(TransferError::Capacity)?;
            if !state.lease.try_resize(reserved).map_err(TransferError::Local)? {
                return Err(TransferError::Capacity);
            }
            state.reserved_bytes = total;
            let mut response =
                self.range(source, &state.etag, offset, total - 1, total, cancel).await?;
            // Do not mark unavailable sources (HEAD/GET 404), cache probes or
            // admission waits as transfers. Only a validated HTTP range starts
            // activity; RAII ends it on completion, cancellation or body error.
            let _activity = crate::helper::metrics::ActivityMetric::parallel(
                "snapshot_download",
                "transfer",
                &self.activity_slot,
            )
            .enter();
            let mut trailer = Vec::with_capacity((length + FOOTER_BYTES as u64) as usize);
            while let Some(bytes) = self.next(&mut response, cancel).await? {
                let bytes = bytes.as_ref();
                *received += bytes.len() as u64;
                if trailer.len() + bytes.len() > length as usize + FOOTER_BYTES {
                    return Err(TransferError::Invalid(anyhow::anyhow!("oversized chunk map")));
                }
                trailer.extend_from_slice(bytes);
            }
            if trailer.len() != length as usize + FOOTER_BYTES {
                return Err(TransferError::Network(anyhow::anyhow!("truncated chunk map")));
            }
            let directory = self.directory.clone();
            let etag = state.etag.clone();
            let (file, map, hashes) = tokio::task::spawn_blocking(move || {
                let mut file = tempfile::Builder::new()
                    .prefix(TEMP_PREFIX)
                    .tempfile_in(&directory)
                    .map_err(|error| TransferError::Local(error.into()))?;
                file.as_file_mut()
                    .set_len(total)
                    .and_then(|_| file.write_all(MAGIC))
                    .and_then(|_| file.seek(SeekFrom::Start(offset)).map(|_| ()))
                    .and_then(|_| file.write_all(&trailer))
                    .map_err(|error| TransferError::Local(error.into()))?;
                let map = chunks::verify_map(&mut file).map_err(TransferError::Invalid)?;
                if map.offset != offset || map.length != length || map.etag() != etag {
                    return Err(TransferError::Invalid(anyhow::anyhow!(
                        "chunk map does not match HTTP metadata"
                    )));
                }
                let hashes = read_hashes(file.as_file_mut(), &map).map_err(TransferError::Local)?;
                Ok((file, map, hashes))
            })
            .await
            .map_err(|error| TransferError::Local(error.into()))??;
            state.map = Some(map);
            state.hashes = hashes;
            state.file = Some(DownloadFile::Private(file));
            if let Some(admission) = &mut state.admission {
                admission.admitted();
            }
        }
        let map = state.map.as_ref().expect("verified chunk map");
        let total = map.offset + map.length + FOOTER_BYTES as u64;
        let payload_end = map.offset;
        let start = 4 + (state.verified_chunks * CHUNK_BYTES) as u64;
        if start < payload_end {
            let mut response =
                self.range(source, &state.etag, start, payload_end - 1, total, cancel).await?;
            let _activity = crate::helper::metrics::ActivityMetric::parallel(
                "snapshot_download",
                "transfer",
                &self.activity_slot,
            )
            .enter();
            task.source_etag = Some(state.etag.clone());
            let mut chunk = Vec::with_capacity(CHUNK_BYTES);
            let mut window = Duration::ZERO;
            let mut window_bytes = 0u64;
            let mut position = start;
            loop {
                let awaiting = Instant::now();
                let next = self.next(&mut response, cancel).await?;
                window += awaiting.elapsed();
                let Some(bytes) = next else { break };
                let mut bytes = bytes.as_ref();
                *received += bytes.len() as u64;
                window_bytes += bytes.len() as u64;
                while !bytes.is_empty() {
                    if position >= payload_end {
                        return Err(TransferError::Invalid(anyhow::anyhow!(
                            "oversized chunk payload"
                        )));
                    }
                    let target = (payload_end - (4 + (state.verified_chunks * CHUNK_BYTES) as u64))
                        .min(CHUNK_BYTES as u64) as usize;
                    let take = (target - chunk.len()).min(bytes.len());
                    chunk.extend_from_slice(&bytes[..take]);
                    bytes = &bytes[take..];
                    position += take as u64;
                    if chunk.len() == target {
                        // Keep disk writes and chunk hashing off the reactor.
                        // The blocking job returns the state even on failure;
                        // only a successful hash AND write advance the prefix.
                        let replacement = state.alternative();
                        let mut owned = std::mem::replace(state, replacement);
                        let data = std::mem::take(&mut chunk);
                        let (owned, mut data, result) = tokio::task::spawn_blocking(move || {
                            let result = (|| {
                                if blake3::hash(&data).as_bytes()
                                    != &owned.hashes[owned.verified_chunks]
                                {
                                    return Err(TransferError::Invalid(anyhow::anyhow!(
                                        "snapshot chunk {} hash mismatch",
                                        owned.verified_chunks
                                    )));
                                }
                                let offset = 4 + (owned.verified_chunks * CHUNK_BYTES) as u64;
                                let file = owned.file_mut();
                                file.seek(SeekFrom::Start(offset))
                                    .and_then(|_| file.write_all(&data))
                                    .map_err(|error| TransferError::Local(error.into()))?;
                                owned.verified_chunks += 1;
                                Ok(())
                            })();
                            (owned, data, result)
                        })
                        .await
                        .map_err(|error| TransferError::Local(error.into()))?;
                        *state = owned;
                        result?;
                        data.clear();
                        chunk = data;
                    }
                }
                // Count network wait only: hashing or a busy local disk must
                // not make a fast, honest provider look slow.
                if window >= RATE_WINDOW {
                    if (window_bytes as f64) < MIN_BYTES_PER_SECOND as f64 * window.as_secs_f64() {
                        return Err(TransferError::Network(anyhow::anyhow!(
                            "snapshot source below minimum transfer rate"
                        )));
                    }
                    window = Duration::ZERO;
                    window_bytes = 0;
                }
            }
            if position != payload_end {
                return Err(TransferError::Network(anyhow::anyhow!("truncated chunk payload")));
            }
        }
        state.complete = state.verified_chunks == state.hashes.len();
        Ok(())
    }

    async fn range(
        &self,
        source: &url::Url,
        etag: &str,
        start: u64,
        end: u64,
        total: u64,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<reqwest::Response, TransferError> {
        let response = self
            .headers(
                cancel,
                self.client
                    .get(source.clone())
                    .header(ACCEPT_ENCODING, "identity")
                    .header(RANGE, format!("bytes={start}-{end}"))
                    .header(IF_RANGE, etag)
                    .send(),
            )
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(TransferError::NotPublished);
        }
        if response.status().is_client_error() || response.status().is_server_error() {
            return Err(TransferError::Network(anyhow::anyhow!(
                "GET {source}: {}",
                response.status()
            )));
        }
        let actual = strong_etag(&response).map_err(TransferError::Invalid)?;
        if actual != etag {
            return Err(TransferError::EtagChanged(actual));
        }
        let expected = format!("bytes {start}-{end}/{total}");
        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT
            || response.headers().get(CONTENT_RANGE).and_then(|v| v.to_str().ok())
                != Some(expected.as_str())
            || response.content_length() != Some(end - start + 1)
        {
            return Err(TransferError::Invalid(anyhow::anyhow!("invalid snapshot range response")));
        }
        Ok(response)
    }

    async fn next(
        &self,
        response: &mut reqwest::Response,
        cancel: &mut watch::Receiver<bool>,
    ) -> Result<Option<impl AsRef<[u8]>>, TransferError> {
        // Bound a stalled read too, not just a slow stream that keeps producing.
        self.wait(cancel, async {
            tokio::time::timeout(READ_IDLE_TIMEOUT, response.chunk())
                .await
                .map_err(|_| anyhow::anyhow!("snapshot source stalled"))?
                .map_err(anyhow::Error::from)
        })
        .await
    }

    async fn headers(
        &self,
        cancel: &mut watch::Receiver<bool>,
        request: impl Future<Output = Result<reqwest::Response, reqwest::Error>>,
    ) -> Result<reqwest::Response, TransferError> {
        // send() completes when headers arrive. Body reads have their own idle
        // and rate checks; cancellation and the snapshot deadline cover both.
        self.wait(cancel, async {
            tokio::time::timeout(self.limits.request_timeout, request)
                .await
                .map_err(|_| anyhow::anyhow!("snapshot response headers timed out"))?
                .map_err(anyhow::Error::from)
        })
        .await
    }

    async fn wait<T, E: Into<anyhow::Error>>(
        &self,
        cancel: &mut watch::Receiver<bool>,
        operation: impl Future<Output = Result<T, E>>,
    ) -> Result<T, TransferError> {
        if *cancel.borrow() || Instant::now() >= self.limits.deadline {
            return Err(TransferError::Cancelled);
        }
        tokio::select! {
            biased;
            _ = cancel.changed() => Err(TransferError::Cancelled),
            _ = tokio::time::sleep_until(self.limits.deadline.into()) => Err(TransferError::Cancelled),
            result = operation => result.map_err(|error| TransferError::Network(error.into())),
        }
    }
}

fn probe_cache(
    directory: &Path,
    id: [u8; 32],
    limits: BlobDownloadLimits,
    cancellation: watch::Receiver<bool>,
) -> Result<PartitionDownloadState, TransferError> {
    let cancel = || *cancellation.borrow() || Instant::now() >= limits.deadline;
    let cache = DownloadCache::open(directory, None).map_err(TransferError::Local)?;
    let lease = cache.acquire(&hex::encode(id), 0, &cancel).map_err(TransferError::Local)?;
    let mut state = PartitionDownloadState::empty(Arc::new(lease));
    match File::open(state.lease.path()) {
        Ok(mut file) => {
            let length = file.metadata().map_err(|e| TransferError::Local(e.into()))?.len();
            if let Ok(map) = chunks::verify_file(&mut file, &cancel) {
                state.etag = map.etag();
                state.hashes = read_hashes(&mut file, &map).map_err(TransferError::Local)?;
                state.verified_chunks = state.hashes.len();
                state.reserved_bytes = length;
                state.map = Some(map);
                state.file = Some(DownloadFile::Cached(file));
                state.complete = true;
                return Ok(state);
            }
            if cancel() {
                return Err(TransferError::Cancelled);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(TransferError::Local(error.into())),
    }
    state.lease.invalidate().map_err(TransferError::Local)?;
    state.lease.try_resize(0).map_err(TransferError::Local)?;
    Ok(state)
}

fn strong_etag(response: &reqwest::Response) -> anyhow::Result<String> {
    anyhow::ensure!(
        response.headers().get(CONTENT_ENCODING).is_none_or(|v| v == "identity"),
        "encoded snapshot response"
    );
    let etag = response
        .headers()
        .get(ETAG)
        .ok_or_else(|| anyhow::anyhow!("missing snapshot ETag"))?
        .to_str()?;
    anyhow::ensure!(
        etag.len() == 66
            && etag.starts_with('"')
            && etag.ends_with('"')
            && snapshot_storage::valid_id(&etag[1..65]),
        "invalid snapshot ETag"
    );
    Ok(etag.to_owned())
}

fn integer_header(response: &reqwest::Response, name: &str) -> anyhow::Result<u64> {
    Ok(response
        .headers()
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("missing {name}"))?
        .to_str()?
        .parse()?)
}

fn read_hashes(file: &mut File, map: &ChunkMapMetadata) -> anyhow::Result<Vec<[u8; 32]>> {
    file.seek(SeekFrom::Start(map.offset + 12))?;
    let mut hashes = vec![[0; 32]; (map.length as usize - 12) / 32];
    for hash in &mut hashes {
        file.read_exact(hash)?;
    }
    file.rewind()?;
    Ok(hashes)
}

#[cfg(test)]
#[path = "partition_downloader_tests.rs"]
mod tests;
