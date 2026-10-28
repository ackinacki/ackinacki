//! Previous blocking transport retained only as a compatibility test reference.
//! Production partition downloads use the Tokio PartitionDownloader actor;
//! the separate legacy blob service retains its existing download policy.
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use reqwest::blocking::Client;
use reqwest::header::ACCEPT_ENCODING;
use reqwest::header::CONTENT_ENCODING;
use reqwest::header::CONTENT_RANGE;
use reqwest::header::ETAG;
use reqwest::header::IF_RANGE;
use reqwest::header::RANGE;
use reqwest::StatusCode;
use tempfile::NamedTempFile;
use url::Url;

use super::download_cache::CacheLease;
use super::download_cache::DownloadCache;
use super::download_cache::TEMP_PREFIX;

#[derive(Clone, Copy)]
pub(crate) enum ResumePolicy {
    /// Fixed codec/compression profile: validated partitions are byte-identical.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Unframed transport exercised by HTTP tests")
    )]
    CanonicalPartition,
    /// Chunked primary/partition envelope; resume only complete payload chunks.
    ChunkedPartition,
    /// Primary proof bundles may differ. Resume only at the same URL with a
    /// strong ETag and If-Range; otherwise restart from byte zero.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Runtime primary downloads use the legacy transport")
    )]
    SourceBound,
}

/// Historical transport settings, kept only by this cfg(test) compatibility
/// reference. Production actors no longer impose a per-blob byte ceiling.
#[derive(Clone, Copy)]
pub(crate) struct BlobDownloadLimits {
    pub rounds: u8,
    pub max_bytes: u64,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub retry_delay: Duration,
    pub deadline: Instant,
}

pub(crate) struct VerifiedBlobDownloader {
    client: Client,
    limits: BlobDownloadLimits,
}

#[derive(Default)]
struct TransferProgress {
    validator: Option<String>,
    expected_size: Option<u64>,
    payload_end: Option<u64>,
}

enum PreparedFile {
    Cached(File),
    Downloaded(NamedTempFile),
}

/// Historical test-reference lifecycle: downloaded files remain private until
/// the validation callback succeeds and accept() persists them into the cache.
pub(crate) struct PreparedBlob {
    destination: PathBuf,
    file: PreparedFile,
    // Drop the file (especially a private temporary) before releasing its budget.
    lease: CacheLease,
}

impl PreparedBlob {
    pub(crate) fn accept(self) -> anyhow::Result<File> {
        let mut file = match self.file {
            PreparedFile::Cached(file) => file,
            PreparedFile::Downloaded(file) => {
                file.as_file().sync_all()?;
                let file =
                    file.persist_noclobber(&self.destination).map_err(|error| error.error)?;
                File::open(self.destination.parent().expect("cache directory"))?.sync_all()?;
                file
            }
        };
        self.lease.accepted()?;
        file.rewind()?;
        Ok(file)
    }
}

impl VerifiedBlobDownloader {
    pub(crate) fn reserve<C: Fn() -> bool>(
        &self,
        directory: &Path,
        id: &[u8; 32],
        sources: &[Url],
        policy: ResumePolicy,
        should_cancel: &C,
    ) -> anyhow::Result<(CacheLease, Option<File>)> {
        let lease = DownloadCache::open(directory, None)?.acquire(&hex::encode(id), 0, &|| {
            should_cancel() || Instant::now() >= self.limits.deadline
        })?;
        self.check(should_cancel)?;
        match File::open(lease.path()) {
            Ok(mut cached) => {
                let length = cached.metadata()?.len();
                if length > 0 && length <= self.limits.max_bytes {
                    let checked = if matches!(policy, ResumePolicy::ChunkedPartition) {
                        snapshot_storage::chunks::verify_file(&mut cached, &should_cancel)
                            .map(|_| ())
                    } else {
                        Ok(())
                    };
                    if checked.is_ok() {
                        self.check(should_cancel)?;
                        cached.rewind()?;
                        return Ok((lease, Some(cached)));
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        self.check(should_cancel)?;
        // This happens before prefetch advances its reservation cursor. The
        // caller cannot admit a later blob while a bad/missing earlier cache
        // entry is still waiting for its complete download reservation.
        lease.invalidate()?;
        lease.resize(0, &|| false)?;
        self.reserve_download(&lease, sources, should_cancel)?;
        Ok((lease, None))
    }

    fn reserve_download<C: Fn() -> bool>(
        &self,
        lease: &CacheLease,
        sources: &[Url],
        should_cancel: &C,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(!sources.is_empty(), "no snapshot blob sources");
        for source in sources {
            anyhow::ensure!(
                matches!(source.scheme(), "http" | "https") && source.host().is_some(),
                "snapshot source must be HTTP(S)"
            );
        }
        lease.resize(self.limits.max_bytes, &|| {
            should_cancel() || Instant::now() >= self.limits.deadline
        })
    }

    pub(crate) fn new(limits: BlobDownloadLimits) -> anyhow::Result<Self> {
        anyhow::ensure!(limits.rounds > 0 && limits.max_bytes > 0, "invalid blob download limits");
        anyhow::ensure!(
            !limits.connect_timeout.is_zero() && !limits.request_timeout.is_zero(),
            "download timeouts must be non-zero"
        );
        // Peer transfers are direct. HTTP content decoding must never change
        // compressed-file byte offsets or hide a Content-Encoding header.
        let client = Client::builder()
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(limits.connect_timeout)
            .timeout(limits.request_timeout);
        // Acceptance-only source isolation. Ordinary builds never read this
        // variable and continue to ignore ambient proxy settings.
        #[cfg(feature = "test_upgrade")]
        let client = match std::env::var("TEST_SNAPSHOT_DOWNLOAD_PROXY") {
            Ok(url) => client.proxy(reqwest::Proxy::all(url)?),
            Err(_) => client,
        };
        let client = client.build()?;
        Ok(Self { client, limits })
    }

    /// Sources are full resource URLs, already ordered by the snapshot source
    /// planner. The shared download-cache manager leases this ID and reserves
    /// the maximum transfer size before a temporary file is created. Unfinished
    /// files are private and removed on cancellation/exhaustion; completed files
    /// are reused only after validation. Accepted files become LRU candidates
    /// after the consumer releases its lease. No untrusted path component is
    /// accepted here.
    ///
    /// verify must validate the complete decoded content against trusted roots,
    /// not just HTTP framing. It may run again after a failed source and must
    /// be side-effect-free or provide its own isolated rollback. Returning T
    /// allows callers to retain parsed results rather than decode them twice.
    /// The callback also owns its parsing/allocation limits and cancellation.
    /// No active archive/state may be published from this callback.
    #[cfg(test)]
    pub(crate) fn download<T, C, V>(
        &self,
        directory: &Path,
        id: &[u8; 32],
        sources: &[Url],
        policy: ResumePolicy,
        should_cancel: &C,
        verify: V,
    ) -> anyhow::Result<(File, T)>
    where
        C: Fn() -> bool,
        V: FnMut(&mut File) -> anyhow::Result<T>,
    {
        let (prepared, value) =
            self.download_prepared(directory, id, sources, policy, should_cancel, verify)?;
        Ok((prepared.accept()?, value))
    }

    #[cfg(test)]
    fn download_prepared<T, C, V>(
        &self,
        directory: &Path,
        id: &[u8; 32],
        sources: &[Url],
        policy: ResumePolicy,
        should_cancel: &C,
        verify: V,
    ) -> anyhow::Result<(PreparedBlob, T)>
    where
        C: Fn() -> bool,
        V: FnMut(&mut File) -> anyhow::Result<T>,
    {
        let reservation = self.reserve(directory, id, sources, policy, should_cancel)?;
        self.download_reserved(reservation, sources, policy, should_cancel, verify)
    }

    fn download_reserved<T, C, V>(
        &self,
        reservation: (CacheLease, Option<File>),
        sources: &[Url],
        policy: ResumePolicy,
        should_cancel: &C,
        mut verify: V,
    ) -> anyhow::Result<(PreparedBlob, T)>
    where
        C: Fn() -> bool,
        V: FnMut(&mut File) -> anyhow::Result<T>,
    {
        let (lease, cached) = reservation;
        self.check(should_cancel)?;
        let destination = lease.path();
        if let Some(mut cached) = cached {
            if let Ok(value) = verify(&mut cached) {
                self.check(should_cancel)?;
                cached.rewind()?;
                return Ok((
                    PreparedBlob { destination, file: PreparedFile::Cached(cached), lease },
                    value,
                ));
            }
            self.check(should_cancel)?;
            drop(cached);
            lease.invalidate()?;
            lease.resize(0, &|| false)?;
            // Runtime content validation happens in the staged consumer,
            // which aborts on failure. Only the synchronous test helper can
            // reject a transport-valid cached file here and retry sources.
            self.reserve_download(&lease, sources, should_cancel)?;
        }
        let mut temporary = tempfile::Builder::new()
            .prefix(TEMP_PREFIX)
            .tempfile_in(destination.parent().expect("cache directory"))?;
        let mut owner = None;
        let mut progress = TransferProgress::default();
        let mut last_error = None;
        for round in 0..self.limits.rounds {
            for source in sources {
                self.check(should_cancel)?;
                if matches!(policy, ResumePolicy::SourceBound)
                    && (owner.as_ref() != Some(source) || progress.validator.is_none())
                {
                    reset(temporary.as_file_mut())?;
                    progress = TransferProgress::default();
                }
                owner = Some(source.clone());
                let attempt = self.transfer(
                    source,
                    temporary.as_file_mut(),
                    policy,
                    &mut progress,
                    should_cancel,
                );
                match attempt {
                    Ok(true) => {
                        temporary.as_file_mut().rewind()?;
                        match verify(temporary.as_file_mut()) {
                            Ok(value) => {
                                self.check(should_cancel)?;
                                temporary.as_file_mut().rewind()?;
                                lease.resize(temporary.as_file().metadata()?.len(), &|| {
                                    should_cancel()
                                })?;
                                return Ok((
                                    PreparedBlob {
                                        destination,
                                        file: PreparedFile::Downloaded(temporary),
                                        lease,
                                    },
                                    value,
                                ));
                            }
                            Err(error) => {
                                last_error =
                                    Some(error.context("snapshot blob verification failed"));
                                reset(temporary.as_file_mut())?;
                                progress = TransferProgress::default();
                            }
                        }
                    }
                    Ok(false) => {} // Valid incomplete range: continue from its end.
                    Err(error) => {
                        last_error = Some(error);
                    }
                }
            }
            if round + 1 < self.limits.rounds {
                let until = Instant::now()
                    .checked_add(self.limits.retry_delay)
                    .unwrap_or(self.limits.deadline)
                    .min(self.limits.deadline);
                while Instant::now() < until {
                    self.check(should_cancel)?;
                    std::thread::sleep(
                        until
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(25)),
                    );
                }
            }
        }
        self.check(should_cancel)?;
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("incomplete HTTP ranges")))
            .context("snapshot blob download exhausted source/retry budget")
    }

    fn check<C: Fn() -> bool>(&self, should_cancel: &C) -> anyhow::Result<()> {
        anyhow::ensure!(!should_cancel(), "snapshot blob download cancelled");
        anyhow::ensure!(Instant::now() < self.limits.deadline, "snapshot blob download deadline");
        Ok(())
    }

    fn transfer<C: Fn() -> bool>(
        &self,
        source: &Url,
        file: &mut File,
        policy: ResumePolicy,
        progress: &mut TransferProgress,
        should_cancel: &C,
    ) -> anyhow::Result<bool> {
        let TransferProgress { validator, expected_size, payload_end } = progress;
        let mut offset = file.metadata()?.len();
        if offset > 0 && matches!(policy, ResumePolicy::ChunkedPartition) {
            // Until the final map is available, retain only complete chunks.
            // Without map bounds a saved prefix might already include part of
            // the trailer. Restart rather than treating map bytes as a chunk.
            let retained = payload_end.map_or(0, |end| offset.min(end));
            let boundary = retained.saturating_sub(4)
                / snapshot_storage::chunks::CHUNK_BYTES as u64
                * snapshot_storage::chunks::CHUNK_BYTES as u64;
            offset = if boundary == 0 || validator.is_none() { 0 } else { 4 + boundary };
            file.set_len(offset)?;
        }
        self.check(should_cancel)?;
        let timeout = self
            .limits
            .request_timeout
            .min(self.limits.deadline.saturating_duration_since(Instant::now()));
        anyhow::ensure!(!timeout.is_zero(), "snapshot blob download deadline");
        let mut request =
            self.client.get(source.clone()).header(ACCEPT_ENCODING, "identity").timeout(timeout);
        if offset > 0 {
            request = request.header(RANGE, format!("bytes={offset}-"));
            if matches!(policy, ResumePolicy::SourceBound | ResumePolicy::ChunkedPartition) {
                request = request
                    .header(IF_RANGE, validator.as_ref().expect("source-bound resume validator"));
            }
        }
        let mut response = request.send()?;
        if !response.status().is_success() {
            if response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
                reset(file)?;
                *expected_size = None;
                *validator = None;
            }
            anyhow::bail!("snapshot HTTP status {}", response.status());
        }
        let response_etag = response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .filter(|value| value.starts_with('"') && value.ends_with('"'));
        if matches!(policy, ResumePolicy::ChunkedPartition) && validator.is_some() {
            // Another representation must not erase the retained prefix.
            anyhow::ensure!(
                response_etag == validator.as_deref(),
                "snapshot ETag changed during resume"
            );
        }
        // Invalid framing must discard the saved prefix before another source
        // can resume it. Network errors/404s retain a usable prefix instead.
        let framing = (|| -> anyhow::Result<_> {
            anyhow::ensure!(
                response
                    .headers()
                    .get(CONTENT_ENCODING)
                    .is_none_or(|encoding| encoding.as_bytes().eq_ignore_ascii_case(b"identity")),
                "encoded HTTP representation cannot be resumed"
            );
            let etag = response
                .headers()
                .get(ETAG)
                .and_then(|value| value.to_str().ok())
                .filter(|value| value.starts_with('"') && value.ends_with('"'))
                .map(str::to_owned);
            let (start, count, total) = match response.status() {
                StatusCode::OK => (0, response.content_length(), response.content_length()),
                StatusCode::PARTIAL_CONTENT => {
                    let header = response
                        .headers()
                        .get(CONTENT_RANGE)
                        .ok_or_else(|| anyhow::anyhow!("missing Content-Range"))?
                        .to_str()?;
                    let (start, end, total) = parse_range(header)?;
                    anyhow::ensure!(start == offset, "Content-Range starts at wrong offset");
                    anyhow::ensure!(
                        expected_size.is_none_or(|expected| expected == total),
                        "HTTP resource size changed"
                    );
                    if offset > 0 && matches!(policy, ResumePolicy::SourceBound) {
                        anyhow::ensure!(
                            etag.as_ref() == validator.as_ref(),
                            "primary ETag changed during resume"
                        );
                    }
                    let count = end - start + 1;
                    anyhow::ensure!(
                        response.content_length().is_none_or(|length| length == count),
                        "Content-Range length mismatch"
                    );
                    (start, Some(count), Some(total))
                }
                _ => anyhow::bail!("unsupported snapshot success status"),
            };
            anyhow::ensure!(
                total.is_none_or(|total| total > 0 && total <= self.limits.max_bytes),
                "snapshot HTTP size budget exceeded"
            );
            let map = if matches!(policy, ResumePolicy::ChunkedPartition) {
                let offset = response.headers().get(snapshot_storage::chunks::MAP_OFFSET_HEADER);
                let length = response.headers().get(snapshot_storage::chunks::MAP_LENGTH_HEADER);
                match (offset, length) {
                    (None, None) => None,
                    (Some(offset), Some(length)) => {
                        let offset = offset.to_str()?.parse::<u64>()?;
                        let length = length.to_str()?.parse::<u64>()?;
                        anyhow::ensure!(
                            offset >= 4
                                && length >= 12
                                && length
                                    <= (12 + 32 * snapshot_storage::chunks::MAX_CHUNKS) as u64
                                && offset.checked_add(length).and_then(|n| n
                                    .checked_add(snapshot_storage::chunks::FOOTER_BYTES as u64))
                                    == total,
                            "invalid HTTP chunk map bounds"
                        );
                        Some((offset, length))
                    }
                    _ => anyhow::bail!("incomplete HTTP chunk map headers"),
                }
            } else {
                None
            };
            Ok((start, count, total, etag, map))
        })();
        let (start, count, total, etag, map) = match framing {
            Ok(value) => value,
            Err(error) => {
                reset(file)?;
                *expected_size = None;
                *validator = None;
                return Err(error);
            }
        };
        if start == 0 {
            reset(file)?;
        }
        *expected_size = total;
        *validator = etag;
        *payload_end = map.map(|(offset, _)| offset);
        file.seek(std::io::SeekFrom::End(0))?;
        let mut received = 0u64;
        let mut buffer = [0; 64 * 1024];
        loop {
            self.check(should_cancel)?;
            let read = response.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            received = received
                .checked_add(read as u64)
                .ok_or_else(|| anyhow::anyhow!("HTTP length overflow"))?;
            if received > self.limits.max_bytes.saturating_sub(start)
                || count.is_some_and(|count| received > count)
            {
                reset(file)?;
                *expected_size = None;
                *validator = None;
                anyhow::bail!("snapshot HTTP body exceeds declared size or budget");
            }
            file.write_all(&buffer[..read])?;
        }
        anyhow::ensure!(count.is_none_or(|count| received == count), "truncated HTTP body");
        anyhow::ensure!(file.metadata()?.len() > 0, "empty snapshot HTTP body");
        let complete = total
            .is_none_or(|total| file.metadata().map(|meta| meta.len() == total).unwrap_or(false));
        if complete && matches!(policy, ResumePolicy::ChunkedPartition) {
            let verified = (|| -> anyhow::Result<()> {
                let actual = snapshot_storage::chunks::verify_file(file, should_cancel)?;
                anyhow::ensure!(
                    map.is_none_or(|map| map == (actual.offset, actual.length)),
                    "HTTP chunk map differs from footer"
                );
                anyhow::ensure!(
                    validator.as_ref().is_none_or(|etag| *etag == actual.etag()),
                    "snapshot chunk map ETag mismatch"
                );
                Ok(())
            })();
            if let Err(error) = verified {
                reset(file)?;
                *expected_size = None;
                *validator = None;
                return Err(error);
            }
        }
        Ok(complete)
    }
}

fn reset(file: &mut File) -> anyhow::Result<()> {
    file.set_len(0)?;
    file.rewind()?;
    Ok(())
}

fn parse_range(value: &str) -> anyhow::Result<(u64, u64, u64)> {
    let (range, total) = value
        .split_once(' ')
        .filter(|(unit, _)| unit.eq_ignore_ascii_case("bytes"))
        .map(|(_, value)| value)
        .and_then(|value| value.split_once('/'))
        .ok_or_else(|| anyhow::anyhow!("invalid Content-Range"))?;
    let (start, end) =
        range.split_once('-').ok_or_else(|| anyhow::anyhow!("invalid Content-Range bounds"))?;
    anyhow::ensure!(
        [start, end, total]
            .iter()
            .all(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())),
        "invalid Content-Range number"
    );
    let (start, end, total) = (start.parse()?, end.parse()?, total.parse()?);
    anyhow::ensure!(start <= end && end < total, "invalid Content-Range bounds");
    Ok((start, end, total))
}

#[cfg(test)]
#[path = "resumable_download_tests.rs"]
mod tests;
