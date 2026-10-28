use std::collections::HashMap;
use std::io::BufRead;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Condvar;
use parking_lot::Mutex;

use super::super::snapshot_consumption::tests::limits;
use super::super::snapshot_consumption::tests::Target;
use super::super::snapshot_manifest::ThreadSnapshotManifest;
use super::super::snapshot_publication::publish_partitioned_snapshot;
use super::super::snapshot_publication::tests::Fixture;
use super::super::thread_snapshot_import::BlobDownloadLimits;
use super::*;

#[test]
fn snapshot_http_import_rejects_protocol_root_overflow_before_trust_or_staging(
) -> anyhow::Result<()> {
    use super::super::snapshot_manifest::MAX_SNAPSHOT_ROOTS_PER_LIST;
    use crate::node::services::sync::snapshot_compression::write_partition_blob;
    let source = Fixture::new()?;
    let primary = hex::encode(source.state.block_id.as_array());
    let body = write_partition_blob(Vec::new(), |writer| {
        writer.write_all(super::super::snapshot_partitioned::MAGIC)?;
        writer.write_all(&(MAX_SNAPSHOT_ROOTS_PER_LIST + 1).to_le_bytes())?;
        Ok(())
    })?;
    let server =
        Server::new(HashMap::from([(primary.clone(), body)]), primary.clone(), None, None, None);
    let target = Target::new()?;
    let cache = tempfile::tempdir()?;
    let downloader = downloader();
    let urls = [server.base.join(&primary)?];
    let mut request =
        request(&source, &target, cache.path(), &downloader, &urls, &[], fetch_limits());
    request.import_limits.roots_per_list = u64::MAX;
    let error = download_and_stage_snapshot(
        request,
        |_| panic!("invalid primary must not reach trust verification"),
        &|| false,
    )
    .err()
    .unwrap();
    assert!(format!("{error:#}").contains("root budget exceeded"));
    assert_eq!(server.part_requests(), 0);
    target.assert_aborted();
    assert_eq!(std::fs::read_dir(cache.path())?.count(), 0);
    Ok(())
}

#[derive(Default)]
struct ServerState {
    raw_requests: Mutex<Vec<String>>,
    unpublished_requests: Mutex<HashMap<String, usize>>,
    http_errors: Mutex<HashMap<String, u16>>,
    etag_override: Mutex<Option<String>>,
    stop: AtomicBool,
    release: AtomicBool,
    gated: AtomicBool,
    hold_all_partitions: AtomicBool,
    active: AtomicUsize,
    peak: AtomicUsize,
    requests: Mutex<Vec<String>>,
    pulse: Mutex<()>,
    changed: Condvar,
}

pub(crate) struct Server {
    pub(crate) base: Url,
    primary: String,
    state: Arc<ServerState>,
    handle: Option<JoinHandle<()>>,
}

impl Server {
    pub(crate) fn new(
        blobs: HashMap<String, Vec<u8>>,
        primary: String,
        gate: Option<String>,
        second: Option<String>,
        partial: Option<String>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base =
            Url::parse(&format!("http://{}/v2/storage/", listener.local_addr().unwrap())).unwrap();
        let state = Arc::new(ServerState::default());
        let shared = state.clone();
        let primary_name = primary.clone();
        let handle =
            std::thread::Builder::new().name("snapshot-test-http".into()).spawn(move || {
                std::thread::scope(|scope| {
                    while !shared.stop.load(Ordering::Acquire) {
                        match listener.accept() {
                            Ok((mut stream, _)) => {
                                let (state, blobs, primary, gate, second, partial) =
                                    (&shared, &blobs, &primary_name, &gate, &second, &partial);
                                scope.spawn(move || {
                                    serve_request(
                                        &mut stream,
                                        state,
                                        blobs,
                                        primary,
                                        gate,
                                        second,
                                        partial,
                                    );
                                });
                            }
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                std::thread::sleep(Duration::from_millis(2))
                            }
                            Err(error) => panic!("HTTP accept: {error}"),
                        }
                    }
                })
            });
        Self { base, primary, state, handle: Some(handle.unwrap()) }
    }

    fn wait(&self, predicate: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut guard = self.state.pulse.lock();
        while !predicate() {
            assert!(
                Instant::now() < deadline,
                "HTTP test wait timed out: {:?}",
                self.state.requests.lock()
            );
            self.state.changed.wait_for(&mut guard, Duration::from_millis(10));
        }
    }

    fn release(&self) {
        self.state.release.store(true, Ordering::Release);
        self.state.changed.notify_all();
    }

    fn part_requests(&self) -> usize {
        self.state.requests.lock().iter().filter(|name| **name != self.primary).count()
    }

    pub(crate) fn raw_requests(&self) -> Vec<String> {
        self.state.raw_requests.lock().clone()
    }

    pub(crate) fn override_etag(&self, value: Option<String>) {
        *self.state.etag_override.lock() = value;
    }

    pub(crate) fn defer_publication(&self, name: &str, requests: usize) {
        self.state.unpublished_requests.lock().insert(name.to_owned(), requests);
    }

    fn fail_requests(&self, name: &str, status: u16) {
        self.state.http_errors.lock().insert(name.to_owned(), status);
    }

    pub(crate) fn payloads_started(&self) -> usize {
        self.state.requests.lock().len()
    }

    pub(crate) fn release_payload(&self) {
        self.release();
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Release);
        self.state.changed.notify_all();
        self.handle.take().unwrap().join().unwrap();
    }
}

fn read_request(stream: &mut TcpStream) -> Option<String> {
    let mut reader = std::io::BufReader::new(stream);
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        if reader.read_until(b'\n', &mut bytes).ok()? == 0 || bytes.len() > 16_384 {
            return None;
        }
    }
    String::from_utf8(bytes).ok()
}

/// HEAD/map requests are transport setup, not payload download slots. Record and
/// gate payload requests only, so concurrency assertions measure actual overlap.
fn serve_request(
    stream: &mut TcpStream,
    state: &ServerState,
    blobs: &HashMap<String, Vec<u8>>,
    primary: &str,
    gate: &Option<String>,
    second: &Option<String>,
    partial: &Option<String>,
) {
    stream.set_nonblocking(false).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let Some(request) = read_request(stream) else { return };
    state.raw_requests.lock().push(request.clone());
    let line = request.lines().next().unwrap();
    let mut parts = line.split_whitespace();
    let head = parts.next() == Some("HEAD");
    let name = parts.next().unwrap().rsplit('/').next().unwrap();
    let name = name
        .strip_prefix("accounts_")
        .or_else(|| name.strip_prefix("messages_"))
        .unwrap_or(name)
        .to_owned();
    if let Some(status) = state.http_errors.lock().get(&name) {
        let _ = write!(
            stream,
            "HTTP/1.1 {status} Test error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        return;
    }
    let unpublished = {
        let mut pending = state.unpublished_requests.lock();
        pending.get_mut(&name).is_some_and(|remaining| {
            if *remaining == 0 {
                return false;
            }
            *remaining -= 1;
            true
        })
    };
    if unpublished {
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        return;
    }
    let Some(body) = blobs.get(&name) else {
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        return;
    };
    let metadata = snapshot_storage::chunks::read_footer(&mut std::io::Cursor::new(body)).ok();
    let range = request.lines().find_map(|line| {
        line.to_ascii_lowercase().strip_prefix("range: bytes=").map(str::to_owned)
    });
    let (start, end) = range.as_ref().map_or((0, body.len()), |range| {
        let (start, end) = range.split_once('-').unwrap();
        (
            start.parse::<usize>().unwrap(),
            if end.is_empty() { body.len() } else { end.parse::<usize>().unwrap() + 1 },
        )
    });
    let payload = !head && metadata.as_ref().is_none_or(|map| start < map.offset as usize);
    let mut occurrence = 0;
    if payload {
        occurrence = {
            let mut requests = state.requests.lock();
            requests.push(name.clone());
            requests.iter().filter(|seen| *seen == &name).count()
        };
        let active = state.active.fetch_add(1, Ordering::AcqRel) + 1;
        state.peak.fetch_max(active, Ordering::AcqRel);
        state.changed.notify_all();
        if gate.as_ref() == Some(&name)
            || (state.hold_all_partitions.load(Ordering::Acquire) && name != primary)
        {
            state.gated.store(true, Ordering::Release);
            state.changed.notify_all();
            let mut pulse = state.pulse.lock();
            while !state.release.load(Ordering::Acquire) && !state.stop.load(Ordering::Acquire) {
                state.changed.wait_for(&mut pulse, Duration::from_millis(10));
            }
        } else if second.as_ref() == Some(&name) {
            let mut pulse = state.pulse.lock();
            while !state.gated.load(Ordering::Acquire) && !state.stop.load(Ordering::Acquire) {
                state.changed.wait_for(&mut pulse, Duration::from_millis(10));
            }
        }
        state.active.fetch_sub(1, Ordering::AcqRel);
        state.changed.notify_all();
    }
    if state.stop.load(Ordering::Acquire) {
        return;
    }
    if start >= body.len() || end > body.len() || start >= end {
        let _ = stream.write_all(
            b"HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        return;
    }
    let mut headers =
        format!("Content-Length: {}\r\n", if head { body.len() } else { end - start });
    if let Some(metadata) = metadata {
        let etag = state.etag_override.lock().clone().unwrap_or_else(|| metadata.etag());
        headers.push_str(&format!(
            "ETag: {}\r\nsnapshot-chunk-map-offset: {}\r\nsnapshot-chunk-map-length: {}\r\n",
            etag, metadata.offset, metadata.length
        ));
    }
    let status = if range.is_some() && !head {
        headers.push_str(&format!("Content-Range: bytes {start}-{}/{}\r\n", end - 1, body.len()));
        "206 Partial Content"
    } else {
        "200 OK"
    };
    let response = format!("HTTP/1.1 {status}\r\nConnection: close\r\n{headers}\r\n");
    let _ = stream.write_all(response.as_bytes());
    if !head {
        let sent_end = if payload && partial.as_ref() == Some(&name) && occurrence == 1 {
            start + (end - start) / 2
        } else {
            end
        };
        let _ = stream.write_all(&body[start..sent_end]);
    }
}

fn published(
    source: &Fixture,
) -> anyhow::Result<(ThreadSnapshotManifest, HashMap<String, Vec<u8>>)> {
    let directory = tempfile::tempdir()?;
    let manifest = publish_partitioned_snapshot(
        source.publication(source.pin()?, directory.path()),
        &|| false,
        |_| Ok(()),
    )?;
    let blobs = std::fs::read_dir(directory.path())?
        .chain(std::fs::read_dir(directory.path().join(".snapshot-resources/blobs"))?)
        .filter(|entry| {
            entry.as_ref().map_or(true, |entry| {
                entry.path().is_file() && entry.file_name().to_string_lossy().len() == 64
            })
        })
        .map(|entry| {
            let entry = entry?;
            Ok((entry.file_name().into_string().unwrap(), std::fs::read(entry.path())?))
        })
        .collect::<anyhow::Result<_>>()?;
    Ok((manifest, blobs))
}

fn downloader() -> BlobDownloadLimits {
    BlobDownloadLimits {
        rounds: 2,
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(2),
        retry_delay: Duration::ZERO,
        deadline: Instant::now() + Duration::from_secs(15),
    }
}

fn fetch_limits() -> SnapshotFetchLimits {
    SnapshotFetchLimits::default()
}

fn request<'a>(
    source: &Fixture,
    target: &'a Target,
    cache: &'a Path,
    downloader: &BlobDownloadLimits,
    primary_sources: &'a [Url],
    known_sources: &'a [(NodeIdentifier, Url)],
    fetch_limits: SnapshotFetchLimits,
) -> SnapshotHttpImport<'a> {
    SnapshotHttpImport {
        repository: &target.repository,
        cache_directory: cache,
        expected: (source.state.thread_id, source.state.block_id),
        primary_sources,
        known_sources,
        fallback_sources: &[],
        download_limits: *downloader,
        import_limits: limits(),
        fetch_limits,
    }
}

fn assert_state(staged: &StagedPartitionedSnapshot<'_>, source: &Fixture) {
    let state = staged.state();
    assert_eq!(
        state.shard_state.0.durable.map().hash(),
        source.state.shard_state.0.durable.map().hash()
    );
    assert_eq!(
        state.shard_state.0.durable.map().capacity(),
        source.state.shard_state.0.durable.map().capacity()
    );
    assert_eq!(state.messages.root_hash(), source.state.messages.root_hash());
    assert_eq!(state.messages.capacity(), source.state.messages.capacity());
    assert_eq!(
        state.high_priority_messages.root_hash(),
        source.state.high_priority_messages.root_hash()
    );
    assert_eq!(
        state.high_priority_messages.capacity(),
        source.state.high_priority_messages.capacity()
    );
}

#[test]
fn snapshot_http_import_reuses_a_full_cache_without_network_or_eviction() -> anyhow::Result<()> {
    use crate::services::blob_sync::external_fileshares_based::download_cache::DownloadCache;
    let source = Fixture::new()?;
    let (_, blobs) = published(&source)?;
    let cache = tempfile::tempdir()?;
    let total = blobs.values().map(|blob| blob.len() as u64).sum::<u64>();
    for (id, bytes) in &blobs {
        std::fs::write(cache.path().join(id), bytes)?;
    }
    let _owner = DownloadCache::open(cache.path(), Some(total))?;
    let downloader = BlobDownloadLimits {
        rounds: 1,
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(2),
        retry_delay: Duration::ZERO,
        deadline: Instant::now() + Duration::from_secs(15),
    };
    let target = Target::new()?;
    let staged = download_and_stage_snapshot(
        request(&source, &target, cache.path(), &downloader, &[], &[], fetch_limits()),
        |_| Ok(()),
        &|| false,
    )?;
    assert_state(&staged, &source);
    assert_eq!(std::fs::read_dir(cache.path())?.count(), blobs.len());
    drop(staged);
    target.assert_aborted();
    Ok(())
}

#[test]
fn snapshot_http_import_completes_with_cache_smaller_than_snapshot_and_download_slots(
) -> anyhow::Result<()> {
    use crate::services::blob_sync::external_fileshares_based::download_cache::DownloadCache;
    let source = Fixture::new()?;
    let (_, blobs) = published(&source)?;
    let maximum = blobs.values().map(|blob| blob.len() as u64).max().unwrap();
    assert!(blobs.values().map(|blob| blob.len() as u64).sum::<u64>() > maximum);
    let primary = hex::encode(source.state.block_id.as_array());
    let server = Server::new(blobs, primary.clone(), None, None, None);
    let cache = tempfile::tempdir()?;
    let _owner = DownloadCache::open(cache.path(), Some(maximum))?;
    let downloader = BlobDownloadLimits {
        rounds: 2,
        connect_timeout: Duration::from_millis(300),
        request_timeout: Duration::from_secs(2),
        retry_delay: Duration::ZERO,
        deadline: Instant::now() + Duration::from_secs(15),
    };
    let target = Target::new()?;
    let urls = [server.base.join(&primary)?];
    let known = [(source.snapshot.bk_set().iter().next().unwrap().node_id(), server.base.clone())];
    for _ in 0..2 {
        let staged = download_and_stage_snapshot(
            request(&source, &target, cache.path(), &downloader, &urls, &known, fetch_limits()),
            |_| Ok(()),
            &|| false,
        )?;
        assert_state(&staged, &source);
        let bytes = std::fs::read_dir(cache.path())?
            .map(|item| Ok(item?.metadata()?.len()))
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .sum::<u64>();
        assert!(bytes <= maximum);
        // Discard the staged import to simulate a retry after a later failure.
        drop(staged);
        target.assert_aborted();
    }
    assert!(server
        .raw_requests()
        .iter()
        .any(|request| request.contains(&format!("/v2/storage/{primary}.partitions/accounts_"))));
    Ok(())
}

#[test]
fn snapshot_http_import_downloaders_do_not_wait_for_an_earlier_partition() -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (manifest, blobs) = published(&source)?;
    let first = hex::encode(manifest.partition_blob_id(0).unwrap());
    let second = hex::encode(manifest.partition_blob_id(1).unwrap());
    let primary = hex::encode(source.state.block_id.as_array());
    for single_worker in [false, true] {
        let target = Target::new()?;
        let cache = tempfile::tempdir()?;
        let downloader = downloader();
        let server = Server::new(
            blobs.clone(),
            primary.clone(),
            Some(first.clone()),
            (!single_worker).then(|| second.clone()),
            None,
        );
        let urls = [server.base.join(&primary)?];
        let known =
            [(source.snapshot.bk_set().iter().next().unwrap().node_id(), server.base.clone())];
        let fetch = SnapshotFetchLimits { concurrent_downloads: if single_worker { 1 } else { 4 } };
        std::thread::scope(|scope| -> anyhow::Result<()> {
            let imported = scope.spawn(|| {
                download_and_stage_snapshot(
                    request(&source, &target, cache.path(), &downloader, &urls, &known, fetch),
                    |_| Ok(()),
                    &|| false,
                )
            });
            let expected = if single_worker { 1 } else { blobs.len() - 1 };
            server.wait(|| {
                // A recorded request may still be waiting to enter its handler.
                // Observe actual overlap before releasing the gated response.
                server.state.gated.load(Ordering::Acquire)
                    && server.part_requests() >= expected
                    && server.state.peak.load(Ordering::Acquire)
                        >= if single_worker { 1 } else { 2 }
                    && (single_worker || cache.path().join(&second).exists())
            });
            assert_eq!(server.part_requests(), expected);
            assert!(!cache.path().join(&first).exists());
            assert!(server.state.peak.load(Ordering::Acquire) <= fetch.concurrent_downloads);
            assert!(cache.path().join(&primary).exists());
            server.release();
            let staged = imported.join().unwrap()?;
            assert_state(&staged, &source);
            target.assert_active_unchanged();
            assert!(target.count("A") > 0);
            assert_eq!(
                std::fs::read_dir(cache.path())?.count(),
                manifest.account_partition_roots.len()
                    + manifest.normal_message_partition_roots.len()
                    + manifest.high_priority_message_partition_roots.len()
                    + 1
            );
            assert!(server.state.peak.load(Ordering::Acquire) <= fetch.concurrent_downloads);
            if !single_worker {
                assert!(server.state.peak.load(Ordering::Acquire) >= 2);
            }
            drop(staged);
            target.assert_aborted();
            let count = server.state.requests.lock().len();
            // No endpoints at all: every cached blob must be reverified and fed
            // through the same consumer without issuing network requests.
            let cached = download_and_stage_snapshot(
                request(&source, &target, cache.path(), &downloader, &[], &[], fetch),
                |_| Ok(()),
                &|| false,
            )?;
            assert_state(&cached, &source);
            assert_eq!(server.state.requests.lock().len(), count);
            drop(cached);
            target.assert_aborted();
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
fn snapshot_http_import_uses_four_independent_download_slots() -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (_, blobs) = published(&source)?;
    assert!(blobs.len() > 5);
    let primary = hex::encode(source.state.block_id.as_array());
    let server = Server::new(blobs, primary.clone(), None, None, None);
    server.state.hold_all_partitions.store(true, Ordering::Release);
    let target = Target::new()?;
    let cache = tempfile::tempdir()?;
    let downloader = downloader();
    let urls = [server.base.join(&primary)?];
    let known = [(source.snapshot.bk_set().iter().next().unwrap().node_id(), server.base.clone())];
    assert_eq!(fetch_limits().concurrent_downloads, 4);
    std::thread::scope(|scope| -> anyhow::Result<()> {
        let imported = scope.spawn(|| {
            download_and_stage_snapshot(
                request(&source, &target, cache.path(), &downloader, &urls, &known, fetch_limits()),
                |_| Ok(()),
                &|| false,
            )
        });
        server.wait(|| server.state.active.load(Ordering::Acquire) == 4);
        assert_eq!(server.part_requests(), 4);
        server.release();
        let staged = imported.join().unwrap()?;
        assert_state(&staged, &source);
        assert_eq!(server.state.peak.load(Ordering::Acquire), 4);
        drop(staged);
        target.assert_aborted();
        Ok(())
    })
}

#[test]
fn snapshot_http_import_resume_does_not_repeat_staged_writes() -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (manifest, blobs) = published(&source)?;
    let first = hex::encode(manifest.partition_blob_id(0).unwrap());
    let primary = hex::encode(source.state.block_id.as_array());
    let server = Server::new(blobs, primary.clone(), None, None, Some(first.clone()));
    let target = Target::new()?;
    let cache = tempfile::tempdir()?;
    let downloader = downloader();
    let urls = [server.base.join(&primary)?];
    let known = [(source.snapshot.bk_set().iter().next().unwrap().node_id(), server.base.clone())];
    let staged = download_and_stage_snapshot(
        request(&source, &target, cache.path(), &downloader, &urls, &known, fetch_limits()),
        |_| Ok(()),
        &|| false,
    )?;
    assert_state(&staged, &source);
    assert_eq!(server.state.requests.lock().iter().filter(|name| **name == first).count(), 2);
    target.assert_written_once(source.state.shard_state.0.durable.map().iter().count());
    drop(staged);
    target.assert_aborted();
    Ok(())
}

#[test]
fn snapshot_http_import_installs_through_repository_commit_and_recovery_path() -> anyhow::Result<()>
{
    let source = Fixture::new()?;
    let (_, blobs) = published(&source)?;
    let primary = hex::encode(source.state.block_id.as_array());
    let server = Server::new(blobs, primary.clone(), None, None, None);
    let directory = tempfile::tempdir()?;
    let (mut repository, _) = super::super::tests::make_test_repository(&directory)?;
    let accounts = repository.thread_accounts_repository.clone();
    let cache = tempfile::tempdir()?;
    let downloader = downloader();
    let urls = [server.base.join(&primary)?];
    let known = [(source.snapshot.bk_set().iter().next().unwrap().node_id(), server.base.clone())];
    let status = repository.download_and_install_partitioned_snapshot(
        SnapshotHttpImport {
            repository: accounts.durable_map_repo(),
            cache_directory: cache.path(),
            expected: (source.state.thread_id, source.state.block_id),
            primary_sources: &urls,
            known_sources: &known,
            fallback_sources: &[],
            download_limits: downloader,
            import_limits: limits(),
            fetch_limits: fetch_limits(),
        },
        &source.state.thread_id,
        |_| Ok(()),
        &|| false,
    )?;
    assert!(matches!(status, super::super::SnapshotImportStatus::Imported));
    let restored = repository
        .get_full_optimistic_state_no_cache(&source.state.block_id, &source.state.thread_id, None)?
        .unwrap();
    assert_eq!(
        restored.shard_state.0.durable.map().hash(),
        source.state.shard_state.0.durable.map().hash()
    );
    assert_eq!(restored.messages.root_hash(), source.state.messages.root_hash());
    assert_eq!(
        restored.high_priority_messages.root_hash(),
        source.state.high_priority_messages.root_hash()
    );
    assert!(
        !super::super::RepositoryImpl::snapshot_import_recovery_path(&repository.data_dir).exists()
    );
    Ok(())
}

#[test]
fn snapshot_http_import_late_corruption_aborts_staging_and_keeps_verified_cache(
) -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (manifest, mut blobs) = published(&source)?;
    let index =
        manifest.account_partition_roots.len() + manifest.normal_message_partition_roots.len();
    let bad = hex::encode(manifest.partition_blob_id(index).unwrap());
    blobs.insert(
        bad.clone(),
        crate::node::services::sync::snapshot_compression::write_partition_blob(
            Vec::new(),
            |writer| {
                writer.write_all(b"invalid-logical-partition")?;
                Ok(())
            },
        )?,
    );
    let primary = hex::encode(source.state.block_id.as_array());
    let server = Server::new(blobs, primary.clone(), Some(bad.clone()), None, None);
    let target = Target::new()?;
    let cache = tempfile::tempdir()?;
    let downloader = downloader();
    let urls = [server.base.join(&primary)?];
    let known = [(source.snapshot.bk_set().iter().next().unwrap().node_id(), server.base.clone())];
    std::thread::scope(|scope| {
        let imported = scope.spawn(|| {
            download_and_stage_snapshot(
                request(&source, &target, cache.path(), &downloader, &urls, &known, fetch_limits()),
                |_| Ok(()),
                &|| false,
            )
        });
        // Independent downloaders may encounter bad later blobs before import
        // reaches them. Explicitly delay this failure to exercise cached successes.
        server.wait(|| {
            manifest.account_partition_roots.iter().all(|root| {
                cache
                    .path()
                    .join(hex::encode(super::super::snapshot_manifest::account_partition_blob_id(
                        root,
                    )))
                    .exists()
            })
        });
        server.release();
        assert!(imported.join().unwrap().is_err());
    });
    target.assert_aborted();
    assert!(!cache.path().join(&bad).exists());
    for index in 0..manifest.account_partition_roots.len() {
        assert!(cache
            .path()
            .join(hex::encode(manifest.partition_blob_id(index).unwrap()))
            .exists());
    }
    assert!(std::fs::read_dir(cache.path())?.all(|entry| entry.unwrap().file_name().len() == 64));
    Ok(())
}

#[test]
fn snapshot_http_import_cancel_stops_and_joins_prefetch_and_aborts_both_copies(
) -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (manifest, blobs) = published(&source)?;
    let gate_index =
        manifest.account_partition_roots.len() + manifest.normal_message_partition_roots.len();
    let gate = hex::encode(manifest.partition_blob_id(gate_index).unwrap());
    let primary = hex::encode(source.state.block_id.as_array());
    let server = Server::new(blobs, primary.clone(), Some(gate), None, None);
    let target = Target::new()?;
    let cache = tempfile::tempdir()?;
    let downloader = downloader();
    let urls = [server.base.join(&primary)?];
    let known = [(source.snapshot.bk_set().iter().next().unwrap().node_id(), server.base.clone())];
    let cancelled = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let imported = scope.spawn(|| {
            download_and_stage_snapshot(
                request(&source, &target, cache.path(), &downloader, &urls, &known, fetch_limits()),
                |_| Ok(()),
                &|| cancelled.load(Ordering::Acquire),
            )
        });
        server.wait(|| server.state.gated.load(Ordering::Acquire) && target.count("A") > 0);
        cancelled.store(true, Ordering::Release);
        server.release();
        assert!(imported.join().unwrap().is_err());
    });
    target.assert_aborted();
    assert!(std::fs::read_dir(cache.path())?.all(|entry| entry.unwrap().file_name().len() == 64));
    Ok(())
}

#[test]
fn snapshot_http_import_rejects_corrupt_cache_without_evicting_unused_good_prefetches(
) -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (manifest, blobs) = published(&source)?;
    let primary = hex::encode(source.state.block_id.as_array());
    let server = Server::new(blobs, primary.clone(), None, None, None);
    let target = Target::new()?;
    let cache = tempfile::tempdir()?;
    let downloader = downloader();
    let urls = [server.base.join(&primary)?];
    let known = [(source.snapshot.bk_set().iter().next().unwrap().node_id(), server.base.clone())];
    drop(download_and_stage_snapshot(
        request(&source, &target, cache.path(), &downloader, &urls, &known, fetch_limits()),
        |_| Ok(()),
        &|| false,
    )?);
    let count = std::fs::read_dir(cache.path())?.count();
    let bad = hex::encode(manifest.partition_blob_id(0).unwrap());
    std::fs::write(cache.path().join(&bad), b"bad cached partition")?;
    let requests = server.state.requests.lock().len();
    assert!(download_and_stage_snapshot(
        request(&source, &target, cache.path(), &downloader, &[], &[], fetch_limits()),
        |_| Ok(()),
        &|| false
    )
    .is_err());
    assert!(!cache.path().join(&bad).exists());
    assert_eq!(std::fs::read_dir(cache.path())?.count(), count - 1);
    assert_eq!(server.state.requests.lock().len(), requests);
    target.assert_aborted();
    Ok(())
}

#[test]
fn snapshot_http_import_fetch_failure_cancels_other_workers_and_preserves_error(
) -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (manifest, mut blobs) = published(&source)?;
    let first = hex::encode(manifest.partition_blob_id(0).unwrap());
    let missing = hex::encode(manifest.partition_blob_id(1).unwrap());
    blobs.remove(&missing);
    let primary = hex::encode(source.state.block_id.as_array());
    let server = Server::new(blobs, primary.clone(), Some(first), None, None);
    server.fail_requests(&missing, 503);
    let target = Target::new()?;
    let cache = tempfile::tempdir()?;
    let downloader = downloader();
    let urls = [server.base.join(&primary)?];
    let known = [(source.snapshot.bk_set().iter().next().unwrap().node_id(), server.base.clone())];
    let error = download_and_stage_snapshot(
        request(&source, &target, cache.path(), &downloader, &urls, &known, fetch_limits()),
        |_| Ok(()),
        &|| false,
    )
    .err()
    .unwrap();
    server.release();
    assert!(
        format!("{error:#}").contains("503"),
        "original fetch failure must survive cancellation: {error:#}"
    );
    target.assert_aborted();
    // Ready partitions can now be imported while the first transfer is stalled.
    // Such verified entries survive failure; incomplete private files must not.
    assert!(cache.path().join(&primary).exists());
    assert!(!cache.path().join(&missing).exists());
    assert!(std::fs::read_dir(cache.path())?.all(|entry| entry.unwrap().file_name().len() == 64));
    Ok(())
}

#[test]
fn snapshot_http_import_unpublished_partition_waits_until_deadline_and_aborts_staging(
) -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (manifest, mut blobs) = published(&source)?;
    let missing = hex::encode(manifest.partition_blob_id(0).unwrap());
    blobs.remove(&missing);
    let primary = hex::encode(source.state.block_id.as_array());
    let server = Server::new(blobs, primary.clone(), None, None, None);
    let target = Target::new()?;
    let cache = tempfile::tempdir()?;
    let mut limits = downloader();
    limits.retry_delay = Duration::from_millis(2);
    let started = Instant::now();
    limits.deadline = started + Duration::from_secs(1);
    let urls = [server.base.join(&primary)?];
    let known = [(source.snapshot.bk_set().iter().next().unwrap().node_id(), server.base.clone())];
    let result = download_and_stage_snapshot(
        request(&source, &target, cache.path(), &limits, &urls, &known, fetch_limits()),
        |_| Ok(()),
        &|| false,
    );
    assert!(result.is_err());
    assert!(started.elapsed() >= Duration::from_secs(1), "404 must not fail before the deadline");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "deadline must cancel the waiting importer"
    );
    let polls = server
        .raw_requests()
        .iter()
        .filter(|request| {
            request.starts_with("HEAD ") && request.lines().next().unwrap().contains(&missing)
        })
        .count();
    assert!(polls > usize::from(limits.rounds), "404 must survive more than the error budget");
    target.assert_aborted();
    assert!(!cache.path().join(missing).exists());
    Ok(())
}

#[test]
fn snapshot_http_import_empty_manifest_needs_only_verified_primary() -> anyhow::Result<()> {
    let mut source = Fixture::new()?;
    source.state.shard_state.0.durable =
        account_state::DurableThreadAccountsState::with_map(account_state::ThreadAccountMap::new());
    source.state.messages = crate::types::thread_message_queue::ThreadMessageQueueState::empty();
    source.state.high_priority_messages =
        crate::types::thread_message_queue::ThreadMessageQueueState::empty();
    let (manifest, blobs) = published(&source)?;
    assert_eq!(manifest, ThreadSnapshotManifest::default());
    let primary = hex::encode(source.state.block_id.as_array());
    let server = Server::new(blobs, primary.clone(), None, None, None);
    let target = Target::new()?;
    let cache = tempfile::tempdir()?;
    let downloader = downloader();
    let urls = [server.base.join(&primary)?];
    let staged = download_and_stage_snapshot(
        request(&source, &target, cache.path(), &downloader, &urls, &[], fetch_limits()),
        |_| Ok(()),
        &|| false,
    )?;
    assert_state(&staged, &source);
    assert_eq!(server.part_requests(), 0);
    assert_eq!(std::fs::read_dir(cache.path())?.count(), 1);
    drop(staged);
    target.assert_aborted();
    Ok(())
}

#[test]
#[allow(clippy::disallowed_methods)] // Exercise unwinding across the sync/async import boundary.
fn snapshot_http_import_panic_cancels_tasks_and_releases_private_files() -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (_, blobs) = published(&source)?;
    let primary = hex::encode(source.state.block_id.as_array());
    let server = Server::new(blobs, primary.clone(), None, None, None);
    let cache = tempfile::tempdir()?;
    let target = Target::new()?;
    let urls = [server.base.join(&primary)?];
    let known = [(source.snapshot.bk_set().iter().next().unwrap().node_id(), server.base.clone())];
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        download_and_stage_snapshot(
            request(&source, &target, cache.path(), &downloader(), &urls, &known, fetch_limits()),
            |_| panic!("primary verifier panic"),
            &|| false,
        )
        .map(drop)
    }));
    assert!(result.is_err());
    target.assert_aborted();
    assert_eq!(std::fs::read_dir(cache.path())?.count(), 0);
    Ok(())
}

#[test]
fn snapshot_http_import_untrusted_primary_and_invalid_limits_never_fetch_partitions(
) -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (_, blobs) = published(&source)?;
    let primary = hex::encode(source.state.block_id.as_array());
    let server = Server::new(blobs, primary.clone(), None, None, None);
    let target = Target::new()?;
    let cache = tempfile::tempdir()?;
    let downloader = downloader();
    let urls = [server.base.join(&primary)?];
    for mode in 0..2 {
        let mut fetch = fetch_limits();
        if mode == 1 {
            fetch.concurrent_downloads = 0;
        }
        assert!(download_and_stage_snapshot(
            request(&source, &target, cache.path(), &downloader, &urls, &[], fetch),
            |_| anyhow::bail!("untrusted primary"),
            &|| false
        )
        .is_err());
        target.assert_aborted();
        assert_eq!(server.part_requests(), 0);
        assert_eq!(std::fs::read_dir(cache.path())?.count(), 0);
    }
    Ok(())
}
