use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;

use super::*;

struct Server {
    url: Url,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Server {
    fn new(responses: Vec<Vec<u8>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = Url::parse(&format!("http://{}/blob", listener.local_addr().unwrap())).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let handle = std::thread::Builder::new().name("blob-test-http".into()).spawn(move || {
            for response in responses {
                let until = Instant::now() + Duration::from_secs(5);
                let mut stream = loop {
                    if stopping.load(Ordering::Relaxed) || Instant::now() >= until {
                        return;
                    }
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(2))
                        }
                        Err(error) => panic!("test HTTP accept: {error}"),
                    }
                };
                // macOS inherits O_NONBLOCK from the accepting listener.
                stream.set_nonblocking(false).unwrap();
                stream.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
                let request = read_request(&mut stream);
                recorded.lock().unwrap().push(request);
                let _ = stream.write_all(&response);
            }
        });
        Self { url, requests, stop, handle: Some(handle.unwrap()) }
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().unwrap().join().unwrap();
    }
}

fn read_request(stream: &mut TcpStream) -> String {
    use std::io::BufRead;
    let mut bytes = Vec::new();
    let mut reader = std::io::BufReader::new(stream);
    while !bytes.ends_with(b"\r\n\r\n") {
        assert!(
            reader.read_until(b'\n', &mut bytes).expect("read complete HTTP request") > 0,
            "client closed before the complete HTTP headers: {bytes:?}"
        );
        assert!(bytes.len() < 16_384);
    }
    String::from_utf8(bytes).unwrap()
}

fn response(status: &str, headers: &str, body: &[u8]) -> Vec<u8> {
    let mut bytes = format!("HTTP/1.1 {status}\r\nConnection: close\r\n{headers}\r\n").into_bytes();
    bytes.extend_from_slice(body);
    bytes
}

fn full(body: &[u8]) -> Vec<u8> {
    response("200 OK", &format!("Content-Length: {}\r\n", body.len()), body)
}

fn downloader(rounds: u8) -> VerifiedBlobDownloader {
    VerifiedBlobDownloader::new(BlobDownloadLimits {
        rounds,
        max_bytes: 1024 * 1024,
        connect_timeout: Duration::from_millis(200),
        request_timeout: Duration::from_secs(1),
        retry_delay: Duration::ZERO,
        deadline: Instant::now() + Duration::from_secs(5),
    })
    .unwrap()
}

#[test]
fn chunked_resume_retains_only_complete_payload_chunks() -> anyhow::Result<()> {
    use snapshot_storage::chunks::*;
    let mut writer = ChunkWriter::new(Vec::new())?;
    writer.write_all(&vec![71; CHUNK_BYTES * 2 + 123])?;
    let (blob, map) = writer.finish()?;
    let headers = format!(
        "ETag: {}\r\n{MAP_OFFSET_HEADER}: {}\r\n{MAP_LENGTH_HEADER}: {}\r\n",
        map.etag(),
        map.offset,
        map.length,
    );
    let boundary = 4 + CHUNK_BYTES;
    let first = Server::new(vec![response(
        "200 OK",
        &format!("{headers}Content-Length: {}\r\n", blob.len()),
        &blob[..boundary + 321],
    )]);
    let second = Server::new(vec![response(
        "206 Partial Content",
        &format!(
            "{headers}Content-Length: {}\r\nContent-Range: bytes {boundary}-{}/{}\r\n",
            blob.len() - boundary,
            blob.len() - 1,
            blob.len(),
        ),
        &blob[boundary..],
    )]);
    let download = VerifiedBlobDownloader::new(BlobDownloadLimits {
        max_bytes: 4 * CHUNK_BYTES as u64,
        ..downloader(1).limits
    })?;
    let directory = tempfile::tempdir()?;
    let (mut file, ()) = download.download(
        directory.path(),
        &[42; 32],
        &[first.url.clone(), second.url.clone()],
        ResumePolicy::ChunkedPartition,
        &|| false,
        |_| Ok(()),
    )?;
    assert_eq!(verify_file(&mut file, &|| false)?, map);
    assert!(second.requests()[0]
        .to_ascii_lowercase()
        .contains(&format!("range: bytes={boundary}-\r\n")));
    assert!(second.requests()[0].to_ascii_lowercase().contains("if-range:"));
    assert_eq!(first.requests().len(), 1);
    Ok(())
}

#[test]
fn chunked_corruption_is_rejected_before_consumer_and_retried() -> anyhow::Result<()> {
    use snapshot_storage::chunks::*;
    let mut writer = ChunkWriter::new(Vec::new())?;
    writer.write_all(b"compressed payload")?;
    let (blob, map) = writer.finish()?;
    let mut bad = blob.clone();
    bad[4] ^= 1;
    let first = Server::new(vec![full(&bad)]);
    let second = Server::new(vec![full(&blob)]);
    let directory = tempfile::tempdir()?;
    let mut checks = 0;
    let (_, ()) = downloader(1).download(
        directory.path(),
        &[43; 32],
        &[first.url.clone(), second.url.clone()],
        ResumePolicy::ChunkedPartition,
        &|| false,
        |file| {
            checks += 1;
            assert_eq!(verify_file(file, &|| false)?, map);
            Ok(())
        },
    )?;
    assert_eq!(checks, 1);
    assert!(!second.requests()[0].to_ascii_lowercase().contains("range:"));
    Ok(())
}

fn verify(file: &mut File) -> anyhow::Result<Vec<u8>> {
    let mut body = Vec::new();
    file.read_to_end(&mut body)?;
    anyhow::ensure!(body == b"abcdefghij", "content root mismatch");
    Ok(body)
}

#[test]
fn cache_hits_do_not_reserve_transfer_maximum_or_evict_other_cached_blobs() -> anyhow::Result<()> {
    use super::super::download_cache::DownloadCache;
    let directory = tempfile::tempdir()?;
    for index in [1, 2, 3] {
        std::fs::write(directory.path().join(hex::encode([index; 32])), b"abcdefghij")?;
    }
    let _cache = DownloadCache::open(directory.path(), Some(30))?;
    let download =
        VerifiedBlobDownloader::new(BlobDownloadLimits { max_bytes: 30, ..downloader(1).limits })?;
    for index in [1, 2, 3] {
        let (_, bytes) = download.download(
            directory.path(),
            &[index; 32],
            &[],
            ResumePolicy::CanonicalPartition,
            &|| false,
            verify,
        )?;
        assert_eq!(bytes, b"abcdefghij");
        assert_eq!(std::fs::read_dir(directory.path())?.count(), 3);
    }
    Ok(())
}

#[test]
fn primary_transport_uses_the_same_budget_and_releases_consumed_files() -> anyhow::Result<()> {
    use super::super::download_blob::download_snapshot_blob;
    use super::super::download_cache::CacheReader;
    use super::super::download_cache::DownloadCache;
    let directory = tempfile::tempdir()?;
    let cache = DownloadCache::open(directory.path(), Some(10))?;
    let server = Server::new(vec![response("200 OK", "Content-Length: 10\r\n", b"abcdefghij")]);
    let id = hex::encode([90; 32]);
    let lease =
        download_snapshot_blob(&cache, &id, std::slice::from_ref(&server.url), 1, None, None)?;
    let mut reader = CacheReader::new(lease)?;
    assert!(cache.acquire(&hex::encode([91; 32]), 1, &|| true).is_err());
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    assert_eq!(bytes, b"abcdefghij");
    let next = cache.acquire(&hex::encode([91; 32]), 10, &|| false)?;
    assert!(!directory.path().join(id).exists());
    drop(next);
    Ok(())
}

#[test]
fn primary_transport_rejects_known_and_unknown_oversized_responses() -> anyhow::Result<()> {
    use super::super::download_blob::download_snapshot_blob;
    use super::super::download_cache::DownloadCache;
    for headers in ["Content-Length: 11\r\n", "Connection: close\r\n"] {
        let directory = tempfile::tempdir()?;
        let cache = DownloadCache::open(directory.path(), Some(10))?;
        let server = Server::new(vec![response("200 OK", headers, b"abcdefghijk")]);
        assert!(download_snapshot_blob(
            &cache,
            &hex::encode([92; 32]),
            std::slice::from_ref(&server.url),
            1,
            None,
            None
        )
        .is_err());
        assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
    }
    Ok(())
}

#[test]
fn resumable_http_cross_source_range_and_verified_cache_reuse() -> anyhow::Result<()> {
    let first = Server::new(vec![response("200 OK", "Content-Length: 10\r\n", b"abcd")]);
    let second = Server::new(vec![response(
        "206 Partial Content",
        "Content-Length: 6\r\nContent-Range: bytes 4-9/10\r\n",
        b"efghij",
    )]);
    let directory = tempfile::tempdir()?;
    let download = downloader(1);
    let (mut file, parsed) = download.download(
        directory.path(),
        &[1; 32],
        &[first.url.clone(), second.url.clone()],
        ResumePolicy::CanonicalPartition,
        &|| false,
        verify,
    )?;
    assert_eq!(parsed, b"abcdefghij");
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    assert_eq!(bytes, parsed);
    assert!(first.requests()[0].to_ascii_lowercase().contains("accept-encoding: identity"));
    assert!(second.requests()[0].to_ascii_lowercase().contains("range: bytes=4-"));
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 1);
    // Verify cached bytes even with no available sources.
    let (_, cached) = download.download(
        directory.path(),
        &[1; 32],
        &[],
        ResumePolicy::CanonicalPartition,
        &|| false,
        verify,
    )?;
    assert_eq!(cached, parsed);
    Ok(())
}

#[test]
fn resumable_http_ignored_range_replaces_prefix_instead_of_appending() -> anyhow::Result<()> {
    let server = Server::new(vec![
        response("200 OK", "Content-Length: 10\r\n", b"abcd"),
        full(b"abcdefghij"),
    ]);
    let directory = tempfile::tempdir()?;
    downloader(2).download(
        directory.path(),
        &[1; 32],
        std::slice::from_ref(&server.url),
        ResumePolicy::CanonicalPartition,
        &|| false,
        verify,
    )?;
    assert!(server.requests()[1].to_ascii_lowercase().contains("range: bytes=4-"));
    assert_eq!(std::fs::read(directory.path().join(hex::encode([1; 32])))?, b"abcdefghij");
    Ok(())
}

#[test]
fn resumable_http_bad_range_and_corrupt_content_restart_cleanly() -> anyhow::Result<()> {
    for bad in [
        response(
            "206 Partial Content",
            "Content-Length: 6\r\nContent-Range: bytes 3-8/10\r\n",
            b"efghij",
        ),
        response(
            "206 Partial Content",
            "Content-Length: 6\r\nContent-Range: bytes 4-9/11\r\n",
            b"efghij",
        ),
        response("206 Partial Content", "Content-Length: 6\r\n", b"efghij"),
        response(
            "206 Partial Content",
            "Content-Length: 6\r\nContent-Range: bytes 4-9/10\r\nContent-Encoding: gzip\r\n",
            b"efghij",
        ),
        response(
            "416 Range Not Satisfiable",
            "Content-Length: 0\r\nContent-Range: bytes */10\r\n",
            b"",
        ),
        response(
            "206 Partial Content",
            "Content-Length: 6\r\nContent-Range: bytes 4-9/10\r\n",
            b"BADBAD",
        ),
    ] {
        let server = Server::new(vec![
            response("200 OK", "Content-Length: 10\r\n", b"abcd"),
            bad,
            full(b"abcdefghij"),
        ]);
        let directory = tempfile::tempdir()?;
        downloader(3).download(
            directory.path(),
            &[1; 32],
            std::slice::from_ref(&server.url),
            ResumePolicy::CanonicalPartition,
            &|| false,
            verify,
        )?;
        let requests = server.requests();
        assert_eq!(requests.len(), 3);
        assert!(!requests[2].to_ascii_lowercase().contains("\r\nrange:"));
    }
    Ok(())
}

#[test]
fn resumable_http_404_keeps_prefix_for_next_source_and_incomplete_ranges_progress(
) -> anyhow::Result<()> {
    let first = Server::new(vec![response("200 OK", "Content-Length: 10\r\n", b"abcd")]);
    let absent = Server::new(vec![response("404 Not Found", "Content-Length: 0\r\n", b"")]);
    let last = Server::new(vec![
        response(
            "206 Partial Content",
            "Content-Length: 3\r\nContent-Range: bytes 4-6/10\r\n",
            b"efg",
        ),
        response(
            "206 Partial Content",
            "Content-Length: 3\r\nContent-Range: bytes 7-9/10\r\n",
            b"hij",
        ),
    ]);
    let directory = tempfile::tempdir()?;
    // Put last twice to request the next (valid but incomplete) range without a
    // second pass through servers whose test response is already exhausted.
    downloader(1).download(
        directory.path(),
        &[1; 32],
        &[first.url.clone(), absent.url.clone(), last.url.clone(), last.url.clone()],
        ResumePolicy::CanonicalPartition,
        &|| false,
        verify,
    )?;
    assert!(absent.requests()[0].to_ascii_lowercase().contains("range: bytes=4-"));
    assert!(last.requests()[1].to_ascii_lowercase().contains("range: bytes=7-"));
    Ok(())
}

#[test]
fn resumable_http_primary_requires_same_source_and_strong_etag() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let same = Server::new(vec![
        response("200 OK", "Content-Length: 10\r\nETag: \"bundle\"\r\n", b"abcd"),
        response(
            "206 Partial Content",
            "Content-Length: 6\r\nContent-Range: bytes 4-9/10\r\nETag: \"bundle\"\r\n",
            b"efghij",
        ),
    ]);
    downloader(2).download(
        directory.path(),
        &[1; 32],
        std::slice::from_ref(&same.url),
        ResumePolicy::SourceBound,
        &|| false,
        verify,
    )?;
    let request = same.requests()[1].to_ascii_lowercase();
    assert!(request.contains("range: bytes=4-"), "requests: {:?}", same.requests());
    assert!(request.contains("if-range: \"bundle\""), "requests: {:?}", same.requests());
    for etag in ["", "ETag: W/\"weak\"\r\n"] {
        let same = Server::new(vec![
            response("200 OK", &format!("Content-Length: 10\r\n{etag}"), b"abcd"),
            full(b"abcdefghij"),
        ]);
        let directory = tempfile::tempdir()?;
        downloader(2).download(
            directory.path(),
            &[1; 32],
            std::slice::from_ref(&same.url),
            ResumePolicy::SourceBound,
            &|| false,
            verify,
        )?;
        assert!(!same.requests()[1].to_ascii_lowercase().contains("\r\nrange:"));
    }
    let first = Server::new(vec![response(
        "200 OK",
        "Content-Length: 10\r\nETag: \"bundle\"\r\n",
        b"abcd",
    )]);
    let second = Server::new(vec![full(b"abcdefghij")]);
    let directory = tempfile::tempdir()?;
    downloader(1).download(
        directory.path(),
        &[1; 32],
        &[first.url.clone(), second.url.clone()],
        ResumePolicy::SourceBound,
        &|| false,
        verify,
    )?;
    assert!(!second.requests()[0].to_ascii_lowercase().contains("\r\nrange:"));
    Ok(())
}

#[test]
fn resumable_http_changed_primary_validator_discards_saved_prefix() -> anyhow::Result<()> {
    let server = Server::new(vec![
        response("200 OK", "Content-Length: 10\r\nETag: \"old\"\r\n", b"abcd"),
        response(
            "206 Partial Content",
            "Content-Length: 6\r\nContent-Range: bytes 4-9/10\r\nETag: \"new\"\r\n",
            b"efghij",
        ),
        full(b"abcdefghij"),
    ]);
    let directory = tempfile::tempdir()?;
    downloader(3).download(
        directory.path(),
        &[1; 32],
        std::slice::from_ref(&server.url),
        ResumePolicy::SourceBound,
        &|| false,
        verify,
    )?;
    assert!(!server.requests()[2].to_ascii_lowercase().contains("\r\nrange:"));
    Ok(())
}

#[test]
fn resumable_http_bad_cache_is_reverified_and_replaced() -> anyhow::Result<()> {
    for cached in [b"corrupt".as_slice(), b""] {
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join(hex::encode([1; 32])), cached)?;
        let server = Server::new(vec![full(b"abcdefghij")]);
        downloader(1).download(
            directory.path(),
            &[1; 32],
            std::slice::from_ref(&server.url),
            ResumePolicy::CanonicalPartition,
            &|| false,
            verify,
        )?;
        assert_eq!(std::fs::read_dir(directory.path())?.count(), 1);
    }
    Ok(())
}

#[test]
fn resumable_http_limits_deadline_cancellation_and_failed_validation_leave_no_files(
) -> anyhow::Result<()> {
    for mode in 0..6 {
        let directory = tempfile::tempdir()?;
        let server = Server::new(vec![if mode == 5 {
            response("200 OK", "", b"abcdefghij")
        } else {
            full(b"abcdefghij")
        }]);
        let mut downloader = downloader(1);
        if mode == 0 || mode == 5 {
            downloader.limits.max_bytes = 5;
        }
        if mode == 1 {
            downloader.limits.deadline = Instant::now();
        }
        let calls = std::cell::Cell::new(0);
        let result = downloader.download(
            directory.path(),
            &[1; 32],
            std::slice::from_ref(&server.url),
            ResumePolicy::CanonicalPartition,
            &|| {
                calls.set(calls.get() + 1);
                mode == 2 || (mode == 3 && calls.get() >= 4)
            },
            |file| {
                if mode == 4 {
                    anyhow::bail!("bad root");
                }
                verify(file)
            },
        );
        assert!(result.is_err(), "mode={mode}");
        assert_eq!(std::fs::read_dir(directory.path())?.count(), 0, "mode={mode}");
    }
    Ok(())
}

#[test]
fn resumable_http_source_plan_and_real_account_codec_return_verified_map() -> anyhow::Result<()> {
    use account_state::read_account_partition_checked;
    use account_state::write_account_partition_checked;

    use crate::node::services::sync::snapshot_compression::write_partition_blob;
    use crate::repository::repository_impl::snapshot_manifest::build_snapshot_manifest;
    use crate::repository::repository_impl::snapshot_publication::tests::Fixture;
    use crate::repository::repository_impl::snapshot_sources::SnapshotSourcePlan;
    let source = Fixture::new()?;
    let part =
        source.state.shard_state.0.durable.map().partition_by_capacity(256).unwrap().remove(0);
    let pin = source.pin()?;
    let blob = write_partition_blob(Vec::new(), |mut writer| {
        write_account_partition_checked(
            source.repo.durable_map_repo(),
            &part.map,
            &mut writer,
            Default::default(),
            &|| false,
        )
    })?;
    drop(pin);
    let cut = blob.len() / 2;
    let server = Server::new(vec![
        response("200 OK", &format!("Content-Length: {}\r\n", blob.len()), &blob[..cut]),
        response(
            "206 Partial Content",
            &format!(
                "Content-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\n",
                blob.len() - cut,
                cut,
                blob.len() - 1,
                blob.len()
            ),
            &blob[cut..],
        ),
    ]);
    let manifest = build_snapshot_manifest(
        source.state.shard_state.0.durable.map(),
        &source.state.messages,
        &source.state.high_priority_messages,
        256,
        1000,
    )?;
    let node = source.snapshot.bk_set().iter().next().unwrap().node_id();
    let plan = SnapshotSourcePlan::new(
        &manifest,
        source.snapshot.bk_set(),
        [(node, server.url.join("/v2/storage/")?)],
    )?;
    let directory = tempfile::tempdir()?;
    let verified = std::cell::Cell::new(0);
    let (_, decoded_map) = downloader(2).download(
        directory.path(),
        &manifest.partition_blob_id(0).unwrap(),
        &plan.partition_sources(0)?,
        ResumePolicy::CanonicalPartition,
        &|| false,
        |file| {
            verified.set(verified.get() + 1);
            let input = snapshot_storage::chunks::PayloadReader::new(file)?;
            let mut payload = zstd::Decoder::with_buffer(input)?.single_frame();
            let map = read_account_partition_checked(
                &mut payload,
                multi_map::MapKeyPath::default(),
                part.map.hash(),
                &|| false,
                |_, _| Ok(()),
            )?;
            payload.finish().verify_trailer()?;
            Ok(map)
        },
    )?;
    assert_eq!(verified.get(), 1, "never decode a partial HTTP transfer");
    assert_eq!(decoded_map.hash(), part.map.hash());
    assert_eq!(decoded_map.capacity(), part.map.capacity());
    assert!(server.requests()[0].contains(&hex::encode(manifest.partition_blob_id(0).unwrap())));
    assert!(server.requests()[1].to_ascii_lowercase().contains(&format!("range: bytes={cut}-")));
    Ok(())
}

#[test]
fn resumable_http_rejects_bad_range_values_without_overflow() {
    for value in [
        "",
        "bytes */10",
        "bytes 0-0/*",
        "bytes 9-4/10",
        "bytes 0-10/10",
        "bytes +4-9/10",
        "bytes 0-18446744073709551615/18446744073709551615",
    ] {
        assert!(parse_range(value).is_err(), "{value}");
    }
    assert_eq!(parse_range("bytes 4-9/10").unwrap(), (4, 9, 10));
}
