use std::collections::HashMap;

use super::*;
use crate::repository::repository_impl::snapshot_http_import::tests::Server;

fn limits() -> BlobDownloadLimits {
    BlobDownloadLimits {
        rounds: 2,
        connect_timeout: Duration::from_secs(1),
        request_timeout: Duration::from_secs(20),
        retry_delay: Duration::ZERO,
        deadline: Instant::now() + Duration::from_secs(30),
    }
}

fn body() -> anyhow::Result<Vec<u8>> {
    // No VM/body allocations are needed for a compressed-byte transport test.
    let mut writer = chunks::ChunkWriter::new(Vec::new())?;
    writer.write_all(&vec![57; CHUNK_BYTES * 3 + CHUNK_BYTES / 4])?;
    Ok(writer.finish()?.0)
}

fn server(body: Vec<u8>, partial: bool, gated: bool) -> Server {
    let id = hex::encode([42; 32]);
    Server::new(
        HashMap::from([(id.clone(), body)]),
        id.clone(),
        gated.then(|| id.clone()),
        None,
        partial.then_some(id),
    )
}

async fn attempt(
    worker: &PartitionDownloader,
    source: Option<url::Url>,
    state: Option<PartitionDownloadState>,
    cancel: watch::Receiver<bool>,
) -> PartitionDownloadResponse {
    let (reply, mut replies) = mpsc::channel(1);
    worker
        .run(PartitionDownloaderCommand::Download {
            task_id: 7,
            blob_id: [42; 32],
            source,
            state,
            retained_bytes: 0,
            cancel,
            admission: None,
            reply,
        })
        .await;
    let response = replies.recv().await.expect("one terminal response");
    assert_eq!(response.task_id, 7);
    assert!(replies.recv().await.is_none(), "there must be no intermediate progress responses");
    response
}

fn url(server: &Server) -> url::Url {
    server.base.join(&hex::encode([42; 32])).unwrap()
}

/// Serve one resumed payload, independently controlling header latency and body
/// pacing. Send one whole chunk immediately so failures must retain new progress.
async fn paced_payload(
    bytes: Vec<u8>,
    header_delay: Duration,
    pause: Duration,
    portion: usize,
) -> anyhow::Result<(url::Url, tokio::task::JoinHandle<anyhow::Result<()>>)> {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let source = url::Url::parse(&format!("http://{}/payload", listener.local_addr()?))?;
    let map = chunks::read_footer(&mut std::io::Cursor::new(&bytes))?;
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(stream.read_u8().await?);
        }
        let start = 4 + CHUNK_BYTES;
        let end = map.offset as usize;
        assert!(String::from_utf8_lossy(&request)
            .to_ascii_lowercase()
            .contains(&format!("range: bytes={start}-{}", end - 1)));
        tokio::time::sleep(header_delay).await;
        stream
            .write_all(
                format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{}/{}\r\nETag: {}\r\nConnection: close\r\n\r\n",
                    end - start, end - 1, bytes.len(), map.etag()
                )
                .as_bytes(),
            )
            .await?;
        stream.write_all(&bytes[start..start + CHUNK_BYTES]).await?;
        for piece in bytes[start + CHUNK_BYTES..end].chunks(portion) {
            tokio::time::sleep(pause).await;
            stream.write_all(piece).await?;
        }
        Ok(())
    });
    Ok((source, task))
}

async fn interrupted_prefix(
    worker: &PartitionDownloader,
    bytes: Vec<u8>,
    cancel: watch::Receiver<bool>,
) -> PartitionDownloadState {
    let interrupted = server(bytes, true, false);
    let response = attempt(worker, Some(url(&interrupted)), None, cancel).await;
    assert!(matches!(response.result, Err(TransferError::Network(_))));
    let state = response.state.unwrap();
    assert_eq!(state.verified_chunks, 1);
    state
}

#[tokio::test]
async fn snapshot_partition_download_rejects_single_blob_larger_than_cache() -> anyhow::Result<()> {
    let bytes = body()?;
    let server = server(bytes.clone(), false, false);
    let directory = tempfile::tempdir()?;
    let _cache = DownloadCache::open(directory.path(), Some(bytes.len() as u64 - 1))?;
    let worker = PartitionDownloader::new(directory.path(), limits())?;
    let (_cancel, cancel) = watch::channel(false);
    let response = attempt(&worker, Some(url(&server)), None, cancel).await;
    assert!(
        matches!(response.result, Err(TransferError::Local(ref error))
        if error.to_string().contains("snapshot blob exceeds download cache budget")),
        "{:?}",
        response.result
    );
    assert_eq!(response.bytes_received, 0, "oversized blobs must be rejected at HEAD");
    assert!(server.raw_requests().iter().all(|request| request.starts_with("HEAD")));
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_body_can_outlive_header_timeout() -> anyhow::Result<()> {
    let cache = tempfile::tempdir()?;
    let mut worker = PartitionDownloader::new(cache.path(), limits())?;
    let (_cancel, cancel) = watch::channel(false);
    let bytes = body()?;
    let state = interrupted_prefix(&worker, bytes.clone(), cancel.clone()).await;
    // Use real sockets and a small header timeout instead of making this test
    // download for the runtime's 120 seconds. The body lasts well beyond it.
    worker.limits.request_timeout = Duration::from_millis(200);
    let worker = PartitionDownloader::new(cache.path(), worker.limits)?;
    let (source, serving) =
        paced_payload(bytes, Duration::ZERO, Duration::from_millis(10), 16 * 1024).await?;
    let response = attempt(&worker, Some(source), Some(state), cancel).await;
    serving.await??;
    assert!(response.elapsed > worker.limits.request_timeout);
    assert!(response.result.is_ok(), "{:?}", response.result);
    let mut state = response.state.unwrap();
    assert!(state.complete);
    chunks::verify_file(state.file_mut(), &|| false)?;
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_header_timeout_still_applies() -> anyhow::Result<()> {
    let cache = tempfile::tempdir()?;
    let mut worker = PartitionDownloader::new(cache.path(), limits())?;
    let (_cancel, cancel) = watch::channel(false);
    let bytes = body()?;
    let state = interrupted_prefix(&worker, bytes.clone(), cancel.clone()).await;
    worker.limits.request_timeout = Duration::from_millis(100);
    let worker = PartitionDownloader::new(cache.path(), worker.limits)?;
    let (source, serving) =
        paced_payload(bytes, Duration::from_secs(2), Duration::ZERO, CHUNK_BYTES).await?;
    let response = attempt(&worker, Some(source), Some(state), cancel).await;
    serving.abort();
    assert!(matches!(response.result, Err(TransferError::Network(ref error))
        if error.to_string().contains("headers timed out")));
    assert_eq!(response.state.unwrap().verified_chunks, 1);
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_idle_preserves_new_verified_prefix() -> anyhow::Result<()> {
    let cache = tempfile::tempdir()?;
    let mut download_limits = limits();
    download_limits.deadline = Instant::now() + Duration::from_secs(60);
    let worker = PartitionDownloader::new(cache.path(), download_limits)?;
    let (_cancel, cancel) = watch::channel(false);
    let bytes = body()?;
    let state = interrupted_prefix(&worker, bytes.clone(), cancel.clone()).await;
    let (source, serving) =
        paced_payload(bytes.clone(), Duration::ZERO, READ_IDLE_TIMEOUT * 2, 1024).await?;
    let response = attempt(&worker, Some(source), Some(state), cancel.clone()).await;
    serving.abort();
    assert!(matches!(response.result, Err(TransferError::Network(ref error))
        if error.to_string().contains("stalled")));
    assert!(response.elapsed >= READ_IDLE_TIMEOUT);
    let state = response.state.unwrap();
    assert_eq!(state.verified_chunks, 2);
    let good = server(bytes, false, false);
    let resumed = attempt(&worker, Some(url(&good)), Some(state), cancel).await;
    assert!(resumed.result.is_ok(), "{:?}", resumed.result);
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_low_rate_preserves_new_verified_prefix() -> anyhow::Result<()>
{
    let cache = tempfile::tempdir()?;
    let worker = PartitionDownloader::new(cache.path(), limits())?;
    let (_cancel, cancel) = watch::channel(false);
    let bytes = body()?;
    let state = interrupted_prefix(&worker, bytes.clone(), cancel.clone()).await;
    let (source, serving) =
        paced_payload(bytes.clone(), Duration::ZERO, Duration::from_millis(500), 1024).await?;
    let response = attempt(&worker, Some(source), Some(state), cancel.clone()).await;
    serving.abort();
    assert!(matches!(response.result, Err(TransferError::Network(ref error))
        if error.to_string().contains("minimum transfer rate")));
    let state = response.state.unwrap();
    assert_eq!(state.verified_chunks, 2);
    let good = server(bytes, false, false);
    let resumed = attempt(&worker, Some(url(&good)), Some(state), cancel).await;
    assert!(resumed.result.is_ok(), "{:?}", resumed.result);
    Ok(())
}

#[test]
fn snapshot_partition_download_proxy_is_acceptance_only() -> anyhow::Result<()> {
    let proxy = server(body()?, false, false);
    // Environment is changed only in a child process. Other HTTP tests run
    // concurrently and must never inherit a temporary proxy configuration.
    let result = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "repository::repository_impl::thread_snapshot_import::partition_downloader::tests::snapshot_partition_download_proxy_child",
            "--ignored",
            "--nocapture",
        ])
        .env("TEST_SNAPSHOT_DOWNLOAD_PROXY", proxy.base.origin().ascii_serialization())
        .output()?;
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    if cfg!(feature = "test_upgrade") {
        assert!(proxy.raw_requests().iter().any(|request| request.starts_with("HEAD")));
        assert!(proxy.payloads_started() > 0);
    } else {
        assert!(proxy.raw_requests().is_empty(), "ordinary builds must ignore the test proxy");
    }
    Ok(())
}

#[tokio::test]
#[ignore = "invoked by snapshot_partition_download_proxy_is_acceptance_only in an isolated process"]
async fn snapshot_partition_download_proxy_child() -> anyhow::Result<()> {
    let cache = tempfile::tempdir()?;
    let worker = PartitionDownloader::new(cache.path(), limits())?;
    let (_cancel, cancel) = watch::channel(false);
    // A non-listening peer address succeeds only via the explicit test proxy.
    let source = url::Url::parse(&format!("http://127.0.0.1:1/{}", hex::encode([42; 32])))?;
    let response = attempt(&worker, Some(source), None, cancel).await;
    if cfg!(feature = "test_upgrade") {
        assert!(response.result.is_ok(), "{:?}", response.result);
        let mut state = response.state.unwrap();
        assert!(state.complete);
        chunks::verify_file(state.file_mut(), &|| false)?;
    } else {
        assert!(matches!(response.result, Err(TransferError::Network(_))));
    }
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_resume_retains_verified_chunks_across_sources(
) -> anyhow::Result<()> {
    let bytes = body()?;
    let interrupted = server(bytes.clone(), true, false);
    let good = server(bytes, false, false);
    let cache = tempfile::tempdir()?;
    let worker = PartitionDownloader::new(cache.path(), limits())?;
    let (_cancel, cancel) = watch::channel(false);
    let first = attempt(&worker, Some(url(&interrupted)), None, cancel.clone()).await;
    assert!(matches!(first.result, Err(TransferError::Network(_))));
    let state = first.state.expect("resume state survives disconnect");
    assert_eq!(state.verified_chunks, 1);
    assert!(!state.complete);
    let second = attempt(&worker, Some(url(&good)), Some(state), cancel).await;
    assert!(second.result.is_ok(), "{:?}", second.result);
    let mut state = second.state.unwrap();
    assert!(state.complete);
    assert_eq!(state.verified_chunks, 4);
    chunks::verify_file(state.file_mut(), &|| false)?;
    assert!(good.raw_requests().iter().any(|request| request
        .to_ascii_lowercase()
        .contains(&format!("range: bytes={}-", 4 + CHUNK_BYTES))));
    assert!(
        !good.raw_requests().iter().any(|request| request.starts_with("HEAD")),
        "resume already knows the map"
    );
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_changed_etag_preserves_original_prefix() -> anyhow::Result<()>
{
    let server = server(body()?, true, false);
    let cache = tempfile::tempdir()?;
    let worker = PartitionDownloader::new(cache.path(), limits())?;
    let (_cancel, cancel) = watch::channel(false);
    let first = attempt(&worker, Some(url(&server)), None, cancel.clone()).await;
    let state = first.state.unwrap();
    let original = state.etag.clone();
    server.override_etag(Some(format!("\"{}\"", "11".repeat(32))));
    let changed = attempt(&worker, Some(url(&server)), Some(state), cancel.clone()).await;
    assert!(matches!(changed.result, Err(TransferError::EtagChanged(_))));
    let state = changed.state.unwrap();
    assert_eq!(state.etag, original);
    assert_eq!(state.verified_chunks, 1);
    server.override_etag(None);
    let resumed = attempt(&worker, Some(url(&server)), Some(state), cancel).await;
    assert!(resumed.result.is_ok(), "{:?}", resumed.result);
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_404_does_not_attribute_saved_etag_to_new_source(
) -> anyhow::Result<()> {
    let server = server(body()?, true, false);
    let cache = tempfile::tempdir()?;
    let worker = PartitionDownloader::new(cache.path(), limits())?;
    let (_cancel, cancel) = watch::channel(false);
    let first = attempt(&worker, Some(url(&server)), None, cancel.clone()).await;
    assert!(first.source_etag.is_some());
    let state = first.state.unwrap();
    let original = state.etag.clone();
    let absent = server.base.join("missing")?;
    let failed = attempt(&worker, Some(absent), Some(state), cancel).await;
    assert!(matches!(failed.result, Err(TransferError::NotPublished)));
    assert!(failed.source_etag.is_none(), "404 must not bind a source to somebody else's ETag");
    assert_eq!(failed.state.unwrap().etag, original);
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_bad_chunk_never_advances_verified_prefix() -> anyhow::Result<()>
{
    let bytes = body()?;
    let mut corrupt = bytes.clone();
    corrupt[4 + CHUNK_BYTES + 15] ^= 1;
    let bad = server(corrupt, false, false);
    let good = server(bytes, false, false);
    let cache = tempfile::tempdir()?;
    let worker = PartitionDownloader::new(cache.path(), limits())?;
    let (_cancel, cancel) = watch::channel(false);
    let first = attempt(&worker, Some(url(&bad)), None, cancel.clone()).await;
    assert!(matches!(first.result, Err(TransferError::Invalid(_))));
    let state = first.state.unwrap();
    assert_eq!(state.verified_chunks, 1);
    let resumed = attempt(&worker, Some(url(&good)), Some(state), cancel).await;
    assert!(resumed.result.is_ok(), "{:?}", resumed.result);
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_bad_map_is_rejected_before_payload() -> anyhow::Result<()> {
    let mut bytes = body()?;
    let footer = chunks::read_footer(&mut std::io::Cursor::new(&bytes))?;
    bytes[footer.offset as usize + 12] ^= 1;
    let server = server(bytes, false, false);
    let cache = tempfile::tempdir()?;
    let worker = PartitionDownloader::new(cache.path(), limits())?;
    let (_cancel, cancel) = watch::channel(false);
    let response = attempt(&worker, Some(url(&server)), None, cancel).await;
    assert!(matches!(response.result, Err(TransferError::Invalid(_))));
    assert_eq!(response.state.unwrap().verified_chunks, 0);
    assert_eq!(server.payloads_started(), 0);
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_cancellation_interrupts_stalled_http() -> anyhow::Result<()> {
    let server = server(body()?, false, true);
    let source = url(&server);
    let cache = tempfile::tempdir()?;
    let worker = PartitionDownloader::new(cache.path(), limits())?;
    let (cancel_tx, cancel) = watch::channel(false);
    let task = tokio::spawn(async move { attempt(&worker, Some(source), None, cancel).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while server.payloads_started() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    cancel_tx.send(true)?;
    let response = tokio::time::timeout(Duration::from_secs(1), task).await??;
    assert!(matches!(response.result, Err(TransferError::Cancelled)));
    let state = response.state.unwrap();
    assert_eq!(state.verified_chunks, 0);
    server.release_payload();
    drop(state);
    assert_eq!(std::fs::read_dir(cache.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_cache_is_private_until_accepted_and_then_reusable(
) -> anyhow::Result<()> {
    let server = server(body()?, false, false);
    let cache = tempfile::tempdir()?;
    let worker = PartitionDownloader::new(cache.path(), limits())?;
    let (_cancel, cancel) = watch::channel(false);
    let response = attempt(&worker, Some(url(&server)), None, cancel.clone()).await;
    assert!(response.result.is_ok(), "{:?}", response.result);
    let state = response.state.unwrap();
    let destination = cache.path().join(hex::encode([42; 32]));
    assert!(!destination.exists());
    tokio::task::spawn_blocking(move || state.accept()).await??;
    assert!(destination.exists());
    let requests = server.raw_requests().len();
    let cached = attempt(&worker, None, None, cancel).await;
    assert!(cached.result.is_ok(), "{:?}", cached.result);
    assert_eq!(server.raw_requests().len(), requests);
    assert!(cached.state.unwrap().complete);
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_no_source_and_precancelled_attempts_return_once(
) -> anyhow::Result<()> {
    let cache = tempfile::tempdir()?;
    let worker = PartitionDownloader::new(cache.path(), limits())?;
    let (cancel_tx, cancel) = watch::channel(false);
    let response = attempt(&worker, None, None, cancel.clone()).await;
    assert!(matches!(response.result, Err(TransferError::CacheMiss)));
    assert!(response.state.is_some());
    drop(response);
    cancel_tx.send(true)?;
    let response = attempt(&worker, None, None, cancel).await;
    assert!(response.result.is_err());
    assert_eq!(std::fs::read_dir(cache.path())?.count(), 0);
    Ok(())
}

#[tokio::test]
async fn snapshot_partition_download_cancel_interrupts_disk_admission_wait() -> anyhow::Result<()> {
    let cache = tempfile::tempdir()?;
    let worker = PartitionDownloader::new(cache.path(), limits())?;
    let (_previous, preceding) = oneshot::channel();
    let (admission, _following) = DownloadAdmission::next(Some(preceding));
    let (cancel_tx, cancel) = watch::channel(false);
    let (reply, mut replies) = mpsc::channel(1);
    let task = tokio::spawn(async move {
        worker
            .run(PartitionDownloaderCommand::Download {
                task_id: 3,
                blob_id: [42; 32],
                source: None,
                state: None,
                retained_bytes: 0,
                cancel,
                admission: Some(admission),
                reply,
            })
            .await;
    });
    tokio::task::yield_now().await;
    cancel_tx.send(true)?;
    let response = tokio::time::timeout(Duration::from_secs(1), replies.recv()).await?.unwrap();
    assert!(matches!(response.result, Err(TransferError::Cancelled)));
    assert!(response.state.is_none(), "no state existed before disk admission");
    task.await?;
    assert_eq!(std::fs::read_dir(cache.path())?.count(), 0);
    Ok(())
}
