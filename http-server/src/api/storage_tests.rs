use std::io::Write;

use salvo::test::ResponseExt;
use salvo::test::TestClient;
use snapshot_storage::SnapshotResources;

#[handler]
async fn test_health(res: &mut Response) {
    res.render(Text::Plain("healthy"));
}

#[test]
fn snapshot_reads_finish_with_one_blocking_worker() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let name = hex::encode([21; 32]);
    std::fs::write(directory.path().join(&name), b"0123456789")?;
    let service = service(&store);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            let url = format!("http://localhost/v2/storage/{name}");
            let read = || async {
                let mut response = TestClient::get(&url)
                    .add_header("range", "bytes=4-", true)
                    .send(&service)
                    .await;
                assert_eq!(response.status_code, Some(StatusCode::PARTIAL_CONTENT));
                response.take_bytes(None).await
            };
            let (first, second) = tokio::join!(read(), read());
            Ok::<_, anyhow::Error>((first?, second?))
        })
        .await
    });
    // A regression can strand spawn_blocking tasks. Do not hang the entire
    // test process while dropping the intentionally tiny runtime on failure.
    runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    let (first, second) = result??;
    assert_eq!(first.as_ref(), b"456789");
    assert_eq!(second.as_ref(), b"456789");
    Ok(())
}

#[test]
fn latest_waiting_for_gc_does_not_block_unrelated_async_requests() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let name = hex::encode([22; 32]);
    std::fs::write(directory.path().join(&name), b"snapshot")?;
    std::fs::write(directory.path().join(format!("{name}.header")), b"header")?;
    let service = Service::new(
        Router::new()
            .push(
                Router::with_path("latest")
                    .get(super::super::storage_latest::StorageLatestHandler::new(store.clone())),
            )
            .push(Router::with_path("health").get(test_health)),
    );
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let resources = store.clone();
    let holder = std::thread::Builder::new().name("snapshot-test-gc".into()).spawn(move || {
        resources.with_read(|| {
            locked_tx.send(())?;
            // Bound the simulated GC so the old blocking implementation fails
            // instead of wedging this test's single async worker forever.
            release_rx.recv_timeout(std::time::Duration::from_secs(2))?;
            Ok(())
        })
    })?;
    locked_rx.recv_timeout(std::time::Duration::from_secs(2))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            tokio::join!(TestClient::get("http://localhost/latest").send(&service), async {
                tokio::task::yield_now().await;
                let mut response = TestClient::get("http://localhost/health").send(&service).await;
                let healthy = response.take_string().await?;
                release_tx.send(())?;
                Ok::<_, anyhow::Error>(healthy)
            })
        })
        .await
    });
    runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    let holder_result = holder.join().expect("GC holder panicked");
    let (response, healthy) = result?;
    assert_eq!(healthy?, "healthy");
    holder_result?;
    assert_eq!(response.status_code, Some(StatusCode::TEMPORARY_REDIRECT));
    Ok(())
}

#[tokio::test]
async fn snapshot_dates_and_suffix_ranges_preserve_conditional_behavior() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let name = hex::encode([23; 32]);
    std::fs::write(directory.path().join(&name), b"0123456789")?;
    let service = service(&store);
    let url = format!("http://localhost/v2/storage/{name}");
    let response = TestClient::get(&url).send(&service).await;
    let modified = response.headers().get("last-modified").unwrap().to_str()?;
    let response =
        TestClient::get(&url).add_header("if-modified-since", modified, true).send(&service).await;
    assert_eq!(response.status_code, Some(StatusCode::NOT_MODIFIED));
    let response = TestClient::get(&url)
        .add_header("if-unmodified-since", "Thu, 01 Jan 1970 00:00:00 GMT", true)
        .send(&service)
        .await;
    assert_eq!(response.status_code, Some(StatusCode::PRECONDITION_FAILED));
    // Conditional responses take precedence over both valid and unsatisfiable ranges.
    for range in ["bytes=4-", "bytes=10-"] {
        let mut response = TestClient::get(&url)
            .add_header("if-modified-since", modified, true)
            .add_header("range", range, true)
            .send(&service)
            .await;
        assert_eq!(response.status_code, Some(StatusCode::NOT_MODIFIED));
        assert!(!response.headers().contains_key("content-range"));
        assert!(response.take_bytes(None).await?.is_empty());
        let response = TestClient::get(&url)
            .add_header("if-unmodified-since", "Thu, 01 Jan 1970 00:00:00 GMT", true)
            .add_header("range", range, true)
            .send(&service)
            .await;
        assert_eq!(response.status_code, Some(StatusCode::PRECONDITION_FAILED));
        assert!(!response.headers().contains_key("content-range"));
    }
    let mut response =
        TestClient::get(&url).add_header("range", "bytes=-3", true).send(&service).await;
    assert_eq!(response.status_code, Some(StatusCode::PARTIAL_CONTENT));
    assert_eq!(response.headers().get("content-range").unwrap(), "bytes 7-9/10");
    assert_eq!(response.take_bytes(None).await?.as_ref(), b"789");
    let response = TestClient::get(&url).add_header("range", "bytes=-0", true).send(&service).await;
    assert_eq!(response.status_code, Some(StatusCode::RANGE_NOT_SATISFIABLE));
    let response = TestClient::get(&url)
        .add_header("if-none-match", "\"different\"", true)
        .add_header("if-modified-since", modified, true)
        .send(&service)
        .await;
    assert_eq!(response.status_code, Some(StatusCode::OK));
    Ok(())
}

#[tokio::test]
async fn snapshot_scoped_partitions_are_served_but_symlinked_directories_are_not(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let publication = store.begin([1; 32], [[2; 32]])?;
    let name = publication.publish_partition_checked(
        [2; 32],
        snapshot_storage::PartitionKind::Accounts,
        |writer| {
            writer.write_all(b"partition")?;
            Ok(())
        },
        &|| false,
    )?;
    let service = service(&store);
    let url = format!("http://localhost/v2/storage/{name}");
    let mut response = TestClient::get(&url).send(&service).await;
    assert_eq!(response.status_code, Some(StatusCode::OK));
    assert_eq!(response.take_bytes(None).await?.as_ref(), b"partition");
    let parent = directory.path().join(format!("{}.partitions", hex::encode([1; 32])));
    let moved = directory.path().join("private-moved");
    std::fs::rename(&parent, &moved)?;
    std::os::unix::fs::symlink(&moved, &parent)?;
    assert_eq!(TestClient::get(&url).send(&service).await.status_code, Some(StatusCode::NOT_FOUND));
    Ok(())
}

use super::*;

fn service(store: &SnapshotResources) -> Service {
    Service::new(
        Router::new()
            .push(
                Router::with_path("v2/storage_latest")
                    .get(super::super::storage_latest::StorageLatestHandler::new(store.clone())),
            )
            .push(
                Router::with_path("v2/storage/{*path}")
                    .get(StorageHandler::new(store.clone()))
                    .head(StorageHandler::new(store.clone())),
            ),
    )
}

#[tokio::test]
async fn chunked_storage_serves_map_metadata_on_get_head_and_range() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let attempt = store.begin([3; 32], [])?;
    let mut expected = None;
    attempt.publish_checked(
        [3; 32],
        |output| {
            let mut writer = snapshot_storage::chunks::ChunkWriter::new(output)?;
            writer.write_all(b"compressed payload")?;
            let (_, map) = writer.finish()?;
            expected = Some(map.clone());
            Ok(())
        },
        &|| false,
    )?;
    let map = expected.unwrap();
    let service = service(&store);
    let url = format!("http://localhost/v2/storage/{}", hex::encode([3; 32]));
    for (head, range) in [(false, false), (true, false), (false, true)] {
        let request = if head { TestClient::head(&url) } else { TestClient::get(&url) };
        let request = if range {
            request.add_header("range", "bytes=4-", true).add_header("if-range", map.etag(), true)
        } else {
            request
        };
        let mut response = request.send(&service).await;
        assert_eq!(
            response.status_code,
            Some(if range { StatusCode::PARTIAL_CONTENT } else { StatusCode::OK })
        );
        assert_eq!(response.headers().get("etag").unwrap(), map.etag().as_str());
        assert_eq!(
            response.headers().get(snapshot_storage::chunks::MAP_OFFSET_HEADER).unwrap(),
            map.offset.to_string().as_str()
        );
        assert_eq!(
            response.headers().get(snapshot_storage::chunks::MAP_LENGTH_HEADER).unwrap(),
            map.length.to_string().as_str()
        );
        let body = response.take_bytes(None).await?;
        if head {
            assert!(body.is_empty());
        } else if !range {
            assert_eq!(
                snapshot_storage::chunks::verify_file(&mut std::io::Cursor::new(body), &|| false)?,
                map
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn managed_storage_strong_etag_range_if_range_and_head() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let attempt = store.begin([1; 32], [])?;
    attempt.publish([1; 32], |writer| {
        writer.write_all(b"0123456789")?;
        Ok(())
    })?;
    attempt.publish_header(b"header")?;
    let service = service(&store);
    let url = format!("http://localhost/v2/storage/{}", hex::encode([1; 32]));
    let response = TestClient::get(&url).send(&service).await;
    assert_eq!(response.status_code, Some(StatusCode::OK));
    let etag = response.headers().get("etag").unwrap().to_str()?.to_owned();
    assert!(!etag.starts_with("W/"));
    for (validator, expected) in [
        (etag.clone(), StatusCode::PARTIAL_CONTENT),
        ("\"different\"".into(), StatusCode::OK),
        (format!("W/{etag}"), StatusCode::OK),
    ] {
        let mut response = TestClient::get(&url)
            .add_header("range", "bytes=4-", true)
            .add_header("if-range", validator, true)
            .send(&service)
            .await;
        assert_eq!(response.status_code, Some(expected));
        assert_eq!(response.headers().get("etag").unwrap(), etag.as_str());
        assert_eq!(
            response.take_bytes(None).await?.as_ref(),
            if expected == StatusCode::PARTIAL_CONTENT {
                b"456789".as_slice()
            } else {
                b"0123456789".as_slice()
            }
        );
    }
    let mut response = TestClient::head(&url).send(&service).await;
    assert_eq!(response.headers().get("content-length").unwrap(), "10");
    assert!(response.take_bytes(None).await?.is_empty());
    let response =
        TestClient::get(&url).add_header("range", "bytes=10-", true).send(&service).await;
    assert_eq!(response.status_code, Some(StatusCode::RANGE_NOT_SATISFIABLE));
    assert_eq!(response.headers().get("content-range").unwrap(), "bytes */10");
    let response =
        TestClient::get(&url).add_header("if-none-match", &etag, true).send(&service).await;
    assert_eq!(response.status_code, Some(StatusCode::NOT_MODIFIED));
    // An open HTTP stream owns its inode even if explicit GC retires the URL.
    let mut in_flight = TestClient::get(&url).send(&service).await;
    drop(attempt);
    store.retain(&Default::default())?;
    assert_eq!(in_flight.take_bytes(None).await?.as_ref(), b"0123456789");
    let attempt = store.begin([1; 32], [])?;
    attempt.publish([1; 32], |writer| {
        writer.write_all(b"abcdefghij")?;
        Ok(())
    })?;
    let mut response = TestClient::get(&url)
        .add_header("range", "bytes=4-", true)
        .add_header("if-range", &etag, true)
        .send(&service)
        .await;
    assert_eq!(response.status_code, Some(StatusCode::OK));
    assert_ne!(response.headers().get("etag").unwrap(), etag.as_str());
    assert_eq!(response.take_bytes(None).await?.as_ref(), b"abcdefghij");
    Ok(())
}

#[tokio::test]
async fn discovery_announces_primary_before_partitions_and_never_latest_partition(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let attempt = store.begin([1; 32], [[2; 32]])?;
    attempt.publish([1; 32], |writer| {
        writer.write_all(b"primary")?;
        Ok(())
    })?;
    let service = service(&store);
    let mut listing = TestClient::get("http://localhost/v2/storage/").send(&service).await;
    assert!(!listing.take_string().await?.contains(&hex::encode([1; 32])));
    attempt.publish_header(b"header")?;
    let response = TestClient::get("http://localhost/v2/storage_latest").send(&service).await;
    assert_eq!(
        response.headers().get("location").unwrap(),
        format!("/v2/storage/{}", hex::encode([1; 32])).as_str()
    );
    attempt.publish([2; 32], |writer| {
        writer.write_all(b"partition")?;
        Ok(())
    })?;
    let mut listing = TestClient::get("http://localhost/v2/storage/").send(&service).await;
    let listing = listing.take_string().await?;
    assert!(listing.contains(&format!("{}.header", hex::encode([1; 32]))));
    assert!(!listing.contains(&hex::encode([2; 32])));
    assert!(!listing.contains(".snapshot-resources"));
    for path in [".snapshot-resources/lock", "../Cargo.toml", "random", "01"] {
        let response =
            TestClient::get(format!("http://localhost/v2/storage/{path}")).send(&service).await;
        assert_eq!(response.status_code, Some(StatusCode::NOT_FOUND));
    }
    Ok(())
}

#[tokio::test]
async fn legacy_primary_and_header_remain_discoverable_and_range_readable() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let name = hex::encode([7; 32]);
    std::fs::write(directory.path().join(&name), b"legacy")?;
    std::fs::write(directory.path().join(format!("{name}.header")), b"old header")?;
    let service = service(&store);
    let mut response = TestClient::get(format!("http://localhost/v2/storage/{name}"))
        .add_header("range", "bytes=1-", true)
        .send(&service)
        .await;
    assert_eq!(response.status_code, Some(StatusCode::PARTIAL_CONTENT));
    assert_eq!(response.take_string().await?, "egacy");
    let response = TestClient::get("http://localhost/v2/storage_latest").send(&service).await;
    assert_eq!(response.headers().get("location").unwrap(), format!("/v2/storage/{name}").as_str());
    Ok(())
}

#[tokio::test]
async fn gc_can_unlink_a_resource_after_http_opens_it() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let publication = store.begin([8; 32], [])?;
    publication.publish([8; 32], |writer| {
        writer.write_all(&vec![b'x'; 128 * 1024])?;
        Ok(())
    })?;
    drop(publication);
    let service = service(&store);
    let name = hex::encode([8; 32]);
    let mut response =
        TestClient::get(format!("http://localhost/v2/storage/{name}")).send(&service).await;
    assert_eq!(response.status_code, Some(StatusCode::OK));
    store.retain(&Default::default())?;
    assert!(!directory.path().join(name).exists());
    assert_eq!(response.take_bytes(None).await?.as_ref(), vec![b'x'; 128 * 1024]);
    Ok(())
}
