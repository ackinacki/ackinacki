use std::io::Write;

use salvo::test::ResponseExt;
use salvo::test::TestClient;
use snapshot_storage::SnapshotResources;

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
    let service = service(directory.path());
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

fn service(directory: &std::path::Path) -> Service {
    Service::new(
        Router::new()
            .push(
                Router::with_path("v2/storage_latest")
                    .get(super::super::storage_latest::StorageLatestHandler::new(directory.into())),
            )
            .push(
                Router::with_path("v2/storage/{*path}")
                    .get(StorageHandler::new(directory.into()))
                    .head(StorageHandler::new(directory.into())),
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
    let service = service(directory.path());
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
    let service = service(directory.path());
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
    let service = service(directory.path());
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
    let name = hex::encode([7; 32]);
    std::fs::write(directory.path().join(&name), b"legacy")?;
    std::fs::write(directory.path().join(format!("{name}.header")), b"old header")?;
    let service = service(directory.path());
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
