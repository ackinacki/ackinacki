use super::*;

#[test]
fn snapshot_partition_directories_share_bodies_and_gc_removes_only_retired_links(
) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let first = store.begin([1; 32], [[3; 32]])?;
    let second = store.begin([2; 32], [[3; 32]])?;
    for publication in [&first, &second] {
        publication.publish_partition_checked(
            [3; 32],
            PartitionKind::Accounts,
            |writer| {
                writer.write_all(b"shared partition")?;
                Ok(())
            },
            &|| false,
        )?;
    }
    let a = directory.path().join(partition_path(&[1; 32], &[3; 32], PartitionKind::Accounts));
    let b = directory.path().join(partition_path(&[2; 32], &[3; 32], PartitionKind::Accounts));
    assert_eq!(fs::metadata(&a)?.ino(), fs::metadata(&b)?.ino());
    assert!(!directory.path().join(hex::encode([3; 32])).exists());
    store.retain(&BTreeSet::new())?;
    assert!(a.is_file() && b.is_file(), "active publications protect aliases and pool");
    drop(first);
    drop(second);
    store.retain(&BTreeSet::from([[2; 32]]))?;
    assert!(!a.exists());
    assert!(!a.parent().unwrap().exists());
    assert_eq!(fs::read(&b)?, b"shared partition");
    assert!(directory.path().join(PRIVATE).join("blobs").join(hex::encode([3; 32])).exists());
    store.retain(&BTreeSet::new())?;
    assert!(!b.exists());
    assert_eq!(fs::read_dir(directory.path().join(PRIVATE).join("blobs"))?.count(), 0);
    Ok(())
}

#[test]
fn gc_recovers_orphan_partition_links_without_touching_unknown_directories() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let publication = store.begin([1; 32], [[2; 32]])?;
    let name = publication.publish_partition_checked(
        [2; 32],
        PartitionKind::Accounts,
        |writer| {
            let mut chunks = chunks::ChunkWriter::new(writer)?;
            chunks.write_all(&zstd::encode_all(b"payload".as_slice(), 3)?)?;
            chunks.finish()?;
            Ok(())
        },
        &|| false,
    )?;
    drop(publication);
    fs::remove_file(store.private("records", &[1; 32]))?;
    let unknown = directory.path().join(format!("{}.partitions", hex::encode([9; 32])));
    fs::create_dir(&unknown)?;
    fs::write(unknown.join("unknown"), b"leave alone")?;
    store.retain(&BTreeSet::new())?;
    assert!(!directory.path().join(name).exists());
    assert_eq!(fs::read(unknown.join("unknown"))?, b"leave alone");
    assert_eq!(fs::read_dir(directory.path().join(PRIVATE).join("blobs"))?.count(), 0);
    Ok(())
}

#[test]
fn partition_namespace_rejects_traversal_and_unknown_names() {
    let path = partition_path(&[1; 32], &[2; 32], PartitionKind::Messages);
    assert_eq!(
        partition_ids(&path),
        Some((hex::encode([1; 32]).as_str(), hex::encode([2; 32]).as_str()))
    );
    for invalid in [
        format!("{path}/extra"),
        format!("../{path}"),
        path.replace("messages_", "unknown_"),
        path.replace(".partitions/", ".partitions/../"),
    ] {
        assert!(partition_ids(&invalid).is_none());
    }
}

#[test]
fn publication_refuses_an_untracked_partition_directory() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let path = directory.path().join(format!("{}.partitions", hex::encode([1; 32])));
    fs::create_dir(&path)?;
    fs::write(path.join("keep"), b"untracked")?;
    assert!(store.begin([1; 32], [[2; 32]]).is_err());
    assert_eq!(fs::read(path.join("keep"))?, b"untracked");
    Ok(())
}

fn publish(publication: &Publication, id: u8, body: &[u8]) -> anyhow::Result<()> {
    publication.publish([id; 32], |writer| {
        writer.write_all(body)?;
        Ok(())
    })?;
    Ok(())
}

#[test]
fn gc_discovers_chunked_orphans_but_protects_references_and_unknown_files() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let mut writer = chunks::ChunkWriter::new(Vec::new())?;
    writer.write_all(&zstd::encode_all(b"payload".as_slice(), 3)?)?;
    let (blob, map) = writer.finish()?;
    let path = |id| directory.path().join(hex::encode([id; 32]));

    // A missing record must not make a recognized new-format file immortal.
    fs::write(path(1), &blob)?;
    let orphan_header = directory.path().join(format!("{}.header", hex::encode([1; 32])));
    fs::write(&orphan_header, b"orphan header")?;
    let retained = store.begin([2; 32], [[3; 32]])?;
    for id in [2, 3] {
        publish(&retained, id, &blob)?;
    }
    drop(retained);
    let active = store.begin([4; 32], [[3; 32], [5; 32]])?;
    for id in [4, 5] {
        publish(&active, id, &blob)?;
    }
    // Old ZSTD snapshots and corrupt new envelopes are not orphan candidates.
    let mut legacy = chunks::MAGIC.to_vec();
    legacy.extend(zstd::encode_all(b"legacy".as_slice(), 3)?);
    fs::write(path(6), &legacy)?;
    let mut bad = blob.clone();
    bad[map.offset as usize + 12] ^= 1;
    fs::write(path(7), &bad)?;
    let mut bad_footer = blob.clone();
    let footer = bad_footer.len() - chunks::FOOTER_BYTES;
    bad_footer[footer..footer + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    fs::write(path(8), &bad_footer)?;
    fs::write(directory.path().join("not-a-resource"), &blob)?;
    #[cfg(unix)]
    std::os::unix::fs::symlink(path(2), path(9))?;

    store.retain(&BTreeSet::from([[2; 32]]))?;
    assert!(!path(1).exists());
    assert!(!orphan_header.exists());
    for id in 2..=8 {
        assert!(path(id).exists(), "resource {id}");
    }
    assert!(directory.path().join("not-a-resource").exists());
    #[cfg(unix)]
    assert!(fs::symlink_metadata(path(9))?.file_type().is_symlink());

    // Active publication also protects a shared partition after another
    // snapshot is retired. Once the lease is released, ordinary GC removes it.
    store.retain(&BTreeSet::new())?;
    assert!(!path(2).exists());
    assert!(path(3).exists());
    drop(active);
    store.retain(&BTreeSet::new())?;
    for id in [3, 4, 5] {
        assert!(!path(id).exists());
    }
    for id in [6, 7, 8] {
        assert!(path(id).exists());
    }
    Ok(())
}

#[test]
fn shared_partitions_survive_retirement_and_reopening() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    for primary in [1, 2] {
        let attempt = store.begin([primary; 32], [[3; 32]])?;
        publish(&attempt, primary, b"primary")?;
        publish(&attempt, 3, b"shared")?;
        attempt.publish_header(b"header")?;
    }
    let store = SnapshotResources::open(directory.path())?;
    store.retain(&BTreeSet::from([[2; 32]]))?;
    assert!(!directory.path().join(hex::encode([1; 32])).exists());
    assert_eq!(fs::read(directory.path().join(hex::encode([3; 32])))?, b"shared");
    assert_eq!(
        snapshot_headers(directory.path())?.iter().map(|entry| &entry.0).collect::<Vec<_>>(),
        vec![&hex::encode([2; 32])]
    );
    store.retain(&BTreeSet::new())?;
    assert!(snapshot_headers(directory.path())?.is_empty());
    assert!(!directory.path().join(hex::encode([3; 32])).exists());
    Ok(())
}

#[test]
fn gc_retains_active_attempt_and_failed_attempt_is_explicitly_retired() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let attempt = store.begin([1; 32], [[3; 32], [4; 32]])?;
    publish(&attempt, 1, b"primary")?;
    attempt.publish_header(b"header")?;
    assert_eq!(snapshot_headers(directory.path())?.len(), 1);
    assert!(!directory.path().join(hex::encode([3; 32])).exists());
    // Independent handle models a separate GC process/worker.
    SnapshotResources::open(directory.path())?.retain(&BTreeSet::new())?;
    publish(&attempt, 3, b"partition")?;
    assert!(attempt
        .publish([4; 32], |writer| {
            writer.write_all(b"unfinished")?;
            anyhow::bail!("cancelled")
        })
        .is_err());
    assert!(!directory.path().join(hex::encode([4; 32])).exists());
    drop(attempt);
    let leftover = store.staging(&[1; 32]).join(".tmp-crashed-writer");
    fs::write(&leftover, b"unfinished after process crash")?;
    assert_eq!(snapshot_headers(directory.path())?.len(), 1);
    store.retain(&BTreeSet::new())?;
    assert!(!leftover.exists());
    assert!(snapshot_headers(directory.path())?.is_empty());
    assert_eq!(fs::read_dir(directory.path())?.count(), 1); // private index only
    Ok(())
}

#[test]
fn collision_does_not_replace_resource_and_publication_needs_no_sidecar() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let attempt = store.begin([1; 32], [])?;
    publish(&attempt, 1, b"complete")?;
    publish(&attempt, 1, b"complete")?;
    assert!(publish(&attempt, 1, b"different").is_err());
    assert_eq!(fs::read(directory.path().join(hex::encode([1; 32])))?, b"complete");
    assert_eq!(fs::read_dir(directory.path().join(PRIVATE).join("blobs"))?.count(), 0);
    assert!(attempt.publish([2; 32], |_| Ok(())).is_err());
    Ok(())
}

#[test]
fn chunked_republication_compares_size_and_footer_not_a_second_payload_digest() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let attempt = store.begin([1; 32], [[2; 32]])?;
    let encode = |byte| -> anyhow::Result<Vec<u8>> {
        let mut writer = chunks::ChunkWriter::new(Vec::new())?;
        writer.write_all(&vec![byte; chunks::CHUNK_BYTES + 17])?;
        Ok(writer.finish()?.0)
    };
    let original = encode(7)?;
    let name = attempt.publish_partition_checked(
        [2; 32],
        PartitionKind::Accounts,
        |writer| {
            writer.write_all(&original)?;
            Ok(())
        },
        &|| false,
    )?;
    let pool = directory.path().join(PRIVATE).join("blobs").join(hex::encode([2; 32]));
    // Locally published files are immutable: deduplication trusts their footer,
    // unlike download/import validation. Deliberately alter payload only to
    // prove that this path no longer scans/hashes the full existing body.
    let mut same_footer = original.clone();
    same_footer[4] ^= 1;
    fs::write(&pool, &same_footer)?;
    attempt.publish_partition_checked(
        [2; 32],
        PartitionKind::Accounts,
        |writer| {
            writer.write_all(&original)?;
            Ok(())
        },
        &|| false,
    )?;
    assert_eq!(fs::read(directory.path().join(&name))?, same_footer);
    let different_footer = encode(8)?;
    assert_eq!(original.len(), different_footer.len());
    assert!(attempt
        .publish_partition_checked(
            [2; 32],
            PartitionKind::Accounts,
            |writer| {
                writer.write_all(&different_footer)?;
                Ok(())
            },
            &|| false
        )
        .is_err());
    assert_eq!(fs::read(&pool)?, same_footer);
    // Size is checked independently of the footer representation identity.
    let mut truncated = original.clone();
    truncated.pop();
    fs::write(&pool, &truncated)?;
    assert!(attempt
        .publish_partition_checked(
            [2; 32],
            PartitionKind::Accounts,
            |writer| {
                writer.write_all(&original)?;
                Ok(())
            },
            &|| false
        )
        .is_err());
    Ok(())
}

#[test]
fn malformed_index_is_fail_closed_and_untracked_files_are_never_swept() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    {
        let attempt = store.begin([1; 32], [])?;
        publish(&attempt, 1, b"primary")?;
    }
    fs::write(directory.path().join(hex::encode([9; 32])), b"untracked cache or legacy")?;
    fs::write(store.private("records", &[1; 32]), b"broken")?;
    assert!(store.retain(&BTreeSet::new()).is_err());
    assert!(directory.path().join(hex::encode([1; 32])).exists());
    assert!(directory.path().join(hex::encode([9; 32])).exists());
    Ok(())
}

#[test]
fn discovery_excludes_orphans_partitions_indexes_and_temporary_files() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    for name in [
        hex::encode([1; 32]),
        format!("{}.header", hex::encode([2; 32])),
        ".tmp".into(),
        "random".into(),
    ] {
        fs::write(directory.path().join(name), b"not a snapshot")?;
    }
    assert!(snapshot_headers(directory.path())?.is_empty());
    fs::write(directory.path().join(format!("{}.header", hex::encode([1; 32]))), b"ready")?;
    assert_eq!(snapshot_headers(directory.path())?.len(), 1);
    Ok(())
}

#[test]
fn membership_cannot_change_and_header_requires_primary() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let attempt = store.begin([1; 32], [[2; 32]])?;
    assert!(attempt.publish_header(b"early").is_err());
    drop(attempt);
    assert!(store.begin([1; 32], [[3; 32]]).is_err());
    assert!(store.begin([4; 32], [[4; 32]]).is_err());
    Ok(())
}

#[test]
fn legacy_header_is_not_adopted_and_active_encoding_survives_gc() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let legacy = directory.path().join(hex::encode([9; 32]));
    let header = legacy.with_extension("header");
    fs::write(&legacy, b"legacy")?;
    fs::write(&header, b"legacy header")?;
    assert!(store.begin([9; 32], []).is_err());
    let attempt = store.begin([1; 32], [])?;
    attempt.publish([1; 32], |writer| {
        writer.write_all(b"first")?;
        store.retain(&BTreeSet::new())?;
        assert!(!directory.path().join(hex::encode([1; 32])).exists());
        writer.write_all(b"last")?;
        Ok(())
    })?;
    drop(attempt);
    store.retain(&BTreeSet::new())?;
    assert_eq!(fs::read(legacy)?, b"legacy");
    assert_eq!(fs::read(header)?, b"legacy header");
    Ok(())
}

#[test]
fn gc_recovers_owned_body_published_before_header_after_crash() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let attempt = store.begin([1; 32], [])?;
    // Model interruption between atomic body publication and discovery header.
    let path = directory.path().join(hex::encode([1; 32]));
    fs::write(&path, b"complete but not announced")?;
    drop(attempt);
    let store = SnapshotResources::open(directory.path())?;
    store.retain(&BTreeSet::new())?;
    assert!(!path.exists());
    store.retain(&BTreeSet::new())?;
    Ok(())
}

fn time(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

#[test]
fn registered_legacy_snapshots_share_retention_with_partitioned_snapshots() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    for id in 1..=3 {
        let path = directory.path().join(hex::encode([id; 32]));
        fs::write(&path, b"legacy body")?;
        fs::write(path.with_extension("header"), b"header")?;
        for file in [&path, &path.with_extension("header")] {
            File::open(file)?.set_modified(time(0))?;
        }
        store.register_legacy_snapshot([id; 32], b"header", &[1], id as u64)?;
    }
    let record = store.private("records", &[1; 32]);
    let before = fs::read(&record)?;
    store.register_legacy_snapshot([1; 32], b"header", &[1], 1)?;
    assert_eq!(before, fs::read(record)?);
    // A new-format publication must protect all of its partitions while old
    // monolithic snapshots age out of the same thread's latest-two window.
    let active = store.begin_snapshot([4; 32], [[8; 32]], &[1], 4)?;
    publish(&active, 4, b"primary")?;
    publish(&active, 8, b"partition")?;
    active.publish_header(b"new header")?;
    store.retain_recent(RetentionPolicy::default(), time(7200))?;
    for id in [1, 2] {
        let path = directory.path().join(hex::encode([id; 32]));
        assert!(!path.exists());
        assert!(!path.with_extension("header").exists());
    }
    for id in [3, 4, 8] {
        assert!(directory.path().join(hex::encode([id; 32])).exists());
    }
    Ok(())
}

#[test]
fn legacy_registration_rejects_incomplete_changed_or_chunked_files() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let path = directory.path().join(hex::encode([1; 32]));
    fs::write(&path, b"legacy")?;
    assert!(store.register_legacy_snapshot([1; 32], b"header", &[1], 1).is_err());
    fs::write(path.with_extension("header"), b"header")?;
    assert!(store.register_legacy_snapshot([1; 32], b"changed", &[1], 1).is_err());
    let mut writer = chunks::ChunkWriter::new(Vec::new())?;
    writer.write_all(&zstd::encode_all(b"new payload".as_slice(), 3)?)?;
    fs::write(&path, writer.finish()?.0)?;
    assert!(store.register_legacy_snapshot([1; 32], b"header", &[1], 1).is_err());
    assert!(store.records()?.is_empty());
    Ok(())
}

fn retention_record(
    id: u8,
    thread: u8,
    seq_no: u64,
    started: u64,
    ready: Option<u64>,
) -> SnapshotRecord {
    SnapshotRecord {
        version: 1,
        primary: [id; 32],
        partitions: BTreeSet::new(),
        retention: Some(RetentionInfo {
            thread: vec![thread],
            seq_no,
            started_at: time(started),
            primary_ready_at: ready.map(time),
        }),
    }
}

#[test]
fn retention_policy_keeps_age_or_two_latest_per_thread_not_latest_publication() {
    let mut records = vec![
        retention_record(1, 1, 1, 0, Some(6400)), // exactly one hour old: eligible
        retention_record(2, 1, 2, 0, Some(0)),
        retention_record(3, 1, 3, 0, Some(0)),
        retention_record(4, 1, 0, 0, Some(6401)), // recently published old block
        retention_record(5, 1, 99, 0, None),      // failed before primary: not top two
        retention_record(6, 1, 100, 9999, None),  // incomplete but still in grace
        retention_record(7, 2, 1, 0, Some(0)),
        retention_record(8, 2, 2, 0, Some(0)),
        retention_record(9, 1, 0, 0, Some(10001)), // clock moved backwards
    ];
    let mut old = retention_record(10, 1, 0, 0, None);
    old.retention = None;
    records.push(old);
    assert_eq!(
        retention_keep(&records, RetentionPolicy::default(), time(10000)),
        [2, 3, 4, 6, 7, 8, 9, 10].into_iter().map(|id| [id; 32]).collect()
    );
}

#[test]
fn automatic_retention_sweeps_expired_records_preserves_threads_and_shared_bodies(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    for id in 1..=6 {
        let thread = if id <= 4 { 1 } else { 2 };
        let attempt = store.begin_snapshot([id; 32], [[9; 32]], &[thread], id as u64)?;
        publish(&attempt, id, b"primary")?;
        publish(&attempt, 9, b"shared")?;
        attempt.publish_header(b"header")?;
        let path = store.private("records", &[id; 32]);
        let mut record: SnapshotRecord = serde_json::from_reader(File::open(&path)?)?;
        let info = record.retention.as_mut().unwrap();
        info.started_at = time(0);
        info.primary_ready_at = Some(time(0));
        atomic_write(&path, &serde_json::to_vec(&record)?)?;
    }
    // An old but still active attempt is protected regardless of the keep set.
    let active = store.begin_snapshot([7; 32], [], &[1], 7)?;
    let path = store.private("records", &[7; 32]);
    let mut record: SnapshotRecord = serde_json::from_reader(File::open(&path)?)?;
    record.retention.as_mut().unwrap().started_at = time(0);
    atomic_write(&path, &serde_json::to_vec(&record)?)?;
    let store = SnapshotResources::open(directory.path())?;
    store.retain_recent(RetentionPolicy::default(), time(3600))?;
    for id in 1..=6 {
        assert_eq!(directory.path().join(hex::encode([id; 32])).exists(), id >= 3);
    }
    assert!(store.private("records", &[7; 32]).exists());
    assert_eq!(fs::read(directory.path().join(hex::encode([9; 32])))?, b"shared");
    drop(active);
    store.retain_recent(RetentionPolicy::default(), time(3600))?;
    assert!(!store.private("records", &[7; 32]).exists());
    Ok(())
}

#[test]
fn retention_retries_preserve_first_ready_time_and_identity() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SnapshotResources::open(directory.path())?;
    let attempt = store.begin_snapshot([1; 32], [], &[1], 10)?;
    publish(&attempt, 1, b"primary")?;
    attempt.publish_header(b"header")?;
    drop(attempt);
    let path = store.private("records", &[1; 32]);
    let before = fs::read(&path)?;
    let attempt = store.begin_snapshot([1; 32], [], &[1], 10)?;
    attempt.publish_header(b"header")?;
    assert_eq!(fs::read(&path)?, before);
    drop(attempt);
    assert!(store.begin_snapshot([1; 32], [], &[2], 10).is_err());
    assert!(store.begin_snapshot([1; 32], [], &[1], 11).is_err());
    // Optional metadata keeps the old ownership-index schema readable.
    let mut value: serde_json::Value = serde_json::from_slice(&before)?;
    value.as_object_mut().unwrap().remove("retention");
    atomic_write(&path, &serde_json::to_vec(&value)?)?;
    let attempt = store.begin_snapshot([1; 32], [], &[1], 10)?;
    attempt.publish_header(b"header")?;
    let after: SnapshotRecord = serde_json::from_reader(File::open(path)?)?;
    assert!(after.retention.unwrap().primary_ready_at.is_some());
    Ok(())
}
