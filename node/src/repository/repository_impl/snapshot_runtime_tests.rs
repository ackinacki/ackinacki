//! Runtime dispatch/worker tests with inline messages and an in-memory archive.
//! These do not replace process-level rolling-upgrade acceptance.
use super::snapshot_http_import::tests::Server;
use super::snapshot_publication::publish_partitioned_snapshot;
use super::snapshot_publication::tests::Fixture;
use super::*;
use crate::config::GlobalConfig;
use crate::node::services::sync::snapshot_runtime_import::import_downloaded_snapshot;

fn published(source: &Fixture) -> anyhow::Result<(tempfile::TempDir, Server)> {
    let directory = tempfile::tempdir()?;
    publish_partitioned_snapshot(
        source.publication(source.pin()?, directory.path()),
        &|| false,
        |_| Ok(()),
    )?;
    let blobs = fs::read_dir(directory.path())?
        .chain(fs::read_dir(directory.path().join(".snapshot-resources/blobs"))?)
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().into_string().ok()?;
            snapshot_storage::valid_id(&name).then(|| (name, fs::read(entry.path()).unwrap()))
        })
        .collect();
    let server = Server::new(blobs, source.state.block_id.to_string(), None, None, None);
    Ok((directory, server))
}

#[test]
fn runtime_partitioned_import_uses_static_fallback_and_real_publisher_worker() -> anyhow::Result<()>
{
    let mut source = Fixture::new()?;
    let mut history = crate::types::history_proof::HistoryBlockData::new();
    history.update_from_pure_data([0x42; 32], source.snapshot.block_height)?;
    source.snapshot.history_cursor.insert(0, history);
    let parent = source.snapshot.finalized_block().data().parent();
    source.snapshot.ancestor_blocks_data = vec![
        super::tests::make_ancestor_block_data(
            source.state.block_id,
            parent,
            source.state.block_seq_no,
        ),
        super::tests::make_ancestor_block_data(parent, BlockIdentifier::default(), 0.into()),
    ];
    let (blobs, server) = published(&source)?;
    let raw = fs::read(blobs.path().join(source.state.block_id.to_string()))?;
    let directory = tempfile::tempdir()?;
    let (mut repository, block_states) = super::tests::make_test_repository(&directory)?;
    repository.config_read = source.configs.clone();
    let cache = directory.path().join("download-cache");
    let status = import_downloaded_snapshot(
        &mut raw.as_slice(),
        &mut repository,
        (source.state.thread_id, source.state.block_id),
        source.snapshot.block_height,
        &cache,
        &[],
        std::slice::from_ref(&server.base),
        &|| false,
    )?;
    assert!(matches!(status, SnapshotImportStatus::Imported));
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
    repository.thread_accounts_repository().start_archive_update_service()?;

    let output = tempfile::tempdir()?;
    let publisher = source.snapshot.bk_set.iter().next().unwrap().node_id();
    let service = crate::node::services::sync::FileSavingService::builder()
        .root_path(output.path().to_owned())
        .publisher(publisher)
        .repository(repository.clone())
        .block_state_repository(block_states)
        .shared_services(repository.shared_services.clone())
        .message_db(repository.message_db.clone())
        .build();
    service.save_object(
        &source.state.block_id,
        &source.state.thread_id,
        account_state::AnchorBlockRef {
            anchor_block_id: source.state.block_id,
            anchor_thread_id: source.state.thread_id,
            cross_thread_refs: Default::default(),
        },
        Some(restored),
        source.state.block_id.to_string().into(),
        source.state.block_id,
    )?;
    let until = Instant::now() + Duration::from_secs(5);
    let primary_path = output.path().join(source.state.block_id.to_string());
    while !RepositoryImpl::snapshot_header_path(&primary_path).exists() && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
    service.flush()?;
    assert!(primary_path.exists(), "production publisher did not publish primary");
    let mut primary_file = File::open(&primary_path)?;
    let primary = snapshot_consumption::decode_blob(&mut primary_file, &|| false, |reader| {
        snapshot_partitioned::read_primary_snapshot_checked(
            reader,
            snapshot_manifest::MAX_SNAPSHOT_ROOTS_PER_LIST,
            &|| false,
        )
    })?;
    let part_path = |index| {
        snapshot_storage::partition_path(
            source.state.block_id.as_array(),
            &primary.manifest.partition_blob_id(index).unwrap(),
            if index < primary.manifest.account_partition_roots.len() {
                snapshot_storage::PartitionKind::Accounts
            } else {
                snapshot_storage::PartitionKind::Messages
            },
        )
    };
    let until = Instant::now() + Duration::from_secs(5);
    while (0..snapshot_schedule::manifest_partition_count(&primary.manifest))
        .any(|index| !output.path().join(part_path(index)).exists())
        && Instant::now() < until
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    service.shutdown_snapshot_workers(Duration::from_secs(2))?;
    assert!(RepositoryImpl::snapshot_header_path(&primary_path).exists());
    for index in 0..snapshot_schedule::manifest_partition_count(&primary.manifest) {
        assert!(output.path().join(part_path(index)).is_file());
    }
    Ok(())
}

#[test]
fn runtime_dispatch_rejects_intermediate_markers_wrong_height_and_protocol_before_staging(
) -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (blobs, _server) = published(&source)?;
    let raw = fs::read(blobs.path().join(source.state.block_id.to_string()))?;
    let directory = tempfile::tempdir()?;
    let (mut repository, _) = super::tests::make_test_repository(&directory)?;
    repository.config_read = source.configs.clone();
    let cache = directory.path().join("cache");
    let wrong = BlockHeight::builder()
        .thread_identifier(source.state.thread_id)
        .height(*source.snapshot.block_height.height() + 1)
        .build();
    let error = import_downloaded_snapshot(
        &mut raw.as_slice(),
        &mut repository,
        (source.state.thread_id, source.state.block_id),
        wrong,
        &cache,
        &[],
        &[],
        &|| false,
    )
    .unwrap_err();
    assert!(error.to_string().contains("height mismatch"));
    let unknown = crate::versioning::ProtocolVersion::parse("unavailable")?;
    repository.config_read = ConfigRead::new(unknown, GlobalConfig::default());
    assert!(import_downloaded_snapshot(
        &mut raw.as_slice(),
        &mut repository,
        (source.state.thread_id, source.state.block_id),
        source.snapshot.block_height,
        &cache,
        &[],
        &[],
        &|| false
    )
    .is_err());
    for marker in [b"SNAP2V3", b"SNAP2V4"] {
        let mut encoded = b"ZSTD".to_vec();
        encoded.extend(zstd::encode_all(marker.as_slice(), 3)?);
        assert!(import_downloaded_snapshot(
            &mut encoded.as_slice(),
            &mut repository,
            (source.state.thread_id, source.state.block_id),
            source.snapshot.block_height,
            &cache,
            &[],
            &[],
            &|| false
        )
        .is_err());
    }
    assert!(!cache.exists());
    assert!(!RepositoryImpl::snapshot_import_recovery_path(&repository.data_dir).exists());
    Ok(())
}

#[test]
fn runtime_dispatch_imports_baseline_only_under_retired_protocol() -> anyhow::Result<()> {
    let mut source = Fixture::new()?;
    let old_config =
        GlobalConfig { engine_version: semver::Version::new(1, 0, 7), ..GlobalConfig::default() };
    let old = crate::versioning::ProtocolVersion::builder()
        .canonical_config_hash(&old_config)
        .tvm_engine_version(old_config.engine_version.clone())
        .gossip_version(old_config.gossip_version)
        .build();
    source
        .snapshot
        .set_block_protocol_version_state(BlockProtocolVersionState::Current(old.clone()));
    source.snapshot.optimistic_state =
        SnapshotVersion::Legacy.serialize_state(source.state.clone())?;
    let durable = tempfile::NamedTempFile::new()?;
    let _pin = source.pin()?;
    source.repo.export_snapshot_v1_to_writer_checked(
        &source.state.shard_state.0,
        &mut durable.as_file(),
        &|| false,
    )?;
    let mut raw = Vec::new();
    write_legacy_thread_snapshot(&source.snapshot, Some(durable.path()), &mut raw)?;
    for compressed in [false, true] {
        let directory = tempfile::tempdir()?;
        let (mut repository, _) = super::tests::make_test_repository(&directory)?;
        repository.config_read =
            source.configs.clone().with_retired(old.clone(), old_config.clone());
        let bytes = if compressed {
            let mut bytes = b"ZSTD".to_vec();
            bytes.extend(zstd::encode_all(raw.as_slice(), 3)?);
            bytes
        } else {
            raw.clone()
        };
        let status = import_downloaded_snapshot(
            &mut bytes.as_slice(),
            &mut repository,
            (source.state.thread_id, source.state.block_id),
            source.snapshot.block_height,
            &directory.path().join("cache"),
            &[],
            &[],
            &|| false,
        )?;
        assert!(matches!(status, SnapshotImportStatus::Imported));
    }
    Ok(())
}
