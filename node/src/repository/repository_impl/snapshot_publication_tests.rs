use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use account_state::read_account_partition_checked;
use account_state::AccountPartition;
use account_state::AnchorBlockRef;
use account_state::ThreadAccount;
use account_state::ThreadAccountsRepository;
use multi_map::MapHash;
use multi_map::MapKeyPath;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifier;
use node_types::ThreadAccountsHash;

use super::super::snapshot_manifest::build_snapshot_manifest;
use super::super::snapshot_partitioned::read_primary_snapshot_checked;
use super::*;
use crate::block_keeper_system::BlockKeeperData;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::config::GlobalConfig;
use crate::message::WrappedMessage;
use crate::repository::optimistic_shard_state::OptimisticShardState;
use crate::types::thread_message_queue::partition_codec::read_message_partition_checked;
use crate::types::thread_message_queue::partition_codec::MessagePartitionImportLimits;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AccountInbox;
use crate::versioning::block_protocol_version_state::BlockProtocolVersionState;
use crate::versioning::ProtocolVersion;

pub(crate) struct Fixture {
    _directory: tempfile::TempDir,
    pub(crate) repo: ThreadAccountsRepository,
    pub(crate) snapshot: ThreadSnapshot,
    pub(crate) state: OptimisticStateImpl,
    publisher: NodeIdentifier,
    pub(crate) configs: ConfigRead,
}

fn protocol(config: &GlobalConfig) -> ProtocolVersion {
    ProtocolVersion::builder()
        .canonical_config_hash(config)
        .tvm_engine_version(config.engine_version.clone())
        .gossip_version(config.gossip_version)
        .build()
}

fn account(i: u8, version: u64) -> (AccountRouting, ThreadAccount) {
    let id = AccountIdentifier::new([i; 32]);
    let routing = id.routing(DAppIdentifier::new([i / 4; 32]));
    let account = tvm_block::Account::with_address(
        tvm_block::MsgAddressInt::with_standart(
            None,
            0,
            tvm_types::AccountId::from_raw(id.as_slice().to_vec(), 256),
        )
        .unwrap(),
    );
    let shard = tvm_block::ShardAccount::with_params(
        &account,
        [i; 32].into(),
        version,
        Some(routing.dapp_id().as_array().into()),
    )
    .unwrap();
    (routing, ThreadAccount::from_tvm(shard))
}

fn inbox(lt: u64) -> AccountInbox {
    let mut inbox = AccountInbox::empty();
    for lt in [lt, lt + 1] {
        let header = tvm_block::InternalMessageHeader { created_lt: lt, ..Default::default() };
        let body = Arc::new(WrappedMessage::from_tvm(&tvm_block::Message::with_int_header(header)));
        inbox.commitment.add(&body.identifier());
        inbox.add_messages(vec![(body.identifier(), body)]).unwrap();
    }
    inbox
}

impl Fixture {
    pub(crate) fn new() -> anyhow::Result<Self> {
        let directory = tempfile::tempdir()?;
        let repo = ThreadAccountsRepository::builder(directory.path().join("accounts"))
            .set_apply_to_durable(true)
            .build()?;
        let mut snapshot = super::super::tests::make_test_snapshot();
        let mut state = OptimisticStateImpl::zero();
        state.block_id = snapshot.finalized_block().data().identifier();
        state.block_seq_no = snapshot.finalized_block().data().seq_no();
        state.thread_id = *snapshot.finalized_block().data().common_section().thread_id();
        repo.ensure_thread(&state.thread_id, &BlockIdentifier::default(), true)?;
        repo.start_archive_update_service()?;
        let mut builder = repo.state_builder(&state.thread_id, 0, &state.shard_state.0);
        for i in 1..=12 {
            let (route, body) = account(i, 1);
            builder.insert_account(&route, &body);
        }
        let transition = builder.build(None)?;
        repo.finalize_thread_transition(
            &state.block_id,
            &state.thread_id,
            0,
            &transition.new_state,
            transition.account_operations,
        )?;
        assert!(repo.flush_pending_and_wait_for_drain(Duration::from_secs(5)));
        state.shard_state = OptimisticShardState(transition.new_state);
        for i in 1..=3 {
            state.messages.set_inbox(account(i, 1).0, inbox(i as u64 * 10));
            state.high_priority_messages.set_inbox(account(i, 1).0, inbox(i as u64 * 100));
        }
        let mut bk = BlockKeeperSet::new();
        for i in 1..=3 {
            bk.insert(
                i as u16,
                BlockKeeperData {
                    owner_address: AccountIdentifier::new([i; 32]),
                    ..Default::default()
                },
            );
        }
        snapshot.set_bk_set(bk);
        let config = GlobalConfig { snapshot_partition_size_bytes: 256, ..GlobalConfig::default() };
        let version = protocol(&config);
        snapshot
            .set_block_protocol_version_state(BlockProtocolVersionState::Current(version.clone()));
        Ok(Self {
            _directory: directory,
            repo,
            snapshot,
            state,
            publisher: AccountIdentifier::new([1; 32]).into(),
            configs: ConfigRead::new(version, config),
        })
    }

    pub(crate) fn pin(&self) -> anyhow::Result<PinHandle> {
        let mut request = self.repo.request_snapshot_pin(AnchorBlockRef {
            anchor_block_id: self.state.block_id,
            anchor_thread_id: self.state.thread_id,
            cross_thread_refs: HashMap::new(),
        });
        let pin = self
            .repo
            .acquire_snapshot_pin(self.state.thread_id, self.state.block_id, Duration::from_secs(2))
            .ok_or_else(|| anyhow::anyhow!("test pin acquisition failed"))?;
        request.disarm();
        Ok(pin)
    }

    pub(crate) fn publication<'a>(
        &'a self,
        pin: PinHandle,
        directory: &'a Path,
    ) -> PartitionedSnapshotPublication<'a> {
        PartitionedSnapshotPublication {
            snapshot: &self.snapshot,
            state: &self.state,
            account_repository: self.repo.durable_map_repo(),
            pin,
            publisher: &self.publisher,
            directory,
            configs: self.configs.clone(),
            max_roots_per_list: 1000,
            retention: RetentionPolicy::default(),
            account_read_limits: PartitionReadLimits {
                accounts_per_batch: 1,
                in_flight_batches: 2,
                ..Default::default()
            },
        }
    }

    fn queue_next(&self) -> anyhow::Result<BlockIdentifier> {
        let (route, updated) = account(1, 2);
        let mut builder =
            self.repo.state_builder(&self.state.thread_id, 1, &self.state.shard_state.0);
        builder.insert_account(&route, &updated);
        let transition = builder.build(None)?;
        let next = BlockIdentifier::new([77; 32]);
        self.repo.finalize_thread_transition(
            &next,
            &self.state.thread_id,
            1,
            &transition.new_state,
            transition.account_operations,
        )?;
        assert!(!self.repo.flush_pending_and_wait_for_drain(Duration::from_millis(20)));
        Ok(next)
    }

    fn archive_block(&self) -> BlockIdentifier {
        self.repo
            .get_archive_thread_control_states()
            .unwrap()
            .into_iter()
            .find(|control| control.thread_id == self.state.thread_id)
            .unwrap()
            .last_block_id
            .unwrap()
    }
}

fn entries(directory: &Path) -> Vec<String> {
    let mut entries: Vec<_> = std::fs::read_dir(directory)
        .unwrap()
        .flat_map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            if name.ends_with(".partitions") {
                std::fs::read_dir(entry.path())
                    .unwrap()
                    .map(|file| format!("{name}/{}", file.unwrap().file_name().to_string_lossy()))
                    .collect::<Vec<_>>()
            } else {
                vec![name]
            }
        })
        .filter(|name| !name.starts_with('.') && !name.ends_with(".header"))
        .collect();
    entries.sort();
    entries
}

fn payload(directory: &Path, name: &str) -> anyhow::Result<Vec<u8>> {
    let bytes = std::fs::read(directory.join(name))?;
    assert_eq!(&bytes[..4], b"ZSTD");
    let input = snapshot_storage::chunks::PayloadReader::new(bytes.as_slice())?;
    let mut decoder = zstd::Decoder::with_buffer(input)?.single_frame();
    let mut payload = Vec::new();
    std::io::Read::read_to_end(&mut decoder, &mut payload)?;
    decoder.finish().verify_trailer()?;
    Ok(payload)
}

fn names(manifest: &ThreadSnapshotManifest, primary: &[u8; 32]) -> Vec<String> {
    manifest
        .account_partition_roots
        .iter()
        .map(|root| {
            snapshot_storage::partition_path(
                primary,
                &account_partition_blob_id(root),
                snapshot_storage::PartitionKind::Accounts,
            )
        })
        .chain(manifest.normal_message_partition_roots.iter().map(|root| {
            snapshot_storage::partition_path(
                primary,
                &message_partition_blob_id(MessageQueueKind::Normal, root),
                snapshot_storage::PartitionKind::Messages,
            )
        }))
        .chain(manifest.high_priority_message_partition_roots.iter().map(|root| {
            snapshot_storage::partition_path(
                primary,
                &message_partition_blob_id(MessageQueueKind::HighPriority, root),
                snapshot_storage::PartitionKind::Messages,
            )
        }))
        .collect()
}

#[test]
fn snapshot_partition_capacity_uses_effective_block_config_and_rejects_invalid_configs(
) -> anyhow::Result<()> {
    let old_config = GlobalConfig { snapshot_partition_size_bytes: 256, ..GlobalConfig::default() };
    let new_config = GlobalConfig {
        engine_version: semver::Version::new(1, 0, 9),
        snapshot_partition_size_bytes: 4096,
        ..GlobalConfig::default()
    };
    let old = protocol(&old_config);
    let new = protocol(&new_config);
    let configs = ConfigRead::new(new.clone(), new_config).with_retired(old.clone(), old_config);
    for version in [
        BlockProtocolVersionState::Current(old.clone()),
        BlockProtocolVersionState::TransitionLocked {
            from: old.clone(),
            to: new.clone(),
            milestone: BlockIdentifier::default(),
        },
        BlockProtocolVersionState::CompleteTransition { from: old, to: new.clone() },
    ] {
        assert_eq!(partition_capacity_for_block(&version, &configs)?, 256);
    }
    assert_eq!(
        partition_capacity_for_block(&BlockProtocolVersionState::Current(new), &configs)?,
        4096
    );
    assert!(partition_capacity_for_block(
        &BlockProtocolVersionState::Current(ProtocolVersion::parse("unknown")?),
        &configs,
    )
    .is_err());
    for config in [
        GlobalConfig { engine_version: semver::Version::new(1, 0, 7), ..GlobalConfig::default() },
        GlobalConfig { snapshot_partition_size_bytes: 0, ..GlobalConfig::default() },
    ] {
        let version = protocol(&config);
        let configs = ConfigRead::new(version.clone(), config);
        assert!(partition_capacity_for_block(
            &BlockProtocolVersionState::Current(version),
            &configs
        )
        .is_err());
    }
    Ok(())
}

#[test]
fn snapshot_publication_freezes_block_selected_capacity_across_config_replacement(
) -> anyhow::Result<()> {
    let mut fixture = Fixture::new()?;
    let old_config = GlobalConfig { snapshot_partition_size_bytes: 256, ..GlobalConfig::default() };
    let new_config = GlobalConfig {
        engine_version: semver::Version::new(1, 0, 9),
        snapshot_partition_size_bytes: 4096,
        ..GlobalConfig::default()
    };
    let old = protocol(&old_config);
    let new = protocol(&new_config);
    fixture.configs =
        ConfigRead::new(new.clone(), new_config.clone()).with_retired(old.clone(), old_config);
    fixture.snapshot.set_block_protocol_version_state(BlockProtocolVersionState::Current(old));
    let expected = |capacity| {
        build_snapshot_manifest(
            fixture.state.shard_state.0.durable.map(),
            &fixture.state.messages,
            &fixture.state.high_priority_messages,
            capacity,
            1000,
        )
    };
    let small = expected(256)?;
    let large = expected(4096)?;
    assert_ne!(small, large);
    let output = tempfile::tempdir()?;
    let mut live_configs = fixture.configs.clone();
    let mut publication = fixture.publication(fixture.pin()?, output.path());
    publication.configs = live_configs.clone();
    let mut callbacks = 0;
    let manifest = publish_partitioned_snapshot(publication, &|| false, |_| {
        if callbacks == 0 {
            // Replace the caller's live registry after publishing primary.
            // The in-progress attempt owns its original registry snapshot.
            live_configs = ConfigRead::new(new.clone(), new_config.clone());
        }
        callbacks += 1;
        Ok(())
    })?;
    assert_eq!(manifest, small);
    assert_eq!(callbacks, 1 + manifest_partition_count(&small));
    fixture.configs = live_configs;
    fixture.snapshot.set_block_protocol_version_state(BlockProtocolVersionState::Current(new));
    let output = tempfile::tempdir()?;
    let manifest = publish_partitioned_snapshot(
        fixture.publication(fixture.pin()?, output.path()),
        &|| false,
        |_| Ok(()),
    )?;
    assert_eq!(manifest, large);
    Ok(())
}

#[test]
fn snapshot_publication_missing_block_config_publishes_nothing() -> anyhow::Result<()> {
    let mut fixture = Fixture::new()?;
    fixture.snapshot.set_block_protocol_version_state(BlockProtocolVersionState::Current(
        ProtocolVersion::parse("unknown")?,
    ));
    let output = tempfile::tempdir()?;
    assert!(publish_partitioned_snapshot(
        fixture.publication(fixture.pin()?, output.path()),
        &|| false,
        |_| panic!("unconfigured snapshot must not publish"),
    )
    .is_err());
    assert_eq!(std::fs::read_dir(output.path())?.count(), 0);
    Ok(())
}

#[test]
fn snapshot_publication_primary_first_scheduled_blobs_roundtrip_and_pin_spans_last_callback(
) -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = tempfile::tempdir()?;
    let pin = fixture.pin()?;
    let next = fixture.queue_next()?;
    let manifest = build_snapshot_manifest(
        fixture.state.shard_state.0.durable.map(),
        &fixture.state.messages,
        &fixture.state.high_priority_messages,
        256,
        1000,
    )?;
    let ranked = rank_snapshot_publishers(&fixture.snapshot.bk_set, &manifest)?;
    let rank = ranked.iter().position(|id| id == &fixture.publisher).unwrap();
    let flat = names(&manifest, fixture.state.block_id.as_array());
    let mut expected = vec![hex::encode(fixture.state.block_id.as_array())];
    expected.extend(
        partition_publication_order(flat.len(), rank, ranked.len())?.map(|i| flat[i].clone()),
    );
    let mut published = Vec::new();
    let actual =
        publish_partitioned_snapshot(fixture.publication(pin, output.path()), &|| false, |name| {
            assert_eq!(fixture.archive_block(), fixture.state.block_id);
            assert!(!fixture.repo.wait_for_drain(Duration::from_millis(5)));
            let raw = payload(output.path(), name)?;
            if published.is_empty() {
                let primary = read_primary_snapshot_checked(&mut raw.as_slice(), 1000, &|| false)?;
                assert_eq!(primary.manifest, manifest);
                assert_eq!(entries(output.path()), vec![expected[0].clone()]);
                let headers = snapshot_storage::snapshot_headers(output.path())?;
                assert_eq!(headers.len(), 1);
                assert_eq!(headers[0].0, expected[0]);
                let header: super::super::ThreadSnapshotHeader = bincode::deserialize(
                    &std::fs::read(output.path().join(format!("{}.header", expected[0])))?,
                )?;
                assert_eq!(header.block_id, fixture.state.block_id);
                SnapshotResources::open(output.path())?.retain(&Default::default())?;
                assert!(output.path().join(name).is_file());
            }
            assert_eq!(name, expected[published.len()]);
            published.push(name.to_string());
            Ok(())
        })?;
    assert_eq!(actual, manifest);
    assert_eq!(published, expected);
    assert_eq!(entries(output.path()).len(), flat.len() + 1);
    assert!(fixture.repo.flush_pending_and_wait_for_drain(Duration::from_secs(5)));
    assert_eq!(fixture.archive_block(), next);
    let mut account_parts = Vec::new();
    for root in &manifest.account_partition_roots {
        let raw = payload(
            output.path(),
            &snapshot_storage::partition_path(
                fixture.state.block_id.as_array(),
                &account_partition_blob_id(root),
                snapshot_storage::PartitionKind::Accounts,
            ),
        )?;
        let map = read_account_partition_checked(
            &mut raw.as_slice(),
            MapKeyPath::default(),
            ThreadAccountsHash::new(*root),
            &|| false,
            |_, _| Ok(()),
        )?;
        account_parts.push(AccountPartition { capacity: map.capacity().bytes(), map });
    }
    let merged = account_state::ThreadAccountMap::merge_partitions(&account_parts).unwrap();
    assert_eq!(merged.hash(), fixture.state.shard_state.0.durable.map().hash());
    for (kind, roots, source) in [
        (
            MessageQueueKind::Normal,
            &manifest.normal_message_partition_roots,
            &fixture.state.messages,
        ),
        (
            MessageQueueKind::HighPriority,
            &manifest.high_priority_message_partition_roots,
            &fixture.state.high_priority_messages,
        ),
    ] {
        let mut merged = ThreadMessageQueueState::empty();
        for root in roots {
            let raw = payload(
                output.path(),
                &snapshot_storage::partition_path(
                    fixture.state.block_id.as_array(),
                    &message_partition_blob_id(kind, root),
                    snapshot_storage::PartitionKind::Messages,
                ),
            )?;
            let part = read_message_partition_checked(
                &mut raw.as_slice(),
                kind,
                MapHash(*root),
                MessagePartitionImportLimits {
                    max_inboxes: 1000,
                    max_messages_per_inbox: 1000,
                    max_messages: 1000,
                },
                &|| false,
            )?;
            for dapp in part.messages.values() {
                for (route, inbox) in dapp.messages.iter() {
                    assert!(merged.account_inbox_by_routing(&route).is_none());
                    merged.set_inbox(route, inbox.as_ref().clone());
                }
            }
        }
        assert_eq!(merged.root_hash(), source.root_hash());
        assert_eq!(merged.capacity(), source.capacity());
    }
    Ok(())
}

#[test]
fn snapshot_publication_cancel_or_callback_failure_preserves_completed_primary_and_releases_pin(
) -> anyhow::Result<()> {
    for cancel_attempt in [false, true] {
        let fixture = Fixture::new()?;
        let output = tempfile::tempdir()?;
        let pin = fixture.pin()?;
        let next = fixture.queue_next()?;
        let cancelled = AtomicBool::new(false);
        let result = publish_partitioned_snapshot(
            fixture.publication(pin, output.path()),
            &|| cancelled.load(Ordering::Relaxed),
            |_| {
                if cancel_attempt {
                    cancelled.store(true, Ordering::Relaxed);
                    Ok(())
                } else {
                    anyhow::bail!("announcement failed")
                }
            },
        );
        assert!(result.is_err());
        assert_eq!(entries(output.path()), vec![hex::encode(fixture.state.block_id.as_array())]);
        assert!(fixture.repo.flush_pending_and_wait_for_drain(Duration::from_secs(5)));
        assert_eq!(fixture.archive_block(), next);
    }
    Ok(())
}

#[test]
fn snapshot_publication_rejects_wrong_pin_unknown_publisher_and_bad_body_without_leaking_temps(
) -> anyhow::Result<()> {
    for mode in 0..3 {
        let mut fixture = Fixture::new()?;
        let output = tempfile::tempdir()?;
        let pin = fixture.pin()?;
        let next = fixture.queue_next()?;
        if mode == 0 {
            fixture.state.block_id = BlockIdentifier::new([88; 32]);
        }
        if mode == 1 {
            fixture.publisher = AccountIdentifier::new([99; 32]).into();
        }
        if mode == 2 {
            let route = account(1, 1).0;
            let mut inbox =
                fixture.state.messages.account_inbox_by_routing(&route).unwrap().clone();
            inbox.commitment.added[0] ^= 1;
            fixture.state.messages.set_inbox(route, inbox);
        }
        assert!(publish_partitioned_snapshot(
            fixture.publication(pin, output.path()),
            &|| false,
            |_| Ok(())
        )
        .is_err());
        assert!(entries(output.path())
            .iter()
            .all(|name| name.len() == 64 || snapshot_storage::partition_ids(name).is_some()));
        if mode < 2 {
            assert!(entries(output.path()).is_empty());
        } else {
            assert!(output.path().join(hex::encode(fixture.state.block_id.as_array())).is_file());
        }
        assert!(fixture.repo.flush_pending_and_wait_for_drain(Duration::from_secs(5)));
        assert_eq!(fixture.archive_block(), next);
    }
    Ok(())
}

#[test]
fn snapshot_publication_retries_identical_files_without_overwriting_conflicting_resources(
) -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = tempfile::tempdir()?;
    let first = publish_partitioned_snapshot(
        fixture.publication(fixture.pin()?, output.path()),
        &|| false,
        |_| Ok(()),
    )?;
    let original: Vec<_> = entries(output.path())
        .into_iter()
        .map(|name| (name.clone(), std::fs::read(output.path().join(name)).unwrap()))
        .collect();
    assert!(fixture.repo.wait_for_drain(Duration::from_secs(5)));
    let second = publish_partitioned_snapshot(
        fixture.publication(fixture.pin()?, output.path()),
        &|| false,
        |_| Ok(()),
    )?;
    assert_eq!(first, second);
    for (name, bytes) in original {
        assert_eq!(std::fs::read(output.path().join(name))?, bytes);
    }
    assert!(fixture.repo.wait_for_drain(Duration::from_secs(5)));
    let primary = hex::encode(fixture.state.block_id.as_array());
    std::fs::write(output.path().join(&primary), b"existing different primary")?;
    let error = publish_partitioned_snapshot(
        fixture.publication(fixture.pin()?, output.path()),
        &|| false,
        |_| Ok(()),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("different bytes"));
    assert_eq!(std::fs::read(output.path().join(primary))?, b"existing different primary");
    assert!(entries(output.path())
        .iter()
        .all(|name| name.len() == 64 || snapshot_storage::partition_ids(name).is_some()));
    Ok(())
}

#[test]
fn snapshot_publication_pre_cancelled_attempt_leaves_no_files_and_releases_pin(
) -> anyhow::Result<()> {
    let fixture = Fixture::new()?;
    let output = tempfile::tempdir()?;
    let pin = fixture.pin()?;
    let next = fixture.queue_next()?;
    assert!(publish_partitioned_snapshot(
        fixture.publication(pin, output.path()),
        &|| true,
        |_| panic!("cancelled publication must not announce resources"),
    )
    .is_err());
    assert!(entries(output.path()).is_empty());
    assert!(fixture.repo.flush_pending_and_wait_for_drain(Duration::from_secs(5)));
    assert_eq!(fixture.archive_block(), next);
    Ok(())
}

#[test]
fn snapshot_publication_concurrent_writers_never_overwrite_the_winner() -> anyhow::Result<()> {
    for identical in [true, false] {
        let output = tempfile::tempdir()?;
        let barrier = std::sync::Barrier::new(2);
        let results = std::thread::scope(|scope| {
            let spawn = |primary, body: &'static [u8]| {
                let output = output.path();
                let barrier = &barrier;
                scope.spawn(move || {
                    let store = SnapshotResources::open(output).unwrap();
                    let resources = store.begin([primary; 32], [[42; 32]]).unwrap();
                    publish_blob(&resources, &[42; 32], None, &|| false, |writer| {
                        writer.write_all(body)?;
                        barrier.wait();
                        Ok(())
                    })
                })
            };
            let a = spawn(1, b"first payload");
            let b = spawn(2, if identical { b"first payload" } else { b"second payload" });
            [a.join().unwrap(), b.join().unwrap()]
        });
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            if identical { 2 } else { 1 }
        );
        let name = hex::encode([42; 32]);
        assert_eq!(entries(output.path()), vec![name.clone()]);
        let body = payload(output.path(), &name)?;
        assert!(body == b"first payload" || (!identical && body == b"second payload"));
    }
    Ok(())
}

#[test]
fn snapshot_publication_atomic_blob_visibility_and_mid_write_cleanup() -> anyhow::Result<()> {
    let output = tempfile::tempdir()?;
    let id = [9; 32];
    let name = hex::encode(id);
    let store = SnapshotResources::open(output.path())?;
    let resources = store.begin(id, [[10; 32], [11; 32]])?;
    publish_blob(&resources, &id, None, &|| false, |writer| {
        writer.write_all(b"complete payload")?;
        writer.flush()?;
        assert!(!output.path().join(&name).exists());
        Ok(())
    })?;
    assert_eq!(payload(output.path(), &name)?, b"complete payload");
    let cancelled = AtomicBool::new(false);
    assert!(publish_blob(
        &resources,
        &[10; 32],
        None,
        &|| cancelled.load(Ordering::Relaxed),
        |writer| {
            writer.write_all(b"partial payload")?;
            cancelled.store(true, Ordering::Relaxed);
            Ok(())
        }
    )
    .is_err());
    assert!(publish_blob(&resources, &[11; 32], None, &|| false, |writer| {
        writer.write_all(b"partial payload")?;
        anyhow::bail!("payload error")
    })
    .is_err());
    assert_eq!(entries(output.path()), vec![name]);
    Ok(())
}
