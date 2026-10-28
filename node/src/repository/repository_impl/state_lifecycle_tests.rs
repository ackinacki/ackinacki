//! Local lifecycle checks. The archive is an in-memory stand-in for the external
//! KV service; repository handles and checkpoint files are recreated independently.
//! These are not node-process, live-network migration, or multi-thread tests.
use account_state::ArchiveStateStore;
use account_state::ThreadAccount;
use account_state::ThreadAccountMap;
use node_types::DAppIdentifier;

use super::*;
use crate::repository::optimistic_shard_state::OptimisticShardState;

fn account_fixture() -> (AccountRouting, ThreadAccount) {
    let id = AccountIdentifier::new([42; 32]);
    let route = id.routing(DAppIdentifier::new([7; 32]));
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
        [8; 32].into(),
        19,
        Some(route.dapp_id().as_array().into()),
    )
    .unwrap();
    (route, ThreadAccount::from(shard))
}

fn populated_state(
    repository: &ThreadAccountsRepository,
) -> anyhow::Result<(
    OptimisticStateImpl,
    AccountRouting,
    ThreadAccount,
    Vec<Vec<Arc<WrappedMessage>>>,
)> {
    let (mut state, bodies) = super::inbox_snapshot_tests::fixture();
    state.block_id = super::tests::make_test_snapshot().finalized_block.data().identifier();
    let (route, account) = account_fixture();
    let mut builder = repository.state_builder(&state.thread_id, 0, &state.shard_state.0);
    builder.insert_account(&route, &account);
    let transition = builder.build(None)?;
    repository.ensure_thread(&state.thread_id, &BlockIdentifier::default(), true)?;
    repository.start_archive_update_service()?;
    repository.finalize_thread_transition(
        &state.block_id,
        &state.thread_id,
        0,
        &transition.new_state,
        transition.account_operations,
    )?;
    ensure!(
        repository.flush_pending_and_wait_for_drain(Duration::from_secs(5)),
        "archive did not drain"
    );
    state.shard_state = OptimisticShardState(transition.new_state);
    validate_snapshot_inboxes(&state, &bodies)?;
    Ok((state, route, account, bodies))
}

fn assert_maps_equal(expected: &OptimisticStateImpl, actual: &OptimisticStateImpl) {
    let before = expected.shard_state.0.durable.map();
    let after = actual.shard_state.0.durable.map();
    assert_eq!(before.hash(), after.hash());
    assert_eq!(before.capacity(), after.capacity());
    let parts = |map: &ThreadAccountMap| {
        map.partition_by_capacity(256)
            .unwrap()
            .iter()
            .map(|part| (part.map.hash(), part.capacity))
            .collect::<Vec<_>>()
    };
    assert_eq!(parts(before), parts(after));
    for (before, after) in [
        (&expected.messages, &actual.messages),
        (&expected.high_priority_messages, &actual.high_priority_messages),
    ] {
        assert_eq!(before.root_hash(), after.root_hash());
        assert_eq!(before.capacity(), after.capacity());
        let parts = |queue: &crate::types::thread_message_queue::ThreadMessageQueueState| {
            queue
                .partition_by_capacity(128)
                .unwrap()
                .iter()
                .map(|part| (part.root_hash, part.capacity))
                .collect::<Vec<_>>()
        };
        assert_eq!(parts(before), parts(after));
        assert_eq!(bincode::serialize(before).unwrap(), bincode::serialize(after).unwrap());
    }
}

#[test]
fn legacy_publication_roundtrips_accounts_and_both_inbox_priorities() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let source = ThreadAccountsRepository::builder(dir.path().join("source")).build()?;
    let (state, route, account, bodies) = populated_state(&source)?;
    let mut durable = NamedTempFile::new_in(dir.path())?;
    source.export_snapshot_v1_to_writer_checked(
        &state.shard_state.0,
        durable.as_file_mut(),
        &|| false,
    )?;
    let mut snapshot = super::tests::make_test_snapshot();
    snapshot.optimistic_state = SnapshotVersion::Legacy.serialize_state(state.clone())?;
    snapshot.db_messages = bodies;
    let mut bytes = Vec::new();
    write_legacy_thread_snapshot(&snapshot, Some(durable.path()), &mut bytes)?;
    assert!(bytes.starts_with(snapshot_v1::MAGIC));
    let header = read_snapshot_header_from_reader(&mut bytes.as_slice())?.unwrap();
    assert_eq!(header.block_id, state.block_id);
    for compressed in [false, true] {
        let target_dir = tempfile::tempdir_in(dir.path())?;
        let (mut repo, _) = super::tests::make_test_repository(&target_dir)?;
        let status = if compressed {
            let compressed = zstd::encode_all(bytes.as_slice(), 1)?;
            repo.set_state_from_snapshot_reader_checked(
                &mut zstd::Decoder::new(compressed.as_slice())?,
                &state.thread_id,
                Arc::new(Mutex::new(HashSet::new())),
            )?
        } else {
            repo.set_state_from_snapshot_reader_checked(
                &mut bytes.as_slice(),
                &state.thread_id,
                Arc::new(Mutex::new(HashSet::new())),
            )?
        };
        assert!(matches!(status, SnapshotImportStatus::Imported));
        let restored = repo
            .get_full_optimistic_state_no_cache(&state.block_id, &state.thread_id, None)?
            .unwrap();
        assert_eq!(
            restored.shard_state.0.durable.map().hash(),
            state.shard_state.0.durable.map().hash()
        );
        assert_eq!(
            restored.shard_state.0.durable.map().capacity(),
            state.shard_state.0.durable.map().capacity()
        );
        for (before, after) in [
            (&state.messages, &restored.messages),
            (&state.high_priority_messages, &restored.high_priority_messages),
        ] {
            assert_eq!(before.cursor(), after.cursor());
            for (dapp_id, dapp) in before.messages.iter() {
                assert_eq!(dapp.cursor(), after.messages.get(&dapp_id).unwrap().cursor());
                for (route, inbox) in dapp.messages.iter() {
                    let actual = after.account_inbox_by_routing(&route).unwrap();
                    assert_eq!(
                        bincode::serialize(&inbox.messages)?,
                        bincode::serialize(&actual.messages)?
                    );
                    assert_eq!(inbox.canonical_size(), actual.canonical_size());
                }
            }
        }
        validate_snapshot_inboxes(&restored, &snapshot.db_messages)?;
        assert_eq!(
            repo.thread_accounts_repository
                .state_account(&restored.shard_state.0, &route)?
                .unwrap()
                .canonical_bytes(),
            account.canonical_bytes()
        );
    }
    Ok(())
}

#[test]
fn checkpoints_restore_three_roots_capacities_and_partition_plans_with_fresh_repository_handles(
) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let archive = ArchiveStateStore::in_memory();
    let repository =
        ThreadAccountsRepository::new(dir.path().join("maps"), archive.clone(), true, None)?;
    let (mut state, route, account, bodies) = populated_state(&repository)?;
    // The local checkpoint owns these tail bodies; no external message DB is
    // involved in this test. Compact-history transport is covered below.
    let routing = AccountRouting::default();
    let mut inbox = state.messages.account_inbox_by_routing(&routing).unwrap().clone();
    let mut tail: std::collections::VecDeque<_> =
        bodies[0].iter().map(|message| (message.identifier(), message.clone())).collect();
    tail.extend(inbox.tail_sequence().iter().cloned());
    inbox.messages.set_compacted_history(None);
    inbox.messages.set_tail_sequence(tail);
    state.messages.set_inbox(routing, inbox);
    validate_snapshot_inboxes(&state, &[])?;
    let checkpoint = dir.path().join("state.checkpoint");
    state.clone().save_to_file(&checkpoint, &repository)?;
    drop(repository);

    let reopened = ThreadAccountsRepository::new(dir.path().join("maps"), archive, true, None)?;
    let restored = OptimisticStateImpl::load_from_file(&checkpoint, &reopened)?;
    assert_maps_equal(&state, &restored);
    validate_snapshot_inboxes(&restored, &[])?;
    let loaded = reopened.state_account(&restored.shard_state.0, &route)?.unwrap();
    assert_eq!(loaded.account_hash(), account.account_hash());
    assert_eq!(loaded.canonical_bytes(), account.canonical_bytes());
    Ok(())
}

#[test]
fn checkpoint_restore_rejects_missing_durable_map() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let repository = ThreadAccountsRepository::builder(dir.path().join("maps")).build()?;
    let (state, _, _, _) = populated_state(&repository)?;
    let checkpoint = dir.path().join("state.checkpoint");
    state.clone().save_to_file(&checkpoint, &repository)?;

    // Keep the checkpoint metadata intact but point restore at a directory
    // without its companion account map.
    let incomplete = ThreadAccountsRepository::builder(dir.path().join("missing-maps")).build()?;
    let error = match OptimisticStateImpl::load_from_file(&checkpoint, &incomplete) {
        Ok(_) => panic!("checkpoint without its durable account map was accepted"),
        Err(error) => error,
    };
    let message = format!("{error:#}");
    assert!(message.contains("Missing durable account map"), "{message}");
    assert!(message.contains(&state.block_id.to_string()), "{message}");
    assert!(checkpoint.exists());
    let restored = OptimisticStateImpl::load_from_file(&checkpoint, &repository)?;
    assert_maps_equal(&state, &restored);
    Ok(())
}

#[test]
fn checkpoint_restore_rejects_legacy_and_truncated_durable_maps() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let maps = dir.path().join("maps");
    let repository = ThreadAccountsRepository::builder(&maps).build()?;
    let (state, _, _, _) = populated_state(&repository)?;
    let checkpoint = dir.path().join("state.checkpoint");
    state.clone().save_to_file(&checkpoint, &repository)?;
    let map_path = maps
        .join("_optimistic_state")
        .join(format!("{}.accounts_map", state.block_id.to_hex_string()));
    let original = fs::read(&map_path)?;
    assert_eq!(&original[..8], b"ACMAPV3\0");
    drop(repository);

    let mut legacy = original.clone();
    legacy[..8].copy_from_slice(b"ACMAPV2\0");
    for invalid in [legacy, original[..7].to_vec()] {
        fs::write(&map_path, &invalid)?;
        let reopened = ThreadAccountsRepository::builder(&maps).build()?;
        assert!(OptimisticStateImpl::load_from_file(&checkpoint, &reopened).is_err());
        assert_eq!(fs::read(&map_path)?, invalid);
        assert!(checkpoint.exists());
    }

    fs::write(&map_path, original)?;
    let reopened = ThreadAccountsRepository::builder(&maps).build()?;
    let restored = OptimisticStateImpl::load_from_file(&checkpoint, &reopened)?;
    assert_maps_equal(&state, &restored);
    Ok(())
}

#[test]
fn checkpoint_restore_accepts_explicitly_saved_empty_durable_map() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let repository = ThreadAccountsRepository::builder(dir.path().join("maps")).build()?;
    let state = OptimisticStateImpl::default();
    let checkpoint = dir.path().join("empty.checkpoint");
    state.clone().save_to_file(&checkpoint, &repository)?;
    drop(repository);

    let reopened = ThreadAccountsRepository::builder(dir.path().join("maps")).build()?;
    let restored = OptimisticStateImpl::load_from_file(&checkpoint, &reopened)?;
    assert_maps_equal(&state, &restored);
    Ok(())
}
