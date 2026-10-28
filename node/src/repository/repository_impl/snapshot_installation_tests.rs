use std::io::Read;

use account_state::AccountInfo;
use account_state::ArchiveStateStore;

use super::super::snapshot_consumption::stage_partitioned_snapshot;
use super::super::snapshot_consumption::tests::limits;
use super::super::snapshot_publication::publish_partitioned_snapshot;
use super::super::snapshot_publication::tests::Fixture;
use super::*;

fn publish(source: &Fixture) -> anyhow::Result<tempfile::TempDir> {
    let directory = tempfile::tempdir()?;
    publish_partitioned_snapshot(
        source.publication(source.pin()?, directory.path()),
        &|| false,
        |_| Ok(()),
    )?;
    Ok(directory)
}

fn stage<'a>(
    source: &Fixture,
    accounts: &'a ThreadAccountsRepository,
    directory: &Path,
) -> anyhow::Result<StagedPartitionedSnapshot<'a>> {
    stage_partitioned_snapshot(
        &mut File::open(directory.join(source.state.block_id.to_string()))?,
        accounts.durable_map_repo(),
        (source.state.thread_id, source.state.block_id),
        limits(),
        |id| {
            Ok(Box::new(File::open(
                directory.join(".snapshot-resources/blobs").join(hex::encode(id)),
            )?) as Box<dyn Read>)
        },
        |_| Ok(()), // Explicit test trust oracle, not consensus proof verification.
        &|| false,
    )
}

fn assert_restored(repository: &RepositoryImpl, source: &Fixture) -> anyhow::Result<()> {
    let state = repository
        .get_full_optimistic_state_no_cache(&source.state.block_id, &source.state.thread_id, None)?
        .expect("imported optimistic state");
    assert_eq!(
        state.shard_state.0.durable.map().hash(),
        source.state.shard_state.0.durable.map().hash()
    );
    assert_eq!(
        state.shard_state.0.durable.map().capacity(),
        source.state.shard_state.0.durable.map().capacity()
    );
    for (expected, actual) in [
        (&source.state.messages, &state.messages),
        (&source.state.high_priority_messages, &state.high_priority_messages),
    ] {
        assert_eq!(expected.root_hash(), actual.root_hash());
        assert_eq!(expected.capacity(), actual.capacity());
    }
    for (route, info) in state.shard_state.0.durable.map().iter() {
        if matches!(info, AccountInfo::AccountHash(_)) {
            let expected = source.repo.state_account(&source.state.shard_state.0, &route)?.unwrap();
            let actual = repository
                .thread_accounts_repository
                .state_account(&state.shard_state.0, &route)?
                .unwrap();
            assert_eq!(expected.canonical_bytes(), actual.canonical_bytes());
        }
    }
    assert!(!RepositoryImpl::snapshot_import_recovery_path(&repository.data_dir).exists());
    Ok(())
}

#[test]
fn partitioned_installation_rejects_history_conflict_before_archive_commit() -> anyhow::Result<()> {
    use crate::types::history_proof::HistoryBlockData;

    let mut source = Fixture::new()?;
    let mut incoming = HistoryBlockData::new();
    incoming.update_from_pure_data([0x42; 32], source.snapshot.block_height)?;
    source.snapshot.history_cursor = HistoryLayerData::from_iter([(0, incoming)]);
    let blobs = publish(&source)?;
    let directory = tempfile::tempdir()?;
    let (mut repository, _) = super::super::tests::make_test_repository(&directory)?;
    let accounts = repository.thread_accounts_repository.clone();
    let before = format!("{:?}", accounts.get_archive_thread_control_states()?);
    let mut existing = HistoryBlockData::new();
    existing.update_from_pure_data([0x43; 32], source.snapshot.block_height)?;
    let block = repository.block_state_repository.get(&source.state.block_id)?;
    block.guarded_mut(|state| {
        state.set_history_cursor(HistoryLayerData::from_iter([(0, existing)]))
    })?;
    let staged = stage(&source, &accounts, blobs.path())?;
    let error = repository
        .install_partitioned_snapshot(staged, &source.state.thread_id, &|| false)
        .expect_err("history conflict must be rejected before publication");
    assert!(error.to_string().contains("history cursor mismatch"));
    assert_eq!(before, format!("{:?}", accounts.get_archive_thread_control_states()?));
    assert!(!RepositoryImpl::snapshot_import_recovery_path(&repository.data_dir).exists());
    drop(stage(&source, &accounts, blobs.path())?);
    Ok(())
}

#[test]
fn partitioned_installation_commits_three_maps_and_reopens_checkpoint() -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let blobs = publish(&source)?;
    let directory = tempfile::tempdir()?;
    let archive = ArchiveStateStore::in_memory();
    let path = directory.path().join("accounts");
    let accounts = ThreadAccountsRepository::new(path.clone(), archive.clone(), true, None)?;
    let (mut repository, _) =
        super::super::tests::make_test_repository_with_accounts(&directory, accounts.clone())?;
    let staged = stage(&source, &accounts, blobs.path())?;
    let status =
        repository.install_partitioned_snapshot(staged, &source.state.thread_id, &|| false)?;
    assert!(matches!(status, SnapshotImportStatus::Imported));
    assert_restored(&repository, &source)?;
    drop(repository);
    drop(accounts);
    let accounts = ThreadAccountsRepository::new(path, archive, true, None)?;
    let (repository, _) =
        super::super::tests::make_test_repository_with_accounts(&directory, accounts)?;
    assert_restored(&repository, &source)?;
    Ok(())
}

#[test]
fn partitioned_installation_rechecks_eligibility_and_aborts_staging() -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let blobs = publish(&source)?;
    let directory = tempfile::tempdir()?;
    let (mut repository, _) = super::super::tests::make_test_repository(&directory)?;
    let accounts = repository.thread_accounts_repository.clone();
    // The attempt becomes obsolete only after staging has completed;
    // the commit gate must make the final decision.
    let staged = stage(&source, &accounts, blobs.path())?;
    repository.persist_snapshot_import_metadata(
        source.state.thread_id,
        &source.snapshot.finalized_block,
    )?;
    let block = repository.block_state_repository.get(&source.state.block_id)?;
    block.guarded_mut(|state| state.set_finalized())?;
    let status =
        repository.install_partitioned_snapshot(staged, &source.state.thread_id, &|| false)?;
    assert!(matches!(
        status,
        SnapshotImportStatus::Skipped(SnapshotImportDecision::AlreadyCovered { .. })
    ));
    assert!(accounts
        .get_archive_thread_control_states()?
        .iter()
        .all(|state| state.last_block_id != Some(source.state.block_id)));
    assert!(!RepositoryImpl::snapshot_import_recovery_path(&repository.data_dir).exists());
    Ok(())
}

#[test]
fn partitioned_installation_cancel_or_recovery_write_failure_keeps_controls_unpublished(
) -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let blobs = publish(&source)?;
    for cancel_on_call in 0..=3 {
        let directory = tempfile::tempdir()?;
        let (mut repository, _) = super::super::tests::make_test_repository(&directory)?;
        let accounts = repository.thread_accounts_repository.clone();
        let before = accounts.get_archive_thread_control_states()?;
        let staged = stage(&source, &accounts, blobs.path())?;
        if cancel_on_call == 0 {
            // A directory at the record's destination forces persistence to fail.
            fs::create_dir(RepositoryImpl::snapshot_import_recovery_path(&repository.data_dir))?;
        }
        let calls = std::cell::Cell::new(0);
        assert!(repository
            .install_partitioned_snapshot(staged, &source.state.thread_id, &|| {
                calls.set(calls.get() + 1);
                calls.get() == cancel_on_call
            })
            .is_err());
        assert_eq!(
            format!("{before:?}"),
            format!("{:?}", accounts.get_archive_thread_control_states()?)
        );
        // Retrying in the same provisional epoch must remain safe after cleanup.
        drop(stage(&source, &accounts, blobs.path())?);
    }
    Ok(())
}

#[test]
fn partitioned_installation_startup_recovers_only_after_archive_publication() -> anyhow::Result<()>
{
    let source = Fixture::new()?;
    let blobs = publish(&source)?;
    for (published, missing_map) in [(false, false), (true, false), (true, true)] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("accounts");
        let archive = ArchiveStateStore::in_memory();
        let accounts = ThreadAccountsRepository::new(path.clone(), archive.clone(), true, None)?;
        let (repository, _) =
            super::super::tests::make_test_repository_with_accounts(&directory, accounts.clone())?;
        let staged = stage(&source, &accounts, blobs.path())?;
        let serialized = bincode::serialize(staged.state())?;
        let (primary, state, epoch) = staged.into_parts();
        let mut cleanup = StagedImportGuard::new(accounts.clone(), Some(epoch));
        accounts.state_save(&state.block_id, &state.shard_state.0)?;
        repository.save_snapshot_import_recovery(&SnapshotImportRecovery {
            cur_thread_id: state.thread_id,
            snapshot_thread_id: state.thread_id,
            snapshot_block_id: state.block_id,
            prefix: primary.metadata.into_install_prefix(serialized),
            staged: epoch,
        })?;
        if published {
            accounts.commit_staged_import(epoch, &state.thread_id, &state.block_id)?;
            cleanup.disarm();
        }
        drop(cleanup);
        drop(repository);
        drop(accounts);
        if missing_map {
            fs::remove_file(
                path.join("_optimistic_state")
                    .join(format!("{}.accounts_map", source.state.block_id.to_hex_string(),)),
            )?;
        }
        let accounts = ThreadAccountsRepository::new(path, archive, true, None)?;
        let reopened =
            super::super::tests::make_test_repository_with_accounts(&directory, accounts);
        if missing_map {
            let error = reopened.err().expect("missing recovery map must reject startup");
            assert!(error
                .to_string()
                .contains("Missing durable account map for snapshot recovery"));
            assert!(RepositoryImpl::snapshot_import_recovery_path(&directory.path().join("repo"))
                .exists());
            continue;
        }
        let (repository, _) = reopened?;
        if published {
            assert_restored(&repository, &source)?;
        } else {
            assert!(!repository
                .thread_accounts_repository
                .get_archive_thread_control_states()?
                .iter()
                .any(|s| s.last_block_id == Some(source.state.block_id)));
            assert!(!RepositoryImpl::snapshot_import_recovery_path(&repository.data_dir).exists());
        }
    }
    Ok(())
}

#[test]
fn partitioned_installation_rejects_another_archive() -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let blobs = publish(&source)?;
    let directory = tempfile::tempdir()?;
    let (mut repository, _) = super::super::tests::make_test_repository(&directory)?;
    let other_dir = tempfile::tempdir()?;
    let other = ThreadAccountsRepository::builder(other_dir.path().join("accounts")).build()?;
    let error = repository
        .install_partitioned_snapshot(
            stage(&source, &other, blobs.path())?,
            &source.state.thread_id,
            &|| false,
        )
        .unwrap_err();
    assert!(error.to_string().contains("does not match"));
    Ok(())
}
