use std::cell::Cell;
use std::fs::File;
use std::io::Cursor;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use account_state::ArchiveStateStore;
use account_state::ArchiveStoreConfig;
use account_state::KVStore;
use account_state::ThreadAccount;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;

use super::super::snapshot_manifest::ThreadSnapshotManifest;
use super::super::snapshot_partitioned::write_primary_snapshot_checked;
use super::super::snapshot_publication::publish_partitioned_snapshot;
use super::super::snapshot_publication::tests::Fixture;
use super::*;
use crate::node::services::sync::snapshot_compression::write_partition_blob;

pub(crate) fn limits() -> SnapshotImportLimits {
    SnapshotImportLimits {
        roots_per_list: 1000,
        messages: MessagePartitionImportLimits {
            max_inboxes: 1000,
            max_messages_per_inbox: 100,
            max_messages: 1000,
        },
        accounts_per_batch: 2,
        account_bytes_per_batch: 256,
    }
}

pub(crate) struct Target {
    _directory: tempfile::TempDir,
    pub(crate) repository: DurableThreadAccountsRepository,
    archive: ArchiveStateStore,
    kv: KVStore,
    old_route: AccountRouting,
    old_account: ThreadAccount,
}

impl Target {
    pub(crate) fn new() -> anyhow::Result<Self> {
        let kv = KVStore::InMemory(Default::default());
        let archive = ArchiveStateStore::new(
            kv.clone(),
            ArchiveStoreConfig {
                aerospike_address: None,
                node_id: String::new(),
                write_parallelism: 0,
            },
        )?;
        let thread = ThreadIdentifier::default();
        archive.ensure_thread(&thread, &BlockIdentifier::default())?;
        archive.reset()?;
        let old_route = AccountIdentifier::new([255; 32]).routing(DAppIdentifier::default());
        let old_account = ThreadAccount::redirect(DAppIdentifier::new([88; 32]));
        archive.thread_init_from_entries(
            &thread,
            BlockIdentifier::default(),
            [(old_route, old_account.clone())].into_iter().map(Ok),
        )?;
        let directory = tempfile::tempdir()?;
        let repository =
            DurableThreadAccountsRepository::new(directory.path().into(), archive.clone(), None)?;
        Ok(Self { _directory: directory, repository, archive, kv, old_route, old_account })
    }

    pub(crate) fn count(&self, copy: &str) -> usize {
        // Target initialization reset to epoch 1; the private import is epoch 2.
        let mut count = 0;
        self.kv
            .enumerate_checked(&format!("accounts_{copy}_e2"), &|| false, &mut |_| {
                count += 1;
                Ok(())
            })
            .unwrap();
        count
    }

    pub(crate) fn assert_written_once(&self, expected: usize) {
        for copy in ["A", "B"] {
            self.kv
                .enumerate_checked(&format!("accounts_{copy}_e2"), &|| false, &mut |record| {
                    assert_eq!(record.generation, 1);
                    Ok(())
                })
                .unwrap();
            assert_eq!(self.count(copy), expected);
        }
    }

    pub(crate) fn assert_active_unchanged(&self) {
        let control =
            self.archive.get_thread_control_state(&ThreadIdentifier::default()).unwrap().unwrap();
        assert_eq!(control.last_block_id, Some(BlockIdentifier::default()));
        assert_eq!(
            self.archive
                .read_account_operation(&self.old_route)
                .unwrap()
                .unwrap()
                .canonical_bytes(),
            self.old_account.canonical_bytes()
        );
    }

    pub(crate) fn assert_aborted(&self) {
        self.assert_active_unchanged();
        assert_eq!(self.count("A"), 0);
        assert_eq!(self.count("B"), 0);
    }
}

fn publish(source: &Fixture) -> anyhow::Result<(tempfile::TempDir, ThreadSnapshotManifest)> {
    let directory = tempfile::tempdir()?;
    let manifest = publish_partitioned_snapshot(
        source.publication(source.pin()?, directory.path()),
        &|| false,
        |_| Ok(()),
    )?;
    Ok((directory, manifest))
}

fn open(directory: &Path, id: [u8; 32]) -> anyhow::Result<Box<dyn Read>> {
    let flat = directory.join(hex::encode(id));
    Ok(Box::new(File::open(if flat.exists() {
        flat
    } else {
        directory.join(".snapshot-resources/blobs").join(hex::encode(id))
    })?))
}

fn primary(source: &Fixture, manifest: &ThreadSnapshotManifest) -> anyhow::Result<Cursor<Vec<u8>>> {
    Ok(Cursor::new(write_partition_blob(Vec::new(), |writer| {
        write_primary_snapshot_checked(&source.snapshot, &source.state, manifest, writer, &|| false)
    })?))
}

fn load<'a>(
    source: &Fixture,
    target: &'a Target,
    directory: &Path,
    manifest: &ThreadSnapshotManifest,
    limits: SnapshotImportLimits,
) -> anyhow::Result<StagedPartitionedSnapshot<'a>> {
    stage_partitioned_snapshot(
        &mut primary(source, manifest)?,
        &target.repository,
        (source.state.thread_id, source.state.block_id),
        limits,
        |id| open(directory, id),
        |decoded| {
            // Test trust oracle; real consensus validation is a runtime concern.
            anyhow::ensure!(decoded.manifest == *manifest, "unexpected manifest");
            Ok(())
        },
        &|| false,
    )
}

#[test]
fn snapshot_consumption_publisher_roundtrip_stays_staged_until_explicit_handoff(
) -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (directory, manifest) = publish(&source)?;
    let target = Target::new()?;
    // Use the actual primary file produced by the publisher, not a reconstructed
    // monolithic snapshot or an in-memory body collection.
    let mut primary =
        File::open(directory.path().join(hex::encode(source.state.block_id.as_array())))?;
    let staged = stage_partitioned_snapshot(
        &mut primary,
        &target.repository,
        (source.state.thread_id, source.state.block_id),
        limits(),
        |id| open(directory.path(), id),
        |decoded| {
            anyhow::ensure!(decoded.manifest == manifest, "unexpected manifest");
            Ok(())
        },
        &|| false,
    )?;
    assert_eq!(staged.primary().manifest, manifest);
    let state = staged.state();
    assert_eq!(state.block_id, source.state.block_id);
    assert_eq!(state.block_seq_no, source.state.block_seq_no);
    assert_eq!(state.thread_id, source.state.thread_id);
    assert_eq!(
        state.shard_state.0.durable.map().hash(),
        source.state.shard_state.0.durable.map().hash()
    );
    assert_eq!(
        state.shard_state.0.durable.map().capacity(),
        source.state.shard_state.0.durable.map().capacity()
    );
    for (actual, expected) in [
        (&state.messages, &source.state.messages),
        (&state.high_priority_messages, &source.state.high_priority_messages),
    ] {
        assert_eq!(actual.root_hash(), expected.root_hash());
        assert_eq!(actual.capacity(), expected.capacity());
        assert_eq!(actual.length(), expected.length());
        for (_, dapp) in expected.messages.iter() {
            for (routing, inbox) in dapp.messages.iter() {
                let restored = actual.account_inbox_by_routing(&routing).unwrap();
                assert_eq!(restored.commitment, inbox.commitment);
                assert_eq!(restored.tail_sequence().len(), inbox.tail_sequence().len());
                for ((id, body), (expected_id, expected_body)) in
                    restored.tail_sequence().iter().zip(inbox.tail_sequence())
                {
                    assert_eq!(id, expected_id);
                    assert_eq!(body.canonical_bytes(), expected_body.canonical_bytes());
                }
            }
        }
    }
    assert!(state.account_operations.is_empty());
    target.assert_active_unchanged();
    let records = state.shard_state.0.durable.map().iter().count();
    assert_eq!(target.count("A"), records);
    assert_eq!(target.count("B"), records);
    let (_, state, epoch) = staged.into_parts();
    // Isolated test archive: explicitly exercise ownership transfer and commit.
    // Runtime must use its finalization gate/recovery workflow instead.
    target.repository.commit_staged_import(epoch, &state.thread_id, &state.block_id)?;
    assert_eq!(
        target.archive.get_thread_control_state(&state.thread_id)?.unwrap().last_block_id,
        Some(state.block_id)
    );
    for (route, info) in state.shard_state.0.durable.map().iter() {
        let body = target.archive.read_account_operation(&route)?.unwrap();
        match info {
            account_state::AccountInfo::AccountHash(hash) => assert_eq!(body.account_hash(), *hash),
            account_state::AccountInfo::Redirect(dapp) => {
                assert_eq!(body.get_dapp_id(), Some(*dapp))
            }
        }
    }
    assert!(target.archive.read_account_operation(&target.old_route)?.is_none());
    Ok(())
}

#[test]
fn snapshot_consumption_drop_and_late_error_discard_both_staged_copies() -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (directory, manifest) = publish(&source)?;
    let target = Target::new()?;
    let staged = load(&source, &target, directory.path(), &manifest, limits())?;
    assert!(target.count("A") > 0);
    drop(staged);
    target.assert_aborted();
    let mut bytes = Vec::new();
    open(
        directory.path(),
        message_partition_blob_id(
            MessageQueueKind::HighPriority,
            manifest.high_priority_message_partition_roots.last().unwrap(),
        ),
    )?
    .read_to_end(&mut bytes)?;
    bytes.truncate(bytes.len() - 2);
    let saw_staging = Cell::new(false);
    let error = stage_partitioned_snapshot(
        &mut primary(&source, &manifest)?,
        &target.repository,
        (source.state.thread_id, source.state.block_id),
        limits(),
        |id| {
            if id
                == message_partition_blob_id(
                    MessageQueueKind::HighPriority,
                    manifest.high_priority_message_partition_roots.last().unwrap(),
                )
            {
                saw_staging.set(target.count("A") > 0 && target.count("B") > 0);
                Ok(Box::new(Cursor::new(bytes.clone())) as Box<dyn Read>)
            } else {
                open(directory.path(), id)
            }
        },
        |_| Ok(()),
        &|| false,
    )
    .err()
    .unwrap();
    assert!(saw_staging.get(), "late failure must occur after account bodies were staged");
    assert!(!error.to_string().is_empty());
    target.assert_aborted();
    Ok(())
}

#[test]
fn snapshot_consumption_rejects_untrusted_or_wrong_primary_before_partition_lookup(
) -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (directory, manifest) = publish(&source)?;
    for mode in 0..4 {
        let target = Target::new()?;
        let mut expected = (source.state.thread_id, source.state.block_id);
        if mode == 0 {
            expected.1 = BlockIdentifier::new([99; 32]);
        }
        if mode == 1 {
            expected.0 = ThreadIdentifier::new(&BlockIdentifier::new([99; 32]), 1);
        }
        let result = stage_partitioned_snapshot(
            &mut primary(&source, &manifest)?,
            &target.repository,
            expected,
            limits(),
            |_| panic!("no partition lookup before primary verification"),
            |_| {
                if mode == 2 {
                    anyhow::bail!("untrusted primary");
                }
                Ok(())
            },
            &|| mode == 3,
        );
        assert!(result.is_err());
        target.assert_aborted();
    }
    // Sanity: valid data still loads after those rejected attempts.
    let target = Target::new()?;
    drop(load(&source, &target, directory.path(), &manifest, limits())?);
    target.assert_aborted();
    Ok(())
}

#[test]
fn snapshot_consumption_accepts_ready_partitions_in_reverse_order_with_accounts_first(
) -> anyhow::Result<()> {
    struct ReverseReady<'a> {
        directory: &'a Path,
        visited: &'a mut Vec<[u8; 32]>,
    }
    impl SnapshotPartitionSource for ReverseReady<'_> {
        fn read_partition<T>(
            &mut self,
            _: [u8; 32],
            _: impl FnOnce(&mut dyn Read) -> anyhow::Result<T>,
        ) -> anyhow::Result<T> {
            anyhow::bail!("readiness-driven importer must not request a fixed blob")
        }

        fn read_any_partition<T>(
            &mut self,
            pending: &BTreeMap<[u8; 32], usize>,
            read: impl FnOnce(usize, &mut dyn Read) -> anyhow::Result<T>,
        ) -> anyhow::Result<([u8; 32], T)> {
            let (&id, &index) = pending.iter().max_by_key(|(_, index)| **index).unwrap();
            self.visited.push(id);
            let mut blob = open(self.directory, id)?;
            Ok((id, read(index, &mut *blob)?))
        }
    }
    let source = Fixture::new()?;
    let (directory, manifest) = publish(&source)?;
    let decoded = decode_blob(&mut primary(&source, &manifest)?, &|| false, |reader| {
        read_primary_snapshot_checked(reader, limits().roots_per_list, &|| false)
    })?;
    let target = Target::new()?;
    let mut visited = Vec::new();
    let staged = stage_verified_snapshot(
        decoded,
        &target.repository,
        limits(),
        ReverseReady { directory: directory.path(), visited: &mut visited },
        &|| false,
    )?;
    let account_ids: Vec<_> =
        manifest.account_partition_roots.iter().rev().map(account_partition_blob_id).collect();
    let message_ids: Vec<_> = manifest
        .normal_message_partition_roots
        .iter()
        .map(|root| message_partition_blob_id(MessageQueueKind::Normal, root))
        .chain(
            manifest
                .high_priority_message_partition_roots
                .iter()
                .map(|root| message_partition_blob_id(MessageQueueKind::HighPriority, root)),
        )
        .rev()
        .collect();
    assert!(account_ids.len() > 1);
    assert!(!manifest.high_priority_message_partition_roots.is_empty());
    assert_eq!(visited, account_ids.into_iter().chain(message_ids).collect::<Vec<_>>());
    assert_eq!(
        staged.state().shard_state.0.durable.map().hash(),
        source.state.shard_state.0.durable.map().hash()
    );
    assert_eq!(staged.state().messages.root_hash(), source.state.messages.root_hash());
    assert_eq!(
        staged.state().high_priority_messages.root_hash(),
        source.state.high_priority_messages.root_hash()
    );
    drop(staged);
    target.assert_aborted();
    Ok(())
}

#[test]
fn snapshot_consumption_rejects_missing_corrupt_reordered_and_overlapping_partitions(
) -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (directory, manifest) = publish(&source)?;
    assert!(manifest.account_partition_roots.len() > 1);
    assert!(manifest.normal_message_partition_roots.len() > 1);
    for mode in 0..7 {
        let target = Target::new()?;
        let mut altered = manifest.clone();
        match mode {
            0 => {
                altered.account_partition_roots[0] = [77; 32];
            }
            1 => {
                altered.account_partition_roots.swap(0, 1);
            }
            2 => {
                altered.account_partition_roots.insert(1, altered.account_partition_roots[0]);
            }
            3 => {
                altered.normal_message_partition_roots.swap(0, 1);
            }
            4 => {
                altered
                    .high_priority_message_partition_roots
                    .insert(1, altered.high_priority_message_partition_roots[0]);
            }
            _ => {}
        }
        let result = stage_partitioned_snapshot(
            &mut primary(&source, &altered)?,
            &target.repository,
            (source.state.thread_id, source.state.block_id),
            limits(),
            |id| {
                if mode == 5
                    && id == account_partition_blob_id(&manifest.account_partition_roots[0])
                {
                    return Ok(Box::new(Cursor::new(b"corrupt".to_vec())) as Box<dyn Read>);
                }
                if mode == 6
                    && id
                        == message_partition_blob_id(
                            MessageQueueKind::HighPriority,
                            &manifest.high_priority_message_partition_roots[0],
                        )
                {
                    return open(
                        directory.path(),
                        message_partition_blob_id(
                            MessageQueueKind::Normal,
                            &manifest.normal_message_partition_roots[0],
                        ),
                    );
                }
                open(directory.path(), id)
            },
            |_| Ok(()),
            &|| false,
        );
        assert!(result.is_err(), "mode={mode}");
        target.assert_aborted();
    }
    Ok(())
}

#[test]
fn snapshot_consumption_budgets_and_cancellation_abort_after_partial_writes() -> anyhow::Result<()>
{
    let source = Fixture::new()?;
    let (directory, manifest) = publish(&source)?;
    for mode in [1, 4, 5, 6] {
        let target = Target::new()?;
        let mut limits = limits();
        match mode {
            1 => {
                limits.roots_per_list = 0;
            }
            4 => {
                limits.accounts_per_batch = 0;
            }
            _ => {}
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let result = stage_partitioned_snapshot(
            &mut primary(&source, &manifest)?,
            &target.repository,
            (source.state.thread_id, source.state.block_id),
            limits,
            |id| {
                if mode == 5 && target.count("A") > 0 {
                    cancelled.store(true, Ordering::Relaxed);
                }
                if mode == 6 {
                    return Ok(Box::new(CancelAfterRead {
                        input: open(directory.path(), id)?,
                        cancelled: cancelled.clone(),
                    }) as Box<dyn Read>);
                }
                open(directory.path(), id)
            },
            |_| Ok(()),
            &|| cancelled.load(Ordering::Relaxed),
        );
        assert!(result.is_err(), "mode={mode}");
        target.assert_aborted();
    }
    Ok(())
}

struct CancelAfterRead {
    input: Box<dyn Read>,
    cancelled: Arc<AtomicBool>,
}
impl Read for CancelAfterRead {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let count = self.input.read(bytes)?;
        self.cancelled.store(true, Ordering::Relaxed);
        Ok(count)
    }
}

#[test]
fn snapshot_consumption_empty_manifest_and_different_write_batches() -> anyhow::Result<()> {
    let mut source = Fixture::new()?;
    let (directory, manifest) = publish(&source)?;
    let target = Target::new()?;
    for (count, bytes) in [(1, 1), (2, 256), (1000, 1024 * 1024)] {
        let mut limits = limits();
        limits.accounts_per_batch = count;
        limits.account_bytes_per_batch = bytes;
        let staged = load(&source, &target, directory.path(), &manifest, limits)?;
        assert_eq!(
            staged.state().shard_state.0.durable.map().hash(),
            source.state.shard_state.0.durable.map().hash()
        );
        assert_eq!(staged.state().messages.root_hash(), source.state.messages.root_hash());
        drop(staged);
        target.assert_aborted();
    }
    source.state.shard_state.0.durable =
        DurableThreadAccountsState::with_map(ThreadAccountMap::new());
    source.state.messages = ThreadMessageQueueState::empty();
    source.state.high_priority_messages = ThreadMessageQueueState::empty();
    let (directory, manifest) = publish(&source)?;
    assert_eq!(manifest, ThreadSnapshotManifest::default());
    let mut limits = limits();
    limits.roots_per_list = 0;
    let staged = stage_partitioned_snapshot(
        &mut primary(&source, &manifest)?,
        &target.repository,
        (source.state.thread_id, source.state.block_id),
        limits,
        |_| panic!("empty manifest must not resolve partitions"),
        |_| Ok(()),
        &|| false,
    )?;
    assert!(staged.state().shard_state.0.durable.map().is_empty());
    assert!(staged.state().messages.is_empty());
    assert!(staged.state().high_priority_messages.is_empty());
    assert_eq!(snapshot_storage::snapshot_headers(directory.path())?.len(), 1);
    assert_eq!(
        std::fs::read_dir(directory.path())?
            .filter(|entry| entry.as_ref().is_ok_and(|entry| entry
                .file_name()
                .to_string_lossy()
                .len()
                == 64))
            .count(),
        1
    );
    drop(staged);
    target.assert_aborted();
    Ok(())
}

#[test]
#[allow(clippy::disallowed_methods)] // Explicitly tests cleanup while unwinding.
fn snapshot_consumption_unwind_also_aborts_partial_staging() -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let (directory, manifest) = publish(&source)?;
    let target = Target::new()?;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        stage_partitioned_snapshot(
            &mut primary(&source, &manifest).unwrap(),
            &target.repository,
            (source.state.thread_id, source.state.block_id),
            limits(),
            |id| {
                if target.count("A") > 0 {
                    panic!("injected resolver panic");
                }
                open(directory.path(), id)
            },
            |_| Ok(()),
            &|| false,
        )
    }));
    assert!(result.is_err());
    target.assert_aborted();
    Ok(())
}

#[test]
fn snapshot_consumption_inbox_merge_is_disjoint_and_order_independent() -> anyhow::Result<()> {
    let source = Fixture::new()?;
    let queue = &source.state.messages;
    let routings: Vec<_> = queue.messages.values().flat_map(|dapp| dapp.messages.keys()).collect();
    let parts: Vec<_> = routings
        .iter()
        .map(|routing| {
            let mut part = queue.clone();
            part.remove_accounts(routings.iter().filter(|other| *other != routing).copied());
            part
        })
        .collect();
    let merged = ThreadMessageQueueState::merge_partitions(&parts).unwrap();
    assert_eq!(merged.root_hash(), queue.root_hash());
    assert_eq!(merged.capacity(), queue.capacity());
    assert_eq!(merged.length(), queue.length());
    let reverse: Vec<_> = parts.iter().rev().cloned().collect();
    assert_eq!(
        ThreadMessageQueueState::merge_partitions(&reverse).unwrap().root_hash(),
        queue.root_hash()
    );
    assert!(
        ThreadMessageQueueState::merge_partitions(&[parts[0].clone(), parts[0].clone()]).is_err()
    );
    assert!(ThreadMessageQueueState::merge_partitions(&[]).unwrap().is_empty());
    Ok(())
}

#[test]
fn snapshot_consumption_envelope_requires_one_complete_frame_without_trailing_data(
) -> anyhow::Result<()> {
    let valid = write_partition_blob(Vec::new(), |writer| {
        writer.write_all(b"payload")?;
        Ok(())
    })?;
    let read = |bytes: &[u8]| {
        decode_blob(&mut Cursor::new(bytes), &|| false, |reader| {
            let mut body = Vec::new();
            reader.read_to_end(&mut body)?;
            Ok(body)
        })
    };
    let read_preverified = |bytes: &[u8]| {
        decode_blob_with_transport(&mut Cursor::new(bytes), &|| false, true, |reader| {
            let mut body = Vec::new();
            reader.read_to_end(&mut body)?;
            Ok(body)
        })
    };
    assert_eq!(read(&valid)?, b"payload");
    assert_eq!(read_preverified(&valid)?, b"payload");
    let mut trailing = valid.clone();
    trailing.push(0);
    assert!(read(&trailing).is_err());
    assert!(read_preverified(&trailing).is_err());
    let mut second = valid.clone();
    second.extend_from_slice(&valid[4..]);
    assert!(read(&second).is_err());
    assert!(read_preverified(&second).is_err());
    let mut encoder = zstd::Encoder::new(Vec::new(), 3)?;
    encoder.window_log(22)?;
    encoder.write_all(b"payload")?;
    let mut oversized_window = COMPRESSED_SNAPSHOT_MAGIC.to_vec();
    oversized_window.extend(encoder.finish()?);
    assert!(read(&oversized_window).is_err());
    assert!(read_preverified(&oversized_window).is_err());
    for end in 0..valid.len() {
        assert!(read(&valid[..end]).is_err(), "end={end}");
        assert!(read_preverified(&valid[..end]).is_err(), "preverified end={end}");
    }
    Ok(())
}
