//! Local SNAP2V2 files for startup recovery and operator tools.
use account_state::read_account_partition_checked;
use account_state::AccountCapacity;
use account_state::AccountInfo;
use account_state::ThreadAccount;
use node_types::ThreadAccountsHash;

use super::snapshot_consumption::decode_blob;
use super::snapshot_consumption::stage_partitioned_snapshot;
use super::snapshot_consumption::SnapshotImportLimits;
use super::snapshot_manifest::account_partition_blob_id;
use super::snapshot_manifest::message_partition_blob_id;
use super::snapshot_manifest::ThreadSnapshotManifest;
use super::snapshot_manifest::MAX_SNAPSHOT_ROOTS_PER_LIST;
use super::snapshot_partitioned::read_primary_snapshot_checked;
use super::snapshot_partitioned::write_primary_snapshot_checked;
use super::snapshot_partitioned::PrimaryThreadSnapshot;
use super::*;
use crate::node::services::sync::snapshot_compression::write_partition_blob;
use crate::types::thread_message_queue::partition_codec::read_message_partition_checked;
use crate::types::thread_message_queue::partition_codec::write_message_partition_checked;
use crate::types::thread_message_queue::partition_codec::MessageQueueKind;
use crate::types::thread_message_queue::ThreadMessageQueueState;

pub struct SnapshotFile {
    snapshot: ThreadSnapshot,
    manifest: ThreadSnapshotManifest,
    source: PathBuf,
    account_replacements: BTreeMap<AccountRouting, ThreadAccount>,
}

impl SnapshotFile {
    pub fn snapshot(&self) -> &ThreadSnapshot {
        &self.snapshot
    }

    pub fn snapshot_mut(&mut self) -> &mut ThreadSnapshot {
        &mut self.snapshot
    }

    pub fn primary_path(&self) -> &Path {
        &self.source
    }

    /// Replace accounts from a zerostate. Redirect entries are rebuilt together
    /// with their target so account lookup continues to work after routing changes.
    pub fn replace_accounts(&mut self, accounts: Vec<(AccountRouting, ThreadAccount)>) {
        for (routing, account) in accounts {
            self.account_replacements.insert(routing, account);
        }
    }

    pub fn into_snapshot(self) -> ThreadSnapshot {
        self.snapshot
    }
}

pub(super) fn read_primary(path: &Path) -> anyhow::Result<PrimaryThreadSnapshot> {
    decode_blob(&mut File::open(path)?, &|| false, |reader| {
        read_primary_snapshot_checked(reader, MAX_SNAPSHOT_ROOTS_PER_LIST, &|| false)
    })
}

/// Read only the primary metadata; account and message partitions are not opened.
pub fn read_snapshot_metadata(path: &Path) -> anyhow::Result<ThreadSnapshot> {
    let path = resolve_snapshot_primary(path)?;
    Ok(read_primary(&path)?.metadata.into_install_prefix(Vec::new()).into_snapshot())
}

fn resolve_snapshot_primary(path: &Path) -> anyhow::Result<PathBuf> {
    if !path.is_dir() {
        return Ok(path.to_owned());
    }
    let headers = snapshot_storage::snapshot_headers(path)?;
    anyhow::ensure!(
        headers.len() == 1,
        "expected exactly one snapshot in {} (found {})",
        path.display(),
        headers.len()
    );
    Ok(path.join(&headers[0].0))
}

fn partition_path(path: &Path, id: &[u8; 32], kind: snapshot_storage::PartitionKind) -> PathBuf {
    let prefix = match kind {
        snapshot_storage::PartitionKind::Accounts => "accounts",
        snapshot_storage::PartitionKind::Messages => "messages",
    };
    let mut directory = path.as_os_str().to_owned();
    directory.push(".partitions");
    PathBuf::from(directory).join(format!("{prefix}_{}", hex::encode(id)))
}

/// Read the primary metadata and both message queues. Account partitions remain
/// immutable and are preserved by the writer; this tool does not load an archive.
pub fn read_snapshot_file(path: &Path) -> anyhow::Result<SnapshotFile> {
    let path = resolve_snapshot_primary(path)?;
    let path = path.as_path();
    let primary = read_primary(path)?;
    let manifest = primary.manifest;
    let source = &primary.metadata.state;
    let mut state = OptimisticStateImpl::zero();
    state.block_seq_no = source.block_seq_no;
    state.block_id = source.block_id;
    state.shard_state = source.shard_state.clone();
    state.block_info = source.block_info.clone();
    state.threads_table = source.threads_table.clone();
    state.thread_id = source.thread_id;
    state.thread_refs_state = source.thread_refs_state.clone();
    state.cropped = source.cropped.clone();
    for (kind, roots, queue) in [
        (MessageQueueKind::Normal, &manifest.normal_message_partition_roots, &mut state.messages),
        (
            MessageQueueKind::HighPriority,
            &manifest.high_priority_message_partition_roots,
            &mut state.high_priority_messages,
        ),
    ] {
        let parts = roots
            .iter()
            .map(|root| {
                let id = message_partition_blob_id(kind, root);
                let path = partition_path(path, &id, snapshot_storage::PartitionKind::Messages);
                decode_blob(&mut File::open(path)?, &|| false, |reader| {
                    read_message_partition_checked(
                        reader,
                        kind,
                        multi_map::MapHash(*root),
                        SnapshotImportLimits::default().messages,
                        &|| false,
                    )
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        *queue = ThreadMessageQueueState::merge_partitions(&parts)
            .map_err(|error| anyhow::anyhow!("merging snapshot message partitions: {error:?}"))?;
    }
    let snapshot =
        primary.metadata.into_install_prefix(bincode::serialize(&state)?).into_snapshot();
    Ok(SnapshotFile {
        snapshot,
        manifest,
        source: path.to_owned(),
        account_replacements: BTreeMap::new(),
    })
}

fn write_blob(
    path: &Path,
    payload: impl FnOnce(&mut dyn Write) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let directory = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(directory)?;
    let mut temp = tempfile::NamedTempFile::new_in(directory)?;
    write_partition_blob(temp.as_file_mut(), payload)?;
    temp.as_file_mut().sync_all()?;
    temp.persist(path)
        .map_err(|error| anyhow::anyhow!("persist snapshot {}: {}", path.display(), error.error))?;
    Ok(())
}

fn read_account_part(
    path: &Path,
    root: &[u8; 32],
    mut consume: impl FnMut(AccountRouting, ThreadAccount) -> anyhow::Result<()>,
) -> anyhow::Result<account_state::ThreadAccountMap> {
    let id = account_partition_blob_id(root);
    let path = partition_path(path, &id, snapshot_storage::PartitionKind::Accounts);
    decode_blob(&mut File::open(path)?, &|| false, |mut reader| {
        let (map, _) = read_account_partition_checked(
            &mut reader,
            multi_map::MapKeyPath::default(),
            ThreadAccountsHash::new(*root),
            &|| false,
            &mut consume,
        )?;
        Ok(map)
    })
}

fn rewrite_account_partitions(
    path: &Path,
    file: &SnapshotFile,
    manifest: &mut ThreadSnapshotManifest,
) -> anyhow::Result<()> {
    let mut changes: BTreeMap<AccountRouting, Option<ThreadAccount>> = BTreeMap::new();
    let mut replacement_routes = BTreeMap::new();
    for (routing, account) in &file.account_replacements {
        anyhow::ensure!(
            replacement_routes.insert(*routing.account_id(), *routing).is_none(),
            "multiple replacements for one account"
        );
        let default = routing.account_id().redirect();
        changes.insert(*routing, Some(account.clone()));
        if *routing != default {
            changes.insert(default, Some(ThreadAccount::redirect(*routing.dapp_id())));
        }
    }
    let mut bounds = Vec::new();
    for root in &manifest.account_partition_roots {
        let mut first = None;
        let mut last = None;
        read_account_part(&file.source, root, |routing, account| {
            first.get_or_insert(routing);
            last = Some(routing);
            if routing.is_maybe_redirect() && account.is_redirect() {
                if let Some(target) = account.get_dapp_id() {
                    let old = routing.with_dapp_id(target);
                    if replacement_routes.get(routing.account_id()).is_some_and(|new| *new != old) {
                        changes.insert(old, None);
                    }
                }
            }
            Ok(())
        })?;
        let (first, last) =
            (first.ok_or_else(|| anyhow::anyhow!("empty account partition"))?, last.unwrap());
        anyhow::ensure!(
            bounds.last().is_none_or(|(_, previous)| *previous < first),
            "account partition ranges overlap"
        );
        bounds.push((first, last));
    }
    let mut grouped: Vec<BTreeMap<AccountRouting, Option<ThreadAccount>>> =
        (0..bounds.len().max(1)).map(|_| BTreeMap::new()).collect();
    for (routing, account) in changes {
        let index = bounds
            .iter()
            .position(|(_, last)| routing <= *last)
            .unwrap_or(bounds.len().saturating_sub(1));
        grouped[index].insert(routing, account);
    }
    let mut roots = Vec::new();
    for (index, changes) in grouped.into_iter().enumerate() {
        let original = manifest.account_partition_roots.get(index);
        if changes.is_empty() {
            let root = *original.expect("existing unmodified partition");
            let id = account_partition_blob_id(&root);
            let source =
                partition_path(&file.source, &id, snapshot_storage::PartitionKind::Accounts);
            let target = partition_path(path, &id, snapshot_storage::PartitionKind::Accounts);
            if source != target {
                fs::create_dir_all(target.parent().unwrap())?;
                let mut temp = tempfile::NamedTempFile::new_in(target.parent().unwrap())?;
                std::io::copy(&mut File::open(source)?, temp.as_file_mut())?;
                temp.as_file_mut().sync_all()?;
                temp.persist(target).map_err(|error| error.error)?;
            }
            roots.push(root);
            continue;
        }
        let mut bodies = BTreeMap::new();
        let mut map = if let Some(root) = original {
            read_account_part(&file.source, root, |routing, account| {
                bodies.insert(routing, account);
                Ok(())
            })?
        } else {
            account_state::ThreadAccountMap::new()
        };
        for (routing, account) in changes {
            let info = account.as_ref().map(|account| {
                let info = if account.is_redirect() {
                    AccountInfo::Redirect(account.get_dapp_id().expect("redirect target"))
                } else {
                    AccountInfo::AccountHash(account.account_hash())
                };
                (info, AccountCapacity::new(account.canonical_size()))
            });
            map = map.update_single(&routing, info);
            if let Some(account) = account {
                bodies.insert(routing, account);
            } else {
                bodies.remove(&routing);
            }
        }
        if map.is_empty() {
            continue;
        }
        let root = *map.hash().as_array();
        let id = account_partition_blob_id(&root);
        let target = partition_path(path, &id, snapshot_storage::PartitionKind::Accounts);
        write_blob(&target, |writer| {
            writer.write_all(account_state::ACCOUNT_PARTITION_MAGIC)?;
            for (routing, account) in &bodies {
                let bytes = account.canonical_bytes();
                writer.write_all(routing.dapp_id().as_slice())?;
                writer.write_all(routing.account_id().as_slice())?;
                writer.write_all(&(bytes.len() as u64).to_le_bytes())?;
                writer.write_all(bytes)?;
            }
            Ok(())
        })?;
        roots.push(root);
    }
    manifest.account_partition_roots = roots;
    Ok(())
}

/// Save a SNAP2V2 primary and its partitions. Account contents are preserved;
/// message changes are encoded as new content-addressed partitions first.
pub fn write_snapshot_file(path: &Path, file: &SnapshotFile) -> anyhow::Result<()> {
    if let Ok(existing) = fs::canonicalize(path) {
        let name = existing.file_name().and_then(|name| name.to_str()).unwrap_or_default();
        if snapshot_storage::valid_id(name) {
            let record = existing
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(".snapshot-resources/records")
                .join(format!("{name}.json"));
            anyhow::ensure!(
                !record.exists(),
                "cannot overwrite a managed snapshot; use --out-path"
            );
        }
    }
    let state: OptimisticStateImpl = bincode::deserialize(&file.snapshot.optimistic_state)?;
    anyhow::ensure!(
        state.shard_state.0.durable.map().is_empty(),
        "snapshot helper cannot replace durable accounts; use the account archive tools"
    );
    let mut manifest = file.manifest.clone();
    if !file.account_replacements.is_empty() {
        rewrite_account_partitions(path, file, &mut manifest)?;
    } else {
        for root in &manifest.account_partition_roots {
            let id = account_partition_blob_id(root);
            let source =
                partition_path(&file.source, &id, snapshot_storage::PartitionKind::Accounts);
            let target = partition_path(path, &id, snapshot_storage::PartitionKind::Accounts);
            if source != target {
                fs::create_dir_all(target.parent().unwrap())?;
                let mut temp = tempfile::NamedTempFile::new_in(target.parent().unwrap())?;
                std::io::copy(&mut File::open(source)?, temp.as_file_mut())?;
                temp.as_file_mut().sync_all()?;
                temp.persist(target).map_err(|error| error.error)?;
            }
        }
    }
    for (kind, queue, roots) in [
        (MessageQueueKind::Normal, &state.messages, &mut manifest.normal_message_partition_roots),
        (
            MessageQueueKind::HighPriority,
            &state.high_priority_messages,
            &mut manifest.high_priority_message_partition_roots,
        ),
    ] {
        roots.clear();
        for part in queue
            .partition_by_capacity(
                crate::config::GlobalConfig::default().snapshot_partition_size_bytes,
            )
            .map_err(|error| anyhow::anyhow!("partitioning snapshot messages: {error:?}"))?
        {
            let root = part.root_hash.0;
            let id = message_partition_blob_id(kind, &root);
            let target = partition_path(path, &id, snapshot_storage::PartitionKind::Messages);
            write_blob(&target, |writer| {
                write_message_partition_checked(&part, kind, writer, &|| false)
            })?;
            roots.push(root);
        }
    }
    write_blob(path, |writer| {
        write_primary_snapshot_checked(&file.snapshot, &state, &manifest, writer, &|| false)
    })?;
    let block = file.snapshot.finalized_block.data();
    let header = ThreadSnapshotHeader {
        block_id: block.identifier(),
        thread_id: *block.common_section().thread_id(),
        seq_no: block.seq_no(),
        round: *block.common_section().round(),
    };
    save_to_file(&RepositoryImpl::snapshot_header_path(path), &header, true)
}

pub fn rewrite_snapshot_file<F>(input: &Path, output: &Path, mutate: F) -> anyhow::Result<()>
where
    F: FnOnce(&mut ThreadSnapshot) -> anyhow::Result<()>,
{
    let mut file = read_snapshot_file(input)?;
    mutate(file.snapshot_mut())?;
    write_snapshot_file(output, &file)
}

impl RepositoryImpl {
    pub fn import_local_snapshot(
        &mut self,
        path: &Path,
        expected: (ThreadIdentifier, BlockIdentifier),
    ) -> anyhow::Result<SnapshotImportStatus> {
        let accounts = self.thread_accounts_repository.clone();
        let configs = self.config_read();
        let staged = stage_partitioned_snapshot(
            &mut File::open(path)?,
            accounts.durable_map_repo(),
            expected,
            SnapshotImportLimits::default(),
            |id| {
                let account = partition_path(path, &id, snapshot_storage::PartitionKind::Accounts);
                let message = partition_path(path, &id, snapshot_storage::PartitionKind::Messages);
                Ok(Box::new(File::open(if account.exists() { account } else { message })?)
                    as Box<dyn Read>)
            },
            |primary| {
                super::snapshot_manifest::partition_capacity_for_block(
                    &primary.metadata.block_protocol_version_state,
                    &configs,
                )
                .map(|_| ())
            },
            &|| false,
        )?;
        self.install_partitioned_snapshot(staged, &expected.0, &|| false)
    }
}

#[cfg(test)]
mod tests {
    use super::super::snapshot_publication::publish_partitioned_snapshot;
    use super::super::snapshot_publication::tests::Fixture;
    use super::*;

    #[test]
    fn helper_roundtrip_and_local_import_preserve_accounts_and_both_queues() -> anyhow::Result<()> {
        let source = Fixture::new()?;
        let directory = tempfile::tempdir()?;
        publish_partitioned_snapshot(
            source.publication(source.pin()?, directory.path()),
            &|| false,
            |_| Ok(()),
        )?;
        let path = directory.path().join(source.state.block_id.to_string());
        let file = read_snapshot_file(&path)?;
        let state: OptimisticStateImpl = bincode::deserialize(file.snapshot().optimistic_state())?;
        assert_eq!(state.messages.root_hash(), source.state.messages.root_hash());
        assert_eq!(
            state.high_priority_messages.root_hash(),
            source.state.high_priority_messages.root_hash()
        );
        let output_directory = directory.path().join("output");
        let output = output_directory.join(hex::encode(source.state.block_id.as_array()));
        write_snapshot_file(&output, &file)?;
        assert!(output_directory.join(format!("{}.header", source.state.block_id)).is_file());
        assert!(output_directory.join(format!("{}.partitions", source.state.block_id)).is_dir());
        assert_eq!(
            read_snapshot_metadata(&output_directory)?.finalized_block().data().identifier(),
            source.state.block_id
        );
        write_snapshot_file(&output, &file)?;

        assert_eq!(
            read_primary(&output)?.manifest.account_partition_roots,
            file.manifest.account_partition_roots
        );
        let target = tempfile::tempdir()?;
        let (mut repository, _) = super::super::tests::make_test_repository(&target)?;
        repository.config_read = source.configs.clone();
        assert!(matches!(
            repository
                .import_local_snapshot(&output, (source.state.thread_id, source.state.block_id))?,
            SnapshotImportStatus::Imported
        ));
        let restored = repository
            .get_full_optimistic_state_no_cache(
                &source.state.block_id,
                &source.state.thread_id,
                None,
            )?
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
        Ok(())
    }

    #[test]
    fn metadata_read_does_not_require_partition_files() -> anyhow::Result<()> {
        let source = Fixture::new()?;
        let directory = tempfile::tempdir()?;
        publish_partitioned_snapshot(
            source.publication(source.pin()?, directory.path()),
            &|| false,
            |_| Ok(()),
        )?;
        let path = directory.path().join(source.state.block_id.to_string());
        fs::remove_dir_all(path.with_extension("partitions"))?;
        let metadata = read_snapshot_metadata(&path)?;
        assert_eq!(metadata.finalized_block().data().identifier(), source.state.block_id);
        let from_directory = read_snapshot_metadata(directory.path())?;
        assert_eq!(from_directory.finalized_block().data().identifier(), source.state.block_id);
        assert!(read_snapshot_file(&path).is_err());
        Ok(())
    }

    #[test]
    fn rewritten_accounts_import_with_new_routing_and_unchanged_accounts() -> anyhow::Result<()> {
        let source = Fixture::new()?;
        let directory = tempfile::tempdir()?;
        publish_partitioned_snapshot(
            source.publication(source.pin()?, directory.path()),
            &|| false,
            |_| Ok(()),
        )?;
        let path = directory.path().join(source.state.block_id.to_string());
        let mut file = read_snapshot_file(&path)?;
        let id = node_types::AccountIdentifier::new([2; 32]);
        let routing = id.routing(node_types::DAppIdentifier::new([42; 32]));
        let tvm_account =
            tvm_block::Account::with_address(tvm_block::MsgAddressInt::with_standart(
                None,
                0,
                tvm_types::AccountId::from_raw(id.as_slice().to_vec(), 256),
            )?);
        let body = ThreadAccount::from_tvm(tvm_block::ShardAccount::with_params(
            &tvm_account,
            [77; 32].into(),
            77,
            Some(routing.dapp_id().as_array().into()),
        )?);
        file.replace_accounts(vec![(routing, body.clone())]);
        assert!(
            write_snapshot_file(&path, &file).is_err(),
            "managed source must not be overwritten"
        );
        let output = directory.path().join("rewritten");
        write_snapshot_file(&output, &file)?;
        let rewritten = read_primary(&output)?;
        assert!(
            file.manifest
                .account_partition_roots
                .iter()
                .any(|root| rewritten.manifest.account_partition_roots.contains(root)),
            "unchanged partitions should be reused"
        );
        assert!(
            file.manifest
                .account_partition_roots
                .iter()
                .any(|root| !rewritten.manifest.account_partition_roots.contains(root)),
            "changed partitions must have new roots"
        );
        let target = tempfile::tempdir()?;
        let (mut repository, _) = super::super::tests::make_test_repository(&target)?;
        repository.config_read = source.configs.clone();
        assert!(matches!(
            repository
                .import_local_snapshot(&output, (source.state.thread_id, source.state.block_id))?,
            SnapshotImportStatus::Imported
        ));
        let restored = repository
            .get_full_optimistic_state_no_cache(
                &source.state.block_id,
                &source.state.thread_id,
                None,
            )?
            .unwrap();
        let map = restored.shard_state.0.durable.map();
        assert_eq!(map.get(&routing), Some(AccountInfo::AccountHash(body.account_hash())));
        assert_eq!(
            repository
                .thread_accounts_repository()
                .durable_map_repo()
                .state_account(&restored.shard_state.0.durable, &routing)?
                .unwrap()
                .canonical_bytes(),
            body.canonical_bytes()
        );
        assert_eq!(map.get(&id.redirect()), Some(AccountInfo::Redirect(*routing.dapp_id())));
        assert!(map.get(&id.routing(node_types::DAppIdentifier::new([0; 32]))).is_none());
        let original_id = node_types::AccountIdentifier::new([3; 32]);
        let original = original_id.routing(node_types::DAppIdentifier::new([0; 32]));
        assert_eq!(map.get(&original), source.state.shard_state.0.durable.map().get(&original));
        Ok(())
    }

    #[test]
    fn helper_and_local_import_reject_monolithic_snapshots() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("snapshot");
        let (mut repository, _) = super::super::tests::make_test_repository(&directory)?;
        for bytes in [b"SNAP2V1".to_vec(), {
            let mut bytes = b"ZSTD".to_vec();
            bytes.extend(zstd::encode_all(b"SNAP2V1".as_slice(), 3)?);
            bytes
        }] {
            fs::write(&path, bytes)?;
            assert!(read_snapshot_file(&path).is_err());
            assert!(repository
                .import_local_snapshot(
                    &path,
                    (ThreadIdentifier::default(), BlockIdentifier::default())
                )
                .is_err());
        }
        Ok(())
    }
}
