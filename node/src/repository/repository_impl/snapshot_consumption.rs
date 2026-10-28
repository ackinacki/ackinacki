//! Local SNAP2V2 assembly. No HTTP, runtime activation or automatic state commit.
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::Read;

use account_state::read_account_partition_checked;
use account_state::AccountPartition;
use account_state::DurableThreadAccountsRepository;
use account_state::DurableThreadAccountsState;
use account_state::StagedArchiveEpoch;
use account_state::ThreadAccountMap;
use multi_map::MapHash;
use multi_map::MapKeyPath;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::ThreadAccountsHash;
use node_types::ThreadIdentifier;

use super::snapshot_manifest::account_partition_blob_id;
use super::snapshot_manifest::message_partition_blob_id;
#[cfg(test)]
use super::snapshot_partitioned::read_primary_snapshot_checked;
use super::snapshot_partitioned::PrimaryThreadSnapshot;
#[cfg(test)]
use crate::node::services::sync::snapshot_compression::COMPRESSED_SNAPSHOT_MAGIC;
use crate::node::services::sync::snapshot_compression::PARTITION_WINDOW_LOG;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::types::thread_message_queue::partition_codec::read_message_partition_checked;
use crate::types::thread_message_queue::partition_codec::MessagePartitionImportLimits;
use crate::types::thread_message_queue::partition_codec::MessageQueueKind;
use crate::types::thread_message_queue::ThreadMessageQueueState;

/// Bounded working batches and structural limits supplied by the import owner.
#[derive(Clone, Copy)]
pub(crate) struct SnapshotImportLimits {
    /// Local budget, additionally capped by SNAP2V2's per-list protocol maximum.
    pub roots_per_list: u64,
    pub messages: MessagePartitionImportLimits,
    pub accounts_per_batch: usize,
    pub account_bytes_per_batch: u64,
}

/// A provider must call read exactly once, without retrying its side effects.
/// HTTP retries finish before this boundary; staging and root verification are
/// one pass. The provider accepts a private download into cache only on success.
pub(crate) trait SnapshotPartitionSource {
    /// True only for immutable files fully checked by the transport/cache layer.
    /// Local/general resolvers default to checking chunks while decoding.
    fn chunks_verified(&self) -> bool {
        false
    }

    fn read_partition<T>(
        &mut self,
        id: [u8; 32],
        read: impl FnOnce(&mut dyn Read) -> anyhow::Result<T>,
    ) -> anyhow::Result<T>;

    /// Read any available requested blob exactly once. The value identifies its
    /// manifest position, not arrival order. Local resolvers retain manifest order;
    /// HTTP sources override this to avoid waiting behind a slow earlier transfer.
    fn read_any_partition<T>(
        &mut self,
        pending: &BTreeMap<[u8; 32], usize>,
        read: impl FnOnce(usize, &mut dyn Read) -> anyhow::Result<T>,
    ) -> anyhow::Result<([u8; 32], T)> {
        let (&id, &index) = pending
            .iter()
            .min_by_key(|(_, index)| **index)
            .ok_or_else(|| anyhow::anyhow!("no snapshot partitions requested"))?;
        let value = self.read_partition(id, |blob| read(index, blob))?;
        Ok((id, value))
    }
}

impl<F> SnapshotPartitionSource for F
where
    F: FnMut([u8; 32]) -> anyhow::Result<Box<dyn Read>>,
{
    fn read_partition<T>(
        &mut self,
        id: [u8; 32],
        read: impl FnOnce(&mut dyn Read) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let mut blob = self(id)?;
        read(&mut blob)
    }
}

/// Verified local contents in a private archive epoch. Active archive controls
/// and repository state are untouched. Drop aborts the epoch, including after
/// cancellation or unwinding. As with existing snapshot imports, the caller
/// must serialize staging/handoff: epochs are not independently reserved slots.
pub(crate) struct StagedPartitionedSnapshot<'a> {
    repository: Cow<'a, DurableThreadAccountsRepository>,
    staged: Option<StagedArchiveEpoch>,
    ready: Option<(PrimaryThreadSnapshot, OptimisticStateImpl)>,
}

impl StagedPartitionedSnapshot<'_> {
    pub(super) fn belongs_to(&self, repository: &DurableThreadAccountsRepository) -> bool {
        std::ptr::eq(self.repository.as_ref(), repository)
    }

    /// Move the cleanup guard across spawn_blocking without extending a borrowed
    /// repository's lifetime. Repository clones share the same archive/accumulator.
    pub(super) fn into_owned(mut self) -> StagedPartitionedSnapshot<'static> {
        StagedPartitionedSnapshot {
            repository: Cow::Owned(self.repository.as_ref().clone()),
            staged: self.staged.take(),
            ready: self.ready.take(),
        }
    }

    /// Reattach a completed async handoff to the original request's repository.
    /// The import owner calls this only with the repository it cloned for staging.
    pub(super) fn with_repository<'a>(
        mut self,
        repository: &'a DurableThreadAccountsRepository,
    ) -> StagedPartitionedSnapshot<'a> {
        StagedPartitionedSnapshot {
            repository: Cow::Borrowed(repository),
            staged: self.staged.take(),
            ready: self.ready.take(),
        }
    }

    #[cfg(test)]
    pub(crate) fn primary(&self) -> &PrimaryThreadSnapshot {
        &self.ready.as_ref().expect("completed snapshot assembly").0
    }

    pub(crate) fn state(&self) -> &OptimisticStateImpl {
        &self.ready.as_ref().expect("completed snapshot assembly").1
    }

    /// Transfer cleanup ownership to the repository's commit/recovery workflow.
    /// This does NOT publish anything. The owner must recheck import eligibility,
    /// drain the accumulator, persist recovery metadata and commit/apply under
    /// its existing finalization gate, or abort the returned staged epoch.
    pub(crate) fn into_parts(
        mut self,
    ) -> (PrimaryThreadSnapshot, OptimisticStateImpl, StagedArchiveEpoch) {
        let (primary, state) = self.ready.take().expect("completed snapshot assembly");
        (primary, state, self.staged.take().expect("owned staged epoch"))
    }
}

impl Drop for StagedPartitionedSnapshot<'_> {
    fn drop(&mut self) {
        if let Some(staged) = self.staged.take() {
            self.repository.abort_staged_import(staged);
        }
    }
}

/// Read independently compressed blobs from a local resolver. Primary is checked
/// against the requested (thread, block), then verify_primary MUST establish its
/// trust/protocol/proofs before any staging or partition lookup. Codec success
/// is not consensus proof verification. Until final block commitments are wired,
/// callers must not treat an unauthenticated manifest as a trusted state root.
///
/// Partition roots authenticate decoded leaves, rolling inbox chains authenticate
/// visible messages, and canonical ranges checked in manifest order reject
/// overlap/reordered manifests independently of download completion order.
/// Complete maps are assembled with checked disjoint merges, not last-write-wins.
/// Only the supported full-scope single-thread runtime is accepted here.
#[cfg(test)]
pub(crate) fn stage_partitioned_snapshot<'a, O, V, C>(
    primary_blob: &mut dyn Read,
    repository: &'a DurableThreadAccountsRepository,
    expected: (ThreadIdentifier, BlockIdentifier),
    limits: SnapshotImportLimits,
    open_partition: O,
    verify_primary: V,
    should_cancel: &C,
) -> anyhow::Result<StagedPartitionedSnapshot<'a>>
where
    O: FnMut([u8; 32]) -> anyhow::Result<Box<dyn Read>>,
    V: FnOnce(&PrimaryThreadSnapshot) -> anyhow::Result<()>,
    C: Fn() -> bool,
{
    check_cancel(should_cancel)?;
    anyhow::ensure!(
        limits.accounts_per_batch > 0 && limits.account_bytes_per_batch > 0,
        "snapshot import batch limits must be non-zero"
    );
    let primary = decode_blob(primary_blob, should_cancel, |reader| {
        read_primary_snapshot_checked(reader, limits.roots_per_list, should_cancel)
    })?;
    check_primary_identity(&primary, expected)?;
    verify_primary(&primary)?;
    stage_verified_snapshot(primary, repository, limits, open_partition, should_cancel)
}

pub(super) fn check_primary_identity(
    primary: &PrimaryThreadSnapshot,
    expected: (ThreadIdentifier, BlockIdentifier),
) -> anyhow::Result<()> {
    anyhow::ensure!(
        (primary.metadata.state.thread_id, primary.metadata.state.block_id) == expected,
        "snapshot primary does not match requested thread/block"
    );
    anyhow::ensure!(expected.0 == ThreadIdentifier::default(), "unsupported snapshot thread scope");
    Ok(())
}

/// Shared assembly after identity/protocol/proof verification. Both local and
/// HTTP frontends use this path; no network retry can re-run a staging callback.
pub(super) fn stage_verified_snapshot<'a, S: SnapshotPartitionSource, C: Fn() -> bool>(
    primary: PrimaryThreadSnapshot,
    repository: &'a DurableThreadAccountsRepository,
    limits: SnapshotImportLimits,
    mut partitions: S,
    should_cancel: &C,
) -> anyhow::Result<StagedPartitionedSnapshot<'a>> {
    check_cancel(should_cancel)?;
    primary.manifest.validate_root_limits(limits.roots_per_list)?;
    let expected = (primary.metadata.state.thread_id, primary.metadata.state.block_id);
    let activity_slot = expected.0.to_string();
    let phase = |name| {
        crate::helper::metrics::ActivityMetric::parallel("snapshot_import", name, &activity_slot)
            .enter()
    };

    let staged = repository.begin_staged_import_after_current_epoch();
    let mut result = StagedPartitionedSnapshot {
        repository: Cow::Borrowed(repository),
        staged: Some(staged),
        ready: None,
    };
    // Summary phases include file waits, validation, archive writes and final
    // assembly. Their even/odd siblings cover actual partition processing only,
    // so gaps expose transport starvation without per-blob metric cardinality.
    let accounts_activity = phase("2_accounts");
    let mut account_parts = Vec::new();
    let mut total_account_bytes = 0;
    let mut account_bounds = vec![None; primary.manifest.account_partition_roots.len()];
    let mut batch = Vec::new();
    let mut batch_bytes = 0u64;
    let flush = |batch: &mut Vec<_>| -> anyhow::Result<()> {
        check_cancel(should_cancel)?;
        if !batch.is_empty() {
            repository.append_staged_snapshot_accounts(
                staged,
                &expected.0,
                &expected.1,
                batch.drain(..).map(Ok),
            )?;
        }
        Ok(())
    };
    let mut pending = partition_requests(
        primary.manifest.account_partition_roots.iter().map(account_partition_blob_id),
    )?;
    let chunks_verified = partitions.chunks_verified();
    while !pending.is_empty() {
        check_cancel(should_cancel)?;
        let (id, map) = partitions.read_any_partition(&pending, |index, blob| {
            // Alternate by import order, not manifest index: files arrive in
            // arbitrary order and adjacent indexes need not alternate parity.
            let _partition_activity = phase(if account_parts.len() % 2 == 0 {
                "2_accounts_even"
            } else {
                "2_accounts_odd"
            });
            let root = &primary.manifest.account_partition_roots[index];
            let mut first = None;
            let mut previous_account = None;
            let map =
                decode_blob_with_transport(blob, should_cancel, chunks_verified, |mut reader| {
                    read_account_partition_checked(
                        &mut reader,
                        MapKeyPath::default(),
                        ThreadAccountsHash::new(*root),
                        should_cancel,
                        |routing, account| {
                            check_order(&mut previous_account, routing)?;
                            first.get_or_insert(routing);
                            let size = account.canonical_size();
                            add_bytes(&mut total_account_bytes, size)?;
                            if !batch.is_empty()
                                && (batch.len() >= limits.accounts_per_batch
                                    || size
                                        > limits
                                            .account_bytes_per_batch
                                            .saturating_sub(batch_bytes))
                            {
                                flush(&mut batch)?;
                                batch_bytes = 0;
                            }
                            batch.push((routing, account.into_canonical_bytes().into_vec()));
                            batch_bytes = batch_bytes.checked_add(size).ok_or_else(|| {
                                anyhow::anyhow!("snapshot batch capacity overflow")
                            })?;
                            Ok(())
                        },
                    )
                })?;
            account_bounds[index] = first.zip(previous_account);
            Ok(map)
        })?;
        anyhow::ensure!(pending.remove(&id).is_some(), "unexpected account partition");
        account_parts.push(AccountPartition { capacity: map.capacity().bytes(), map });
    }
    check_partition_bounds(&account_bounds)?;
    flush(&mut batch)?;
    let map = ThreadAccountMap::merge_partitions(&account_parts)
        .map_err(|error| anyhow::anyhow!("merging account partitions: {error:?}"))?;
    anyhow::ensure!(map.capacity().bytes() == total_account_bytes, "account capacity mismatch");
    drop(account_parts);
    drop(accounts_activity);

    let messages_activity = phase("3_messages");
    let mut total_message_bytes = 0;
    // Both priorities belong to the message phase. A slow normal-queue download
    // must not prevent importing a ready high-priority partition (or vice versa).
    let message_roots: Vec<_> = primary
        .manifest
        .normal_message_partition_roots
        .iter()
        .map(|root| (MessageQueueKind::Normal, root))
        .chain(
            primary
                .manifest
                .high_priority_message_partition_roots
                .iter()
                .map(|root| (MessageQueueKind::HighPriority, root)),
        )
        .collect();
    let mut pending = partition_requests(
        message_roots.iter().map(|(kind, root)| message_partition_blob_id(*kind, root)),
    )?;
    let mut normal_parts = Vec::new();
    let mut high_parts = Vec::new();
    let mut message_bounds = vec![None; message_roots.len()];
    while !pending.is_empty() {
        check_cancel(should_cancel)?;
        let (id, (index, kind, part)) =
            partitions.read_any_partition(&pending, |index, blob| {
                let _partition_activity =
                    phase(if (normal_parts.len() + high_parts.len()) % 2 == 0 {
                        "3_messages_even"
                    } else {
                        "3_messages_odd"
                    });
                let (kind, root) = message_roots[index];
                let part =
                    decode_blob_with_transport(blob, should_cancel, chunks_verified, |reader| {
                        read_message_partition_checked(
                            reader,
                            kind,
                            MapHash(*root),
                            limits.messages,
                            should_cancel,
                        )
                    })?;
                add_bytes(&mut total_message_bytes, part.capacity().bytes())?;
                let mut first = None;
                let mut previous = None;
                for (_, dapp) in part.messages.iter() {
                    for routing in dapp.messages.keys() {
                        check_cancel(should_cancel)?;
                        check_order(&mut previous, routing)?;
                        first.get_or_insert(routing);
                    }
                }
                message_bounds[index] = first.zip(previous);
                Ok((index, kind, part))
            })?;
        anyhow::ensure!(pending.remove(&id).is_some(), "unexpected message partition");
        match kind {
            MessageQueueKind::Normal => normal_parts.push((index, part)),
            MessageQueueKind::HighPriority => high_parts.push((index, part)),
        }
    }
    let normal_count = primary.manifest.normal_message_partition_roots.len();
    check_partition_bounds(&message_bounds[..normal_count])?;
    check_partition_bounds(&message_bounds[normal_count..])?;
    let merge_queue = |mut parts: Vec<(usize, ThreadMessageQueueState)>| {
        parts.sort_unstable_by_key(|(index, _)| *index);
        let parts: Vec<_> = parts.into_iter().map(|(_, part)| part).collect();
        ThreadMessageQueueState::merge_partitions(&parts)
            .map_err(|error| anyhow::anyhow!("merging inbox partitions: {error:?}"))
    };
    let messages = merge_queue(normal_parts)?;
    let high = merge_queue(high_parts)?;
    drop(messages_activity);
    check_cancel(should_cancel)?;

    let source = &primary.metadata.state;
    let mut state = OptimisticStateImpl::zero();
    state.block_id = source.block_id;
    state.block_seq_no = source.block_seq_no;
    state.thread_id = source.thread_id;
    state.block_info = source.block_info.clone();
    state.threads_table = source.threads_table.clone();
    state.thread_refs_state = source.thread_refs_state.clone();
    state.cropped = source.cropped.clone();
    state.shard_state = source.shard_state.clone();
    #[cfg(feature = "monitor-accounts-number")]
    {
        state.accounts_number = map
            .iter()
            .filter(|(_, info)| matches!(info, account_state::AccountInfo::AccountHash(_)))
            .count() as u64;
    }
    state.shard_state.0.durable = DurableThreadAccountsState::with_map(map);
    state.messages = messages;
    state.high_priority_messages = high;
    result.ready = Some((primary, state));
    Ok(result)
}

fn check_order(
    previous: &mut Option<AccountRouting>,
    routing: AccountRouting,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        previous.is_none_or(|previous| previous < routing),
        "snapshot partitions overlap or are out of canonical order"
    );
    *previous = Some(routing);
    Ok(())
}

fn partition_requests(
    ids: impl Iterator<Item = [u8; 32]>,
) -> anyhow::Result<BTreeMap<[u8; 32], usize>> {
    let mut pending = BTreeMap::new();
    for (index, id) in ids.enumerate() {
        anyhow::ensure!(pending.insert(id, index).is_none(), "duplicate snapshot partition");
    }
    Ok(pending)
}

/// Arrival order is arbitrary; canonical ranges are checked in manifest order.
/// Keep only two keys per partition, not a set of every imported account/inbox.
fn check_partition_bounds(
    bounds: &[Option<(AccountRouting, AccountRouting)>],
) -> anyhow::Result<()> {
    let mut previous = None;
    for range in bounds {
        let (first, last) = range.ok_or_else(|| anyhow::anyhow!("empty snapshot partition"))?;
        check_order(&mut previous, first)?;
        previous = Some(last);
    }
    Ok(())
}

fn add_bytes(total: &mut u64, size: u64) -> anyhow::Result<()> {
    *total =
        total.checked_add(size).ok_or_else(|| anyhow::anyhow!("snapshot capacity overflow"))?;
    Ok(())
}

/// Require exactly one complete compressed frame per independent blob, including
/// EOF after it. The codec must consume its whole uncompressed payload.
#[cfg(test)]
pub(super) fn decode_blob<T, C: Fn() -> bool>(
    input: &mut dyn Read,
    should_cancel: &C,
    read_payload: impl FnOnce(&mut dyn Read) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    decode_blob_with_transport(input, should_cancel, false, read_payload)
}

/// Transport verification does not replace semantic validation: both paths
/// enforce one complete zstd frame, trailer bounds, and the payload codec/root.
pub(super) fn decode_blob_with_transport<T, C: Fn() -> bool>(
    input: &mut dyn Read,
    should_cancel: &C,
    chunks_verified: bool,
    read_payload: impl FnOnce(&mut dyn Read) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    let input = CancelReader { input, should_cancel };
    let input = if chunks_verified {
        snapshot_storage::chunks::PayloadReader::new_preverified(input)?
    } else {
        snapshot_storage::chunks::PayloadReader::new(input)?
    };
    let mut decoder = zstd::Decoder::with_buffer(input)?.single_frame();
    decoder.window_log_max(PARTITION_WINDOW_LOG)?;
    let result = read_payload(&mut decoder)?;
    anyhow::ensure!(decoder.read(&mut [0])? == 0, "trailing snapshot payload bytes");
    decoder.finish().verify_trailer()?;
    check_cancel(should_cancel)?;
    Ok(result)
}

struct CancelReader<'a, C> {
    input: &'a mut dyn Read,
    should_cancel: &'a C,
}
impl<C: Fn() -> bool> Read for CancelReader<'_, C> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        if (self.should_cancel)() {
            return Err(std::io::Error::other("snapshot consumption cancelled"));
        }
        self.input.read(bytes)
    }
}

fn check_cancel<C: Fn() -> bool>(should_cancel: &C) -> anyhow::Result<()> {
    anyhow::ensure!(!should_cancel(), "snapshot consumption cancelled");
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_consumption_tests.rs"]
pub(crate) mod tests;
