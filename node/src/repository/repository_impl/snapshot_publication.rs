//! Local publication kernel; production worker and network-format cutover remain pending.
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;

use account_state::write_account_partition_checked;
use account_state::DurableThreadAccountsRepository;
use account_state::PartitionReadLimits;
use account_state::PinHandle;
use snapshot_storage::Publication;
use snapshot_storage::RetentionPolicy;
use snapshot_storage::SnapshotResources;

use super::snapshot_manifest::account_partition_blob_id;
use super::snapshot_manifest::manifest_from_partitions;
use super::snapshot_manifest::message_partition_blob_id;
use super::snapshot_manifest::partition_capacity_for_block;
use super::snapshot_manifest::plan_snapshot_partitions;
use super::snapshot_manifest::ThreadSnapshotManifest;
use super::snapshot_partitioned::write_primary_snapshot_checked;
use super::snapshot_schedule::manifest_partition_count;
use super::snapshot_schedule::partition_publication_order;
use super::snapshot_schedule::rank_snapshot_publishers;
use super::ThreadSnapshot;
use super::ThreadSnapshotHeader;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::config::config_read::ConfigRead;
use crate::node::services::sync::snapshot_compression::write_partition_blob;
use crate::node::NodeIdentifier;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::types::thread_message_queue::partition_codec::write_message_partition_checked;
use crate::types::thread_message_queue::partition_codec::MessageQueueKind;

/// One publication attempt. The owned whole-thread pin must have been acquired
/// from account_repository's parent repository for this state's block boundary.
/// Capacity is resolved once from the block's effective protocol in the captured
/// config registry. Root limits remain explicit; no defaults are assigned here.
/// Account read limits are local operational settings.
/// The destination directory must already exist; its creation/lifecycle belongs
/// to the storage owner, not an individual publication attempt.
pub(crate) struct PartitionedSnapshotPublication<'a> {
    pub snapshot: &'a ThreadSnapshot,
    pub state: &'a OptimisticStateImpl,
    pub account_repository: &'a DurableThreadAccountsRepository,
    pub pin: PinHandle,
    pub publisher: &'a NodeIdentifier,
    pub directory: &'a Path,
    pub configs: ConfigRead,
    /// Optional stricter operational budget; cannot exceed the wire hard limit.
    pub max_roots_per_list: u64,
    pub account_read_limits: PartitionReadLimits,
    pub retention: RetentionPolicy,
}

/// Publish the primary, then lazily materialize partition bodies in this node's
/// frozen schedule. on_published runs only after complete durable publication,
/// with the flat hex resource name used under /v2/storage/. It may announce
/// progress or stop the attempt by returning an error. The pin is still held
/// during every callback, including the last one.
///
/// Failure/cancellation drops the pin and the current temporary file. Previously
/// completed blobs stay available for other consumers/publications to reuse.
pub(crate) fn publish_partitioned_snapshot<C, F>(
    publication: PartitionedSnapshotPublication<'_>,
    should_cancel: &C,
    mut on_published: F,
) -> anyhow::Result<ThreadSnapshotManifest>
where
    C: Fn() -> bool + Sync,
    F: FnMut(&str) -> anyhow::Result<()>,
{
    let p = &publication;
    let activity_slot = p.state.thread_id.to_string();
    let phase = |name| {
        crate::helper::metrics::ActivityMetric::parallel("snapshot_export", name, &activity_slot)
            .enter()
    };
    check_cancel(should_cancel)?;
    anyhow::ensure!(
        p.pin.anchor_block_id() == &p.state.block_id
            && p.pin.anchor_thread_id() == &p.state.thread_id,
        "publication pin/state boundary mismatch"
    );
    let partition_capacity =
        partition_capacity_for_block(&p.snapshot.block_protocol_version_state, &p.configs)?;
    let (accounts, normal, high) = plan_snapshot_partitions(
        p.state.shard_state.0.durable.map(),
        &p.state.messages,
        &p.state.high_priority_messages,
        partition_capacity,
        p.max_roots_per_list,
    )?;
    let manifest = manifest_from_partitions(&accounts, &normal, &high);
    let publishers = rank_snapshot_publishers(&p.snapshot.bk_set, &manifest)?;
    let rank = publishers
        .iter()
        .position(|node| node == p.publisher)
        .ok_or_else(|| anyhow::anyhow!("snapshot publisher is not in the committed BK set"))?;
    let order =
        partition_publication_order(manifest_partition_count(&manifest), rank, publishers.len())?;
    check_cancel(should_cancel)?;
    let store = SnapshotResources::open(p.directory)?;
    let resources = store.begin_snapshot(
        *p.state.block_id.as_array(),
        (0..manifest_partition_count(&manifest))
            .map(|index| manifest.partition_blob_id(index).expect("planned partition")),
        p.state.thread_id.as_ref(),
        u64::from(u32::from(p.state.block_seq_no)),
    )?;
    // The primary is block-addressed, never a member of the manifest lists.
    let name =
        publish_blob(&resources, p.state.block_id.as_array(), None, should_cancel, |writer| {
            write_primary_snapshot_checked(p.snapshot, p.state, &manifest, writer, should_cancel)
        })?;
    let block = p.snapshot.finalized_block.data();
    resources.publish_header(&bincode::serialize(&ThreadSnapshotHeader {
        block_id: block.identifier(),
        thread_id: p.state.thread_id,
        seq_no: block.seq_no(),
        round: *block.common_section().round(),
    })?)?;
    on_published(&name)?;
    // Alternate by actual publication order, not manifest index: the scheduled
    // stride may visit only even or only odd indexes in the first wave. Fixed
    // parity labels keep adjacent partitions visually separate without creating
    // a new metric series for every partition.
    // A summary spans the first through last partition of its type, including
    // intervening work of the other type. Start lazily: empty queues must not
    // produce an activity, and cancellation/error drops every active guard.
    let mut accounts_activity = None;
    let mut messages_activity = None;
    let mut accounts_remaining = accounts.len();
    let mut messages_remaining = normal.len() + high.len();
    for (ordinal, index) in order.enumerate() {
        check_cancel(should_cancel)?;
        let name = if index < accounts.len() {
            accounts_activity.get_or_insert_with(|| phase("1_accounts"));
            let _activity =
                phase(if ordinal % 2 == 0 { "1_accounts_even" } else { "1_accounts_odd" });
            let part = &accounts[index];
            let id = account_partition_blob_id(&manifest.account_partition_roots[index]);
            publish_blob(
                &resources,
                &id,
                Some(snapshot_storage::PartitionKind::Accounts),
                should_cancel,
                |mut writer| {
                    write_account_partition_checked(
                        p.account_repository,
                        &part.map,
                        &mut writer,
                        p.account_read_limits,
                        should_cancel,
                    )
                },
            )?
        } else {
            messages_activity.get_or_insert_with(|| phase("2_messages"));
            let _activity =
                phase(if ordinal % 2 == 0 { "2_messages_even" } else { "2_messages_odd" });
            let index = index - accounts.len();
            let (part, kind, root) = if index < normal.len() {
                (
                    &normal[index],
                    MessageQueueKind::Normal,
                    &manifest.normal_message_partition_roots[index],
                )
            } else {
                let index = index - normal.len();
                (
                    &high[index],
                    MessageQueueKind::HighPriority,
                    &manifest.high_priority_message_partition_roots[index],
                )
            };
            let id = message_partition_blob_id(kind, root);
            publish_blob(
                &resources,
                &id,
                Some(snapshot_storage::PartitionKind::Messages),
                should_cancel,
                |writer| write_message_partition_checked(part, kind, writer, should_cancel),
            )?
        };
        on_published(&name)?;
        if index < accounts.len() {
            accounts_remaining -= 1;
            if accounts_remaining == 0 {
                drop(accounts_activity.take());
            }
        } else {
            messages_remaining -= 1;
            if messages_remaining == 0 {
                drop(messages_activity.take());
            }
        }
    }
    check_cancel(should_cancel)?;
    // Storage cleanup failure must not invalidate already announced blobs.
    if let Err(error) = store.retain_recent(p.retention, std::time::SystemTime::now()) {
        tracing::warn!("snapshot resource retention failed: {error}");
    }
    // Explicitly keep pin ownership through all writes/callbacks and checks.
    drop(publication);
    Ok(manifest)
}

/// Atomic immutable-file publication. ID bytes, not caller-provided paths,
/// determine the final basename. No partially encoded blob is exposed there.
fn publish_blob<C, F>(
    resources: &Publication,
    id: &[u8; 32],
    kind: Option<snapshot_storage::PartitionKind>,
    should_cancel: &C,
    write_payload: F,
) -> anyhow::Result<String>
where
    C: Fn() -> bool,
    F: FnOnce(&mut dyn Write) -> anyhow::Result<()>,
{
    check_cancel(should_cancel)?;
    let write = |output: &mut dyn Write| {
        let mut buffered = BufWriter::with_capacity(64 * 1024, output);
        let writer = CancellableWriter { inner: &mut buffered, should_cancel };
        write_partition_blob(writer, write_payload)?;
        buffered.flush()?;
        check_cancel(should_cancel)?;
        Ok(())
    };
    match kind {
        Some(kind) => resources.publish_partition_checked(*id, kind, write, should_cancel),
        None => resources.publish_checked(*id, write, should_cancel),
    }
}

struct CancellableWriter<'a, W, C> {
    inner: W,
    should_cancel: &'a C,
}
impl<W: Write, C: Fn() -> bool> Write for CancellableWriter<'_, W, C> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        // Interrupted would make write_all retry a permanent cancellation.
        if (self.should_cancel)() {
            return Err(std::io::Error::other("snapshot publication cancelled"));
        }
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if (self.should_cancel)() {
            return Err(std::io::Error::other("snapshot publication cancelled"));
        }
        self.inner.flush()
    }
}

fn check_cancel<C: Fn() -> bool>(should_cancel: &C) -> anyhow::Result<()> {
    anyhow::ensure!(!should_cancel(), "snapshot publication cancelled");
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_publication_tests.rs"]
pub(crate) mod tests;
