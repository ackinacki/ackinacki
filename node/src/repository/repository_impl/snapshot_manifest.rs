//! Logical manifest commitments and partition content addresses. No block-format
//! activation, storage paths or publisher identity participates in these hashes.
use account_state::AccountPartition;
use account_state::ThreadAccountMap;
use account_state::ACCOUNT_PARTITION_MAGIC;
use serde::Serialize;

use crate::config::config_read::ConfigRead;
use crate::types::thread_message_queue::partition::InboxPartition;
use crate::types::thread_message_queue::partition_codec::MessageQueueKind;
use crate::types::thread_message_queue::partition_codec::MESSAGE_PARTITION_MAGIC;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::versioning::block_protocol_version_state::BlockProtocolVersionState;

const MANIFEST_HASH_DOMAIN: &[u8] = b"ACKI_NACKI_THREAD_SNAPSHOT_MANIFEST_V1";

/// Hard limit for each SNAP2V2 root list, independently for all three kinds.
/// Local budgets may be smaller, never larger. This is not a payload-size cap.
pub(crate) const MAX_SNAPSHOT_ROOTS_PER_LIST: u64 = 65_536;

/// Resolve once at the exported block's effective protocol, never from the
/// running binary's default. Transition blocks still use their `from` config.
pub(crate) fn partition_capacity_for_block(
    version: &BlockProtocolVersionState,
    configs: &ConfigRead,
) -> anyhow::Result<u64> {
    let config = configs.get(version.to_use()).ok_or_else(|| {
        anyhow::anyhow!("No configuration for snapshot block protocol {}", version.to_use())
    })?;
    anyhow::ensure!(
        config.engine_version >= semver::Version::new(1, 0, 8),
        "partitioned snapshot publication requires engine version 1.0.8 or later"
    );
    anyhow::ensure!(
        config.snapshot_partition_size_bytes > 0,
        "snapshot partition capacity must be positive"
    );
    Ok(config.snapshot_partition_size_bytes)
}

/// Order within each list is canonical partition order, not root-hash order.
/// No primary entry or duplicate thread/block/cut/version metadata is stored.
/// Wire Vec lengths are little-endian; logical hash boundaries use u64be.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct ThreadSnapshotManifest {
    pub account_partition_roots: Vec<[u8; 32]>,
    pub normal_message_partition_roots: Vec<[u8; 32]>,
    pub high_priority_message_partition_roots: Vec<[u8; 32]>,
}

impl ThreadSnapshotManifest {
    pub(crate) fn validate_root_limits(&self, local_limit: u64) -> anyhow::Result<()> {
        for count in [
            self.account_partition_roots.len(),
            self.normal_message_partition_roots.len(),
            self.high_priority_message_partition_roots.len(),
        ] {
            check_root_limit(count, local_limit)?;
        }
        Ok(())
    }

    /// Same flattened index space as publisher scheduling and consumer planning.
    pub(crate) fn partition_blob_id(&self, index: usize) -> Option<[u8; 32]> {
        if let Some(root) = self.account_partition_roots.get(index) {
            return Some(account_partition_blob_id(root));
        }
        let index = index.checked_sub(self.account_partition_roots.len())?;
        if let Some(root) = self.normal_message_partition_roots.get(index) {
            return Some(message_partition_blob_id(MessageQueueKind::Normal, root));
        }
        let index = index.checked_sub(self.normal_message_partition_roots.len())?;
        self.high_priority_message_partition_roots
            .get(index)
            .map(|root| message_partition_blob_id(MessageQueueKind::HighPriority, root))
    }
}

/// Derive canonical roots solely from the three authoritative maps and their
/// cached capacity metadata. Does not materialize or read any account/message
/// body. Limits and capacity must come from the selected protocol configuration;
/// this helper does not assign activation values or change publication rules.
#[cfg(test)]
pub(crate) fn build_snapshot_manifest(
    accounts: &ThreadAccountMap,
    normal: &ThreadMessageQueueState,
    high_priority: &ThreadMessageQueueState,
    partition_capacity: u64,
    max_roots_per_list: u64,
) -> anyhow::Result<ThreadSnapshotManifest> {
    let (accounts, normal, high) = plan_snapshot_partitions(
        accounts,
        normal,
        high_priority,
        partition_capacity,
        max_roots_per_list,
    )?;
    Ok(manifest_from_partitions(&accounts, &normal, &high))
}

pub(crate) fn plan_snapshot_partitions(
    accounts: &ThreadAccountMap,
    normal: &ThreadMessageQueueState,
    high_priority: &ThreadMessageQueueState,
    partition_capacity: u64,
    max_roots_per_list: u64,
) -> anyhow::Result<(Vec<AccountPartition>, Vec<InboxPartition>, Vec<InboxPartition>)> {
    let accounts = accounts
        .partition_by_capacity(partition_capacity)
        .map_err(|error| anyhow::anyhow!("planning account partitions: {error:?}"))?;
    check_root_limit(accounts.len(), max_roots_per_list)?;
    let message_parts = |queue: &ThreadMessageQueueState| -> anyhow::Result<Vec<InboxPartition>> {
        let partitions = queue
            .partition_by_capacity(partition_capacity)
            .map_err(|error| anyhow::anyhow!("planning inbox partitions: {error:?}"))?;
        check_root_limit(partitions.len(), max_roots_per_list)?;
        Ok(partitions)
    };
    Ok((accounts, message_parts(normal)?, message_parts(high_priority)?))
}

pub(crate) fn manifest_from_partitions(
    accounts: &[AccountPartition],
    normal: &[InboxPartition],
    high: &[InboxPartition],
) -> ThreadSnapshotManifest {
    ThreadSnapshotManifest {
        account_partition_roots: accounts.iter().map(|part| *part.map.hash().as_array()).collect(),
        normal_message_partition_roots: normal.iter().map(|part| part.root_hash.0).collect(),
        high_priority_message_partition_roots: high.iter().map(|part| part.root_hash.0).collect(),
    }
}

fn check_root_limit(count: usize, max_roots: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        count as u64 <= max_roots.min(MAX_SNAPSHOT_ROOTS_PER_LIST),
        "snapshot manifest root limit exceeded"
    );
    Ok(())
}

/// One streaming BLAKE3 invocation over the domain followed by each ordered
/// list's u64be count and raw roots. No serializer or intermediate list hashes.
pub(crate) fn snapshot_manifest_hash(manifest: &ThreadSnapshotManifest) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(MANIFEST_HASH_DOMAIN);
    for roots in [
        &manifest.account_partition_roots,
        &manifest.normal_message_partition_roots,
        &manifest.high_priority_message_partition_roots,
    ] {
        hasher.update(&(roots.len() as u64).to_be_bytes());
        for root in roots {
            hasher.update(root);
        }
    }
    *hasher.finalize().as_bytes()
}

pub(crate) fn account_partition_blob_id(root: &[u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(ACCOUNT_PARTITION_MAGIC);
    hasher.update(root);
    *hasher.finalize().as_bytes()
}

pub(crate) fn message_partition_blob_id(kind: MessageQueueKind, root: &[u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(MESSAGE_PARTITION_MAGIC);
    hasher.update(&[match kind {
        MessageQueueKind::Normal => 0,
        MessageQueueKind::HighPriority => 1,
    }]);
    hasher.update(root);
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
#[path = "snapshot_manifest_tests.rs"]
mod tests;
