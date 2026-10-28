//! Capacity planning over DApp/account keys. Whole inboxes are indivisible.
//! Planning reads cached Merkle nodes and capacities, never message bodies/DB.
use std::sync::Arc;

use multi_map::node::NodeAug;
use multi_map::partition_nested_by_capacity;
use multi_map::CapacityPartitionError;
use multi_map::MapHash;
use multi_map::MapKeyPath;
use multi_map::MultiMapAug;
use multi_map::MultiMapValue;
use node_types::Blake3Hashable;
use node_types::DAppIdentifier;

use super::InboxCapacity;
use super::ThreadMessageQueueState;
use crate::types::AccountInbox;

pub type InboxPartitionMap = MultiMapAug<Arc<AccountInbox>, InboxCapacity>;

/// Sparse L2 maps in canonical DApp order, with the corresponding L1 root.
/// No scheduler indexes or message-body copies are needed to derive this root.
pub struct InboxPartition {
    pub capacity: u64,
    pub root_hash: MapHash,
    pub dapps: Vec<(DAppIdentifier, InboxPartitionMap)>,
}

// A DApp leaf commits only to its L2 root, exactly like DAppMessageQueueState.
// Used to derive the L1 partition root without reconstructing scheduler lengths.
#[derive(Clone)]
struct DAppInboxRoot([u8; 32]);

impl Blake3Hashable for DAppInboxRoot {
    fn update_hasher(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&self.0);
    }
}

impl MultiMapValue for DAppInboxRoot {
    fn write_value<W: std::io::Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        Ok(writer.write_all(&self.0)?)
    }

    fn read_value<R: std::io::Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut root = [0; 32];
        reader.read_exact(&mut root)?;
        Ok(Self(root))
    }
}

pub(super) fn inbox_partition_root(dapps: &[(DAppIdentifier, InboxPartitionMap)]) -> MapHash {
    let leaves: Vec<_> = dapps
        .iter()
        .map(|(id, map)| MultiMapAug {
            root: NodeAug::new_leaf_aug(DAppInboxRoot(map.root.hash()), map.root.augmentation()),
            root_path: MapKeyPath { prefix: multi_map::MapKey(*id.as_array()), len: 256 },
        })
        .collect();
    let map = MultiMapAug::new().graft_batch(&leaves);
    MapHash(map.root.hash())
}

impl ThreadMessageQueueState {
    /// Checked union of sparse partition queues. L2 scopes for the same DApp
    /// merge without copying inbox/message bodies. Overlap is an error; queue
    /// scheduling cursors are local metadata and restart at the first key.
    pub(crate) fn merge_partitions(parts: &[Self]) -> Result<Self, multi_map::DisjointMergeError> {
        let mut dapps = std::collections::BTreeMap::<_, Vec<_>>::new();
        for part in parts {
            for (id, dapp) in part.messages.iter() {
                dapps.entry(id).or_default().push(&dapp.messages);
            }
        }
        let mut result = Self::empty();
        for (id, maps) in dapps {
            let mut dapp = super::DAppMessageQueueState::empty();
            dapp.messages = super::maps::QueueMap::merge_disjoint(&maps)?;
            dapp.normalize_cursor();
            result.messages.insert(id, Arc::new(dapp));
        }
        result.normalize_cursor();
        Ok(result)
    }

    pub fn capacity(&self) -> InboxCapacity {
        self.messages.capacity()
    }

    /// Shared 90%-fill / <10%-tail policy; complete inboxes are indivisible.
    /// A large DApp can span partitions; a partition can cross DApp boundaries.
    pub fn partition_by_capacity(
        &self,
        target: u64,
    ) -> Result<Vec<InboxPartition>, CapacityPartitionError> {
        let maps = self.messages.map.iter().map(|(key, dapp)| (key, &dapp.messages.map));
        let planned =
            partition_nested_by_capacity(maps, target, |capacity| Some(capacity.bytes()))?;

        Ok(planned
            .into_iter()
            .map(|part| {
                let mut dapps = Vec::with_capacity(part.maps.len());
                for (key, map) in part.maps {
                    dapps.push((DAppIdentifier::new(key.0), map));
                }
                let root_hash = inbox_partition_root(&dapps);
                InboxPartition { capacity: part.capacity, root_hash, dapps }
            })
            .collect())
    }
}
