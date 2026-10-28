//! Account adapters for the shared multi-map capacity planner.
use std::collections::BTreeMap;

use multi_map::node::NodeAug;
use multi_map::partition_nested_by_capacity;
use multi_map::CapacityPartitionError;
use multi_map::DisjointMergeError;
use multi_map::MapKey;
use multi_map::MapKeyPath;
use multi_map::MultiMapAug;

use super::maps::L1MapValue;
use super::AccountCapacity;
use super::AccountInfo;
use super::ThreadAccountMap;

type AccountMap = MultiMapAug<AccountInfo, AccountCapacity>;

/// One sparse two-level map, rooted in the same L1 scope as the source.
#[derive(Clone, Debug)]
pub struct AccountPartition {
    pub map: ThreadAccountMap,
    pub capacity: u64,
}

impl ThreadAccountMap {
    /// Partition across complete AccountRouting keys without reading bodies.
    /// Uses the shared 90%-fill / <10%-tail policy; an account is indivisible.
    pub fn partition_by_capacity(
        &self,
        target: u64,
    ) -> Result<Vec<AccountPartition>, CapacityPartitionError> {
        let maps = self.dapps.iter().map(|(key, dapp)| (key, &dapp.0));
        let planned =
            partition_nested_by_capacity(maps, target, |capacity| Some(capacity.bytes()))?;

        Ok(planned
            .into_iter()
            .map(|part| {
                let leaves: Vec<_> = part
                    .maps
                    .into_iter()
                    .map(|(key, inner)| {
                        let capacity = inner.root.augmentation();
                        MultiMapAug {
                            root: NodeAug::new_leaf_aug(L1MapValue::new(inner), capacity),
                            root_path: MapKeyPath { prefix: key, len: 256 },
                        }
                    })
                    .collect();
                let empty = MultiMapAug { root: NodeAug::Empty, root_path: self.dapps.root_path };
                AccountPartition {
                    map: Self { dapps: empty.graft_batch(&leaves) },
                    capacity: part.capacity,
                }
            })
            .collect())
    }

    /// Checked union, NOT prefix replacement. Partial maps for the same DApp
    /// are merged at L2 before L1 is rebuilt. Partition order does not matter.
    /// An empty list yields an empty full-root map.
    pub fn merge_partitions(
        partitions: &[AccountPartition],
    ) -> Result<ThreadAccountMap, DisjointMergeError> {
        let Some(first) = partitions.first() else {
            return Ok(Self::new());
        };

        let mut dapps: BTreeMap<MapKey, Vec<AccountMap>> = BTreeMap::new();
        for partition in partitions {
            if partition.map.dapps.root_path != first.map.dapps.root_path {
                return Err(DisjointMergeError::IncompatibleRootPath);
            }
            if partition.capacity != partition.map.capacity().bytes() {
                return Err(DisjointMergeError::MalformedTree);
            }
            for (key, dapp) in partition.map.dapps.iter() {
                dapps.entry(key).or_default().push(dapp.0.clone());
            }
        }
        let mut leaves = Vec::with_capacity(dapps.len());
        for (key, maps) in dapps {
            let map = MultiMapAug::merge_disjoint_batch(&maps)?;
            let capacity = map.root.augmentation();
            leaves.push(MultiMapAug {
                root: NodeAug::new_leaf_aug(L1MapValue::new(map), capacity),
                root_path: MapKeyPath { prefix: key, len: 256 },
            });
        }
        let empty = MultiMapAug { root: NodeAug::Empty, root_path: first.map.dapps.root_path };
        Ok(Self { dapps: empty.graft_batch(&leaves) })
    }
}

#[cfg(test)]
#[path = "partition_tests.rs"]
mod tests;
