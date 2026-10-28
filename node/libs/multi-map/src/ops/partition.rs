use crate::node::NodeAug;
use crate::MapKey;
use crate::MapKeyPath;
use crate::MultiMapAug;
use crate::MultiMapAugmentation;
use crate::MultiMapValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CapacityPartitionError {
    InvalidPartitionCapacity,
    InvalidAugmentation,
    InvalidKeyOrder,
}

pub(crate) struct PlannedPartition<V: MultiMapValue, A: MultiMapAugmentation> {
    pub capacity: u64,
    pub subtrees: Vec<MultiMapAug<V, A>>,
}

/// One partition over an ordered sequence of nested maps. Each entry is a
/// sparse inner map rooted in the same scope as its corresponding source map.
/// An inner leaf is indivisible; a partition can cross outer-key boundaries.
#[derive(Clone, Debug)]
pub struct NestedCapacityPartition<V: MultiMapValue, A: MultiMapAugmentation> {
    pub capacity: u64,
    pub maps: Vec<(MapKey, MultiMapAug<V, A>)>,
}

struct PlannedMap<V: MultiMapValue, A: MultiMapAugmentation> {
    key: MapKey,
    source_path: MapKeyPath,
    subtrees: Vec<MultiMapAug<V, A>>,
}

struct GroupedPartition<V: MultiMapValue, A: MultiMapAugmentation> {
    capacity: u64,
    maps: Vec<PlannedMap<V, A>>,
}

// One traversal and one fill/tail policy for both single- and two-level maps.
struct Planner<'a, V: MultiMapValue, A: MultiMapAugmentation, F> {
    target: u64,
    capacity: &'a F,
    current: GroupedPartition<V, A>,
    complete: Vec<GroupedPartition<V, A>>,
}

impl<'a, V, A, F> Planner<'a, V, A, F>
where
    V: MultiMapValue,
    A: MultiMapAugmentation,
    F: Fn(&A) -> Option<u64>,
{
    fn new(target: u64, capacity: &'a F) -> Result<Self, CapacityPartitionError> {
        if target == 0 {
            return Err(CapacityPartitionError::InvalidPartitionCapacity);
        }
        Ok(Self {
            target,
            capacity,
            current: GroupedPartition { capacity: 0, maps: Vec::new() },
            complete: Vec::new(),
        })
    }

    fn close(&mut self) {
        if !self.current.maps.is_empty() {
            self.complete.push(std::mem::replace(
                &mut self.current,
                GroupedPartition { capacity: 0, maps: Vec::new() },
            ));
        }
    }

    fn consider(
        &mut self,
        key: MapKey,
        source_path: MapKeyPath,
        root: &NodeAug<V, A>,
        path: MapKeyPath,
    ) -> Result<(), CapacityPartitionError> {
        if root.is_empty() {
            return Ok(());
        }
        let bytes = (self.capacity)(&root.augmentation())
            .ok_or(CapacityPartitionError::InvalidAugmentation)?;
        let mut combined = self
            .current
            .capacity
            .checked_add(bytes)
            .ok_or(CapacityPartitionError::InvalidAugmentation)?;
        if combined > self.target
            && !self.current.maps.is_empty()
            && self.current.capacity >= self.target - self.target / 10
        {
            self.close();
            combined = bytes;
        }
        if combined <= self.target || matches!(root, NodeAug::Leaf(_)) {
            self.current.capacity = combined;
            if self.current.maps.last().is_none_or(|map| map.key != key) {
                self.current.maps.push(PlannedMap { key, source_path, subtrees: Vec::new() });
            }
            self.current
                .maps
                .last_mut()
                .unwrap()
                .subtrees
                .push(MultiMapAug { root: root.clone(), root_path: path });
            return Ok(());
        }
        let depth = path.len as usize / 4;
        match root {
            NodeAug::Ext(ext) => {
                let count = ext.nibble_count as usize;
                let mut child_path = path;
                for i in 0..count {
                    set_nibble(&mut child_path.prefix, depth + i, ext.nibbles[i]);
                }
                child_path.len = ((depth + count) * 4) as u16;
                self.consider(key, source_path, &ext.child, child_path)?;
            }
            NodeAug::Branch(branch) => {
                for nibble in 0..16u8 {
                    let child = &branch.children[nibble as usize];
                    if child.is_empty() {
                        continue;
                    }
                    let mut child_path = path;
                    set_nibble(&mut child_path.prefix, depth, nibble);
                    child_path.len = ((depth + 1) * 4) as u16;
                    self.consider(key, source_path, child, child_path)?;
                }
            }
            NodeAug::Empty | NodeAug::Leaf(_) => unreachable!(),
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<GroupedPartition<V, A>>, CapacityPartitionError> {
        self.close();
        if self.complete.len() > 1
            && self.complete.last().unwrap().capacity < self.target.div_ceil(10)
        {
            let trailing = self.complete.pop().unwrap();
            let previous = self.complete.last_mut().unwrap();
            previous.capacity = previous
                .capacity
                .checked_add(trailing.capacity)
                .ok_or(CapacityPartitionError::InvalidAugmentation)?;
            for map in trailing.maps {
                if let Some(last) = previous.maps.last_mut().filter(|last| last.key == map.key) {
                    last.subtrees.extend(map.subtrees);
                } else {
                    previous.maps.push(map);
                }
            }
        }
        Ok(self.complete)
    }
}

pub(crate) fn plan_capacity_partitions<V, A, F>(
    root: &NodeAug<V, A>,
    root_path: MapKeyPath,
    target: u64,
    capacity: &F,
) -> Result<Vec<PlannedPartition<V, A>>, CapacityPartitionError>
where
    V: MultiMapValue,
    A: MultiMapAugmentation,
    F: Fn(&A) -> Option<u64>,
{
    let mut planner = Planner::new(target, capacity)?;
    planner.consider(MapKey([0; 32]), root_path, root, root_path)?;
    Ok(planner
        .finish()?
        .into_iter()
        .map(|part| PlannedPartition {
            capacity: part.capacity,
            subtrees: part.maps.into_iter().next().unwrap().subtrees,
        })
        .collect())
}

/// Partition nested maps in lexicographic (outer key, inner key) order.
/// Input outer keys MUST be strictly increasing, including entries with empty
/// maps. Close at 90% before descending into a non-fitting subtree, keep leaves
/// whole, and merge a final remainder strictly smaller than 10% into its predecessor.
/// Only cached augmentations/nodes are read; output maps share source subtrees.
pub fn partition_nested_by_capacity<'a, V, A, I, F>(
    maps: I,
    target: u64,
    capacity: F,
) -> Result<Vec<NestedCapacityPartition<V, A>>, CapacityPartitionError>
where
    V: MultiMapValue + 'a,
    A: MultiMapAugmentation + 'a,
    I: IntoIterator<Item = (MapKey, &'a MultiMapAug<V, A>)>,
    F: Fn(&A) -> Option<u64>,
{
    let mut planner = Planner::new(target, &capacity)?;
    let mut previous = None;
    for (key, map) in maps {
        if previous.is_some_and(|previous| previous >= key) {
            return Err(CapacityPartitionError::InvalidKeyOrder);
        }
        previous = Some(key);
        planner.consider(key, map.root_path, &map.root, map.root_path)?;
    }

    Ok(planner
        .finish()?
        .into_iter()
        .map(|part| NestedCapacityPartition {
            capacity: part.capacity,
            maps: part
                .maps
                .into_iter()
                .map(|planned| {
                    let empty =
                        MultiMapAug { root: NodeAug::Empty, root_path: planned.source_path };
                    (planned.key, empty.graft_batch(&planned.subtrees))
                })
                .collect(),
        })
        .collect())
}

fn set_nibble(key: &mut MapKey, depth: usize, value: u8) {
    if depth.is_multiple_of(2) {
        key.0[depth / 2] = (key.0[depth / 2] & 15) | (value << 4);
    } else {
        key.0[depth / 2] = (key.0[depth / 2] & 240) | value;
    }
}
