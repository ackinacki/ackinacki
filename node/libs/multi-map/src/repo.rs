use std::io::Read as IoRead;
use std::io::Write as IoWrite;
use std::sync::Arc;

use crate::live_metrics::LiveMultiMapNodeCounter;
use crate::node::BranchDataAug;
use crate::node::ExtDataAug;
use crate::node::LeafDataAug;
use crate::node::NodeAug;
use crate::ops;
use crate::MapHash;
use crate::MapKey;
use crate::MapKeyPath;
use crate::MultiMapAugmentation;
use crate::MultiMapValue;

fn check_cancelled<F>(should_cancel: &F) -> anyhow::Result<()>
where
    F: Fn() -> bool,
{
    if should_cancel() {
        anyhow::bail!("snapshot save cancelled");
    }
    Ok(())
}

/// Immutable persistent map holding a root node and prefix path.
/// Updates and structural operations return new maps without changing the source.
/// Clone is cheap (Arc reference count bump). NOT Copy.
#[derive(Clone)]
pub struct MultiMapAug<V: MultiMapValue, A: MultiMapAugmentation> {
    pub root: NodeAug<V, A>,
    pub root_path: MapKeyPath,
}

pub type MultiMap<V> = MultiMapAug<V, ()>;

#[derive(Clone, Debug)]
pub struct CapacityPartition<V: MultiMapValue, A: MultiMapAugmentation> {
    pub map: MultiMapAug<V, A>,
    pub capacity: u64,
}

impl<V: MultiMapValue, A: MultiMapAugmentation> std::fmt::Debug for MultiMapAug<V, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiMapAug")
            .field("root_hash", &hex::encode(self.root.hash()))
            .field("root_path_len", &self.root_path.len)
            .finish()
    }
}

impl<V: MultiMapValue, A: MultiMapAugmentation> Default for MultiMapAug<V, A> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: MultiMapValue, A: MultiMapAugmentation> MultiMapAug<V, A> {
    /// Serialize a map to a writer. Format is self-contained binary.
    pub fn write<W: IoWrite>(&self, writer: &mut W) -> anyhow::Result<()> {
        writer.write_all(&self.root_path.len.to_le_bytes())?;
        writer.write_all(&self.root_path.prefix.0)?;
        Self::write_node(&self.root, writer)
    }

    /// Serialize a map to a writer, checking `should_cancel` between nodes.
    pub fn write_checked<W, F>(&self, writer: &mut W, should_cancel: &F) -> anyhow::Result<()>
    where
        W: IoWrite,
        F: Fn() -> bool,
    {
        check_cancelled(should_cancel)?;
        writer.write_all(&self.root_path.len.to_le_bytes())?;
        writer.write_all(&self.root_path.prefix.0)?;
        Self::write_node_checked(&self.root, writer, should_cancel)
    }

    /// Serialize a map without hashes — smaller output, faster write.
    /// Must be read back with `read_no_hash`.
    pub fn write_no_hash<W: IoWrite>(&self, writer: &mut W) -> anyhow::Result<()> {
        writer.write_all(&self.root_path.len.to_le_bytes())?;
        writer.write_all(&self.root_path.prefix.0)?;
        Self::write_node_no_hash(&self.root, writer)
    }

    /// Deserialize a map from a reader. Hashes are read from the stream.
    pub fn read<R: IoRead>(reader: &mut R) -> anyhow::Result<Self> {
        let mut len_buf = [0u8; 2];
        reader.read_exact(&mut len_buf)?;
        let mut prefix = [0u8; 32];
        reader.read_exact(&mut prefix)?;
        let root_path = MapKeyPath { prefix: MapKey(prefix), len: u16::from_le_bytes(len_buf) };
        let root = Self::read_node(reader)?;
        Ok(MultiMapAug { root, root_path })
    }

    /// Deserialize a map written with `write_no_hash`.
    /// Hashes are recomputed from children (bottom-up) during read.
    pub fn read_no_hash<R: IoRead>(reader: &mut R) -> anyhow::Result<Self> {
        let mut len_buf = [0u8; 2];
        reader.read_exact(&mut len_buf)?;
        let mut prefix = [0u8; 32];
        reader.read_exact(&mut prefix)?;
        let root_path = MapKeyPath { prefix: MapKey(prefix), len: u16::from_le_bytes(len_buf) };
        let root = Self::read_node_no_hash(reader)?;
        Ok(MultiMapAug { root, root_path })
    }

    // --- write helpers (with hashes) ---

    fn write_node<W: IoWrite>(node: &NodeAug<V, A>, w: &mut W) -> anyhow::Result<()> {
        match node {
            NodeAug::Empty => w.write_all(&[0x00])?,
            NodeAug::Leaf(data) => {
                w.write_all(&[0x01])?;
                w.write_all(&data.hash)?;
                data.value.write_value(w)?;
                data.augmentation.write(w)?;
            }
            NodeAug::Branch(data) => {
                w.write_all(&[0x02])?;
                w.write_all(&data.hash)?;
                w.write_all(&data.bitmap.to_le_bytes())?;
                for child in data.children.iter() {
                    Self::write_node(child, w)?;
                }
            }
            NodeAug::Ext(data) => {
                w.write_all(&[0x03])?;
                w.write_all(&data.hash)?;
                w.write_all(&[data.nibble_count])?;
                w.write_all(&data.nibbles[..data.nibble_count as usize])?;
                Self::write_node(&data.child, w)?;
            }
        }
        Ok(())
    }

    fn write_node_checked<W, F>(
        node: &NodeAug<V, A>,
        w: &mut W,
        should_cancel: &F,
    ) -> anyhow::Result<()>
    where
        W: IoWrite,
        F: Fn() -> bool,
    {
        check_cancelled(should_cancel)?;
        match node {
            NodeAug::Empty => w.write_all(&[0x00])?,
            NodeAug::Leaf(data) => {
                w.write_all(&[0x01])?;
                w.write_all(&data.hash)?;
                data.value.write_value(w)?;
                data.augmentation.write(w)?;
            }
            NodeAug::Branch(data) => {
                w.write_all(&[0x02])?;
                w.write_all(&data.hash)?;
                w.write_all(&data.bitmap.to_le_bytes())?;
                for child in data.children.iter() {
                    Self::write_node_checked(child, w, should_cancel)?;
                }
            }
            NodeAug::Ext(data) => {
                w.write_all(&[0x03])?;
                w.write_all(&data.hash)?;
                w.write_all(&[data.nibble_count])?;
                w.write_all(&data.nibbles[..data.nibble_count as usize])?;
                Self::write_node_checked(&data.child, w, should_cancel)?;
            }
        }
        Ok(())
    }

    // --- write helpers (compact, no hashes) ---

    fn write_node_no_hash<W: IoWrite>(node: &NodeAug<V, A>, w: &mut W) -> anyhow::Result<()> {
        match node {
            NodeAug::Empty => w.write_all(&[0x00])?,
            NodeAug::Leaf(data) => {
                w.write_all(&[0x01])?;
                data.value.write_value_no_hash(w)?;
                data.augmentation.write(w)?;
            }
            NodeAug::Branch(data) => {
                w.write_all(&[0x02])?;
                w.write_all(&data.bitmap.to_le_bytes())?;
                for child in data.children.iter() {
                    Self::write_node_no_hash(child, w)?;
                }
            }
            NodeAug::Ext(data) => {
                w.write_all(&[0x03])?;
                w.write_all(&[data.nibble_count])?;
                w.write_all(&data.nibbles[..data.nibble_count as usize])?;
                Self::write_node_no_hash(&data.child, w)?;
            }
        }
        Ok(())
    }

    // --- read helpers (with stored hashes) ---

    fn read_node<R: IoRead>(r: &mut R) -> anyhow::Result<NodeAug<V, A>> {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag)?;
        match tag[0] {
            0x00 => Ok(NodeAug::Empty),
            0x01 => Self::read_leaf(r),
            0x02 => Self::read_branch(r),
            0x03 => Self::read_ext(r),
            other => Err(anyhow::anyhow!("invalid node tag: 0x{other:02x}")),
        }
    }

    fn read_leaf<R: IoRead>(r: &mut R) -> anyhow::Result<NodeAug<V, A>> {
        let mut hash = [0u8; 32];
        r.read_exact(&mut hash)?;
        let value = V::read_value(r)?;
        let augmentation = A::read(r)?;
        Ok(NodeAug::Leaf(Arc::new(LeafDataAug {
            hash,
            value,
            augmentation,
            _live_counter: LiveMultiMapNodeCounter::new(),
        })))
    }

    fn read_branch<R: IoRead>(r: &mut R) -> anyhow::Result<NodeAug<V, A>> {
        let mut hash = [0u8; 32];
        r.read_exact(&mut hash)?;
        let mut bm = [0u8; 2];
        r.read_exact(&mut bm)?;
        let bitmap = u16::from_le_bytes(bm);
        let mut children: [NodeAug<V, A>; 16] = Default::default();
        for slot in children.iter_mut() {
            *slot = Self::read_node(r)?;
        }
        let augmentation = children
            .iter()
            .filter(|child| !child.is_empty())
            .fold(A::identity(), |acc, child| A::combine(&acc, &child.augmentation()));
        Ok(NodeAug::Branch(Arc::new(BranchDataAug {
            hash,
            bitmap,
            children,
            augmentation,
            _live_counter: LiveMultiMapNodeCounter::new(),
        })))
    }

    fn read_ext<R: IoRead>(r: &mut R) -> anyhow::Result<NodeAug<V, A>> {
        let mut hash = [0u8; 32];
        r.read_exact(&mut hash)?;
        let mut count_buf = [0u8; 1];
        r.read_exact(&mut count_buf)?;
        let nibble_count = count_buf[0];
        let mut nibbles = [0u8; 64];
        r.read_exact(&mut nibbles[..nibble_count as usize])?;
        let child = Self::read_node(r)?;
        if child.is_empty() {
            return Err(anyhow::anyhow!("ext node has no child"));
        }
        let augmentation = child.augmentation();
        Ok(NodeAug::Ext(Arc::new(ExtDataAug {
            hash,
            nibble_count,
            nibbles,
            child,
            augmentation,
            _live_counter: LiveMultiMapNodeCounter::new(),
        })))
    }

    // --- read helpers (rehash from children) ---

    fn read_node_no_hash<R: IoRead>(r: &mut R) -> anyhow::Result<NodeAug<V, A>> {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag)?;
        match tag[0] {
            0x00 => Ok(NodeAug::Empty),
            0x01 => Self::read_leaf_no_hash(r),
            0x02 => Self::read_branch_no_hash(r),
            0x03 => Self::read_ext_no_hash(r),
            other => anyhow::bail!("invalid node tag: 0x{other:02x}",),
        }
    }

    fn read_leaf_no_hash<R: IoRead>(r: &mut R) -> anyhow::Result<NodeAug<V, A>> {
        let value = V::read_value_no_hash(r)?;
        let augmentation = A::read(r)?;
        Ok(NodeAug::new_leaf_aug(value, augmentation))
    }

    fn read_branch_no_hash<R: IoRead>(r: &mut R) -> anyhow::Result<NodeAug<V, A>> {
        let mut bm = [0u8; 2];
        r.read_exact(&mut bm)?;
        let bitmap = u16::from_le_bytes(bm);
        let mut children: [NodeAug<V, A>; 16] = Default::default();
        for slot in children.iter_mut() {
            *slot = Self::read_node_no_hash(r)?;
        }
        Ok(NodeAug::new_branch(bitmap, children)) // new_branch computes hash from children
    }

    fn read_ext_no_hash<R: IoRead>(r: &mut R) -> anyhow::Result<NodeAug<V, A>> {
        let mut count_buf = [0u8; 1];
        r.read_exact(&mut count_buf)?;
        let nibble_count = count_buf[0];
        let mut nibbles = [0u8; 64];
        r.read_exact(&mut nibbles[..nibble_count as usize])?;
        let child = Self::read_node_no_hash(r)?;
        if child.is_empty() {
            return Err(anyhow::anyhow!("ext node has no child"));
        }
        Ok(NodeAug::new_ext(&nibbles[..nibble_count as usize], child)) // new_ext computes hash
    }
}

// Core operations available for all MultiMapValue types (including non-Copy, non-Serialize).
impl<V: MultiMapValue, A: MultiMapAugmentation> MultiMapAug<V, A> {
    pub fn new() -> Self {
        MultiMapAug { root: NodeAug::Empty, root_path: MapKeyPath::default() }
    }

    pub fn hash(&self) -> MapHash {
        MapHash(self.root.hash())
    }

    pub fn get(&self, key: &MapKey) -> Option<V> {
        ops::get(&self.root, &self.root_path, key)
    }

    pub fn get_aug(&self, key: &MapKey) -> Option<(V, A)> {
        ops::get_leaf(&self.root, &self.root_path, key)
            .map(|leaf| (leaf.value.clone(), leaf.augmentation.clone()))
    }

    pub fn update_aug(&self, updates: &[(MapKey, Option<(V, A)>)]) -> Self {
        let new_root = ops::update_aug(&self.root, &self.root_path, updates);
        MultiMapAug { root: new_root, root_path: self.root_path }
    }

    /// Fast path for single key insert/replace/delete.
    /// Skips sorting, dedup, and range splitting — walks the trie directly.
    pub fn update_single_aug(&self, key: &MapKey, value: Option<(V, A)>) -> Self {
        let new_root = ops::update_single_aug(&self.root, &self.root_path, key, value);
        MultiMapAug { root: new_root, root_path: self.root_path }
    }

    pub fn split(&self, key_path: MapKeyPath) -> (Self, Self) {
        let (without, branch) = ops::split(&self.root, self.root_path, key_path);
        (
            MultiMapAug { root: without, root_path: self.root_path },
            MultiMapAug { root: branch, root_path: key_path },
        )
    }

    /// Graft `b` into this map at `b`'s prefix path. This is the inverse of
    /// [`Self::split`], not a union — see [`ops::graft`] for the full contract.
    pub fn graft(&self, b: &Self) -> Self {
        let grafted = ops::graft(&self.root, self.root_path, &b.root, b.root_path);
        MultiMapAug { root: grafted, root_path: self.root_path }
    }

    /// Graft multiple subtrees into this map in a single pass.
    /// Much faster than calling `graft` repeatedly because canonicalize runs only once.
    /// Same contract as [`Self::graft`]: subtrees must be at distinct, pre-pruned slots.
    pub fn graft_batch(&self, subtrees: &[Self]) -> Self {
        let sub_pairs: Vec<_> = subtrees.iter().map(|s| (s.root.clone(), s.root_path)).collect();
        let grafted = ops::graft_batch(&self.root, self.root_path, &sub_pairs);
        MultiMapAug { root: grafted, root_path: self.root_path }
    }

    /// Checked union of two maps with the same root path.
    ///
    /// Unlike `graft`, neither input replaces a scope in the other. Any
    /// overlapping leaf or incompatible root path is rejected.
    pub fn merge_disjoint(&self, right: &Self) -> Result<Self, ops::DisjointMergeError> {
        if self.root_path != right.root_path {
            return Err(ops::DisjointMergeError::IncompatibleRootPath);
        }
        let root = ops::merge_disjoint(&self.root, &right.root, (self.root_path.len as usize) / 4)?;
        Ok(MultiMapAug { root, root_path: self.root_path })
    }

    pub fn merge_disjoint_batch(maps: &[Self]) -> Result<Self, ops::DisjointMergeError> {
        let Some(first) = maps.first() else {
            return Ok(Self::new());
        };
        let mut merged = MultiMapAug { root: NodeAug::Empty, root_path: first.root_path };
        for map in maps {
            merged = merged.merge_disjoint(map)?;
        }
        Ok(merged)
    }

    /// Deterministically split a map into capacity-balanced sparse maps.
    ///
    /// Each result is assembled from complete source subtrees, has the same
    /// root path as `map`, and can be recombined with `merge_disjoint_batch`.
    /// `partition_capacity` is the target. A partition at least 90% full is
    /// closed rather than descending into a non-fitting subtree. Below that
    /// threshold, the planner descends until it can add a whole subtree or an
    /// indivisible leaf; the latter may take the partition over the target. A
    /// final remainder smaller than 10% is merged into its predecessor. A zero
    /// target is rejected.
    /// Returning `None` from `capacity` rejects an invalid or overflowed
    /// augmentation.
    pub fn partition_by_capacity<F>(
        &self,
        partition_capacity: u64,
        capacity: F,
    ) -> Result<Vec<CapacityPartition<V, A>>, ops::CapacityPartitionError>
    where
        F: Fn(&A) -> Option<u64>,
    {
        let planned = ops::plan_capacity_partitions(
            &self.root,
            self.root_path,
            partition_capacity,
            &capacity,
        )?;
        Ok(planned
            .into_iter()
            .map(|partition| {
                let subtrees: Vec<_> = partition
                    .subtrees
                    .into_iter()
                    .map(|subtree| (subtree.root, subtree.root_path))
                    .collect();
                let root = ops::graft_batch(&NodeAug::Empty, self.root_path, &subtrees);
                CapacityPartition {
                    map: MultiMapAug { root, root_path: self.root_path },
                    capacity: partition.capacity,
                }
            })
            .collect())
    }

    pub fn collect_values(&self) -> Vec<(MapKey, V)> {
        let mut result = Vec::new();
        if self.root.is_empty() {
            return result;
        }

        let start_nibbles = (self.root_path.len as usize) / 4;
        let mut stack: Vec<(&NodeAug<V, A>, Vec<u8>)> = vec![(&self.root, Vec::new())];

        while let Some((node, nibble_path)) = stack.pop() {
            match node {
                NodeAug::Empty => {}
                NodeAug::Leaf(data) => {
                    let mut key_bytes = self.root_path.prefix.0;
                    for (i, &nib) in nibble_path.iter().enumerate() {
                        let pos = start_nibbles + i;
                        let byte_idx = pos / 2;
                        if pos.is_multiple_of(2) {
                            key_bytes[byte_idx] = (key_bytes[byte_idx] & 0x0F) | (nib << 4);
                        } else {
                            key_bytes[byte_idx] = (key_bytes[byte_idx] & 0xF0) | (nib & 0x0F);
                        }
                    }
                    result.push((MapKey(key_bytes), data.value.clone()));
                }
                NodeAug::Branch(data) => {
                    for nib in 0..16u8 {
                        if !data.children[nib as usize].is_empty() {
                            let mut child_path = nibble_path.clone();
                            child_path.push(nib);
                            stack.push((&data.children[nib as usize], child_path));
                        }
                    }
                }
                NodeAug::Ext(data) => {
                    let mut child_path = nibble_path.clone();
                    child_path.extend_from_slice(&data.nibbles[..data.nibble_count as usize]);
                    stack.push((&data.child, child_path));
                }
            }
        }

        result
    }

    /// Returns a lazy iterator over all (key, &value) pairs in the map.
    pub fn iter(&self) -> MapIterAug<'_, V, A> {
        let start_nibbles = (self.root_path.len as usize) / 4;
        let mut stack = Vec::new();
        if !self.root.is_empty() {
            stack.push((&self.root, Vec::new()));
        }
        MapIterAug { stack, prefix: self.root_path.prefix.0, start_nibbles }
    }

    /// Iterate in the same order as `iter`, borrowing each leaf's augmentation alongside
    /// its value. Consumers need not search the tree again for metadata already visited.
    pub fn iter_with_augmentation(&self) -> impl Iterator<Item = (MapKey, &V, &A)> {
        let mut iter = self.iter();
        std::iter::from_fn(move || iter.next_with_augmentation())
    }
}

impl<V: MultiMapValue> MultiMapAug<V, ()> {
    pub fn update(&self, updates: &[(MapKey, Option<V>)]) -> Self {
        let new_root = ops::update(&self.root, &self.root_path, updates);
        MultiMapAug { root: new_root, root_path: self.root_path }
    }

    pub fn update_single(&self, key: &MapKey, value: Option<V>) -> MultiMap<V> {
        let new_root = ops::update_single(&self.root, &self.root_path, key, value);
        MultiMapAug { root: new_root, root_path: self.root_path }
    }
}

/// Lazy iterator over (MapKey, &V) entries in a MultiMap.
pub struct MapIterAug<'a, V: MultiMapValue, A: MultiMapAugmentation> {
    stack: Vec<(&'a NodeAug<V, A>, Vec<u8>)>,
    prefix: [u8; 32],
    start_nibbles: usize,
}

pub type MapIter<'a, V> = MapIterAug<'a, V, ()>;

impl<'a, V: MultiMapValue, A: MultiMapAugmentation> Iterator for MapIterAug<'a, V, A> {
    type Item = (MapKey, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_with_augmentation().map(|(key, value, _)| (key, value))
    }
}

impl<'a, V: MultiMapValue, A: MultiMapAugmentation> MapIterAug<'a, V, A> {
    /// Advance the shared traversal, returning leaf metadata without a lookup or clone.
    pub fn next_with_augmentation(&mut self) -> Option<(MapKey, &'a V, &'a A)> {
        loop {
            let (node, nibble_path) = self.stack.pop()?;
            match node {
                NodeAug::Empty => continue,
                NodeAug::Leaf(data) => {
                    let mut key_bytes = self.prefix;
                    for (i, &nib) in nibble_path.iter().enumerate() {
                        let pos = self.start_nibbles + i;
                        let byte_idx = pos / 2;
                        if pos.is_multiple_of(2) {
                            key_bytes[byte_idx] = (key_bytes[byte_idx] & 0x0F) | (nib << 4);
                        } else {
                            key_bytes[byte_idx] = (key_bytes[byte_idx] & 0xF0) | (nib & 0x0F);
                        }
                    }
                    return Some((MapKey(key_bytes), &data.value, &data.augmentation));
                }
                NodeAug::Branch(data) => {
                    // Push in reverse order so lower nibbles come out first
                    for nib in (0..16u8).rev() {
                        if !data.children[nib as usize].is_empty() {
                            let mut child_path = nibble_path.clone();
                            child_path.push(nib);
                            self.stack.push((&data.children[nib as usize], child_path));
                        }
                    }
                    continue;
                }
                NodeAug::Ext(data) => {
                    let mut child_path = nibble_path.clone();
                    child_path.extend_from_slice(&data.nibbles[..data.nibble_count as usize]);
                    self.stack.push((&data.child, child_path));
                    continue;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use blake3::Hasher;
    use node_types::Blake3Hashable;
    use serde::Deserialize;
    use serde::Serialize;

    use super::*;
    use crate::ops::prefix_bits_match;

    #[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct TV(pub u64);

    impl MultiMapValue for TV {
        fn write_value<W: IoWrite>(&self, w: &mut W) -> anyhow::Result<()> {
            w.write_all(&self.0.to_be_bytes())?;
            Ok(())
        }

        fn read_value<R: IoRead>(r: &mut R) -> anyhow::Result<Self> {
            let mut buf = [0u8; 8];
            r.read_exact(&mut buf)?;
            Ok(TV(u64::from_be_bytes(buf)))
        }
    }

    impl Blake3Hashable for TV {
        fn update_hasher(&self, hasher: &mut Hasher) {
            hasher.update(&self.0.to_be_bytes());
        }
    }

    #[test]
    fn arc_values_preserve_hashes_and_wire_representation() {
        let key = MapKey([7; 32]);
        let value = TV(42);
        let plain = MultiMapAug::new().update_single(&key, Some(value));
        let shared = MultiMapAug::new().update_single(&key, Some(Arc::new(value)));
        assert_eq!(plain.hash(), shared.hash());
        let mut plain_bytes = Vec::new();
        let mut shared_bytes = Vec::new();
        plain.write(&mut plain_bytes).unwrap();
        shared.write(&mut shared_bytes).unwrap();
        assert_eq!(plain_bytes, shared_bytes);
        shared_bytes.clear();
        shared.write_no_hash(&mut shared_bytes).unwrap();
        let restored =
            MultiMapAug::<Arc<TV>, ()>::read_no_hash(&mut shared_bytes.as_slice()).unwrap();
        assert_eq!(restored.hash(), plain.hash());
        assert_eq!(*restored.get(&key).unwrap(), value);
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    struct Capacity {
        bytes: u64,
    }

    fn single_delete_coalesces_extensions<A: MultiMapAugmentation>(augmentation: A) {
        for shared_nibbles in 1..64 {
            let a = MapKey([0xe8; 32]);
            let mut b = a;
            b.0[shared_nibbles / 2] ^= if shared_nibbles % 2 == 0 { 0x10 } else { 0x01 };
            for keep_other_branch in [false, true] {
                let mut fresh = MultiMapAug::<TV, A>::new()
                    .update_single_aug(&a, Some((TV(1), augmentation.clone())));
                if keep_other_branch {
                    fresh = fresh.update_single_aug(
                        &MapKey([0xf8; 32]),
                        Some((TV(2), augmentation.clone())),
                    );
                }
                let original = fresh.clone();
                let history = fresh
                    .update_single_aug(&b, Some((TV(3), augmentation.clone())))
                    .update_single_aug(&b, None);
                assert_eq!(history.iter().collect::<Vec<_>>(), fresh.iter().collect::<Vec<_>>());
                assert_eq!(history.hash(), fresh.hash(), "shared nibbles: {shared_nibbles}");
                let mut expected = Vec::new();
                original.write(&mut expected).unwrap();
                let mut actual = Vec::new();
                history.write(&mut actual).unwrap();
                assert_eq!(actual, expected, "canonical bytes: {shared_nibbles}");
                assert_eq!(original.hash(), fresh.hash());
            }
        }
    }

    #[test]
    fn single_delete_preserves_canonical_shape_with_and_without_augmentation() {
        single_delete_coalesces_extensions(());
        single_delete_coalesces_extensions(Capacity { bytes: 123 });
    }

    impl MultiMapAugmentation for Capacity {
        fn identity() -> Self {
            Self::default()
        }

        fn combine(left: &Self, right: &Self) -> Self {
            Self { bytes: left.bytes + right.bytes }
        }

        fn write<W: IoWrite>(&self, writer: &mut W) -> anyhow::Result<()> {
            writer.write_all(&self.bytes.to_le_bytes())?;
            Ok(())
        }

        fn read<R: IoRead>(reader: &mut R) -> anyhow::Result<Self> {
            let mut bytes = [0; 8];
            reader.read_exact(&mut bytes)?;
            Ok(Self { bytes: u64::from_le_bytes(bytes) })
        }
    }

    fn verify_capacity(node: &NodeAug<TV, Capacity>) -> Capacity {
        let expected = match node {
            NodeAug::Empty => Capacity::identity(),
            NodeAug::Leaf(data) => data.augmentation,
            NodeAug::Ext(data) => verify_capacity(&data.child),
            NodeAug::Branch(data) => data
                .children
                .iter()
                .map(verify_capacity)
                .fold(Capacity::identity(), |acc, child| Capacity::combine(&acc, &child)),
        };
        assert_eq!(node.augmentation(), expected);
        expected
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    struct CheckedTotal {
        value: u64,
        overflowed: bool,
    }

    impl MultiMapAugmentation for CheckedTotal {
        fn identity() -> Self {
            Self::default()
        }

        fn combine(left: &Self, right: &Self) -> Self {
            let (value, overflowed) = left.value.overflowing_add(right.value);
            Self { value, overflowed: left.overflowed || right.overflowed || overflowed }
        }

        fn write<W: IoWrite>(&self, writer: &mut W) -> anyhow::Result<()> {
            writer.write_all(&self.value.to_le_bytes())?;
            writer.write_all(&[u8::from(self.overflowed)])?;
            Ok(())
        }

        fn read<R: IoRead>(reader: &mut R) -> anyhow::Result<Self> {
            let mut value = [0; 8];
            let mut overflowed = [0];
            reader.read_exact(&mut value)?;
            reader.read_exact(&mut overflowed)?;
            if overflowed[0] > 1 {
                anyhow::bail!("invalid augmentation overflow flag")
            }
            Ok(Self { value: u64::from_le_bytes(value), overflowed: overflowed[0] != 0 })
        }
    }

    fn key(i: usize) -> MapKey {
        let mut h = Hasher::new();
        h.update(b"key:");
        h.update(&(i as u64).to_le_bytes());
        MapKey(*h.finalize().as_bytes())
    }

    fn ordered_key(i: u8) -> MapKey {
        let mut key = [0; 32];
        key[31] = i;
        MapKey(key)
    }

    #[test]
    fn augmented_iteration_preserves_order_and_returns_leaf_metadata() {
        fn check(map: &MultiMapAug<TV, Capacity>) {
            let hash = map.hash();
            let entries: Vec<_> = map.iter_with_augmentation().collect();
            assert_eq!(
                entries.iter().map(|(key, value, _)| (*key, *value)).collect::<Vec<_>>(),
                map.iter().collect::<Vec<_>>()
            );
            assert!(entries.windows(2).all(|pair| pair[0].0 < pair[1].0));
            for (key, value, capacity) in &entries {
                assert_eq!(map.get_aug(key), Some((**value, **capacity)));
            }
            assert_eq!(
                entries.iter().map(|(_, _, capacity)| capacity.bytes).sum::<u64>(),
                map.root.augmentation().bytes
            );
            // Both APIs must advance the very same cursor, not separate walks.
            let mut mixed = map.iter();
            for (i, (key, value, capacity)) in entries.iter().enumerate() {
                if i.is_multiple_of(2) {
                    assert_eq!(mixed.next(), Some((*key, *value)));
                } else {
                    assert_eq!(mixed.next_with_augmentation(), Some((*key, *value, *capacity)));
                }
            }
            assert!(mixed.next_with_augmentation().is_none());
            assert!(mixed.next().is_none());
            assert_eq!(map.hash(), hash);
        }

        for count in [0, 1, 64] {
            let mut map = MultiMapAug::new();
            for i in 0..count {
                map = map.update_single_aug(
                    &key(i),
                    Some((TV(i as u64), Capacity { bytes: i as u64 * 13 })),
                );
            }
            check(&map);
            let replaced = map
                .update_single_aug(&key(7), Some((TV(700), Capacity { bytes: 999 })))
                .update_single_aug(&key(11), None);
            check(&replaced);
            let (without, subtree) = replaced.split(MapKeyPath { prefix: key(7), len: 4 });
            check(&without);
            check(&subtree);
            let mut encoded = Vec::new();
            replaced.write(&mut encoded).unwrap();
            let restored = MultiMapAug::read(&mut encoded.as_slice()).unwrap();
            check(&restored);
            assert_eq!(
                replaced.iter_with_augmentation().collect::<Vec<_>>(),
                restored.iter_with_augmentation().collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn augmented_map_maintains_and_serializes_derived_capacity() {
        let updates: Vec<_> = (0..32)
            .map(|i| (key(i), Some((TV(i as u64), Capacity { bytes: (i + 1) as u64 }))))
            .collect();
        let map = MultiMapAug::new().update_aug(&updates);
        let plain_updates: Vec<_> =
            updates.iter().map(|(key, value)| (*key, value.map(|(value, _)| value))).collect();
        let plain = MultiMap::new().update(&plain_updates);

        assert_eq!(map.hash(), plain.hash());
        assert_eq!(verify_capacity(&map.root), Capacity { bytes: 528 });

        let replaced = map.update_single_aug(&key(7), Some((TV(700), Capacity { bytes: 1000 })));
        assert_eq!(verify_capacity(&replaced.root), Capacity { bytes: 1520 });
        let deleted = replaced.update_single_aug(&key(3), None);
        assert_eq!(verify_capacity(&deleted.root), Capacity { bytes: 1516 });

        for no_hash in [false, true] {
            let mut bytes = Vec::new();
            let restored = if no_hash {
                deleted.write_no_hash(&mut bytes).unwrap();
                MultiMapAug::<TV, Capacity>::read_no_hash(&mut bytes.as_slice()).unwrap()
            } else {
                deleted.write(&mut bytes).unwrap();
                MultiMapAug::<TV, Capacity>::read(&mut bytes.as_slice()).unwrap()
            };
            assert_eq!(restored.hash(), deleted.hash());
            assert_eq!(verify_capacity(&restored.root), Capacity { bytes: 1516 });
        }

        let mut truncated = Vec::new();
        deleted.write(&mut truncated).unwrap();
        truncated.pop();
        assert!(MultiMapAug::<TV, Capacity>::read(&mut truncated.as_slice()).is_err());

        let split_path = MapKeyPath { prefix: key(0), len: 4 };
        let (without, subtree) = deleted.split(split_path);
        let grafted = without.graft(&subtree);
        assert_eq!(grafted.hash(), deleted.hash());
        assert_eq!(verify_capacity(&grafted.root), Capacity { bytes: 1516 });
    }

    #[test]
    fn augmentation_can_propagate_overflow_without_fallible_map_operations() {
        let map = MultiMapAug::new().update_aug(&[
            (key(1), Some((TV(1), CheckedTotal { value: u64::MAX, overflowed: false }))),
            (key(2), Some((TV(2), CheckedTotal { value: 1, overflowed: false }))),
        ]);
        assert!(map.root.augmentation().overflowed);
    }

    #[test]
    fn capacity_augmentation_adds_only_its_value_to_each_node_allocation() {
        use std::mem::size_of;

        assert_eq!(
            size_of::<crate::node::LeafDataAug<TV, Capacity>>()
                - size_of::<crate::node::LeafData<TV>>(),
            size_of::<Capacity>()
        );
        assert_eq!(
            size_of::<crate::node::BranchDataAug<TV, Capacity>>()
                - size_of::<crate::node::BranchData<TV>>(),
            size_of::<Capacity>()
        );
        assert_eq!(
            size_of::<crate::node::ExtDataAug<TV, Capacity>>()
                - size_of::<crate::node::ExtData<TV>>(),
            size_of::<Capacity>()
        );
    }

    #[test]
    fn checked_disjoint_merge_preserves_augmentation_and_rejects_overlap() {
        let all_updates: Vec<_> = (0..64)
            .map(|i| (key(i), Some((TV(i as u64), Capacity { bytes: (i + 1) as u64 }))))
            .collect();
        let left_updates: Vec<_> = all_updates.iter().step_by(2).cloned().collect();
        let right_updates: Vec<_> = all_updates.iter().skip(1).step_by(2).cloned().collect();
        let expected = MultiMapAug::new().update_aug(&all_updates);
        let left = MultiMapAug::new().update_aug(&left_updates);
        let right = MultiMapAug::new().update_aug(&right_updates);

        let merged = left.merge_disjoint(&right).unwrap();
        let reverse = right.merge_disjoint(&left).unwrap();
        let batch =
            MultiMapAug::<TV, Capacity>::merge_disjoint_batch(&[left.clone(), right.clone()])
                .unwrap();
        assert_eq!(merged.hash(), expected.hash());
        assert_eq!(reverse.hash(), expected.hash());
        assert_eq!(batch.hash(), expected.hash());
        assert_eq!(verify_capacity(&merged.root), Capacity { bytes: 2080 });

        assert_eq!(
            left.merge_disjoint(&left).unwrap_err(),
            ops::DisjointMergeError::OverlappingLeaf
        );
        let mut incompatible = right;
        incompatible.root_path.len = 4;
        assert_eq!(
            left.merge_disjoint(&incompatible).unwrap_err(),
            ops::DisjointMergeError::IncompatibleRootPath
        );
    }

    #[test]
    fn capacity_partitioning_obeys_bounds_and_reconstructs_the_source() {
        let weights = [6, 4, 7, 3, 25, 2];
        let updates: Vec<_> = weights
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                (ordered_key(index as u8), Some((TV(index as u64), Capacity { bytes: *bytes })))
            })
            .collect();
        let source = MultiMapAug::new().update_aug(&updates);

        let partitions =
            source.partition_by_capacity(15, |augmentation| Some(augmentation.bytes)).unwrap();
        assert_eq!(
            partitions.iter().map(|partition| partition.capacity).collect::<Vec<_>>(),
            [17, 28, 2]
        );
        assert!(partitions.iter().all(|partition| partition.map.root_path == source.root_path));
        for partition in &partitions {
            assert_eq!(partition.map.root.augmentation().bytes, partition.capacity);
        }

        let maps: Vec<_> = partitions.iter().map(|partition| partition.map.clone()).collect();
        let reconstructed = MultiMapAug::<TV, Capacity>::merge_disjoint_batch(&maps).unwrap();
        assert_eq!(reconstructed.hash(), source.hash());
        assert_eq!(reconstructed.collect_values(), source.collect_values());

        let again =
            source.partition_by_capacity(15, |augmentation| Some(augmentation.bytes)).unwrap();
        assert_eq!(
            partitions.iter().map(|partition| partition.map.hash()).collect::<Vec<_>>(),
            again.iter().map(|partition| partition.map.hash()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn nested_partitioning_of_one_map_matches_single_level_bytes_and_scopes() {
        let weights = [6, 4, 7, 3, 25, 1];
        let updates: Vec<_> = weights
            .into_iter()
            .enumerate()
            .map(|(i, bytes)| (ordered_key(i as u8), Some((TV(i as u64), Capacity { bytes }))))
            .collect();
        let source = MultiMapAug::new().update_aug(&updates);
        for bits in [0, 1, 4, 7, 8, 129, 255, 256] {
            let (rest, cut) = source.split(MapKeyPath { prefix: ordered_key(3), len: bits });
            for map in [&rest, &cut] {
                for target in [1, 15, 1000] {
                    let single = map.partition_by_capacity(target, |a| Some(a.bytes)).unwrap();
                    let nested =
                        crate::partition_nested_by_capacity([(ordered_key(9), map)], target, |a| {
                            Some(a.bytes)
                        })
                        .unwrap();
                    assert_eq!(single.len(), nested.len());
                    for (a, b) in single.iter().zip(&nested) {
                        assert_eq!(a.capacity, b.capacity);
                        assert_eq!(b.maps.len(), 1);
                        assert_eq!(b.maps[0].0, ordered_key(9));
                        assert_eq!(a.map.root_path, b.maps[0].1.root_path);
                        let mut expected = Vec::new();
                        let mut actual = Vec::new();
                        a.map.write(&mut expected).unwrap();
                        (b.maps[0].1).write(&mut actual).unwrap();
                        assert_eq!(expected, actual);
                    }
                }
            }
        }
    }

    #[test]
    fn nested_partitioning_crosses_outer_keys_and_reconstructs_both_maps() {
        let build = |count: u8| {
            let updates: Vec<_> = (0..count)
                .map(|i| (ordered_key(i), Some((TV(i as u64), Capacity { bytes: 1000 }))))
                .collect();
            MultiMapAug::new().update_aug(&updates)
        };
        let first = build(24);
        let second = build(6);
        let source = [(ordered_key(1), &first), (ordered_key(2), &second)];
        let parts = crate::partition_nested_by_capacity(source, 10_000, |a| Some(a.bytes)).unwrap();
        assert_eq!(parts.iter().map(|p| p.capacity).collect::<Vec<_>>(), vec![10_000; 3]);
        assert_eq!(parts[2].maps.len(), 2);
        for (key, original) in source {
            let maps: Vec<_> = parts
                .iter()
                .flat_map(|p| &p.maps)
                .filter(|(k, _)| *k == key)
                .map(|(_, map)| map.clone())
                .collect();
            let merged = MultiMapAug::<TV, Capacity>::merge_disjoint_batch(&maps).unwrap();
            assert_eq!(merged.hash(), original.hash());
            assert_eq!(merged.collect_values(), original.collect_values());
        }
        let first = MultiMapAug::new()
            .update_single_aug(&ordered_key(1), Some((TV(1), Capacity { bytes: 100 })));
        let second = MultiMapAug::new()
            .update_single_aug(&ordered_key(1), Some((TV(2), Capacity { bytes: 9 })));
        let tail = crate::partition_nested_by_capacity(
            [(ordered_key(1), &first), (ordered_key(2), &second)],
            100,
            |a| Some(a.bytes),
        )
        .unwrap();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].capacity, 109);
        assert_eq!(tail[0].maps.len(), 2);
    }

    #[test]
    fn nested_partitioning_rejects_invalid_inputs_including_empty_duplicate_maps() {
        let empty = MultiMapAug::new();
        let map = empty.update_single_aug(&ordered_key(1), Some((TV(1), Capacity { bytes: 1 })));
        for keys in [[1, 1], [2, 1]] {
            assert_eq!(
                crate::partition_nested_by_capacity(
                    [(ordered_key(keys[0]), &map), (ordered_key(keys[1]), &empty)],
                    100,
                    |a| Some(a.bytes)
                )
                .unwrap_err(),
                ops::CapacityPartitionError::InvalidKeyOrder
            );
        }
        assert_eq!(
            crate::partition_nested_by_capacity([(ordered_key(1), &map)], 100, |_| None)
                .unwrap_err(),
            ops::CapacityPartitionError::InvalidAugmentation
        );
        assert_eq!(
            crate::partition_nested_by_capacity([(ordered_key(1), &empty)], 0, |a| Some(a.bytes))
                .unwrap_err(),
            ops::CapacityPartitionError::InvalidPartitionCapacity
        );
        assert!(crate::partition_nested_by_capacity([(ordered_key(1), &empty)], 100, |a| Some(
            a.bytes
        ))
        .unwrap()
        .is_empty());
    }

    #[test]
    fn capacity_partitioning_handles_empty_exact_and_invalid_inputs() {
        let empty = MultiMapAug::<TV, Capacity>::new();
        assert!(empty
            .partition_by_capacity(20, |augmentation| Some(augmentation.bytes))
            .unwrap()
            .is_empty());

        let exact = empty.update_aug(&[
            (ordered_key(0), Some((TV(0), Capacity { bytes: 5 }))),
            (ordered_key(1), Some((TV(1), Capacity { bytes: 5 }))),
        ]);
        let partitions =
            exact.partition_by_capacity(10, |augmentation| Some(augmentation.bytes)).unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].capacity, 10);

        assert_eq!(
            exact.partition_by_capacity(0, |augmentation| Some(augmentation.bytes)).unwrap_err(),
            ops::CapacityPartitionError::InvalidPartitionCapacity
        );
        assert_eq!(
            exact.partition_by_capacity(10, |_| None).unwrap_err(),
            ops::CapacityPartitionError::InvalidAugmentation
        );

        let one_oversized =
            empty.update_aug(&[(ordered_key(0), Some((TV(0), Capacity { bytes: 25 })))]);
        let partitions = one_oversized
            .partition_by_capacity(15, |augmentation| Some(augmentation.bytes))
            .unwrap();
        assert_eq!(partitions.iter().map(|partition| partition.capacity).collect::<Vec<_>>(), [25]);

        let consecutive_oversized = empty.update_aug(&[
            (ordered_key(0), Some((TV(0), Capacity { bytes: 25 }))),
            (ordered_key(1), Some((TV(1), Capacity { bytes: 30 }))),
            (ordered_key(2), Some((TV(2), Capacity { bytes: 2 }))),
        ]);
        let partitions = consecutive_oversized
            .partition_by_capacity(15, |augmentation| Some(augmentation.bytes))
            .unwrap();
        assert_eq!(
            partitions.iter().map(|partition| partition.capacity).collect::<Vec<_>>(),
            [25, 30, 2]
        );

        let tiny_trailing_partition = empty.update_aug(&[
            (ordered_key(0), Some((TV(0), Capacity { bytes: 25 }))),
            (ordered_key(1), Some((TV(1), Capacity { bytes: 30 }))),
            (ordered_key(2), Some((TV(2), Capacity { bytes: 1 }))),
        ]);
        let partitions = tiny_trailing_partition
            .partition_by_capacity(15, |augmentation| Some(augmentation.bytes))
            .unwrap();
        assert_eq!(
            partitions.iter().map(|partition| partition.capacity).collect::<Vec<_>>(),
            [25, 31]
        );

        let material_trailing_partition = empty.update_aug(&[
            (ordered_key(0), Some((TV(0), Capacity { bytes: 25 }))),
            (ordered_key(1), Some((TV(1), Capacity { bytes: 30 }))),
            (ordered_key(2), Some((TV(2), Capacity { bytes: 3 }))),
        ]);
        let partitions = material_trailing_partition
            .partition_by_capacity(15, |augmentation| Some(augmentation.bytes))
            .unwrap();
        assert_eq!(
            partitions.iter().map(|partition| partition.capacity).collect::<Vec<_>>(),
            [25, 30, 3]
        );
    }

    #[test]
    fn capacity_partitioning_supports_non_aligned_root_paths() {
        let mut updates = Vec::new();
        for index in 0..12u8 {
            let mut key = [0; 32];
            key[0] = 0xab;
            key[1] = 0x80;
            key[31] = index;
            updates.push((MapKey(key), Some((TV(index as u64), Capacity { bytes: 4 }))));
        }
        let source = MultiMapAug::new().update_aug(&updates);
        let root_path = MapKeyPath { prefix: updates[0].0, len: 9 };
        let (_, view) = source.split(root_path);

        let partitions =
            view.partition_by_capacity(12, |augmentation| Some(augmentation.bytes)).unwrap();
        assert!(partitions.iter().all(|partition| partition.map.root_path == root_path));
        let maps: Vec<_> = partitions.into_iter().map(|partition| partition.map).collect();
        let reconstructed = MultiMapAug::<TV, Capacity>::merge_disjoint_batch(&maps).unwrap();
        assert_eq!(reconstructed.hash(), view.hash());
    }

    #[test]
    fn capacity_partitioning_uses_cached_subtree_capacity() {
        use std::cell::Cell;

        let updates: Vec<_> = (0..4096u16)
            .map(|index| {
                let mut key = [0; 32];
                key[30..].copy_from_slice(&index.to_be_bytes());
                (MapKey(key), Some((TV(index as u64), Capacity { bytes: 1 })))
            })
            .collect();
        let source = MultiMapAug::new().update_aug(&updates);
        let capacity_reads = Cell::new(0usize);
        let partitions = source
            .partition_by_capacity(300, |augmentation| {
                capacity_reads.set(capacity_reads.get() + 1);
                Some(augmentation.bytes)
            })
            .unwrap();

        assert_eq!(partitions.len(), 15);
        assert!(capacity_reads.get() < 256, "planner descended too far");
        assert!(capacity_reads.get() < updates.len());
        assert!(partitions[..14].iter().all(|partition| partition.capacity == 288));
        assert_eq!(partitions[14].capacity, 64);
    }

    type TestMap = MultiMap<TV>;

    #[test]
    fn trait_empty_get() {
        let m = TestMap::new();
        assert_eq!(m.get(&key(0)), None);
    }

    #[test]
    fn trait_insert_get() {
        let m0 = TestMap::new();
        let m1 = m0.update(&[(key(1), Some(TV(42)))]);
        assert_eq!(m1.get(&key(1)), Some(TV(42)));
        assert_eq!(m0.get(&key(1)), None);
    }

    #[test]
    fn compatibility_merge_names_match_graft() {
        let source =
            TestMap::new().update(&[(ordered_key(1), Some(TV(1))), (ordered_key(2), Some(TV(2)))]);
        let (without, subtree) = source.split(MapKeyPath { prefix: ordered_key(1), len: 256 });
        let expected = source.hash();
        assert_eq!((without.graft(&subtree)).hash(), expected);
        assert_eq!((without.graft_batch(std::slice::from_ref(&subtree))).hash(), expected);
        assert_eq!(
            ops::merge(&without.root, without.root_path, &subtree.root, subtree.root_path).hash(),
            expected.0,
        );
        assert_eq!(
            ops::merge_batch(
                &without.root,
                without.root_path,
                &[(subtree.root.clone(), subtree.root_path)],
            )
            .hash(),
            expected.0,
        );

        // Full-depth views must survive both map encodings.
        for compact in [false, true] {
            let mut bytes = Vec::new();
            let restored = if compact {
                subtree.write_no_hash(&mut bytes).unwrap();
                TestMap::read_no_hash(&mut bytes.as_slice()).unwrap()
            } else {
                subtree.write(&mut bytes).unwrap();
                TestMap::read(&mut bytes.as_slice()).unwrap()
            };
            assert_eq!(restored.root_path, subtree.root_path);
            assert_eq!(restored.hash(), subtree.hash());
            assert_eq!((without.graft(&restored)).hash(), expected);
        }
    }

    #[test]
    fn trait_split_merge_roundtrip() {
        let m0 = TestMap::new();
        let updates: Vec<_> = (0..500).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = m0.update(&updates);
        let orig_hash = m.hash();

        let pfx = key(50).0;
        let path = MapKeyPath { prefix: MapKey(pfx), len: 8 };

        let (without, branch) = m.split(path);
        let merged = without.graft(&branch);

        assert_eq!(merged.hash(), orig_hash);
    }

    #[test]
    fn split_same_root_path_keeps_existing_subtrie_in_branch() {
        let m0 = TestMap::new();
        let updates: Vec<_> = (0..500).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = m0.update(&updates);

        let pfx = key(50).0;
        let path = MapKeyPath { prefix: MapKey(pfx), len: 8 };
        let (inside_idx, inside_key) = (0..500)
            .map(|i| (i, key(i)))
            .find(|(_, k)| prefix_bits_match_local(&pfx, &k.0, 8))
            .unwrap();

        let (_without, branch) = m.split(path);
        assert_eq!(branch.get(&inside_key), Some(TV(inside_idx as u64)));

        let (without_again, branch_again) = branch.split(path);

        assert_eq!(branch_again.get(&inside_key), branch.get(&inside_key));
        assert_eq!(without_again.get(&inside_key), None);
    }

    fn prefix_bits_match_local(prefix: &[u8; 32], k: &[u8; 32], bits: u16) -> bool {
        prefix_bits_match(prefix, k, bits)
    }

    #[test]
    fn patch_respects_root_key_path_view_filter() {
        let m0 = TestMap::new();
        let updates: Vec<_> = (0..200).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = m0.update(&updates);

        let pfx = key(10).0;
        let path = MapKeyPath { prefix: MapKey(pfx), len: 9 };

        let (_without, branch) = m.split(path);

        let inside_key =
            (0..200).map(key).find(|k| prefix_bits_match_local(&pfx, &k.0, 9)).unwrap();
        let outside_key =
            (0..200).map(key).find(|k| !prefix_bits_match_local(&pfx, &k.0, 9)).unwrap();

        let updated = branch.update(&[(inside_key, Some(TV(999))), (outside_key, Some(TV(888)))]);

        assert_eq!(updated.get(&inside_key), Some(TV(999)));
        assert_eq!(updated.get(&outside_key), None);
    }

    #[test]
    fn merge_with_global_empty_clears_all() {
        let m0 = TestMap::new();
        let updates: Vec<_> = (0..500).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = m0.update(&updates);

        let empty = TestMap::new();
        let merged = m.graft(&empty);

        assert_eq!(merged.hash(), TestMap::new().hash());
    }

    #[test]
    fn merge_empty_with_a_restores_a_for_global_root_path() {
        let m0 = TestMap::new();
        let updates: Vec<_> = (0..500).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = m0.update(&updates);
        let orig_hash = m.hash();

        let empty = TestMap::new();
        let merged = empty.graft(&m);

        assert_eq!(merged.hash(), orig_hash);
    }

    #[test]
    fn concurrent_readers_and_writer() {
        use std::thread;

        let m0 = TestMap::new();
        let updates: Vec<_> = (0..1000).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let base = m0.update(&updates);

        let base_clone = base.clone();

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let m = base_clone.clone();
                thread::Builder::new()
                    .spawn(move || {
                        for i in 0..1000 {
                            let got = m.get(&key(i));
                            assert_eq!(got, Some(TV(i as u64)));
                        }
                    })
                    .unwrap()
            })
            .collect();

        let mut current = base;
        for batch in 0..100 {
            let upd: Vec<_> =
                (0..10).map(|i| (key(1000 + batch * 10 + i), Some(TV(9999)))).collect();
            current = current.update(&upd);
        }

        for handle in readers {
            handle.join().unwrap();
        }

        assert_eq!(current.get(&key(1999)), Some(TV(9999)));
    }

    #[test]
    fn write_read_roundtrip() {
        let m0 = TestMap::new();
        let updates: Vec<_> = (0..500).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = m0.update(&updates);
        let orig_hash = m.hash();

        let mut buf = Vec::new();
        m.write(&mut buf).unwrap();

        let m2 = TestMap::read(&mut std::io::Cursor::new(&buf)).unwrap();

        for i in 0..500 {
            assert_eq!(m2.get(&key(i)), Some(TV(i as u64)));
        }
        assert_eq!(m2.hash(), orig_hash);
    }

    #[test]
    fn write_read_empty() {
        let m = TestMap::new();

        let mut buf = Vec::new();
        m.write(&mut buf).unwrap();

        let m2 = TestMap::read(&mut std::io::Cursor::new(&buf)).unwrap();
        assert!(m2.root.is_empty());
        assert_eq!(m.hash(), m2.hash());
    }

    #[test]
    fn write_read_after_deletes() {
        let m0 = TestMap::new();
        let updates: Vec<_> = (0..500).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = m0.update(&updates);
        let deletes: Vec<_> = (0..500).filter(|i| i % 2 == 0).map(|i| (key(i), None)).collect();
        let m = m.update(&deletes);
        let orig_hash = m.hash();

        let mut buf = Vec::new();
        m.write(&mut buf).unwrap();
        let m2 = TestMap::read(&mut std::io::Cursor::new(&buf)).unwrap();

        for i in 0..500 {
            if i % 2 == 0 {
                assert_eq!(m2.get(&key(i)), None);
            } else {
                assert_eq!(m2.get(&key(i)), Some(TV(i as u64)));
            }
        }
        assert_eq!(m2.hash(), orig_hash);
    }

    #[test]
    fn write_read_split_merge() {
        let m0 = TestMap::new();
        let updates: Vec<_> = (0..1000).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = m0.update(&updates);
        let orig_hash = m.hash();

        let pfx = key(100).0;
        let path = MapKeyPath { prefix: MapKey(pfx), len: 8 };
        let (a, b) = m.split(path);

        let mut buf_a = Vec::new();
        let mut buf_b = Vec::new();
        a.write(&mut buf_a).unwrap();
        b.write(&mut buf_b).unwrap();

        let a2 = TestMap::read(&mut std::io::Cursor::new(&buf_a)).unwrap();
        let b2 = TestMap::read(&mut std::io::Cursor::new(&buf_b)).unwrap();

        let merged = a2.graft(&b2);
        assert_eq!(merged.hash(), orig_hash);
    }

    #[test]
    fn collect_values_all_present() {
        let m0 = TestMap::new();
        let updates: Vec<_> = (0..200).map(|i| (key(i), Some(TV(i as u64 * 5)))).collect();
        let m = m0.update(&updates);

        let collected = m.collect_values();
        assert_eq!(collected.len(), 200);

        let map: std::collections::HashMap<MapKey, TV> = collected.into_iter().collect();
        for i in 0..200 {
            assert_eq!(map.get(&key(i)), Some(&TV(i as u64 * 5)));
        }
    }

    #[test]
    fn collect_values_empty() {
        let m = TestMap::new();
        assert!(m.collect_values().is_empty());
    }

    #[test]
    #[ignore]
    fn update_strategy_comparison() {
        use std::time::Instant;

        use rand::Rng;
        use rand::SeedableRng;

        const INITIAL_KEYS: usize = 20_000;
        const KEYS_PER_BLOCK: usize = 1000;
        const N_ROUNDS: usize = 50;

        // Build initial map
        let mut map = TestMap::new();
        for chunk_start in (0..INITIAL_KEYS).step_by(1000) {
            let chunk_end = (chunk_start + 1000).min(INITIAL_KEYS);
            let updates: Vec<_> =
                (chunk_start..chunk_end).map(|i| (key(i), Some(TV(i as u64)))).collect();
            map = map.update(&updates);
        }

        // Pre-generate updates
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let key_range = INITIAL_KEYS * 2;
        let all_updates: Vec<Vec<(MapKey, Option<TV>)>> = (0..N_ROUNDS)
            .map(|_| {
                (0..KEYS_PER_BLOCK)
                    .map(|_| {
                        let ki = rng.gen_range(0..key_range);
                        (key(ki), Some(TV(rng.gen::<u64>())))
                    })
                    .collect()
            })
            .collect();

        eprintln!("\n=== Update Strategy Comparison ===");
        eprintln!("  Initial map: {INITIAL_KEYS} keys");
        eprintln!("  Block size:  {KEYS_PER_BLOCK} keys");
        eprintln!("  Rounds:      {N_ROUNDS}");

        // --- Strategy A: Current batch update ---
        {
            let t = Instant::now();
            for updates in &all_updates {
                let _ = map.update(updates);
            }
            let elapsed = t.elapsed();
            let per_block = elapsed.as_micros() as f64 / N_ROUNDS as f64;
            eprintln!("\n  A. Batch update (current):");
            eprintln!("    per block: {:.0} µs ({:.2} ms)", per_block, per_block / 1000.0);
        }

        // --- Strategy B: Per-key sequential update (apply one key at a time) ---
        {
            let t = Instant::now();
            for updates in &all_updates {
                let mut m = map.clone();
                for kv in updates {
                    m = m.update(std::slice::from_ref(kv));
                }
            }
            let elapsed = t.elapsed();
            let per_block = elapsed.as_micros() as f64 / N_ROUNDS as f64;
            eprintln!("\n  B. Per-key update (1000 × single-key update):");
            eprintln!("    per block: {:.0} µs ({:.2} ms)", per_block, per_block / 1000.0);
        }

        // --- Strategy C: Small batches (100 keys × 10 batches) ---
        {
            let t = Instant::now();
            for updates in &all_updates {
                let mut m = map.clone();
                for chunk in updates.chunks(100) {
                    m = m.update(chunk);
                }
            }
            let elapsed = t.elapsed();
            let per_block = elapsed.as_micros() as f64 / N_ROUNDS as f64;
            eprintln!("\n  C. Small batches (10 × 100 keys):");
            eprintln!("    per block: {:.0} µs ({:.2} ms)", per_block, per_block / 1000.0);
        }

        // --- Strategy D: Small batches (50 keys × 20 batches) ---
        {
            let t = Instant::now();
            for updates in &all_updates {
                let mut m = map.clone();
                for chunk in updates.chunks(50) {
                    m = m.update(chunk);
                }
            }
            let elapsed = t.elapsed();
            let per_block = elapsed.as_micros() as f64 / N_ROUNDS as f64;
            eprintln!("\n  D. Small batches (20 × 50 keys):");
            eprintln!("    per block: {:.0} µs ({:.2} ms)", per_block, per_block / 1000.0);
        }

        // --- Strategy E: Batch of 1000 but with pre-sorted input ---
        {
            let mut sorted_updates: Vec<Vec<(MapKey, Option<TV>)>> = all_updates.clone();
            for batch in &mut sorted_updates {
                batch.sort_unstable_by_key(|a| a.0 .0);
            }
            for batch in &mut sorted_updates {
                batch.dedup_by_key(|kv| kv.0);
            }

            let t = Instant::now();
            for updates in &sorted_updates {
                let _ = map.update(updates);
            }
            let elapsed = t.elapsed();
            let per_block = elapsed.as_micros() as f64 / N_ROUNDS as f64;
            eprintln!("\n  E. Batch 1000 pre-sorted+deduped:");
            eprintln!("    per block: {:.0} µs ({:.2} ms)", per_block, per_block / 1000.0);
        }

        eprintln!();
    }

    #[test]
    #[ignore]
    fn block_cost_diagnostic() {
        use std::time::Instant;

        use rand::Rng;
        use rand::SeedableRng;

        const INITIAL_KEYS: usize = 20_000;
        const KEYS_PER_BLOCK: usize = 1000;

        // Build initial map
        let mut map = TestMap::new();
        for chunk_start in (0..INITIAL_KEYS).step_by(1000) {
            let chunk_end = (chunk_start + 1000).min(INITIAL_KEYS);
            let updates: Vec<_> =
                (chunk_start..chunk_end).map(|i| (key(i), Some(TV(i as u64)))).collect();
            map = map.update(&updates);
        }

        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let key_range = INITIAL_KEYS * 2;

        eprintln!("\n=== Block Cost Diagnostic ===\n");

        // Test 1: Random keys, update existing trie (the slow case)
        {
            let updates: Vec<_> = (0..KEYS_PER_BLOCK)
                .map(|_| {
                    let ki = rng.gen_range(0..key_range);
                    (key(ki), Some(TV(rng.gen::<u64>())))
                })
                .collect();

            // Warm up
            for _ in 0..3 {
                let _ = map.update(&updates);
            }

            let n = 50;
            let t = Instant::now();
            for _ in 0..n {
                let _ = map.update(&updates);
            }
            let per = t.elapsed().as_micros() as f64 / n as f64;
            eprintln!("  1. Random 1000 keys on 20K trie:     {per:.0} µs");
        }

        // Test 2: Sequential keys (good locality)
        {
            let updates: Vec<_> =
                (0..KEYS_PER_BLOCK).map(|i| (key(i), Some(TV(i as u64 + 999)))).collect();

            let n = 50;
            let t = Instant::now();
            for _ in 0..n {
                let _ = map.update(&updates);
            }
            let per = t.elapsed().as_micros() as f64 / n as f64;
            eprintln!("  2. Sequential 1000 keys on 20K trie: {per:.0} µs");
        }

        // Test 3: Random keys on a SMALL trie (2K keys — fits in cache)
        {
            let mut small_map = TestMap::new();
            let inserts: Vec<_> = (0..2000).map(|i| (key(i), Some(TV(i as u64)))).collect();
            small_map = small_map.update(&inserts);

            let updates: Vec<_> = (0..KEYS_PER_BLOCK)
                .map(|_| {
                    let ki = rng.gen_range(0..4000);
                    (key(ki), Some(TV(rng.gen::<u64>())))
                })
                .collect();

            let n = 50;
            let t = Instant::now();
            for _ in 0..n {
                let _ = small_map.update(&updates);
            }
            let per = t.elapsed().as_micros() as f64 / n as f64;
            eprintln!("  3. Random 1000 keys on 2K trie:      {per:.0} µs");
        }

        // Test 4: Small batch (100 keys) on 20K trie
        {
            let updates: Vec<_> = (0..100)
                .map(|_| {
                    let ki = rng.gen_range(0..key_range);
                    (key(ki), Some(TV(rng.gen::<u64>())))
                })
                .collect();

            let n = 200;
            let t = Instant::now();
            for _ in 0..n {
                let _ = map.update(&updates);
            }
            let per = t.elapsed().as_micros() as f64 / n as f64;
            eprintln!("  4. Random 100 keys on 20K trie:      {per:.0} µs");
        }

        // Test 5: Just the sort/normalize cost (no trie update)
        {
            let updates: Vec<_> = (0..KEYS_PER_BLOCK)
                .map(|_| {
                    let ki = rng.gen_range(0..key_range);
                    (key(ki), Some(TV(rng.gen::<u64>())))
                })
                .collect();

            let n = 500;
            let t = Instant::now();
            for _ in 0..n {
                let mut sorted = updates.clone();
                sorted.sort_unstable_by_key(|a| a.0 .0);
            }
            let per = t.elapsed().as_micros() as f64 / n as f64;
            eprintln!("  5. Sort 1000 MapKeys only:           {per:.0} µs");
        }

        // Test 6: Just BLAKE3 hashing cost
        {
            use blake3::Hasher;
            let n_hashes = 5000;
            let data = [0u8; 66];

            let rounds = 200;
            let t = Instant::now();
            for _ in 0..rounds {
                for _ in 0..n_hashes {
                    let mut h = Hasher::new();
                    h.update(&data);
                    std::hint::black_box(h.finalize());
                }
            }
            let per = t.elapsed().as_micros() as f64 / rounds as f64;
            eprintln!("  6. {n_hashes} BLAKE3 hashes:              {per:.0} µs");
        }

        // Test 7: Just Arc::new allocation cost
        {
            use std::sync::Arc;
            let n_allocs = 5000;

            let rounds = 200;
            let t = Instant::now();
            for _ in 0..rounds {
                for _ in 0..n_allocs {
                    std::hint::black_box(Arc::new([0u8; 200]));
                }
            }
            let per = t.elapsed().as_micros() as f64 / rounds as f64;
            eprintln!("  7. {n_allocs} Arc::new([u8;200]):          {per:.0} µs");
        }

        eprintln!();
    }

    #[test]
    #[ignore]
    fn block_cost_breakdown() {
        use std::time::Instant;

        use rand::Rng;
        use rand::SeedableRng;

        const INITIAL_KEYS: usize = 20_000;
        const KEYS_PER_BLOCK: usize = 1000;
        const N_BLOCKS: usize = 200;

        // Build initial map
        let mut map = TestMap::new();
        for chunk_start in (0..INITIAL_KEYS).step_by(1000) {
            let chunk_end = (chunk_start + 1000).min(INITIAL_KEYS);
            let updates: Vec<_> =
                (chunk_start..chunk_end).map(|i| (key(i), Some(TV(i as u64)))).collect();
            map = map.update(&updates);
        }

        eprintln!("\n=== Block Cost Breakdown ===");
        eprintln!("  Initial map: {INITIAL_KEYS} keys");
        eprintln!("  Block size:  {KEYS_PER_BLOCK} keys");
        eprintln!("  Blocks:      {N_BLOCKS}");

        // Pre-generate blocks
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let key_range = INITIAL_KEYS * 2;
        let blocks: Vec<Vec<(MapKey, Option<TV>)>> = (0..N_BLOCKS)
            .map(|_| {
                (0..KEYS_PER_BLOCK)
                    .map(|_| {
                        let ki = rng.gen_range(0..key_range);
                        (key(ki), Some(TV(rng.gen::<u64>())))
                    })
                    .collect()
            })
            .collect();

        // Warm up
        {
            let mut m = map.clone();
            for block in &blocks[..5] {
                m = m.update(block);
            }
        }

        // Measure: sequential apply (single trie growing over time)
        let mut m = map.clone();
        let t = Instant::now();
        for block in &blocks {
            m = m.update(block);
        }
        let total = t.elapsed();
        let per_block = total.as_micros() as f64 / N_BLOCKS as f64;

        eprintln!("\n  Sequential (single BT, trie growing):");
        eprintln!("    total:           {:.3}s", total.as_secs_f64());
        eprintln!("    per block:       {:.0} µs ({:.2} ms)", per_block, per_block / 1000.0);
        eprintln!("    blocks/sec:      {:.0}", 1_000_000.0 / per_block);

        // Measure: independent applies (same initial map, no growth)
        let t = Instant::now();
        for block in &blocks {
            let _ = map.update(block);
        }
        let total_ind = t.elapsed();
        let per_block_ind = total_ind.as_micros() as f64 / N_BLOCKS as f64;

        eprintln!("\n  Independent (same base map, no growth):");
        eprintln!("    total:           {:.3}s", total_ind.as_secs_f64());
        eprintln!(
            "    per block:       {:.0} µs ({:.2} ms)",
            per_block_ind,
            per_block_ind / 1000.0
        );
        eprintln!("    blocks/sec:      {:.0}", 1_000_000.0 / per_block_ind);
    }

    #[test]
    #[ignore]
    fn block_throughput() {
        use std::time::Instant;

        use rand::Rng;
        use rand::SeedableRng;
        use rayon::prelude::*;

        const N_BT: usize = 500;
        const BLOCKS_PER_SEC: usize = 3;
        const DURATION_SECS: usize = 10;
        const KEYS_PER_BLOCK: usize = 1000;
        const INITIAL_KEYS: usize = 20_000;

        let total_blocks = N_BT * BLOCKS_PER_SEC * DURATION_SECS;

        eprintln!("=== Block Throughput Benchmark ===");
        eprintln!("  Blockchain threads: {N_BT}");
        eprintln!("  Blocks/sec/BT:      {BLOCKS_PER_SEC}");
        eprintln!("  Keys/block:         {KEYS_PER_BLOCK}");
        eprintln!("  Duration:           {DURATION_SECS}s");
        eprintln!("  Total blocks:       {total_blocks}");
        eprintln!("  Initial keys/BT:    {INITIAL_KEYS}");
        eprintln!();

        // --- Phase 1: Create initial root map for each BT ---
        eprintln!("Phase 1: Building initial maps for {N_BT} BTs...");
        let t1 = Instant::now();

        let mut bt_maps: Vec<MultiMap<TV>> = Vec::with_capacity(N_BT);
        for bt in 0..N_BT {
            let mut map = TestMap::new();
            let base = bt * INITIAL_KEYS;
            for chunk_start in (0..INITIAL_KEYS).step_by(1000) {
                let chunk_end = (chunk_start + 1000).min(INITIAL_KEYS);
                let updates: Vec<_> = (chunk_start..chunk_end)
                    .map(|i| (key(base + i), Some(TV((base + i) as u64))))
                    .collect();
                map = map.update(&updates);
            }
            bt_maps.push(map);
        }

        eprintln!("  Done in {:.2}s", t1.elapsed().as_secs_f64());
        eprintln!();

        // --- Phase 2: Pre-generate block flow for each BT ---
        eprintln!("Phase 2: Generating block flow ({total_blocks} blocks)...");
        let t2 = Instant::now();

        struct Block {
            updates: Vec<(MapKey, Option<TV>)>,
            expected_root_hash: [u8; 32],
        }

        let mut bt_blocks: Vec<Vec<Block>> = Vec::with_capacity(N_BT);

        for (bt, bt_map) in bt_maps.iter_mut().enumerate().take(N_BT) {
            let mut rng = rand::rngs::StdRng::seed_from_u64(bt as u64 * 31337);
            let mut current_map = bt_map.clone();
            let base = bt * INITIAL_KEYS;
            let key_range = INITIAL_KEYS * 2;

            let mut blocks_for_bt = Vec::with_capacity(BLOCKS_PER_SEC * DURATION_SECS);

            for _block_idx in 0..(BLOCKS_PER_SEC * DURATION_SECS) {
                let updates: Vec<_> = (0..KEYS_PER_BLOCK)
                    .map(|_| {
                        let ki = base + rng.gen_range(0..key_range);
                        (key(ki), Some(TV(rng.gen::<u64>())))
                    })
                    .collect();

                let new_map = current_map.update(&updates);
                let root_hash = new_map.root.hash();

                blocks_for_bt.push(Block { updates, expected_root_hash: root_hash });

                current_map = new_map;
            }

            bt_blocks.push(blocks_for_bt);
        }

        eprintln!("  Done in {:.2}s", t2.elapsed().as_secs_f64());
        eprintln!();

        // --- Phase 3: Apply all blocks — parallel across BTs, sequential within BT ---
        eprintln!("Phase 3: Applying {total_blocks} blocks ({N_BT} BTs in parallel)...");

        let initial_maps = bt_maps.clone();

        let t3 = Instant::now();

        let results: Vec<(usize, usize, MultiMap<TV>, bool)> = bt_blocks
            .into_par_iter()
            .enumerate()
            .map(|(bt, blocks)| {
                let mut map = initial_maps[bt].clone();
                let mut applied = 0usize;
                let mut verified = true;

                for block in &blocks {
                    let new_map = map.update(&block.updates);
                    let hash = new_map.root.hash();

                    if hash != block.expected_root_hash {
                        verified = false;
                        break;
                    }

                    map = new_map;
                    applied += 1;
                }

                (bt, applied, map, verified)
            })
            .collect();

        let elapsed = t3.elapsed();

        let total_applied: usize = results.iter().map(|(_, a, _, _)| *a).sum();
        let all_verified = results.iter().all(|(_, _, _, ok)| *ok);
        let blocks_per_sec = total_applied as f64 / elapsed.as_secs_f64();
        let target = (N_BT * BLOCKS_PER_SEC) as f64;

        eprintln!("  Elapsed:     {:.3}s", elapsed.as_secs_f64());
        eprintln!("  Applied:     {total_applied} / {total_blocks}");
        eprintln!("  Verified:    {all_verified}");
        eprintln!("  Throughput:  {blocks_per_sec:.0} blocks/sec");
        eprintln!("  Target:      {target:.0} blocks/sec");
        eprintln!("  Headroom:    {:.1}x", blocks_per_sec / target);
        eprintln!();

        if blocks_per_sec >= target {
            eprintln!("  PASS");
        } else {
            eprintln!("  FAIL — {:.1}% of target", blocks_per_sec / target * 100.0);
        }

        assert!(all_verified, "hash verification failed");
        assert_eq!(total_applied, total_blocks, "not all blocks applied");
    }
}
