use std::io::Read as IoRead;
use std::io::Write as IoWrite;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::live_metrics::LiveMultiMapNodeCounter;
use crate::node::BranchData;
use crate::node::ExtData;
use crate::node::LeafData;
use crate::node::Node;
use crate::ops;
use crate::MapHash;
use crate::MapKey;
use crate::MapKeyPath;
use crate::MultiMapValue;

/// A reference to a MultiMap — holds the root node and prefix path.
/// Clone is cheap (Arc reference count bump). NOT Copy.
#[derive(Clone)]
pub struct MultiMap<V: MultiMapValue> {
    pub root: Node<V>,
    pub root_path: MapKeyPath,
}

impl<V: MultiMapValue> std::fmt::Debug for MultiMap<V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiMapRef")
            .field("root_hash", &hex::encode(self.root.hash()))
            .field("root_path_len", &self.root_path.len)
            .finish()
    }
}

/// Stateless repository — exists to satisfy the MapRepository trait.
/// All state lives in MultiMapRef via Arc-owned node trees.
#[derive(Clone)]
pub struct MultiMapRepository<V: MultiMapValue> {
    _phantom: PhantomData<V>,
}

impl<V: MultiMapValue> MultiMapRepository<V> {
    pub fn new() -> Self {
        Self { _phantom: PhantomData }
    }
}

impl<V: MultiMapValue> Default for MultiMapRepository<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: MultiMapValue> MultiMapRepository<V> {
    /// Serialize a map to a writer. Format is self-contained binary.
    pub fn write_map<W: IoWrite>(&self, map: &MultiMap<V>, writer: &mut W) -> anyhow::Result<()> {
        writer.write_all(&[map.root_path.len])?;
        writer.write_all(&map.root_path.prefix.0)?;
        Self::write_node(&map.root, writer)
    }

    /// Serialize a map without hashes — smaller output, faster write.
    /// Must be read back with `read_map_no_hash`.
    pub fn write_map_no_hash<W: IoWrite>(
        &self,
        map: &MultiMap<V>,
        writer: &mut W,
    ) -> anyhow::Result<()> {
        writer.write_all(&[map.root_path.len])?;
        writer.write_all(&map.root_path.prefix.0)?;
        Self::write_node_no_hash(&map.root, writer)
    }

    /// Deserialize a map from a reader. Hashes are read from the stream.
    pub fn read_map<R: IoRead>(&self, reader: &mut R) -> anyhow::Result<MultiMap<V>> {
        let mut len_buf = [0u8; 1];
        reader.read_exact(&mut len_buf)?;
        let mut prefix = [0u8; 32];
        reader.read_exact(&mut prefix)?;
        let root_path = MapKeyPath { prefix: MapKey(prefix), len: len_buf[0] };
        let root = Self::read_node(reader)?;
        Ok(MultiMap { root, root_path })
    }

    /// Deserialize a map written with `write_map_no_hash`.
    /// Hashes are recomputed from children (bottom-up) during read.
    pub fn read_map_no_hash<R: IoRead>(&self, reader: &mut R) -> anyhow::Result<MultiMap<V>> {
        let mut len_buf = [0u8; 1];
        reader.read_exact(&mut len_buf)?;
        let mut prefix = [0u8; 32];
        reader.read_exact(&mut prefix)?;
        let root_path = MapKeyPath { prefix: MapKey(prefix), len: len_buf[0] };
        let root = Self::read_node_no_hash(reader)?;
        Ok(MultiMap { root, root_path })
    }

    // --- write helpers (with hashes) ---

    fn write_node<W: IoWrite>(node: &Node<V>, w: &mut W) -> anyhow::Result<()> {
        match node {
            Node::Empty => w.write_all(&[0x00])?,
            Node::Leaf(data) => {
                w.write_all(&[0x01])?;
                w.write_all(&data.hash)?;
                data.value.write_value(w)?
            }
            Node::Branch(data) => {
                w.write_all(&[0x02])?;
                w.write_all(&data.hash)?;
                w.write_all(&data.bitmap.to_le_bytes())?;
                for child in data.children.iter() {
                    Self::write_node(child, w)?;
                }
            }
            Node::Ext(data) => {
                w.write_all(&[0x03])?;
                w.write_all(&data.hash)?;
                w.write_all(&[data.nibble_count])?;
                w.write_all(&data.nibbles[..data.nibble_count as usize])?;
                Self::write_node(&data.child, w)?;
            }
        }
        Ok(())
    }

    // --- write helpers (compact, no hashes) ---

    fn write_node_no_hash<W: IoWrite>(node: &Node<V>, w: &mut W) -> anyhow::Result<()> {
        match node {
            Node::Empty => w.write_all(&[0x00])?,
            Node::Leaf(data) => {
                w.write_all(&[0x01])?;
                data.value.write_value_no_hash(w)?;
            }
            Node::Branch(data) => {
                w.write_all(&[0x02])?;
                w.write_all(&data.bitmap.to_le_bytes())?;
                for child in data.children.iter() {
                    Self::write_node_no_hash(child, w)?;
                }
            }
            Node::Ext(data) => {
                w.write_all(&[0x03])?;
                w.write_all(&[data.nibble_count])?;
                w.write_all(&data.nibbles[..data.nibble_count as usize])?;
                Self::write_node_no_hash(&data.child, w)?;
            }
        }
        Ok(())
    }

    // --- read helpers (with stored hashes) ---

    fn read_node<R: IoRead>(r: &mut R) -> anyhow::Result<Node<V>> {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag)?;
        match tag[0] {
            0x00 => Ok(Node::Empty),
            0x01 => Self::read_leaf(r),
            0x02 => Self::read_branch(r),
            0x03 => Self::read_ext(r),
            other => Err(anyhow::anyhow!("invalid node tag: 0x{other:02x}")),
        }
    }

    fn read_leaf<R: IoRead>(r: &mut R) -> anyhow::Result<Node<V>> {
        let mut hash = [0u8; 32];
        r.read_exact(&mut hash)?;
        let value = V::read_value(r)?;
        Ok(Node::Leaf(Arc::new(LeafData {
            hash,
            value,
            _live_counter: LiveMultiMapNodeCounter::new(),
        })))
    }

    fn read_branch<R: IoRead>(r: &mut R) -> anyhow::Result<Node<V>> {
        let mut hash = [0u8; 32];
        r.read_exact(&mut hash)?;
        let mut bm = [0u8; 2];
        r.read_exact(&mut bm)?;
        let bitmap = u16::from_le_bytes(bm);
        let mut children: [Node<V>; 16] = Default::default();
        for slot in children.iter_mut() {
            *slot = Self::read_node(r)?;
        }
        Ok(Node::Branch(Arc::new(BranchData {
            hash,
            bitmap,
            children,
            _live_counter: LiveMultiMapNodeCounter::new(),
        })))
    }

    fn read_ext<R: IoRead>(r: &mut R) -> anyhow::Result<Node<V>> {
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
        Ok(Node::Ext(Arc::new(ExtData {
            hash,
            nibble_count,
            nibbles,
            child,
            _live_counter: LiveMultiMapNodeCounter::new(),
        })))
    }

    // --- read helpers (rehash from children) ---

    fn read_node_no_hash<R: IoRead>(r: &mut R) -> anyhow::Result<Node<V>> {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag)?;
        match tag[0] {
            0x00 => Ok(Node::Empty),
            0x01 => Self::read_leaf_no_hash(r),
            0x02 => Self::read_branch_no_hash(r),
            0x03 => Self::read_ext_no_hash(r),
            other => anyhow::bail!("invalid node tag: 0x{other:02x}",),
        }
    }

    fn read_leaf_no_hash<R: IoRead>(r: &mut R) -> anyhow::Result<Node<V>> {
        let value = V::read_value_no_hash(r)?;
        Ok(Node::new_leaf(value))
    }

    fn read_branch_no_hash<R: IoRead>(r: &mut R) -> anyhow::Result<Node<V>> {
        let mut bm = [0u8; 2];
        r.read_exact(&mut bm)?;
        let bitmap = u16::from_le_bytes(bm);
        let mut children: [Node<V>; 16] = Default::default();
        for slot in children.iter_mut() {
            *slot = Self::read_node_no_hash(r)?;
        }
        Ok(Node::new_branch(bitmap, children)) // new_branch computes hash from children
    }

    fn read_ext_no_hash<R: IoRead>(r: &mut R) -> anyhow::Result<Node<V>> {
        let mut count_buf = [0u8; 1];
        r.read_exact(&mut count_buf)?;
        let nibble_count = count_buf[0];
        let mut nibbles = [0u8; 64];
        r.read_exact(&mut nibbles[..nibble_count as usize])?;
        let child = Self::read_node_no_hash(r)?;
        if child.is_empty() {
            return Err(anyhow::anyhow!("ext node has no child"));
        }
        Ok(Node::new_ext(&nibbles[..nibble_count as usize], child)) // new_ext computes hash
    }
}

// Core operations available for all MultiMapValue types (including non-Copy, non-Serialize).
impl<V: MultiMapValue> MultiMapRepository<V> {
    pub fn new_map() -> MultiMap<V> {
        MultiMap { root: Node::Empty, root_path: MapKeyPath::default() }
    }

    pub fn map_hash(&self, map: &MultiMap<V>) -> MapHash {
        MapHash(map.root.hash())
    }

    pub fn map_get(&self, map: &MultiMap<V>, key: &MapKey) -> Option<V> {
        ops::get(&map.root, &map.root_path, key)
    }

    pub fn map_update(&self, map: &MultiMap<V>, updates: &[(MapKey, Option<V>)]) -> MultiMap<V> {
        let new_root = ops::update(&map.root, &map.root_path, updates);
        MultiMap { root: new_root, root_path: map.root_path }
    }

    /// Fast path for single key insert/replace/delete.
    /// Skips sorting, dedup, and range splitting — walks the trie directly.
    pub fn map_update_single(
        &self,
        map: &MultiMap<V>,
        key: &MapKey,
        value: Option<V>,
    ) -> MultiMap<V> {
        let new_root = ops::update_single(&map.root, &map.root_path, key, value);
        MultiMap { root: new_root, root_path: map.root_path }
    }

    pub fn map_split(&self, map: &MultiMap<V>, key_path: MapKeyPath) -> (MultiMap<V>, MultiMap<V>) {
        let (without, branch) = ops::split(&map.root, map.root_path, key_path);
        (
            MultiMap { root: without, root_path: map.root_path },
            MultiMap { root: branch, root_path: key_path },
        )
    }

    pub fn merge(&self, a: &MultiMap<V>, b: &MultiMap<V>) -> MultiMap<V> {
        let merged = ops::merge(&a.root, a.root_path, &b.root, b.root_path);
        MultiMap { root: merged, root_path: a.root_path }
    }

    /// Merge multiple subtrees into `a` in a single pass.
    /// Much faster than calling `merge` repeatedly because canonicalize runs only once.
    pub fn merge_batch(&self, a: &MultiMap<V>, subtrees: &[MultiMap<V>]) -> MultiMap<V> {
        let sub_pairs: Vec<_> = subtrees.iter().map(|s| (s.root.clone(), s.root_path)).collect();
        let merged = ops::merge_batch(&a.root, a.root_path, &sub_pairs);
        MultiMap { root: merged, root_path: a.root_path }
    }

    pub fn collect_values(&self, map: &MultiMap<V>) -> Vec<(MapKey, V)> {
        let mut result = Vec::new();
        if map.root.is_empty() {
            return result;
        }

        let start_nibbles = (map.root_path.len as usize) / 4;
        let mut stack: Vec<(&Node<V>, Vec<u8>)> = vec![(&map.root, Vec::new())];

        while let Some((node, nibble_path)) = stack.pop() {
            match node {
                Node::Empty => {}
                Node::Leaf(data) => {
                    let mut key_bytes = map.root_path.prefix.0;
                    for (i, &nib) in nibble_path.iter().enumerate() {
                        let pos = start_nibbles + i;
                        let byte_idx = pos / 2;
                        if pos % 2 == 0 {
                            key_bytes[byte_idx] = (key_bytes[byte_idx] & 0x0F) | (nib << 4);
                        } else {
                            key_bytes[byte_idx] = (key_bytes[byte_idx] & 0xF0) | (nib & 0x0F);
                        }
                    }
                    result.push((MapKey(key_bytes), data.value.clone()));
                }
                Node::Branch(data) => {
                    for nib in 0..16u8 {
                        if !data.children[nib as usize].is_empty() {
                            let mut child_path = nibble_path.clone();
                            child_path.push(nib);
                            stack.push((&data.children[nib as usize], child_path));
                        }
                    }
                }
                Node::Ext(data) => {
                    let mut child_path = nibble_path.clone();
                    child_path.extend_from_slice(&data.nibbles[..data.nibble_count as usize]);
                    stack.push((&data.child, child_path));
                }
            }
        }

        result
    }

    /// Returns a lazy iterator over all (key, &value) pairs in the map.
    pub fn iter<'a>(&'a self, map: &'a MultiMap<V>) -> MapIter<'a, V> {
        let start_nibbles = (map.root_path.len as usize) / 4;
        let mut stack = Vec::new();
        if !map.root.is_empty() {
            stack.push((&map.root, Vec::new()));
        }
        MapIter { stack, prefix: map.root_path.prefix.0, start_nibbles }
    }
}

/// Lazy iterator over (MapKey, &V) entries in a MultiMap.
pub struct MapIter<'a, V: MultiMapValue> {
    stack: Vec<(&'a Node<V>, Vec<u8>)>,
    prefix: [u8; 32],
    start_nibbles: usize,
}

impl<'a, V: MultiMapValue> Iterator for MapIter<'a, V> {
    type Item = (MapKey, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (node, nibble_path) = self.stack.pop()?;
            match node {
                Node::Empty => continue,
                Node::Leaf(data) => {
                    let mut key_bytes = self.prefix;
                    for (i, &nib) in nibble_path.iter().enumerate() {
                        let pos = self.start_nibbles + i;
                        let byte_idx = pos / 2;
                        if pos % 2 == 0 {
                            key_bytes[byte_idx] = (key_bytes[byte_idx] & 0x0F) | (nib << 4);
                        } else {
                            key_bytes[byte_idx] = (key_bytes[byte_idx] & 0xF0) | (nib & 0x0F);
                        }
                    }
                    return Some((MapKey(key_bytes), &data.value));
                }
                Node::Branch(data) => {
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
                Node::Ext(data) => {
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

    fn key(i: usize) -> MapKey {
        let mut h = Hasher::new();
        h.update(b"key:");
        h.update(&(i as u64).to_le_bytes());
        MapKey(*h.finalize().as_bytes())
    }

    type Repo = MultiMapRepository<TV>;

    #[test]
    fn trait_empty_get() {
        let repo = Repo::new();
        let m = Repo::new_map();
        assert_eq!(repo.map_get(&m, &key(0)), None);
    }

    #[test]
    fn trait_insert_get() {
        let repo = Repo::new();
        let m0 = Repo::new_map();
        let m1 = repo.map_update(&m0, &[(key(1), Some(TV(42)))]);
        assert_eq!(repo.map_get(&m1, &key(1)), Some(TV(42)));
        assert_eq!(repo.map_get(&m0, &key(1)), None);
    }

    #[test]
    fn trait_split_merge_roundtrip() {
        let repo = Repo::new();
        let m0 = Repo::new_map();
        let updates: Vec<_> = (0..500).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = repo.map_update(&m0, &updates);
        let orig_hash = repo.map_hash(&m);

        let pfx = key(50).0;
        let path = MapKeyPath { prefix: MapKey(pfx), len: 8 };

        let (without, branch) = repo.map_split(&m, path);
        let merged = repo.merge(&without, &branch);

        assert_eq!(repo.map_hash(&merged), orig_hash);
    }

    fn prefix_bits_match_local(prefix: &[u8; 32], k: &[u8; 32], bits: u8) -> bool {
        prefix_bits_match(prefix, k, bits)
    }

    #[test]
    fn patch_respects_root_key_path_view_filter() {
        let repo = Repo::new();
        let m0 = Repo::new_map();
        let updates: Vec<_> = (0..200).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = repo.map_update(&m0, &updates);

        let pfx = key(10).0;
        let path = MapKeyPath { prefix: MapKey(pfx), len: 9 };

        let (_without, branch) = repo.map_split(&m, path);

        let inside_key =
            (0..200).map(key).find(|k| prefix_bits_match_local(&pfx, &k.0, 9)).unwrap();
        let outside_key =
            (0..200).map(key).find(|k| !prefix_bits_match_local(&pfx, &k.0, 9)).unwrap();

        let updated =
            repo.map_update(&branch, &[(inside_key, Some(TV(999))), (outside_key, Some(TV(888)))]);

        assert_eq!(repo.map_get(&updated, &inside_key), Some(TV(999)));
        assert_eq!(repo.map_get(&updated, &outside_key), None);
    }

    #[test]
    fn merge_with_global_empty_clears_all() {
        let repo = Repo::new();
        let m0 = Repo::new_map();
        let updates: Vec<_> = (0..500).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = repo.map_update(&m0, &updates);

        let empty = Repo::new_map();
        let merged = repo.merge(&m, &empty);

        assert_eq!(repo.map_hash(&merged), repo.map_hash(&Repo::new_map()));
    }

    #[test]
    fn merge_empty_with_a_restores_a_for_global_root_path() {
        let repo = Repo::new();
        let m0 = Repo::new_map();
        let updates: Vec<_> = (0..500).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = repo.map_update(&m0, &updates);
        let orig_hash = repo.map_hash(&m);

        let empty = Repo::new_map();
        let merged = repo.merge(&empty, &m);

        assert_eq!(repo.map_hash(&merged), orig_hash);
    }

    #[test]
    fn concurrent_readers_and_writer() {
        use std::thread;

        let repo = Repo::new();
        let m0 = Repo::new_map();
        let updates: Vec<_> = (0..1000).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let base = repo.map_update(&m0, &updates);

        let base_clone = base.clone();
        let repo_clone = repo.clone();

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let r = repo_clone.clone();
                let m = base_clone.clone();
                thread::Builder::new()
                    .spawn(move || {
                        for i in 0..1000 {
                            let got = r.map_get(&m, &key(i));
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
            current = repo.map_update(&current, &upd);
        }

        for handle in readers {
            handle.join().unwrap();
        }

        assert_eq!(repo.map_get(&current, &key(1999)), Some(TV(9999)));
    }

    #[test]
    fn write_read_roundtrip() {
        let repo = Repo::new();
        let m0 = Repo::new_map();
        let updates: Vec<_> = (0..500).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = repo.map_update(&m0, &updates);
        let orig_hash = repo.map_hash(&m);

        let mut buf = Vec::new();
        repo.write_map(&m, &mut buf).unwrap();

        let m2 = repo.read_map(&mut std::io::Cursor::new(&buf)).unwrap();

        for i in 0..500 {
            assert_eq!(repo.map_get(&m2, &key(i)), Some(TV(i as u64)));
        }
        assert_eq!(repo.map_hash(&m2), orig_hash);
    }

    #[test]
    fn write_read_empty() {
        let repo = Repo::new();
        let m = Repo::new_map();

        let mut buf = Vec::new();
        repo.write_map(&m, &mut buf).unwrap();

        let m2 = repo.read_map(&mut std::io::Cursor::new(&buf)).unwrap();
        assert!(m2.root.is_empty());
        assert_eq!(repo.map_hash(&m), repo.map_hash(&m2));
    }

    #[test]
    fn write_read_after_deletes() {
        let repo = Repo::new();
        let m0 = Repo::new_map();
        let updates: Vec<_> = (0..500).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = repo.map_update(&m0, &updates);
        let deletes: Vec<_> = (0..500).filter(|i| i % 2 == 0).map(|i| (key(i), None)).collect();
        let m = repo.map_update(&m, &deletes);
        let orig_hash = repo.map_hash(&m);

        let mut buf = Vec::new();
        repo.write_map(&m, &mut buf).unwrap();
        let m2 = repo.read_map(&mut std::io::Cursor::new(&buf)).unwrap();

        for i in 0..500 {
            if i % 2 == 0 {
                assert_eq!(repo.map_get(&m2, &key(i)), None);
            } else {
                assert_eq!(repo.map_get(&m2, &key(i)), Some(TV(i as u64)));
            }
        }
        assert_eq!(repo.map_hash(&m2), orig_hash);
    }

    #[test]
    fn write_read_split_merge() {
        let repo = Repo::new();
        let m0 = Repo::new_map();
        let updates: Vec<_> = (0..1000).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = repo.map_update(&m0, &updates);
        let orig_hash = repo.map_hash(&m);

        let pfx = key(100).0;
        let path = MapKeyPath { prefix: MapKey(pfx), len: 8 };
        let (a, b) = repo.map_split(&m, path);

        let mut buf_a = Vec::new();
        let mut buf_b = Vec::new();
        repo.write_map(&a, &mut buf_a).unwrap();
        repo.write_map(&b, &mut buf_b).unwrap();

        let a2 = repo.read_map(&mut std::io::Cursor::new(&buf_a)).unwrap();
        let b2 = repo.read_map(&mut std::io::Cursor::new(&buf_b)).unwrap();

        let merged = repo.merge(&a2, &b2);
        assert_eq!(repo.map_hash(&merged), orig_hash);
    }

    #[test]
    fn collect_values_all_present() {
        let repo = Repo::new();
        let m0 = Repo::new_map();
        let updates: Vec<_> = (0..200).map(|i| (key(i), Some(TV(i as u64 * 5)))).collect();
        let m = repo.map_update(&m0, &updates);

        let collected = repo.collect_values(&m);
        assert_eq!(collected.len(), 200);

        let map: std::collections::HashMap<MapKey, TV> = collected.into_iter().collect();
        for i in 0..200 {
            assert_eq!(map.get(&key(i)), Some(&TV(i as u64 * 5)));
        }
    }

    #[test]
    fn collect_values_empty() {
        let repo = Repo::new();
        let m = Repo::new_map();
        assert!(repo.collect_values(&m).is_empty());
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

        let repo = Repo::new();

        // Build initial map
        let mut map = Repo::new_map();
        for chunk_start in (0..INITIAL_KEYS).step_by(1000) {
            let chunk_end = (chunk_start + 1000).min(INITIAL_KEYS);
            let updates: Vec<_> =
                (chunk_start..chunk_end).map(|i| (key(i), Some(TV(i as u64)))).collect();
            map = repo.map_update(&map, &updates);
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
                let _ = repo.map_update(&map, updates);
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
                    m = repo.map_update(&m, std::slice::from_ref(kv));
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
                    m = repo.map_update(&m, chunk);
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
                    m = repo.map_update(&m, chunk);
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
                let _ = repo.map_update(&map, updates);
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

        let repo = Repo::new();

        // Build initial map
        let mut map = Repo::new_map();
        for chunk_start in (0..INITIAL_KEYS).step_by(1000) {
            let chunk_end = (chunk_start + 1000).min(INITIAL_KEYS);
            let updates: Vec<_> =
                (chunk_start..chunk_end).map(|i| (key(i), Some(TV(i as u64)))).collect();
            map = repo.map_update(&map, &updates);
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
                let _ = repo.map_update(&map, &updates);
            }

            let n = 50;
            let t = Instant::now();
            for _ in 0..n {
                let _ = repo.map_update(&map, &updates);
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
                let _ = repo.map_update(&map, &updates);
            }
            let per = t.elapsed().as_micros() as f64 / n as f64;
            eprintln!("  2. Sequential 1000 keys on 20K trie: {per:.0} µs");
        }

        // Test 3: Random keys on a SMALL trie (2K keys — fits in cache)
        {
            let mut small_map = Repo::new_map();
            let inserts: Vec<_> = (0..2000).map(|i| (key(i), Some(TV(i as u64)))).collect();
            small_map = repo.map_update(&small_map, &inserts);

            let updates: Vec<_> = (0..KEYS_PER_BLOCK)
                .map(|_| {
                    let ki = rng.gen_range(0..4000);
                    (key(ki), Some(TV(rng.gen::<u64>())))
                })
                .collect();

            let n = 50;
            let t = Instant::now();
            for _ in 0..n {
                let _ = repo.map_update(&small_map, &updates);
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
                let _ = repo.map_update(&map, &updates);
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

        let repo = Repo::new();

        // Build initial map
        let mut map = Repo::new_map();
        for chunk_start in (0..INITIAL_KEYS).step_by(1000) {
            let chunk_end = (chunk_start + 1000).min(INITIAL_KEYS);
            let updates: Vec<_> =
                (chunk_start..chunk_end).map(|i| (key(i), Some(TV(i as u64)))).collect();
            map = repo.map_update(&map, &updates);
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
                m = repo.map_update(&m, block);
            }
        }

        // Measure: sequential apply (single trie growing over time)
        let mut m = map.clone();
        let t = Instant::now();
        for block in &blocks {
            m = repo.map_update(&m, block);
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
            let _ = repo.map_update(&map, block);
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

        let repo = Repo::new();

        // --- Phase 1: Create initial root map for each BT ---
        eprintln!("Phase 1: Building initial maps for {N_BT} BTs...");
        let t1 = Instant::now();

        let mut bt_maps: Vec<MultiMap<TV>> = Vec::with_capacity(N_BT);
        for bt in 0..N_BT {
            let mut map = Repo::new_map();
            let base = bt * INITIAL_KEYS;
            for chunk_start in (0..INITIAL_KEYS).step_by(1000) {
                let chunk_end = (chunk_start + 1000).min(INITIAL_KEYS);
                let updates: Vec<_> = (chunk_start..chunk_end)
                    .map(|i| (key(base + i), Some(TV((base + i) as u64))))
                    .collect();
                map = repo.map_update(&map, &updates);
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

                let new_map = repo.map_update(&current_map, &updates);
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
        let repo_ref = &repo;

        let t3 = Instant::now();

        let results: Vec<(usize, usize, MultiMap<TV>, bool)> = bt_blocks
            .into_par_iter()
            .enumerate()
            .map(|(bt, blocks)| {
                let mut map = initial_maps[bt].clone();
                let mut applied = 0usize;
                let mut verified = true;

                for block in &blocks {
                    let new_map = repo_ref.map_update(&map, &block.updates);
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
