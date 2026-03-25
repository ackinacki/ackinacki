use std::io::Read as IoRead;
use std::io::Write as IoWrite;
use std::io::{self};
use std::marker::PhantomData;
use std::sync::Arc;

use trie_map::MapHash;
use trie_map::MapKey;
use trie_map::MapKeyPath;
use trie_map::MapRepository;
use trie_map::MapValue;

use crate::node::BranchData;
use crate::node::ExtData;
use crate::node::LeafData;
use crate::node::Node;
use crate::ops;
use crate::MultiMapValue;

/// A reference to a MultiMap — holds the root node and prefix path.
/// Clone is cheap (Arc reference count bump). NOT Copy.
#[derive(Clone)]
pub struct MultiMapRef<V: MultiMapValue> {
    pub root: Node<V>,
    pub root_path: MapKeyPath,
}

impl<V: MultiMapValue> std::fmt::Debug for MultiMapRef<V> {
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
    pub fn write_map<W: IoWrite>(&self, map: &MultiMapRef<V>, writer: &mut W) -> io::Result<()> {
        writer.write_all(&[map.root_path.len])?;
        writer.write_all(&map.root_path.prefix.0)?;
        Self::write_node(&map.root, writer)
    }

    /// Deserialize a map from a reader.
    pub fn read_map<R: IoRead>(&self, reader: &mut R) -> io::Result<MultiMapRef<V>> {
        let mut len_buf = [0u8; 1];
        reader.read_exact(&mut len_buf)?;
        let mut prefix = [0u8; 32];
        reader.read_exact(&mut prefix)?;
        let root_path = MapKeyPath { prefix: MapKey(prefix), len: len_buf[0] };
        let root = Self::read_node(reader)?;
        Ok(MultiMapRef { root, root_path })
    }

    // --- write helpers ---

    fn write_node<W: IoWrite>(node: &Node<V>, w: &mut W) -> io::Result<()> {
        match node {
            Node::Empty => w.write_all(&[0x00]),
            Node::Leaf(data) => {
                w.write_all(&[0x01])?;
                w.write_all(&data.hash)?;
                data.value.write_value(w)
            }
            Node::Branch(data) => {
                w.write_all(&[0x02])?;
                w.write_all(&data.hash)?;
                w.write_all(&data.bitmap.to_le_bytes())?;
                for child in data.children.iter() {
                    Self::write_node(child, w)?;
                }
                Ok(())
            }
            Node::Ext(data) => {
                w.write_all(&[0x03])?;
                w.write_all(&data.hash)?;
                w.write_all(&[data.nibble_count])?;
                w.write_all(&data.nibbles[..data.nibble_count as usize])?;
                Self::write_node(&data.child, w)
            }
        }
    }

    // --- read helpers ---

    fn read_node<R: IoRead>(r: &mut R) -> io::Result<Node<V>> {
        let mut tag = [0u8; 1];
        r.read_exact(&mut tag)?;
        match tag[0] {
            0x00 => Ok(Node::Empty),
            0x01 => Self::read_leaf(r),
            0x02 => Self::read_branch(r),
            0x03 => Self::read_ext(r),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid node tag: 0x{other:02x}"),
            )),
        }
    }

    fn read_leaf<R: IoRead>(r: &mut R) -> io::Result<Node<V>> {
        let mut hash = [0u8; 32];
        r.read_exact(&mut hash)?;
        let value = V::read_value(r)?;
        Ok(Node::Leaf(Arc::new(LeafData { hash, value })))
    }

    fn read_branch<R: IoRead>(r: &mut R) -> io::Result<Node<V>> {
        let mut hash = [0u8; 32];
        r.read_exact(&mut hash)?;
        let mut bm = [0u8; 2];
        r.read_exact(&mut bm)?;
        let bitmap = u16::from_le_bytes(bm);
        let mut children: [Node<V>; 16] = Default::default();
        for slot in children.iter_mut() {
            *slot = Self::read_node(r)?;
        }
        Ok(Node::Branch(Arc::new(BranchData { hash, bitmap, children })))
    }

    fn read_ext<R: IoRead>(r: &mut R) -> io::Result<Node<V>> {
        let mut hash = [0u8; 32];
        r.read_exact(&mut hash)?;
        let mut count_buf = [0u8; 1];
        r.read_exact(&mut count_buf)?;
        let nibble_count = count_buf[0];
        let mut nibbles = [0u8; 64];
        r.read_exact(&mut nibbles[..nibble_count as usize])?;
        let child = Self::read_node(r)?;
        if child.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "ext node has no child"));
        }
        Ok(Node::Ext(Arc::new(ExtData { hash, nibble_count, nibbles, child })))
    }
}

// Core operations available for all MultiMapValue types (including non-Copy, non-Serialize).
impl<V: MultiMapValue> MultiMapRepository<V> {
    pub fn new_map() -> MultiMapRef<V> {
        MultiMapRef { root: Node::Empty, root_path: MapKeyPath::default() }
    }

    pub fn map_hash(&self, map: &MultiMapRef<V>) -> MapHash {
        MapHash(map.root.hash())
    }

    pub fn map_get(&self, map: &MultiMapRef<V>, key: &MapKey) -> Option<V> {
        ops::get(&map.root, &map.root_path, key)
    }

    pub fn map_update(
        &self,
        map: &MultiMapRef<V>,
        updates: &[(MapKey, Option<V>)],
    ) -> MultiMapRef<V> {
        let new_root = ops::update(&map.root, &map.root_path, updates);
        MultiMapRef { root: new_root, root_path: map.root_path }
    }

    pub fn map_split(
        &self,
        map: &MultiMapRef<V>,
        key_path: MapKeyPath,
    ) -> (MultiMapRef<V>, MultiMapRef<V>) {
        let (without, branch) = ops::split(&map.root, map.root_path, key_path);
        (
            MultiMapRef { root: without, root_path: map.root_path },
            MultiMapRef { root: branch, root_path: key_path },
        )
    }

    pub fn merge(&self, a: &MultiMapRef<V>, b: &MultiMapRef<V>) -> MultiMapRef<V> {
        let merged = ops::merge(&a.root, a.root_path, &b.root, b.root_path);
        MultiMapRef { root: merged, root_path: a.root_path }
    }

    pub fn collect_values(&self, map: &MultiMapRef<V>) -> Vec<(MapKey, V)> {
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
}

impl<V: MapValue> MapRepository for MultiMapRepository<V> {
    type MapRef = MultiMapRef<V>;
    type Value = V;

    fn new_map() -> Self::MapRef {
        Self::new_map()
    }

    fn map_hash(&self, map: &Self::MapRef) -> MapHash {
        self.map_hash(map)
    }

    fn map_key_path(&self, map: &Self::MapRef) -> MapKeyPath {
        map.root_path
    }

    fn map_get(&self, map: &Self::MapRef, key: &MapKey) -> Option<V> {
        self.map_get(map, key)
    }

    fn map_batch_get(&self, map: &Self::MapRef, keys: &[MapKey]) -> Vec<Option<V>> {
        keys.iter().map(|k| ops::get(&map.root, &map.root_path, k)).collect()
    }

    fn map_update(&self, map: &Self::MapRef, updates: &[(MapKey, Option<V>)]) -> Self::MapRef {
        self.map_update(map, updates)
    }

    fn map_split(&self, map: &Self::MapRef, key_path: MapKeyPath) -> (Self::MapRef, Self::MapRef) {
        self.map_split(map, key_path)
    }

    fn merge(&self, a: &Self::MapRef, b: &Self::MapRef) -> Self::MapRef {
        MultiMapRepository::merge(self, a, b)
    }
}

#[cfg(test)]
mod tests {
    use blake3::Hasher;
    use node_types::Blake3Hashable;
    use serde::Deserialize;
    use serde::Serialize;

    use super::*;

    #[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct TV(pub u64);

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

    #[test]
    fn trait_cross_hash_with_trie_map() {
        use trie_map::trie::smt::TrieMapRepository;

        let trie_repo = TrieMapRepository::<TV>::new();
        let multi_repo = Repo::new();

        let updates: Vec<_> = (0..200).map(|i| (key(i), Some(TV(i as u64 * 7)))).collect();

        let t = trie_repo.map_update(&TrieMapRepository::<TV>::new_map(), &updates);
        let m = multi_repo.map_update(&Repo::new_map(), &updates);

        assert_eq!(trie_repo.map_hash(&t).0, multi_repo.map_hash(&m).0);
    }

    #[test]
    fn trait_generic_function() {
        fn count_present<R: MapRepository>(repo: &R, map: &R::MapRef, keys: &[MapKey]) -> usize {
            keys.iter().filter(|k| repo.map_get(map, k).is_some()).count()
        }

        let repo = Repo::new();
        let m0 = Repo::new_map();
        let updates: Vec<_> = (0..50).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = repo.map_update(&m0, &updates);

        let all_keys: Vec<_> = (0..100).map(key).collect();
        assert_eq!(count_present(&repo, &m, &all_keys), 50);
    }

    fn prefix_bits_match_local(prefix: &[u8; 32], k: &[u8; 32], bits: u8) -> bool {
        trie_map::trie::arena::prefix_bits_match(prefix, k, bits)
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
    fn cross_hash_comprehensive() {
        use trie_map::trie::smt::TrieMapRepository;

        let trie_repo = TrieMapRepository::<TV>::new();
        let multi_repo = Repo::new();

        let updates: Vec<_> = (0..1000).map(|i| (key(i), Some(TV(i as u64)))).collect();

        let t = trie_repo.map_update(&TrieMapRepository::<TV>::new_map(), &updates);
        let m = multi_repo.map_update(&Repo::new_map(), &updates);
        assert_eq!(trie_repo.map_hash(&t).0, multi_repo.map_hash(&m).0, "after insert");

        // Delete every 3rd
        let del: Vec<_> = (0..1000).filter(|i| i % 3 == 0).map(|i| (key(i), None)).collect();
        let t2 = trie_repo.map_update(&t, &del);
        let m2 = multi_repo.map_update(&m, &del);
        assert_eq!(trie_repo.map_hash(&t2).0, multi_repo.map_hash(&m2).0, "after delete");

        // Split
        let pfx = key(100).0;
        let split_path = MapKeyPath { prefix: MapKey(pfx), len: 12 };
        let (t_a, t_b) = trie_repo.map_split(&t2, split_path);
        let (m_a, m_b) = multi_repo.map_split(&m2, split_path);
        assert_eq!(trie_repo.map_hash(&t_a).0, multi_repo.map_hash(&m_a).0, "split without");
        assert_eq!(trie_repo.map_hash(&t_b).0, multi_repo.map_hash(&m_b).0, "split branch");

        // Merge back
        let t3 = trie_repo.merge(&t_a, &t_b);
        let m3 = multi_repo.merge(&m_a, &m_b);
        assert_eq!(trie_repo.map_hash(&t3).0, multi_repo.map_hash(&m3).0, "after merge");
        assert_eq!(trie_repo.map_hash(&t2).0, multi_repo.map_hash(&m3).0, "merge roundtrip");
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
    fn write_read_cross_hash() {
        use trie_map::trie::smt::TrieMapRepository;

        let trie_repo = TrieMapRepository::<TV>::new();
        let multi_repo = Repo::new();

        let updates: Vec<_> = (0..200).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let t = trie_repo.map_update(&TrieMapRepository::<TV>::new_map(), &updates);
        let m = multi_repo.map_update(&Repo::new_map(), &updates);

        let mut buf = Vec::new();
        multi_repo.write_map(&m, &mut buf).unwrap();
        let m2 = multi_repo.read_map(&mut std::io::Cursor::new(&buf)).unwrap();

        assert_eq!(trie_repo.map_hash(&t).0, multi_repo.map_hash(&m2).0);
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
    #[ignore] // run explicitly: cargo test --release concurrent_throughput -- --nocapture --ignored
    fn concurrent_throughput() {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::AtomicU64;
        use std::sync::atomic::Ordering;
        use std::sync::Arc as StdArc;
        use std::thread;
        use std::time::Duration;
        use std::time::Instant;

        use parking_lot::RwLock;
        use trie_map::trie::smt::TrieMapRepository;

        const N_KEYS: usize = 50_000;
        const N_READERS: usize = 8;
        const N_WRITERS: usize = 8;
        const DURATION_SECS: u64 = 5;
        const WRITE_BATCH: usize = 50;

        let keys: Vec<MapKey> = (0..N_KEYS).map(key).collect();

        // ---------------------------------------------------------------
        // trie-map: readers and writers contend on internal arena lock
        // ---------------------------------------------------------------
        {
            let trie_repo = StdArc::new(TrieMapRepository::<TV>::new());

            let inserts: Vec<_> = (0..N_KEYS).map(|i| (keys[i], Some(TV(i as u64)))).collect();
            let mut map = TrieMapRepository::<TV>::new_map();
            for chunk in inserts.chunks(1000) {
                map = trie_repo.map_update(&map, chunk);
            }

            let current = StdArc::new(RwLock::new(map));
            let read_ops = StdArc::new(AtomicU64::new(0));
            let write_ops = StdArc::new(AtomicU64::new(0));
            let stop = StdArc::new(AtomicBool::new(false));

            let start = Instant::now();
            let mut handles = Vec::new();

            for _ in 0..N_READERS {
                let repo = StdArc::clone(&trie_repo);
                let cur = StdArc::clone(&current);
                let ops = StdArc::clone(&read_ops);
                let stop = StdArc::clone(&stop);
                let keys = keys.clone();
                handles.push(
                    thread::Builder::new()
                        .spawn(move || {
                            let mut count = 0u64;
                            let mut i = 0usize;
                            while !stop.load(Ordering::Relaxed) {
                                let map = *cur.read();
                                for _ in 0..100 {
                                    let _ = repo.map_get(&map, &keys[i % N_KEYS]);
                                    i += 1;
                                    count += 1;
                                }
                            }
                            ops.fetch_add(count, Ordering::Relaxed);
                        })
                        .unwrap(),
                );
            }

            for w in 0..N_WRITERS {
                let repo = StdArc::clone(&trie_repo);
                let cur = StdArc::clone(&current);
                let ops = StdArc::clone(&write_ops);
                let stop = StdArc::clone(&stop);
                let keys = keys.clone();
                handles.push(
                    thread::Builder::new()
                        .spawn(move || {
                            let mut count = 0u64;
                            let mut idx = w * 1000;
                            while !stop.load(Ordering::Relaxed) {
                                let map = *cur.read();
                                let batch: Vec<_> = (0..WRITE_BATCH)
                                    .map(|_| {
                                        idx = (idx + 1) % N_KEYS;
                                        (keys[idx], Some(TV((idx as u64).wrapping_add(count))))
                                    })
                                    .collect();
                                let new_map = repo.map_update(&map, &batch);
                                *cur.write() = new_map;
                                count += 1;
                            }
                            ops.fetch_add(count, Ordering::Relaxed);
                        })
                        .unwrap(),
                );
            }

            thread::sleep(Duration::from_secs(DURATION_SECS));
            stop.store(true, Ordering::Relaxed);

            for h in handles {
                h.join().unwrap();
            }

            let elapsed = start.elapsed();
            let reads = read_ops.load(Ordering::Relaxed);
            let writes = write_ops.load(Ordering::Relaxed);
            let read_rate = reads as f64 / elapsed.as_secs_f64();
            let write_rate = writes as f64 / elapsed.as_secs_f64();

            eprintln!("=== trie-map ({N_READERS}R + {N_WRITERS}W, {DURATION_SECS}s) ===");
            eprintln!("  reads:  {reads:>12} total, {read_rate:>12.0} ops/sec");
            eprintln!("  writes: {writes:>12} total, {write_rate:>12.0} batches/sec");
        }

        // ---------------------------------------------------------------
        // multi-map: readers clone root Arc then traverse lock-free
        // ---------------------------------------------------------------
        {
            let multi_repo = StdArc::new(Repo::new());

            let inserts: Vec<_> = (0..N_KEYS).map(|i| (keys[i], Some(TV(i as u64)))).collect();
            let mut map = Repo::new_map();
            for chunk in inserts.chunks(1000) {
                map = multi_repo.map_update(&map, chunk);
            }

            let current = StdArc::new(RwLock::new(map));
            let read_ops = StdArc::new(AtomicU64::new(0));
            let write_ops = StdArc::new(AtomicU64::new(0));
            let stop = StdArc::new(AtomicBool::new(false));

            let start = Instant::now();
            let mut handles = Vec::new();

            for _ in 0..N_READERS {
                let repo = StdArc::clone(&multi_repo);
                let cur = StdArc::clone(&current);
                let ops = StdArc::clone(&read_ops);
                let stop = StdArc::clone(&stop);
                let keys = keys.clone();
                handles.push(
                    thread::Builder::new()
                        .spawn(move || {
                            let mut count = 0u64;
                            let mut i = 0usize;
                            while !stop.load(Ordering::Relaxed) {
                                let map = cur.read().clone();
                                for _ in 0..100 {
                                    let _ = repo.map_get(&map, &keys[i % N_KEYS]);
                                    i += 1;
                                    count += 1;
                                }
                            }
                            ops.fetch_add(count, Ordering::Relaxed);
                        })
                        .unwrap(),
                );
            }

            for w in 0..N_WRITERS {
                let repo = StdArc::clone(&multi_repo);
                let cur = StdArc::clone(&current);
                let ops = StdArc::clone(&write_ops);
                let stop = StdArc::clone(&stop);
                let keys = keys.clone();
                handles.push(
                    thread::Builder::new()
                        .spawn(move || {
                            let mut count = 0u64;
                            let mut idx = w * 1000;
                            while !stop.load(Ordering::Relaxed) {
                                let map = cur.read().clone();
                                let batch: Vec<_> = (0..WRITE_BATCH)
                                    .map(|_| {
                                        idx = (idx + 1) % N_KEYS;
                                        (keys[idx], Some(TV((idx as u64).wrapping_add(count))))
                                    })
                                    .collect();
                                let new_map = repo.map_update(&map, &batch);
                                *cur.write() = new_map;
                                count += 1;
                            }
                            ops.fetch_add(count, Ordering::Relaxed);
                        })
                        .unwrap(),
                );
            }

            thread::sleep(Duration::from_secs(DURATION_SECS));
            stop.store(true, Ordering::Relaxed);

            for h in handles {
                h.join().unwrap();
            }

            let elapsed = start.elapsed();
            let reads = read_ops.load(Ordering::Relaxed);
            let writes = write_ops.load(Ordering::Relaxed);
            let read_rate = reads as f64 / elapsed.as_secs_f64();
            let write_rate = writes as f64 / elapsed.as_secs_f64();

            eprintln!("=== multi-map ({N_READERS}R + {N_WRITERS}W, {DURATION_SECS}s) ===");
            eprintln!("  reads:  {reads:>12} total, {read_rate:>12.0} ops/sec");
            eprintln!("  writes: {writes:>12} total, {write_rate:>12.0} batches/sec");
        }
    }

    #[test]
    #[ignore]
    fn block_cost_multimap_vs_triemap() {
        use std::time::Instant;

        use rand::Rng;
        use rand::SeedableRng;
        use trie_map::trie::smt::TrieMapRepository;
        use trie_map::MapRepository;

        const INITIAL_KEYS: usize = 20_000;
        const KEYS_PER_BLOCK: usize = 1000;
        const N_ROUNDS: usize = 50;

        eprintln!("\n=== MultiMap vs TrieMap: Block Update Cost ===");
        eprintln!(
            "  Initial map: {INITIAL_KEYS} keys, Block: {KEYS_PER_BLOCK} keys, Rounds: {N_ROUNDS}"
        );

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

        // --- MultiMap ---
        {
            let repo = Repo::new();
            let mut map = Repo::new_map();
            for chunk_start in (0..INITIAL_KEYS).step_by(1000) {
                let chunk_end = (chunk_start + 1000).min(INITIAL_KEYS);
                let updates: Vec<_> =
                    (chunk_start..chunk_end).map(|i| (key(i), Some(TV(i as u64)))).collect();
                map = repo.map_update(&map, &updates);
            }

            // Warm up
            for updates in &all_updates[..3] {
                let _ = repo.map_update(&map, updates);
            }

            let t = Instant::now();
            for updates in &all_updates {
                let _ = repo.map_update(&map, updates);
            }
            let elapsed = t.elapsed();
            let per_block = elapsed.as_micros() as f64 / N_ROUNDS as f64;
            eprintln!("\n  MultiMap:  {:.0} µs/block ({:.2} ms)", per_block, per_block / 1000.0);
        }

        // --- TrieMap ---
        {
            let repo = TrieMapRepository::<TV>::new();
            let mut map = TrieMapRepository::<TV>::new_map();
            for chunk_start in (0..INITIAL_KEYS).step_by(1000) {
                let chunk_end = (chunk_start + 1000).min(INITIAL_KEYS);
                let updates: Vec<_> =
                    (chunk_start..chunk_end).map(|i| (key(i), Some(TV(i as u64)))).collect();
                map = repo.map_update(&map, &updates);
            }

            // Warm up
            for updates in &all_updates[..3] {
                let _ = repo.map_update(&map, updates);
            }

            let t = Instant::now();
            for updates in &all_updates {
                let _ = repo.map_update(&map, updates);
            }
            let elapsed = t.elapsed();
            let per_block = elapsed.as_micros() as f64 / N_ROUNDS as f64;
            eprintln!("  TrieMap:   {:.0} µs/block ({:.2} ms)", per_block, per_block / 1000.0);
        }

        eprintln!();
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
                batch.sort_unstable_by(|a, b| a.0 .0.cmp(&b.0 .0));
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
                sorted.sort_unstable_by(|a, b| a.0 .0.cmp(&b.0 .0));
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

        let mut bt_maps: Vec<MultiMapRef<TV>> = Vec::with_capacity(N_BT);
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

        let results: Vec<(usize, usize, MultiMapRef<TV>, bool)> = bt_blocks
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

    #[test]
    #[ignore]
    fn concurrent_pipeline() {
        use std::collections::VecDeque;
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::AtomicU64;
        use std::sync::atomic::Ordering;
        use std::sync::Arc as StdArc;
        use std::thread;
        use std::time::Duration;
        use std::time::Instant;

        use parking_lot::Mutex;
        use trie_map::trie::smt::TrieMapRepository;

        const N_INITIAL_KEYS: usize = 50_000;
        const N_READERS: usize = 8;
        const N_WRITERS: usize = 8;
        const DURATION_SECS: u64 = 5;
        const KEYS_PER_BLOCK: usize = 100;
        const MAX_QUEUE_SIZE: usize = 64;

        let all_keys: Vec<MapKey> = (0..500_000).map(key).collect();

        eprintln!("\n--- Pipeline: {N_WRITERS} writers produce blocks of {KEYS_PER_BLOCK} keys, {N_READERS} readers consume and verify ---\n");

        // ---------------------------------------------------------------
        // trie-map
        // ---------------------------------------------------------------
        {
            type TRef = <TrieMapRepository<TV> as MapRepository>::MapRef;

            struct Block {
                map: TRef,
                added_keys: Vec<usize>,
            }

            let trie_repo = StdArc::new(TrieMapRepository::<TV>::new());

            let inserts: Vec<_> =
                (0..N_INITIAL_KEYS).map(|i| (all_keys[i], Some(TV(i as u64)))).collect();
            let mut map = TrieMapRepository::<TV>::new_map();
            for chunk in inserts.chunks(1000) {
                map = trie_repo.map_update(&map, chunk);
            }

            let queue: StdArc<Mutex<VecDeque<StdArc<Block>>>> =
                StdArc::new(Mutex::new(VecDeque::new()));
            queue
                .lock()
                .push_back(StdArc::new(Block { map, added_keys: (0..N_INITIAL_KEYS).collect() }));

            let key_cursor = StdArc::new(AtomicU64::new(N_INITIAL_KEYS as u64));
            let blocks_produced = StdArc::new(AtomicU64::new(0));
            let read_ops = StdArc::new(AtomicU64::new(0));
            let read_hits = StdArc::new(AtomicU64::new(0));
            let stop = StdArc::new(AtomicBool::new(false));

            let start = Instant::now();
            let mut handles = Vec::new();

            for _ in 0..N_WRITERS {
                let repo = StdArc::clone(&trie_repo);
                let queue = StdArc::clone(&queue);
                let cursor = StdArc::clone(&key_cursor);
                let produced = StdArc::clone(&blocks_produced);
                let stop = StdArc::clone(&stop);
                let keys = all_keys.clone();
                handles.push(
                    thread::Builder::new()
                        .spawn(move || {
                            while !stop.load(Ordering::Relaxed) {
                                let prev = {
                                    let q = queue.lock();
                                    match q.back() {
                                        Some(b) => b.map,
                                        None => continue,
                                    }
                                };

                                let base = cursor
                                    .fetch_add(KEYS_PER_BLOCK as u64, Ordering::Relaxed)
                                    as usize;
                                if base + KEYS_PER_BLOCK >= keys.len() {
                                    break;
                                }

                                let batch: Vec<_> = (0..KEYS_PER_BLOCK)
                                    .map(|j| (keys[base + j], Some(TV((base + j) as u64))))
                                    .collect();
                                let new_map = repo.map_update(&prev, &batch);

                                let block = StdArc::new(Block {
                                    map: new_map,
                                    added_keys: (base..base + KEYS_PER_BLOCK).collect(),
                                });

                                {
                                    let mut q = queue.lock();
                                    q.push_back(block);
                                    while q.len() > MAX_QUEUE_SIZE {
                                        q.pop_front();
                                    }
                                }
                                produced.fetch_add(1, Ordering::Relaxed);
                            }
                        })
                        .unwrap(),
                );
            }

            for _ in 0..N_READERS {
                let repo = StdArc::clone(&trie_repo);
                let queue = StdArc::clone(&queue);
                let ops = StdArc::clone(&read_ops);
                let hits = StdArc::clone(&read_hits);
                let stop = StdArc::clone(&stop);
                let keys = all_keys.clone();
                handles.push(
                    thread::Builder::new()
                        .spawn(move || {
                            let mut local_ops = 0u64;
                            let mut local_hits = 0u64;
                            let mut last_seen_len = 0usize;
                            while !stop.load(Ordering::Relaxed) {
                                let block = {
                                    let q = queue.lock();
                                    match q.back() {
                                        Some(b) => StdArc::clone(b),
                                        None => continue,
                                    }
                                };

                                for &ki in &block.added_keys {
                                    if ki < keys.len() {
                                        local_ops += 1;
                                        if repo.map_get(&block.map, &keys[ki]).is_some() {
                                            local_hits += 1;
                                        }
                                    }
                                }

                                for j in 0..100 {
                                    let idx = (last_seen_len + j) % N_INITIAL_KEYS;
                                    local_ops += 1;
                                    if repo.map_get(&block.map, &keys[idx]).is_some() {
                                        local_hits += 1;
                                    }
                                }
                                last_seen_len = last_seen_len.wrapping_add(100);
                            }
                            ops.fetch_add(local_ops, Ordering::Relaxed);
                            hits.fetch_add(local_hits, Ordering::Relaxed);
                        })
                        .unwrap(),
                );
            }

            thread::sleep(Duration::from_secs(DURATION_SECS));
            stop.store(true, Ordering::Relaxed);
            for h in handles {
                h.join().unwrap();
            }

            let elapsed = start.elapsed();
            let blocks = blocks_produced.load(Ordering::Relaxed);
            let reads = read_ops.load(Ordering::Relaxed);
            let hits_val = read_hits.load(Ordering::Relaxed);

            eprintln!("=== trie-map pipeline ({DURATION_SECS}s) ===");
            eprintln!(
                "  blocks produced:  {blocks:>12}  ({:>10.0}/sec)",
                blocks as f64 / elapsed.as_secs_f64()
            );
            eprintln!(
                "  read ops:         {reads:>12}  ({:>10.0}/sec)",
                reads as f64 / elapsed.as_secs_f64()
            );
            eprintln!(
                "  read hits:        {hits_val:>12}  ({:.1}% hit rate)",
                hits_val as f64 / reads.max(1) as f64 * 100.0
            );
        }

        // ---------------------------------------------------------------
        // multi-map
        // ---------------------------------------------------------------
        {
            struct Block {
                map: MultiMapRef<TV>,
                added_keys: Vec<usize>,
            }

            let multi_repo = StdArc::new(Repo::new());

            let inserts: Vec<_> =
                (0..N_INITIAL_KEYS).map(|i| (all_keys[i], Some(TV(i as u64)))).collect();
            let mut map = Repo::new_map();
            for chunk in inserts.chunks(1000) {
                map = multi_repo.map_update(&map, chunk);
            }

            let queue: StdArc<Mutex<VecDeque<StdArc<Block>>>> =
                StdArc::new(Mutex::new(VecDeque::new()));
            queue
                .lock()
                .push_back(StdArc::new(Block { map, added_keys: (0..N_INITIAL_KEYS).collect() }));

            let key_cursor = StdArc::new(AtomicU64::new(N_INITIAL_KEYS as u64));
            let blocks_produced = StdArc::new(AtomicU64::new(0));
            let read_ops = StdArc::new(AtomicU64::new(0));
            let read_hits = StdArc::new(AtomicU64::new(0));
            let stop = StdArc::new(AtomicBool::new(false));

            let start = Instant::now();
            let mut handles = Vec::new();

            for _ in 0..N_WRITERS {
                let repo = StdArc::clone(&multi_repo);
                let queue = StdArc::clone(&queue);
                let cursor = StdArc::clone(&key_cursor);
                let produced = StdArc::clone(&blocks_produced);
                let stop = StdArc::clone(&stop);
                let keys = all_keys.clone();
                handles.push(
                    thread::Builder::new()
                        .spawn(move || {
                            while !stop.load(Ordering::Relaxed) {
                                let prev = {
                                    let q = queue.lock();
                                    match q.back() {
                                        Some(b) => b.map.clone(),
                                        None => continue,
                                    }
                                };

                                let base = cursor
                                    .fetch_add(KEYS_PER_BLOCK as u64, Ordering::Relaxed)
                                    as usize;
                                if base + KEYS_PER_BLOCK >= keys.len() {
                                    break;
                                }

                                let batch: Vec<_> = (0..KEYS_PER_BLOCK)
                                    .map(|j| (keys[base + j], Some(TV((base + j) as u64))))
                                    .collect();
                                let new_map = repo.map_update(&prev, &batch);

                                let block = StdArc::new(Block {
                                    map: new_map,
                                    added_keys: (base..base + KEYS_PER_BLOCK).collect(),
                                });

                                {
                                    let mut q = queue.lock();
                                    q.push_back(block);
                                    while q.len() > MAX_QUEUE_SIZE {
                                        q.pop_front();
                                    }
                                }
                                produced.fetch_add(1, Ordering::Relaxed);
                            }
                        })
                        .unwrap(),
                );
            }

            for _ in 0..N_READERS {
                let repo = StdArc::clone(&multi_repo);
                let queue = StdArc::clone(&queue);
                let ops = StdArc::clone(&read_ops);
                let hits = StdArc::clone(&read_hits);
                let stop = StdArc::clone(&stop);
                let keys = all_keys.clone();
                handles.push(
                    thread::Builder::new()
                        .spawn(move || {
                            let mut local_ops = 0u64;
                            let mut local_hits = 0u64;
                            let mut last_seen_len = 0usize;
                            while !stop.load(Ordering::Relaxed) {
                                let block = {
                                    let q = queue.lock();
                                    match q.back() {
                                        Some(b) => StdArc::clone(b),
                                        None => continue,
                                    }
                                };

                                for &ki in &block.added_keys {
                                    if ki < keys.len() {
                                        local_ops += 1;
                                        if repo.map_get(&block.map, &keys[ki]).is_some() {
                                            local_hits += 1;
                                        }
                                    }
                                }

                                for j in 0..100 {
                                    let idx = (last_seen_len + j) % N_INITIAL_KEYS;
                                    local_ops += 1;
                                    if repo.map_get(&block.map, &keys[idx]).is_some() {
                                        local_hits += 1;
                                    }
                                }
                                last_seen_len = last_seen_len.wrapping_add(100);
                            }
                            ops.fetch_add(local_ops, Ordering::Relaxed);
                            hits.fetch_add(local_hits, Ordering::Relaxed);
                        })
                        .unwrap(),
                );
            }

            thread::sleep(Duration::from_secs(DURATION_SECS));
            stop.store(true, Ordering::Relaxed);
            for h in handles {
                h.join().unwrap();
            }

            let elapsed = start.elapsed();
            let blocks = blocks_produced.load(Ordering::Relaxed);
            let reads = read_ops.load(Ordering::Relaxed);
            let hits_val = read_hits.load(Ordering::Relaxed);

            eprintln!("=== multi-map pipeline ({DURATION_SECS}s) ===");
            eprintln!(
                "  blocks produced:  {blocks:>12}  ({:>10.0}/sec)",
                blocks as f64 / elapsed.as_secs_f64()
            );
            eprintln!(
                "  read ops:         {reads:>12}  ({:>10.0}/sec)",
                reads as f64 / elapsed.as_secs_f64()
            );
            eprintln!(
                "  read hits:        {hits_val:>12}  ({:.1}% hit rate)",
                hits_val as f64 / reads.max(1) as f64 * 100.0
            );
        }
    }
}
