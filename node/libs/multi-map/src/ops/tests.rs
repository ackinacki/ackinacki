use blake3::Hasher;
use node_types::Blake3Hashable;
use serde::Deserialize;
use serde::Serialize;
use trie_map::trie::arena::nibble_at;
use trie_map::MapKey;
use trie_map::MapKeyPath;

use crate::node::Node;
use crate::ops::get;
use crate::ops::merge;
use crate::ops::split;
use crate::ops::update;

// ====================== test helpers ======================

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TV(pub u64);

impl Blake3Hashable for TV {
    fn update_hasher(&self, hasher: &mut Hasher) {
        hasher.update(&self.0.to_be_bytes());
    }
}

fn default_path() -> MapKeyPath {
    MapKeyPath::default()
}

fn mk_key(bytes: &[u8]) -> MapKey {
    let mut k = [0u8; 32];
    k[..bytes.len()].copy_from_slice(bytes);
    MapKey(k)
}

fn key(i: usize) -> MapKey {
    let hash = blake3::hash(format!("key:{i}").as_bytes());
    MapKey(*hash.as_bytes())
}

fn value(i: u64) -> TV {
    TV(i)
}

fn root_hash<V: crate::MultiMapValue>(node: &Node<V>) -> [u8; 32] {
    node.hash()
}

fn prefix_bits_match_local(prefix: &[u8; 32], k: &[u8; 32], bits: u8) -> bool {
    trie_map::trie::arena::prefix_bits_match(prefix, k, bits)
}

// ====================== get tests ======================

#[test]
fn get_empty_returns_none() {
    let root: Node<TV> = Node::Empty;
    assert_eq!(get(&root, &default_path(), &mk_key(&[0x12])), None);
}

#[test]
fn get_single_leaf() {
    let key = mk_key(&[0xAB, 0xCD]);
    let leaf = Node::new_leaf(TV(42));

    let nibbles: Vec<u8> = (0..64).map(|i| nibble_at(&key.0, i)).collect();
    let root: Node<TV> = Node::new_ext(&nibbles, leaf);

    assert_eq!(get(&root, &default_path(), &key), Some(TV(42)));
    assert_eq!(get(&root, &default_path(), &mk_key(&[0x00])), None);
}

#[test]
fn get_branch_two_keys() {
    let key_a = mk_key(&[0x10]);
    let key_b = mk_key(&[0x20]);

    let leaf_a = Node::new_leaf(TV(1));
    let leaf_b = Node::new_leaf(TV(2));

    let nibs_a: Vec<u8> = (1..64).map(|i| nibble_at(&key_a.0, i)).collect();
    let nibs_b: Vec<u8> = (1..64).map(|i| nibble_at(&key_b.0, i)).collect();

    let ext_a = Node::new_ext(&nibs_a, leaf_a);
    let ext_b = Node::new_ext(&nibs_b, leaf_b);

    let bitmap = (1u16 << 1) | (1u16 << 2);
    let mut children: [Node<TV>; 16] = Default::default();
    children[1] = ext_a;
    children[2] = ext_b;
    let root: Node<TV> = Node::new_branch(bitmap, children);

    assert_eq!(get(&root, &default_path(), &key_a), Some(TV(1)));
    assert_eq!(get(&root, &default_path(), &key_b), Some(TV(2)));
    assert_eq!(get(&root, &default_path(), &mk_key(&[0x30])), None);
}

#[test]
fn get_with_root_path_prefix_filter() {
    let key_in = mk_key(&[0xAB, 0x12]);
    let key_out = mk_key(&[0xCD, 0x12]);

    let leaf = Node::new_leaf(TV(7));
    let nibs: Vec<u8> = (2..64).map(|i| nibble_at(&key_in.0, i)).collect();
    let root: Node<TV> = Node::new_ext(&nibs, leaf);

    let path = MapKeyPath { prefix: MapKey([0xAB; 32]), len: 8 };

    assert_eq!(get(&root, &path, &key_in), Some(TV(7)));
    assert_eq!(get(&root, &path, &key_out), None);
}

// ====================== update tests ======================

#[test]
fn insert_single_key() {
    let root: Node<TV> = Node::Empty;
    let key = mk_key(&[0xAB]);
    let new_root = update(&root, &default_path(), &[(key, Some(TV(1)))]);
    assert!(!new_root.is_empty());
    assert_eq!(get(&new_root, &default_path(), &key), Some(TV(1)));
}

#[test]
fn insert_and_delete() {
    let root: Node<TV> = Node::Empty;
    let k1 = mk_key(&[0x10]);
    let k2 = mk_key(&[0x20]);

    let r1 = update(&root, &default_path(), &[(k1, Some(TV(1))), (k2, Some(TV(2)))]);
    assert_eq!(get(&r1, &default_path(), &k1), Some(TV(1)));
    assert_eq!(get(&r1, &default_path(), &k2), Some(TV(2)));

    let r2 = update(&r1, &default_path(), &[(k1, None)]);
    assert_eq!(get(&r2, &default_path(), &k1), None);
    assert_eq!(get(&r2, &default_path(), &k2), Some(TV(2)));
}

#[test]
fn last_write_wins() {
    let root: Node<TV> = Node::Empty;
    let key = mk_key(&[0xAA]);
    let new_root = update(
        &root,
        &default_path(),
        &[(key, Some(TV(1))), (key, Some(TV(2))), (key, Some(TV(3)))],
    );
    assert_eq!(get(&new_root, &default_path(), &key), Some(TV(3)));
}

#[test]
fn delete_nonexistent_key() {
    let root: Node<TV> = Node::Empty;
    let key = mk_key(&[0x42]);
    let new_root = update(&root, &default_path(), &[(key, None)]);
    assert!(new_root.is_empty());
}

#[test]
fn insert_many_keys_and_read_back() {
    let root: Node<TV> = Node::Empty;
    let mut updates: Vec<(MapKey, Option<TV>)> = Vec::new();
    for i in 0..256u64 {
        let key = mk_key(&[i as u8]);
        updates.push((key, Some(TV(i))));
    }
    let new_root = update(&root, &default_path(), &updates);
    for i in 0..256u64 {
        let key = mk_key(&[i as u8]);
        assert_eq!(get(&new_root, &default_path(), &key), Some(TV(i)));
    }
}

#[test]
fn cross_hash_matches_trie_map() {
    use trie_map::trie::arena::Arena;

    let keys_and_vals: Vec<(MapKey, TV)> = (0..50u64)
        .map(|i| {
            let mut k = [0u8; 32];
            k[0] = (i * 7) as u8;
            k[1] = (i * 13) as u8;
            (MapKey(k), TV(i * 100))
        })
        .collect();

    let root_path = default_path();
    let updates: Vec<(MapKey, Option<TV>)> =
        keys_and_vals.iter().map(|(k, v)| (*k, Some(*v))).collect();

    let mut arena = Arena::<TV>::new();
    let (tm_root, _) = arena.update(0, root_path, &updates);
    let tm_hash = arena.nodes[tm_root as usize].hash;

    let mm_root: Node<TV> = Node::Empty;
    let mm_root = update(&mm_root, &root_path, &updates);
    let mm_hash = mm_root.hash();

    assert_eq!(tm_hash, mm_hash, "trie-map and multi-map root hashes must match");

    for (k, v) in &keys_and_vals {
        assert_eq!(get(&mm_root, &root_path, k), Some(*v));
    }
}

#[test]
fn cross_hash_with_deletes() {
    use trie_map::trie::arena::Arena;

    let root_path = default_path();

    let all_keys: Vec<(MapKey, TV)> = (0..30u64)
        .map(|i| {
            let mut k = [0u8; 32];
            k[0] = (i * 11) as u8;
            k[1] = (i * 3) as u8;
            (MapKey(k), TV(i))
        })
        .collect();

    let inserts: Vec<(MapKey, Option<TV>)> = all_keys.iter().map(|(k, v)| (*k, Some(*v))).collect();
    let deletes: Vec<(MapKey, Option<TV>)> =
        all_keys[0..10].iter().map(|(k, _)| (*k, None)).collect();

    let mut arena = Arena::<TV>::new();
    let (tm_root, _) = arena.update(0, root_path, &inserts);
    let (tm_root, _) = arena.update(tm_root, root_path, &deletes);
    let tm_hash = arena.nodes[tm_root as usize].hash;

    let mm_root: Node<TV> = Node::Empty;
    let mm_root = update(&mm_root, &root_path, &inserts);
    let mm_root = update(&mm_root, &root_path, &deletes);
    let mm_hash = mm_root.hash();

    assert_eq!(tm_hash, mm_hash, "hashes must match after deletes");
}

#[test]
fn incremental_updates_match_trie_map() {
    use trie_map::trie::arena::Arena;

    let root_path = default_path();

    let mut arena = Arena::<TV>::new();
    let mut tm_root: u32 = 0;
    let mut mm_root: Node<TV> = Node::Empty;

    for batch in 0..5u64 {
        let updates: Vec<(MapKey, Option<TV>)> = (0..20u64)
            .map(|i| {
                let idx = batch * 20 + i;
                let mut k = [0u8; 32];
                k[0] = (idx * 7 % 256) as u8;
                k[1] = (idx * 13 % 256) as u8;
                k[2] = (idx * 17 % 256) as u8;
                (MapKey(k), Some(TV(idx)))
            })
            .collect();

        let (new_tm, _) = arena.update(tm_root, root_path, &updates);
        tm_root = new_tm;
        mm_root = update(&mm_root, &root_path, &updates);

        let tm_hash = arena.nodes[tm_root as usize].hash;
        let mm_hash = mm_root.hash();
        assert_eq!(tm_hash, mm_hash, "batch {} hashes must match", batch);
    }
}

// ====================== split / merge tests ======================

#[test]
fn split_merge_roundtrip_nibble_aligned() {
    let path = default_path();
    let n = 4000usize;
    let keys: Vec<MapKey> = (0..n).map(key).collect();
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i as u64)))).collect();
    let original = update(&Node::Empty, &path, &updates);

    let len_bits: u8 = 12;
    let pfx = keys[123].0;
    let split_path = MapKeyPath { prefix: MapKey(pfx), len: len_bits };

    let (without, branch) = split(&original, path, split_path);
    let merged = merge(&without, path, &branch, split_path);

    assert!(get(&branch, &split_path, &keys[123]).is_some());
    assert!(get(&without, &path, &keys[123]).is_none());

    for (i, k) in keys.iter().enumerate() {
        let in_pfx = prefix_bits_match_local(&pfx, &k.0, len_bits);
        assert_eq!(get(&branch, &split_path, k).is_some(), in_pfx, "i={i}");
        assert_eq!(get(&without, &path, k).is_some(), !in_pfx, "i={i}");
        assert_eq!(get(&merged, &path, k), get(&original, &path, k), "i={i}");
    }

    assert_eq!(root_hash(&merged), root_hash(&original));
}

#[test]
fn split_merge_roundtrip_unaligned() {
    let path = default_path();
    let n = 8000usize;
    let keys: Vec<MapKey> = (0..n).map(key).collect();
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i as u64)))).collect();
    let original = update(&Node::Empty, &path, &updates);

    let len_bits: u8 = 5;
    let pfx = keys[777].0;
    let split_path = MapKeyPath { prefix: MapKey(pfx), len: len_bits };

    let (without, branch) = split(&original, path, split_path);
    let merged = merge(&without, path, &branch, split_path);

    for (i, k) in keys.iter().enumerate() {
        let in_pfx = prefix_bits_match_local(&pfx, &k.0, len_bits);
        assert_eq!(get(&branch, &split_path, k).is_some(), in_pfx, "i={i}");
        assert_eq!(get(&without, &path, k).is_some(), !in_pfx, "i={i}");
        assert_eq!(get(&merged, &path, k), get(&original, &path, k), "i={i}");
    }

    assert_eq!(root_hash(&merged), root_hash(&original));
}

#[test]
fn split_merge_stress_many_prefix_lengths() {
    let path = default_path();
    let n = 20_000usize;
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i as u64)))).collect();
    let original = update(&Node::Empty, &path, &updates);
    let orig_hash = root_hash(&original);

    let lens: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 15, 16, 31, 63, 64];
    for (t, &len_bits) in lens.iter().enumerate() {
        let pfx = key((t * 7919) % n).0;
        let split_path = MapKeyPath { prefix: MapKey(pfx), len: len_bits };

        let (a, b) = split(&original, path, split_path);
        let merged = merge(&a, path, &b, split_path);

        assert_eq!(root_hash(&merged), orig_hash, "len_bits={len_bits}");
    }
}

#[test]
fn merge_with_empty_subtree() {
    let path = default_path();
    let updates: Vec<_> = (0..2000).map(|i| (key(i), Some(value(i as u64)))).collect();
    let original = update(&Node::Empty, &path, &updates);

    let empty: Node<TV> = Node::Empty;
    let empty_path = MapKeyPath::default();
    let merged = merge(&original, path, &empty, empty_path);
    assert!(merged.is_empty());
}

#[test]
fn merge_empty_with_full_restores() {
    let path = default_path();
    let updates: Vec<_> = (0..2000).map(|i| (key(i), Some(value(i as u64)))).collect();
    let original = update(&Node::Empty, &path, &updates);
    let orig_hash = root_hash(&original);

    let empty: Node<TV> = Node::Empty;
    let merged = merge(&empty, path, &original, path);

    assert_eq!(root_hash(&merged), orig_hash);
}

#[test]
fn cross_hash_split_merge_matches_trie_map() {
    use trie_map::trie::smt::TrieMapRepository;
    use trie_map::MapRepository;

    let path = default_path();
    let n = 500usize;
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i as u64)))).collect();

    let trie_repo = TrieMapRepository::<TV>::new();
    let t = trie_repo.map_update(&TrieMapRepository::<TV>::new_map(), &updates);
    let m = update(&Node::Empty, &path, &updates);

    let pfx = key(50).0;
    let split_path = MapKeyPath { prefix: MapKey(pfx), len: 8 };

    let (t_without, t_branch) = trie_repo.map_split(&t, split_path);
    let (m_without, m_branch) = split(&m, path, split_path);

    assert_eq!(trie_repo.map_hash(&t_without).0, root_hash(&m_without), "without hashes");
    assert_eq!(trie_repo.map_hash(&t_branch).0, root_hash(&m_branch), "branch hashes");

    let t_merged = trie_repo.merge(&t_without, &t_branch);
    let m_merged = merge(&m_without, path, &m_branch, split_path);

    assert_eq!(trie_repo.map_hash(&t_merged).0, root_hash(&m_merged), "merged hashes");
    assert_eq!(trie_repo.map_hash(&t).0, root_hash(&m_merged), "merged matches original");
}
