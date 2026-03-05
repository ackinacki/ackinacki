use std::time::Instant;

use blake3::Hasher;
use node_types::Blake3Hashable;
use serde::Deserialize;
use serde::Serialize;

use crate::trie::smt::TrieMapRepository;
use crate::MapKey;
use crate::MapKeyPath;
use crate::MapRepository;

fn dbg_enabled() -> bool {
    std::env::var_os("SMT_DEBUG").is_some()
}

fn key(i: usize) -> MapKey {
    let mut h = Hasher::new();
    h.update(b"key:");
    h.update(&(i as u64).to_le_bytes());
    MapKey(*h.finalize().as_bytes())
}

fn value(i: usize) -> TestValue {
    TestValue(i)
}

fn prefix_bits_match(prefix: &[u8; 32], k: &[u8; 32], bits: u8) -> bool {
    let bits = bits as usize;
    let full = bits / 8;
    let rem = bits % 8;

    if full > 0 && prefix[..full] != k[..full] {
        return false;
    }
    if rem == 0 {
        return true;
    }
    let mask = 0xFFu8 << (8 - rem);
    (prefix[full] & mask) == (k[full] & mask)
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
struct TestValue(pub usize);

impl std::fmt::Display for TestValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

type MapRepo = TrieMapRepository<TestValue>;

impl Blake3Hashable for TestValue {
    fn update_hasher(&self, hasher: &mut Hasher) {
        // stable, endian-defined encoding
        hasher.update(&(self.0 as u64).to_be_bytes());
    }
}

#[test]
fn empty_get_is_none() {
    let r = MapRepo::new();
    let m = MapRepo::new_map();
    assert_eq!(r.map_get(&m, &key(1)), None);
    assert_eq!(r.map_hash(&m).0.len(), 32);
}

#[test]
fn insert_and_get() {
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();
    let k = key(42);
    let v = value(7);

    let m1 = repo.map_update(&m0, &[(k, Some(v))]);
    repo.print_tree(&m1);
    assert_eq!(repo.map_get(&m1, &k), Some(v));
    assert_eq!(repo.map_get(&m0, &k), None, "persistence: old version must not change");
}

#[test]
fn update_existing_key() {
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();
    let k = key(10);

    let m1 = repo.map_update(&m0, &[(k, Some(value(1)))]);
    let m2 = repo.map_update(&m1, &[(k, Some(value(2)))]);

    assert_eq!(repo.map_get(&m1, &k), Some(value(1)));
    assert_eq!(repo.map_get(&m2, &k), Some(value(2)));
}

#[test]
fn delete_key() {
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();
    let k = key(99);

    let m1 = repo.map_update(&m0, &[(k, Some(value(1)))]);
    let m2 = repo.map_update(&m1, &[(k, None)]);

    assert_eq!(repo.map_get(&m1, &k), Some(value(1)));
    assert_eq!(repo.map_get(&m2, &k), None);
}

#[test]
fn put_with_branch_two_keys_custom_prefix() {
    // crafted keys to ensure they diverge early and force a BRANCH at some point
    fn mk(prefix: &[u8]) -> MapKey {
        let mut key = [0u8; 32];
        key[..prefix.len()].copy_from_slice(prefix);
        MapKey(key)
    }

    let k1 = mk(&[0x12, 0x12]);
    let k2 = mk(&[0x34, 0x13]);

    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();

    let m1 = repo.map_update(&m0, &[(k1, Some(value(1))), (k2, Some(value(2)))]);
    if dbg_enabled() {
        repo.print_tree(&m1);
    }

    assert_eq!(repo.map_get(&m1, &k1), Some(value(1)));
    assert_eq!(repo.map_get(&m1, &k2), Some(value(2)));
}

#[test]
fn ext_mismatch_children_order_regression_new_nib_lt_old_nib() {
    // This catches the classic bug: in EXT mismatch split, children slice must be packed in nibble order.
    // We craft two keys such that at first mismatch nibble, new_nib < old_nib.
    fn mk(bytes: &[u8; 2]) -> MapKey {
        let mut key = [0u8; 32];
        key[0] = bytes[0];
        key[1] = bytes[1];
        MapKey(key)
    }

    // Key A starts with nibble 'f' at depth 0 (0xF0)
    let ka = mk(&[0xF0, 0x00]);
    // Key B starts with nibble '1' at depth 0 (0x10) => new_nib=1 < old_nib=15 in some insert orders
    let kb = mk(&[0x10, 0x00]);

    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();

    // insert A then B
    let m1 = repo.map_update(&m0, &[(ka, Some(value(11)))]);
    let m2 = repo.map_update(&m1, &[(kb, Some(value(22)))]);
    assert_eq!(repo.map_get(&m2, &ka), Some(value(11)));
    assert_eq!(repo.map_get(&m2, &kb), Some(value(22)));

    // now reverse order in a fresh map
    let n1 = repo.map_update(&m0, &[(kb, Some(value(22)))]);
    let n2 = repo.map_update(&n1, &[(ka, Some(value(11)))]);
    assert_eq!(repo.map_get(&n2, &ka), Some(value(11)));
    assert_eq!(repo.map_get(&n2, &kb), Some(value(22)));
}

#[test]
fn batch_get_works() {
    let count = 100usize;
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();
    let keys: Vec<MapKey> = (0..count).map(key).collect();

    let mut updates = Vec::new();
    for (i, key) in keys.iter().enumerate().take(count) {
        if i % 3 == 0 {
            updates.push((*key, Some(value(i))));
        }
    }
    let m1 = repo.map_update(&m0, &updates);
    repo.print_tree(&m1);
    println!("{:?}", repo.get_stat([m1.root]));

    let got = repo.map_batch_get(&m1, &keys);
    for (i, g) in got.into_iter().enumerate() {
        if i % 3 == 0 {
            assert_eq!(g, Some(value(i)));
        } else {
            assert_eq!(g, None);
        }
    }
}

#[test]
fn root_hash_changes_on_updates_and_is_stable_for_same_version() {
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();
    let h0 = repo.map_hash(&m0);

    let m1 = repo.map_update(&m0, &[(key(1), Some(value(1)))]);
    let h1 = repo.map_hash(&m1);

    assert_ne!(h0, h1);
    assert_eq!(repo.map_hash(&m1), h1);
}

#[test]
fn delete_causes_compression_but_values_still_correct() {
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();

    // Put a bunch, then delete most of them; Patricia should compress single-child chains.
    let n = 2000usize;
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i)))).collect();
    let m1 = repo.map_update(&m0, &updates);

    let mut deletions = Vec::new();
    for i in 0..n {
        if i % 10 != 0 {
            deletions.push((key(i), None));
        }
    }
    let m2 = repo.map_update(&m1, &deletions);

    for i in 0..n {
        let got = repo.map_get(&m2, &key(i));
        if i % 10 == 0 {
            assert_eq!(got, Some(value(i)));
        } else {
            assert_eq!(got, None);
        }
    }
}

#[test]
fn split_merge_roundtrip_nibble_aligned_bits() {
    let len_bits: u8 = 12;
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();

    let n = 4000;
    let keys: Vec<MapKey> = (0..n).map(key).collect();
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i)))).collect();
    let start = Instant::now();
    let original = repo.map_update(&m0, &updates);
    if dbg_enabled() {
        eprintln!("patch took: {} ms", start.elapsed().as_millis());
    }

    let pfx = keys[123].0;
    let path = MapKeyPath { prefix: MapKey(pfx), len: len_bits };

    let (without_branch, branch) = repo.map_split(&original, path);
    let merged = repo.merge(&without_branch, &branch);

    // Pivot key must be in a branch and not in without_branch
    let pivot = keys[123];
    assert!(prefix_bits_match(&pfx, &pivot.0, len_bits));
    assert!(repo.map_get(&branch, &pivot).is_some());
    assert!(repo.map_get(&without_branch, &pivot).is_none());

    for (i, k) in keys.iter().enumerate() {
        let orig = repo.map_get(&original, k);
        let in_pfx = prefix_bits_match(&pfx, &k.0, len_bits);
        let present_in_without_branch = repo.map_get(&without_branch, k).is_some();
        let present_in_branch = repo.map_get(&branch, k).is_some();

        assert_eq!(present_in_without_branch, !in_pfx, "i={i}");
        assert_eq!(present_in_branch, in_pfx, "i={i}");
        assert_eq!(repo.map_get(&merged, k), orig, "i={i}");
    }

    // Hash roundtrip should hold in canonical form
    assert_eq!(repo.map_hash(&merged), repo.map_hash(&original));
}

#[test]
fn split_merge_roundtrip_unaligned_bits() {
    let len_bits: u8 = 5;
    let repo = MapRepo::new();

    let m0 = MapRepo::new_map();
    let n = 8000;
    let keys: Vec<MapKey> = (0..n).map(key).collect();
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i)))).collect();
    let m = repo.map_update(&m0, &updates);

    let pfx = keys[777].0;
    let path = MapKeyPath { prefix: MapKey(pfx), len: len_bits };

    let (without_branch, branch) = repo.map_split(&m, path);
    let merged = repo.merge(&without_branch, &branch);

    // Pivot key must be in a branch and not in without_branch
    let pivot = keys[777];
    assert!(prefix_bits_match(&pfx, &pivot.0, len_bits));
    assert!(repo.map_get(&branch, &pivot).is_some());
    assert!(repo.map_get(&without_branch, &pivot).is_none());

    for (i, k) in keys.iter().enumerate() {
        let orig = repo.map_get(&m, k);
        let in_pfx = prefix_bits_match(&pfx, &k.0, len_bits);

        assert_eq!(repo.map_get(&without_branch, k).is_some(), !in_pfx, "i={i}");
        assert_eq!(repo.map_get(&branch, k).is_some(), in_pfx, "i={i}");
        assert_eq!(repo.map_get(&merged, k), orig, "i={i}");
    }

    assert_eq!(repo.map_hash(&merged), repo.map_hash(&m));
}

#[test]
fn split_merge_many_prefixes_stress() {
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();

    let n = 20_000usize;
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i)))).collect();
    let m = repo.map_update(&m0, &updates);

    let lens: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 15, 16, 31, 63, 64];
    for (t, &len_bits) in lens.iter().enumerate() {
        let pfx = key((t * 7919) % n).0;
        let path = MapKeyPath { prefix: MapKey(pfx), len: len_bits };

        let (a, b) = repo.map_split(&m, path);
        let merged = repo.merge(&a, &b);

        assert_eq!(repo.map_hash(&merged), repo.map_hash(&m), "len_bits={len_bits}");
    }
}

#[test]
fn merge_with_global_empty_clears_all() {
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();

    let updates: Vec<_> = (0..2000).map(|i| (key(i), Some(value(i)))).collect();
    let a = repo.map_update(&m0, &updates);

    let empty_global = MapRepo::new_map(); // root_path len=0
    let merged = repo.merge(&a, &empty_global);

    // should be empty
    for i in 0..2000 {
        assert_eq!(repo.map_get(&merged, &key(i)), None);
    }
    assert_eq!(repo.map_hash(&merged), repo.map_hash(&empty_global));
}

#[test]
fn merge_empty_with_a_restores_a_for_global_root_path() {
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();

    let updates: Vec<_> = (0..2000).map(|i| (key(i), Some(value(i)))).collect();
    let a = repo.map_update(&m0, &updates);

    assert_eq!(repo.map_key_path(&a).len, 0);

    let empty = MapRepo::new_map();
    let merged = repo.merge(&empty, &a);

    assert_eq!(repo.map_hash(&merged), repo.map_hash(&a));
}

#[test]
fn patch_respects_root_key_path_view_filter() {
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();
    let updates: Vec<_> = (0..200).map(|i| (key(i), Some(value(i)))).collect();
    let m = repo.map_update(&m0, &updates);

    let pfx = key(10).0;
    let path = MapKeyPath { prefix: MapKey(pfx), len: 9 };

    let (_without_branch, branch) = repo.map_split(&m, path);

    let inside_key = (0..200).map(key).find(|k| prefix_bits_match(&pfx, &k.0, 9)).unwrap();
    let outside_key = (0..200).map(key).find(|k| !prefix_bits_match(&pfx, &k.0, 9)).unwrap();

    let a2 = repo.map_update(
        &branch,
        &[
            (inside_key, Some(value(999))),
            (outside_key, Some(value(888))), // should be ignored
        ],
    );

    assert_eq!(repo.map_get(&a2, &inside_key), Some(value(999)));
    assert_eq!(repo.map_get(&a2, &outside_key), None);
}

#[test]
fn deterministic_fuzz_small() {
    // tiny deterministic fuzz: apply random ops, then verify against BTreeMap model
    use std::collections::BTreeMap;

    let repo = MapRepo::new();
    let mut map_ref = MapRepo::new_map();
    let mut model = BTreeMap::<MapKey, TestValue>::new();

    let mut rng = 0x1234_5678_9abc_def0u64;
    let mut next_u64 = || {
        // xor shift 64*
        rng ^= rng >> 12;
        rng ^= rng << 25;
        rng ^= rng >> 27;
        rng = rng.wrapping_mul(2685821657736338717);
        rng
    };

    for step in 0..2000usize {
        let r = next_u64();
        let k = key((r as usize) % 500);
        let op = (r >> 60) & 3;

        match op {
            0 | 1 => {
                // put
                let v = value(((r >> 8) as usize) % 10_000);
                map_ref = repo.map_update(&map_ref, &[(k, Some(v))]);
                model.insert(k, v);
            }
            _ => {
                // delete
                map_ref = repo.map_update(&map_ref, &[(k, None)]);
                model.remove(&k);
            }
        }

        if step % 200 == 0 {
            // verify some random keys
            for _ in 0..50 {
                let kk = key((next_u64() as usize) % 500);
                let got = repo.map_get(&map_ref, &kk);
                let exp = model.get(&kk).copied();
                assert_eq!(got, exp, "step={step}");
            }
        }
    }

    // final full verification on all 500 keys
    for i in 0..500usize {
        let kk = key(i);
        assert_eq!(repo.map_get(&map_ref, &kk), model.get(&kk).copied(), "final i={i}");
    }
}

#[test]
fn root_hash_order_independence_fuzzer() {
    // ---------- tiny deterministic RNG + shuffle (no rand crate) ----------
    #[derive(Clone)]
    struct XorShift64 {
        s: u64,
    }
    impl XorShift64 {
        fn new(seed: u64) -> Self {
            Self { s: seed.max(1) }
        }

        fn next_u64(&mut self) -> u64 {
            let mut x = self.s;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.s = x;
            x
        }

        fn gen_usize(&mut self, n: usize) -> usize {
            debug_assert!(n > 0);
            (self.next_u64() as usize) % n
        }
    }

    fn shuffle<T>(xs: &mut [T], rng: &mut XorShift64) {
        for i in (1..xs.len()).rev() {
            let j = rng.gen_usize(i + 1);
            xs.swap(i, j);
        }
    }

    // ---------- keys crafted to share long prefix (Patricia-heavy) ----------
    fn key_with_shared_prefix(i: u32) -> MapKey {
        let mut b = [0u8; 32];
        // 30 bytes identical => EXT chains are very likely
        b[0..30].copy_from_slice(&[
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x10, 0x32, 0x54, 0x76, 0x98, 0xba,
            0xdc, 0xfe, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55,
            0x66, 0x77,
        ]);
        // the last 2 bytes vary
        b[30] = (i >> 8) as u8;
        b[31] = (i & 0xFF) as u8;
        MapKey(b)
    }

    // ---------- setup ----------
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();

    // Build a base map with many keys (shared prefix).
    // This makes the trie non-trivial before we apply the permuted batch.
    let base_n = 2000u32;
    let mut base_updates = Vec::with_capacity(base_n as usize);
    for i in 0..base_n {
        base_updates.push((key_with_shared_prefix(i), Some(value(i as usize))));
    }
    let base_map = repo.map_update(&m0, &base_updates);
    let base_hash = repo.map_hash(&base_map);

    // Prepare a batch with mixed operations:
    // - update some existing keys
    // - delete some existing keys
    // - insert new keys (different tail range)
    let mut ops: Vec<(MapKey, Option<TestValue>)> = Vec::new();

    // Update 600 existing keys
    for i in 200..800u32 {
        ops.push((key_with_shared_prefix(i), Some(value((10_000 + i) as usize))));
    }
    // Delete 400 existing keys
    for i in 900..1300u32 {
        ops.push((key_with_shared_prefix(i), None));
    }
    // Insert 600 new keys (outside base range)
    for i in 5000..5600u32 {
        ops.push((key_with_shared_prefix(i), Some(value((20_000 + i) as usize))));
    }

    // Compute a reference result with a fixed order
    let ref_map = repo.map_update(&base_map, &ops);
    let ref_hash = repo.map_hash(&ref_map);

    // Sanity: hash should change vs. base
    assert_ne!(ref_hash, base_hash);

    // Verify semantic expectations helper
    let check_semantics = |map: &<MapRepo as MapRepository>::MapRef| {
        // updated keys present
        for i in 200..800u32 {
            let k = key_with_shared_prefix(i);
            assert_eq!(repo.map_get(map, &k), Some(value((10_000 + i) as usize)));
        }
        // deleted keys absent
        for i in 900..1300u32 {
            let k = key_with_shared_prefix(i);
            assert_eq!(repo.map_get(map, &k), None);
        }
        // inserted keys present
        for i in 5000..5600u32 {
            let k = key_with_shared_prefix(i);
            assert_eq!(repo.map_get(map, &k), Some(value((20_000 + i) as usize)));
        }
    };

    check_semantics(&ref_map);

    // Try many permutations; fail fast if we find different root hash.
    let mut rng = XorShift64::new(0xC0FFEE_u64);
    let trials = 200usize;

    for t in 0..trials {
        let mut perm = ops.clone();
        shuffle(&mut perm, &mut rng);

        let m = repo.map_update(&base_map, &perm);
        check_semantics(&m);

        let h = repo.map_hash(&m);
        if h != ref_hash {
            println!("FOUND hash difference at trial={t}");
            println!("ref = {}", hex::encode(ref_hash.0));
            println!("got = {}", hex::encode(h.0));
            panic!("root hash depends on update order (trial={t})");
        }
    }
}

// ==================== Snapshot Export/Import Tests ====================

fn durable_repo() -> (crate::durable::DurableMapRepository<TestValue, u32, ()>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let repo =
        crate::durable::DurableMapRepository::<TestValue, u32, ()>::load(dir.path().to_path_buf())
            .unwrap();
    (repo, dir)
}

#[test]
fn test_snapshot_empty_map() {
    let (repo, _dir) = durable_repo();
    let m = crate::durable::DurableMapRepository::<TestValue, u32, ()>::new_map();

    let snapshot = repo.export_snapshot(&m);
    assert_eq!(snapshot.nodes.len(), 1, "Empty snapshot should have 1 node (canonical empty)");
    assert_eq!(snapshot.values.len(), 0);
    assert_eq!(snapshot.branch_children.len(), 0);
    assert_eq!(snapshot.ext_paths.len(), 0);

    let imported = repo.import_snapshot(snapshot);
    let orig_hash = repo.map_hash(&m);
    let imported_hash = repo.map_hash(&imported);
    assert_eq!(orig_hash, imported_hash, "Hash of imported empty map should match original");
    assert!(repo.map_get(&imported, &key(42)).unwrap().is_none());
}

#[test]
fn test_snapshot_single_entry() {
    let (repo1, _dir1) = durable_repo();
    let m = crate::durable::DurableMapRepository::<TestValue, u32, ()>::new_map();
    let m = repo1.map_update(&m, &[(key(1), Some(value(42)))]).unwrap();

    let snapshot = repo1.export_snapshot(&m);

    let (repo2, _dir2) = durable_repo();
    let imported = repo2.import_snapshot(snapshot);

    assert_eq!(
        repo2.map_get(&imported, &key(1)).unwrap(),
        Some(value(42)),
        "key(1) should have value(42)"
    );
    assert!(repo2.map_get(&imported, &key(2)).unwrap().is_none(), "key(2) should be None");
    assert_eq!(repo1.map_hash(&m), repo2.map_hash(&imported), "Hash should match after import");
}

#[test]
fn test_snapshot_many_entries() {
    let (repo1, _dir1) = durable_repo();
    let m = crate::durable::DurableMapRepository::<TestValue, u32, ()>::new_map();

    let updates: Vec<_> = (0..1000).map(|i| (key(i), Some(value(i * 10)))).collect();
    let m = repo1.map_update(&m, &updates).unwrap();
    let orig_hash = repo1.map_hash(&m);

    let snapshot = repo1.export_snapshot(&m);

    let (repo2, _dir2) = durable_repo();
    let imported = repo2.import_snapshot(snapshot);

    for i in 0..1000 {
        assert_eq!(
            repo2.map_get(&imported, &key(i)).unwrap(),
            Some(value(i * 10)),
            "Key {} should have value {}",
            i,
            i * 10
        );
    }
    assert!(repo2.map_get(&imported, &key(1001)).unwrap().is_none());
    assert_eq!(orig_hash, repo2.map_hash(&imported), "Hash should match");
}

#[test]
fn test_snapshot_after_deletions() {
    let (repo1, _dir1) = durable_repo();
    let m = crate::durable::DurableMapRepository::<TestValue, u32, ()>::new_map();

    let updates: Vec<_> = (0..500).map(|i| (key(i), Some(value(i)))).collect();
    let m = repo1.map_update(&m, &updates).unwrap();

    // Delete even keys
    let deletions: Vec<_> = (0..500).filter(|i| i % 2 == 0).map(|i| (key(i), None)).collect();
    let m = repo1.map_update(&m, &deletions).unwrap();
    let orig_hash = repo1.map_hash(&m);

    let snapshot = repo1.export_snapshot(&m);

    let (repo2, _dir2) = durable_repo();
    let imported = repo2.import_snapshot(snapshot);

    for i in 0..500 {
        let got = repo2.map_get(&imported, &key(i)).unwrap();
        if i % 2 == 0 {
            assert!(got.is_none(), "Even key {} should be deleted", i);
        } else {
            assert_eq!(got, Some(value(i)), "Odd key {} should have value {}", i, i);
        }
    }
    assert_eq!(orig_hash, repo2.map_hash(&imported), "Hash should match after deletions");
}

#[test]
fn test_snapshot_serialization_roundtrip() {
    let (repo1, _dir1) = durable_repo();
    let m = crate::durable::DurableMapRepository::<TestValue, u32, ()>::new_map();

    let updates: Vec<_> = (0..100).map(|i| (key(i), Some(value(i * 7)))).collect();
    let m = repo1.map_update(&m, &updates).unwrap();
    let orig_hash = repo1.map_hash(&m);

    let snapshot = repo1.export_snapshot(&m);

    // Serialize to bincode (this is how data travels over the network)
    let bytes = bincode::serialize(&snapshot).unwrap();
    let deserialized: crate::trie::smt::TrieMapSnapshot<TestValue> =
        bincode::deserialize(&bytes).unwrap();

    let (repo2, _dir2) = durable_repo();
    let imported = repo2.import_snapshot(deserialized);

    for i in 0..100 {
        assert_eq!(
            repo2.map_get(&imported, &key(i)).unwrap(),
            Some(value(i * 7)),
            "Key {} should have value {} after bincode roundtrip",
            i,
            i * 7
        );
    }
    assert_eq!(orig_hash, repo2.map_hash(&imported), "Hash should match after bincode roundtrip");
}

#[test]
fn test_snapshot_independence() {
    let (repo, _dir) = durable_repo();
    let m = crate::durable::DurableMapRepository::<TestValue, u32, ()>::new_map();

    let m1 = repo.map_update(&m, &[(key(1), Some(value(1))), (key(2), Some(value(2)))]).unwrap();

    let snapshot = repo.export_snapshot(&m1);
    let m2 = repo.import_snapshot(snapshot);

    // Update m1
    let m1_updated = repo.map_update(&m1, &[(key(1), Some(value(100)))]).unwrap();

    // m2 should still have original value
    assert_eq!(
        repo.map_get(&m1_updated, &key(1)).unwrap(),
        Some(value(100)),
        "m1 updated should have value(100)"
    );
    assert_eq!(
        repo.map_get(&m2, &key(1)).unwrap(),
        Some(value(1)),
        "m2 should still have value(1) — snapshot is independent"
    );
    assert_eq!(
        repo.map_get(&m2, &key(2)).unwrap(),
        Some(value(2)),
        "m2 should still have value(2)"
    );
}

#[test]
fn test_snapshot_with_split_and_merge() {
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();

    let updates: Vec<_> = (0..200).map(|i| (key(i), Some(value(i)))).collect();
    let original = repo.map_update(&m0, &updates);
    let orig_hash = repo.map_hash(&original);

    let pfx = key(50).0;
    let path = MapKeyPath { prefix: MapKey(pfx), len: 8 };
    let (part_a, part_b) = repo.map_split(&original, path);

    // Export each part
    let snap_a = repo.export_snapshot(&part_a);
    let snap_b = repo.export_snapshot(&part_b);

    // Import into a new repo
    let repo2 = MapRepo::new();
    let imported_a = repo2.import_snapshot(snap_a);
    let imported_b = repo2.import_snapshot(snap_b);

    // Merge
    let merged = repo2.merge(&imported_a, &imported_b);

    for i in 0..200 {
        assert_eq!(
            repo2.map_get(&merged, &key(i)),
            Some(value(i)),
            "Key {} should exist after split+export+import+merge",
            i
        );
    }
    assert_eq!(
        orig_hash,
        repo2.map_hash(&merged),
        "Hash should match after split+export+import+merge"
    );
}

#[test]
fn test_snapshot_concurrent_import() {
    let repo = MapRepo::new();

    // Create two independent maps with different data
    let m0 = MapRepo::new_map();
    let map1 = repo.map_update(&m0, &(0..50).map(|i| (key(i), Some(value(i)))).collect::<Vec<_>>());
    let map2 = repo
        .map_update(&m0, &(1000..1050).map(|i| (key(i), Some(value(i * 2)))).collect::<Vec<_>>());

    let snap1 = repo.export_snapshot(&map1);
    let snap2 = repo.export_snapshot(&map2);

    // Import both into a single repo
    let repo2 = MapRepo::new();
    let imported1 = repo2.import_snapshot(snap1);
    let imported2 = repo2.import_snapshot(snap2);

    // Both should work independently
    for i in 0..50 {
        assert_eq!(
            repo2.map_get(&imported1, &key(i)),
            Some(value(i)),
            "Imported1 key {} should have value {}",
            i,
            i
        );
        assert!(
            repo2.map_get(&imported1, &key(1000 + i)).is_none(),
            "Imported1 should not contain keys from map2"
        );
    }
    for i in 1000..1050 {
        assert_eq!(
            repo2.map_get(&imported2, &key(i)),
            Some(value(i * 2)),
            "Imported2 key {} should have value {}",
            i,
            i * 2
        );
        assert!(
            repo2.map_get(&imported2, &key(i - 1000)).is_none(),
            "Imported2 should not contain keys from map1"
        );
    }
}

#[test]
#[ignore]
fn test_snapshot_large_tree() {
    let n = 100_000usize;
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();

    let start = Instant::now();
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i * 3)))).collect();
    let m = repo.map_update(&m0, &updates);
    println!("Built tree with {} entries in {:?}", n, start.elapsed());

    let start = Instant::now();
    let snapshot = repo.export_snapshot(&m);
    println!(
        "Exported snapshot in {:?} (nodes={}, values={}, bc={}, ext={})",
        start.elapsed(),
        snapshot.nodes.len(),
        snapshot.values.len(),
        snapshot.branch_children.len(),
        snapshot.ext_paths.len()
    );

    let start = Instant::now();
    let bytes = bincode::serialize(&snapshot).unwrap();
    println!("Serialized snapshot: {} bytes in {:?}", bytes.len(), start.elapsed());

    let start = Instant::now();
    let deserialized: crate::trie::smt::TrieMapSnapshot<TestValue> =
        bincode::deserialize(&bytes).unwrap();
    println!("Deserialized snapshot in {:?}", start.elapsed());

    let repo2 = MapRepo::new();
    let start = Instant::now();
    let imported = repo2.import_snapshot(deserialized);
    println!("Imported snapshot in {:?}", start.elapsed());

    // Spot-check 1000 random keys
    let mut rng = 0x12345678u64;
    for _ in 0..1000 {
        rng ^= rng >> 12;
        rng ^= rng << 25;
        rng ^= rng >> 27;
        rng = rng.wrapping_mul(2685821657736338717);
        let i = (rng as usize) % n;
        assert_eq!(
            repo2.map_get(&imported, &key(i)),
            Some(value(i * 3)),
            "Spot check failed for key {}",
            i
        );
    }
    assert_eq!(repo.map_hash(&m), repo2.map_hash(&imported), "Hash mismatch on large tree");
}

// ==================== collect_values Tests ====================

#[test]
fn test_collect_values_empty() {
    let repo = MapRepo::new();
    let m = MapRepo::new_map();
    let values = repo.collect_values(&m);
    assert!(values.is_empty(), "Collecting from empty map should yield nothing");
}

#[test]
fn test_collect_values_roundtrip() {
    let repo = MapRepo::new();
    let m0 = MapRepo::new_map();

    let n = 200usize;
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i * 5)))).collect();
    let m = repo.map_update(&m0, &updates);

    let collected = repo.collect_values(&m);
    assert_eq!(collected.len(), n, "Should collect {} values", n);

    // Verify all expected keys are present
    let collected_map: std::collections::HashMap<MapKey, TestValue> =
        collected.into_iter().collect();
    for i in 0..n {
        let v = collected_map.get(&key(i));
        assert_eq!(
            v,
            Some(&value(i * 5)),
            "collect_values key {} should map to value {}",
            i,
            i * 5
        );
    }
}
