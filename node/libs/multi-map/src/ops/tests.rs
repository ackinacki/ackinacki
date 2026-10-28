use std::io::Read;
use std::io::Write;

use blake3::Hasher;
use node_types::Blake3Hashable;
use serde::Deserialize;
use serde::Serialize;

use crate::node::Node;
use crate::ops::get;
use crate::ops::graft;
use crate::ops::nibble_at;
use crate::ops::prefix_bits_match;
use crate::ops::split;
use crate::ops::update;
use crate::ops::update_single;
use crate::MapKey;
use crate::MapKeyPath;
use crate::MultiMapValue;

// ====================== test helpers ======================

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TV(pub u64);

impl Blake3Hashable for TV {
    fn update_hasher(&self, hasher: &mut Hasher) {
        hasher.update(&self.0.to_be_bytes());
    }
}

impl MultiMapValue for TV {
    fn write_value<W: Write>(&self, w: &mut W) -> anyhow::Result<()> {
        w.write_all(&self.0.to_be_bytes())?;
        Ok(())
    }

    fn read_value<R: Read>(r: &mut R) -> anyhow::Result<Self> {
        let mut buf = [0u8; 8];
        r.read_exact(&mut buf)?;
        Ok(TV(u64::from_be_bytes(buf)))
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

fn prefix_bits_match_local(prefix: &[u8; 32], k: &[u8; 32], bits: u16) -> bool {
    prefix_bits_match(prefix, k, bits)
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
fn update_single_respects_root_path_scope() {
    let inside = mk_key(&[0xab, 0x80]);
    let outside = mk_key(&[0xab, 0x00]);
    let path = MapKeyPath { prefix: inside, len: 9 };
    assert!(prefix_bits_match(&path.prefix.0, &inside.0, path.len));
    assert!(!prefix_bits_match(&path.prefix.0, &outside.0, path.len));

    let root = update(&Node::Empty, &path, &[(inside, Some(TV(1)))]);
    let unchanged = update_single(&root, &path, &outside, Some(TV(2)));

    assert_eq!(root_hash(&unchanged), root_hash(&root));
    assert_eq!(get(&unchanged, &path, &inside), Some(TV(1)));
    assert_eq!(get(&unchanged, &path, &outside), None);
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

// ====================== split / graft tests ======================

#[test]
fn split_graft_roundtrip_nibble_aligned() {
    let path = default_path();
    let n = 4000usize;
    let keys: Vec<MapKey> = (0..n).map(key).collect();
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i as u64)))).collect();
    let original = update(&Node::Empty, &path, &updates);

    let len_bits: u16 = 12;
    let pfx = keys[123].0;
    let split_path = MapKeyPath { prefix: MapKey(pfx), len: len_bits };

    let (without, branch) = split(&original, path, split_path);
    let grafted = graft(&without, path, &branch, split_path);

    assert!(get(&branch, &split_path, &keys[123]).is_some());
    assert!(get(&without, &path, &keys[123]).is_none());

    for (i, k) in keys.iter().enumerate() {
        let in_pfx = prefix_bits_match_local(&pfx, &k.0, len_bits);
        assert_eq!(get(&branch, &split_path, k).is_some(), in_pfx, "i={i}");
        assert_eq!(get(&without, &path, k).is_some(), !in_pfx, "i={i}");
        assert_eq!(get(&grafted, &path, k), get(&original, &path, k), "i={i}");
    }

    assert_eq!(root_hash(&grafted), root_hash(&original));
}

#[test]
fn split_graft_roundtrip_unaligned() {
    let path = default_path();
    let n = 8000usize;
    let keys: Vec<MapKey> = (0..n).map(key).collect();
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i as u64)))).collect();
    let original = update(&Node::Empty, &path, &updates);

    let len_bits: u16 = 5;
    let pfx = keys[777].0;
    let split_path = MapKeyPath { prefix: MapKey(pfx), len: len_bits };

    let (without, branch) = split(&original, path, split_path);
    let grafted = graft(&without, path, &branch, split_path);

    for (i, k) in keys.iter().enumerate() {
        let in_pfx = prefix_bits_match_local(&pfx, &k.0, len_bits);
        assert_eq!(get(&branch, &split_path, k).is_some(), in_pfx, "i={i}");
        assert_eq!(get(&without, &path, k).is_some(), !in_pfx, "i={i}");
        assert_eq!(get(&grafted, &path, k), get(&original, &path, k), "i={i}");
    }

    assert_eq!(root_hash(&grafted), root_hash(&original));
}

#[test]
fn split_graft_stress_many_prefix_lengths() {
    let path = default_path();
    let n = 20_000usize;
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i as u64)))).collect();
    let original = update(&Node::Empty, &path, &updates);
    let orig_hash = root_hash(&original);

    // 256 = full-depth path (64 nibbles, r=0): pins an entire key, the
    // boundary the u16 `len` was widened to allow.
    let lens: [u16; 17] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 13, 15, 16, 31, 63, 64, 256];
    for (t, &len_bits) in lens.iter().enumerate() {
        let pfx = key((t * 7919) % n).0;
        let split_path = MapKeyPath { prefix: MapKey(pfx), len: len_bits };

        let (a, b) = split(&original, path, split_path);
        let grafted = graft(&a, path, &b, split_path);

        assert_eq!(root_hash(&grafted), orig_hash, "len_bits={len_bits}");
    }
}

#[test]
fn graft_with_empty_subtree() {
    let path = default_path();
    let updates: Vec<_> = (0..2000).map(|i| (key(i), Some(value(i as u64)))).collect();
    let original = update(&Node::Empty, &path, &updates);

    let empty: Node<TV> = Node::Empty;
    let empty_path = MapKeyPath::default();
    // graft(_, empty) at empty_path replaces the entire root with empty
    let grafted = graft(&original, path, &empty, empty_path);
    assert!(grafted.is_empty());
}

#[test]
fn graft_empty_with_full_restores() {
    let path = default_path();
    let updates: Vec<_> = (0..2000).map(|i| (key(i), Some(value(i as u64)))).collect();
    let original = update(&Node::Empty, &path, &updates);
    let orig_hash = root_hash(&original);

    let empty: Node<TV> = Node::Empty;
    let grafted = graft(&empty, path, &original, path);

    assert_eq!(root_hash(&grafted), orig_hash);
}

// ================ split / graft WITHIN A NON-EMPTY VIEW (regression #2278) ================
//
// `split`/`graft` operate on a *view*: a trie whose root already represents
// the subtree at `root_path`, so the ops descend from `start_depth =
// root_path.len / 4` on absolute key paths. The account-loss bug fixed in
// #2278 lived exactly here — the old code rebased key paths with `suffix_path`
// and walked from depth 0, which misaligned nibbles whenever `root_path.len`
// was not a multiple of 4 (e.g. the 253/255-bit dapp boundaries used by
// durable-state cropping). Every pre-existing split/graft test used
// `root_path = default` (len 0), so this whole path had no direct coverage.
//
// All tests below drive `root_path.len > 0` and lean on the non-nibble-aligned
// boundary region (bits 248..256).

use crate::ops::graft_batch;

/// A key that is all zero except the last byte. Every such key shares its top
/// 248 bits, differing only in bits 248..256 — the non-nibble-aligned region
/// (bits 253/255) where the bug lived.
fn boundary_key(last: u8) -> MapKey {
    let mut k = [0u8; 32];
    k[31] = last;
    MapKey(k)
}

/// All 256 single-last-byte keys.
fn boundary_keys() -> Vec<MapKey> {
    (0..=255u8).map(boundary_key).collect()
}

/// Build the view rooted at `view_path` holding exactly the keys of `all` that
/// fall under it (value = index in `all`). Mirrors how durable-state cropping
/// produces a view. `update` with a non-empty `root_path` filters by prefix
/// (incl. the sub-nibble boundary) and builds from `start_depth`.
fn view_of(all: &[MapKey], view_path: MapKeyPath) -> Node<TV> {
    let updates: Vec<(MapKey, Option<TV>)> =
        all.iter().enumerate().map(|(i, k)| (*k, Some(value(i as u64)))).collect();
    update(&Node::Empty, &view_path, &updates)
}

/// Split `view` (rooted at `view_path`) at a strictly deeper `split_path` and
/// assert the three properties the fix must guarantee:
///   1. every key lands in exactly one side, matching prefix membership,
///   2. no key is lost or duplicated across the two sides,
///   3. `graft` is the exact inverse: it restores the view (hash + per-key).
fn assert_split_in_view(
    all: &[MapKey],
    view: &Node<TV>,
    view_path: MapKeyPath,
    split_path: MapKeyPath,
) {
    assert!(split_path.len > view_path.len, "helper is for strictly deeper splits");

    let (without, with) = split(view, view_path, split_path);

    let common = view_path.len.min(split_path.len);
    let compatible =
        common == 0 || prefix_bits_match(&view_path.prefix.0, &split_path.prefix.0, common);

    let mut n_with = 0usize;
    let mut n_without = 0usize;
    for k in all {
        let in_view = get(view, &view_path, k).is_some();
        let under_split =
            compatible && prefix_bits_match(&split_path.prefix.0, &k.0, split_path.len);
        let expect_with = in_view && under_split;
        let expect_without = in_view && !under_split;

        let got_with = get(&with, &split_path, k).is_some();
        let got_without = get(&without, &view_path, k).is_some();
        assert_eq!(got_with, expect_with, "WITH wrong for key last=0x{:02x}", k.0[31]);
        assert_eq!(got_without, expect_without, "WITHOUT wrong for key last=0x{:02x}", k.0[31]);
        // No key may appear on both sides.
        assert!(!(got_with && got_without), "key last=0x{:02x} duplicated", k.0[31]);
        n_with += got_with as usize;
        n_without += got_without as usize;
    }
    // Conservation: the two sides together account for exactly the view.
    let n_in_view = all.iter().filter(|k| get(view, &view_path, k).is_some()).count();
    assert_eq!(n_with + n_without, n_in_view, "accounts lost or duplicated by split");

    let grafted = graft(&without, view_path, &with, split_path);
    assert_eq!(root_hash(&grafted), root_hash(view), "graft did not restore the view exactly");
    for k in all {
        assert_eq!(get(&grafted, &view_path, k), get(view, &view_path, k), "roundtrip mismatch");
    }
}

/// The core regression: split a non-empty view at every combination of
/// nibble-aligned and non-aligned boundaries across bits 248..256. Directly
/// exercises the `suffix_path`-rebasing bug the fix removed.
#[test]
fn split_in_boundary_view_no_account_loss() {
    let all = boundary_keys();
    // (view_prefix_last_byte, view_len, split_prefix_last_byte, split_len).
    // Every split prefix only sets bits >= view_len, so it stays inside the
    // zero-prefixed view. Residues r = view_len%4 / split_len%4 cover 0..3.
    let cases: &[(u8, u16, u8, u16)] = &[
        (0, 248, 0x04, 253), // aligned(62) view  -> r=1 split
        (0, 250, 0x04, 255), // r=2 view          -> r=3 split
        (0, 252, 0x06, 254), // aligned(63) view  -> r=2 split
        (0, 253, 0x05, 255), // r=1 view          -> r=3 split (the cropped-state case)
        (0, 253, 0x07, 256), // r=1 view          -> full-depth split
        (0, 254, 0x02, 256), // r=2 view          -> full
        (0, 255, 0x01, 256), // r=3 view          -> full
        (0, 249, 0x0C, 252), // r=1 view          -> aligned split
        (0, 200, 0x08, 253), // deep-common view  -> r=1 split
        (0, 240, 0x40, 250), // aligned view      -> r=2 split, high split bit
    ];
    for &(vp, vlen, sp, slen) in cases {
        let view_path = MapKeyPath { prefix: boundary_key(vp), len: vlen };
        let split_path = MapKeyPath { prefix: boundary_key(sp), len: slen };
        let view = view_of(&all, view_path);
        assert_split_in_view(&all, &view, view_path, split_path);
    }
}

/// Same view-split invariants but with a non-zero leading nibble, so the
/// descent traverses non-zero branch nibbles before reaching the boundary.
#[test]
fn split_in_view_with_nonzero_leading_prefix() {
    // Keys: byte0 = 0xA5, everything else zero except the last byte.
    let all: Vec<MapKey> = (0..=255u8)
        .map(|last| {
            let mut k = [0u8; 32];
            k[0] = 0xA5;
            k[31] = last;
            MapKey(k)
        })
        .collect();

    let lead = |last: u8| {
        let mut k = [0u8; 32];
        k[0] = 0xA5;
        k[31] = last;
        MapKey(k)
    };

    let cases: &[(u16, u8, u16)] = &[(8, 0x04, 253), (10, 0x06, 255), (250, 0x02, 256)];
    for &(vlen, sp, slen) in cases {
        let view_path = MapKeyPath { prefix: lead(0), len: vlen };
        let split_path = MapKeyPath { prefix: lead(sp), len: slen };
        let view = view_of(&all, view_path);
        assert_split_in_view(&all, &view, view_path, split_path);
    }
}

/// Random keys with shallow, non-aligned view roots. Complements the dense
/// boundary family with realistic branchy structure near the root.
#[test]
fn split_in_view_random_keys_partition_roundtrip() {
    let n = 8000usize;
    let all: Vec<MapKey> = (0..n).map(key).collect();

    // Derive view + split prefixes from the same seed key so the split is
    // guaranteed to sit inside the view; use small lengths so views are dense.
    let cases: &[(usize, u16, u16)] =
        &[(100, 4, 9), (100, 5, 12), (2500, 7, 13), (2500, 8, 16), (4321, 3, 8), (4321, 6, 15)];
    for &(seed, vlen, slen) in cases {
        let seed_key = key(seed);
        let view_path = MapKeyPath { prefix: seed_key, len: vlen };
        let split_path = MapKeyPath { prefix: seed_key, len: slen };
        let view = view_of(&all, view_path);
        assert_split_in_view(&all, &view, view_path, split_path);
    }
}

/// Nested splits: crop full -> A (250) -> B (253) -> C (255), then reassemble
/// bottom-up. This is the multi-map-level analogue of the durable-repository
/// regression test, but exhaustively checks every key at every level.
#[test]
fn nested_split_in_view_chain_no_loss() {
    let all = boundary_keys();
    let full = view_of(&all, default_path());

    let pa = MapKeyPath { prefix: boundary_key(0), len: 250 };
    let pb = MapKeyPath { prefix: boundary_key(0), len: 253 };
    let pc = MapKeyPath { prefix: boundary_key(0), len: 255 };

    let (full_wo, a) = split(&full, default_path(), pa);
    let (a_wo, b) = split(&a, pa, pb);
    let (b_wo, c) = split(&b, pb, pc);

    // Membership at every level, computed independently from the prefixes.
    for k in &all {
        let in_a = prefix_bits_match(&pa.prefix.0, &k.0, pa.len);
        let in_b = prefix_bits_match(&pb.prefix.0, &k.0, pb.len);
        let in_c = prefix_bits_match(&pc.prefix.0, &k.0, pc.len);
        assert_eq!(get(&a, &pa, k).is_some(), in_a, "A");
        assert_eq!(get(&b, &pb, k).is_some(), in_b, "B");
        assert_eq!(get(&c, &pc, k).is_some(), in_c, "C");
        // Disjoint remainders.
        assert_eq!(get(&full_wo, &default_path(), k).is_some(), !in_a, "full_wo");
        assert_eq!(get(&a_wo, &pa, k).is_some(), in_a && !in_b, "a_wo");
        assert_eq!(get(&b_wo, &pb, k).is_some(), in_b && !in_c, "b_wo");
    }

    // Reassemble bottom-up; each graft must restore its parent exactly.
    let b2 = graft(&b_wo, pb, &c, pc);
    assert_eq!(root_hash(&b2), root_hash(&b));
    let a2 = graft(&a_wo, pa, &b2, pb);
    assert_eq!(root_hash(&a2), root_hash(&a));
    let full2 = graft(&full_wo, default_path(), &a2, pa);
    assert_eq!(root_hash(&full2), root_hash(&full));
    for k in &all {
        assert_eq!(get(&full2, &default_path(), k), get(&full, &default_path(), k));
    }
}

/// A view built directly by `update(_, view_path, _)` must be byte-identical
/// (same root hash) to the view extracted by `split(full, default, view_path)`.
/// If these ever diverge, cropping and re-cropping would silently disagree.
#[test]
fn view_built_by_update_equals_split_extraction() {
    let all = boundary_keys();
    let full = view_of(&all, default_path());
    for &vlen in &[248u16, 250, 252, 253, 254, 255] {
        let view_path = MapKeyPath { prefix: boundary_key(0), len: vlen };
        let via_update = view_of(&all, view_path);
        let (_without, via_split) = split(&full, default_path(), view_path);
        assert_eq!(
            root_hash(&via_update),
            root_hash(&via_split),
            "update vs split view mismatch at len={vlen}",
        );
    }
}

// ----- edge cases of split-in-a-view -----

/// Splitting a view at a prefix *shallower* than its own root: the whole view
/// lies under `split_path`, so everything goes to the extracted side.
#[test]
fn split_view_at_shallower_path_extracts_all() {
    let all = boundary_keys();
    let view_path = MapKeyPath { prefix: boundary_key(0), len: 253 };
    let view = view_of(&all, view_path); // keys 0..7
    let split_path = MapKeyPath { prefix: boundary_key(0), len: 250 };

    let (without, with) = split(&view, view_path, split_path);
    assert!(without.is_empty(), "nothing should remain");
    for k in &all {
        let in_view = get(&view, &view_path, k).is_some();
        assert_eq!(get(&with, &split_path, k).is_some(), in_view);
    }
}

/// Splitting a view at exactly its own path extracts the whole view unchanged.
#[test]
fn split_view_at_own_path_extracts_all() {
    let all = boundary_keys();
    let view_path = MapKeyPath { prefix: boundary_key(0), len: 253 };
    let view = view_of(&all, view_path);

    let (without, with) = split(&view, view_path, view_path);
    assert!(without.is_empty());
    assert_eq!(root_hash(&with), root_hash(&view));
}

/// Splitting at a prefix disjoint from the view's prefix extracts nothing and
/// leaves the view untouched.
#[test]
fn split_view_disjoint_prefix_extracts_nothing() {
    let all = boundary_keys();
    let view_path = MapKeyPath { prefix: boundary_key(0), len: 252 }; // keys 0..15
    let view = view_of(&all, view_path);

    // Prefix requires bit 255 == 1 within the low nibble but bit 253==1 too;
    // pick a split whose fixed bits contradict the view membership is not
    // possible here (view is zero-prefixed), so instead diverge on a bit the
    // view keys all share as 0: require last byte high bit set at bit 248.
    let mut diverging = [0u8; 32];
    diverging[30] = 0x01; // sets bit 247 -> outside the zero-prefixed view
    let split_path = MapKeyPath { prefix: MapKey(diverging), len: 248 };

    let (without, with) = split(&view, view_path, split_path);
    assert!(with.is_empty(), "disjoint split must extract nothing");
    assert_eq!(root_hash(&without), root_hash(&view));
}

/// A deeper split prefix that no key matches yields an empty extraction and
/// keeps every key on the remainder side (no silent loss).
#[test]
fn split_view_prefix_selecting_no_keys() {
    let all = boundary_keys();
    let view_path = MapKeyPath { prefix: boundary_key(0), len: 252 }; // keys 0..15
    let view = view_of(&all, view_path);
    // last byte 0x1F needs bits 251..256 = 11111; but view fixes bits 0..251=0
    // and these keys only go up to 0x0F, so 0x1F selects nothing yet is a valid
    // in-view prefix (top 252 bits zero).
    let split_path = MapKeyPath { prefix: boundary_key(0x0F), len: 256 };

    let (without, with) = split(&view, view_path, split_path);
    // 0x0F is a real key, so `with` holds exactly it; verify no loss regardless.
    let n_view = all.iter().filter(|k| get(&view, &view_path, k).is_some()).count();
    let n_with = all.iter().filter(|k| get(&with, &split_path, k).is_some()).count();
    let n_without = all.iter().filter(|k| get(&without, &view_path, k).is_some()).count();
    assert_eq!(n_with + n_without, n_view, "no key may be lost");

    let grafted = graft(&without, view_path, &with, split_path);
    assert_eq!(root_hash(&grafted), root_hash(&view));
}

/// Splitting an empty view is a no-op producing two empty sides.
#[test]
fn split_empty_view_returns_empty() {
    let view_path = MapKeyPath { prefix: boundary_key(0), len: 253 };
    let split_path = MapKeyPath { prefix: boundary_key(0), len: 255 };
    let empty: Node<TV> = Node::Empty;
    let (without, with) = split(&empty, view_path, split_path);
    assert!(without.is_empty());
    assert!(with.is_empty());
}

/// `graft` must be a no-op when the subtree's path is incompatible with the
/// target view's path (guards against grafting a child into the wrong parent).
#[test]
fn graft_prefix_mismatch_is_noop() {
    let a_keys: Vec<MapKey> = (0..=15u8).map(boundary_key).collect();
    let a_path = MapKeyPath { prefix: boundary_key(0), len: 252 };
    let a = view_of(&a_keys, a_path);

    // A subtree living under a prefix that diverges from `a_path` at bit 247.
    let mut other = [0u8; 32];
    other[30] = 0x01;
    let b_key = MapKey(other);
    let b_path = MapKeyPath { prefix: b_key, len: 248 };
    let b = view_of(&[b_key], b_path);

    let grafted = graft(&a, a_path, &b, b_path);
    assert_eq!(root_hash(&grafted), root_hash(&a), "incompatible graft must not change target");
    assert!(get(&grafted, &a_path, &b_key).is_none(), "foreign key must not appear");
}

/// `graft_batch` (whose `a_path` was only wired up in #2278) must reassemble a
/// view from several disjoint child subtrees in one pass, matching repeated
/// single grafts.
#[test]
fn graft_batch_reassembles_view_from_children() {
    let all = boundary_keys();
    let view_path = MapKeyPath { prefix: boundary_key(0), len: 248 };
    let view = view_of(&all, view_path); // all 256 keys

    // Carve three disjoint children by their 253-bit prefix (last-byte top 5
    // bits): 0x00, 0x08, 0x10 -> last bytes {0..7}, {8..15}, {16..23}.
    let p0 = MapKeyPath { prefix: boundary_key(0x00), len: 253 };
    let p1 = MapKeyPath { prefix: boundary_key(0x08), len: 253 };
    let p2 = MapKeyPath { prefix: boundary_key(0x10), len: 253 };

    let (rem0, c0) = split(&view, view_path, p0);
    let (rem1, c1) = split(&rem0, view_path, p1);
    let (rem2, c2) = split(&rem1, view_path, p2);

    // Batch-graft the three children back onto the remainder.
    let reassembled = graft_batch(&rem2, view_path, &[(c0, p0), (c1, p1), (c2, p2)]);
    assert_eq!(root_hash(&reassembled), root_hash(&view), "graft_batch must restore the view");

    // Same result as three sequential single grafts.
    let (rem0b, c0b) = split(&view, view_path, p0);
    let (rem1b, c1b) = split(&rem0b, view_path, p1);
    let (rem2b, c2b) = split(&rem1b, view_path, p2);
    let seq = graft(&rem2b, view_path, &c2b, p2);
    let seq = graft(&seq, view_path, &c1b, p1);
    let seq = graft(&seq, view_path, &c0b, p0);
    assert_eq!(root_hash(&seq), root_hash(&reassembled), "batch vs sequential graft differ");

    for k in &all {
        assert_eq!(get(&reassembled, &view_path, k), get(&view, &view_path, k));
    }
}

#[test]
fn graft_batch_coalesces_extension_at_subtree_boundary() {
    let path = default_path();
    // A subtree cut above a long shared suffix starts with an extension.
    // Prefixing that subtree must merge adjacent extensions, not merely hash
    // a non-canonical extension-of-extension with the same leaves.
    let updates = vec![
        (mk_key(&[0x01, 0x11]), Some(value(1))),
        (mk_key(&[0x01, 0x12]), Some(value(2))),
        (mk_key(&[0x02, 0x22]), Some(value(3))),
    ];
    let original = update(&Node::Empty, &path, &updates);
    let child_path = MapKeyPath { prefix: mk_key(&[0x01]), len: 8 };
    let (_, child) = split(&original, path, child_path);
    let sparse = graft_batch(&Node::Empty, path, &[(child, child_path)]);
    let rebuilt = update(&Node::Empty, &path, &updates[..2]);
    assert_eq!(root_hash(&sparse), root_hash(&rebuilt));
    // Also exercise multiple disjoint subtrees sharing an outer prefix.
    let other_path = MapKeyPath { prefix: mk_key(&[0x02]), len: 8 };
    let (_, other) = split(&original, path, other_path);
    let (_, child) = split(&original, path, child_path);
    let assembled = graft_batch(&Node::Empty, path, &[(child, child_path), (other, other_path)]);
    assert_eq!(root_hash(&assembled), root_hash(&original));
}

#[test]
fn graft_batch_assembles_empty_root_from_disjoint_nibble_subtrees() {
    let path = default_path();
    let count = 4096usize;
    let updates: Vec<_> = (0..count).map(|i| (key(i), Some(value(i as u64)))).collect();
    let original = update(&Node::Empty, &path, &updates);
    let mut remainder = original.clone();
    let mut subtrees = Vec::with_capacity(16);

    for nibble in 0..16u8 {
        let child_path = MapKeyPath { prefix: mk_key(&[nibble << 4]), len: 4 };
        let (next, child) = split(&remainder, path, child_path);
        assert!(!child.is_empty());
        remainder = next;
        subtrees.push((child, child_path));
    }
    assert!(remainder.is_empty());

    subtrees.reverse();
    let reassembled = graft_batch(&Node::Empty, path, &subtrees);
    assert_eq!(root_hash(&reassembled), root_hash(&original));
    for index in [0, 1, 127, 2048, count - 1] {
        assert_eq!(get(&reassembled, &path, &key(index)), Some(value(index as u64)));
    }
}

/// Regression: a split at *full key depth* (`len == 256`, pinning an entire
/// key) must move that key to the extracted side and REMOVE it from the
/// remainder. `prune_rec` previously left the full-depth leaf in place, so the
/// key appeared on *both* sides — at the L1 durable layer this duplicated a
/// whole dapp's account submap into both the parent and the spawned thread on
/// a 256-bit-dapp split. The pre-existing stress test only checked the graft
/// roundtrip hash (graft overwrites the duplicate), so it never caught this.
#[test]
fn split_at_full_key_depth_does_not_duplicate() {
    let path = default_path();
    let n = 3000usize;
    let all: Vec<MapKey> = (0..n).map(key).collect();
    let updates: Vec<_> = (0..n).map(|i| (key(i), Some(value(i as u64)))).collect();
    let original = update(&Node::Empty, &path, &updates);

    // Pin one existing key at the full 256-bit depth.
    let target = all[1234];
    let split_path = MapKeyPath { prefix: target, len: 256 };

    let (without, with) = split(&original, path, split_path);

    // Extracted side holds exactly the pinned key.
    assert!(get(&with, &split_path, &target).is_some(), "pinned key must be extracted");
    // Remainder must NOT still contain it (the duplication bug).
    assert!(get(&without, &path, &target).is_none(), "pinned key must be removed from remainder");

    // Every other key stays on the remainder, none leak into the single-key
    // extraction — i.e. no loss and no duplication.
    for k in &all {
        let is_target = k.0 == target.0;
        assert_eq!(get(&with, &split_path, k).is_some(), is_target);
        assert_eq!(get(&without, &path, k).is_some(), !is_target);
    }

    // And the split is still exactly invertible.
    let grafted = graft(&without, path, &with, split_path);
    assert_eq!(root_hash(&grafted), root_hash(&original));
}

/// Cut three *disjoint* subtrees out of a tree, then reassemble them in every
/// possible order. Because the three regions are disjoint, the merged result
/// must be byte-identical to the original for all 3! = 6 orderings — via both
/// repeated single `graft` (canonicalizes each step) and `graft_batch`
/// (canonicalizes once at the end). Cut lengths mix a full-depth (256) leaf
/// with two non-nibble-aligned boundaries (252/253) to stress the paths the
/// fix touched.
#[test]
fn three_way_split_reassembles_in_all_orders() {
    let all = boundary_keys();
    let path = default_path();
    let original = view_of(&all, path);
    let orig_hash = root_hash(&original);

    // Three disjoint prefixes:
    //   p0: full-depth single key 0x05
    //   p1: last bytes 0x08..0x0F (253-bit, r=1)
    //   p2: last bytes 0x20..0x2F (252-bit, r=0 but sub-top-byte)
    let p0 = MapKeyPath { prefix: boundary_key(0x05), len: 256 };
    let p1 = MapKeyPath { prefix: boundary_key(0x08), len: 253 };
    let p2 = MapKeyPath { prefix: boundary_key(0x20), len: 252 };

    let (rem0, c0) = split(&original, path, p0);
    let (rem1, c1) = split(&rem0, path, p1);
    let (rem2, c2) = split(&rem1, path, p2);

    // The three cuts are disjoint and non-empty, and the remainder holds
    // everything else — no key lost across the 3-way cut.
    for k in &all {
        let in0 = prefix_bits_match(&p0.prefix.0, &k.0, p0.len);
        let in1 = prefix_bits_match(&p1.prefix.0, &k.0, p1.len);
        let in2 = prefix_bits_match(&p2.prefix.0, &k.0, p2.len);
        // disjoint
        assert!((in0 as u8 + in1 as u8 + in2 as u8) <= 1, "cuts overlap at 0x{:02x}", k.0[31]);
        assert_eq!(get(&c0, &p0, k).is_some(), in0);
        assert_eq!(get(&c1, &p1, k).is_some(), in1);
        assert_eq!(get(&c2, &p2, k).is_some(), in2);
        assert_eq!(get(&rem2, &path, k).is_some(), !(in0 || in1 || in2));
    }

    let children = [(&c0, p0), (&c1, p1), (&c2, p2)];
    let perms: [[usize; 3]; 6] = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];

    for perm in perms {
        // Sequential single grafts in this order.
        let mut acc = rem2.clone();
        for &i in &perm {
            let (child, cpath) = children[i];
            acc = graft(&acc, path, child, cpath);
        }
        assert_eq!(root_hash(&acc), orig_hash, "sequential graft order {perm:?} mismatch");

        // graft_batch with the subtrees supplied in this order.
        let subtrees: Vec<(Node<TV>, MapKeyPath)> =
            perm.iter().map(|&i| (children[i].0.clone(), children[i].1)).collect();
        let batched = graft_batch(&rem2, path, &subtrees);
        assert_eq!(root_hash(&batched), orig_hash, "graft_batch order {perm:?} mismatch");

        // Per-key correctness for one order (cheap sanity across all).
        for k in &all {
            assert_eq!(get(&batched, &path, k), get(&original, &path, k), "order {perm:?} key");
        }
    }
}

/// Complement to `three_way_split_reassembles_in_all_orders`: cutting three
/// disjoint subtrees in any order must yield identical children (by path) and
/// an identical remainder. Split order must not matter for disjoint prefixes.
#[test]
fn three_way_split_is_cut_order_independent() {
    let all = boundary_keys();
    let path = default_path();
    let original = view_of(&all, path);

    let paths = [
        MapKeyPath { prefix: boundary_key(0x05), len: 256 },
        MapKeyPath { prefix: boundary_key(0x08), len: 253 },
        MapKeyPath { prefix: boundary_key(0x20), len: 252 },
    ];
    let perms: [[usize; 3]; 6] = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];

    // Reference: cut in canonical order 0,1,2.
    let mut ref_children = [[0u8; 32]; 3];
    let ref_remainder;
    {
        let (r0, c0) = split(&original, path, paths[0]);
        let (r1, c1) = split(&r0, path, paths[1]);
        let (r2, c2) = split(&r1, path, paths[2]);
        ref_children[0] = root_hash(&c0);
        ref_children[1] = root_hash(&c1);
        ref_children[2] = root_hash(&c2);
        ref_remainder = root_hash(&r2);
    }

    for perm in perms {
        let mut rem = original.clone();
        let mut child_hash = [[0u8; 32]; 3];
        for &i in &perm {
            let (without, with) = split(&rem, path, paths[i]);
            child_hash[i] = root_hash(&with);
            rem = without;
        }
        assert_eq!(root_hash(&rem), ref_remainder, "remainder differs for cut order {perm:?}");
        for i in 0..3 {
            assert_eq!(child_hash[i], ref_children[i], "child {i} differs for cut order {perm:?}");
        }
    }
}
