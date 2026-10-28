//! Regression coverage for canonical shape, scoped split/graft, and partition union.
use std::io::Read;
use std::io::Write;

use node_types::Blake3Hashable;
use rand::seq::SliceRandom;
use rand::Rng;
use rand::SeedableRng;

use crate::MapKey;
use crate::MapKeyPath;
use crate::MultiMapAug;
use crate::MultiMapAugmentation;
use crate::MultiMapValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Value(u64);

impl Blake3Hashable for Value {
    fn update_hasher(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&self.0.to_le_bytes());
    }
}

impl MultiMapValue for Value {
    fn write_value<W: Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        Ok(writer.write_all(&self.0.to_le_bytes())?)
    }

    fn read_value<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        Ok(Self(u64::from_le_bytes(bytes)))
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Capacity(u64);

impl MultiMapAugmentation for Capacity {
    fn identity() -> Self {
        Self(0)
    }

    fn combine(left: &Self, right: &Self) -> Self {
        Self(left.0 + right.0)
    }

    fn replace_child(current: &Self, old: Option<&Self>, new: Option<&Self>) -> Option<Self> {
        Some(Self(current.0 - old.map_or(0, |cap| cap.0) + new.map_or(0, |cap| cap.0)))
    }

    fn write<W: Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        Ok(writer.write_all(&self.0.to_le_bytes())?)
    }

    fn read<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        Ok(Self(u64::from_le_bytes(bytes)))
    }
}

type Map = MultiMapAug<Value, Capacity>;

fn key(byte: u8) -> MapKey {
    MapKey([byte; 32])
}

fn path(byte: u8, len: u16) -> MapKeyPath {
    MapKeyPath { prefix: key(byte), len }
}

fn entries(map: &Map) -> Vec<(MapKey, Value, Capacity)> {
    map.iter().map(|(key, value)| (key, *value, map.get_aug(&key).unwrap().1)).collect()
}

fn bytes(map: &Map) -> Vec<u8> {
    let mut bytes = Vec::new();
    map.write(&mut bytes).unwrap();
    bytes
}

fn rebuild(scope: MapKeyPath, entries: &[(MapKey, Value, Capacity)]) -> Map {
    let updates: Vec<_> =
        entries.iter().map(|(key, value, cap)| (*key, Some((*value, *cap)))).collect();
    Map { root_path: scope, ..Map::new() }.update_aug(&updates)
}

fn assert_same(actual: &Map, expected: &Map) {
    assert_eq!(actual.root_path, expected.root_path);
    assert_eq!(entries(actual), entries(expected));
    assert_eq!(actual.hash(), expected.hash());
    assert_eq!(actual.root.augmentation(), expected.root.augmentation());
    assert_eq!(bytes(actual), bytes(expected));
}

fn fixture() -> Map {
    rebuild(
        MapKeyPath::default(),
        &[(key(0x10), Value(1), Capacity(10)), (key(0x20), Value(2), Capacity(20))],
    )
}

#[test]
fn splitting_absent_prefix_is_a_noop_even_inside_an_extension() {
    let source = fixture();
    let before = bytes(&source);
    let (rest, cut) = source.split(path(0x1f, 8));
    assert!(cut.root.is_empty());
    assert_same(&rest, &source);
    assert_same(&rest.graft(&cut), &source);
    assert_same(&rest.graft_batch(&[cut]), &source);
    assert_eq!(bytes(&source), before);
}

#[test]
fn deleting_absent_keys_keeps_the_original_canonical_tree() {
    for source in
        [fixture(), rebuild(MapKeyPath::default(), &[(key(0x10), Value(1), Capacity(10))])]
    {
        for missing in [0x11, 0x1f, 0xff] {
            assert_same(&source.update_single_aug(&key(missing), None), &source);
            assert_same(&source.update_aug(&[(key(missing), None)]), &source);
        }
        assert_same(&source.update_aug(&[(key(0x11), None), (key(0x1f), None)]), &source);
    }
}

#[test]
fn ancestor_graft_restricts_the_subtree_to_the_receivers_scope() {
    let (_, view) = fixture().split(path(0x10, 8));
    let (rest, cut) = view.split(path(0x10, 4));
    assert!(rest.root.is_empty());
    assert_same(&rest.graft(&cut), &view);
    assert_same(&rest.graft_batch(std::slice::from_ref(&cut)), &view);
    // Changes inside the detached, broader scope must not leak outside the
    // receiver's narrower view when the subtree is grafted back.
    let modified = cut.update_single_aug(&key(0x11), Some((Value(3), Capacity(30))));
    assert_same(&rest.graft(&modified), &view);
}

#[test]
fn all_bit_boundaries_preserve_split_graft_and_disjoint_union() {
    for base_bits in [0, 1, 3, 4, 7, 8, 9, 127, 255, 256] {
        let source = rebuild(
            path(0x10, base_bits),
            &[
                (key(0x10), Value(1), Capacity(10)),
                (key(0x11), Value(2), Capacity(20)),
                (key(0x20), Value(3), Capacity(30)),
            ],
        );
        let before = bytes(&source);
        let all = entries(&source);
        for prefix in [0x00, 0x10, 0x11, 0x1f, 0x20, 0xff] {
            for bits in 0..=256 {
                let scope = path(prefix, bits);
                let (rest, cut) = source.split(scope);
                let (inside, outside): (Vec<_>, Vec<_>) =
                    all.iter().copied().partition(|(key, _, _)| {
                        crate::ops::prefix_bits_match(&scope.prefix.0, &key.0, bits)
                    });
                assert_same(&cut, &rebuild(scope, &inside));
                assert_same(&rest, &rebuild(source.root_path, &outside));
                assert_same(&rest.graft(&cut), &source);
                assert_same(&rest.graft_batch(std::slice::from_ref(&cut)), &source);
                let detached = Map { root_path: source.root_path, ..Map::new() }.graft(&cut);
                assert_same(&rest.merge_disjoint(&detached).unwrap(), &source);
            }
        }
        assert_eq!(bytes(&source), before);
    }
}

#[test]
fn modified_detached_subtree_and_remainder_graft_to_the_reference_map() {
    let source = rebuild(
        MapKeyPath::default(),
        &[
            (key(0x10), Value(1), Capacity(10)),
            (key(0x11), Value(2), Capacity(20)),
            (key(0x20), Value(3), Capacity(30)),
        ],
    );
    for bits in 0..=256 {
        let scope = path(0x10, bits);
        let (mut rest, mut cut) = source.split(scope);
        // Only update within the extracted scope and outside it in the
        // remainder, including removing the last leaf of either side.
        let cut_key = entries(&cut).first().map(|entry| entry.0);
        let rest_key = entries(&rest).first().map(|entry| entry.0);
        let mut expected = source.clone();
        if let Some(key) = cut_key {
            cut = cut.update_single_aug(&key, None);
            expected = expected.update_single_aug(&key, None);
        }
        if let Some(key) = rest_key {
            rest = rest.update_single_aug(&key, Some((Value(99), Capacity(100))));
            expected = expected.update_single_aug(&key, Some((Value(99), Capacity(100))));
        }
        assert_same(&rest.graft(&cut), &expected);
        assert_same(&rest.graft_batch(&[cut]), &expected);
    }
}

#[test]
fn seeded_capacity_partitions_rebuild_and_merge_after_deletions() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(421);
    for fixture in 0..200 {
        let mut updates = Vec::new();
        for i in 0..40 {
            let mut bytes = [0; 32];
            rng.fill(&mut bytes);
            if fixture % 3 == 0 {
                bytes[..30].fill(0);
            }
            if fixture % 3 == 1 {
                bytes[..28].fill(0xe8);
            }
            updates.push((MapKey(bytes), Some((Value(i), Capacity(rng.gen_range(1..=100))))));
        }
        let mut source = Map::new().update_aug(&updates);
        for (key, _) in updates.iter().step_by(3) {
            source = source.update_single_aug(key, None);
        }
        assert_same(&source, &rebuild(source.root_path, &entries(&source)));
        let before = bytes(&source);
        for target in [1, 50, 100, 300, 10000] {
            let parts = source.partition_by_capacity(target, |a| Some(a.0)).unwrap();
            assert_eq!(parts.iter().map(|p| p.capacity).sum::<u64>(), source.root.augmentation().0);
            for part in &parts {
                assert_same(&part.map, &rebuild(part.map.root_path, &entries(&part.map)));
                assert_eq!(part.capacity, part.map.root.augmentation().0);
            }
            let maps: Vec<_> = parts.into_iter().rev().map(|p| p.map).collect();
            assert_same(&Map::merge_disjoint_batch(&maps).unwrap(), &source);
        }
        assert_eq!(bytes(&source), before);
    }
}

fn matches(scope: MapKeyPath, key: &MapKey) -> bool {
    (0..usize::from(scope.len)).all(|bit| {
        let mask = 0x80 >> (bit % 8);
        scope.prefix.0[bit / 8] & mask == key.0[bit / 8] & mask
    })
}

fn random_key_in_scope(rng: &mut rand::rngs::StdRng, scope: MapKeyPath) -> MapKey {
    let mut key = [0; 32];
    rng.fill(&mut key);
    for bit in 0..usize::from(scope.len) {
        let mask = 0x80 >> (bit % 8);
        key[bit / 8] = (key[bit / 8] & !mask) | (scope.prefix.0[bit / 8] & mask);
    }
    MapKey(key)
}

fn assert_roundtrip(map: &Map) {
    assert_same(&Map::read(&mut bytes(map).as_slice()).unwrap(), map);
    let mut unhash = Vec::new();
    map.write_no_hash(&mut unhash).unwrap();
    assert_same(&Map::read_no_hash(&mut unhash.as_slice()).unwrap(), map);
}

fn assert_partition_reassembly(map: &Map) {
    for target in [1, 1000, u64::MAX] {
        let parts = map.partition_by_capacity(target, |cap| Some(cap.0)).unwrap();
        if map.root.is_empty() {
            assert!(parts.is_empty());
            continue;
        }
        assert_eq!(parts.iter().map(|part| part.capacity).sum::<u64>(), map.root.augmentation().0);
        for part in &parts {
            assert_same(&part.map, &rebuild(map.root_path, &entries(&part.map)));
            assert_roundtrip(&part.map);
        }
        let maps: Vec<_> = parts.into_iter().rev().map(|part| part.map).collect();
        assert_same(&Map::merge_disjoint_batch(&maps).unwrap(), map);
    }
}

/// A vector oracle deliberately does not use any trie operation to decide which
/// keys belong to a scope or what a mutation's final contents should be.
fn mutate_scoped(
    map: &Map,
    oracle: &mut Vec<(MapKey, Value, Capacity)>,
    rng: &mut rand::rngs::StdRng,
    excluded: Option<MapKeyPath>,
) -> Map {
    let mut updates = Vec::new();
    for _ in 0..8 {
        let key = if !oracle.is_empty() && rng.gen_bool(0.6) {
            oracle[rng.gen_range(0..oracle.len())].0
        } else {
            random_key_in_scope(rng, map.root_path)
        };
        if excluded.is_some_and(|scope| matches(scope, &key)) {
            continue;
        }
        let value =
            rng.gen_bool(0.6).then(|| (Value(rng.gen()), Capacity(rng.gen_range(0..=1000))));
        oracle.retain(|(existing, _, _)| *existing != key);
        if let Some((value, capacity)) = value {
            oracle.push((key, value, capacity));
        }
        updates.push((key, value));
    }
    let mut single = map.clone();
    for (key, value) in &updates {
        single = single.update_single_aug(key, *value);
    }
    let batch = map.update_aug(&updates);
    let expected = rebuild(map.root_path, oracle);
    assert_same(&single, &expected);
    assert_same(&batch, &expected);
    single
}

fn recursive_split_mutate(
    map: &Map,
    oracle: &mut Vec<(MapKey, Value, Capacity)>,
    rng: &mut rand::rngs::StdRng,
    depth: usize,
) -> Map {
    let before = bytes(map);
    let mut map2 = mutate_scoped(map, oracle, rng, None);
    if depth != 0 {
        let prefix = if !oracle.is_empty() && rng.gen_bool(0.8) {
            oracle[rng.gen_range(0..oracle.len())].0
        } else {
            random_key_in_scope(rng, map.root_path)
        };
        // Include ancestors, equal scopes, non-nibble boundaries and full keys.
        let len = if rng.gen_bool(0.8) {
            rng.gen_range(map.root_path.len..=256)
        } else {
            rng.gen_range(0..=256)
        };
        let scope = MapKeyPath { prefix, len };
        let (rest, cut) = map2.split(scope);
        let (mut inside, mut outside): (Vec<_>, Vec<_>) =
            oracle.iter().copied().partition(|(key, _, _)| matches(scope, key));
        assert_same(&cut, &rebuild(scope, &inside));
        assert_same(&rest, &rebuild(map.root_path, &outside));
        assert_roundtrip(&cut);
        assert_roundtrip(&rest);
        // When cut is broader than this view, only mutate within the original
        // view. Separate tests cover restriction of a changed ancestor graft.
        if scope.len >= map.root_path.len {
            let modified_cut = recursive_split_mutate(&cut, &mut inside, rng, depth - 1);
            // The remainder retains the original root_path, but its mutations
            // must avoid the hole owned by the detached subtree.
            let modified_rest = mutate_scoped(&rest, &mut outside, rng, Some(scope));
            outside.extend(inside);
            *oracle = outside;
            let expected = rebuild(map.root_path, oracle);
            map2 = modified_rest.graft(&modified_cut);
            assert_same(&map2, &expected);
            assert_same(&modified_rest.graft_batch(&[modified_cut]), &expected);
        } else {
            assert_same(&rest.graft(&cut), &map2);
        }
    }
    assert_eq!(bytes(map), before);
    assert_same(&map2, &rebuild(map.root_path, oracle));
    assert_partition_reassembly(&map2);
    map2
}

fn check_nested_seed(seed: u64) {
    // The harness captures this on success and exposes the last seed on failure.
    eprintln!("nested split/graft seed: {seed}");
    let mut rng = rand::rngs::StdRng::seed_from_u64(0x5a11_0000 + seed);
    let mut oracle = Vec::new();
    for i in 0..32 {
        let mut key = random_key_in_scope(&mut rng, MapKeyPath::default());
        let shared_bytes = [0, 1, 15, 30, 31][seed as usize % 5];
        key.0[..shared_bytes].fill(0xe8);
        oracle.push((key, Value(i), Capacity(rng.gen_range(0..=1000))));
    }
    // Deduplicate through the independent oracle, retaining the last value.
    oracle.reverse();
    let mut seen = std::collections::HashSet::new();
    oracle.retain(|entry| seen.insert(entry.0));
    let source = rebuild(MapKeyPath::default(), &oracle);
    let result = recursive_split_mutate(&source, &mut oracle, &mut rng, 5);
    assert_roundtrip(&result);
}

#[test]
fn seeded_nested_splits_with_mutations_and_serialized_subtrees() {
    for seed in 0..300 {
        check_nested_seed(seed);
    }
}

#[test]
#[ignore = "extended deterministic split/graft stress; run explicitly in release"]
fn extended_nested_split_graft_stress() {
    for seed in 300..10_300 {
        check_nested_seed(seed);
    }
}

#[test]
fn disjoint_prefix_frontier_grafts_in_any_order() {
    for seed in 0..200 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0xf012_0000 + seed);
        let mut oracle = Vec::new();
        for i in 0..48 {
            let mut key = random_key_in_scope(&mut rng, MapKeyPath::default());
            if seed % 2 == 0 {
                key.0[..30].fill(0xff);
            }
            oracle.push((key, Value(i), Capacity(rng.gen_range(0..=1000))));
        }
        let source = rebuild(MapKeyPath::default(), &oracle);
        let mut frontier = vec![source.clone()];
        // Recursively bisect prefix scopes. Unlike repeated arbitrary cuts,
        // these scopes are disjoint, so any graft order is permitted.
        for _ in 0..280 {
            let nonempty: Vec<_> = frontier
                .iter()
                .enumerate()
                .filter(|(_, map)| !map.root.is_empty() && map.root_path.len < 256)
                .map(|(index, _)| index)
                .collect();
            let index = if !nonempty.is_empty() && rng.gen_bool(0.9) {
                nonempty[rng.gen_range(0..nonempty.len())]
            } else {
                rng.gen_range(0..frontier.len())
            };
            let map = frontier.swap_remove(index);
            if map.root_path.len == 256 {
                frontier.push(map);
                continue;
            }
            let len = map.root_path.len + 1;
            let mut prefix = map.root_path.prefix;
            let bit = usize::from(len - 1);
            prefix.0[bit / 8] &= !(0x80 >> (bit % 8));
            let (one, zero) = map.split(MapKeyPath { prefix, len });
            prefix.0[bit / 8] |= 0x80 >> (bit % 8);
            // Re-scope the remainder to the complementary prefix.
            let (_, one) = one.split(MapKeyPath { prefix, len });
            frontier.extend([zero, one]);
        }
        for part in &frontier {
            let expected: Vec<_> =
                oracle.iter().copied().filter(|(key, _, _)| matches(part.root_path, key)).collect();
            assert_same(part, &rebuild(part.root_path, &expected));
            assert_roundtrip(part);
        }
        for _ in 0..3 {
            frontier.shuffle(&mut rng);
            assert_same(&Map::new().graft_batch(&frontier), &source);
            let combined = frontier.iter().fold(Map::new(), |map, part| map.graft(part));
            assert_same(&combined, &source);
            let rescoped: Vec<_> = frontier.iter().map(|part| Map::new().graft(part)).collect();
            assert_same(&Map::merge_disjoint_batch(&rescoped).unwrap(), &source);
        }
    }
}

#[test]
fn nibble_aligned_subtree_assembly_matches_sequential_graft_and_leaf_rebuild() {
    for seed in 0..250 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0xba7c_0000 + seed);
        let shared = [0, 1, 16, 30, 31][seed as usize % 5];
        let mut oracle = Vec::new();
        for i in 0..32 {
            let mut key = random_key_in_scope(&mut rng, MapKeyPath::default());
            key.0[..shared].fill(0xe8);
            // Guarantee distinct keys even with 31 shared bytes.
            key.0[31] = i;
            oracle.push((key, Value(u64::from(i)), Capacity(rng.gen_range(0..=1000))));
        }
        let source = rebuild(MapKeyPath::default(), &oracle);
        let mut rest = source.clone();
        let mut parts = Vec::new();
        for (key, _, _) in &oracle {
            let mut min_bits = 0;
            for (other, _, _) in &oracle {
                if key == other {
                    continue;
                }
                let common: u16 = key
                    .0
                    .iter()
                    .zip(other.0)
                    .map(|(a, b)| a ^ b)
                    .take_while(|xor| *xor == 0)
                    .count() as u16
                    * 8;
                let first = usize::from(common / 8);
                let shared_bits = common + (key.0[first] ^ other.0[first]).leading_zeros() as u16;
                min_bits = min_bits.max(shared_bits + 1);
            }
            let min_nibbles = min_bits.div_ceil(4);
            let len = rng.gen_range(min_nibbles..=64) * 4;
            let (remaining, part) = rest.split(MapKeyPath { prefix: *key, len });
            rest = remaining;
            assert_eq!(entries(&part).len(), 1);
            parts.push(part);
        }
        assert!(rest.root.is_empty());
        for _ in 0..3 {
            parts.shuffle(&mut rng);
            assert_same(&Map::new().graft_batch(&parts), &source);
            assert_same(&parts.iter().fold(Map::new(), |map, part| map.graft(part)), &source);
        }
    }
}

#[test]
fn exhaustive_small_branch_occupancies_split_and_restore_after_leaf_removal() {
    let suffixes = [0x00, 0x01, 0x0f, 0x10, 0x1f, 0x80, 0xf0, 0xff];
    for byte in [0, 31] {
        for occupied in 0u16..256 {
            let oracle: Vec<_> = suffixes
                .iter()
                .enumerate()
                .filter(|(index, _)| occupied & (1 << index) != 0)
                .map(|(index, suffix)| {
                    let mut key = [0xe8; 32];
                    key[byte] = *suffix;
                    (MapKey(key), Value(index as u64), Capacity(index as u64))
                })
                .collect();
            let source = rebuild(MapKeyPath::default(), &oracle);
            for suffix in suffixes {
                let mut prefix = [0xe8; 32];
                prefix[byte] = suffix;
                for local_bits in 0..=8 {
                    let scope =
                        MapKeyPath { prefix: MapKey(prefix), len: (byte * 8 + local_bits) as u16 };
                    let (rest, cut) = source.split(scope);
                    let (inside, outside): (Vec<_>, Vec<_>) =
                        oracle.iter().copied().partition(|(key, _, _)| matches(scope, key));
                    assert_same(&rest, &rebuild(source.root_path, &outside));
                    assert_same(&cut, &rebuild(scope, &inside));
                    assert_same(&rest.graft(&cut), &source);
                    if let Some((key, _, _)) = inside.first() {
                        let modified_cut = cut.update_single_aug(key, None);
                        let expected: Vec<_> = oracle
                            .iter()
                            .copied()
                            .filter(|(candidate, _, _)| candidate != key)
                            .collect();
                        assert_same(
                            &rest.graft(&modified_cut),
                            &rebuild(source.root_path, &expected),
                        );
                    }
                }
            }
        }
    }
}
