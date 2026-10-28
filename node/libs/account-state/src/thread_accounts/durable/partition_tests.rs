use node_types::AccountHash;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::DAppIdentifier;

use super::*;

fn routing(dapp: u8, account: u8) -> AccountRouting {
    AccountRouting::new(DAppIdentifier::new([dapp; 32]), AccountIdentifier::new([account; 32]))
}

fn info(seed: u64) -> AccountInfo {
    AccountInfo::AccountHash(AccountHash::new(*blake3::hash(&seed.to_le_bytes()).as_bytes()))
}

fn fixture() -> ThreadAccountMap {
    let updates: Vec<_> = [1u8, 127, 128, 255]
        .into_iter()
        .flat_map(|dapp| {
            (0..if dapp == 1 { 33 } else { 3 }).map(move |account| {
                let value = if account == 0 {
                    AccountInfo::Redirect(DAppIdentifier::new([42; 32]))
                } else {
                    info(1000 * dapp as u64 + account as u64)
                };
                (
                    routing(dapp, account),
                    Some((value, AccountCapacity::new(1024 + 37 * account as u64))),
                )
            })
        })
        .collect();
    (ThreadAccountMap::new()).update(&updates)
}

fn bytes(map: &ThreadAccountMap) -> Vec<u8> {
    let mut bytes = Vec::new();
    map.write(&mut bytes).unwrap();
    bytes
}

fn check_round_trip(source: &ThreadAccountMap, target: u64) -> Vec<AccountPartition> {
    let before = bytes(source);
    let parts = source.partition_by_capacity(target).unwrap();
    let source_entries: Vec<_> = source.iter().map(|(key, value)| (key, *value)).collect();
    let mut entries = Vec::new();
    for part in &parts {
        assert_eq!(part.map.dapps.root_path, source.dapps.root_path);
        assert_eq!(part.capacity, part.map.capacity().bytes());
        let restored = ThreadAccountMap::read(&mut bytes(&part.map).as_slice()).unwrap();
        assert_eq!(restored.hash(), part.map.hash());
        let mut rebuilt = MultiMapAug { root_path: source.dapps.root_path, ..MultiMapAug::new() };
        for (key, dapp) in part.map.dapps.iter() {
            let mut inner = MultiMapAug { root_path: dapp.0.root_path, ..MultiMapAug::new() };
            for (account, info) in dapp.0.iter() {
                let (_, capacity) = dapp.0.get_aug(&account).unwrap();
                inner = inner.update_single_aug(&account, Some((*info, capacity)));
            }
            let capacity = inner.root.augmentation();
            rebuilt = rebuilt.update_single_aug(&key, Some((L1MapValue::new(inner), capacity)));
        }
        assert_eq!(rebuilt.hash().0, *part.map.hash().as_array());
        entries.extend(part.map.iter().map(|(key, value)| (key, *value)));
    }
    assert_eq!(entries, source_entries);
    assert_eq!(parts.iter().map(|part| part.capacity).sum::<u64>(), source.capacity().bytes());
    let merged = ThreadAccountMap::merge_partitions(&parts).unwrap();
    assert_eq!(merged.hash(), source.hash());
    if !source.dapps.root.is_empty() {
        assert_eq!(bytes(&merged), before);
    }
    let reverse: Vec<_> = parts.iter().rev().cloned().collect();
    assert_eq!((ThreadAccountMap::merge_partitions(&reverse).unwrap()).hash(), source.hash());
    assert_eq!(bytes(source), before);
    parts
}

#[test]
fn sparse_partitions_reconstruct_from_records_with_long_shared_prefixes() {
    let updates: Vec<_> = (0..4)
        .flat_map(|dapp| {
            (1..=9).map(move |account| {
                (routing(dapp, account), Some((info(account as u64), AccountCapacity::new(73))))
            })
        })
        .collect();
    check_round_trip(&ThreadAccountMap::new().update(&updates), 256);
}

#[test]
fn partitions_cut_inside_dapp_and_continue_across_the_next_dapp() {
    let updates: Vec<_> = (1..=2)
        .flat_map(|dapp| {
            (0..if dapp == 1 { 24 } else { 6 }).map(move |account| {
                (routing(dapp, account), Some((info(account as u64), AccountCapacity::new(1000))))
            })
        })
        .collect();
    let source = (ThreadAccountMap::new()).update(&updates);
    let parts = check_round_trip(&source, 10_000);
    assert_eq!(parts.iter().map(|part| part.capacity).collect::<Vec<_>>(), vec![10_000; 3]);
    assert!((parts[0].map).get(&routing(1, 9)).is_some());
    assert!((parts[1].map).get(&routing(1, 10)).is_some());
    assert!((parts[2].map).get(&routing(1, 23)).is_some());
    assert!((parts[2].map).get(&routing(2, 0)).is_some());

    for part in &parts {
        for (key, dapp) in part.map.dapps.iter() {
            let original =
                multi_map::ops::get_leaf(&source.dapps.root, &source.dapps.root_path, &key)
                    .unwrap();
            for (account, _) in dapp.0.iter() {
                let a = multi_map::ops::get_leaf(
                    &original.value.0.root,
                    &original.value.0.root_path,
                    &account,
                )
                .unwrap();
                let b =
                    multi_map::ops::get_leaf(&dapp.0.root, &dapp.0.root_path, &account).unwrap();
                assert!(std::ptr::eq(a, b));
            }
        }
    }
}

#[test]
fn canonical_partitions_ignore_update_history_and_survive_prefix_views() {
    let source = fixture();
    let mut rebuilt = ThreadAccountMap::new();
    let mut entries: Vec<_> = source
        .iter()
        .map(|(key, value)| (key, *value, source.account_capacity(&key).unwrap()))
        .collect();
    entries.reverse();
    for (key, value, capacity) in entries {
        rebuilt = rebuilt.update(&[(key, Some((info(9999), AccountCapacity::new(1))))]);
        rebuilt = rebuilt.update(&[(key, None)]);
        rebuilt = rebuilt.update(&[(key, Some((value, capacity)))]);
    }
    for target in [1, 4096, 16_384, u64::MAX] {
        let expected = check_round_trip(&source, target);
        let actual = rebuilt.partition_by_capacity(target).unwrap();
        assert_eq!(expected.len(), actual.len());
        for (a, b) in expected.iter().zip(&actual) {
            assert_eq!(a.capacity, b.capacity);
            assert_eq!(bytes(&a.map), bytes(&b.map));
        }
    }

    for bits in [0, 1, 4, 7, 8, 128, 255, 256] {
        let (rest, part) = source.dapps.split(MapKeyPath { prefix: MapKey([1; 32]), len: bits });
        let rest = ThreadAccountMap { dapps: rest };
        let part = ThreadAccountMap { dapps: part };
        check_round_trip(&rest, 4096);
        check_round_trip(&part, 4096);
    }
}

#[test]
fn partial_l2_scope_is_preserved() {
    let source = fixture();
    let dapp = source.dapps.get(&MapKey([1; 32])).unwrap();
    let (_, part) = dapp.0.split(MapKeyPath { prefix: MapKey([7; 32]), len: 7 });
    let capacity = part.root.augmentation();
    let scoped = ThreadAccountMap {
        dapps: MultiMapAug::new()
            .update_single_aug(&MapKey([1; 32]), Some((L1MapValue::new(part), capacity))),
    };
    check_round_trip(&scoped, 2048);
    assert_eq!(scoped.partition_by_capacity(u64::MAX).unwrap().len(), 1);
}

#[test]
fn fill_threshold_oversized_leaf_and_tiny_tail_follow_the_protocol_rules() {
    for (weights, expected) in [
        (vec![0, 0], vec![0]),
        (vec![89, 12], vec![101]),
        (vec![90, 12], vec![90, 12]),
        (vec![100, 9], vec![109]),
        (vec![100, 10], vec![100, 10]),
        (vec![200, 200, 9], vec![200, 209]),
        (vec![u64::MAX], vec![u64::MAX]),
    ] {
        let updates: Vec<_> = weights
            .into_iter()
            .enumerate()
            .map(|(i, weight)| {
                (routing(1, i as u8), Some((info(i as u64), AccountCapacity::new(weight))))
            })
            .collect();
        let source = (ThreadAccountMap::new()).update(&updates);
        let parts = check_round_trip(&source, 100);
        assert_eq!(parts.iter().map(|part| part.capacity).collect::<Vec<_>>(), expected);
    }
    assert!((fixture()).partition_by_capacity(0).is_err());
    assert!(check_round_trip(&ThreadAccountMap::new(), 100).is_empty());
}

#[test]
fn checked_partition_merge_rejects_overlap_wrong_scope_and_wrong_capacity() {
    let parts = check_round_trip(&fixture(), 4096);
    assert!(matches!(
        ThreadAccountMap::merge_partitions(&[parts[0].clone(), parts[0].clone()]),
        Err(DisjointMergeError::OverlappingLeaf)
    ));
    let mut wrong = parts[0].clone();
    wrong.capacity += 1;
    assert!(matches!(
        ThreadAccountMap::merge_partitions(&[wrong]),
        Err(DisjointMergeError::MalformedTree)
    ));
    let mut wrong = parts[0].clone();
    wrong.map.dapps.root_path.len = 1;
    assert!(matches!(
        ThreadAccountMap::merge_partitions(&[parts[0].clone(), wrong]),
        Err(DisjointMergeError::IncompatibleRootPath)
    ));
}

fn long_routing(dapp: u8, account: u8) -> AccountRouting {
    let mut dapp_bytes = [0xe8; 32];
    dapp_bytes[31] = dapp;
    let mut account_bytes = [0xf8; 32];
    account_bytes[31] = account;
    AccountRouting::new(DAppIdentifier::new(dapp_bytes), AccountIdentifier::new(account_bytes))
}

fn long_prefix_entries() -> Vec<(AccountRouting, AccountInfo, AccountCapacity)> {
    [1u8, 33, 2, 17, 1, 5]
        .into_iter()
        .enumerate()
        .flat_map(|(dapp, count)| {
            (0..count).map(move |account| {
                (
                    long_routing(dapp as u8, account),
                    info(1000 * dapp as u64 + u64::from(account)),
                    AccountCapacity::new(50 + u64::from(account)),
                )
            })
        })
        .collect()
}

fn rebuilt(entries: &[(AccountRouting, AccountInfo, AccountCapacity)]) -> ThreadAccountMap {
    ThreadAccountMap::new().update(
        &entries
            .iter()
            .map(|(key, value, capacity)| (*key, Some((*value, *capacity))))
            .collect::<Vec<_>>(),
    )
}

fn same_map(actual: &ThreadAccountMap, expected: &ThreadAccountMap) {
    assert_eq!(actual.iter().collect::<Vec<_>>(), expected.iter().collect::<Vec<_>>());
    assert_eq!(actual.capacity(), expected.capacity());
    assert_eq!(actual.hash(), expected.hash());
    assert_eq!(bytes(actual), bytes(expected));
    for (key, _) in expected.iter() {
        assert_eq!(actual.account_capacity(&key), expected.account_capacity(&key));
    }
    let mut compact = Vec::new();
    actual.write_no_hash(&mut compact).unwrap();
    assert_eq!(
        bytes(&ThreadAccountMap::read_no_hash(&mut compact.as_slice()).unwrap()),
        bytes(expected)
    );
}

fn prefix_matches(prefix: &[u8; 32], key: &[u8; 32], bits: u16) -> bool {
    (0..usize::from(bits)).all(|bit| {
        let mask = 0x80 >> (bit % 8);
        prefix[bit / 8] & mask == key[bit / 8] & mask
    })
}

#[test]
fn l1_split_remove_last_dapp_account_refill_and_graft_at_every_bit() {
    let entries = long_prefix_entries();
    let source = rebuilt(&entries);
    let before = bytes(&source);
    for bits in 0..=256 {
        let selected = long_routing(1, 0);
        let scope = MapKeyPath { prefix: MapKey(*selected.dapp_id().as_array()), len: bits };
        let (rest, cut) = source.dapps.split(scope);
        let mut rest = ThreadAccountMap { dapps: rest };
        let mut cut = ThreadAccountMap { dapps: cut };
        let (mut inside, mut outside): (Vec<_>, Vec<_>) =
            entries.iter().copied().partition(|(key, _, _)| {
                prefix_matches(&scope.prefix.0, key.dapp_id().as_array(), bits)
            });
        // Remove an entire dApp, not merely an L2 leaf; then recreate it in
        // the detached scope while its old versions remain alive in source.
        let removed_dapp = *inside[0].0.dapp_id();
        let removals: Vec<_> = inside
            .iter()
            .filter(|(key, _, _)| *key.dapp_id() == removed_dapp)
            .map(|(key, _, _)| (*key, None))
            .collect();
        cut = cut.update(&removals);
        inside.retain(|(key, _, _)| *key.dapp_id() != removed_dapp);
        assert!(cut.dapp_capacity(&removed_dapp).is_none());
        let replacement = AccountRouting::new(removed_dapp, *long_routing(1, 250).account_id());
        let entry = (replacement, info(9000), AccountCapacity::new(900));
        cut = cut.update(&[(entry.0, Some((entry.1, entry.2)))]);
        inside.push(entry);
        if let Some((key, _, _)) = outside.pop() {
            rest = rest.update(&[(key, None)]);
        }
        let mut expected_entries = outside;
        expected_entries.extend(inside);
        let expected = rebuilt(&expected_entries);
        // Re-read the cut before grafting: no reliance on shared Arc nodes.
        let cut = ThreadAccountMap::read(&mut bytes(&cut).as_slice()).unwrap();
        let result = rest.graft(&cut);
        same_map(&result, &expected);
        same_map(&rest.graft_batch(&[cut]), &expected);
        check_round_trip(&result, 256);
        assert_eq!(bytes(&source), before);
    }
}

#[test]
fn l2_split_mutate_both_sides_rejoin_and_partition_at_every_bit() {
    let entries = long_prefix_entries();
    let source = rebuilt(&entries);
    let before = bytes(&source);
    let selected = long_routing(1, 16);
    let dapp_key = MapKey(*selected.dapp_id().as_array());
    let original_inner = source.dapps.get(&dapp_key).unwrap().0;
    for bits in 0..=256 {
        let scope = MapKeyPath { prefix: MapKey(*selected.account_id().as_array()), len: bits };
        let (mut rest, mut cut) = original_inner.split(scope);
        let mut expected_entries = entries.clone();
        let removed = entries
            .iter()
            .find(|(key, _, _)| {
                key.dapp_id() == selected.dapp_id()
                    && prefix_matches(&scope.prefix.0, key.account_id().as_array(), bits)
            })
            .unwrap()
            .0;
        cut = cut.update_single_aug(&MapKey(*removed.account_id().as_array()), None);
        expected_entries.retain(|(key, _, _)| *key != removed);
        if let Some((key, _, _)) = entries.iter().find(|(key, _, _)| {
            key.dapp_id() == selected.dapp_id()
                && !prefix_matches(&scope.prefix.0, key.account_id().as_array(), bits)
        }) {
            rest = rest.update_single_aug(&MapKey(*key.account_id().as_array()), None);
            expected_entries.retain(|(candidate, _, _)| candidate != key);
        }
        let inner = rest.graft(&cut);
        let batched = rest.graft_batch(&[cut]);
        let mut inner_bytes = Vec::new();
        inner.write(&mut inner_bytes).unwrap();
        let mut batch_bytes = Vec::new();
        batched.write(&mut batch_bytes).unwrap();
        assert_eq!(inner_bytes, batch_bytes);
        let update = (!inner.root.is_empty()).then(|| {
            let capacity = inner.root.augmentation();
            (L1MapValue::new(inner), capacity)
        });
        let result = ThreadAccountMap { dapps: source.dapps.update_single_aug(&dapp_key, update) };
        same_map(&result, &rebuilt(&expected_entries));
        check_round_trip(&result, 256);
        assert_eq!(bytes(&source), before);
    }
}
