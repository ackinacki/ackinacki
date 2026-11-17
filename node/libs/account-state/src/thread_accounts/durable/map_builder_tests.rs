use node_types::AccountHash;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;
use rand::seq::SliceRandom;
use rand::SeedableRng;

use super::*;

fn entry(i: u64) -> (AccountRouting, AccountInfo, AccountCapacity) {
    let account = *blake3::hash(&i.to_le_bytes()).as_bytes();
    let route = AccountRouting::new(
        DAppIdentifier::new([(i % 7) as u8; 32]),
        AccountIdentifier::new(account),
    );
    let info = if i.is_multiple_of(2) {
        AccountInfo::Redirect(DAppIdentifier::new([42; 32]))
    } else {
        AccountInfo::AccountHash(AccountHash::new(account))
    };
    (route, info, AccountCapacity::new(i + 1))
}

fn bytes(map: &ThreadAccountMap) -> Vec<u8> {
    let mut bytes = Vec::new();
    map.write(&mut bytes).unwrap();
    bytes
}

#[test]
fn map_builder_matches_full_map_for_arbitrary_order_and_window_boundaries() -> anyhow::Result<()> {
    let mut entries: Vec<_> = (0..1539).map(entry).collect();
    let mut expected = ThreadAccountMap::new();
    for (route, info, capacity) in &entries {
        expected = expected.update_single(route, Some((*info, *capacity)));
    }
    for shuffled in [false, true] {
        if shuffled {
            entries.shuffle(&mut rand::rngs::StdRng::seed_from_u64(42));
        } else {
            entries.sort_unstable_by_key(|entry| entry.0);
        }
        for size in [1, 7, 128, 500, 1000, 5000] {
            let mut builder = MapBuilder::new(Default::default(), size)?;
            for (route, info, capacity) in &entries {
                builder.push(*route, *info, *capacity)?;
                assert!(builder.window.len() < size);
                if !shuffled {
                    assert!(builder.sorted);
                }
            }
            let built = builder.finish()?;
            assert_eq!(bytes(&built), bytes(&expected), "shuffled={shuffled}, size={size}");
        }
    }
    Ok(())
}

#[test]
fn map_builder_tracks_sort_flag_per_window_and_merges_interleaved_ranges() -> anyhow::Result<()> {
    let mut entries: Vec<_> = (0..4).map(entry).collect();
    entries.sort_unstable_by_key(|entry| entry.0);
    let mut expected = ThreadAccountMap::new();
    let mut builder = MapBuilder::new(Default::default(), 2)?;
    // Both windows are sorted, but neither is an append-only continuation of
    // the other. A single streaming builder would reject the second window.
    for i in [0, 2, 1, 3] {
        let (route, info, capacity) = entries[i];
        builder.push(route, info, capacity)?;
        assert!(builder.sorted);
        expected = expected.update_single(&route, Some((info, capacity)));
    }
    assert_eq!(bytes(&builder.finish()?), bytes(&expected));
    let mut descending = MapBuilder::new(Default::default(), 3)?;
    for i in [2, 1] {
        let (route, info, capacity) = entries[i];
        descending.push(route, info, capacity)?;
    }
    assert!(!descending.sorted);
    descending.flush()?;
    assert!(descending.sorted);
    Ok(())
}

#[test]
fn map_builder_rejects_duplicate_routings_within_and_across_windows() -> anyhow::Result<()> {
    let a = entry(0);
    let b = entry(7); // Same DApp as a: overlaps must be detected at L2.
    let mut within = MapBuilder::new(Default::default(), 2)?;
    within.push(a.0, a.1, a.2)?;
    assert!(within.push(a.0, a.1, a.2).is_err());
    assert!(within.push(b.0, b.1, b.2).is_err());
    assert_eq!(within.window.len(), 2, "errors must not allow unbounded buffer growth");
    let mut across = MapBuilder::new(Default::default(), 2)?;
    across.push(a.0, a.1, a.2)?;
    across.flush()?;
    across.push(b.0, b.1, b.2)?;
    let error = across.push(a.0, a.1, a.2).unwrap_err();
    assert!(error.to_string().contains("duplicate account routing"), "{error:#}");
    assert!(across.flush().is_err());
    assert!(across.finish().is_err(), "partial in-place updates must never be published");
    Ok(())
}

#[test]
fn map_builder_preserves_partial_scope_and_rejects_invalid_configuration() -> anyhow::Result<()> {
    let scope = MapKeyPath { prefix: multi_map::MapKey([0; 32]), len: 9 };
    let mut builder = MapBuilder::new(scope, 10)?;
    let (route, info, capacity) = entry(0);
    builder.push(route, info, capacity)?;
    let outside = entry(1);
    assert!(builder.push(outside.0, outside.1, outside.2).is_err());
    let map = builder.finish()?;
    assert_eq!(map.dapps.root_path, scope);
    assert_eq!(map.get(&route), Some(info));
    assert_eq!(map.capacity(), capacity);
    assert!(MapBuilder::new(scope, 0).is_err());
    assert!(MapBuilder::new(MapKeyPath { len: 257, ..scope }, 10).is_err());
    assert!(MapBuilder::new(scope, 10)?.finish()?.is_empty());
    Ok(())
}

#[test]
fn map_builder_merges_shared_dapp_in_partial_and_leaf_scopes() -> anyhow::Result<()> {
    // At 256 bits the L1 root is already a leaf, so collision handling must
    // still merge its L2 accounts rather than rejecting the whole DApp.
    for len in [1, 9, 255, 256] {
        let scope = MapKeyPath { prefix: multi_map::MapKey([0; 32]), len };
        let mut builder = MapBuilder::new(scope, 3)?;
        let mut expected =
            ThreadAccountMap { dapps: MultiMapAug { root_path: scope, ..MultiMapAug::new() } };
        for i in (0..32).rev() {
            let (route, info, capacity) = entry(i);
            let route = AccountRouting::new(DAppIdentifier::new([0; 32]), *route.account_id());
            builder.push(route, info, capacity)?;
            expected = expected.update_single(&route, Some((info, capacity)));
        }
        assert_eq!(bytes(&builder.finish()?), bytes(&expected), "scope bits={len}");
    }
    Ok(())
}

#[test]
fn map_builder_reuses_existing_extension_and_branch_allocations() -> anyhow::Result<()> {
    use std::sync::Arc;

    use multi_map::node::NodeAug;

    let mut builder = MapBuilder::new(Default::default(), 10)?;
    let dapp = DAppIdentifier::new([0; 32]);
    let info = AccountInfo::Redirect(dapp);
    let route = |last| {
        let mut id = [0; 32];
        id[31] = last;
        AccountRouting::new(dapp, AccountIdentifier::new(id))
    };
    builder.push(route(0), info, AccountCapacity::new(1))?;
    builder.push(route(1), info, AccountCapacity::new(1))?;
    builder.flush()?;
    let addresses = |map: &ThreadAccountMap| {
        let NodeAug::Ext(root) = &map.dapps.root else { panic!("L1 extension") };
        // This temporary lookup clones L2's Arc. It is dropped before the next
        // insertion, leaving the builder with exclusive ownership again.
        let value = map.dapps.get(&multi_map::MapKey([0; 32])).unwrap();
        let NodeAug::Ext(ext) = &value.0.root else { panic!("L2 extension") };
        let NodeAug::Branch(branch) = &ext.child else { panic!("L2 branch") };
        (Arc::as_ptr(root) as usize, Arc::as_ptr(ext) as usize, Arc::as_ptr(branch) as usize)
    };
    let before = addresses(&builder.map);
    builder.push(route(2), info, AccountCapacity::new(1))?;
    builder.flush()?;
    assert_eq!(addresses(&builder.map), before);
    assert_eq!(builder.finish()?.iter().count(), 3);
    Ok(())
}

#[test]
#[allow(clippy::disallowed_methods)] // Test-only: explicitly verify behavior after unwinding.
fn map_builder_cannot_publish_after_caught_in_place_update_panic() -> anyhow::Result<()> {
    let dapp = DAppIdentifier::new([0; 32]);
    let info = AccountInfo::Redirect(dapp);
    let a = AccountRouting::new(dapp, AccountIdentifier::new([0; 32]));
    let b = AccountRouting::new(dapp, AccountIdentifier::new([1; 32]));
    let mut builder = MapBuilder::new(Default::default(), 10)?;
    builder.push(a, info, AccountCapacity::new(u64::MAX))?;
    builder.flush()?;
    builder.push(b, info, AccountCapacity::new(1))?;
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| builder.flush()));
    assert!(failed.is_err());
    assert!(builder.finish().is_err(), "unrefreshed hashes must not escape a poisoned builder");
    Ok(())
}

#[test]
fn map_builder_matches_canonical_map_for_every_scope_bit_length() -> anyhow::Result<()> {
    // Retain the old prototypes' boundary coverage on the builder we actually
    // keep. DApp prefixes may end inside a nibble or directly at an L1 leaf.
    for len in 0..=256 {
        let scope = MapKeyPath { prefix: multi_map::MapKey([0xa5; 32]), len };
        let mut builder = MapBuilder::new(scope, 7)?;
        let mut expected =
            ThreadAccountMap { dapps: MultiMapAug { root_path: scope, ..MultiMapAug::new() } };
        for i in 0u64..80 {
            let mut dapp = *blake3::hash(&i.to_le_bytes()).as_bytes();
            for bit in 0..usize::from(len) {
                let mask = 0x80 >> (bit % 8);
                dapp[bit / 8] = (dapp[bit / 8] & !mask) | (scope.prefix.0[bit / 8] & mask);
            }
            let (route, info, capacity) = entry(i);
            let route = AccountRouting::new(DAppIdentifier::new(dapp), *route.account_id());
            builder.push(route, info, capacity)?;
            expected = expected.update_single(&route, Some((info, capacity)));
        }
        assert_eq!(bytes(&builder.finish()?), bytes(&expected), "scope bits={len}");
    }
    Ok(())
}
