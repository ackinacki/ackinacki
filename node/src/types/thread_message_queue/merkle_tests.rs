use multi_map::MapKey;
use node_types::AccountIdentifier;

use super::*;

fn routing(dapp: u8, account: u8) -> AccountRouting {
    AccountIdentifier::new([account; 32]).routing(DAppIdentifier::new([dapp; 32]))
}

fn inbox(lt: u64) -> AccountInbox {
    let header = tvm_block::InternalMessageHeader { created_lt: lt, ..Default::default() };
    let body = Arc::new(WrappedMessage::from_tvm(&tvm_block::Message::with_int_header(header)));
    let mut inbox = AccountInbox::empty();
    inbox.commitment.add(&body.identifier());
    inbox.add_messages(vec![(body.identifier(), body)]).unwrap();
    inbox
}

fn fixture() -> ThreadMessageQueueState {
    let mut state = ThreadMessageQueueState::empty();
    // Unequal DApps; account IDs overlap between DApps.
    let changes = [1u8, 127, 128, 255]
        .into_iter()
        .flat_map(|dapp| {
            (0..if dapp == 1 { 32 } else { 3 }).map(move |account| {
                (routing(dapp, account), Some(Arc::new(inbox(1000 * dapp as u64 + account as u64))))
            })
        })
        .collect();
    state.update_inboxes(changes, &HashMap::new());
    state
}

#[test]
fn roots_depend_on_final_leaves_not_update_history() {
    let state = fixture();
    let mut rebuilt = ThreadMessageQueueState::empty();
    let entries: Vec<_> = state
        .messages
        .values()
        .flat_map(|dapp| dapp.messages.iter())
        .map(|(routing, inbox)| (routing, inbox.as_ref().clone()))
        .collect();
    for (routing, expected) in entries.into_iter().rev() {
        rebuilt.set_inbox(routing, inbox(9999));
        rebuilt.remove_routing(&routing);
        rebuilt.set_inbox(routing, expected);
    }
    assert_eq!(state.root_hash(), rebuilt.root_hash());
    let restored: ThreadMessageQueueState =
        bincode::deserialize(&bincode::serialize(&state).unwrap()).unwrap();
    assert_eq!(state.root_hash(), restored.root_hash());
    assert_eq!(state.destinations(), restored.destinations());
}

#[test]
fn cursors_and_body_placement_are_not_committed() {
    let state = fixture();
    let mut changed = state.clone();
    let id = DAppIdentifier::new([1; 32]);
    let mut dapp = changed.messages.get(&id).unwrap().as_ref().clone();
    dapp.advance_cursor(7);
    changed.messages.insert(id, Arc::new(dapp));
    changed.advance_cursor(2);
    assert_eq!(state.root_hash(), changed.root_hash());

    let route = routing(1, 0);
    let mut compacted = state.account_inbox_by_routing(&route).unwrap().clone();
    let message_id = compacted.tail_sequence()[0].0.clone();
    compacted.messages.set_tail_sequence(Default::default());
    compacted.messages.set_compacted_history(Some(message_id.clone()..=message_id));
    changed.set_inbox(route, compacted);
    assert_eq!(state.root_hash(), changed.root_hash());
}

#[test]
fn each_chain_is_committed_and_previous_states_remain_unchanged() {
    let state = fixture();
    let root = state.root_hash();
    let route = routing(1, 0);
    for added in [false, true] {
        let mut changed = state.clone();
        let mut value = changed.account_inbox_by_routing(&route).unwrap().clone();
        if added {
            value.commitment.added[0] ^= 1;
        } else {
            value.commitment.consumed[0] ^= 1;
        }
        changed.set_inbox(route, value);
        assert_ne!(root, changed.root_hash());
        assert_eq!(state.root_hash(), root);
        // Untouched DApps and inboxes are shared, not cloned wholesale.
        let untouched = routing(1, 1);
        assert!(Arc::ptr_eq(
            state.messages.get(untouched.dapp_id()).unwrap().messages.get(&untouched).unwrap(),
            changed.messages.get(untouched.dapp_id()).unwrap().messages.get(&untouched).unwrap(),
        ));
        let other = DAppIdentifier::new([128; 32]);
        assert!(Arc::ptr_eq(
            state.messages.get(&other).unwrap(),
            changed.messages.get(&other).unwrap()
        ));
    }
}

#[test]
fn dapp_and_account_split_graft_recover_original_root_and_anchors() {
    let state = fixture();
    let id = DAppIdentifier::new([1; 32]);
    for bits in [0, 1, 4, 7, 8, 128, 256] {
        let dapp_prefix = MapKeyPath { prefix: MapKey([1; 32]), len: bits };
        let (rest, part) = state.split_dapps(dapp_prefix);
        assert_eq!(rest.graft_dapps(&part, dapp_prefix).unwrap().root_hash(), state.root_hash());
        if !part.is_empty() {
            assert!(state.graft_dapps(&part, dapp_prefix).is_err());
        }
        let account_prefix = MapKeyPath { prefix: MapKey([7; 32]), len: bits };
        let (rest, part) = state.split_accounts(id, account_prefix);
        let bytes = bincode::serialize(&part).unwrap();
        let part: ThreadMessageQueueState = bincode::deserialize(&bytes).unwrap();
        let joined = rest.graft_accounts(&part, id, account_prefix).unwrap();
        assert_eq!(joined.root_hash(), state.root_hash());
        assert_eq!(joined.capacity(), state.capacity());
        assert_eq!(rest.capacity().bytes() + part.capacity().bytes(), state.capacity().bytes());
        for (route, value) in state.messages.values().flat_map(|dapp| dapp.messages.iter()) {
            let recovered = joined.account_inbox_by_routing(&route).unwrap();
            assert_eq!(recovered.commitment, value.commitment);
            assert_eq!(recovered.tail_sequence(), value.tail_sequence());
        }
    }
}

#[test]
fn inbox_capacities_propagate_restore_and_remain_outside_hashes() {
    use multi_map::MultiMapAug;
    let state = fixture();

    let mut unaugmented = MultiMapAug::new();
    for (id, dapp) in state.messages.iter() {
        let mut l2 = MultiMapAug::new();
        for (route, inbox) in dapp.messages.iter() {
            l2 = l2.update_single(&MapKey(*route.account_id().as_array()), Some(inbox.clone()));
        }
        assert_eq!(l2.root.hash(), dapp.messages.root_hash().0);
        unaugmented = unaugmented.update_single(&MapKey(*id.as_array()), Some(dapp.clone()));
    }
    assert_eq!(unaugmented.root.hash(), state.root_hash().0);
    let total: u64 = state
        .messages
        .values()
        .map(|dapp| {
            let sum: u64 = dapp
                .messages
                .values()
                .map(|inbox| {
                    inbox
                        .tail_sequence()
                        .iter()
                        .map(|(_, message)| message.canonical_size())
                        .sum::<u64>()
                })
                .sum();
            assert_eq!(dapp.messages.capacity().bytes(), sum);
            sum
        })
        .sum();
    assert_eq!(state.capacity().bytes(), total);
    let restored: ThreadMessageQueueState =
        bincode::deserialize(&bincode::serialize(&state).unwrap()).unwrap();
    assert_eq!(restored.capacity(), state.capacity());
    let route = routing(1, 0);
    let mut changed = state.clone();
    let mut value = changed.account_inbox_by_routing(&route).unwrap().clone();
    value.add_capacity(1).unwrap(); // Forged derived metadata, not a new commitment.
    changed.set_inbox(route, value);
    assert_eq!(changed.root_hash(), state.root_hash());
    assert_eq!(changed.capacity().bytes(), total + 1);
    changed.remove_routing(&route);
    assert_eq!(
        changed.capacity().bytes(),
        total - state.account_inbox_by_routing(&route).unwrap().canonical_size()
    );
    assert_eq!(state.capacity().bytes(), total);
}

#[test]
fn inbox_partitioning_cuts_inside_a_dapp_then_crosses_into_the_next() {
    use multi_map::MultiMapAug;
    let mut state = ThreadMessageQueueState::empty();
    for d in 1..=2 {
        for account in 0..if d == 1 { 24 } else { 6 } {
            state.set_inbox(routing(d, account), inbox(100 * d as u64 + account as u64));
        }
    }
    let unit = inbox(1).canonical_size();
    assert_eq!(state.capacity().bytes(), 30 * unit);
    let partitions = state.partition_by_capacity(10 * unit).unwrap();
    assert_eq!(partitions.iter().map(|p| p.capacity).collect::<Vec<_>>(), vec![10 * unit; 3]);
    assert_eq!(partitions[0].dapps.len(), 1);
    assert_eq!(partitions[1].dapps.len(), 1);
    assert_eq!(partitions[2].dapps.len(), 2);

    let mut rebuilt = ThreadMessageQueueState::empty();
    let mut grouped: std::collections::BTreeMap<_, Vec<_>> = std::collections::BTreeMap::new();
    for partition in &partitions {
        let mut part = ThreadMessageQueueState::empty();
        for (id, map) in &partition.dapps {
            grouped.entry(*id).or_default().push(map.clone());
            for (key, value) in map.iter() {
                let route = AccountIdentifier::new(key.0).routing(*id);
                assert!(rebuilt.account_inbox_by_routing(&route).is_none());
                assert!(Arc::ptr_eq(
                    value,
                    state.messages.get(id).unwrap().messages.get(&route).unwrap()
                ));
                part.set_inbox(route, value.as_ref().clone());
                rebuilt.set_inbox(route, value.as_ref().clone());
            }
        }
        assert_eq!(part.root_hash(), partition.root_hash);
        assert_eq!(part.capacity().bytes(), partition.capacity);
    }
    for (id, maps) in grouped {
        let merged =
            MultiMapAug::merge_disjoint_batch(&maps.into_iter().rev().collect::<Vec<_>>()).unwrap();
        assert_eq!(
            MapHash(merged.root.hash()),
            state.messages.get(&id).unwrap().messages.root_hash()
        );
    }
    assert_eq!(rebuilt.root_hash(), state.root_hash());
    assert_eq!(rebuilt.capacity(), state.capacity());
    let whole = state.partition_by_capacity(100 * unit).unwrap();
    assert_eq!(whole.len(), 1);
    assert_eq!(whole[0].root_hash, state.root_hash());
    assert!(ThreadMessageQueueState::empty().partition_by_capacity(unit).unwrap().is_empty());
    assert!(state.partition_by_capacity(0).is_err());
}

#[test]
fn inbox_partitioning_keeps_oversized_inboxes_whole_and_merges_only_tiny_tail() {
    let unit = inbox(1).canonical_size();
    for (last, expected_len) in [(5, 1), (10, 2)] {
        let mut state = ThreadMessageQueueState::empty();
        for (account, units) in [(0, 100), (1, last)] {
            let mut value = inbox(account as u64);
            value.add_capacity(units * unit - value.canonical_size()).unwrap();
            state.set_inbox(routing(1, account), value);
        }
        let parts = state.partition_by_capacity(100 * unit).unwrap();
        assert_eq!(parts.len(), expected_len);
        assert_eq!(parts.iter().map(|p| p.capacity).sum::<u64>(), (100 + last) * unit);
    }
    let mut state = ThreadMessageQueueState::empty();
    let mut oversized = inbox(1);
    oversized.add_capacity(199 * unit).unwrap();
    state.set_inbox(routing(1, 0), oversized);
    state.set_inbox(routing(1, 1), inbox(2));
    let parts = state.partition_by_capacity(100 * unit).unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].capacity, 201 * unit);
}

#[test]
#[should_panic(expected = "inbox capacity overflow")]
fn queue_capacity_overflow_is_an_invariant_violation() {
    let mut state = ThreadMessageQueueState::empty();
    let mut oversized = inbox(1);
    oversized.add_capacity(u64::MAX - oversized.canonical_size()).unwrap();
    state.set_inbox(routing(1, 0), oversized);
    state.set_inbox(routing(2, 0), inbox(2));
}

#[test]
fn graft_allows_updates_only_in_the_separated_scopes() {
    let state = fixture();
    let id = DAppIdentifier::new([1; 32]);
    let prefix = MapKeyPath { prefix: MapKey([0; 32]), len: 4 };
    let (mut rest, mut part) = state.split_accounts(id, prefix);
    part.set_inbox(routing(1, 2), inbox(42));
    part.remove_routing(&routing(1, 3));
    rest.set_inbox(routing(1, 30), inbox(43));
    let mut expected = state.clone();
    expected.set_inbox(routing(1, 2), inbox(42));
    expected.remove_routing(&routing(1, 3));
    expected.set_inbox(routing(1, 30), inbox(43));
    assert_eq!(rest.graft_accounts(&part, id, prefix).unwrap().root_hash(), expected.root_hash());
    part.set_inbox(routing(1, 31), inbox(44));
    assert!(rest.graft_accounts(&part, id, prefix).is_err());
    let (_, part) = state.split_accounts(id, prefix);
    rest.set_inbox(routing(1, 0), inbox(45));
    assert!(rest.graft_accounts(&part, id, prefix).is_err());
}

#[test]
fn import_rejects_duplicate_or_mixed_dapp_l2_keys() {
    type L2 = maps::QueueMap<AccountRouting, Arc<AccountInbox>>;
    for entries in [
        vec![(routing(1, 0), Arc::new(inbox(1))), (routing(1, 0), Arc::new(inbox(2)))],
        vec![(routing(1, 0), Arc::new(inbox(1))), (routing(2, 1), Arc::new(inbox(2)))],
        vec![(routing(1, 0), Arc::new(inbox(1))), (routing(2, 0), Arc::new(inbox(2)))],
    ] {
        assert!(bincode::deserialize::<L2>(&bincode::serialize(&entries).unwrap()).is_err());
    }
}

#[test]
fn removing_last_inbox_removes_the_dapp_and_resets_to_empty_root() {
    let mut state = fixture();
    state.remove_accounts(state.destinations());
    assert_eq!(state.root_hash(), ThreadMessageQueueState::empty().root_hash());
    assert!(state.messages.is_empty());
    state.set_inbox(routing(1, 0), AccountInbox::empty());
    assert!(state.is_empty());
}

#[test]
fn preview_replaces_bodies_even_when_commitment_does_not_change() {
    let route = routing(1, 0);
    let mut state = ThreadMessageQueueState::empty();
    state.set_inbox(route, inbox(1));
    let old = state.clone();
    let additions = inbox(2).tail_sequence().iter().cloned().collect();
    state.append_messages(&HashMap::from([(route, additions)]));
    assert_eq!(state.root_hash(), old.root_hash());
    assert_eq!(state.account_inbox_by_routing(&route).unwrap().tail_sequence().len(), 2);
    assert_eq!(old.account_inbox_by_routing(&route).unwrap().tail_sequence().len(), 1);
}

#[test]
fn single_key_updates_match_batch_for_replacements_inserts_deletes_and_duplicate_keys() {
    let state = fixture();
    let original = &state.messages.get(&DAppIdentifier::new([1; 32])).unwrap().messages;
    let mut singles = original.clone();
    let mut batch = original.clone();
    let updates = vec![
        (routing(1, 0), Some(Arc::new(inbox(42)))),
        (routing(1, 1), None),
        (routing(1, 200), Some(Arc::new(inbox(43)))),
        (routing(1, 200), None),
        (routing(1, 200), Some(Arc::new(inbox(44)))),
        (routing(1, 250), Some(Arc::new(inbox(45)))),
        (routing(1, 250), None),
        (routing(1, 3), None),
        (routing(1, 3), Some(Arc::new(inbox(46)))),
    ];
    for (key, value) in &updates {
        singles.update_single(*key, value.clone());
    }
    batch.update_batch(updates);
    assert_eq!(singles.root_hash(), batch.root_hash());
    assert_eq!(singles.keys().collect::<Vec<_>>(), batch.keys().collect::<Vec<_>>());
    for (key, value) in singles.iter() {
        let other = batch.get(&key).unwrap();
        assert_eq!(value.commitment, other.commitment);
        assert_eq!(value.tail_sequence(), other.tail_sequence());
    }
    assert_eq!(original.len(), 32);
    assert!(original.get(&routing(1, 200)).is_none());
}

#[test]
fn single_key_build_from_empty_matches_bulk_and_snapshot_rebuild() {
    let entries: Vec<_> = (0u64..256)
        .map(|index| {
            let account = AccountIdentifier::new(*blake3::hash(&index.to_le_bytes()).as_bytes());
            let routing = account.routing(DAppIdentifier::new([1; 32]));
            (routing, Some(Arc::new(inbox(index))))
        })
        .collect();
    let mut batch = maps::QueueMap::new();
    batch.update_batch(entries.clone());
    let mut singles = maps::QueueMap::new();
    for (key, value) in entries.into_iter().rev() {
        singles.update_single(key, value);
    }
    assert_eq!(singles.root_hash(), batch.root_hash());
    assert_eq!(singles.keys().collect::<Vec<_>>(), batch.keys().collect::<Vec<_>>());
    let restored: maps::QueueMap<AccountRouting, Arc<AccountInbox>> =
        bincode::deserialize(&bincode::serialize(&singles).unwrap()).unwrap();
    assert_eq!(restored.root_hash(), batch.root_hash());
    for (key, inbox) in batch.iter() {
        let restored = restored.get(&key).unwrap();
        assert_eq!(restored.commitment, inbox.commitment);
        assert_eq!(restored.tail_sequence(), inbox.tail_sequence());
    }
}

fn walk(state: &ThreadMessageQueueState) -> Vec<MessageIdentifier> {
    use super::account_messages_iterator::AccountMessagesIterator;
    let db = crate::storage::MessageDurableStorage::mem();
    let mut iter = state.iter(&db);
    let mut result = Vec::new();
    for mut inbox in iter.by_ref() {
        result.push(inbox.next().unwrap().unwrap().1);
    }
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
    result
}

#[test]
fn keyed_walk_finishes_each_dapp_before_next_and_wraps_once() {
    let mut state = fixture();
    let id = DAppIdentifier::new([127; 32]);
    state.cursor = Some(MapKey(*id.as_array()));
    let mut dapp = state.messages.get(&id).unwrap().as_ref().clone();
    dapp.cursor = Some(MapKey([1; 32]));
    state.messages.insert(id, Arc::new(dapp));
    let expected_routes: Vec<_> = [127u8, 128, 255, 1]
        .into_iter()
        .flat_map(|d| {
            let accounts = if d == 1 {
                (0..32).collect::<Vec<_>>()
            } else if d == 127 {
                vec![1, 2, 0]
            } else {
                vec![0, 1, 2]
            };
            accounts.into_iter().map(move |a| routing(d, a))
        })
        .collect();
    let expected: Vec<_> = expected_routes
        .iter()
        .map(|route| state.account_inbox_by_routing(route).unwrap().tail_sequence()[0].0.clone())
        .collect();
    assert_eq!(walk(&state), expected);
    let restored: ThreadMessageQueueState =
        bincode::deserialize(&bincode::serialize(&state).unwrap()).unwrap();
    assert_eq!(walk(&restored), expected);
    assert!(walk(&ThreadMessageQueueState::empty()).is_empty());
}

#[test]
fn keyed_walk_matches_legacy_indexed_walk() {
    use account_inbox::iter::iterator::MessagesRangeIterator;

    use super::account_messages_iterator::AccountMessagesIterator;
    use crate::storage::MessageDurableStorage;

    let db = MessageDurableStorage::mem();
    let walk = |queue: &ThreadMessageQueueState| {
        queue.iter(&db).flat_map(|inbox| inbox.map(|item| item.unwrap().1)).collect::<Vec<_>>()
    };
    // Reproduce the old producer's numeric-index traversal independently of the
    // new cyclic Merkle iterator. Unequal DApp sizes catch accidental round-robin
    // traversal between DApps; large offsets exercise cursor wraparound.
    for offset in [0, 1, 2, 3, 31, 37] {
        let mut queue = fixture();
        let ids: Vec<_> = queue.messages.keys().collect();
        for id in ids {
            let mut dapp = queue.messages.get(&id).unwrap().as_ref().clone();
            dapp.advance_cursor(offset);
            queue.messages.insert(id, Arc::new(dapp));
        }
        queue.advance_cursor(offset);
        let old = legacy::Queue::from_current(&queue);
        let mut expected = Vec::new();
        for dapp_offset in 0..old.order_set.len() {
            let id = old.order_set[(old.cursor + dapp_offset) % old.order_set.len()];
            let dapp = &old.messages[&id];
            for account_offset in 0..dapp.order_set.len() {
                let routing = dapp.order_set[(dapp.cursor + account_offset) % dapp.order_set.len()];
                expected.extend(
                    MessagesRangeIterator::new(&db, dapp.messages[&routing].clone())
                        .map(|item| item.unwrap().1),
                );
            }
        }
        assert_eq!(walk(&queue), expected, "cursor offset={offset}");
        let restored = old.into_current(&mut HashMap::new()).unwrap();
        assert_eq!(walk(&restored), expected, "legacy restore, offset={offset}");
    }
}

#[test]
fn keyed_cursor_tracks_anchor_and_advances_by_consumed_count() {
    let mut state = fixture();
    let id = DAppIdentifier::new([127; 32]);
    state.cursor = Some(MapKey(*id.as_array()));
    let mut dapp = state.messages.get(&id).unwrap().as_ref().clone();
    dapp.cursor = Some(MapKey([1; 32]));
    // Consumption need not be at the cursor: rotate by count, not last consumed key.
    dapp.advance_cursor(1);
    assert_eq!(dapp.cursor(), Some(MapKey([2; 32])));
    dapp.advance_cursor(7);
    assert_eq!(dapp.cursor(), Some(MapKey([0; 32])));
    dapp.cursor = Some(MapKey([1; 32]));
    state.messages.insert(id, Arc::new(dapp));
    state.remove_routing(&routing(127, 0));
    assert_eq!(state.messages.get(&id).unwrap().cursor(), Some(MapKey([1; 32])));
    state.remove_routing(&routing(127, 1));
    assert_eq!(state.messages.get(&id).unwrap().cursor(), Some(MapKey([2; 32])));
    state.remove_routing(&routing(127, 2));
    assert_eq!(state.cursor(), Some(MapKey([128; 32])));
    state.advance_cursor(2);
    assert_eq!(state.cursor(), Some(MapKey([1; 32])));
    // A missing anchor at the end wraps to the first key.
    let mut last = state.messages.get(&DAppIdentifier::new([255; 32])).unwrap().as_ref().clone();
    last.cursor = Some(MapKey([255; 32]));
    last.normalize_cursor();
    assert_eq!(last.cursor(), Some(MapKey([0; 32])));
}

#[test]
fn index_free_map_reconstructs_routing_and_derived_length() {
    let mut map = maps::QueueMap::new();
    let route = routing(1, 2);
    map.update_batch(vec![(route, Some(Arc::new(inbox(1)))), (route, None)]);
    assert!(map.is_empty());
    assert!(map.keys().next().is_none());
    map.update_single(route, Some(Arc::new(inbox(1))));
    assert_eq!(map.keys().collect::<Vec<_>>(), vec![route]);
    assert!(map.get(&routing(2, 2)).is_none());
    map.update_single(routing(2, 2), None);
    map.update_batch(vec![(routing(2, 2), None)]);
    assert!(map.get(&route).is_some());
    map.update_single(route, Some(Arc::new(inbox(2))));
    assert_eq!(map.len(), 1);
    map.update_single(routing(1, 3), None);
    assert_eq!(map.len(), 1);
    map.update_single(route, None);
    assert!(map.is_empty());
    let another = routing(2, 2);
    map.update_single(another, Some(Arc::new(inbox(3))));
    assert_eq!(map.keys().collect::<Vec<_>>(), vec![another]);
    assert_eq!(map.len(), 1);
}

#[test]
fn merkle_seek_matches_sorted_reference_for_missing_keys_and_prefix_views() {
    use multi_map::MultiMapAug;

    use super::seek::CyclicIter;
    use super::seek::SeekIter;

    let mut map = MultiMapAug::new();
    let value = Arc::new(AccountInbox::empty());
    let keys: Vec<_> =
        (0..64u64).map(|i| MapKey(*blake3::hash(&i.to_le_bytes()).as_bytes())).collect();
    for key in &keys {
        map = map.update_single(key, Some(value.clone()));
    }
    map = map.update_single(&keys[7], None);
    let probes: Vec<_> = keys.iter().copied().chain([MapKey([0; 32]), MapKey([255; 32])]).collect();
    for bits in [0, 1, 4, 7, 8, 129, 255, 256] {
        let (_, part) = map.split(MapKeyPath { prefix: keys[31], len: bits });
        for source in [&map, &part, &MultiMapAug::new()] {
            let all: Vec<_> = source.iter().map(|(key, _)| key).collect();
            for start in &probes {
                let expected: Vec<_> = all.iter().copied().filter(|key| key >= start).collect();
                assert_eq!(
                    SeekIter::new(source, Some(*start)).map(|(key, _)| key).collect::<Vec<_>>(),
                    expected
                );
                let expected: Vec<_> = all
                    .iter()
                    .copied()
                    .filter(|key| key >= start)
                    .chain(all.iter().copied().filter(|key| key < start))
                    .collect();
                let mut cyclic = CyclicIter::new(source, Some(*start));
                assert_eq!(
                    cyclic.by_ref().take(all.len() + 1).map(|(key, _)| key).collect::<Vec<_>>(),
                    expected
                );
                assert!(cyclic.next().is_none());
                assert!(cyclic.next().is_none());
            }
        }
    }
}
