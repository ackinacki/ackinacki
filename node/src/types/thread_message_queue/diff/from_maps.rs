use std::collections::BTreeMap;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;

use account_inbox::iter::iterator::MessagesRangeIterator;
use account_inbox::storage::DurableStorageRead;
use node_types::AccountRouting;
use node_types::DAppIdentifier;
use tracing::instrument;
use tracing::trace_span;
use typed_builder::TypedBuilder;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::storage::MessageDurableStorage;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AccountInbox;
use crate::types::InboxCommitment;

/// Logical changes only. Applying them requires an explicit base queue and DB.
#[derive(Clone, TypedBuilder)]
#[builder(
    build_method(vis="pub"),
    builder_method(vis="pub"),
    builder_type(vis="pub", name=ThreadMessageQueueStateDiffBuilder),
    field_defaults(setter(prefix="with_")),
)]
pub struct ThreadMessageQueueStateDiff {
    consumed_messages: HashMap<AccountRouting, Vec<MessageIdentifier>>,
    #[builder(setter(into))]
    removed_accounts: Vec<AccountRouting>,
    added_accounts: BTreeMap<AccountRouting, AccountInbox>,
    produced_messages: HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
}

type ChangedInboxes = HashMap<AccountRouting, Option<Arc<AccountInbox>>>;

fn inbox<'a>(
    state: &'a ThreadMessageQueueState,
    changed: &'a ChangedInboxes,
    routing: &AccountRouting,
) -> Option<&'a AccountInbox> {
    match changed.get(routing) {
        Some(inbox) => inbox.as_deref(),
        None => state.account_inbox_by_routing(routing),
    }
}

fn inbox_mut<'a>(
    state: &ThreadMessageQueueState,
    changed: &'a mut ChangedInboxes,
    routing: AccountRouting,
) -> &'a mut AccountInbox {
    let entry = changed.entry(routing).or_insert_with(|| {
        Some(Arc::new(
            state.account_inbox_by_routing(&routing).cloned().unwrap_or_else(AccountInbox::empty),
        ))
    });
    Arc::make_mut(entry.get_or_insert_with(|| Arc::new(AccountInbox::empty())))
}

fn resolve_consumed_routing(
    state: &ThreadMessageQueueState,
    changed: &ChangedInboxes,
    routing: &AccountRouting,
) -> Option<AccountRouting> {
    if inbox(state, changed, routing).is_some() {
        return Some(*routing);
    }
    let mut candidates = state
        .messages
        .values()
        .flat_map(|dapp| dapp.messages.keys())
        .filter(|key| !changed.contains_key(key))
        .chain(changed.iter().filter_map(|(key, inbox)| inbox.as_ref().map(|_| *key)))
        .filter(|key| key.account_id() == routing.account_id());
    let candidate = candidates.next()?;
    candidates.next().is_none().then_some(candidate)
}

fn remove_consumed_produced_overlap(
    consumed: &mut Vec<MessageIdentifier>,
    produced: &mut Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
) -> anyhow::Result<usize> {
    let (Some(first_added), Some(last_consumed)) = (produced.first(), consumed.last()) else {
        return Ok(0);
    };
    let start = consumed.iter().position(|id| id == &first_added.0);
    let end = produced.iter().position(|(id, _)| id == last_consumed);
    match (start, end) {
        (None, None) => Ok(0),
        (Some(start), Some(end)) => {
            let suffix = &consumed[start..];
            let prefix = &produced[..=end];
            anyhow::ensure!(
                suffix.len() == prefix.len() && suffix.iter().zip(prefix).all(|(a, (b, _))| a == b),
                "Consumed suffix does not match produced prefix"
            );
            consumed.truncate(start);
            produced.drain(..=end);
            Ok(end + 1)
        }
        _ => anyhow::bail!("Consumed / produced overlap is not a suffix / prefix"),
    }
}

impl ThreadMessageQueueStateDiff {
    #[instrument(skip_all)]
    pub fn apply(
        self,
        mut state: ThreadMessageQueueState,
        db: &MessageDurableStorage,
    ) -> anyhow::Result<ThreadMessageQueueState> {
        let mut val = self;
        tracing::trace!(
            target: "builder",
            "apply ThreadMessageQueueStateDiff consumed_messages: {:?}",
            val.consumed_messages
        );
        tracing::trace!(
            target: "builder",
            "apply ThreadMessageQueueStateDiff produced_messages: {:?}",
            val.produced_messages
        );

        let mut changed = ChangedInboxes::new();
        let mut consumed_accounts_by_dapp: HashMap<DAppIdentifier, usize> = HashMap::new();

        trace_span!("add accounts").in_scope(|| {
            for (routing, inbox) in val.added_accounts.into_iter() {
                #[cfg(feature = "fail-fast")]
                assert!(state.account_inbox_by_routing(&routing).is_none(), "dirty state detected");
                changed.insert(routing, Some(Arc::new(inbox)));
            }
        });

        // Commit full operation lists before cancelling in-block overlap.
        // Preprocessing leaves commitments untouched until postprocessing.
        let mut commitments: HashMap<AccountRouting, InboxCommitment> = HashMap::new();
        for (routing, messages) in &val.produced_messages {
            let commitment = commitments.entry(*routing).or_insert_with(|| {
                inbox(&state, &changed, routing).map(|inbox| inbox.commitment).unwrap_or_default()
            });
            for (id, _) in messages {
                commitment.add(id);
            }
        }
        for (routing, consumed) in &val.consumed_messages {
            let actual = resolve_consumed_routing(&state, &changed, routing).unwrap_or(*routing);
            let commitment = commitments.entry(actual).or_insert_with(|| {
                inbox(&state, &changed, &actual).map(|inbox| inbox.commitment).unwrap_or_default()
            });
            for id in consumed {
                commitment.consume(id);
            }
        }

        // Remove only a matching consumed suffix / produced prefix. Full
        // lists remain available to the caller before this physical update.
        for (routing, consumed) in &mut val.consumed_messages {
            let produced_routing = if val.produced_messages.contains_key(routing) {
                Some(*routing)
            } else {
                resolve_consumed_routing(&state, &changed, routing)
                    .filter(|route| val.produced_messages.contains_key(route))
            };
            if let Some(produced_routing) = produced_routing {
                let produced = val.produced_messages.get_mut(&produced_routing).unwrap();
                let overlap = remove_consumed_produced_overlap(consumed, produced)?;
                if overlap != 0 {
                    let initial_routing = resolve_consumed_routing(&state, &changed, routing);
                    if let Some(inbox) =
                        initial_routing.and_then(|route| inbox(&state, &changed, &route))
                    {
                        // The normal consumption pass checks every FIFO ID.
                        // Here only require reaching the old tail before consuming
                        // newly added messages; no second DB walk is necessary.
                        let last_old =
                            inbox.tail_sequence().back().map(|(id, _)| id).or_else(|| {
                                inbox.compacted_history().as_ref().map(|range| range.end())
                            });
                        anyhow::ensure!(
                            consumed.last() == last_old,
                            "New messages consumed before the initial inbox was drained"
                        );
                    } else {
                        anyhow::ensure!(
                            consumed.is_empty(),
                            "Consumed old messages have no initial inbox"
                        );
                    }
                }
            }
        }

        trace_span!("add messages", addresses_count = val.produced_messages.len()).in_scope(
            || {
                for (routing, messages) in val.produced_messages.into_iter() {
                    if messages.is_empty() {
                        continue;
                    }

                    let entry = inbox_mut(&state, &mut changed, routing);

                    let added_bytes = messages.iter().try_fold(0u64, |sum, (_, message)| {
                        sum.checked_add(message.canonical_size())
                            .ok_or_else(|| anyhow::anyhow!("Inbox canonical size overflow"))
                    })?;
                    entry.add_capacity(added_bytes)?;

                    let mut tail = entry.tail_sequence().clone();
                    let mut range_ref = entry.compacted_history().clone();

                    tail.extend(
                        messages
                            .iter()
                            .map(|(message, message_key)| (message.clone(), message_key.clone())),
                    );
                    if let Some((message_id, _)) = tail.front() {
                        if let Ok(db_messages) = db.remaining_messages(message_id, tail.len()) {
                            if !db_messages.is_empty() {
                                if let Some(ref mut range) = range_ref {
                                    *range = RangeInclusive::new(
                                        range.start().clone(),
                                        db_messages.last().unwrap().identifier(),
                                    )
                                } else {
                                    range_ref = Some(RangeInclusive::new(
                                        db_messages.first().unwrap().identifier(),
                                        db_messages.last().unwrap().identifier(),
                                    ));
                                }
                            }

                            let db_ids: Vec<_> = db_messages
                                .into_iter()
                                .map(|message| message.identifier())
                                .collect();
                            let mut tail_clone = tail.clone();
                            tail = tail_clone.split_off(db_ids.len());
                            let tail_ids: Vec<_> =
                                tail_clone.into_iter().map(|(id, _)| id).collect();

                            assert_eq!(tail_ids, db_ids);
                        }
                    }
                    entry.messages.set_tail_sequence(tail);
                    entry.messages.set_compacted_history(range_ref);
                }
                Ok::<_, anyhow::Error>(())
            },
        )?;

        trace_span!("remove messages").in_scope(|| {
            for (routing, messages) in val.consumed_messages.into_iter() {
                if messages.is_empty() {
                    continue;
                }
                let actual_routing = resolve_consumed_routing(&state, &changed, &routing)
                    .ok_or_else(|| anyhow::anyhow!("Unexpected consumed message: {routing:?}"))?;
                if actual_routing != routing {
                    tracing::warn!(
                        target: "builder",
                        consumed_routing = ?routing,
                        matched_routing = ?actual_routing,
                        "Consumed message routing mismatched queue routing; falling back to account_id match"
                    );
                }
                let dapp_id = *actual_routing.dapp_id();
                let entry = inbox_mut(&state, &mut changed, actual_routing);

                let mut it = MessagesRangeIterator::new(db, entry.messages.clone());
                let bodies = it.next_range(messages.len())
                    .map_err(|error| anyhow::anyhow!("Reading consumed inbox: {error:?}"))?;
                let mut iterator_ids = Vec::with_capacity(bodies.len());
                let mut consumed_bytes = 0u64;
                for body in bodies {
                    iterator_ids.push(body.identifier());
                    consumed_bytes = consumed_bytes.checked_add(body.canonical_size())
                        .ok_or_else(|| anyhow::anyhow!("Inbox consumed size overflow"))?;
                }
                anyhow::ensure!(messages == iterator_ids, "Consumed messages do not match the FIFO prefix for {actual_routing:?}");
                entry.remove_capacity(consumed_bytes)?;
                entry.messages = it.remaining().clone();

                *consumed_accounts_by_dapp.entry(dapp_id).or_default() += 1;
                if entry.is_empty() {
                    anyhow::ensure!(entry.canonical_size() == 0, "Empty inbox has nonzero canonical size");
                    changed.insert(actual_routing, None);
                }
            }
            Ok::<_, anyhow::Error>(())
        })?;

        trace_span!("remove accounts").in_scope(|| {
            for routing in val.removed_accounts.into_iter() {
                changed.insert(routing, None);
            }
        });

        // Empty inboxes disappear only at the block boundary; a later recreation
        // starts both chains at the seed.
        for (routing, commitment) in commitments {
            if let Some(Some(inbox)) = changed.get_mut(&routing) {
                Arc::make_mut(inbox).commitment = commitment;
            }
        }
        state.update_inboxes(changed, &consumed_accounts_by_dapp);

        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::sync::Arc;

    use node_types::AccountIdentifier;
    use node_types::DAppIdentifier;
    use tvm_block::InternalMessageHeader;
    use tvm_block::Message;

    use super::*;
    use crate::message::identifier::MessageIdentifier;
    use crate::message::WrappedMessage;
    use crate::types::thread_message_queue::DAppMessageQueueState;
    use crate::types::thread_message_queue::ThreadMessageQueueState;
    use crate::types::AccountInbox;

    fn dapp(seed: u8) -> DAppIdentifier {
        DAppIdentifier::new([seed; 32])
    }

    fn account(seed: u8) -> AccountIdentifier {
        AccountIdentifier::new([seed; 32])
    }

    fn routing(dapp_seed: u8, account_seed: u8) -> AccountRouting {
        account(account_seed).routing(dapp(dapp_seed))
    }

    fn create_empty_message() -> (MessageIdentifier, Arc<WrappedMessage>) {
        let msg = Message::with_int_header(InternalMessageHeader::default());
        let wrapped_message = Arc::new(WrappedMessage::from_tvm(&msg));
        let message_id = wrapped_message.identifier();
        (message_id, wrapped_message)
    }

    fn message_with_lt(lt: u64) -> (MessageIdentifier, Arc<WrappedMessage>) {
        let header = InternalMessageHeader { created_lt: lt, ..Default::default() };
        let body = Arc::new(WrappedMessage::from_tvm(&Message::with_int_header(header)));
        (body.identifier(), body)
    }

    fn transition(
        initial: ThreadMessageQueueState,
        route: AccountRouting,
        consumed: Vec<MessageIdentifier>,
        produced: Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    ) -> anyhow::Result<ThreadMessageQueueState> {
        ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(HashMap::from([(route, consumed)]))
            .with_produced_messages(HashMap::from([(route, produced)]))
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .build()
            .apply(initial, &MessageDurableStorage::mem())
    }

    #[test]
    fn capacity_deltas_match_full_fifo_sum_across_many_block_boundaries() {
        let route = routing(1, 10);
        let mut state = ThreadMessageQueueState::empty();
        let mut expected = std::collections::VecDeque::new();
        let mut retained = std::collections::VecDeque::new();
        let mut next_lt = 1;
        for block in 0..200usize {
            let produced: Vec<_> = (0..block % 6)
                .map(|_| {
                    let message = message_with_lt(next_lt);
                    next_lt += 1;
                    message
                })
                .collect();
            expected.extend(produced.clone());
            let count = (block * 7 % 9).min(expected.len());
            let consumed: Vec<_> = expected.drain(..count).map(|(id, _)| id).collect();
            state = transition(state, route, consumed, produced).unwrap();
            let bytes: u64 = expected.iter().map(|(_, message)| message.canonical_size()).sum();
            assert_eq!(state.capacity().bytes(), bytes);
            assert_eq!(
                state.account_inbox_by_routing(&route).map_or(0, AccountInbox::canonical_size),
                bytes
            );
            retained.push_back((state.clone(), bytes));
            if retained.len() > 10 {
                retained.pop_front();
            }
            for (previous, bytes) in &retained {
                assert_eq!(previous.capacity().bytes(), *bytes);
            }
            if block % 13 == 0 {
                state = bincode::deserialize(&bincode::serialize(&state).unwrap()).unwrap();
                assert_eq!(state.capacity().bytes(), bytes);
            }
        }
    }

    #[test]
    fn ordered_overlap_keeps_only_the_unconsumed_suffix() {
        let route = routing(1, 10);
        let old = [message_with_lt(200), message_with_lt(100)];
        let added = [message_with_lt(300), message_with_lt(400), message_with_lt(500)];
        let initial = build_state(HashMap::from([(route, old.to_vec())]));
        let consumed = old.iter().chain(&added[..2]).map(|(id, _)| id.clone()).collect();
        let result = transition(initial.clone(), route, consumed, added.to_vec()).unwrap();
        assert_eq!(
            result.account_inbox_by_routing(&route).unwrap().tail_sequence(),
            &std::collections::VecDeque::from([added[2].clone()])
        );
        assert_eq!(initial.account_inbox_by_routing(&route).unwrap().length(), 2);
    }

    #[test]
    fn building_a_diff_does_not_bind_or_mutate_its_base_queue() {
        let route = routing(1, 10);
        let [a, b, c] = [message_with_lt(1), message_with_lt(2), message_with_lt(3)];
        let base = build_state(HashMap::from([(route, vec![a.clone(), b.clone()])]));
        let before = base.account_inbox_by_routing(&route).unwrap().commitment;
        let diff = ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(HashMap::from([(route, vec![a.0.clone()])]))
            .with_produced_messages(HashMap::from([(route, vec![c.clone()])]))
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .build();
        assert_eq!(base.account_inbox_by_routing(&route).unwrap().commitment, before);
        let db = MessageDurableStorage::mem();
        assert!(diff.clone().apply(ThreadMessageQueueState::empty(), &db).is_err());
        let next = diff.apply(base.clone(), &db).unwrap();
        let inbox = next.account_inbox_by_routing(&route).unwrap();
        assert_eq!(
            inbox.tail_sequence(),
            &std::collections::VecDeque::from([b.clone(), c.clone()])
        );
        inbox.commitment.verify([Ok(b.0), Ok(c.0)]).unwrap();
        assert_eq!(base.account_inbox_by_routing(&route).unwrap().commitment, before);
        assert_eq!(base.length(), 2);
    }

    #[test]
    fn ordered_consumption_rejects_a_permuted_fifo_prefix() {
        let route = routing(1, 10);
        let messages = [message_with_lt(100), message_with_lt(200)];
        let initial = build_state(HashMap::from([(route, messages.to_vec())]));
        assert!(transition(
            initial,
            route,
            vec![messages[1].0.clone(), messages[0].0.clone()],
            vec![]
        )
        .is_err());
    }

    #[test]
    fn ordered_overlap_rejects_gaps_and_permutations() {
        let messages = [message_with_lt(100), message_with_lt(200), message_with_lt(300)];
        for indices in [vec![1], vec![0, 2], vec![0, 2, 1]] {
            let mut consumed = indices.into_iter().map(|i| messages[i].0.clone()).collect();
            assert!(
                remove_consumed_produced_overlap(&mut consumed, &mut messages.to_vec()).is_err()
            );
        }
        let mut consumed = vec![message_with_lt(50).0];
        let mut produced = messages.to_vec();
        assert_eq!(remove_consumed_produced_overlap(&mut consumed, &mut produced).unwrap(), 0);
    }

    #[test]
    fn consuming_added_messages_requires_draining_the_initial_inbox() {
        let route = routing(1, 10);
        let old = message_with_lt(100);
        let new = message_with_lt(200);
        let initial = build_state(HashMap::from([(route, vec![old])]));
        let err = transition(initial, route, vec![new.0.clone()], vec![new]).unwrap_err();
        assert!(err.to_string().contains("before the initial inbox was drained"));
    }

    #[test]
    fn consuming_all_new_messages_removes_the_inbox() {
        let route = routing(1, 10);
        let added = [message_with_lt(100), message_with_lt(200)];
        let result = transition(
            ThreadMessageQueueState::empty(),
            route,
            added.iter().map(|(id, _)| id.clone()).collect(),
            added.to_vec(),
        )
        .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn commitment_keeps_in_block_history_and_resets_only_when_finally_empty() {
        let route = routing(1, 10);
        let [a, b, c] = [message_with_lt(1), message_with_lt(2), message_with_lt(3)];
        let initial = build_state(HashMap::from([(route, vec![a.clone()])]));
        let result =
            transition(initial, route, vec![a.0.clone(), b.0.clone()], vec![b.clone(), c.clone()])
                .unwrap();
        let inbox = result.account_inbox_by_routing(&route).unwrap();
        let mut expected = InboxCommitment::default();
        for message in [&a, &b, &c] {
            expected.add(&message.0);
        }
        expected.consume(&a.0);
        expected.consume(&b.0);
        assert_eq!(inbox.commitment, expected);
        inbox.commitment.verify([Ok(c.0.clone())]).unwrap();
        assert_ne!(inbox.commitment.consumed, InboxCommitment::default().consumed);
        let empty = transition(result, route, vec![c.0.clone()], vec![]).unwrap();
        assert!(empty.is_empty());
        let restarted = transition(empty, route, vec![], vec![c.clone()]).unwrap();
        let direct = build_state(HashMap::from([(route, vec![c.clone()])]));
        assert_eq!(
            restarted.account_inbox_by_routing(&route).unwrap().commitment,
            direct.account_inbox_by_routing(&route).unwrap().commitment
        );
        assert_ne!(restarted.account_inbox_by_routing(&route).unwrap().commitment, expected);
    }

    #[cfg(all(feature = "messages_db", not(feature = "disable_db_for_messages")))]
    #[test]
    fn ordered_consumption_spans_db_history_and_memory_tail() {
        let route = AccountRouting::default();
        let old = [message_with_lt(100), message_with_lt(200)];
        let added = [message_with_lt(300), message_with_lt(400)];
        let db = MessageDurableStorage::mem();
        db.write_messages(HashMap::from([(route, vec![old[0].clone()])])).unwrap();
        let mut inbox = AccountInbox::empty();
        inbox
            .messages
            .set_compacted_history(Some(RangeInclusive::new(old[0].0.clone(), old[0].0.clone())));
        inbox.add_messages(vec![old[1].clone()]).unwrap();
        inbox.rebase(&db).unwrap();
        let mut initial = ThreadMessageQueueState::empty();
        initial.set_inbox(route, inbox);
        let result = ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(HashMap::from([(
                route,
                vec![old[0].0.clone(), old[1].0.clone(), added[0].0.clone()],
            )]))
            .with_produced_messages(HashMap::from([(route, added.to_vec())]))
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .build()
            .apply(initial, &db)
            .unwrap();
        let inbox = result.account_inbox_by_routing(&route).unwrap();
        assert!(inbox.compacted_history().is_none());
        assert_eq!(inbox.tail_sequence(), &std::collections::VecDeque::from([added[1].clone()]));
        assert_eq!(inbox.canonical_size(), added[1].1.canonical_size());
        assert_eq!(result.capacity().bytes(), added[1].1.canonical_size());
    }

    fn inbox_with(message: (MessageIdentifier, Arc<WrappedMessage>)) -> AccountInbox {
        let mut inbox = AccountInbox::empty();
        inbox.commitment.add(&message.0);
        inbox.add_messages(vec![message]).unwrap();
        inbox
    }

    fn build_state(
        produced_messages: HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    ) -> ThreadMessageQueueState {
        ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(HashMap::new())
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(produced_messages)
            .build()
            .apply(ThreadMessageQueueState::empty(), &MessageDurableStorage::mem())
            .unwrap()
    }

    #[test]
    fn test_add_messages_in_two_dapps() {
        let route_a = routing(1, 10);
        let route_b = routing(2, 20);
        let mut produced = HashMap::new();
        produced.insert(route_a, vec![create_empty_message()]);
        produced.insert(route_b, vec![create_empty_message()]);

        let state = build_state(produced);

        assert_eq!(state.messages.len(), 2);
        assert!(state.account_inbox_by_routing(&route_a).is_some());
        assert!(state.account_inbox_by_routing(&route_b).is_some());
        assert_eq!(state.length(), 2);
    }

    #[test]
    fn test_add_two_accounts_inside_one_dapp() {
        let route_a = routing(1, 10);
        let route_b = routing(1, 20);
        let mut produced = HashMap::new();
        produced.insert(route_a, vec![create_empty_message()]);
        produced.insert(route_b, vec![create_empty_message()]);

        let state = build_state(produced);
        let dapp_queue = state.messages.get(route_a.dapp_id()).unwrap();

        assert_eq!(state.messages.len(), 1);
        assert_eq!(dapp_queue.messages.len(), 2);
        assert_eq!(state.length(), 2);
    }

    #[test]
    fn test_consume_one_account_keeps_other_dapp_queue_shared() {
        let route_a = routing(1, 10);
        let route_b = routing(2, 20);
        let msg_a = create_empty_message();
        let msg_b = create_empty_message();
        let mut produced = HashMap::new();
        produced.insert(route_a, vec![msg_a.clone()]);
        produced.insert(route_b, vec![msg_b]);
        let state = build_state(produced);
        let cloned = state.clone();
        let other_dapp_before = Arc::clone(cloned.messages.get(route_b.dapp_id()).unwrap());

        let mut consumed = HashMap::new();
        consumed.insert(route_a, Vec::from([msg_a.0]));
        let new_state = ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(consumed)
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(HashMap::new())
            .build()
            .apply(state, &MessageDurableStorage::mem())
            .unwrap();

        assert!(new_state.account_inbox_by_routing(&route_a).is_none());
        assert!(new_state.account_inbox_by_routing(&route_b).is_some());
        assert!(Arc::ptr_eq(
            &other_dapp_before,
            new_state.messages.get(route_b.dapp_id()).unwrap()
        ));
        assert!(cloned.account_inbox_by_routing(&route_a).is_some());
    }

    #[test]
    fn test_remove_last_account_removes_dapp() {
        let route = routing(1, 10);
        let mut produced = HashMap::new();
        produced.insert(route, vec![create_empty_message()]);
        let state = build_state(produced);

        let new_state = ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(HashMap::new())
            .with_removed_accounts(vec![route])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(HashMap::new())
            .build()
            .apply(state, &MessageDurableStorage::mem())
            .unwrap();

        assert!(new_state.messages.is_empty());
        assert_eq!(new_state.cursor(), None);
    }

    #[test]
    fn test_remove_accounts_cleans_empty_dapps() {
        let route_a = routing(1, 10);
        let route_b = routing(2, 20);
        let mut produced = HashMap::new();
        produced.insert(route_a, vec![create_empty_message()]);
        produced.insert(route_b, vec![create_empty_message()]);
        let mut state = build_state(produced);

        state.remove_accounts([route_a]);

        assert!(state.account_inbox_by_routing(&route_a).is_none());
        assert!(state.account_inbox_by_routing(&route_b).is_some());
        assert_eq!(state.messages.len(), 1);
    }

    #[test]
    fn test_produced_consumed_intersection_is_preserved() {
        let route = routing(1, 10);
        let message = create_empty_message();
        let mut consumed = HashMap::new();
        consumed.insert(route, Vec::from([message.0.clone()]));
        let mut produced = HashMap::new();
        produced.insert(route, vec![message]);

        let state = ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(consumed)
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(produced)
            .build()
            .apply(ThreadMessageQueueState::empty(), &MessageDurableStorage::mem())
            .unwrap();

        assert!(state.is_empty());
    }

    #[test]
    fn test_serde_roundtrip() {
        let route = routing(1, 10);
        let mut produced = HashMap::new();
        produced.insert(route, vec![create_empty_message()]);
        let state = build_state(produced);

        let bytes = bincode::serialize(&state).unwrap();
        let decoded: ThreadMessageQueueState = bincode::deserialize(&bytes).unwrap();

        assert_eq!(decoded.length(), 1);
        assert!(decoded.account_inbox_by_routing(&route).is_some());
    }

    #[test]
    fn test_copy_on_write_one_inbox() {
        let route_a = routing(1, 10);
        let route_b = routing(1, 20);
        let mut produced = HashMap::new();
        produced.insert(route_a, vec![create_empty_message()]);
        produced.insert(route_b, vec![create_empty_message()]);
        let state = build_state(produced);
        let mut changed = state.clone();
        let unchanged_dapp = Arc::clone(state.messages.get(route_a.dapp_id()).unwrap());
        let unchanged_inbox_b = Arc::clone(
            state.messages.get(route_b.dapp_id()).unwrap().messages.get(&route_b).unwrap(),
        );

        changed.remove_accounts([route_a]);

        assert!(state.account_inbox_by_routing(&route_a).is_some());
        assert!(changed.account_inbox_by_routing(&route_a).is_none());
        assert!(!Arc::ptr_eq(&unchanged_dapp, changed.messages.get(route_b.dapp_id()).unwrap()));
        assert!(Arc::ptr_eq(
            &unchanged_inbox_b,
            changed.messages.get(route_b.dapp_id()).unwrap().messages.get(&route_b).unwrap()
        ));
    }

    #[test]
    fn test_add_account() {
        let route = routing(1, 10);
        let mut added_accounts = BTreeMap::new();
        added_accounts.insert(route, inbox_with(create_empty_message()));

        let new_state = ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(HashMap::new())
            .with_removed_accounts(vec![])
            .with_added_accounts(added_accounts)
            .with_produced_messages(HashMap::new())
            .build()
            .apply(ThreadMessageQueueState::empty(), &MessageDurableStorage::mem())
            .unwrap();

        assert!(new_state.account_inbox_by_routing(&route).is_some());
    }

    #[test]
    fn test_consumed_messages_advance_cursor() {
        let route_a = routing(1, 1);
        let route_b = routing(1, 2);
        let route_c = routing(1, 3);
        let msg_a = create_empty_message();
        let msg_b = create_empty_message();
        let msg_c = create_empty_message();

        let mut dapp_queue = DAppMessageQueueState::empty();
        dapp_queue.insert_inbox(route_a, inbox_with(msg_a.clone()));
        dapp_queue.insert_inbox(route_b, inbox_with(msg_b));
        dapp_queue.insert_inbox(route_c, inbox_with(msg_c));
        let mut initial_state = ThreadMessageQueueState::empty();
        initial_state.messages.insert(*route_a.dapp_id(), Arc::new(dapp_queue));

        let mut consumed = HashMap::new();
        consumed.insert(route_a, Vec::from([msg_a.0]));

        let new_state = ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(consumed)
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(HashMap::new())
            .build()
            .apply(initial_state, &MessageDurableStorage::mem())
            .unwrap();

        assert_eq!(new_state.cursor(), Some(multi_map::MapKey(*route_b.dapp_id().as_array())));
        assert_eq!(
            new_state.messages.get(route_b.dapp_id()).unwrap().cursor(),
            Some(multi_map::MapKey(*route_c.account_id().as_array()))
        );
    }

    #[test]
    fn test_consumed_messages_fallback_to_matching_account_id() {
        let queued_route = routing(1, 10);
        let consumed_route = routing(2, 10);
        let message = create_empty_message();

        let mut produced = HashMap::new();
        produced.insert(queued_route, vec![message.clone()]);
        let state = build_state(produced);

        let mut consumed = HashMap::new();
        consumed.insert(consumed_route, Vec::from([message.0]));

        let new_state = ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(consumed)
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(HashMap::new())
            .build()
            .apply(state, &MessageDurableStorage::mem())
            .unwrap();

        assert!(new_state.is_empty());
    }

    #[test]
    #[ignore]
    #[should_panic]
    fn test_produced_messages_out_of_order() {
        let route = routing(1, 10);
        let messages = (0..5).map(|_| create_empty_message()).collect::<Vec<_>>();
        let mut produced_messages = HashMap::new();
        produced_messages.insert(
            route,
            vec![
                messages[0].clone(),
                messages[1].clone(),
                messages[4].clone(),
                messages[3].clone(),
                messages[2].clone(),
            ],
        );
        let mut consumed_messages = HashMap::new();
        consumed_messages
            .insert(route, messages.iter().take(4).map(|(id, _)| id.clone()).collect::<Vec<_>>());

        let _ = ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(consumed_messages)
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(produced_messages)
            .build()
            .apply(ThreadMessageQueueState::empty(), &MessageDurableStorage::mem());
    }
}
