use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
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

#[derive(TypedBuilder)]
#[builder(
    build_method(vis="pub", into=anyhow::Result<ThreadMessageQueueState>),
    builder_method(vis="pub"),
    builder_type(vis="pub", name=ThreadMessageQueueStateBuilder),
    field_defaults(setter(prefix="with_")),
)]
pub struct ThreadMessageQueueStateDiff {
    initial_state: ThreadMessageQueueState,
    consumed_messages: HashMap<AccountRouting, HashSet<MessageIdentifier>>,
    #[builder(setter(into))]
    removed_accounts: Vec<AccountRouting>,
    added_accounts: BTreeMap<AccountRouting, AccountInbox>,
    produced_messages: HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    db: MessageDurableStorage,
}

fn resolve_consumed_routing(
    state: &ThreadMessageQueueState,
    routing: &AccountRouting,
) -> Option<AccountRouting> {
    if state
        .messages
        .get(routing.dapp_id())
        .and_then(|dapp_queue| dapp_queue.messages.get(routing))
        .is_some()
    {
        return Some(*routing);
    }

    let mut fallback = None;
    for dapp_queue in state.messages.values() {
        for candidate in dapp_queue.messages.keys() {
            if candidate.account_id() != routing.account_id() {
                continue;
            }
            if fallback.replace(*candidate).is_some() {
                return None;
            }
        }
    }
    fallback
}

impl std::convert::From<ThreadMessageQueueStateDiff> for anyhow::Result<ThreadMessageQueueState> {
    #[instrument(skip_all)]
    fn from(val: ThreadMessageQueueStateDiff) -> Self {
        tracing::trace!(
            target: "builder",
            "from ThreadMessageQueueStateDiff consumed_messages: {:?}",
            val.consumed_messages
        );
        tracing::trace!(
            target: "builder",
            "from ThreadMessageQueueStateDiff produced_messages: {:?}",
            val.produced_messages
        );

        let mut state = val.initial_state;
        let mut consumed_accounts_by_dapp: HashMap<DAppIdentifier, usize> = HashMap::new();

        trace_span!("add accounts").in_scope(|| {
            for (routing, inbox) in val.added_accounts.into_iter() {
                state
                    .dapp_queue_mut_or_insert_empty(*routing.dapp_id())
                    .insert_inbox(routing, inbox);
            }
        });

        // MessageIdentifier is derived from the message itself and is expected to be globally
        // unique, so produced/consumed intersection remains global rather than per routing.
        let produced_ids: HashSet<&MessageIdentifier> =
            val.produced_messages.values().flat_map(|vec| vec.iter().map(|(id, _)| id)).collect();

        let intersection: HashSet<MessageIdentifier> = val
            .consumed_messages
            .values()
            .flat_map(|set| set.iter())
            .filter(|id| produced_ids.contains(*id))
            .cloned()
            .collect();

        trace_span!("add messages", addresses_count = val.produced_messages.len()).in_scope(|| {
            for (routing, mut messages) in val.produced_messages.into_iter() {
                messages.retain(|(message_id, _message)| !intersection.contains(message_id));
                if messages.is_empty() {
                    continue;
                }

                let entry = state
                    .dapp_queue_mut_or_insert_empty(*routing.dapp_id())
                    .inbox_mut_or_insert_empty(routing);

                let mut tail = entry.tail_sequence().clone();
                let mut range_ref = entry.compacted_history().clone();

                tail.extend(
                    messages
                        .iter()
                        .map(|(message, message_key)| (message.clone(), message_key.clone())),
                );
                if let Some((message_id, _)) = tail.front() {
                    if let Ok(db_messages) = val.db.remaining_messages(message_id, tail.len()) {
                        if !db_messages.is_empty() {
                            if let Some(ref mut range) = range_ref {
                                *range = RangeInclusive::new(
                                    range.start().clone(),
                                    MessageIdentifier::from(db_messages.last().unwrap().clone()),
                                )
                            } else {
                                range_ref = Some(RangeInclusive::new(
                                    MessageIdentifier::from(db_messages.first().unwrap().clone()),
                                    MessageIdentifier::from(db_messages.last().unwrap().clone()),
                                ));
                            }
                        }

                        let db_set: HashSet<MessageIdentifier> = HashSet::from_iter(
                            db_messages.into_iter().map(MessageIdentifier::from),
                        );
                        let mut tail_clone = tail.clone();
                        tail = tail_clone.split_off(db_set.len());
                        let tail_set = HashSet::from_iter(tail_clone.into_iter().map(|(id, _)| id));

                        assert_eq!(tail_set, db_set);
                    }
                }
                entry.set_tail_sequence(tail);
                entry.set_compacted_history(range_ref);
            }
        });

        trace_span!("remove messages").in_scope(|| {
            for (routing, mut messages) in val.consumed_messages.into_iter() {
                messages.retain(|message_id| !intersection.contains(message_id));
                if messages.is_empty() {
                    continue;
                }
                let actual_routing = resolve_consumed_routing(&state, &routing)
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
                let Some(dapp_queue) = state.messages.get_mut(&dapp_id) else {
                    anyhow::bail!("Unexpected consumed message: {routing:?}");
                };
                let dapp_queue = Arc::make_mut(dapp_queue);
                let Some(entry) = dapp_queue.messages.get_mut(&actual_routing) else {
                    anyhow::bail!("Unexpected consumed message: {routing:?}");
                };
                let entry = Arc::make_mut(entry);

                let mut it = MessagesRangeIterator::new(&val.db, entry.clone());
                let iterator_set = HashSet::from_iter(
                    it.next_range(messages.len())
                        .expect("dirty state detected: iterator returned None or Err")
                        .into_iter()
                        .map(MessageIdentifier::from),
                );
                assert_eq!(messages, iterator_set, "dirty state detected: mismatch in sets");
                *entry = it.remaining().clone();

                *consumed_accounts_by_dapp.entry(dapp_id).or_default() += 1;
                if entry.is_empty() {
                    dapp_queue.remove_account(&actual_routing);
                }
                if dapp_queue.is_empty() {
                    state.messages.remove(&dapp_id);
                    state.order_set.remove(&dapp_id);
                }
            }
            Ok::<_, anyhow::Error>(())
        })?;

        trace_span!("remove accounts").in_scope(|| {
            for routing in val.removed_accounts.into_iter() {
                state.remove_routing(&routing);
            }
        });

        for (dapp_id, consumed_accounts) in &consumed_accounts_by_dapp {
            if let Some(dapp_queue) = state.messages.get_mut(dapp_id) {
                Arc::make_mut(dapp_queue).advance_cursor(*consumed_accounts);
            }
        }
        state.advance_cursor(consumed_accounts_by_dapp.len());
        state.normalize_cursor();

        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Arc;

    use account_inbox::range::MessagesRange;
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
        let wrapped_message = Arc::new(WrappedMessage { message: msg });
        let message_id = MessageIdentifier::from(wrapped_message.clone());
        (message_id, wrapped_message)
    }

    fn inbox_with(message: (MessageIdentifier, Arc<WrappedMessage>)) -> AccountInbox {
        let mut inbox: AccountInbox = MessagesRange::empty();
        inbox.add_messages(vec![message]);
        inbox
    }

    fn build_state(
        produced_messages: HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    ) -> ThreadMessageQueueState {
        ThreadMessageQueueState::build_next()
            .with_initial_state(ThreadMessageQueueState::empty())
            .with_consumed_messages(HashMap::new())
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(produced_messages)
            .with_db(MessageDurableStorage::mem())
            .build()
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
        consumed.insert(route_a, HashSet::from([msg_a.0]));
        let new_state = ThreadMessageQueueState::build_next()
            .with_initial_state(state)
            .with_consumed_messages(consumed)
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(HashMap::new())
            .with_db(MessageDurableStorage::mem())
            .build()
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

        let new_state = ThreadMessageQueueState::build_next()
            .with_initial_state(state)
            .with_consumed_messages(HashMap::new())
            .with_removed_accounts(vec![route])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(HashMap::new())
            .with_db(MessageDurableStorage::mem())
            .build()
            .unwrap();

        assert!(new_state.messages.is_empty());
        assert_eq!(new_state.cursor(), 0);
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
        consumed.insert(route, HashSet::from([message.0.clone()]));
        let mut produced = HashMap::new();
        produced.insert(route, vec![message]);

        let state = ThreadMessageQueueState::build_next()
            .with_initial_state(ThreadMessageQueueState::empty())
            .with_consumed_messages(consumed)
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(produced)
            .with_db(MessageDurableStorage::mem())
            .build()
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
        added_accounts.insert(route, AccountInbox::empty());

        let new_state = ThreadMessageQueueState::build_next()
            .with_initial_state(ThreadMessageQueueState::empty())
            .with_consumed_messages(HashMap::new())
            .with_removed_accounts(vec![])
            .with_added_accounts(added_accounts)
            .with_produced_messages(HashMap::new())
            .with_db(MessageDurableStorage::mem())
            .build()
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
        initial_state.order_set.insert(*route_a.dapp_id());

        let mut consumed = HashMap::new();
        consumed.insert(route_a, HashSet::from([msg_a.0]));

        let new_state = ThreadMessageQueueState::build_next()
            .with_initial_state(initial_state)
            .with_consumed_messages(consumed)
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(HashMap::new())
            .with_db(MessageDurableStorage::mem())
            .build()
            .unwrap();

        assert_eq!(new_state.cursor(), 0);
        assert_eq!(new_state.messages.get(route_b.dapp_id()).unwrap().cursor(), 1);
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
        consumed.insert(consumed_route, HashSet::from([message.0]));

        let new_state = ThreadMessageQueueState::build_next()
            .with_initial_state(state)
            .with_consumed_messages(consumed)
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(HashMap::new())
            .with_db(MessageDurableStorage::mem())
            .build()
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
        consumed_messages.insert(
            route,
            messages.iter().take(4).map(|(id, _)| id.clone()).collect::<HashSet<_>>(),
        );

        let _ = ThreadMessageQueueState::build_next()
            .with_initial_state(ThreadMessageQueueState::empty())
            .with_consumed_messages(consumed_messages)
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .with_produced_messages(produced_messages)
            .with_db(MessageDurableStorage::mem())
            .build();
    }
}
