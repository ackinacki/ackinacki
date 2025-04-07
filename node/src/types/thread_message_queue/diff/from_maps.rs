use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::RangeInclusive;
use std::sync::Arc;

use account_inbox::iter::iterator::MessagesRangeIterator;
use account_inbox::range::MessagesRange;
use account_inbox::storage::DurableStorageRead;
use tracing::instrument;
use tracing::trace_span;
use typed_builder::TypedBuilder;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::message_storage::MessageDurableStorage;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AccountAddress;
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
    consumed_messages: HashMap<AccountAddress, HashSet<MessageIdentifier>>,
    #[builder(setter(into))]
    removed_accounts: Vec<AccountAddress>,
    added_accounts: std::collections::BTreeMap<AccountAddress, AccountInbox>,
    produced_messages: HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    db: MessageDurableStorage,
}

impl std::convert::From<ThreadMessageQueueStateDiff> for anyhow::Result<ThreadMessageQueueState> {
    #[instrument(skip_all)]
    fn from(val: ThreadMessageQueueStateDiff) -> Self {
        let state = val.initial_state;
        // TODO: fix this:
        // Note: dirty solution
        let mut state_messages = state.messages.clone();
        let mut state_order = state.order_set.clone();
        let state_cursor = state.cursor;

        trace_span!("add accounts").in_scope(|| {
            for (account_address, inbox) in val.added_accounts.into_iter() {
                let prev = state_messages.insert(account_address.clone(), inbox);
                #[cfg(feature = "fail-fast")]
                assert!(prev.is_none(), "dirty state detected");
                if !state_order.contains(&account_address.clone()) {
                    state_order.insert(account_address.clone());
                }
            }
        });
        // tracing::trace!("produced_messages {:?}", val.produced_messages);
        // tracing::trace!("consumed_messages {:?}", val.consumed_messages);
        trace_span!("add messages", addresses_count = val.produced_messages.len(),).in_scope(
            || {
                for (addr, messages) in val.produced_messages.into_iter() {
                    let entry = state_messages.entry(addr.clone()).or_insert(MessagesRange::<
                        MessageIdentifier,
                        Arc<WrappedMessage>,
                    >::empty(
                    ));

                    let mut tail = entry.tail_sequence().clone();
                    let mut range_ref = entry.compacted_history().clone();

                    if !state_order.contains(&addr) && !messages.is_empty() {
                        state_order.insert(addr.clone());
                    }
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
                                        MessageIdentifier::from(
                                            db_messages.last().unwrap().clone(),
                                        ),
                                    )
                                } else {
                                    range_ref = Some(RangeInclusive::new(
                                        MessageIdentifier::from(
                                            db_messages.first().unwrap().clone(),
                                        ),
                                        MessageIdentifier::from(
                                            db_messages.last().unwrap().clone(),
                                        ),
                                    ));
                                }
                            }
                            let db_set: HashSet<MessageIdentifier> = HashSet::from_iter(
                                db_messages.into_iter().map(MessageIdentifier::from),
                            );
                            let mut tail_clone = tail.clone();
                            tail = tail_clone.split_off(db_set.len());
                            let tail_set =
                                HashSet::from_iter(tail_clone.into_iter().map(|(id, _)| id));
                            assert_eq!(tail_set, db_set);
                        }
                    }
                    entry.set_tail_sequence(tail);
                    entry.set_compacted_history(range_ref);
                    // tracing::trace!("addr entry {:?} {:?}", addr, entry);
                }
            },
        );
        trace_span!("remove messages").in_scope(|| {
            for (addr, messages) in val.consumed_messages.into_iter() {
                let Some(entry) = state_messages.get_mut(&addr) else {
                    anyhow::bail!("Unexpected consumed message: {:?}", addr);
                };
                // tracing::trace!("entry {:?}", entry);
                let mut it = MessagesRangeIterator::new(&val.db, entry.clone());
                let iterator_set = HashSet::from_iter(
                    it.next_range(messages.len())
                        .expect("dirty state detected: iterator returned None or Err")
                        .into_iter()
                        .map(MessageIdentifier::from),
                );
                assert_eq!(messages, iterator_set, "dirty state detected: mismatch in sets");
                *entry = it.remaining().clone();
                if entry.is_empty() {
                    state_messages.remove(&addr);
                    state_order.remove(&addr);
                }
            }
            Ok::<_, anyhow::Error>(())
        })?;

        trace_span!("remove accounts").in_scope(|| {
            for addr in val.removed_accounts.into_iter() {
                state_messages.remove(&addr);
                state_order.remove(&addr);
            }
        });
        let new_cursor =
            if !state_messages.is_empty() { state_cursor % state_messages.len() } else { 0 };

        let new_state = ThreadMessageQueueState {
            messages: state_messages,
            order_set: state_order,
            cursor: new_cursor,
        };
        Ok(new_state)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Arc;

    use tvm_block::InternalMessageHeader;
    use tvm_block::Message;

    use super::*;
    use crate::message::identifier::MessageIdentifier;
    use crate::message::WrappedMessage;
    use crate::types::thread_message_queue::order_set::OrderSet;
    use crate::types::thread_message_queue::ThreadMessageQueueState;
    use crate::types::AccountAddress;
    use crate::types::AccountInbox;

    fn create_empty_state() -> ThreadMessageQueueState {
        ThreadMessageQueueState { messages: BTreeMap::new(), order_set: OrderSet::new(), cursor: 0 }
    }

    fn create_empty_message() -> (MessageIdentifier, Arc<WrappedMessage>) {
        let msg = Message::with_int_header(InternalMessageHeader::default());
        let wrapped_message = Arc::new(WrappedMessage { message: msg });
        let message_id = MessageIdentifier::from(wrapped_message.clone());
        (message_id, wrapped_message)
    }

    #[test]
    fn test_add_account() {
        let initial_state = create_empty_state();
        let account_address = AccountAddress::default();
        let mut added_accounts = BTreeMap::new();
        let account_inbox: AccountInbox = MessagesRange::empty();
        added_accounts.insert(account_address.clone(), account_inbox);
        let db_path = tempfile::tempdir().unwrap().into_path().join("test_simple_1.db");
        let storage = MessageDurableStorage::new(db_path).expect("Failed to create DB");
        let state_diff = ThreadMessageQueueStateDiff {
            initial_state,
            consumed_messages: HashMap::new(),
            removed_accounts: Vec::new(),
            added_accounts,
            produced_messages: HashMap::new(),
            db: storage,
        };
        let new_state_res: anyhow::Result<ThreadMessageQueueState> = state_diff.into();
        let new_state = new_state_res.unwrap();
        assert!(new_state.messages.contains_key(&account_address));
    }

    #[test]
    fn test_remove_account() {
        let account_address = AccountAddress::default();
        let mut initial_state = create_empty_state();
        initial_state.messages.insert(account_address.clone(), MessagesRange::empty());
        let db_path = tempfile::tempdir().unwrap().into_path().join("test_simple_2.db");
        let storage = MessageDurableStorage::new(db_path).expect("Failed to create DB");
        let state_diff = ThreadMessageQueueStateDiff {
            initial_state,
            consumed_messages: HashMap::new(),
            removed_accounts: vec![account_address.clone()],
            added_accounts: BTreeMap::new(),
            produced_messages: HashMap::new(),
            db: storage,
        };

        let new_state_res: anyhow::Result<ThreadMessageQueueState> = state_diff.into();
        let new_state = new_state_res.unwrap();
        assert!(!new_state.messages.contains_key(&account_address));
    }

    #[test]
    #[cfg(feature = "messages_db")]
    fn test_produced_messages_with_db_write() {
        let account_address = AccountAddress::default();
        let (message_id, wrapped_message) = create_empty_message();
        let mut produced_messages = HashMap::new();
        produced_messages
            .insert(account_address.clone(), vec![(message_id.clone(), wrapped_message.clone())]);
        let db_path = tempfile::tempdir().unwrap().into_path().join("test_simple_3.db");
        let storage = MessageDurableStorage::new(db_path.clone()).expect("Failed to create DB");
        let message_blob =
            bincode::serialize(&wrapped_message).expect("Failed to serialize message");
        storage
            .write_message(
                &account_address.0.to_hex_string(),
                &message_blob,
                &message_id.inner().hash.to_hex_string(),
            )
            .expect("Failed to write message to DB");
        let db_rowid = storage
            .get_rowid_by_hash(&message_id.inner().hash.to_hex_string())
            .expect("DB query failed");
        assert!(db_rowid.is_some(), "Message was not found in DB after insertion");
        let state_diff = ThreadMessageQueueStateDiff {
            initial_state: create_empty_state(),
            consumed_messages: HashMap::new(),
            removed_accounts: vec![],
            added_accounts: BTreeMap::new(),
            produced_messages,
            db: storage,
        };
        let new_state_res: anyhow::Result<ThreadMessageQueueState> = state_diff.into();
        let new_state = new_state_res.unwrap();
        let account_messages = new_state.messages.get(&account_address);
        assert!(account_messages.is_some(), "Account was not found in state.messages");
        let account_range = account_messages.unwrap().compacted_history();
        assert!(account_range.clone().is_some(), "Message range was not updated correctly");
        let range = account_range.clone().unwrap();
        assert_eq!(range.start(), &message_id, "Start of range is incorrect");
        assert_eq!(range.end(), &message_id, "End of range is incorrect");
    }

    #[test]
    fn test_consumed_messages() {
        let account_address = AccountAddress::default();
        let (message_id, wrapped_message) = create_empty_message();
        let mut initial_state = create_empty_state();
        let mut inbox: AccountInbox = MessagesRange::empty();
        let add_message = vec![(message_id.clone(), wrapped_message)];
        inbox.add_messages(add_message);
        initial_state.messages.insert(account_address.clone(), inbox);

        let mut consumed_messages = HashMap::new();
        consumed_messages.insert(account_address.clone(), HashSet::from([message_id]));
        let db_path = tempfile::tempdir().unwrap().into_path().join("test_simple_2.db");
        let storage = MessageDurableStorage::new(db_path).expect("Failed to create DB");
        let state_diff = ThreadMessageQueueStateDiff {
            initial_state,
            consumed_messages,
            removed_accounts: vec![],
            added_accounts: BTreeMap::new(),
            produced_messages: HashMap::new(),
            db: storage,
        };
        let new_state_res: anyhow::Result<ThreadMessageQueueState> = state_diff.into();
        let new_state = new_state_res.unwrap();
        assert!(!new_state.messages.contains_key(&account_address));
    }

    #[test]
    #[ignore]
    #[should_panic]
    fn test_produced_messages_out_of_order() {
        let account_address = AccountAddress::default();
        let messages = (0..5).map(|_| create_empty_message()).collect::<Vec<_>>();

        let initial_state = create_empty_state();
        let mut inbox: AccountInbox = MessagesRange::empty();
        let mut add_message = Vec::new();
        for (id, msg) in &messages {
            add_message.push((id.clone(), msg.clone()));
        }
        inbox.add_messages(add_message);
        let mut produced_messages = HashMap::new();
        produced_messages.insert(
            account_address.clone(),
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
            account_address.clone(),
            messages.iter().take(4).map(|(id, _)| id.clone()).collect::<HashSet<_>>(),
        );
        let db_path = tempfile::tempdir().unwrap().into_path().join("test_simple_3.db");
        let storage = MessageDurableStorage::new(db_path).expect("Failed to create DB");
        let state_diff = ThreadMessageQueueStateDiff {
            initial_state,
            consumed_messages,
            removed_accounts: vec![],
            added_accounts: BTreeMap::new(),
            produced_messages,
            db: storage,
        };
        let _new_state_res: anyhow::Result<ThreadMessageQueueState> = state_diff.into();
    }
}
