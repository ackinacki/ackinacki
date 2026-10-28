use std::collections::HashMap;
use std::sync::Arc;

use account_inbox::iter::iterator::MessagesRangeIterator;
use account_state::ThreadAccountsRepository;
use account_state::ThreadAccountsState;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::DAppIdentifier;
use tvm_block::HashmapAugType;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::storage::MessageDurableStorage;
use crate::types::account::WrappedAccount;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AckiNackiBlock;

impl AckiNackiBlock {
    pub fn get_data_for_postprocessing(
        &self,
        initial_optimistic_state: &mut OptimisticStateImpl,
        updated_shard_state: ThreadAccountsState,
        thread_accounts_repository: &ThreadAccountsRepository,
        message_db: &MessageDurableStorage,
        high_priority_messages: &ThreadMessageQueueState,
    ) -> anyhow::Result<(
        HashMap<AccountRouting, Vec<MessageIdentifier>>,
        HashMap<AccountRouting, Vec<MessageIdentifier>>,
        HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
        HashMap<AccountRouting, Option<WrappedAccount>>,
        HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    )> {
        let mut accounts_that_changed_their_dapp_id: HashMap<
            AccountRouting,
            Option<WrappedAccount>,
        > = HashMap::new();

        let mut produced_internal_messages: HashMap<
            AccountRouting,
            Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
        > = HashMap::new();
        let block_extra = self
            .tvm_block()
            .read_extra()
            .map_err(|e| anyhow::format_err!("Failed to read block extra: {e}"))?;
        let out_msg_descr = block_extra
            .read_out_msg_descr()
            .map_err(|e| anyhow::format_err!("Failed to read out msg descr: {e}"))?;
        out_msg_descr
            .iterate_objects(|out_msg| {
                let msg = out_msg
                    .read_message()?
                    .ok_or(tvm_types::error!("Failed to read block out message"))?;
                if let Some(dest_account_id) = msg.int_dst_account_id().map(AccountIdentifier::from)
                {
                    if let Some(header) = msg.int_header() {
                        let dest_dapp_id = header
                            .dest_dapp_id
                            .as_ref()
                            .map(DAppIdentifier::from)
                            .unwrap_or_else(|| dest_account_id.redirect_dapp_id());
                        let wrapped_message = WrappedMessage::from_tvm(&msg);
                        let message_identifier = wrapped_message.identifier();
                        let entry = produced_internal_messages
                            .entry(dest_account_id.routing(dest_dapp_id))
                            .or_default();
                        entry.push((message_identifier, Arc::new(wrapped_message)));
                    }
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iter out msgs: {e}"))?;
        let in_msg_descr = block_extra
            .read_in_msg_descr()
            .map_err(|e| anyhow::format_err!("Failed to read in msg descr: {e}"))?;
        let consumed_internal_messages = ordered_consumed_messages(&in_msg_descr)?;
        let (consumed_internal_messages, consumed_high_priority_messages) =
            split_consumed_messages_by_priority(
                consumed_internal_messages,
                high_priority_messages,
                message_db,
            )?;
        // TODO: need change set in common section
        self.block
            .read_extra()
            .map_err(|e| anyhow::format_err!("Failed to read block extra: {e}"))?
            .read_account_blocks()
            .map_err(|e| anyhow::format_err!("Failed to read account blocks: {e}"))?
            .iterate_objects(|account_block| {
                // tracing::trace!(target: "node", "get_data_for_postprocessing: parse {}", account_block.account_id().to_hex_string());
                if account_block.dapp_id_changed() {
                    // tracing::trace!(target: "node", "get_data_for_postprocessing: dapp id changed {}", account_block.account_id().to_hex_string());
                    let acc_id = AccountIdentifier::from(account_block.account_id());
                    if let Some(account) = thread_accounts_repository
                        .state_account(&updated_shard_state, &acc_id.redirect())
                        .map_err(|err| tvm_types::error!("{}", err))?
                    {
                        match account.get_dapp_id() {
                            Some(new_dapp_id) => {
                                accounts_that_changed_their_dapp_id.insert(
                                    acc_id.routing(new_dapp_id),
                                    Some(WrappedAccount { account_id: acc_id, account }),
                                );
                            }
                            None => {
                                accounts_that_changed_their_dapp_id.insert(acc_id.redirect(), None);
                            }
                        }
                    } else {
                        accounts_that_changed_their_dapp_id.insert(acc_id.redirect(), None);
                    }
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iterate account blocks: {e}"))?;

        let mut produced_internal_messages_to_the_current_thread: HashMap<
            AccountRouting,
            Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
        > = HashMap::new();
        let mut produced_internal_messages_to_other_threads: HashMap<
            AccountRouting,
            Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
        > = HashMap::new();
        for (routing, mut data) in produced_internal_messages {
            if initial_optimistic_state.does_routing_belong_to_the_state(&routing) {
                produced_internal_messages_to_the_current_thread
                    .entry(routing)
                    .or_default()
                    .append(&mut data);
            } else {
                produced_internal_messages_to_other_threads
                    .entry(routing)
                    .or_default()
                    .append(&mut data);
            }
        }

        Ok((
            consumed_internal_messages,
            consumed_high_priority_messages,
            produced_internal_messages_to_the_current_thread,
            accounts_that_changed_their_dapp_id,
            produced_internal_messages_to_other_threads,
        ))
    }
}

// Execution processes high first. Match its FIFO prefix, then leave the
// remaining ordered consumption to normal (including in-block messages).
fn split_consumed_messages_by_priority(
    consumed: HashMap<AccountRouting, Vec<MessageIdentifier>>,
    high: &ThreadMessageQueueState,
    db: &MessageDurableStorage,
) -> anyhow::Result<(
    HashMap<AccountRouting, Vec<MessageIdentifier>>,
    HashMap<AccountRouting, Vec<MessageIdentifier>>,
)> {
    let mut normal_consumed = HashMap::new();
    let mut high_consumed = HashMap::new();
    for (routing, mut ids) in consumed {
        let inbox = high.account_inbox_by_routing(&routing).or_else(|| {
            let mut matches = high
                .messages
                .values()
                .flat_map(|dapp| dapp.messages.iter())
                .filter(|(candidate, _)| candidate.account_id() == routing.account_id());
            let (_, inbox) = matches.next()?;
            if matches.next().is_some() {
                return None;
            }
            Some(inbox.as_ref())
        });
        let mut count = 0;
        if let Some(inbox) = inbox {
            let mut iter = MessagesRangeIterator::new(db, inbox.messages.clone());
            for id in &ids {
                let Some(next) = iter.next() else {
                    break;
                };
                let (_, high_id) =
                    next.map_err(|error| anyhow::anyhow!("Reading high inbox: {error:?}"))?;
                anyhow::ensure!(
                    *id == high_id,
                    "Consumed messages do not match the high FIFO prefix for {routing:?}"
                );
                count += 1;
            }
        }
        if count > 0 {
            high_consumed.insert(routing, ids.drain(..count).collect());
        }
        if !ids.is_empty() {
            normal_consumed.insert(routing, ids);
        }
    }
    Ok((normal_consumed, high_consumed))
}

fn ordered_consumed_messages(
    in_msg_descr: &tvm_block::InMsgDescr,
) -> anyhow::Result<HashMap<AccountRouting, Vec<MessageIdentifier>>> {
    let mut consumed_by_transaction_lt: HashMap<AccountRouting, Vec<(u64, MessageIdentifier)>> =
        HashMap::new();
    in_msg_descr
        .iterate_objects(|in_msg| {
            let msg = in_msg.read_message()?;
            if let Some(header) = msg.int_header() {
                let dst_account_id = AccountIdentifier::from(header.dst.address());
                let dst_dapp_id = header
                    .dest_dapp_id
                    .as_ref()
                    .map(DAppIdentifier::from)
                    .unwrap_or_else(|| dst_account_id.redirect_dapp_id());
                let message_identifier = MessageIdentifier::from(&msg);
                let transaction = in_msg.read_transaction()?.ok_or_else(|| {
                    tvm_types::error!("Consumed internal message has no transaction")
                })?;
                let entry = consumed_by_transaction_lt
                    .entry(dst_account_id.routing(dst_dapp_id))
                    .or_default();
                entry.push((transaction.logical_time(), message_identifier));
            }
            Ok(true)
        })
        .map_err(|e| anyhow::format_err!("Failed to iter in msgs: {e}"))?;
    let consumed_internal_messages = consumed_by_transaction_lt
        .into_iter()
        .map(|(routing, mut messages)| {
            messages.sort_by_key(|(lt, _)| *lt);
            anyhow::ensure!(
                !messages.windows(2).any(|pair| pair[0].0 == pair[1].0),
                "Consumed messages have duplicate transaction LT for {routing:?}"
            );
            Ok((routing, messages.into_iter().map(|(_, id)| id).collect()))
        })
        .collect::<anyhow::Result<HashMap<AccountRouting, Vec<MessageIdentifier>>>>()?;

    Ok(consumed_internal_messages)
}

#[cfg(test)]
mod ordered_consumption_tests {
    use tvm_block::GetRepresentationHash;
    use tvm_block::InternalMessageHeader;
    use tvm_block::Message;
    use tvm_block::MsgEnvelope;
    use tvm_block::Serializable;
    use tvm_block::Transaction;

    use super::*;
    use crate::types::thread_message_queue::ThreadMessageQueueStateDiff;

    fn descriptor(message: &Message, lt: u64) -> tvm_block::InMsg {
        let mut transaction = Transaction::default();
        transaction.set_logical_time(lt);
        transaction.write_in_msg(Some(message)).unwrap();
        let envelope = MsgEnvelope::with_message_and_fee(message, Default::default()).unwrap();
        tvm_block::InMsg::immediate(
            envelope.serialize().unwrap(),
            transaction.serialize().unwrap(),
            Default::default(),
        )
    }

    #[test]
    fn applier_reconstructs_producer_consumption_order_not_descriptor_hash_order() {
        let mut messages: Vec<_> = (1..=4)
            .map(|lt| {
                let header = InternalMessageHeader { created_lt: lt, ..Default::default() };
                Message::with_int_header(header)
            })
            .collect();
        // Deliberately consume in the opposite order to descriptor traversal.
        messages.sort_by_key(|message| std::cmp::Reverse(message.hash().unwrap()));
        let expected: Vec<_> = messages.iter().map(MessageIdentifier::from).collect();
        let mut descriptors = tvm_block::InMsgDescr::default();
        for (index, message) in messages.iter().enumerate() {
            descriptors.insert(&descriptor(message, 10 + index as u64)).unwrap();
        }
        let consumed = ordered_consumed_messages(&descriptors).unwrap();
        assert_eq!(consumed[&AccountIdentifier::default().redirect()], expected);
    }

    #[test]
    fn applier_rejects_ambiguous_transaction_order() {
        let mut descriptors = tvm_block::InMsgDescr::default();
        for lt in [1, 2] {
            let header = InternalMessageHeader { created_lt: lt, ..Default::default() };
            descriptors.insert(&descriptor(&Message::with_int_header(header), 10)).unwrap();
        }
        assert!(ordered_consumed_messages(&descriptors)
            .unwrap_err()
            .to_string()
            .contains("duplicate transaction LT"));
    }

    #[test]
    fn applier_splits_priorities_in_transaction_order_for_one_routing() {
        let routing = AccountIdentifier::default().redirect();
        let messages: Vec<_> = (1..=4)
            .map(|lt| {
                let header = InternalMessageHeader { created_lt: lt, ..Default::default() };
                Message::with_int_header(header)
            })
            .collect();
        let ids: Vec<_> = messages.iter().map(MessageIdentifier::from).collect();
        let db = MessageDurableStorage::mem();
        let mut high = ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(HashMap::new())
            .with_produced_messages(HashMap::from([(
                routing,
                messages[..1]
                    .iter()
                    .map(|message| {
                        let body = Arc::new(WrappedMessage::from_tvm(message));
                        (body.identifier(), body)
                    })
                    .collect(),
            )]))
            .with_removed_accounts(vec![])
            .with_added_accounts(Default::default())
            .build()
            .apply(ThreadMessageQueueState::empty(), &db)
            .unwrap();
        let base = high.clone();
        let body = Arc::new(WrappedMessage::from_tvm(&messages[1]));
        high.append_messages(&HashMap::from([(routing, vec![(body.identifier(), body)])]));
        assert_eq!(base.length(), 1);
        for count in 0..=4 {
            let mut descriptors = tvm_block::InMsgDescr::default();
            for (index, message) in messages[..count].iter().enumerate().rev() {
                descriptors.insert(&descriptor(message, 100 + index as u64)).unwrap();
            }
            let (normal, priority) = split_consumed_messages_by_priority(
                ordered_consumed_messages(&descriptors).unwrap(),
                &high,
                &db,
            )
            .unwrap();
            assert_eq!(priority.get(&routing).cloned().unwrap_or_default(), ids[..count.min(2)]);
            assert_eq!(normal.get(&routing).cloned().unwrap_or_default(), ids[2.min(count)..count]);
        }
        // No skipping or permuting the high FIFO to classify it as normal.
        for invalid in
            [vec![ids[1].clone()], vec![ids[2].clone()], vec![ids[0].clone(), ids[2].clone()]]
        {
            assert!(split_consumed_messages_by_priority(
                HashMap::from([(routing, invalid)]),
                &high,
                &db
            )
            .is_err());
        }
        assert_eq!(high.length(), 2);
        assert_eq!(
            high.account_inbox_by_routing(&routing).unwrap().commitment,
            base.account_inbox_by_routing(&routing).unwrap().commitment
        );
    }

    #[cfg(all(feature = "messages_db", not(feature = "disable_db_for_messages")))]
    #[test]
    fn applier_splits_high_consumption_across_db_history_and_memory_tail() {
        let routing = AccountIdentifier::default().redirect();
        let messages: Vec<_> = (1..=3)
            .map(|lt| {
                let header = InternalMessageHeader { created_lt: lt, ..Default::default() };
                let body = Arc::new(WrappedMessage::from_tvm(&Message::with_int_header(header)));
                (body.identifier(), body)
            })
            .collect();
        let db = MessageDurableStorage::mem();
        db.write_messages(HashMap::from([(routing, vec![messages[0].clone()])])).unwrap();
        let mut inbox = crate::types::AccountInbox::empty();
        inbox.messages.set_compacted_history(Some(messages[0].0.clone()..=messages[0].0.clone()));
        inbox.add_messages(vec![messages[1].clone()]).unwrap();
        inbox.rebase(&db).unwrap();
        let mut high = ThreadMessageQueueState::empty();
        high.set_inbox(routing, inbox);
        let (normal, priority) = split_consumed_messages_by_priority(
            HashMap::from([(routing, messages.iter().map(|m| m.0.clone()).collect())]),
            &high,
            &db,
        )
        .unwrap();
        assert_eq!(
            priority[&routing],
            messages[..2].iter().map(|m| m.0.clone()).collect::<Vec<_>>()
        );
        assert_eq!(normal[&routing], vec![messages[2].0.clone()]);
        assert!(high.account_inbox_by_routing(&routing).unwrap().compacted_history().is_some());
    }
}
