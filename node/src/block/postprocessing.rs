use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use account_state::DurableThreadAccountsStateDiff;
use account_state::ThreadAccountsRepository;
use account_state::ThreadAccountsStateTransition;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use tracing::instrument;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::repository::optimistic_shard_state::OptimisticShardState;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::storage::MessageDurableStorage;
use crate::types::account::WrappedAccount;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::thread_message_queue::ThreadMessageQueueStateDiff;
use crate::types::AccountInbox;
use crate::types::BlockInfo;
use crate::types::BlockSeqNo;
use crate::types::ThreadsTable;

#[allow(clippy::too_many_arguments)]
#[instrument(skip_all)]
pub fn postprocess(
    mut initial_optimistic_state: OptimisticStateImpl,
    consumed_internal_messages: HashMap<AccountRouting, Vec<MessageIdentifier>>,
    consumed_high_priority_messages: HashMap<AccountRouting, Vec<MessageIdentifier>>,
    mut settled_messages: HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    mut added_high_priority_messages: HashMap<
        AccountRouting,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    mut produced_internal_messages_to_the_current_thread: HashMap<
        AccountRouting,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    accounts_that_changed_their_dapp_id: HashMap<AccountRouting, Option<WrappedAccount>>,
    block_id: BlockIdentifier,
    block_seq_no: BlockSeqNo,
    block_height: u64,
    mut state_transition: ThreadAccountsStateTransition,
    mut produced_internal_messages_to_other_threads: HashMap<
        AccountRouting,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    block_info: BlockInfo,
    thread_id: ThreadIdentifier,
    threads_table: ThreadsTable,
    db: MessageDurableStorage,
    thread_accounts_repository: &ThreadAccountsRepository,
    _apply_to_durable: bool,
    #[cfg(feature = "monitor-accounts-number")] updated_accounts_number: u64,
) -> anyhow::Result<(OptimisticStateImpl, CrossThreadRefData, DurableThreadAccountsStateDiff)> {
    // Apply full block additions and consumptions to the base queues, not to
    // the temporary queues used for execution.
    for (routing, messages) in &mut added_high_priority_messages {
        sort_produced_messages(
            messages,
            consumed_high_priority_messages.get(routing).map_or(&[], Vec::as_slice),
        );
    }
    let initial_high_priority_messages = std::mem::replace(
        &mut initial_optimistic_state.high_priority_messages,
        ThreadMessageQueueState::empty(),
    );
    initial_optimistic_state.high_priority_messages = ThreadMessageQueueStateDiff::builder()
        .with_consumed_messages(consumed_high_priority_messages)
        .with_produced_messages(added_high_priority_messages)
        .with_removed_accounts(vec![])
        .with_added_accounts(BTreeMap::new())
        .build()
        .apply(initial_high_priority_messages, &db)?;
    for (routing, messages) in &mut produced_internal_messages_to_the_current_thread {
        sort_produced_messages(
            messages,
            consumed_internal_messages.get(routing).map_or(&[], Vec::as_slice),
        );
    }
    // Cross-thread arrivals precede locally produced messages in the FIFO.
    // Preserve that phase order; do not re-sort the combined addition list.
    for (routing, mut messages) in produced_internal_messages_to_the_current_thread {
        settled_messages.entry(routing).or_default().append(&mut messages);
    }
    for messages in produced_internal_messages_to_other_threads.values_mut() {
        sort_produced_messages(messages, &[]);
    }

    let mut new_thread_refs = initial_optimistic_state.thread_refs_state.clone();
    let current_thread_id = initial_optimistic_state.thread_id;
    let current_thread_last_block = (current_thread_id, block_id, block_seq_no);
    new_thread_refs.update(current_thread_id, current_thread_last_block);

    let mut removed_accounts: Vec<AccountRouting> = vec![];
    let mut outbound_accounts: HashMap<
        AccountRouting,
        (Option<WrappedAccount>, Option<AccountInbox>),
    > = HashMap::new();

    let initial_messages =
        std::mem::replace(&mut initial_optimistic_state.messages, ThreadMessageQueueState::empty());
    let mut messages = ThreadMessageQueueStateDiff::builder()
        .with_consumed_messages(consumed_internal_messages)
        .with_produced_messages(settled_messages)
        .with_removed_accounts(vec![])
        .with_added_accounts(BTreeMap::new())
        .build()
        .apply(initial_messages, &db)?;

    for (routing, account) in &accounts_that_changed_their_dapp_id {
        // Note: we are using input state threads table to determine if this account should be in
        //  the descendant state. In case of thread split those accounts will be cropped into the
        // correct thread.
        if !initial_optimistic_state.does_routing_belong_to_the_state(routing) {
            removed_accounts.push(*routing);
            let account_inbox = messages.account_inbox_by_routing(routing).cloned();
            outbound_accounts.insert(*routing, (account.clone(), account_inbox));
        }
    }
    // Collect redirect stubs for newly created accounts with dapp_id != account_id
    // that stay in this thread. These go directly into durable state, bypassing
    // the builder's re-routing logic.
    let mut explicit_redirects: Vec<(AccountRouting, crate::types::account::WrappedAccount)> =
        vec![];
    for (routing, account) in &accounts_that_changed_their_dapp_id {
        if !routing.is_maybe_redirect()
            && account.is_some()
            && initial_optimistic_state.does_routing_belong_to_the_state(routing)
        {
            explicit_redirects.push((*routing, account.clone().unwrap()));
        }
    }

    let need_state_update = !outbound_accounts.is_empty() || !explicit_redirects.is_empty();
    if need_state_update {
        let mut shard_state = thread_accounts_repository.state_builder(
            &current_thread_id,
            block_height,
            &state_transition.new_state,
        );
        for (account_routing, (_, _)) in &outbound_accounts {
            // let default_account_routing = AccountRouting(
            //     DAppIdentifier(account_routing.1.clone()),
            //     account_routing.1.clone(),
            // );
            // if initial_optimistic_state.does_routing_belong_to_the_state(&default_account_routing) {

            if shard_state.account(account_routing)?.is_some() {
                tracing::debug!(target: "node", "replace account with redirect: {:?}", account_routing);
                shard_state.replace_with_redirect(account_routing)?;
            }
            // }
        }
        for (routing, wrapped_account) in &explicit_redirects {
            let redirect_routing = routing.account_id().redirect();
            let redirect_account = wrapped_account.account.with_redirect(*routing.dapp_id())?;
            tracing::debug!(
                target: "node",
                "create redirect at default routing {:?} for account with routing {:?}",
                redirect_routing,
                routing,
            );
            shard_state.add_explicit_redirect(redirect_routing, redirect_account);
        }
        state_transition.apply(shard_state.build(None)?);
    }

    messages.remove_accounts(removed_accounts);
    initial_optimistic_state.messages = messages;

    let new_state_builder = OptimisticStateImpl::builder()
        .block_seq_no(block_seq_no)
        .block_id(block_id)
        .shard_state(OptimisticShardState(state_transition.new_state.clone()))
        .account_operations(state_transition.account_operations.clone())
        .messages(initial_optimistic_state.messages)
        .high_priority_messages(initial_optimistic_state.high_priority_messages)
        .threads_table(threads_table.clone())
        .thread_id(thread_id)
        .block_info(block_info)
        .thread_refs_state(new_thread_refs)
        .cropped(initial_optimistic_state.cropped);

    #[cfg(feature = "monitor-accounts-number")]
    let new_state_builder = new_state_builder.accounts_number(updated_accounts_number);

    let new_state = new_state_builder.build();

    let cross_thread_ref_data = CrossThreadRefData::builder()
        .block_identifier(block_id)
        .block_seq_no(block_seq_no)
        .block_thread_identifier(thread_id)
        .outbound_messages(produced_internal_messages_to_other_threads)
        .outbound_accounts(outbound_accounts)
        .threads_table(threads_table.clone())
        .parent_block_identifier(initial_optimistic_state.block_id)
        .block_refs(vec![]) // set up later
        .build();

    // Return the diff *after* state_transition.apply(...) above so the caller
    // sees the explicit redirects added by postprocess. Cloning before this
    // point misses them and the BP serializes an incomplete durable_state_update,
    // which leaves descendants on synced nodes unable to resolve the redirect.
    Ok((new_state, cross_thread_ref_data, state_transition.diff.durable))
}

fn sort_produced_messages(
    messages: &mut [(MessageIdentifier, Arc<WrappedMessage>)],
    consumed: &[MessageIdentifier],
) {
    // Same-block execution need not follow (creation LT, hash) order: two
    // messages can have the same LT and execute in reverse hash order.
    // Keep executed additions first, in consumption order, so commitments and
    // FIFO overlap cancellation describe the actual execution. Only additions
    // left queued are ordered by creation LT and representation hash.
    if consumed.is_empty() {
        messages.sort_by(|a, b| a.1.cmp(&b.1));
        return;
    }
    let consumed_positions: HashMap<_, _> =
        consumed.iter().enumerate().map(|(index, id)| (id, index)).collect();
    messages.sort_by(|a, b| match (consumed_positions.get(&a.0), consumed_positions.get(&b.0)) {
        (Some(a), Some(b)) => a.cmp(b),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.1.cmp(&b.1),
    });
}

#[cfg(test)]
mod ordered_addition_tests {
    use super::*;
    use crate::types::thread_message_queue::account_messages_iterator::AccountMessagesIterator;

    // Diagnostic reproducer: exercise the real postprocess entry point, not
    // just the queue diff. No producer/TVM execution is simulated here.
    #[test]
    fn high_priority_consumption_removes_only_the_high_message() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let repository = ThreadAccountsRepository::builder(dir.path().join("durable")).build()?;
        let db = MessageDurableStorage::mem();
        let routing = AccountRouting::default();
        let message = Arc::new(WrappedMessage::from_tvm(&tvm_block::Message::with_int_header(
            tvm_block::InternalMessageHeader::default(),
        )));
        let id = message.identifier();
        let mut state = OptimisticStateImpl::zero();
        state.high_priority_messages = ThreadMessageQueueStateDiff::builder()
            .with_consumed_messages(HashMap::new())
            .with_produced_messages(HashMap::from([(routing, vec![(id.clone(), message)])]))
            .with_removed_accounts(vec![])
            .with_added_accounts(BTreeMap::new())
            .build()
            .apply(ThreadMessageQueueState::empty(), &db)?;
        let before = state.clone();
        let transition =
            repository.state_builder(&state.thread_id, 0, &state.shard_state.0).build(None)?;
        let result = postprocess(
            state,
            HashMap::new(),
            HashMap::from([(routing, vec![id.clone()])]),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            BlockIdentifier::default(),
            BlockSeqNo::default(),
            0,
            transition,
            HashMap::new(),
            BlockInfo::default(),
            ThreadIdentifier::default(),
            ThreadsTable::default(),
            db.clone(),
            &repository,
            false,
            #[cfg(feature = "monitor-accounts-number")]
            0,
        );
        let (after, _, _) = result?;
        assert!(after.high_priority_messages.is_empty());
        assert!(after.messages.is_empty());
        assert_eq!(before.high_priority_messages.length(), 1);
        // Reading for execution does not remove the message from the queue.
        let mut iter = before.high_priority_messages.iter(&db);
        assert_eq!(iter.next().unwrap().next_range(1).unwrap()[0].identifier(), id);
        assert_eq!(before.high_priority_messages.length(), 1);
        assert_eq!(
            before.high_priority_messages.iter(&db).next().unwrap().next_range(1).unwrap()[0]
                .identifier(),
            id
        );
        Ok(())
    }

    #[test]
    fn producer_and_applier_addition_orders_normalize_identically() {
        let messages: Vec<_> = [(30, 0), (20, 1), (10, 0), (20, 2)]
            .into_iter()
            .map(|(lt, at)| {
                let header = tvm_block::InternalMessageHeader {
                    created_lt: lt,
                    created_at: at.into(),
                    ..Default::default()
                };
                let message = Arc::new(WrappedMessage::from_tvm(
                    &tvm_block::Message::with_int_header(header),
                ));
                (message.identifier(), message)
            })
            .collect();
        let mut producer = messages.clone();
        let mut applier = messages.into_iter().rev().collect::<Vec<_>>();
        sort_produced_messages(&mut producer, &[]);
        sort_produced_messages(&mut applier, &[]);
        assert_eq!(producer, applier);
        assert_eq!(
            producer
                .iter()
                .map(|(_, message)| message.tvm_message().unwrap().lt().unwrap())
                .collect::<Vec<_>>(),
            vec![10, 20, 20, 30]
        );
        assert!(producer[1].0.inner().hash < producer[2].0.inner().hash);
        let before = producer.clone();
        sort_produced_messages(&mut producer, &[]);
        assert_eq!(producer, before);
    }

    #[test]
    fn same_block_consumption_precedes_hash_sorted_remainder_for_both_priorities(
    ) -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let repository = ThreadAccountsRepository::builder(dir.path().join("durable")).build()?;
        let db = MessageDurableStorage::mem();
        let routing = AccountRouting::default();
        let mut additions: Vec<_> = (1..=3)
            .map(|at| {
                let header = tvm_block::InternalMessageHeader {
                    created_lt: 4443334351,
                    created_at: at.into(),
                    ..Default::default()
                };
                let message = Arc::new(WrappedMessage::from_tvm(
                    &tvm_block::Message::with_int_header(header),
                ));
                (message.identifier(), message)
            })
            .collect();
        sort_produced_messages(&mut additions, &[]);
        // CI block 95279791 executed equal-LT messages in reverse hash order.
        let execution: Vec<_> = additions.iter().rev().map(|(id, _)| id.clone()).collect();
        for high in [false, true] {
            for count in 1..=additions.len() {
                let consumed = execution[..count].to_vec();
                let mut producer = additions.clone();
                let mut applier: Vec<_> = additions.iter().rev().cloned().collect();
                sort_produced_messages(&mut producer, &consumed);
                sort_produced_messages(&mut applier, &consumed);
                assert_eq!(producer, applier);
                assert_eq!(
                    producer[..count].iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
                    consumed
                );
                let state = OptimisticStateImpl::zero();
                let transition = repository
                    .state_builder(&state.thread_id, 0, &state.shard_state.0)
                    .build(None)?;
                let operations = HashMap::from([(routing, consumed.clone())]);
                let produced = HashMap::from([(routing, additions.clone())]);
                let (after, _, _) = postprocess(
                    state,
                    if high { HashMap::new() } else { operations.clone() },
                    if high { operations } else { HashMap::new() },
                    HashMap::new(),
                    if high { produced.clone() } else { HashMap::new() },
                    if high { HashMap::new() } else { produced },
                    HashMap::new(),
                    BlockIdentifier::default(),
                    BlockSeqNo::default(),
                    0,
                    transition,
                    HashMap::new(),
                    BlockInfo::default(),
                    ThreadIdentifier::default(),
                    ThreadsTable::default(),
                    db.clone(),
                    &repository,
                    false,
                    #[cfg(feature = "monitor-accounts-number")]
                    0,
                )?;
                let (queue, other) = if high {
                    (&after.high_priority_messages, &after.messages)
                } else {
                    (&after.messages, &after.high_priority_messages)
                };
                assert!(other.is_empty());
                let remaining: Vec<_> = queue
                    .iter(&db)
                    .flat_map(|mut inbox| inbox.next_range(10).unwrap())
                    .map(|message| message.identifier())
                    .collect();
                assert_eq!(
                    remaining,
                    additions[..additions.len() - count]
                        .iter()
                        .map(|(id, _)| id.clone())
                        .collect::<Vec<_>>()
                );
                if count == additions.len() {
                    assert!(queue.is_empty());
                } else {
                    let mut expected = crate::types::InboxCommitment::default();
                    for (id, _) in &producer {
                        expected.add(id);
                    }
                    for id in &consumed {
                        expected.consume(id);
                    }
                    assert_eq!(
                        queue.account_inbox_by_routing(&routing).unwrap().commitment,
                        expected
                    );
                }
                crate::repository::repository_impl::validate_snapshot_inboxes(&after, &[])?;
            }
        }
        Ok(())
    }

    #[test]
    fn priority_queues_keep_independent_fifo_and_unconsumed_remainders() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let repository = ThreadAccountsRepository::builder(dir.path().join("durable")).build()?;
        let db = MessageDurableStorage::mem();
        let routing = AccountRouting::default();
        // Cross-thread arrivals precede local additions even when their
        // created LTs do not happen to be globally sorted.
        let messages: Vec<_> = [1, 2, 3, 500, 4]
            .into_iter()
            .map(|lt| {
                let header =
                    tvm_block::InternalMessageHeader { created_lt: lt, ..Default::default() };
                let message = Arc::new(WrappedMessage::from_tvm(
                    &tvm_block::Message::with_int_header(header),
                ));
                (message.identifier(), message)
            })
            .collect();
        let queue = |bodies: Vec<_>| {
            ThreadMessageQueueStateDiff::builder()
                .with_consumed_messages(HashMap::new())
                .with_produced_messages(HashMap::from([(routing, bodies)]))
                .with_removed_accounts(vec![])
                .with_added_accounts(BTreeMap::new())
                .build()
                .apply(ThreadMessageQueueState::empty(), &db)
        };
        // Each priority has one old message and one preprocessed addition.
        // The final normal message is produced inside this block.
        for (high_count, normal_count) in [(0, 0), (1, 0), (2, 0), (2, 1), (2, 2), (2, 3)] {
            let mut state = OptimisticStateImpl::zero();
            state.high_priority_messages = queue(messages[..1].to_vec())?;
            state.messages = queue(messages[2..3].to_vec())?;
            let mut working = state.clone();
            working
                .high_priority_messages
                .append_messages(&HashMap::from([(routing, messages[1..2].to_vec())]));
            working.messages.append_messages(&HashMap::from([(routing, messages[3..4].to_vec())]));
            assert_eq!(working.high_priority_messages.length(), 2);
            assert_eq!(working.messages.length(), 2);
            let before = state.clone();
            let transition =
                repository.state_builder(&state.thread_id, 0, &state.shard_state.0).build(None)?;
            let (after, _, _) = postprocess(
                state,
                HashMap::from([(
                    routing,
                    messages[2..2 + normal_count].iter().map(|m| m.0.clone()).collect(),
                )]),
                HashMap::from([(
                    routing,
                    messages[..high_count].iter().map(|m| m.0.clone()).collect(),
                )]),
                HashMap::from([(routing, messages[3..4].to_vec())]),
                HashMap::from([(routing, messages[1..2].to_vec())]),
                HashMap::from([(routing, messages[4..].to_vec())]),
                HashMap::new(),
                BlockIdentifier::default(),
                BlockSeqNo::default(),
                0,
                transition,
                HashMap::new(),
                BlockInfo::default(),
                ThreadIdentifier::default(),
                ThreadsTable::default(),
                db.clone(),
                &repository,
                false,
                #[cfg(feature = "monitor-accounts-number")]
                0,
            )?;
            let ids = |queue: &ThreadMessageQueueState| {
                queue
                    .iter(&db)
                    .flat_map(|mut inbox| inbox.next_range(10).unwrap())
                    .map(|message| message.identifier())
                    .collect::<Vec<_>>()
            };
            assert_eq!(
                ids(&after.high_priority_messages),
                messages[high_count..2].iter().map(|m| m.0.clone()).collect::<Vec<_>>()
            );
            assert_eq!(
                ids(&after.messages),
                messages[2 + normal_count..].iter().map(|m| m.0.clone()).collect::<Vec<_>>()
            );
            assert_eq!(before.high_priority_messages.length(), 1);
            assert_eq!(before.messages.length(), 1);
            crate::repository::repository_impl::validate_snapshot_inboxes(&after, &[])?;
        }
        Ok(())
    }
}
