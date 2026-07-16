use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
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
use crate::types::AccountInbox;
use crate::types::BlockInfo;
use crate::types::BlockSeqNo;
use crate::types::ThreadsTable;

#[allow(clippy::too_many_arguments)]
#[instrument(skip_all)]
pub fn postprocess(
    mut initial_optimistic_state: OptimisticStateImpl,
    consumed_internal_messages: HashMap<AccountRouting, HashSet<MessageIdentifier>>,
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
    // Prepare produced_internal_messages_to_the_current_thread
    for (addr, messages) in produced_internal_messages_to_the_current_thread.iter_mut() {
        let mut sorted = if let Some(consumed_messages) = consumed_internal_messages.get(addr) {
            let ids_set =
                HashSet::<MessageIdentifier>::from_iter(messages.iter().map(|(id, _)| id.clone()));
            let intersection: HashSet<&MessageIdentifier> =
                HashSet::from_iter(ids_set.intersection(consumed_messages));
            let mut consumed_part = vec![];
            messages.retain(|el| {
                if intersection.contains(&el.0) {
                    consumed_part.push((el.0.clone(), el.1.clone()));
                    false
                } else {
                    true
                }
            });
            consumed_part
        } else {
            vec![]
        };
        messages.sort_by(|a, b| a.1.cmp(&b.1));
        sorted.extend(messages.clone());
        *messages = sorted;
    }

    produced_internal_messages_to_other_threads
        .iter_mut()
        .for_each(|(_addr, messages)| messages.sort_by(|a, b| a.1.cmp(&b.1)));

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
    let mut messages = ThreadMessageQueueState::build_next()
        .with_initial_state(initial_messages)
        .with_consumed_messages(consumed_internal_messages)
        .with_produced_messages(produced_internal_messages_to_the_current_thread)
        .with_removed_accounts(vec![])
        .with_added_accounts(BTreeMap::new())
        .with_db(db.clone())
        .build()?;

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
