use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use tracing::instrument;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::message_storage::MessageDurableStorage;
use crate::repository::optimistic_shard_state::OptimisticShardState;
use crate::repository::optimistic_state::DAppIdTable;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::types::account::WrappedAccount;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AccountAddress;
use crate::types::AccountInbox;
use crate::types::AccountRouting;
use crate::types::BlockEndLT;
use crate::types::BlockIdentifier;
use crate::types::BlockInfo;
use crate::types::BlockSeqNo;
use crate::types::DAppIdentifier;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

#[allow(clippy::too_many_arguments)]
#[instrument(skip_all)]
pub fn postprocess(
    mut initial_optimistic_state: OptimisticStateImpl,
    consumed_internal_messages: HashMap<AccountAddress, HashSet<MessageIdentifier>>,
    mut produced_internal_messages_to_the_current_thread: HashMap<
        AccountAddress,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    accounts_that_changed_their_dapp_id: HashMap<AccountRouting, Option<WrappedAccount>>,
    block_id: BlockIdentifier,
    block_seq_no: BlockSeqNo,
    new_state: OptimisticShardState,
    mut produced_internal_messages_to_other_threads: HashMap<
        AccountRouting,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    new_dapp_id_table: HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)>,
    block_info: BlockInfo,
    thread_id: ThreadIdentifier,
    threads_table: ThreadsTable,
    changed_dapp_ids: DAppIdTable,
    db: MessageDurableStorage,
) -> anyhow::Result<(OptimisticStateImpl, CrossThreadRefData)> {
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
    let current_thread_id = *initial_optimistic_state.get_thread_id();
    let current_thread_last_block = (current_thread_id, block_id.clone(), block_seq_no);
    new_thread_refs.update(current_thread_id, current_thread_last_block.clone());

    let mut removed_accounts: Vec<AccountAddress> = vec![];
    let mut outbound_accounts: HashMap<
        AccountRouting,
        (Option<WrappedAccount>, Option<AccountInbox>),
    > = HashMap::new();
    let messages = ThreadMessageQueueState::build_next()
        .with_initial_state(initial_optimistic_state.messages)
        .with_consumed_messages(consumed_internal_messages)
        .with_produced_messages(produced_internal_messages_to_the_current_thread)
        .with_removed_accounts(vec![])
        .with_added_accounts(BTreeMap::new())
        .with_db(db.clone())
        .build()?;
    initial_optimistic_state.messages = messages;

    for (routing, account) in &accounts_that_changed_their_dapp_id {
        // Note: we are using input state threads table to determine if this account should be in
        //  the descendant state. In case of thread split those accounts will be cropped into the
        // correct thread.
        if !initial_optimistic_state.does_routing_belong_to_the_state(routing)? {
            removed_accounts.push(routing.1.clone());
            let account_inbox =
                initial_optimistic_state.messages.account_inbox(&routing.1).cloned();
            outbound_accounts.insert(routing.clone(), (account.clone(), account_inbox));
        }
    }

    let messages = ThreadMessageQueueState::build_next()
        .with_initial_state(initial_optimistic_state.messages)
        .with_consumed_messages(HashMap::new())
        .with_produced_messages(HashMap::new())
        .with_removed_accounts(removed_accounts)
        .with_added_accounts(BTreeMap::new())
        .with_db(db.clone())
        .build()?;
    initial_optimistic_state.messages = messages;

    let new_state = OptimisticStateImpl::builder()
        .block_seq_no(block_seq_no)
        .block_id(block_id.clone())
        .shard_state(new_state)
        .messages(initial_optimistic_state.messages)
        .threads_table(threads_table.clone())
        .thread_id(thread_id)
        .block_info(block_info)
        .dapp_id_table(new_dapp_id_table.clone())
        .thread_refs_state(new_thread_refs)
        .cropped(initial_optimistic_state.cropped)
        .changed_accounts(initial_optimistic_state.changed_accounts)
        .build();

    let cross_thread_ref_data = CrossThreadRefData::builder()
        .block_identifier(block_id.clone())
        .block_seq_no(block_seq_no)
        .block_thread_identifier(thread_id)
        .dapp_id_table_diff(changed_dapp_ids)
        .outbound_messages(produced_internal_messages_to_other_threads)
        .outbound_accounts(outbound_accounts)
        .threads_table(threads_table.clone())
        .parent_block_identifier(initial_optimistic_state.block_id.clone())
        .block_refs(vec![]) // set up later
        .build();

    Ok((new_state, cross_thread_ref_data))
}
