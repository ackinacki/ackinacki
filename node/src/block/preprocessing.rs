use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use tracing::instrument;
use tracing::trace_span;
use tvm_block::ShardStateUnsplit;

use crate::block_keeper_system::epoch::create_epoch_touch_message;
use crate::block_keeper_system::BlockKeeperData;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::repository::optimistic_shard_state::OptimisticShardState;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::CrossThreadRefData;
use crate::repository::CrossThreadRefDataRead;
use crate::storage::MessageDurableStorage;
use crate::types::account::WrappedAccount;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AccountAddress;
use crate::types::AccountInbox;
use crate::types::AccountRouting;
use crate::types::BlockIdentifier;
use crate::types::DAppIdentifier;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

type State = crate::repository::optimistic_state::OptimisticStateImpl;

pub struct PreprocessingResult {
    pub state: State,
    pub threads_table: ThreadsTable,
    pub redirected_messages: HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    pub settled_messages: HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
}
#[allow(clippy::too_many_arguments)]
#[instrument(skip_all)]
pub fn preprocess<'a, I, TRepo>(
    parent_block_state: State,
    refs: I,
    descendant_thread_identifier: &ThreadIdentifier,
    repository: &TRepo,
    slashing_messages: Vec<Arc<WrappedMessage>>,
    epoch_block_keeper_data: Vec<BlockKeeperData>,
    message_db: MessageDurableStorage,
    metrics: Option<BlockProductionMetrics>,
) -> anyhow::Result<PreprocessingResult>
where
    I: std::iter::Iterator<Item = &'a CrossThreadRefData>,
    CrossThreadRefData: 'a,
    TRepo: CrossThreadRefDataRead,
{
    let in_table = parent_block_state.get_produced_threads_table().clone();
    let mut preprocessed_state = parent_block_state;
    tracing::trace!(
        "preprocessing: {} slashing_messages: {slashing_messages:?}",
        slashing_messages.len()
    );

    // -- Ensure parent reference ---
    //
    // Parent is always referenced. And should not be processed as cross-thread referenced block.
    //
    preprocessed_state.thread_refs_state.update(
        *descendant_thread_identifier,
        (
            *descendant_thread_identifier,
            preprocessed_state.block_id.clone(),
            preprocessed_state.block_seq_no,
        ),
    );

    // Note: OutMsg is not updataed in the shard state now.
    // (not needed) ~~Take parent block OutMsgs and remove all external messages from the state~~
    // ...

    // --- Get all added cross-thread references on this step ---
    //
    // Blocks referenced here will always be from another thread. This includes a case
    // when this thread is a child of another thread and we reference to the descendants
    // or the original thread.
    //
    let mut ref_data = vec![];
    for state in refs {
        ref_data.push(state.as_reference_state_data());
    }
    let mut all_referenced_blocks = preprocessed_state
        .thread_refs_state
        .move_refs(ref_data.into_iter().map(|e| e.1).collect(), |block_id| {
            repository.get_cross_thread_ref_data(block_id)
        })?;
    let _ = tracing::span!(
        tracing::Level::INFO,
        "all_referenced_blocks",
        referenced_blocks_cnt = all_referenced_blocks.len()
    );
    all_referenced_blocks.sort_by(|a, b| match a.block_seq_no().cmp(b.block_seq_no()) {
        std::cmp::Ordering::Equal => {
            match BlockIdentifier::compare(a.block_identifier(), b.block_identifier()) {
                std::cmp::Ordering::Equal => {
                    panic!("this should not happen: all referenced blocks must be unique")
                }
                e => e,
            }
        }
        e => e,
    });

    // --- Update dapp id table ---
    //

    // We need to know:
    // - what dapps were updated
    // - what dapps were removed from this thread
    //
    #[cfg(feature = "allow-threads-merge")]
    compile_error!("need to merge state threads table and DAPP table with other threads");
    let mut preprocessed_state = trace_span!("merge dapp id tables").in_scope(|| {
        for block_referenced in all_referenced_blocks.iter() {
            preprocessed_state.threads_table.merge(block_referenced.threads_table())?;
        }
        Ok::<_, anyhow::Error>(preprocessed_state)
    })?;

    tracing::trace!("Start crop");
    // --- Handle split thread case ---
    preprocessed_state.crop(descendant_thread_identifier, &in_table, message_db.clone())?;

    let mut preprocessed_state = trace_span!("").in_scope(|| {
        let x = import_migrating_accounts_with_their_inboxes(
            &all_referenced_blocks,
            &in_table,
            descendant_thread_identifier,
            preprocessed_state,
            message_db.clone(),
            metrics,
        )?;
        preprocessed_state = x;
        Ok::<_, anyhow::Error>(preprocessed_state)
    })?;
    // Import messages and either settle them or move back to the outbox with the renewed route

    let mut settled_messages: HashMap<
        AccountAddress,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    > = HashMap::new();
    let redirected_messages: HashMap<
        AccountRouting,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    > = HashMap::new();
    trace_span!("get ref messages").in_scope(|| {
        for block_referenced in all_referenced_blocks.iter() {
            for (route, messages) in block_referenced.outbound_messages().iter() {
                if !in_table.is_match(route, *descendant_thread_identifier) {
                    continue;
                }
                // It is possible that this message should be forwarded with the updated route.
                let account_address: AccountAddress = route.1.clone();
                // let actual_route = preprocessed_state.get_account_routing(&account_address, None);
                // if &actual_route != route {
                //     // Forward with the new route
                //     redirected_messages
                //         .entry(actual_route)
                //         .and_modify(|e| e.extend_from_slice(messages))
                //         .or_insert(messages.clone());
                // } else {
                // Settle
                settled_messages
                    .entry(account_address)
                    .and_modify(|e| e.extend_from_slice(messages))
                    .or_insert(messages.clone());
                // }
            }
        }
        Ok::<_, anyhow::Error>(())
    })?;
    let x = preprocessed_state.messages;
    tracing::trace!(
        "preprocessing: ThreadMessageQueueState::build_next: settled_messages.len()={} ",
        settled_messages.len()
    );
    let y = ThreadMessageQueueState::build_next()
        .with_initial_state(x)
        .with_consumed_messages(HashMap::new())
        .with_removed_accounts(vec![])
        .with_added_accounts(BTreeMap::new())
        .with_produced_messages(settled_messages.clone())
        .with_db(message_db.clone())
        .build()?;
    preprocessed_state.messages = y;

    let high_priority_messages = preprocessed_state.high_priority_messages;
    let mut high_priority_messages_map = convert_slashing_messages(slashing_messages)?;
    convert_epoch_messages(&mut high_priority_messages_map, epoch_block_keeper_data)?;
    tracing::trace!(
        "preprocessing: ThreadMessageQueueState::build_next: slashing_messages.len()={} ",
        high_priority_messages_map.len()
    );
    let queue_message_state = ThreadMessageQueueState::build_next()
        .with_initial_state(high_priority_messages)
        .with_consumed_messages(HashMap::new())
        .with_removed_accounts(vec![])
        .with_added_accounts(BTreeMap::new())
        .with_produced_messages(high_priority_messages_map)
        .with_db(message_db.clone())
        .build()?;
    //    preprocessed_state.add_slashing_messages(slashing_messages)?;
    preprocessed_state.high_priority_messages = queue_message_state;

    Ok(PreprocessingResult {
        state: preprocessed_state,
        threads_table: in_table,
        redirected_messages,
        settled_messages,
    })
}

pub fn convert_slashing_messages(
    slashing_messages: Vec<Arc<WrappedMessage>>,
) -> anyhow::Result<HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>> {
    let mut slashing_messages_map: HashMap<
        AccountAddress,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    > = HashMap::new();
    for message in slashing_messages.into_iter() {
        let msg = message.message.clone();
        let info = msg.int_header().unwrap();
        let dst = info.dst.address();
        slashing_messages_map
            .entry(dst.into())
            .or_default()
            .push((MessageIdentifier::from(&*message), message));
    }
    Ok(slashing_messages_map)
}

pub fn convert_epoch_messages(
    high_priority_map: &mut HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    epoch_message: Vec<BlockKeeperData>,
) -> anyhow::Result<()> {
    let now = std::time::SystemTime::now();
    let time = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as u32;
    for data in epoch_message.into_iter() {
        let msg = create_epoch_touch_message(&data, time).map_err(|e| {
            anyhow::Error::msg(format!("Failed to create epoch touch message: {e}"))
        })?;
        let wrapped_message = WrappedMessage { message: msg.clone() };
        let info =
            msg.int_header().ok_or_else(|| anyhow::Error::msg("Failed to get message header"))?;
        let dst = info.dst.address();
        high_priority_map
            .entry(dst.into())
            .or_default()
            .push((MessageIdentifier::from(&wrapped_message), Arc::new(wrapped_message)));
    }
    Ok(())
}

fn import_migrating_accounts_with_their_inboxes(
    all_referenced_blocks: &[CrossThreadRefData],
    in_table: &ThreadsTable,
    descendant_thread_identifier: &ThreadIdentifier,
    mut state: State,
    message_db: MessageDurableStorage,
    metrics: Option<BlockProductionMetrics>,
) -> anyhow::Result<State> {
    tracing::trace!(target: "node", "preprocess: {:?} {:?}", state.block_id, state.thread_id);
    let mut migrated_accounts = vec![];
    let mut migrated_inboxes: BTreeMap<AccountAddress, AccountInbox> = BTreeMap::new();
    let mut outbound_accounts_number = 0;
    let mut removed_accounts = vec![];
    for block_referenced in all_referenced_blocks.iter() {
        for (route, (account_state, inbox)) in block_referenced.outbound_accounts().iter() {
            if account_state.is_none() {
                let default_routing =
                    AccountRouting(DAppIdentifier(route.1.clone()), route.1.clone());
                let (mask, _) = in_table
                    .rows()
                    .find(|(_, thread)| thread == &state.thread_id)
                    .expect("Failed to find thread mask in table");
                if mask.is_match(&default_routing) {
                    tracing::trace!(target: "node", "add to removed");
                    removed_accounts.push(route.1.clone());
                    continue;
                }
            }
            if !in_table.is_match(route, *descendant_thread_identifier) {
                continue;
            }
            let account_address: AccountAddress = route.1.clone();
            let actual_dapp_id = account_state
                .clone()
                .and_then(|acc| acc.account.clone().get_dapp_id().cloned())
                .unwrap_or(account_address.clone().0);
            let actual_route = AccountRouting(actual_dapp_id.into(), account_address.clone());
            assert!(
                &actual_route == route,
                concat!(
                    "Account dapp can be changed only in the account responsible thread. ",
                    "Actual route mismatching the route passed may indicate problems ",
                    "in the dapp table updates.",
                )
            );
            if let Some(account) = account_state {
                migrated_accounts.push(account.clone());
            }
            if let Some(inbox) = inbox {
                migrated_inboxes.insert(account_address, inbox.clone());
            }
            outbound_accounts_number += 1;
        }
    }

    if let Some(m) = metrics.as_ref() {
        m.report_outbound_accounts(outbound_accounts_number, descendant_thread_identifier)
    }

    settle_accounts(&mut state.shard_state, migrated_accounts, removed_accounts)?;
    let x = state.messages;
    let y = ThreadMessageQueueState::build_next()
        .with_initial_state(x)
        .with_consumed_messages(HashMap::new())
        .with_removed_accounts(vec![])
        .with_added_accounts(migrated_inboxes)
        .with_produced_messages(HashMap::new())
        .with_db(message_db.clone())
        .build()?;
    state.messages = y;
    Ok(state)
}

fn settle_accounts(
    shard_state: &mut OptimisticShardState,
    migrated_accounts: Vec<WrappedAccount>,
    removed_accounts: Vec<AccountAddress>,
) -> anyhow::Result<()> {
    if migrated_accounts.is_empty() && removed_accounts.is_empty() {
        return Ok(());
    }
    let mut binding = shard_state.into_shard_state();
    let existing_state = Arc::<ShardStateUnsplit>::make_mut(&mut binding);
    let mut existing_accounts = existing_state
        .read_accounts()
        .map_err(|e| anyhow::format_err!("Failed to read accounts: {e}"))?;
    for wrapped_account in migrated_accounts.into_iter() {
        tracing::trace!("migrate account: {}", wrapped_account.account_id.to_hex_string());
        existing_accounts
            .insert(&wrapped_account.account_id.0, &wrapped_account.account)
            .map_err(|e| anyhow::format_err!("Failed to save account: {e}"))?;
    }
    for acc_id in removed_accounts.iter() {
        tracing::trace!("removed account: {:?}", acc_id);
        existing_accounts
            .remove(&acc_id.0)
            .map_err(|e| anyhow::format_err!("Failed to remove acc: {acc_id} {e}"))?;
    }
    existing_state
        .write_accounts(&existing_accounts)
        .map_err(|e| anyhow::format_err!("Failed to save accounts: {e}"))?;
    *shard_state = binding.into();
    Ok(())
}
