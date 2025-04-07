use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use tracing::instrument;
use tracing::trace_span;
use tvm_block::ShardStateUnsplit;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::message_storage::MessageDurableStorage;
use crate::repository::optimistic_shard_state::OptimisticShardState;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::CrossThreadRefData;
use crate::repository::CrossThreadRefDataRead;
use crate::types::account::WrappedAccount;
use crate::types::thread_message_queue::ThreadMessageQueueState;
use crate::types::AccountAddress;
use crate::types::AccountInbox;
use crate::types::AccountRouting;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

type State = crate::repository::optimistic_state::OptimisticStateImpl;

pub struct PreprocessingResult {
    pub state: State,
    pub threads_table: ThreadsTable,
    pub redirected_messages: HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    pub settled_messages: HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
}

#[instrument(skip_all)]
pub fn preprocess<'a, I, TRepo>(
    parent_block_state: State,
    refs: I,
    descendant_thread_identifier: &ThreadIdentifier,
    repository: &TRepo,
    slashing_messages: Vec<Arc<WrappedMessage>>,
    message_db: MessageDurableStorage,
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
            preprocessed_state.merge_dapp_id_tables(block_referenced.dapp_id_table_diff())?;
            preprocessed_state.threads_table.merge(block_referenced.threads_table())?;
        }
        Ok::<_, anyhow::Error>(preprocessed_state)
    })?;

    // --- Handle split thread case ---
    preprocessed_state.crop(descendant_thread_identifier, &in_table, message_db.clone())?;

    let mut preprocessed_state = trace_span!("").in_scope(|| {
        let x = import_migrating_accounts_with_their_inboxes(
            &all_referenced_blocks,
            &in_table,
            descendant_thread_identifier,
            preprocessed_state,
            message_db.clone(),
        )?;
        preprocessed_state = x;
        Ok::<_, anyhow::Error>(preprocessed_state)
    })?;

    // Import messages and either settle them or move back to the outbox with the renewed route

    let mut settled_messages: HashMap<
        AccountAddress,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    > = HashMap::new();
    let mut redirected_messages: HashMap<
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
                let actual_route = preprocessed_state.get_account_routing(&account_address)?;
                if &actual_route != route {
                    // Forward with the new route
                    redirected_messages
                        .entry(actual_route)
                        .and_modify(|e| e.extend_from_slice(messages))
                        .or_insert(messages.clone());
                } else {
                    // Settle
                    settled_messages
                        .entry(account_address)
                        .and_modify(|e| e.extend_from_slice(messages))
                        .or_insert(messages.clone());
                }
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

    preprocessed_state.add_slashing_messages(slashing_messages)?;

    Ok(PreprocessingResult {
        state: preprocessed_state,
        threads_table: in_table,
        redirected_messages,
        settled_messages,
    })
}

fn import_migrating_accounts_with_their_inboxes(
    all_referenced_blocks: &[CrossThreadRefData],
    in_table: &ThreadsTable,
    descendant_thread_identifier: &ThreadIdentifier,
    mut state: State,
    message_db: MessageDurableStorage,
) -> anyhow::Result<State> {
    let mut migrated_accounts = vec![];
    let mut migrated_inboxes: BTreeMap<AccountAddress, AccountInbox> = BTreeMap::new();
    for block_referenced in all_referenced_blocks.iter() {
        for (route, (account_state, inbox)) in block_referenced.outbound_accounts().iter() {
            if !in_table.is_match(route, *descendant_thread_identifier) {
                continue;
            }
            let account_address: AccountAddress = route.1.clone();
            let actual_route = state.get_account_routing(&account_address)?;
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
        }
    }
    settle_accounts(&mut state.shard_state, migrated_accounts)?;
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
) -> anyhow::Result<()> {
    let mut binding = shard_state.into_shard_state();
    let existing_state = Arc::<ShardStateUnsplit>::make_mut(&mut binding);
    let mut existing_accounts = existing_state
        .read_accounts()
        .map_err(|e| anyhow::format_err!("Failed to read accounts: {e}"))?;
    for wrapped_account in migrated_accounts.into_iter() {
        existing_accounts
            .insert_with_aug(
                &wrapped_account.account_id,
                &wrapped_account.account,
                &wrapped_account.aug,
            )
            .map_err(|e| anyhow::format_err!("Failed to save account: {e}"))?;
    }
    existing_state
        .write_accounts(&existing_accounts)
        .map_err(|e| anyhow::format_err!("Failed to save accounts: {e}"))?;
    *shard_state = binding.into();
    Ok(())
}
