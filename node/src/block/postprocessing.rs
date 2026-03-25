use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use account_state::ThreadAccountsBuilder;
use account_state::ThreadAccountsRepository;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use tracing::instrument;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::repository::accounts::AccountsRepository;
use crate::repository::accounts::NodeThreadAccountsRef;
use crate::repository::accounts::NodeThreadAccountsRepository;
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

#[cfg(feature = "authroot_dapp_repair")]
static AUTH_ROOT_ADDRESS: &str = "0404040404040404040404040404040404040404040404040404040404040404";

#[cfg(feature = "authroot_dapp_repair")]
static AUTH_ROOT_OLD_DAPP_ID: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

#[cfg(feature = "authroot_dapp_repair")]
static AUTH_ROOT_NEW_DAPP_ID: &str =
    "0000000000000000000000000000000000000000000000000000000000000002";

#[allow(clippy::too_many_arguments)]
#[instrument(skip_all)]
pub fn postprocess(
    mut initial_optimistic_state: OptimisticStateImpl,
    consumed_internal_messages: HashMap<AccountIdentifier, HashSet<MessageIdentifier>>,
    mut produced_internal_messages_to_the_current_thread: HashMap<
        AccountIdentifier,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    accounts_that_changed_their_dapp_id: HashMap<AccountRouting, Option<WrappedAccount>>,
    block_id: BlockIdentifier,
    block_seq_no: BlockSeqNo,
    mut new_state: NodeThreadAccountsRef,
    mut produced_internal_messages_to_other_threads: HashMap<
        AccountRouting,
        Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    block_info: BlockInfo,
    thread_id: ThreadIdentifier,
    threads_table: ThreadsTable,
    block_accounts: HashSet<AccountIdentifier>,
    accounts_repo: AccountsRepository,
    thread_accounts_repository: &NodeThreadAccountsRepository,
    db: MessageDurableStorage,
    apply_to_durable: bool,
    #[cfg(feature = "monitor-accounts-number")] updated_accounts_number: u64,
    #[cfg(feature = "authroot_dapp_repair")] is_block_of_retired_version: bool,
    #[cfg(feature = "authroot_dapp_repair")] authroot_dapp_repaired: std::sync::Arc<
        parking_lot::Mutex<Option<BlockSeqNo>>,
    >,
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
    let current_thread_last_block = (current_thread_id, block_id, block_seq_no);
    new_thread_refs.update(current_thread_id, current_thread_last_block);

    let mut removed_accounts: Vec<AccountIdentifier> = vec![];
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
        if !initial_optimistic_state.does_routing_belong_to_the_state(routing) {
            removed_accounts.push(*routing.account_id());
            let account_inbox =
                initial_optimistic_state.messages.account_inbox(routing.account_id()).cloned();
            outbound_accounts.insert(*routing, (account.clone(), account_inbox));
        }
    }
    if !outbound_accounts.is_empty() {
        let mut shard_state = thread_accounts_repository.state_builder(&new_state);
        shard_state.set_apply_to_durable(apply_to_durable);
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
        new_state = shard_state.build(None)?.new_state;
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

    let mut changed_accounts = initial_optimistic_state.changed_accounts;
    let mut cached_accounts = initial_optimistic_state.cached_accounts;
    if let Some(unload_after) = accounts_repo.get_unload_after() {
        changed_accounts.extend(block_accounts.into_iter().map(|acc| (acc, block_seq_no)));
        cached_accounts.retain(|account_id, (seq_no, _)| {
            if *seq_no + accounts_repo.get_store_after() >= block_seq_no
                && !changed_accounts.contains_key(account_id)
            {
                true
            } else {
                tracing::trace!(
                    account_id = account_id.to_hex_string(),
                    "Removing account from cache"
                );
                false
            }
        });
        let mut shard_state = thread_accounts_repository.state_builder(&new_state);
        shard_state.set_apply_to_durable(apply_to_durable);
        let mut deleted = Vec::new();
        for (account_id, seq_no) in std::mem::take(&mut changed_accounts) {
            let account_routing = account_id.dapp_originator();
            if seq_no + unload_after <= block_seq_no {
                if let Some(mut account) = shard_state.account(&account_routing)? {
                    tracing::trace!(
                        account_id = account_id.to_hex_string(),
                        "Unloading account from state"
                    );
                    let state = account
                        .unload_account()
                        .map_err(|e| anyhow::format_err!("Failed to set account external: {e}"))?;
                    cached_accounts.insert(account_id, (block_seq_no, state));
                    shard_state.insert_account(&account_routing, &account);
                } else {
                    deleted.push(account_id);
                }
            } else {
                changed_accounts.insert(account_id, seq_no);
            }
        }
        if !deleted.is_empty() {
            accounts_repo.accounts_deleted(&thread_id, deleted, block_info.prev1().unwrap().end_lt);
        }
        new_state = shard_state.build(None)?.new_state;
    }

    #[cfg(feature = "authroot_dapp_repair")]
    if !is_block_of_retired_version {
        let mut lock = authroot_dapp_repaired.lock();
        if lock.is_none() && is_authroot_dapp_repaired(thread_accounts_repository, &new_state) {
            *lock = Some(BlockSeqNo::from(0));
        }
        let should_repair = match *lock {
            None => true,
            Some(seq_no) => seq_no == block_seq_no,
        };
        if should_repair {
            match repair_authroot_dappid(thread_accounts_repository, &new_state) {
                Ok(repaired_state) => {
                    new_state = repaired_state;
                }
                Err(e) => {
                    tracing::trace!("Failed to repair authRoot dapp_id: {e}");
                }
            }
            if lock.is_none() {
                *lock = Some(block_seq_no);
            }
        }
    }

    let new_state_builder = OptimisticStateImpl::builder()
        .block_seq_no(block_seq_no)
        .block_id(block_id)
        .shard_state(OptimisticShardState(new_state))
        .messages(initial_optimistic_state.messages)
        .high_priority_messages(initial_optimistic_state.high_priority_messages)
        .threads_table(threads_table.clone())
        .thread_id(thread_id)
        .block_info(block_info)
        .thread_refs_state(new_thread_refs)
        .cropped(initial_optimistic_state.cropped)
        .changed_accounts(changed_accounts)
        .cached_accounts(cached_accounts);

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

    Ok((new_state, cross_thread_ref_data))
}

#[cfg(feature = "authroot_dapp_repair")]
fn is_authroot_dapp_repaired(
    thread_accounts_repository: &NodeThreadAccountsRepository,
    state: &NodeThreadAccountsRef,
) -> bool {
    use std::str::FromStr;

    use node_types::DAppIdentifier;
    use tvm_types::UInt256;

    let new_dapp_id = match UInt256::from_str(AUTH_ROOT_NEW_DAPP_ID) {
        Ok(v) => DAppIdentifier::from(v),
        Err(_) => return false,
    };
    let account_id = match UInt256::from_str(AUTH_ROOT_ADDRESS) {
        Ok(v) => AccountIdentifier::from(v),
        Err(_) => return false,
    };

    let new_routing = AccountRouting::new(new_dapp_id, account_id);
    let mut builder = thread_accounts_repository.state_builder(state);
    builder.set_apply_to_durable(true);
    match builder.account(&new_routing) {
        Ok(Some(account_state)) => {
            let cur_dapp = account_state.get_dapp_id();
            if let Some(cur_dapp) = cur_dapp {
                if cur_dapp == new_dapp_id {
                    tracing::info!("authRoot already at new dapp_id, marking repair as done");
                    true
                } else {
                    false
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

#[cfg(feature = "authroot_dapp_repair")]
fn repair_authroot_dappid(
    thread_accounts_repository: &NodeThreadAccountsRepository,
    state: &NodeThreadAccountsRef,
) -> anyhow::Result<NodeThreadAccountsRef> {
    use std::str::FromStr;

    use account_state::ThreadStateAccount;
    use node_types::DAppIdentifier;
    use tvm_types::UInt256;

    let old_dapp_id = DAppIdentifier::from(
        UInt256::from_str(AUTH_ROOT_OLD_DAPP_ID)
            .map_err(|e| anyhow::format_err!("Failed to parse old dapp id: {e}"))?,
    );
    let new_dapp_id = DAppIdentifier::from(
        UInt256::from_str(AUTH_ROOT_NEW_DAPP_ID)
            .map_err(|e| anyhow::format_err!("Failed to parse new dapp id: {e}"))?,
    );
    let account_id = AccountIdentifier::from(
        UInt256::from_str(AUTH_ROOT_ADDRESS)
            .map_err(|e| anyhow::format_err!("Failed to parse authRoot address: {e}"))?,
    );

    let old_routing = AccountRouting::new(old_dapp_id, account_id);
    let new_routing = AccountRouting::new(new_dapp_id, account_id);

    let (updated_state, usage_tree) = state.with_tvm_usage_tree()?;
    let mut builder = thread_accounts_repository.state_builder(&updated_state);
    builder.set_apply_to_durable(true);

    let account = match builder.account(&old_routing)? {
        Some(acc) => acc,
        None => {
            tracing::trace!("authRoot not found at old routing, skipping repair");
            anyhow::bail!("authRoot not found at old routing");
        }
    };

    let thread_account = account.account()?;
    let last_trans_hash = account.last_trans_hash();
    let last_trans_lt = account.last_trans_lt();

    let new_account =
        ThreadStateAccount::new(thread_account, last_trans_hash, last_trans_lt, Some(new_dapp_id))?;

    builder.remove_account(&old_routing);
    builder.insert_account(&new_routing, &new_account);
    let transition = builder.build(Some(&usage_tree))?;

    tracing::info!("Moved authRoot from dapp_id ZERO to MV_DAPP_ID");
    Ok(transition.new_state)
}
