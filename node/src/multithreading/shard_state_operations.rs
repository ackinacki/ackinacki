// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use account_state::ThreadAccountsBuilder;
use account_state::ThreadAccountsRepository;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;

use crate::repository::accounts::NodeThreadAccountsRef;
use crate::repository::accounts::NodeThreadAccountsRepository;
use crate::types::ThreadsTable;

// Shard state contains accounts and out messages produced by these accounts.
// To filter out_messages, the destination address + dapp ID table is used to produce the
// destination route. If the destination route doesn’t match the given thread id with the given
// threads table it should be moved to cross thread messages for further processing.
// To filter accounts we calculate current account routing (<DApp_id>,<Account_id>) and check
// whether it matches the given thread id with the given threads table. If it does not match,
// remove it from the state.
#[allow(clippy::too_many_arguments)]
pub(crate) fn crop_shard_state_based_on_threads_table<F>(
    thread_accounts_repository: &NodeThreadAccountsRepository,
    shard_state: NodeThreadAccountsRef,
    threads_table: &ThreadsTable,
    thread_id: ThreadIdentifier,
    // TODO: remove
    _block_id: BlockIdentifier,
    optimization_skip_shard_accounts_crop: bool,
    removed_accounts_buffer: &mut Vec<AccountIdentifier>,
    mut on_account_callback: F,
    apply_to_durable: bool,
) -> anyhow::Result<NodeThreadAccountsRef>
where
    F: FnMut(&AccountRouting),
{
    tracing::trace!(target: "node", "crop_shard_state_based_on_threads_table: {thread_id:?} {threads_table:?}");
    let mut next_state = thread_accounts_repository.state_builder(&shard_state);
    next_state.set_apply_to_durable(apply_to_durable);

    if !optimization_skip_shard_accounts_crop {
        // Prepare buffer for account keys that will be removed from the shard state
        let mut keys_to_remove_from_state = vec![];
        thread_accounts_repository
            .state_iterate_all_accounts(&shard_state, |routing, shard_account| {
                let account_id = routing.account_id();
                // Calculate current account routing
                let account_routing =
                    account_id.optional_dapp_originator(shard_account.get_dapp_id());
                on_account_callback(&account_routing);

                // If account routing matches thread leave it in the state
                if !threads_table.is_match(&account_routing, thread_id) {
                    keys_to_remove_from_state.push(*routing.account_id());
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iterate and split accounts: {e}"))?;

        removed_accounts_buffer.extend(&keys_to_remove_from_state);
        // Clear removed accounts from the state
        for key in keys_to_remove_from_state {
            let account_routing = key.dapp_originator();
            let (mask, _) = threads_table
                .rows()
                .find(|(_, thread)| thread == &thread_id)
                .expect("Failed to find thread mask in table");
            if !mask.is_match(&account_routing) {
                tracing::debug!(target: "node", "remove account: {:?}", account_routing);
                next_state.remove_account(&account_routing);
            } else {
                tracing::debug!(target: "node", "replace account with redirect: {:?}", account_routing);
                next_state.replace_with_redirect(&account_routing)?;
                let acc = next_state.account(&account_routing)?;
                assert!(acc.is_some());
                assert!(acc.unwrap().is_redirect());
                tracing::debug!(target: "node", "successful replace account with redirect: {:?}", account_routing);
            }
        }
    }
    Ok(next_state.build(None)?.new_state)
}
