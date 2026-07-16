// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use account_state::DurableThreadAccountsState;
use account_state::ThreadAccountsRepository;
use account_state::ThreadAccountsState;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifierPath;
use node_types::ThreadIdentifier;
use tvm_block::ShardAccount;
use tvm_block::ShardAccounts;
use tvm_block::ShardStateUnsplit;

use crate::bitmask::mask::Bitmask;
use crate::types::ThreadsTable;

fn strip_middle(s: &str, keep_start: usize, keep_end: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len();

    if keep_start + keep_end >= len {
        return s.to_string();
    }

    let start: String = chars.iter().take(keep_start).collect();
    let end: String =
        chars.iter().rev().take(keep_end).collect::<Vec<_>>().into_iter().rev().collect();

    format!("{}..{}", start, end)
}

// Shard state contains accounts and out messages produced by these accounts.
// To filter out_messages, the destination address + dapp ID table is used to produce the
// destination route. If the destination route doesn’t match the given thread id with the given
// threads table it should be moved to cross thread messages for further processing.
// To filter accounts we calculate current account routing (<DApp_id>,<Account_id>) and check
// whether it matches the given thread id with the given threads table. If it does not match,
// remove it from the state.
#[allow(clippy::too_many_arguments)]
pub(crate) fn crop_shard_state_based_on_threads_table<F>(
    thread_accounts_repository: &ThreadAccountsRepository,
    shard_state: ThreadAccountsState,
    threads_table: &ThreadsTable,
    thread_id: ThreadIdentifier,
    // TODO: remove
    _block_id: BlockIdentifier,
    optimization_skip_shard_accounts_crop: bool,
    removed_tvm_accounts_buffer: &mut Vec<AccountRouting>,
    mut on_tvm_account_callback: F,
    _apply_to_durable: bool,
) -> anyhow::Result<ThreadAccountsState>
where
    F: FnMut(&AccountRouting),
{
    tracing::trace!(target: "monit", "crop thread: {}", thread_id.to_hex_string().chars().take(8).collect::<String>());
    for (mask, thread) in threads_table.rows() {
        let mask_str = strip_middle(&mask.mask_bits().dapp_id().to_hex_string(), 1, 1);
        let mean_str = strip_middle(&mask.meaningful_mask_bits().dapp_id().to_hex_string(), 1, 1);
        let thread_str = strip_middle(&thread.to_hex_string(), 6, 2);
        tracing::trace!(target: "monit", "crop table mask: {mask_str} meaningful: {mean_str}, thread: {thread_str}");
    }
    tracing::trace!(target: "node", "crop_shard_state_based_on_threads_table: {thread_id:?} {threads_table:?}");

    let mut cropped_state = shard_state.clone();

    if !optimization_skip_shard_accounts_crop {
        // Prepare a buffer for account keys that will be removed from the shard state

        let mut tvm_account_ids_to_remove_from_tvm_state = vec![];
        let mut tvm_account_routings_to_remove_from_message_queue = vec![];
        thread_accounts_repository
            .state_iterate_tvm_accounts(&shard_state, |account_id, shard_account| {
                // Calculate current account routing
                let account_routing = account_id.routing_or_redirect(shard_account.get_dapp_id());
                on_tvm_account_callback(&account_routing);

                // If account routing matches thread leave it in the state
                if !threads_table.is_match(&account_routing, thread_id) {
                    tvm_account_ids_to_remove_from_tvm_state.push(*account_id);
                    tvm_account_routings_to_remove_from_message_queue.push(account_routing);
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iterate and split accounts: {e}"))?;

        removed_tvm_accounts_buffer.extend(&tvm_account_routings_to_remove_from_message_queue);

        let (cropped_tvm_state, cropped_tvm_accounts) = crop_tvm_state(
            threads_table,
            &thread_id,
            cropped_state.tvm.shard_state.clone(),
            &tvm_account_ids_to_remove_from_tvm_state,
        )?;

        let cropped_durable_state = crop_durable_state(
            thread_accounts_repository,
            threads_table,
            &thread_id,
            cropped_state.durable.clone(),
        )?;

        cropped_state = ThreadAccountsState::new(
            cropped_durable_state,
            cropped_tvm_state,
            cropped_tvm_accounts,
        );
    }
    Ok(cropped_state)
}

fn crop_durable_state(
    thread_accounts_repository: &ThreadAccountsRepository,
    threads_table: &ThreadsTable,
    thread_id: &ThreadIdentifier,
    mut cropped_state: DurableThreadAccountsState,
) -> anyhow::Result<DurableThreadAccountsState> {
    let durable_repo = thread_accounts_repository.durable_map_repo();

    let thread_mask = threads_table.rows().find_map(|(row_mask, row_thread)| {
        if row_thread == thread_id {
            Some(row_mask.clone())
        } else {
            None
        }
    });
    if let Some(thread_mask) = thread_mask {
        cropped_state =
            durable_repo.state_split(&cropped_state, bitmask_to_dapp_path(&thread_mask))?.1;
    }
    Ok(cropped_state)
}

fn crop_tvm_state(
    threads_table: &ThreadsTable,
    thread_id: &ThreadIdentifier,
    mut cropped_tvm_state: ShardStateUnsplit,
    account_ids_to_remove_from_tvm_state: &Vec<AccountIdentifier>,
) -> anyhow::Result<(ShardStateUnsplit, ShardAccounts)> {
    let mut cropped_tvm_accounts = cropped_tvm_state
        .read_accounts()
        .map_err(|e| anyhow::format_err!("Failed to read accounts: {e}"))?;
    // Clear removed accounts from the state
    for account_id in account_ids_to_remove_from_tvm_state {
        let account_routing = account_id.redirect();
        let (mask, _) = threads_table
            .rows()
            .find(|(_, thread)| thread == thread_id)
            .expect("Failed to find thread mask in table");
        if !mask.is_match(&account_routing) {
            tracing::debug!(target: "node", "remove account: {:?}", account_routing);
            cropped_tvm_accounts.remove(&account_routing.account_id().into()).map_err(|e| {
                anyhow::format_err!("Failed to remove account from shard state: {e}")
            })?;
        } else {
            tracing::debug!(target: "node", "replace account with redirect: {:?}", account_routing);
            let acc = cropped_tvm_accounts
                .account(&account_routing.account_id().into())
                .map_err(|e| anyhow::format_err!("Failed to find account in shard state: {e}"))?
                .expect("Failed to find account in shard state");
            let acc = ShardAccount::with_redirect(
                acc.last_trans_hash().clone(),
                acc.last_trans_lt(),
                acc.get_dapp_id().cloned(),
            )
            .map_err(|e| anyhow::format_err!("Failed to replace account with redirect: {e}"))?;
            cropped_tvm_accounts
                .insert(&account_id.into(), &acc)
                .map_err(|e| anyhow::format_err!("Failed to replace account with redirect: {e}"))?;
            tracing::debug!(target: "node", "successful replace account with redirect: {:?}", account_routing);
        }
    }
    cropped_tvm_state
        .write_accounts(&cropped_tvm_accounts)
        .map_err(|e| anyhow::format_err!("Failed to write accounts to shard state: {e}"))?;
    Ok((cropped_tvm_state, cropped_tvm_accounts))
}

fn bitmask_to_dapp_path(bitmask: &Bitmask<AccountRouting>) -> DAppIdentifierPath {
    let meaningful = bitmask.meaningful_mask_bits();
    let mut len: u8 = 0;
    for i in 0..256u16 {
        if meaningful.get_bit(i as usize) {
            len += 1;
        } else {
            break;
        }
    }
    DAppIdentifierPath { prefix: *bitmask.mask_bits().dapp_id(), len }
}
