// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::ops::Deref;
use std::sync::Arc;

use tvm_block::HashmapAugType;
use tvm_block::OutMsgQueue;
use tvm_block::ShardStateUnsplit;
use tvm_types::AccountId;
use tvm_types::HashmapRemover;
use tvm_types::HashmapType;

use crate::multithreading::account::get_account_routing_for_account;
use crate::types::AccountAddress;
use crate::types::AccountRouting;
use crate::types::BlockIdentifier;
use crate::types::DAppIdentifier;
use crate::types::ThreadIdentifier;
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
    shard_state: Arc<ShardStateUnsplit>,
    threads_table: &ThreadsTable,
    thread_id: ThreadIdentifier,
    // TODO: remove
    _block_id: BlockIdentifier,
    optimization_skip_shard_accounts_crop: bool,
    removed_accounts_buffer: &mut Vec<AccountAddress>,
    mut on_account_callback: F,
) -> anyhow::Result<Arc<ShardStateUnsplit>>
where
    F: FnMut(&AccountId),
{
    tracing::trace!(target: "node", "crop_shard_state_based_on_threads_table: {thread_id:?} {threads_table:?}");
    let mut shard_state = shard_state.deref().clone();
    // Get accounts from the shard state
    let mut shard_accounts = shard_state
        .read_accounts()
        .map_err(|e| anyhow::format_err!("Failed to read shard state accounts: {e}"))?;

    // Get out messages from the shard state
    let mut out_messages_queue_info = shard_state
        .read_out_msg_queue_info()
        .map_err(|e| anyhow::format_err!("Failed to read shard state messages queue: {e}"))?;

    // Prepare buffer for message keys that will be removed from thread A shard state
    let mut message_keys_to_remove_from_state = vec![];

    out_messages_queue_info
        .out_queue()
        .iterate_slices(|key, message_slice| {
            let (enqueued_message, _create_lt) =
                OutMsgQueue::value_aug(&mut message_slice.clone())?;
            let message = enqueued_message.read_out_msg()?.read_message()?;

            // To define where to put out message, we should find a destination account routing.
            // check message
            if let Some(header) = message.int_header() {
                let dest_address = header.dst.address().clone().into();
                let dest_account_routing = if let Some(dapp_id) = header.dest_dapp_id.clone() {
                    AccountRouting(dapp_id.clone().into(), dest_address)
                } else {
                    AccountRouting(DAppIdentifier(dest_address.clone()), dest_address)
                };

                // If destination account routing doesn't match the thread, remove it and save to cross thread messages
                if !threads_table.is_match(&dest_account_routing, thread_id) {
                    message_keys_to_remove_from_state.push(key);
                }
            } else {
                // If message destination is not internal, remove it
                message_keys_to_remove_from_state.push(key);
            }

            Ok(true)
        })
        .map_err(|e| anyhow::format_err!("Failed to iterate and crop out messages: {e}"))?;
    // Clear removed messages from the state
    for key in message_keys_to_remove_from_state {
        out_messages_queue_info
            .out_queue_mut()
            .remove(key)
            .map_err(|e| anyhow::format_err!("Failed to remove messages from out queue: {e}"))?;
    }
    shard_state
        .write_out_msg_queue_info(&out_messages_queue_info)
        .map_err(|e| anyhow::format_err!("Failed to write messages data to shard state: {e}"))?;

    if !optimization_skip_shard_accounts_crop {
        // Prepare buffer for account keys that will be removed from the shard state
        let mut keys_to_remove_from_state = vec![];
        shard_accounts
            .iterate_accounts(|address, shard_account, _| {
                let address_id = AccountId::from(&address);
                // Calculate current account routing
                let account_routing = get_account_routing_for_account(
                    address_id.clone(),
                    shard_account.get_dapp_id().cloned(),
                );
                on_account_callback(&address_id);

                // If account routing matches thread leave it in the state
                if !threads_table.is_match(&account_routing, thread_id) {
                    keys_to_remove_from_state.push(address);
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to iterate and split accounts: {e}"))?;

        removed_accounts_buffer
            .extend(keys_to_remove_from_state.iter().map(|addr| AccountAddress(addr.clone())));
        // Clear removed accounts from the state
        for key in keys_to_remove_from_state {
            let default_account_routing =
                AccountRouting(key.clone().into(), AccountAddress(key.clone()));
            let (mask, _) = threads_table
                .rows()
                .find(|(_, thread)| thread == &thread_id)
                .expect("Failed to find thread mask in table");
            if !mask.is_match(&default_account_routing) {
                tracing::info!(target: "node", "remove account: {:?}", default_account_routing);
                shard_accounts.remove(&key).map_err(|e| {
                    anyhow::format_err!("Failed to remove account from shard state: {e}")
                })?;
            } else {
                tracing::info!(target: "node", "replace account with redirect: {:?}", default_account_routing);
                shard_accounts.replace_with_redirect(&key).map_err(|e| {
                    anyhow::format_err!("Failed to replace account with redirect: {e}")
                })?;
                let acc = shard_accounts.account(&key.into()).map_err(|e| {
                    anyhow::format_err!("Failed to load redirect account from shard state: {e}")
                })?;
                assert!(acc.is_some());
                assert!(acc.unwrap().is_redirect());
                tracing::info!(target: "node", "successful replace account with redirect: {:?}", default_account_routing);
            }
        }
        shard_state.write_accounts(&shard_accounts).map_err(|e| {
            anyhow::format_err!("Failed to write accounts to filtered shard state: {e}")
        })?;
    }
    Ok(Arc::new(shard_state))
}
