// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::ops::Deref;
use std::sync::Arc;

use tvm_block::Account;
use tvm_block::Augmentation;
use tvm_block::Deserializable;
use tvm_block::HashmapAugType;
use tvm_block::OutMsgQueue;
use tvm_block::ShardAccount;
use tvm_block::ShardAccounts;
use tvm_block::ShardStateUnsplit;
use tvm_types::HashmapRemover;
use tvm_types::HashmapType;

use crate::multithreading::account::get_account_routing_for_account;
use crate::multithreading::account::get_account_routing_for_account_before_dapp_id_init;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

// Shard state contains accounts and out messages produced by these accounts.
// Function splits shard state of thread A in two shard state that match given threads A nd B.
// Accounts are split according to their DAPP id and address, but if account doesn't match any of
// two given threads, it's processed as it has None DAPP id.
// Messages are split according to their destination, but if destination doesn't match any of tho
// given threads, it's processed according its source.
pub(crate) fn split_shard_state_based_on_threads_table(
    shard_state: Arc<ShardStateUnsplit>,
    threads_table: &ThreadsTable,
    thread_id_a: ThreadIdentifier,
    thread_id_b: ThreadIdentifier,
) -> anyhow::Result<(Arc<ShardStateUnsplit>, Arc<ShardStateUnsplit>)> {
    let mut shard_state_a = shard_state.deref().clone();
    // Get accounts from shard state A and prepare buffer for shard state B
    let mut shard_accounts_a = shard_state
        .read_accounts()
        .map_err(|e| anyhow::format_err!("Failed to read shard state accounts: {e}"))?;
    let mut shard_accounts_b = ShardAccounts::new();

    // Initialize split shard state as clone of the parent to keep extra data
    let mut shard_state_b = shard_state_a.clone();

    // Get out messages from shard state A and prepare buffer for shard state B
    let mut out_messages_queue_info_a = shard_state
        .read_out_msg_queue_info()
        .map_err(|e| anyhow::format_err!("Failed to read shard state messages queue: {e}"))?;
    let mut out_messages_queue_info_b = out_messages_queue_info_a.clone();

    // Prepare buffer for message keys that will be removed from thread A shard state
    let mut message_keys_to_remove_from_state_a = vec![];

    out_messages_queue_info_a
        .out_queue()
        .iterate_slices(|key, message_slice| {
            let (enqueued_message, _create_lt) =
                OutMsgQueue::value_aug(&mut message_slice.clone())?;
            let message = enqueued_message.read_out_msg()?.read_message()?;

            // To define where to put out message, we should find a reference account. It can be (it descending order of priority):
            // 1) Destination account from the initial shard state
            // 2) Source account from the initial shard state
            let mut reference_account = None;

            // At first check message destination
            if let Some(dest_account_id) = message.int_dst_account_id() {
                // Check if destination account belongs to the initial thread A
                if let Some(shard_account) = shard_accounts_a.account(&dest_account_id)? {
                    reference_account = Some(shard_account.read_account()?);
                }
            }
            if reference_account.is_none() {
                // Check message source
                if let Some(src_account_id) = message.get_int_src_account_id() {
                    // Check if source account belongs to the initial thread A
                    if let Some(shard_account) = shard_accounts_a.account(&src_account_id)? {
                        reference_account = Some(shard_account.read_account()?);
                    }
                }
            }

            if reference_account.is_none() {
                tvm_types::fail!("Failed to define thread for out message: {:?}", message);
            }
            let account = reference_account.unwrap();
            let selected_thread_id =
                define_thread_id_for_account(&account, threads_table, thread_id_a, thread_id_b)?;
            match selected_thread_id {
                thread if thread == thread_id_a => {}
                thread if thread == thread_id_b => {
                    out_messages_queue_info_b.out_queue_mut().set_builder_serialized(
                        key.clone(),
                        &message_slice.as_builder(),
                        &enqueued_message.aug()?,
                    )?;
                    message_keys_to_remove_from_state_a.push(key);
                }
                _ => {
                    unreachable!(
                        "We are choosing between two threads, there can't be any other variant"
                    );
                }
            }
            Ok(true)
        })
        .map_err(|e| anyhow::format_err!("Failed to iterate and split accounts: {e}"))?;

    // Save messages for state B
    shard_state_b
        .write_out_msg_queue_info(&out_messages_queue_info_b)
        .map_err(|e| anyhow::format_err!("Failed to write messages data to shard state: {e}"))?;

    // Clear removed messages from state A
    for key in message_keys_to_remove_from_state_a {
        out_messages_queue_info_a
            .out_queue_mut()
            .remove(key)
            .map_err(|e| anyhow::format_err!("Failed to remove messages from out queue: {e}"))?;
    }
    shard_state_a
        .write_out_msg_queue_info(&out_messages_queue_info_a)
        .map_err(|e| anyhow::format_err!("Failed to write messages data to shard state: {e}"))?;

    // Prepare buffer for account keys that will be removed from thread A shard state
    let mut keys_to_remove_from_state_a = vec![];

    shard_accounts_a
        .iterate_slices(|key, shard_account_slice| {
            let shard_account = <ShardAccount>::construct_from(&mut shard_account_slice.clone())?;
            let account = shard_account.read_account()?;

            let selected_thread_id =
                define_thread_id_for_account(&account, threads_table, thread_id_a, thread_id_b)?;
            match selected_thread_id {
                thread if thread == thread_id_a => {}
                thread if thread == thread_id_b => {
                    // let account_id =
                    //     account.get_addr().map(|addr| addr.address()).expect("Account should not be None");
                    // let data = shard_account.write_to_new_cell()?;
                    // shard_accounts_b.set_builder_serialized(account_id, &data, &account.aug()?)?;
                    shard_accounts_b.set_builder_serialized(
                        key.clone(),
                        &shard_account_slice.as_builder(),
                        &account.aug()?,
                    )?;
                    keys_to_remove_from_state_a.push(key);
                }
                _ => {
                    unreachable!(
                        "We are choosing between two threads, there can't be any other variant"
                    );
                }
            }
            Ok(true)
        })
        .map_err(|e| anyhow::format_err!("Failed to iterate and split accounts: {e}"))?;

    // Save account for state B
    shard_state_b.write_accounts(&shard_accounts_b).map_err(|e| {
        anyhow::format_err!("Failed to write accounts to filtered shard state: {e}")
    })?;

    // Clear removed accounts from state A
    for key in keys_to_remove_from_state_a {
        shard_accounts_a
            .remove(key)
            .map_err(|e| anyhow::format_err!("Failed to remove account from shard state: {e}"))?;
    }
    shard_state_a.write_accounts(&shard_accounts_a).map_err(|e| {
        anyhow::format_err!("Failed to write accounts to filtered shard state: {e}")
    })?;

    Ok((Arc::new(shard_state_a), Arc::new(shard_state_b)))
}

fn define_thread_id_for_account(
    account: &Account,
    threads_table: &ThreadsTable,
    thread_id_a: ThreadIdentifier,
    thread_id_b: ThreadIdentifier,
) -> tvm_types::Result<ThreadIdentifier> {
    // Calculate current account routing
    let account_routing = get_account_routing_for_account(account);

    // If account routing matches thread B pass it to the split state
    if threads_table.is_match(account_routing.clone(), thread_id_b) {
        Ok(thread_id_b)

    // Else if account routing matches threa A leave it in state
    } else if threads_table.is_match(account_routing, thread_id_a) {
        Ok(thread_id_a)
    } else {
        // Else if account routing matches neither thread A nor thread B check it routing before DAPP id init
        let account_routing_without_dapp_id =
            get_account_routing_for_account_before_dapp_id_init(account);

        if threads_table.is_match(account_routing_without_dapp_id.clone(), thread_id_b) {
            // If account routing matches thread B pass it to the split state
            Ok(thread_id_b)
        } else if threads_table.is_match(account_routing_without_dapp_id, thread_id_a) {
            // Else if account routing matches threa A leave it in state
            Ok(thread_id_b)
        } else {
            // TODO: Check this statement, mb panic here because of unexpected shard state
            // If account doesn't match any of variants above it can mean that shard state can't be split to the given threads
            tvm_types::fail!("Can't split shard state for given threads");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tvm_block::Account;
    use tvm_block::Augmentation;
    use tvm_block::HashmapAugType;
    use tvm_block::MsgAddrStd;
    use tvm_block::MsgAddressInt;
    use tvm_block::Serializable;
    use tvm_block::ShardAccount;
    use tvm_block::ShardAccounts;
    use tvm_block::ShardStateUnsplit;
    use tvm_types::AccountId;
    use tvm_types::UInt256;

    use super::*;
    use crate::bitmask::mask::Bitmask;
    use crate::multithreading::account::get_account_routing_for_account;
    use crate::types::AccountAddress;
    use crate::types::AccountRouting;
    use crate::types::BlockIdentifier;
    use crate::types::DAppIdentifier;
    use crate::types::ThreadIdentifier;
    use crate::types::ThreadsTable;

    #[test]
    fn test_state_split() -> anyhow::Result<()> {
        let mut shard_state = ShardStateUnsplit::default();
        let mut shard_accounts = ShardAccounts::new();
        let mut address = [0_u8; 32];
        for i in 0_u8..10 {
            address[31] = i;
            let account_id = AccountId::from(address);
            let account = Account::with_address(
                MsgAddressInt::AddrStd(MsgAddrStd::with_address(None, 0, account_id.clone())),
                None,
            );
            let shard_account = ShardAccount::with_params(&account, UInt256::default(), 0)
                .map_err(|e| anyhow::format_err!("{e}"))?;
            let data = shard_account.write_to_new_cell().map_err(|e| anyhow::format_err!("{e}"))?;
            shard_accounts
                .set_builder_serialized(
                    account_id,
                    &data,
                    &account.aug().map_err(|e| anyhow::format_err!("{e}"))?,
                )
                .map_err(|e| anyhow::format_err!("{e}"))?;
        }
        shard_state.write_accounts(&shard_accounts).map_err(|e| anyhow::format_err!("{e}"))?;

        let mut threads_table = ThreadsTable::default();
        let thread_id = ThreadIdentifier::new(&BlockIdentifier::default(), 1);
        address[31] = 1;
        let account_address = AccountAddress(AccountId::from(address));
        let account_routing =
            AccountRouting(DAppIdentifier(account_address.clone()), account_address);
        let bitmask = Bitmask::builder()
            .meaningful_mask_bits(account_routing.clone())
            .mask_bits(account_routing)
            .build();

        threads_table.insert_above(0, bitmask, thread_id)?;
        let (filtered_state_a, filtered_state_b) = split_shard_state_based_on_threads_table(
            Arc::new(shard_state.clone()),
            &threads_table,
            ThreadIdentifier::default(),
            thread_id,
        )?;

        let unfiltered_accounts =
            shard_state.read_accounts().map_err(|e| anyhow::format_err!("{e}"))?;
        let mut cnt = 0;
        unfiltered_accounts
            .iterate_objects(|_| {
                cnt += 1;
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("{e}"))?;
        assert_eq!(cnt, 10);

        let filtered_accounts_b =
            filtered_state_b.read_accounts().map_err(|e| anyhow::format_err!("{e}"))?;
        let mut cnt = 0;
        filtered_accounts_b
            .iterate_objects(|shard_account| {
                let account_routing =
                    get_account_routing_for_account(&shard_account.read_account().unwrap());
                assert!(threads_table.is_match(account_routing.clone(), thread_id));
                cnt += 1;
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("{e}"))?;
        assert_eq!(cnt, 5);

        let filtered_accounts_a =
            filtered_state_a.read_accounts().map_err(|e| anyhow::format_err!("{e}"))?;
        let mut cnt = 0;
        filtered_accounts_a
            .iterate_objects(|shard_account| {
                let account_routing =
                    get_account_routing_for_account(&shard_account.read_account().unwrap());
                assert!(
                    threads_table.is_match(account_routing.clone(), ThreadIdentifier::default())
                );
                cnt += 1;
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("{e}"))?;
        assert_eq!(cnt, 5);

        Ok(())
    }
}
