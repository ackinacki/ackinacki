// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use tvm_block::Account;
use tvm_types::AccountId;

use crate::types::AccountAddress;
use crate::types::AccountRouting;
use crate::types::DAppIdentifier;

// Get account routing as a tuple of account DAPP id and account address.
// Function expects that passed account is not None and panics otherwise.
// If accounts DAPP id is None, its address is used as a DAPP id.
pub(crate) fn get_account_routing_for_account(account: &Account) -> AccountRouting {
    // Function expects that passed account is not None and panics otherwise.
    assert!(!account.is_none(), "None account should not be processed in multithreaded routing");
    let account_address = AccountAddress(
        account.get_addr().map(|addr| addr.address()).expect("Account should not be None"),
    );

    // For None dapp id we consider its address as a dapp id mask
    let account_dapp_id = account
        .get_dapp_id()
        .expect("Account should not be None")
        .clone()
        .map(|dapp_id| AccountAddress(AccountId::from(dapp_id)))
        .unwrap_or(account_address.clone());

    AccountRouting(DAppIdentifier(account_dapp_id), account_address)
}

// Get account routing for account ignoring its DAPP id to get account routing before DAPP id was initialized
pub(crate) fn get_account_routing_for_account_before_dapp_id_init(
    account: &Account,
) -> AccountRouting {
    // Function expects that passed account is not None and panics otherwise.
    assert!(!account.is_none(), "None account should not be processed in multithreaded routing");
    let account_address = AccountAddress(
        account.get_addr().map(|addr| addr.address()).expect("Account should not be None"),
    );

    AccountRouting(DAppIdentifier(account_address.clone()), account_address)
}
