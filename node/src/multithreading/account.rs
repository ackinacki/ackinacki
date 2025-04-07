// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use tvm_types::AccountId;
use tvm_types::SliceData;
use tvm_types::UInt256;

use crate::types::AccountAddress;
use crate::types::AccountRouting;
use crate::types::DAppIdentifier;

// Get account routing as a tuple of account DAPP id and account address.
// Function expects that passed account is not None and panics otherwise.
// If accounts DAPP id is None, its address is used as a DAPP id.
pub(crate) fn get_account_routing_for_account(
    account_address: SliceData,
    dapp_id: Option<UInt256>,
) -> AccountRouting {
    // For None dapp id we consider its address as a dapp id mask
    let account_dapp_id =
        dapp_id.map(|dapp_id| AccountAddress(AccountId::from(dapp_id))).map(DAppIdentifier);

    AccountRouting::from((account_dapp_id, AccountAddress(account_address)))
}
