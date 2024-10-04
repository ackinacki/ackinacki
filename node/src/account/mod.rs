// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

mod account_stub;

use std::fmt::Debug;

#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait AccountId: Debug {}

impl AccountId for u64 {}

#[cfg_attr(test, automock(type AccountId=MockAccountId;))]
pub trait Account {
    type AccountId: AccountId;

    fn id(&self) -> Self::AccountId;
}

impl AccountId for tvm_types::AccountId {}

impl Account for tvm_block::Account {
    type AccountId = tvm_types::AccountId;

    fn id(&self) -> Self::AccountId {
        self.get_id().unwrap()
    }
}
