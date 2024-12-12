// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;

use serde::Deserialize;
use serde::Serialize;

pub mod abi;
pub mod dappconfig;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct DappConfig {
    pub is_unlimit: bool,
    pub available_balance: i128,
}

impl DappConfig {
    pub fn set_is_unlimit(&mut self, value: bool) {
        self.is_unlimit = value;
    }

    pub fn set_available_balance(&mut self, value: i128) {
        self.available_balance = value;
    }
}
