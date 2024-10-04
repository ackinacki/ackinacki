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
    pub available_credit: i128,
    pub credit_per_block: u128,
    pub available_credit_max_value: u128,
    pub start_block_seqno: u128,
    pub end_block_seqno: u128,
    pub last_updated_seqno: u128,
    pub available_personal_limit: u128,
}

impl DappConfig {
    pub fn set_is_unlimit(&mut self, value: bool) {
        self.is_unlimit = value;
    }

    pub fn set_available_credit(&mut self, value: i128) {
        self.available_credit = value;
    }

    pub fn set_credit_per_block(&mut self, value: u128) {
        self.credit_per_block = value;
    }

    pub fn set_available_credit_max_value(&mut self, value: u128) {
        self.available_credit_max_value = value;
    }

    pub fn set_start_block_seqno(&mut self, value: u128) {
        self.start_block_seqno = value;
    }

    pub fn set_end_block_seqno(&mut self, value: u128) {
        self.end_block_seqno = value;
    }

    pub fn set_last_updated_seqno(&mut self, value: u128) {
        self.last_updated_seqno = value;
    }

    pub fn set_available_personal_limit(&mut self, value: u128) {
        self.available_personal_limit = value;
    }
}
