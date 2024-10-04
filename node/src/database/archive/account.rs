// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

// use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;
// use tvm_block::Deserializable;
// use tvm_block::ShardAccount;

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct ArchAccount {
    pub id: String,
    pub workchain_id: i32,
    pub boc: Option<String>,
    pub init_code_hash: Option<String>,
    pub last_paid: Option<u32>,
    pub bits: Option<String>,
    pub cells: Option<String>,
    pub public_cells: Option<String>,
    pub last_trans_lt: Option<String>,
    pub last_trans_hash: Option<String>,
    pub balance: Option<String>,
    pub code: Option<String>,
    pub code_hash: Option<String>,
    pub data: Option<String>,
    pub data_hash: Option<String>,
    pub acc_type: u8,
    pub last_trans_chain_order: Option<String>,
    pub dapp_id: Option<String>,
}

// impl From<ArchAccount> for ShardAccount {
//     fn from(val: ArchAccount) -> Self {
//         let boc = tvm_types::base64_decode(val.boc).ok();
//         let account = tvm_block::Account::construct_from_bytes(&boc)
//             .expect("Failed to construct account from BOC");
//         let last_trans_lt: u64 = val.last_trans_lt.parse().expect("Failed to
// parse last_trans_lt");         let last_trans_hash =
// tvm_types::UInt256::from_str(&val.last_trans_hash)
// .expect("Failed to convert last_trans_hash");

//         ShardAccount::with_params(&account, last_trans_hash, last_trans_lt)
//             .expect("Failed to construct ShardAccount")
//     }
// }
