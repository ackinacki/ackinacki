// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use serde::Deserialize;
use serde::Serialize;
use tvm_block::AccountStatus;
use tvm_block::MsgAddressInt;
use tvm_block::Serializable;
use tvm_types::write_boc;
use tvm_types::AccountId;

use crate::helpers::u64_to_string;
use crate::serialization::AccountSerializationSet;
use crate::serialization::DeletedAccountSerializationSet;
// use tvm_block::Deserializable;

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct ArchAccount {
    pub id: String,
    pub workchain_id: i32,
    pub boc: Option<Vec<u8>>,
    pub init_code_hash: Option<String>,
    pub last_paid: Option<u32>,
    pub bits: Option<String>,
    pub cells: Option<String>,
    pub public_cells: Option<String>,
    pub last_trans_lt: Option<String>,
    pub last_trans_hash: Option<String>,
    pub balance: Option<String>,
    pub balance_other: Option<Vec<u8>>,
    pub code: Option<Vec<u8>>,
    pub code_hash: Option<String>,
    pub data: Option<Vec<u8>>,
    pub data_hash: Option<String>,
    pub acc_type: u8,
    pub last_trans_chain_order: Option<String>,
    pub dapp_id: Option<String>,
    pub due_payment: Option<String>,
    pub proof: Option<Vec<u8>>,
    pub prev_code_hash: Option<String>,
    pub state_hash: Option<String>,
    pub split_depth: Option<u32>,
}

impl From<AccountSerializationSet> for ArchAccount {
    fn from(acc: AccountSerializationSet) -> Self {
        let mut arch_account = Self {
            id: acc.account.get_addr().unwrap().to_string(),
            acc_type: serialize_account_status(&acc.account.status()),
            boc: Some(acc.boc),
            dapp_id: acc.account.get_dapp_id().map(|dapp_id| match dapp_id {
                Some(dapp_id_in) => dapp_id_in.to_hex_string(),
                None => "None".to_string(),
            }),
            init_code_hash: acc.account.init_code_hash().map(|h| h.to_hex_string()),
            last_trans_lt: Some(u64_to_string(acc.account.last_tr_time().unwrap_or_default())),
            prev_code_hash: acc.prev_code_hash.map(|h| h.as_hex_string()),
            ..Default::default()
        };

        if let Some(storage_stat) = acc.account.storage_info() {
            arch_account.last_paid = Some(storage_stat.last_paid());
            arch_account.bits = Some(u64_to_string(storage_stat.used().bits()));
            arch_account.cells = Some(u64_to_string(storage_stat.used().cells()));
            arch_account.public_cells = Some(u64_to_string(storage_stat.used().public_cells()));
            if let Some(grams) = storage_stat.due_payment() {
                arch_account.due_payment = Some(format!("{:x}", grams.as_u128()));
            }
        }

        if let Some(cc) = acc.account.balance() {
            arch_account.balance = Some(format!("{:x}", cc.grams.as_u128()));
            arch_account.balance_other =
                write_boc(&cc.other.serialize().expect("Failed to serialize ECC")).ok();
        }

        match acc.account.status() {
            AccountStatus::AccStateActive => {
                if let Some(state) = acc.account.state_init() {
                    if let Some(split_depth) = state.split_depth() {
                        arch_account.split_depth = Some(split_depth.as_u32());
                    }
                    if let Some(cell) = state.code() {
                        if !cell.is_pruned() {
                            arch_account.code = write_boc(cell).ok();
                            arch_account.code_hash = Some(cell.repr_hash().to_hex_string());
                        }
                    }
                    if let Some(cell) = state.data() {
                        if !cell.is_pruned() {
                            arch_account.data = write_boc(cell).ok();
                            arch_account.data_hash = Some(cell.repr_hash().to_hex_string());
                        }
                    }
                }
            }
            AccountStatus::AccStateFrozen => {
                arch_account.state_hash = acc.account.frozen_hash().map(|h| h.to_hex_string());
            }
            AccountStatus::AccStateUninit => {}
            AccountStatus::AccStateNonexist => {
                tracing::error!("Attempt to call serde::Serialize::serialize for AccountNone");
            }
        };

        arch_account
    }
}

impl From<DeletedAccountSerializationSet> for ArchAccount {
    fn from(acc: DeletedAccountSerializationSet) -> Self {
        let addr_int = construct_address(acc.workchain_id, acc.account_id)
            .expect("Failed to construct id of the deleted account");
        Self {
            id: addr_int.to_string(),
            acc_type: serialize_account_status(&AccountStatus::AccStateNonexist),
            prev_code_hash: acc.prev_code_hash.map(|h| h.as_hex_string()),
            ..Default::default()
        }
    }
}

pub(crate) fn serialize_account_status(status: &AccountStatus) -> u8 {
    match status {
        AccountStatus::AccStateUninit => 0b00,
        AccountStatus::AccStateFrozen => 0b10,
        AccountStatus::AccStateActive => 0b01,
        AccountStatus::AccStateNonexist => 0b11,
    }
}

pub(crate) fn construct_address(
    workchain_id: i32,
    account_id: AccountId,
) -> anyhow::Result<MsgAddressInt> {
    let res = if (-128..=127).contains(&workchain_id) && account_id.remaining_bits() == 256 {
        MsgAddressInt::with_standart(None, workchain_id as i8, account_id)
    } else {
        MsgAddressInt::with_variant(None, workchain_id, account_id)
    };

    res.map_err(|e| anyhow::format_err!("{e}"))
}
