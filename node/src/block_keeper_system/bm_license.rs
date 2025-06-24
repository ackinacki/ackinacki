// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::LazyLock;

use tvm_abi::Contract;
use tvm_abi::TokenValue;
use tvm_block::Account;
use tvm_client::abi::Abi;
use tvm_client::encoding::slice_from_cell;
use tvm_types::UInt256;

use super::abi::BLOCK_MANAGER_LICENSE_ABI;

const BM_LICENSE_OWNER_PUBKEY: &str = "_owner_pubkey";

static BM_LICENSE_ABI: LazyLock<Contract> = LazyLock::new(|| {
    Abi::Json(BLOCK_MANAGER_LICENSE_ABI.to_string()).abi().expect("Invalid BM license ABI")
});

pub fn decode_bm_license_pubkey(account: &Account) -> anyhow::Result<Option<String>> {
    let data = match account.get_data() {
        Some(acc_data) => acc_data,
        None => return Ok(None),
    };

    let slice = slice_from_cell(data)
        .map_err(|e| anyhow::format_err!("Failed to decode LicenseBM data cell: {e}"))?;

    let abi = &BM_LICENSE_ABI;

    let decoded = abi
        .decode_storage_fields(slice, true)
        .map_err(|e| anyhow::format_err!("Failed to decode LicenseBM storage: {e}"))?;

    let pubkey = decoded.into_iter().find_map(|token| {
        if token.name == BM_LICENSE_OWNER_PUBKEY {
            if let TokenValue::Optional(_, Some(value)) = token.value {
                if let TokenValue::Uint(value) = *value {
                    let pubkey = UInt256::from_be_bytes(&value.number.to_bytes_be());
                    return Some(pubkey.to_hex_string());
                }
            }
        }
        None
    });

    Ok(pubkey)
}
