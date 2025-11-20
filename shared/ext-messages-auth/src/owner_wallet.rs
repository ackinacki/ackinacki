// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use tvm_abi::TokenValue;
use tvm_block::Account;
use tvm_client::encoding::slice_from_cell;
use tvm_types::UInt256;

use crate::auth::TokenIssuer;

const OWNER_WALLET_SIGN_KEY: &str = "_signing_pubkey";

pub static BK_OWNER_WALLET_ABI: &str = include_str!(
    "../../../contracts/0.79.3_compiled/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json"
);
pub static BM_OWNER_WALLET_ABI: &str = include_str!(
    "../../../contracts/0.79.3_compiled/bksystem/AckiNackiBlockManagerNodeWallet.abi.json"
);

pub fn decode_signing_pubkey(
    account: &Account,
    issuer: &TokenIssuer,
) -> anyhow::Result<Option<String>> {
    let Some(data) = account.get_data() else {
        return Ok(None);
    };

    let slice = slice_from_cell(data)
        .map_err(|e| anyhow::format_err!("Failed to decode Node Wallet data cell: {e}"))?;

    let abi_str = match issuer {
        TokenIssuer::Bk(_) => BK_OWNER_WALLET_ABI.to_string(),
        TokenIssuer::Bm(_) => BM_OWNER_WALLET_ABI.to_string(),
    };
    let abi = tvm_client::abi::Abi::Json(abi_str).abi()?;

    let decoded = abi
        .decode_storage_fields(slice, true)
        .map_err(|e| anyhow::format_err!("Failed to decode Node Wallet storage: {e}"))?;

    let pubkey = decoded.into_iter().find_map(|token| {
        if token.name == OWNER_WALLET_SIGN_KEY {
            if let TokenValue::Uint(value) = token.value {
                let pubkey = UInt256::from_be_bytes(&value.number.to_bytes_be());
                return Some(pubkey.to_hex_string());
            }
        }
        None
    });

    Ok(pubkey)
}
