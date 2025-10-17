use num_traits::Zero;
use tvm_abi::TokenValue;
// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
use tvm_block::Account;
use tvm_client::encoding::slice_from_cell;
use tvm_vm::executor::MVConfig;

use crate::mvconfig::abi::MV_CONFIG_ABI;

const MBN_GLOBAL_TOKEN: &str = "_MBNLst";

fn get_mv_config_abi() -> tvm_client::abi::Abi {
    tvm_client::abi::Abi::Json(MV_CONFIG_ABI.to_string())
}

pub fn decode_mv_config_data(account: &Account) -> anyhow::Result<MVConfig> {
    let abi = get_mv_config_abi();
    let mut config_data = MVConfig::default();
    if let Some(data) = account.get_data() {
        let decoded_data = abi
            .abi()
            .map_err(|e| anyhow::format_err!("Failed to get DAPP config abi: {e}"))?
            .decode_storage_fields(
                slice_from_cell(data)
                    .map_err(|e| anyhow::format_err!("Failed to convert cell to slice: {e}"))?,
                true,
            )
            .map_err(|e| anyhow::format_err!("Failed to decode DAPP config storage: {e}"))?;
        let mut mbn_global = Vec::new();
        for token in decoded_data {
            if token.name == MBN_GLOBAL_TOKEN {
                if let TokenValue::Array(_, data) = token.value {
                    for in_token in data {
                        if let TokenValue::Uint(num) = in_token {
                            let array_num = if num.number.is_zero() {
                                0
                            } else {
                                num.number.to_u64_digits()[0]
                            };
                            mbn_global.push(array_num);
                        }
                    }
                }
            }
        }
        config_data.set_config(mbn_global);
        tracing::trace!(target: "builder", "MVConfig result {:?}", config_data);
        return Ok(config_data);
    }
    Ok(config_data)
}
