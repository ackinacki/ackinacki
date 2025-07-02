// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use num_bigint::Sign;
use tvm_abi::Int;
use tvm_abi::TokenValue;
use tvm_block::messages::InternalMessageHeader;
use tvm_block::Account;
use tvm_block::CurrencyCollection;
use tvm_block::GetRepresentationHash;
use tvm_block::Grams;
use tvm_block::Message;
use tvm_block::MsgAddressInt;
use tvm_block::Serializable;
use tvm_block::StateInit;
use tvm_client::boc::set_code_salt_cell;
use tvm_client::encoding::slice_from_cell;
use tvm_types::AccountId;
use tvm_types::BuilderData;
use tvm_types::SliceData;
use tvm_types::UInt256;

use crate::creditconfig::abi::DAPP_CONFIG_ABI;
use crate::creditconfig::DappConfig;
use crate::types::DAppIdentifier;

const DAPP_DATA: &str = "dapp_id";
const CONFIG_DATA: &str = "_data";
const IS_UNLIMIT_DATA: &str = "is_unlimit";
const AVAILABLE_BALANCE_DATA: &str = "available_balance";

fn get_dapp_config_abi() -> tvm_client::abi::Abi {
    tvm_client::abi::Abi::Json(DAPP_CONFIG_ABI.to_string())
}

pub fn calculate_dapp_config_address(
    dapp_id: DAppIdentifier,
    mut data: StateInit,
) -> anyhow::Result<UInt256> {
    let code = data.code().unwrap();
    let mut b = BuilderData::new();
    dapp_id.write_to(&mut b).map_err(|e| anyhow::format_err!("Failed to write dapp id: {e}"))?;
    let stateinitcode = set_code_salt_cell(code.clone(), b.into_cell().unwrap())
        .map_err(|e| anyhow::format_err!("Failed to set code salt: {e}"))?;
    data.set_code(stateinitcode);
    data.hash().map_err(|e| anyhow::format_err!("Failed to calculate hash: {e}"))
}

fn get_i128_value(value: Int) -> i128 {
    let (sign, data) = value.number.to_u64_digits();
    let mut num: i128 = 0;
    if !data.is_empty() {
        num += data[0] as i128;
    }
    if data.len() >= 2 {
        num += (data[1] as i128) << 64;
    }
    if sign == Sign::Minus {
        num *= -1;
    }
    num
}

pub fn decode_message_config(body: SliceData) -> anyhow::Result<Option<UInt256>> {
    let abi = get_dapp_config_abi();
    let mut dapp = None;
    let decoded_body = abi
        .abi()
        .map_err(|e| anyhow::format_err!("Failed to get DAPP config abi: {e}"))?
        .decode_input(body, true, true)
        .map_err(|e| anyhow::format_err!("Failed to decode DAPP config body: {e}"))?;
    for token in decoded_body.tokens {
        if token.name == DAPP_DATA {
            if let TokenValue::Uint(value) = token.value {
                dapp = Some(UInt256::from_be_bytes(&value.number.to_bytes_be()));
            }
        }
    }
    Ok(dapp)
}

pub fn decode_dapp_config_data(account: &Account) -> anyhow::Result<Option<DappConfig>> {
    let abi = get_dapp_config_abi();
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
        let mut config_data = DappConfig::default();
        for token in decoded_data {
            if token.name == CONFIG_DATA {
                if let TokenValue::Tuple(data) = token.value {
                    for in_token in data {
                        if in_token.name == IS_UNLIMIT_DATA {
                            if let TokenValue::Bool(isunlimit) = in_token.value {
                                config_data.set_is_unlimit(isunlimit);
                            }
                        } else if in_token.name == AVAILABLE_BALANCE_DATA {
                            if let TokenValue::Int(value) = in_token.value {
                                let num = get_i128_value(value);
                                config_data.set_available_balance(num);
                            }
                        }
                    }
                }
            }
        }
        tracing::trace!(target: "builder", "DappConfig result {:?}", config_data);
        return Ok(Some(config_data));
    }
    Ok(None)
}

pub fn create_config_touch_message(
    minted: u128,
    addr: UInt256,
    block_time: u32,
) -> anyhow::Result<Message> {
    tracing::trace!("create_Dapp_Config message: {addr:?}");
    let expire = block_time + 5;
    let parameters = format!(r#"{{"minted": {minted}}}"#,);
    tracing::trace!(target: "builder", "parameters {:?}", parameters);
    let msg_body = tvm_abi::encode_function_call(
        DAPP_CONFIG_ABI,
        "setNewConfig",
        Some(&format!(r#"{{"expire":{expire}}}"#)),
        &parameters,
        true,
        None,
        None,
    )
    .map_err(|e| anyhow::format_err!("Failed to create message body: {e}"))?;
    let src_acc_id = AccountId::from(addr.clone());
    let dst_acc_id = AccountId::from(addr.clone());
    let header = InternalMessageHeader::with_addresses(
        MsgAddressInt::with_standart(None, 0, src_acc_id)
            .map_err(|e| anyhow::format_err!("Failed to get addr: {e}"))?,
        MsgAddressInt::with_standart(None, 0, dst_acc_id)
            .map_err(|e| anyhow::format_err!("Failed to get addr: {e}"))?,
        CurrencyCollection::from_grams(Grams::from(200000000)),
    );
    let body = SliceData::load_cell(
        msg_body
            .into_cell()
            .map_err(|e| anyhow::format_err!("Failed serialize message body: {e}"))?,
    )
    .map_err(|e| anyhow::format_err!("Failed to serialize message body: {e}"))?;
    Ok(Message::with_int_header_and_body(header, body))
}

pub fn get_available_balance_from_config(config: DappConfig) -> i128 {
    if config.is_unlimit {
        return -1;
    }
    if config.available_balance < 0 {
        return 0;
    }
    config.available_balance
}
