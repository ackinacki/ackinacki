// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use num_bigint::Sign;
use tvm_abi::Int;
use tvm_abi::TokenValue;
use tvm_abi::Uint;
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

const CONFIG_DATA: &str = "_data";
const IS_UNLIMIT_DATA: &str = "is_unlimit";
const AVAILABLE_CREDIT_DATA: &str = "available_credit";
const CREDIT_PER_BLOCK_DATA: &str = "credit_per_block";
const AVAILABLE_CREDIT_MAX_VALUE_DATA: &str = "available_credit_max_value";
const START_BLOCK_SEQNO_DATA: &str = "start_block_seqno";
const END_BLOCK_SEQNO_DATA: &str = "end_block_seqno";
const LAST_UPDATED_SEQNO_DATA: &str = "last_updated_seqno";
const AVAILABLE_PERSONAL_LIMIT_DATA: &str = "available_personal_limit";

fn get_dapp_config_abi() -> tvm_client::abi::Abi {
    tvm_client::abi::Abi::Json(DAPP_CONFIG_ABI.to_string())
}
// fn get_dapp_root_abi() -> tvm_client::abi::Abi {
// tvm_client::abi::Abi::Json(DAPP_CONFIG_ABI.to_string())
// }
pub fn calculate_dapp_config_address(
    dapp_id: UInt256,
    mut data: StateInit,
) -> anyhow::Result<UInt256> {
    let code = data.code().unwrap();
    let mut b = BuilderData::new();
    dapp_id.write_to(&mut b).map_err(|e| anyhow::format_err!("{e}"))?;
    let stateinitcode = set_code_salt_cell(code.clone(), b.into_cell().unwrap())
        .map_err(|e| anyhow::format_err!("{e}"))?;
    data.set_code(stateinitcode);
    data.hash().map_err(|e| anyhow::format_err!("{e}"))
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

fn get_u128_value(value: Uint) -> u128 {
    let data = value.number.to_u64_digits();
    let mut num: u128 = 0;
    if !data.is_empty() {
        num += data[0] as u128;
    }
    if data.len() >= 2 {
        num += (data[1] as u128) << 64;
    }
    num
}

pub fn decode_dapp_config_data(account: &Account) -> anyhow::Result<Option<DappConfig>> {
    let abi = get_dapp_config_abi();
    if let Some(data) = account.get_data() {
        let decoded_data = abi
            .abi()
            .map_err(|e| anyhow::format_err!("{e}"))?
            .decode_storage_fields(
                slice_from_cell(data).map_err(|e| anyhow::format_err!("{e}"))?,
                true,
            )
            .map_err(|e| anyhow::format_err!("{e}"))?;
        let mut config_data = DappConfig::default();
        for token in decoded_data {
            if token.name == CONFIG_DATA {
                if let TokenValue::Tuple(data) = token.value {
                    for in_token in data {
                        if in_token.name == IS_UNLIMIT_DATA {
                            if let TokenValue::Bool(isunlimit) = in_token.value {
                                config_data.set_is_unlimit(isunlimit);
                            }
                        } else if in_token.name == AVAILABLE_CREDIT_DATA {
                            if let TokenValue::Int(value) = in_token.value {
                                let num = get_i128_value(value);
                                config_data.set_available_credit(num);
                            }
                        } else if in_token.name == CREDIT_PER_BLOCK_DATA {
                            if let TokenValue::Uint(value) = in_token.value {
                                let num = get_u128_value(value);
                                config_data.set_credit_per_block(num);
                            }
                        } else if in_token.name == AVAILABLE_CREDIT_MAX_VALUE_DATA {
                            if let TokenValue::Uint(value) = in_token.value {
                                let num = get_u128_value(value);
                                config_data.set_available_credit_max_value(num);
                            }
                        } else if in_token.name == START_BLOCK_SEQNO_DATA {
                            if let TokenValue::Uint(value) = in_token.value {
                                let num = get_u128_value(value);
                                config_data.set_start_block_seqno(num);
                            }
                        } else if in_token.name == END_BLOCK_SEQNO_DATA {
                            if let TokenValue::Uint(value) = in_token.value {
                                let num = get_u128_value(value);
                                config_data.set_end_block_seqno(num);
                            }
                        } else if in_token.name == LAST_UPDATED_SEQNO_DATA {
                            if let TokenValue::Uint(value) = in_token.value {
                                let num = get_u128_value(value);
                                config_data.set_last_updated_seqno(num);
                            }
                        } else if in_token.name == AVAILABLE_PERSONAL_LIMIT_DATA {
                            if let TokenValue::Uint(value) = in_token.value {
                                let num = get_u128_value(value);
                                config_data.set_available_personal_limit(num);
                            }
                        }
                    }
                }
            }
        }
        log::trace!(target: "builder", "DappConfig result {:?}", config_data);
        return Ok(Some(config_data));
    }
    Ok(None)
}

pub fn create_config_touch_message(
    config: DappConfig,
    minted: u128,
    addr: UInt256,
    block_time: u32,
) -> anyhow::Result<Message> {
    tracing::trace!("create_Dapp_Config message: {addr:?}");
    let expire = block_time + 5;
    let mut available_credit: i128 = config.available_credit;
    available_credit = available_credit.saturating_sub(minted as i128);
    let parameters = format!(
        r#"{{"is_unlimit": {}, "available_credit": {}, "credit_per_block": {}, "available_credit_max_value": {}, "start_block_seqno": {}, "end_block_seqno": {}}}"#,
        config.is_unlimit,
        available_credit,
        config.credit_per_block,
        config.available_credit_max_value,
        config.start_block_seqno,
        config.end_block_seqno
    );
    log::trace!(target: "builder", "parameters {:?}", parameters);
    let msg_body = tvm_abi::encode_function_call(
        DAPP_CONFIG_ABI,
        "setNewConfig",
        Some(&format!(r#"{{"expire":{}}}"#, expire)),
        &parameters,
        true,
        None,
        None,
    )
    .map_err(|e| anyhow::format_err!("Failed to create message body: {e}"))?;
    let src_acc_id = AccountId::from(UInt256::ZERO);
    let dst_acc_id = AccountId::from(addr);
    let header = InternalMessageHeader::with_addresses(
        MsgAddressInt::with_standart(None, 0, src_acc_id)
            .map_err(|e| anyhow::format_err!("Failed to get addr: {e}"))?,
        MsgAddressInt::with_standart(None, 0, dst_acc_id)
            .map_err(|e| anyhow::format_err!("Failed to get addr: {e}"))?,
        CurrencyCollection::from_grams(Grams::from(100000000)),
    );
    let body = SliceData::load_cell(
        msg_body
            .into_cell()
            .map_err(|e| anyhow::format_err!("Failed serialize message body: {e}"))?,
    )
    .map_err(|e| anyhow::format_err!("Failed to serialize message body: {e}"))?;
    Ok(Message::with_int_header_and_body(header, body))
}

pub fn recalculate_config(mut config: DappConfig, seqno: u128) -> DappConfig {
    let mut available_credit: i128 = config.available_credit;
    let seq_no = std::cmp::min(seqno, config.end_block_seqno);
    if seq_no >= config.last_updated_seqno {
        available_credit = available_credit.saturating_add(
            ((seq_no - config.last_updated_seqno).saturating_mul(config.credit_per_block)) as i128,
        );
        available_credit =
            std::cmp::min(available_credit, config.available_credit_max_value as i128);
        config.available_credit = available_credit;
        config.last_updated_seqno = seqno;
    }
    config
}

pub fn get_available_credit_from_config(config: DappConfig, seq_no: u128) -> i128 {
    if (seq_no < config.start_block_seqno) || (seq_no > config.end_block_seqno) {
        return 0;
    }
    if config.is_unlimit {
        return -1;
    }
    config.available_credit
}

#[test]
pub fn test_recalculate_config() -> anyhow::Result<()> {
    let mut config = DappConfig {
        is_unlimit: false,
        available_credit: 0,
        credit_per_block: 1,
        available_credit_max_value: 10,
        start_block_seqno: 0,
        end_block_seqno: 1000000,
        last_updated_seqno: 0,
        available_personal_limit: 0,
    };
    let mut updated_config = recalculate_config(config, 15);
    let mut updated_ethalon = DappConfig {
        is_unlimit: false,
        available_credit: 10,
        credit_per_block: 1,
        available_credit_max_value: 10,
        start_block_seqno: 0,
        end_block_seqno: 1000000,
        last_updated_seqno: 15,
        available_personal_limit: 0,
    };
    assert!(updated_config == updated_ethalon);
    let mut available_credit = get_available_credit_from_config(updated_ethalon, 15);
    assert!(available_credit == 10);
    config = DappConfig {
        is_unlimit: false,
        available_credit: 0,
        credit_per_block: 1,
        available_credit_max_value: 10,
        start_block_seqno: 0,
        end_block_seqno: 5,
        last_updated_seqno: 0,
        available_personal_limit: 0,
    };
    updated_config = recalculate_config(config, 15);
    updated_ethalon = DappConfig {
        is_unlimit: false,
        available_credit: 5,
        credit_per_block: 1,
        available_credit_max_value: 10,
        start_block_seqno: 0,
        end_block_seqno: 5,
        last_updated_seqno: 15,
        available_personal_limit: 0,
    };
    assert!(updated_config == updated_ethalon);
    available_credit = get_available_credit_from_config(updated_ethalon, 15);
    assert!(available_credit == 0);
    config = DappConfig {
        is_unlimit: true,
        available_credit: 0,
        credit_per_block: 1,
        available_credit_max_value: 10,
        start_block_seqno: 0,
        end_block_seqno: 5,
        last_updated_seqno: 0,
        available_personal_limit: 0,
    };
    updated_config = recalculate_config(config, 15);
    updated_ethalon = DappConfig {
        is_unlimit: true,
        available_credit: 5,
        credit_per_block: 1,
        available_credit_max_value: 10,
        start_block_seqno: 0,
        end_block_seqno: 5,
        last_updated_seqno: 15,
        available_personal_limit: 0,
    };
    assert!(updated_config == updated_ethalon);
    available_credit = get_available_credit_from_config(updated_ethalon.clone(), 15);
    assert!(available_credit == 0);
    available_credit = get_available_credit_from_config(updated_ethalon.clone(), 3);
    assert!(available_credit == -1);
    Ok(())
}
