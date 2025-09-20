// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::str::FromStr;

use num_bigint::BigUint;
use num_traits::Zero;
use tvm_abi::TokenValue;
use tvm_block::Account;
use tvm_block::ExternalInboundMessageHeader;
use tvm_block::Message;
use tvm_block::MsgAddress;
use tvm_block::MsgAddressInt;
use tvm_client::encoding::slice_from_cell;
use tvm_types::AccountId;
use tvm_types::SliceData;
use tvm_types::UInt256;

use crate::block_keeper_system::abi::EPOCH_ABI;
use crate::block_keeper_system::abi::PREEPOCH_ABI;
use crate::block_keeper_system::BlockKeeperData;
use crate::block_keeper_system::BlockKeeperStatus;
use crate::bls::gosh_bls::PubKey;
use crate::node::SignerIndex;

const BLS_PUBKEY_TOKEN_KEY: &str = "_bls_pubkey";
const EPOCH_FINISH_TOKEN_KEY: &str = "_seqNoFinish";
const WAIT_STEP_TOKEN_KEY: &str = "_waitStep";
const STAKE_TOKEN_KEY: &str = "_stake";
const OWNER_TOKEN_KEY: &str = "_owner_address";
const PREEPOCH_OWNER_TOKEN_KEY: &str = "_wallet";
const SIGNER_INDEX_KEY: &str = "_signerIndex";
const OWNER_PUBKEY_KEY: &str = "_owner_pubkey";

fn get_epoch_abi() -> tvm_client::abi::Abi {
    tvm_client::abi::Abi::Json(EPOCH_ABI.to_string())
}

fn get_preepoch_abi() -> tvm_client::abi::Abi {
    tvm_client::abi::Abi::Json(PREEPOCH_ABI.to_string())
}

pub fn decode_epoch_data(
    account: &Account,
) -> anyhow::Result<Option<(SignerIndex, BlockKeeperData)>> {
    let abi = get_epoch_abi();
    if let Some(data) = account.get_data() {
        let decoded_data = abi
            .abi()
            .map_err(|e| anyhow::format_err!("Failed to load epoch ABI: {e}"))?
            .decode_storage_fields(
                slice_from_cell(data)
                    .map_err(|e| anyhow::format_err!("Failed to decode epoch data cell: {e}"))?,
                true,
            )
            .map_err(|e| anyhow::format_err!("Failed to decode epoch storage: {e}"))?;
        let mut block_keeper_bls_key = None;
        let mut block_keeper_epoch_finish = None;
        let mut block_keeper_wait_step = None;
        let mut block_keeper_stake = None;
        let mut wallet_address = None;
        let mut signer_index = None;
        let mut owner_pubkey = None;
        tracing::trace!("decoded epoch data: {decoded_data:?}");
        for token in decoded_data {
            if token.name == BLS_PUBKEY_TOKEN_KEY {
                if let TokenValue::Bytes(pubkey) = token.value {
                    block_keeper_bls_key = Some(PubKey::from(pubkey.as_slice()));
                }
            } else if token.name == EPOCH_FINISH_TOKEN_KEY {
                if let TokenValue::Uint(epoch_finish) = token.value {
                    tracing::trace!("decoded epoch finish: {epoch_finish:?}");
                    block_keeper_epoch_finish = Some(if epoch_finish.number.is_zero() {
                        0
                    } else {
                        epoch_finish.number.to_u64_digits()[0]
                    });
                }
            } else if token.name == WAIT_STEP_TOKEN_KEY {
                if let TokenValue::Uint(value) = token.value {
                    tracing::trace!("decoded wait step: {value:?}");
                    block_keeper_wait_step = Some(if value.number.is_zero() {
                        0
                    } else {
                        value.number.to_u64_digits()[0]
                    });
                }
            } else if token.name == STAKE_TOKEN_KEY {
                if let TokenValue::Uint(stake) = token.value {
                    tracing::trace!("decoded epoch stake: {stake:?}");
                    block_keeper_stake =
                        Some(if stake.number.is_zero() { BigUint::zero() } else { stake.number });
                }
            } else if token.name == OWNER_PUBKEY_KEY {
                if let TokenValue::Uint(pubkey) = token.value {
                    tracing::trace!("decoded owner pubkey: {pubkey:?}");
                    owner_pubkey =
                        Some(UInt256::from_be_bytes(&pubkey.number.to_bytes_be()).inner());
                }
            } else if token.name == OWNER_TOKEN_KEY {
                if let TokenValue::Address(addr) = token.value {
                    tracing::trace!("decoded wallet address: {addr:?}");
                    wallet_address = match addr {
                        MsgAddress::AddrNone => None,
                        MsgAddress::AddrExt(_) => None,
                        MsgAddress::AddrStd(data) => {
                            let addr = data.address;
                            Some(addr)
                        }
                        MsgAddress::AddrVar(_) => None,
                    };
                }
            } else if token.name == SIGNER_INDEX_KEY {
                if let TokenValue::Uint(index) = token.value {
                    tracing::trace!("decoded signer index: {index:?}");
                    signer_index = Some(if index.number.is_zero() {
                        0
                    } else {
                        index.number.to_u32_digits()[0] as SignerIndex
                    });
                }
            }
        }
        tracing::trace!("decoded epoch data: {block_keeper_bls_key:?} {block_keeper_epoch_finish:?} {block_keeper_stake:?} {wallet_address:?} {signer_index:?} {owner_pubkey:?}");
        if let (
            Some(block_keeper_bls_key),
            Some(block_keeper_epoch_finish),
            Some(block_keeper_wait_step),
            Some(block_keeper_stake),
            Some(wallet_address),
            Some(signer_index),
            Some(owner_pubkey),
        ) = (
            block_keeper_bls_key,
            block_keeper_epoch_finish,
            block_keeper_wait_step,
            block_keeper_stake,
            wallet_address,
            signer_index,
            owner_pubkey,
        ) {
            let wallet_address = AccountId::from(wallet_address);
            return Ok(Some((
                signer_index,
                BlockKeeperData {
                    pubkey: block_keeper_bls_key,
                    epoch_finish_seq_no: Some(block_keeper_epoch_finish),
                    wait_step: block_keeper_wait_step,
                    status: BlockKeeperStatus::Active,
                    // TODO: better fix pure unwrap for address
                    address: account.get_addr().unwrap().to_string(),
                    stake: block_keeper_stake,
                    owner_address: wallet_address.into(),
                    signer_index,
                    owner_pubkey,
                },
            )));
        }
    }
    Ok(None)
}

pub fn decode_preepoch_data(
    account: &Account,
) -> anyhow::Result<Option<(SignerIndex, BlockKeeperData)>> {
    let abi = get_preepoch_abi();
    if let Some(data) = account.get_data() {
        let decoded_data = abi
            .abi()
            .map_err(|e| anyhow::format_err!("Failed to load preepoch ABI: {e}"))?
            .decode_storage_fields(
                slice_from_cell(data)
                    .map_err(|e| anyhow::format_err!("Failed to decode preepoch data cell: {e}"))?,
                true,
            )
            .map_err(|e| anyhow::format_err!("Failed to decode preepoch storage: {e}"))?;
        let mut block_keeper_bls_key = None;
        let mut block_keeper_stake = None;
        let mut wallet_address = None;
        let mut signer_index = None;
        let mut owner_pubkey = None;
        let mut block_keeper_wait_step = None;
        tracing::trace!("decoded preepoch data: {decoded_data:?}");
        for token in decoded_data {
            if token.name == BLS_PUBKEY_TOKEN_KEY {
                if let TokenValue::Bytes(pubkey) = token.value {
                    block_keeper_bls_key = Some(PubKey::from(pubkey.as_slice()));
                }
            } else if token.name == STAKE_TOKEN_KEY {
                if let TokenValue::VarUint(_len, stake) = token.value {
                    tracing::trace!("decoded preepoch stake: {stake:?}");
                    block_keeper_stake =
                        Some(if stake.is_zero() { BigUint::zero() } else { stake });
                }
            } else if token.name == WAIT_STEP_TOKEN_KEY {
                if let TokenValue::Uint(value) = token.value {
                    tracing::trace!("decoded wait step: {value:?}");
                    block_keeper_wait_step = Some(if value.number.is_zero() {
                        0
                    } else {
                        value.number.to_u64_digits()[0]
                    });
                }
            } else if token.name == PREEPOCH_OWNER_TOKEN_KEY {
                if let TokenValue::Address(addr) = token.value {
                    tracing::trace!("decoded wallet address: {addr:?}");
                    wallet_address = match addr {
                        MsgAddress::AddrNone => None,
                        MsgAddress::AddrExt(_) => None,
                        MsgAddress::AddrStd(data) => {
                            let addr = data.address.into();
                            Some(addr)
                        }
                        MsgAddress::AddrVar(_) => None,
                    };
                }
            } else if token.name == OWNER_PUBKEY_KEY {
                if let TokenValue::Uint(pubkey) = token.value {
                    tracing::trace!("decoded owner pubkey: {pubkey:?}");
                    owner_pubkey =
                        Some(UInt256::from_be_bytes(&pubkey.number.to_bytes_be()).inner());
                }
            } else if token.name == SIGNER_INDEX_KEY {
                if let TokenValue::Uint(index) = token.value {
                    tracing::trace!("decoded signer index: {index:?}");
                    signer_index = Some(if index.number.is_zero() {
                        0
                    } else {
                        index.number.to_u32_digits()[0] as SignerIndex
                    });
                }
            }
        }
        tracing::trace!("decoded preepoch data: {block_keeper_bls_key:?} {block_keeper_stake:?} {wallet_address:?} {signer_index:?} {owner_pubkey:?}");
        if let (
            Some(block_keeper_bls_key),
            Some(block_keeper_stake),
            Some(block_keeper_wait_step),
            Some(wallet_address),
            Some(signer_index),
            Some(owner_pubkey),
        ) = (
            block_keeper_bls_key,
            block_keeper_stake,
            block_keeper_wait_step,
            wallet_address,
            signer_index,
            owner_pubkey,
        ) {
            return Ok(Some((
                signer_index,
                BlockKeeperData {
                    pubkey: block_keeper_bls_key,
                    epoch_finish_seq_no: None,
                    wait_step: block_keeper_wait_step,
                    status: BlockKeeperStatus::PreEpoch,
                    // TODO: better fix pure unwrap for address
                    address: account.get_addr().unwrap().to_string(),
                    stake: block_keeper_stake,
                    owner_address: wallet_address,
                    signer_index,
                    owner_pubkey,
                },
            )));
        }
    }
    Ok(None)
}

pub fn create_epoch_touch_message(data: &BlockKeeperData, time: u32) -> anyhow::Result<Message> {
    tracing::trace!("create_epoch_touch_message: {data:?}");
    let expire = time + 5;
    let msg_body = tvm_abi::encode_function_call(
        EPOCH_ABI,
        "touch",
        Some(&format!(r#"{{"expire":{expire}}}"#)),
        "{}",
        false,
        None,
        Some(&data.address),
    )
    .map_err(|e| anyhow::format_err!("Failed to create message body: {e}"))?;
    let header = ExternalInboundMessageHeader {
        dst: MsgAddressInt::from_str(&data.address)
            .map_err(|e| anyhow::format_err!("Failed to generate epoch address: {e}"))?,
        ..Default::default()
    };
    let body = SliceData::load_cell(
        msg_body
            .into_cell()
            .map_err(|e| anyhow::format_err!("Failed serialize message body: {e}"))?,
    )
    .map_err(|e| anyhow::format_err!("Failed to serialize message body: {e}"))?;
    Ok(Message::with_ext_in_header_and_body(header, body))
}
