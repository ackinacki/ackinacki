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

use crate::block_keeper_system::abi::EPOCH_ABI;
use crate::block_keeper_system::BlockKeeperData;
use crate::block_keeper_system::BlockKeeperStatus;
use crate::bls::gosh_bls::PubKey;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::types::AccountAddress;

const BLS_PUBKEY_TOKEN_KEY: &str = "_bls_pubkey";
const WALLET_ID_TOKEN_KEY: &str = "_walletId";
const EPOCH_FINISH_TOKEN_KEY: &str = "_unixtimeFinish";
const STAKE_TOKEN_KEY: &str = "_stake";
const OWNER_TOKEN_KEY: &str = "_owner_address";
const SIGNER_INDEX_KEY: &str = "_signerIndex";

fn get_epoch_abi() -> tvm_client::abi::Abi {
    tvm_client::abi::Abi::Json(EPOCH_ABI.to_string())
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
        let mut block_keeper_wallet_id = None;
        let mut block_keeper_epoch_finish = None;
        let mut block_keeper_stake = None;
        let mut wallet_address = None;
        let mut signer_index = None;
        for token in decoded_data {
            if token.name == BLS_PUBKEY_TOKEN_KEY {
                if let TokenValue::Bytes(pubkey) = token.value {
                    block_keeper_bls_key = Some(PubKey::from(pubkey.as_slice()));
                }
            } else if token.name == WALLET_ID_TOKEN_KEY {
                if let TokenValue::Uint(wallet_id) = token.value {
                    // TODO: check that wallet id fits boundaries
                    tracing::trace!("decoded epoch wallet id: {wallet_id:?}");
                    block_keeper_wallet_id = Some(if wallet_id.number.is_zero() {
                        0
                    } else {
                        wallet_id.number.to_u64_digits()[0] as NodeIdentifier
                    });
                }
            } else if token.name == EPOCH_FINISH_TOKEN_KEY {
                if let TokenValue::Uint(epoch_finish) = token.value {
                    tracing::trace!("decoded epoch finish: {epoch_finish:?}");
                    block_keeper_epoch_finish = Some(if epoch_finish.number.is_zero() {
                        0
                    } else {
                        epoch_finish.number.to_u32_digits()[0]
                    });
                }
            } else if token.name == STAKE_TOKEN_KEY {
                if let TokenValue::Uint(stake) = token.value {
                    tracing::trace!("decoded epoch stake: {stake:?}");
                    block_keeper_stake =
                        Some(if stake.number.is_zero() { BigUint::zero() } else { stake.number });
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
        if block_keeper_bls_key.is_some()
            && block_keeper_wallet_id.is_some()
            && block_keeper_epoch_finish.is_some()
            && block_keeper_stake.is_some()
            && wallet_address.is_some()
            && signer_index.is_some()
        {
            let signer_index = signer_index.unwrap();
            return Ok(Some((
                signer_index,
                BlockKeeperData {
                    wallet_index: block_keeper_wallet_id.unwrap(),
                    pubkey: block_keeper_bls_key.unwrap(),
                    epoch_finish_timestamp: block_keeper_epoch_finish.unwrap(),
                    status: BlockKeeperStatus::Active,
                    // TODO: better fix pure unwrap for address
                    address: account.get_addr().unwrap().to_string(),
                    stake: block_keeper_stake.unwrap(),
                    owner_address: AccountAddress(AccountId::from(wallet_address.unwrap())),
                    signer_index,
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
        Some(&format!(r#"{{"expire":{}}}"#, expire)),
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
