// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
use num_traits::Zero;
use tvm_abi::TokenValue;
use tvm_block::messages::InternalMessageHeader;
use tvm_block::Account;
use tvm_block::CurrencyCollection;
use tvm_block::GetRepresentationHash;
use tvm_block::Grams;
use tvm_block::Message;
use tvm_block::MsgAddress;
use tvm_block::MsgAddressInt;
use tvm_block::Serializable;
use tvm_block::StateInit;
use tvm_client::boc::set_code_salt_cell;
use tvm_client::encoding::slice_from_cell;
use tvm_types::AccountId;
use tvm_types::BuilderData;
use tvm_types::SliceData;
use tvm_types::UInt256;

use crate::block_keeper_system::abi::BLOCK_KEEPER_WALLET_ABI;
use crate::block_keeper_system::abi::BLOCK_KEEPER_WALLET_CONFIG_ABI;
use crate::block_keeper_system::BlockKeeperSlashData;
use crate::node::SignerIndex;
use crate::types::AccountAddress;

const WALLET_ID_TOKEN_KEY: &str = "_node_id";
const OWNER_TOKEN_KEY: &str = "_wallet_addr";

fn get_wallet_config_abi() -> tvm_client::abi::Abi {
    tvm_client::abi::Abi::Json(BLOCK_KEEPER_WALLET_CONFIG_ABI.to_string())
}

pub fn decode_wallet_config_data(
    account: &Account,
) -> anyhow::Result<Option<(SignerIndex, AccountAddress)>> {
    let abi = get_wallet_config_abi();
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
        let mut block_keeper_wallet_id = None;
        let mut wallet_address = None;
        for token in decoded_data {
            if token.name == WALLET_ID_TOKEN_KEY {
                if let TokenValue::Uint(wallet_id) = token.value {
                    // TODO: check that wallet id fits boundaries
                    tracing::trace!("decoded epoch wallet id: {wallet_id:?}");
                    block_keeper_wallet_id = Some(if wallet_id.number.is_zero() {
                        0
                    } else {
                        wallet_id.number.to_u64_digits()[0] as SignerIndex
                    });
                }
            } else if token.name == OWNER_TOKEN_KEY {
                if let TokenValue::Address(addr) = token.value {
                    tracing::trace!("decoded wallet address: {addr:?}");
                    wallet_address = match addr {
                        MsgAddress::AddrNone => None,
                        MsgAddress::AddrExt(_) => None,
                        MsgAddress::AddrStd(data) => {
                            let u256addr: UInt256 =
                                data.address.get_bytestring(0).as_slice().into();
                            let addr = AccountId::from(u256addr);
                            Some(addr)
                        }
                        MsgAddress::AddrVar(_) => None,
                    }
                }
            }
        }
        if block_keeper_wallet_id.is_some() && wallet_address.is_some() {
            let index = block_keeper_wallet_id.unwrap();
            return Ok(Some((index, AccountAddress(AccountId::from(wallet_address.unwrap())))));
        }
    }
    Ok(None)
}

pub fn calculate_wallet_config_address(
    node_id: u128,
    mut data: StateInit,
) -> anyhow::Result<UInt256> {
    let code = data.code().unwrap();
    let mut b = BuilderData::new();
    node_id.write_to(&mut b).map_err(|e| anyhow::format_err!("Failed to write dapp id: {e}"))?;
    let stateinitcode = set_code_salt_cell(code.clone(), b.into_cell().unwrap())
        .map_err(|e| anyhow::format_err!("Failed to set code salt: {e}"))?;
    data.set_code(stateinitcode);
    data.hash().map_err(|e| anyhow::format_err!("Failed to calculate hash: {e}"))
}

pub fn create_wallet_slash_message(data: &BlockKeeperSlashData) -> anyhow::Result<Message> {
    let addr: AccountId = data.addr.clone();
    tracing::trace!("create Slash message: {addr:?}");
    let parameters = format!(
        r#"{{"slash_type": {}, "bls_key": "{}"}}"#,
        data.slash_type,
        hex::encode(data.bls_pubkey.as_ref())
    );
    let msg_body = tvm_abi::encode_function_call(
        BLOCK_KEEPER_WALLET_ABI,
        "slash",
        None,
        &parameters,
        true,
        None,
        None,
    )
    .map_err(|e| anyhow::format_err!("Failed to create message body: {e}"))?;
    let src_acc_id = addr.clone();
    let dst_acc_id = addr.clone();
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
