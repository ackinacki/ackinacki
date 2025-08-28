// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
use tvm_block::messages::InternalMessageHeader;
use tvm_block::CurrencyCollection;
use tvm_block::Grams;
use tvm_block::Message;
use tvm_block::MsgAddressInt;
use tvm_types::SliceData;

use crate::block_keeper_system::abi::BLOCK_KEEPER_WALLET_ABI;
use crate::block_keeper_system::BlockKeeperSlashData;

pub fn create_wallet_slash_message(data: &BlockKeeperSlashData) -> anyhow::Result<Message> {
    let addr = data.addr.clone();
    tracing::trace!("create Slash message: {addr:?}");
    let parameters = format!(
        r#"{{"slash_type": {}, "bls_key": "{}"}}"#,
        data.slash_type,
        hex::encode(data.bls_pubkey.as_ref().to_bytes())
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
        MsgAddressInt::with_standart(None, 0, src_acc_id.into())
            .map_err(|e| anyhow::format_err!("Failed to get addr: {e}"))?,
        MsgAddressInt::with_standart(None, 0, dst_acc_id.into())
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
