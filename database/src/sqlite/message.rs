// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use serde::Deserialize;
use serde::Serialize;
use tvm_block::CommonMsgInfo;
use tvm_block::Grams;
use tvm_block::Message;
use tvm_block::Serializable;
use tvm_types::write_boc;

use crate::serialization::MessageSerializationSet;

#[derive(Deserialize, Serialize, Clone, Debug, Default)]
pub struct ArchMessage {
    pub id: String,
    pub transaction_id: Option<String>,
    pub boc: Option<Vec<u8>>,
    pub body: Option<Vec<u8>>,
    pub body_hash: Option<String>,
    pub status: Option<u8>,
    pub msg_type: Option<u8>,
    pub src: Option<String>,
    pub src_dapp_id: Option<String>,
    pub src_workchain_id: Option<i32>,
    pub dst: Option<String>,
    pub dst_workchain_id: Option<i32>,
    pub fwd_fee: Option<String>,
    pub bounce: Option<bool>,
    pub bounced: Option<bool>,
    pub value: Option<String>,
    pub value_other: Option<Vec<u8>>,
    pub created_lt: Option<String>,
    pub created_at: Option<u32>,
    pub dst_chain_order: Option<String>,
    pub src_chain_order: Option<String>,
    pub proof: Option<Vec<u8>>,
    pub code: Option<Vec<u8>>,
    pub code_hash: Option<String>,
    pub data: Option<Vec<u8>>,
    pub data_hash: Option<String>,
    pub msg_chain_order: Option<String>,
}

impl From<MessageSerializationSet> for ArchMessage {
    fn from(msg: MessageSerializationSet) -> Self {
        let id = msg.id.to_hex_string();
        let transaction_id = msg.transaction_id.map(|v| v.as_hex_string());
        let boc = Some(msg.boc);
        let body = msg
            .message
            .body()
            .as_ref()
            .map(|b| write_boc(&b.clone().into_cell()).expect("Failed to serialize msg body"));
        let status = Some(msg.status as u8);
        let proof = msg.proof;
        let mut code = None;
        let mut code_hash = None;
        let mut data = None;
        let mut data_hash = None;
        if let Some(state) = msg.message.state_init() {
            if let Some(cell) = state.code() {
                if !cell.is_pruned() {
                    code = write_boc(cell).ok();
                    code_hash = Some(cell.repr_hash().to_hex_string());
                }
            }
            if let Some(cell) = state.data() {
                if !cell.is_pruned() {
                    data = write_boc(cell).ok();
                    data_hash = Some(cell.repr_hash().to_hex_string());
                }
            }
        }

        let arch_msg = match msg.message.header() {
            CommonMsgInfo::IntMsgInfo(header) => Self {
                id,
                transaction_id,
                boc,
                body,
                status,
                proof,
                code,
                code_hash,
                data,
                data_hash,
                msg_type: Some(0),
                src: Some(header.src.to_string()),
                dst: Some(header.dst.to_string()),
                bounce: Some(header.bounce),
                bounced: Some(header.bounced),
                created_at: Some(header.created_at.as_u32()),
                created_lt: Some(header.created_lt.to_string()),
                value: Some(format!("{:x}", header.value.grams.as_u128())),
                value_other: write_boc(
                    &header.value.other.serialize().expect("Failed to serialize ECC"),
                )
                .ok(),
                fwd_fee: Some(format!("{:x}", header.fwd_fee.as_u128())),
                ..Default::default()
            },
            CommonMsgInfo::ExtInMsgInfo(header) => Self {
                id,
                transaction_id,
                boc,
                status,
                proof,
                code,
                code_hash,
                data,
                data_hash,
                msg_type: Some(1),
                src: Some(header.src.to_string()),
                dst: Some(header.dst.to_string()),
                created_at: msg.transaction_now,
                ..Default::default()
            },
            CommonMsgInfo::ExtOutMsgInfo(header) => Self {
                id,
                transaction_id,
                boc,
                body,
                status,
                proof,
                code,
                code_hash,
                data,
                data_hash,
                msg_type: Some(2),
                src: Some(header.src.to_string()),
                dst: Some(header.dst.to_string()),
                created_at: Some(header.created_at.as_u32()),
                created_lt: Some(header.created_lt.to_string()),
                ..Default::default()
            },
        };

        arch_msg
    }
}

pub(crate) fn get_msg_fees(msg: &Message) -> Option<(&Grams, &Grams)> {
    match msg.header() {
        CommonMsgInfo::IntMsgInfo(header) => Some((&header.ihr_fee, &header.fwd_fee)),
        _ => None,
    }
}
