// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::ComplexObject;
use async_graphql::Enum;
use async_graphql::SimpleObject;

use super::currency::OtherCurrency;
use super::formats::BigIntFormat;
use super::transaction::Transaction;
use crate::helpers::ecc_from_bytes;
use crate::helpers::format_big_int;
use crate::helpers::ToBool;
use crate::helpers::ToInt;
use crate::helpers::ToOptU64;
use crate::schema::db::message::InBlockMessage;
use crate::schema::db::{self};

mod filter;
mod resolver;
pub use filter::MessageFilter;
pub use resolver::MessageLoader;

#[derive(Enum, Clone, Copy, PartialEq, Eq, Debug)]
#[graphql(rename_items = "PascalCase")]
pub enum InMsgTypeEnum {
    External,
    Ihr,
    Immediately,
    Final,
    Transit,
    DiscardedFinal,
    DiscardedTransit,
}

impl From<Option<i64>> for InMsgTypeEnum {
    fn from(val: Option<i64>) -> Self {
        match val.unwrap_or(0) {
            0 => InMsgTypeEnum::External,
            1 => InMsgTypeEnum::Ihr,
            2 => InMsgTypeEnum::Immediately,
            3 => InMsgTypeEnum::Final,
            4 => InMsgTypeEnum::Transit,
            5 => InMsgTypeEnum::DiscardedFinal,
            6 => InMsgTypeEnum::DiscardedTransit,
            _ => todo!(),
        }
    }
}

#[derive(Enum, Clone, Copy, PartialEq, Eq, Debug)]
#[graphql(rename_items = "PascalCase")]
pub enum MessageTypeEnum {
    Internal,
    ExtIn,
    ExtOut,
}

impl From<i64> for MessageTypeEnum {
    fn from(val: i64) -> Self {
        match val {
            0 => MessageTypeEnum::Internal,
            1 => MessageTypeEnum::ExtIn,
            2 => MessageTypeEnum::ExtOut,
            _ => unreachable!(),
        }
    }
}

#[derive(Enum, Clone, Copy, PartialEq, Eq, Debug)]
#[graphql(rename_items = "PascalCase")]
pub enum MessageProcessingStatusEnum {
    Unknown,
    Queued,
    Processing,
    Preliminary,
    Proposed,
    Finalized,
    Refused,
    Transiting,
}

impl From<Option<i64>> for MessageProcessingStatusEnum {
    fn from(val: Option<i64>) -> Self {
        match val.unwrap_or(0) {
            0 => MessageProcessingStatusEnum::Unknown,
            1 => MessageProcessingStatusEnum::Queued,
            2 => MessageProcessingStatusEnum::Processing,
            3 => MessageProcessingStatusEnum::Preliminary,
            4 => MessageProcessingStatusEnum::Proposed,
            5 => MessageProcessingStatusEnum::Finalized,
            6 => MessageProcessingStatusEnum::Refused,
            7 => MessageProcessingStatusEnum::Transiting,
            _ => unreachable!(),
        }
    }
}

#[derive(Enum, Clone, Copy, PartialEq, Eq, Debug)]
#[graphql(rename_items = "PascalCase")]
enum OutMsgTypeEnum {
    External,
    Immediately,
    OutMsgNew,
    Transit,
    DequeueImmediately,
    Dequeue,
    TransitRequired,
    DequeueShort,
    None,
}

impl From<i32> for OutMsgTypeEnum {
    fn from(val: i32) -> Self {
        match val {
            -1 => OutMsgTypeEnum::None,
            0 => OutMsgTypeEnum::External,
            1 => OutMsgTypeEnum::Immediately,
            2 => OutMsgTypeEnum::OutMsgNew,
            3 => OutMsgTypeEnum::Transit,
            4 => OutMsgTypeEnum::DequeueImmediately,
            5 => OutMsgTypeEnum::Dequeue,
            6 => OutMsgTypeEnum::TransitRequired,
            7 => OutMsgTypeEnum::DequeueShort,
            _ => todo!(),
        }
    }
}

#[derive(SimpleObject, Clone, Debug, Default)]
#[graphql(complex, rename_fields = "snake_case")]
pub struct InMsg {
    #[graphql(skip)]
    fwd_fee: Option<String>,
    #[graphql(skip)]
    ihr_fee: Option<String>,
    in_msg: Option<MsgEnvelope>,
    msg_id: Option<String>,
    msg_type: Option<i32>,
    msg_type_name: Option<InMsgTypeEnum>,
    out_msg: Option<MsgEnvelope>,
    proof_created: Option<String>,
    proof_delivered: Option<String>,
    transaction_id: Option<String>,
    #[graphql(skip)]
    transit_fee: Option<String>,
}

#[ComplexObject]
impl InMsg {
    #[graphql(name = "fwd_fee")]
    async fn fwd_fee(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.fwd_fee.clone(), format)
    }

    #[graphql(name = "ihr_fee")]
    async fn ihr_fee(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.ihr_fee.clone(), format)
    }

    #[graphql(name = "transit_fee")]
    async fn transit_fee(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.transit_fee.clone(), format)
    }
}

impl From<db::Message> for InMsg {
    fn from(msg: db::Message) -> Self {
        Self {
            fwd_fee: msg.fwd_fee,
            ihr_fee: None,
            in_msg: None,
            msg_id: Some(msg.id),
            msg_type: msg.msg_type.to_int(),
            msg_type_name: Some(msg.msg_type.into()),
            out_msg: None,
            proof_created: Some("".to_string()),
            proof_delivered: Some("".to_string()),
            transaction_id: msg.transaction_id,
            transit_fee: None,
        }
    }
}

impl From<InBlockMessage> for InMsg {
    fn from(msg: InBlockMessage) -> Self {
        Self {
            fwd_fee: None,
            ihr_fee: None,
            in_msg: None,
            msg_id: Some(msg.msg_id),
            msg_type: None,
            msg_type_name: None,
            out_msg: None,
            proof_created: None,
            proof_delivered: None,
            transaction_id: Some(msg.transaction_id),
            transit_fee: None,
        }
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
pub(crate) struct MsgEnvelope {
    cur_addr: Option<String>,
    #[graphql(skip)]
    fwd_fee_remaining: Option<String>,
    msg_id: Option<String>,
    next_addr: Option<String>,
}

#[ComplexObject]
impl MsgEnvelope {
    #[graphql(name = "fwd_fee_remaining")]
    async fn fwd_fee_remaining(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.fwd_fee_remaining.clone(), format)
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
pub struct Message {
    pub id: String,
    block_id: Option<String>,
    /// A bag of cells with the message structure encoded as base64.
    boc: Option<String>,
    body: Option<String>,
    /// `body` field root hash.
    body_hash: Option<String>,
    /// Bounce flag. If the transaction has been aborted, and the inbound
    /// message has its bounce flag set, then it is “bounced” by
    /// automatically generating an outbound message (with the bounce flag
    /// clear) to its original sender.
    bounce: Option<bool>,
    /// Bounced flag. If the transaction has been aborted, and the inbound
    /// message has its bounce flag set, then it is “bounced” by
    /// automatically generating an outbound message (with the bounce flag
    /// clear) to its original sender.
    bounced: Option<bool>,
    /// Represents contract code in deploy messages.
    code: Option<String>,
    /// `code` field root hash.
    code_hash: Option<String>,
    /// Creation unixtime automatically set by the generating transaction.
    /// The creation unixtime equals the creation unixtime of the block
    /// containing the generating transaction.
    created_at: Option<u64>,
    #[graphql(skip)]
    created_lt: Option<String>,
    /// Represents initial data for a contract in deploy messages.
    data: Option<String>,
    /// `data` field root hash.
    data_hash: Option<String>,
    /// Returns destination address string.
    dst: Option<String>,
    /// Acki Nacki transaction
    pub dst_transaction: Option<Box<Transaction>>,
    /// Collection-unique field for pagination and sorting. This field is
    /// designed to retain logical output order (for logical input order use
    /// transaction.in_message).
    pub dst_chain_order: Option<String>,
    /// Workchain id of the destination address (dst field).
    dst_workchain_id: Option<i32>,
    #[graphql(skip)]
    fwd_fee: Option<String>,
    /// IHR is disabled for the message.
    ihr_disabled: Option<bool>,
    #[graphql(skip)]
    ihr_fee: Option<String>,
    /// Represents contract library in deploy messages.
    library: Option<String>,
    /// `library` field root hash.
    library_hash: Option<String>,
    /// Returns the type of message.
    /// - 0 – internal
    /// - 1 – extIn
    /// - 2 – extOut
    msg_type: Option<i32>,
    msg_type_name: Option<MessageTypeEnum>,
    /// Merkle proof that message is a part of a block it cut from. It is a
    /// bag of cells with Merkle proof struct encoded as base64.
    proof: Option<String>,
    /// Returns source address string.
    src: Option<String>,
    /// Collection-unique field for pagination and sorting. This field is
    /// designed to retain logical output order (for logical input order use
    /// transaction.in_message).
    pub src_chain_order: Option<String>,
    pub src_dapp_id: Option<String>,
    /// Acki Nacki transaction
    pub src_transaction: Option<Box<Transaction>>,
    /// Workchain id of the source address (src field).
    src_workchain_id: Option<i32>,
    /// Returns internal processing status according to the numbers shown.
    /// - 0 – unknown
    /// - 1 – queued
    /// - 2 – processing
    /// - 3 – preliminary
    /// - 4 – proposed
    /// - 5 – finalized
    /// - 6 – refused
    /// - 7 – transiting
    status: Option<i32>,
    status_name: Option<MessageProcessingStatusEnum>,
    /// This is only used for special contracts in masterchain to deploy
    /// messages.
    tick: Option<bool>,
    /// This is only used for special contracts in masterchain to deploy
    /// messages.
    tock: Option<bool>,
    pub transaction_id: Option<String>,
    #[graphql(skip)]
    value: Option<String>,
    value_other: Option<Vec<OtherCurrency>>,
}

impl From<db::Message> for Message {
    fn from(msg: db::Message) -> Self {
        let boc = msg.boc.map(tvm_types::base64_encode);
        let body = msg.body.map(tvm_types::base64_encode);
        let code = msg.code.map(tvm_types::base64_encode);
        let data = msg.data.map(tvm_types::base64_encode);
        let proof = msg.proof.map(tvm_types::base64_encode);
        let value_other = ecc_from_bytes(msg.value_other).expect("Failed to decode ECC");
        Self {
            id: msg.id,
            block_id: None,
            boc,
            body,
            body_hash: msg.body_hash,
            bounce: msg.bounce.to_bool(),
            bounced: msg.bounced.to_bool(),
            code,
            code_hash: msg.code_hash,
            created_at: msg.created_at.to_opt_u64(),
            created_lt: msg.created_lt,
            data,
            data_hash: msg.data_hash,
            dst: msg.dst,
            dst_chain_order: msg.dst_chain_order,
            dst_transaction: None,
            dst_workchain_id: msg.dst_workchain_id.to_int(),
            fwd_fee: msg.fwd_fee,
            ihr_disabled: None,
            ihr_fee: None,
            library: None,
            library_hash: None,
            msg_type: msg.msg_type.to_int(),
            msg_type_name: msg.msg_type.map(|msg_type| msg_type.into()),
            proof,
            src: msg.src,
            src_chain_order: msg.src_chain_order,
            src_dapp_id: msg.src_dapp_id,
            src_transaction: None,
            src_workchain_id: msg.src_workchain_id.to_int(),
            status: msg.status.to_int(),
            status_name: Some(msg.status.into()),
            tick: None,
            tock: None,
            transaction_id: msg.transaction_id,
            value: msg.value,
            value_other,
        }
    }
}

#[ComplexObject]
impl Message {
    #[graphql(name = "created_lt")]
    /// Logical creation time automatically set by the generating transaction.
    async fn created_lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.created_lt.clone(), format)
    }

    #[graphql(name = "ihr_fee")]
    /// This value is subtracted from the value attached to the message and
    /// awarded to the validators of the destination shardchain if they
    /// include the message by the IHR mechanism.
    async fn ihr_fee(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.ihr_fee.clone(), format)
    }

    #[graphql(name = "fwd_fee")]
    /// Original total forwarding fee paid for using the HR mechanism; it is
    /// automatically computed from some configuration parameters and the
    /// size of the message at the time the message is generated.
    async fn fwd_fee(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.fwd_fee.clone(), format)
    }

    #[graphql(name = "value")]
    /// May or may not be present
    async fn value(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.value.clone(), format)
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
pub struct OutMsg {
    #[graphql(skip)]
    import_block_lt: Option<String>,
    imported: Option<InMsg>,
    msg_env_hash: Option<String>,
    msg_id: Option<String>,
    msg_type: Option<i32>,
    msg_type_name: Option<OutMsgTypeEnum>,
    #[graphql(skip)]
    next_addr_pfx: Option<String>,
    next_workchain: Option<i32>,
    out_msg: Option<MsgEnvelope>,
    reimport: Option<InMsg>,
    transaction_id: Option<String>,
}

impl From<Message> for OutMsg {
    fn from(msg: Message) -> Self {
        Self {
            import_block_lt: None,
            imported: None,
            msg_env_hash: None,
            msg_id: Some(msg.id),
            msg_type: msg.msg_type,
            msg_type_name: None,
            next_addr_pfx: None,
            next_workchain: None,
            reimport: None,
            transaction_id: msg.transaction_id,
            out_msg: None,
        }
    }
}

#[ComplexObject]
impl OutMsg {
    #[graphql(name = "import_block_lt")]
    async fn import_block_lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.import_block_lt.clone(), format)
    }

    #[graphql(name = "next_addr_pfx")]
    async fn next_addr_pfx(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.next_addr_pfx.clone(), format)
    }
}
