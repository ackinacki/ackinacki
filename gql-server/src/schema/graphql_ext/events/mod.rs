// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::connection::ConnectionNameType;
use async_graphql::connection::EdgeNameType;
use async_graphql::OutputType;
use async_graphql::SimpleObject;

use crate::helpers::ToOptU64;
use crate::schema::db;

pub struct BlockchainEventEdge;

impl EdgeNameType for BlockchainEventEdge {
    fn type_name<T: OutputType>() -> String {
        "BlockchainEventEdge".to_string()
    }
}

pub struct BlockchainEventsConnection;

impl ConnectionNameType for BlockchainEventsConnection {
    fn type_name<T: OutputType>() -> String {
        "BlockchainEventsConnection".to_string()
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
pub struct Event {
    pub msg_id: String,
    body: Option<String>,
    /// Creation unixtime automatically set by the generating transaction.
    /// The creation unixtime equals the creation unixtime of the block
    /// containing the generating transaction.
    created_at: Option<u64>,
    /// Returns destination address string.
    dst: Option<String>,
    /// Returns source address string.
    src: Option<String>,
    pub src_dapp_id: Option<String>,
    pub msg_chain_order: Option<String>,
}

impl From<db::Message> for Event {
    fn from(msg: db::Message) -> Self {
        Self {
            msg_id: msg.id,
            body: msg.body.map(tvm_types::base64_encode),
            created_at: msg.created_at.to_opt_u64(),
            dst: msg.dst,
            src: msg.src,
            src_dapp_id: msg.src_dapp_id,
            msg_chain_order: msg.msg_chain_order,
        }
    }
}
