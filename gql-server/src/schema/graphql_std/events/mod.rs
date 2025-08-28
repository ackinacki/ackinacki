// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::SimpleObject;

use crate::helpers::ToOptU64;
use crate::schema::db;

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
    pub msg_chain_order: Option<String>,
}

impl From<db::Message> for Event {
    fn from(msg: db::Message) -> Self {
        Self {
            msg_id: msg.id,
            body: msg.body.map(tvm_types::base64_encode),
            created_at: msg.created_at.to_opt_u64(),
            dst: msg.dst,
            msg_chain_order: msg.msg_chain_order,
        }
    }
}
