// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_graphql::SimpleObject;

use crate::defaults;

#[derive(SimpleObject, Clone)]
#[graphql(rename_fields = "camelCase")]
/// GraphQL Server info
pub struct Info {
    /// Server version.
    pub version: Option<String>,
    /// Server unix time in ms.
    pub time: Option<f64>,
    /// Blocks latency in ms (server time - max of blocks.gen_utime * 1000).
    pub blocks_latency: Option<f64>,
    /// Messages latency in ms (server time - max of messages.created_at *
    /// 1000).
    pub messages_latency: Option<f64>,
    /// Transactions latency in ms (server time - max of transactions.now *
    /// 1000).
    pub transactions_latency: Option<f64>,
    /// Overall latency (maximum value of blocksLatency, messagesLatency and
    /// transactionsLatency).
    pub latency: Option<f64>,
    /// Last block time in ms (maximum value of blocks.gen_utime * 1000).
    pub last_block_time: Option<f64>,
    /// Alternative endpoints of q-server.
    pub endpoints: Option<Vec<Option<String>>>,
    /// **EXPERIMENTAL**
    /// Reliable upper boundary for pagination by chain_order field. Before this
    /// boundary data inserts are almost impossible (work in progress to
    /// make them fully impossible when the feature goes into production).
    pub chain_order_boundary: Option<String>,
    #[graphql(deprecation = "Unused in the AckiNacki network")]
    /// Shows whether rempReceipts subscription is enabled.
    pub remp_enabled: Option<bool>,
    /// The maximum number of records returned by the request.
    pub batch_size: Option<u16>,
}

impl Default for Info {
    fn default() -> Self {
        Self {
            version: Some("0.68.0".into()),
            time: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as f64),
            blocks_latency: None,
            messages_latency: None,
            transactions_latency: None,
            latency: Some(5773.0),
            last_block_time: None,
            endpoints: Some(vec![]),
            chain_order_boundary: None,
            remp_enabled: Some(false),
            batch_size: Some(defaults::QUERY_BATCH_SIZE),
        }
    }
}
