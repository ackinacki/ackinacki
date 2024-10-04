// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::futures_util::TryStreamExt;
use serde::Deserialize;
use sqlx::prelude::FromRow;
use sqlx::SqlitePool;
use tvm_client::boc::ParamsOfParse;

use crate::client::BlockchainClient;
use crate::defaults;
use crate::helpers::u64_to_string;
use crate::schema::graphql::blockchain_api::account::BlockchainMasterSeqNoFilter;
use crate::schema::graphql::blockchain_api::account::BlockchainMessageTypeFilterEnum;

#[derive(Deserialize)]
struct MessageBody {
    body: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, FromRow)]
pub struct InBlockMessage {
    pub msg_id: String,
    pub transaction_id: String,
}

pub enum PaginateDirection {
    Forward,
    Backward,
}

#[allow(dead_code)]
pub struct AccountMessagesQueryArgs {
    allow_latest_inconsistent_data: Option<bool>,
    master_seq_no_range: Option<BlockchainMasterSeqNoFilter>,
    counterparties: Option<Vec<String>>,
    msg_type: Option<Vec<BlockchainMessageTypeFilterEnum>>,
    min_value: Option<String>,
    first: Option<usize>,
    after: Option<String>,
    last: Option<usize>,
    before: Option<String>,
}

impl AccountMessagesQueryArgs {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        allow_latest_inconsistent_data: Option<bool>,
        master_seq_no_range: Option<BlockchainMasterSeqNoFilter>,
        counterparties: Option<Vec<String>>,
        msg_type: Option<Vec<BlockchainMessageTypeFilterEnum>>,
        min_value: Option<String>,
        first: Option<usize>,
        after: Option<String>,
        last: Option<usize>,
        before: Option<String>,
    ) -> Self {
        Self {
            allow_latest_inconsistent_data,
            master_seq_no_range,
            counterparties,
            msg_type,
            min_value,
            first,
            after,
            last,
            before,
        }
    }

    fn has_msg_type(&self, value: BlockchainMessageTypeFilterEnum) -> bool {
        match &self.msg_type {
            Some(list) => list.contains(&value),
            None => true,
        }
    }

    fn has_ext_in(&self) -> bool {
        self.has_msg_type(BlockchainMessageTypeFilterEnum::ExtIn)
    }

    fn has_ext_out(&self) -> bool {
        self.has_msg_type(BlockchainMessageTypeFilterEnum::ExtOut)
    }

    fn has_int_in(&self) -> bool {
        self.has_msg_type(BlockchainMessageTypeFilterEnum::IntIn)
    }

    fn has_int_out(&self) -> bool {
        self.has_msg_type(BlockchainMessageTypeFilterEnum::IntOut)
    }

    pub fn get_limit(&self) -> usize {
        1 + if let Some(first) = self.first {
            first
        } else if let Some(last) = self.last {
            last
        } else {
            defaults::QUERY_BATCH_SIZE.into()
        }
    }

    pub fn get_direction(&self) -> PaginateDirection {
        if self.last.is_some() || self.before.is_some() {
            PaginateDirection::Backward
        } else {
            PaginateDirection::Forward
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Default, FromRow, Debug)]
#[sqlx(default)]
pub struct Message {
    #[sqlx(skip)]
    pub rowid: Option<i64>, // id INTEGER PRIMARY KEY,
    pub id: String,                     // msg_id TEXT NOT NULL UNIQUE,
    pub boc: Option<Vec<u8>>,           // boc BLOB,
    pub body: Option<Vec<u8>>,          // body BLOB,
    pub body_hash: Option<String>,      // body_hash TEXT,
    pub status: Option<i64>,            // status INTEGER,
    pub transaction_id: Option<String>, // transaction_id TEXT,
    pub msg_type: Option<i64>,          // msg_type INTEGER,
    pub src: Option<String>,            // src TEXT,
    pub src_workchain_id: Option<i64>,  // src_workchain_id INTEGER,
    pub dst: Option<String>,            // dst TEXT,
    pub dst_workchain_id: Option<i64>,  // dst_workchain_id INTEGER,
    pub import_fee: Option<String>,
    pub fwd_fee: Option<String>, // fwd_fee TEXT,
    pub bounce: Option<i64>,     // bounce INTEGER,
    pub bounced: Option<i64>,    // bounced INTEGER,
    pub value: Option<String>,   // value,
    pub value_other: Option<Vec<u8>>,
    pub created_lt: Option<String>,      // created_lt TEXT,
    pub created_at: Option<i64>,         // created_at INTEGER,
    pub dst_chain_order: Option<String>, // dst_chain_order TEXT,
    pub src_chain_order: Option<String>, // src_chain_order TEXT
    pub proof: Option<String>,
    pub code: Option<Vec<u8>>,
    pub code_hash: Option<String>,
    pub data: Option<Vec<u8>>,
    pub data_hash: Option<String>,
    pub src_dapp_id: Option<String>, // src_dapp_id TEXT
}

impl Message {
    pub async fn list(
        pool: &SqlitePool,
        filter: String,
        order_by: String,
        limit: Option<i32>,
    ) -> anyhow::Result<Vec<Message>> {
        let limit = match limit {
            Some(v) => v as u16,
            None => defaults::QUERY_BATCH_SIZE,
        };

        let sql = format!("SELECT * FROM messages {filter} {order_by} LIMIT {limit}");
        log::debug!("SQL: {sql}");
        let messages =
            sqlx::query_as(&sql).fetch(pool).map_ok(|b| b).try_collect::<Vec<Message>>().await?;

        Ok(messages)
    }

    pub async fn in_block_msgs(
        pool: &SqlitePool,
        block_id: String,
    ) -> anyhow::Result<Vec<InBlockMessage>> {
        let sql = format!(
            "SELECT t.id AS transaction_id, m.id AS msg_id
            FROM blocks b, transactions t, messages m
            WHERE b.id={:?} AND t.block_id=b.id AND m.id=t.in_msg
        ",
            block_id
        );

        let messages = sqlx::query_as(&sql)
            .fetch(pool)
            .map_ok(|m| {
                log::debug!("m: {m:?}");
                m
            })
            .try_collect::<Vec<InBlockMessage>>()
            .await?;

        log::debug!("in block messages: {:?}", messages);
        Ok(messages)
    }

    pub async fn account_messages(
        pool: &SqlitePool,
        an_client: BlockchainClient,
        account: String,
        args: AccountMessagesQueryArgs,
    ) -> anyhow::Result<Vec<Message>> {
        let has_inbound = args.has_ext_in() || args.has_int_in();
        let has_outbound = args.has_ext_out() || args.has_int_out();
        let limit = args.get_limit();
        let direction = args.get_direction();

        let mut where_ops = vec![];
        {
            let mut ops = vec![];
            if has_inbound {
                ops.push(format!("dst={:?}", account));
            }
            if has_outbound {
                ops.push(format!("src={:?}", account));
            }
            if !ops.is_empty() {
                where_ops.push(format!("({})", ops.join(" OR ")));
            }
        };

        if let Some(msg_types) = args.msg_type {
            if !msg_types.is_empty() {
                let u8ed = msg_types
                    .into_iter()
                    .map(|t| (<BlockchainMessageTypeFilterEnum as Into<u8>>::into(t)).to_string())
                    .collect::<Vec<String>>();
                where_ops.push(format!("msg_type IN ({})", u8ed.join(",")));
            }
        }

        if let Some(after) = args.after {
            if !after.is_empty() {
                where_ops.push(format!("dst_chain_order > {:?}", after));
            }
        }
        if let Some(before) = args.before {
            if !before.is_empty() {
                where_ops.push(format!("dst_chain_order < {:?}", before));
            }
        }

        if let Some(seq_no_range) = args.master_seq_no_range {
            if let Some(start) = seq_no_range.start {
                let start = u64_to_string(start as u64);
                where_ops.push(format!("dst_chain_order >= {start:?}"));
            }
            if let Some(end) = seq_no_range.end {
                let end = u64_to_string(end as u64);
                where_ops.push(format!("dst_chain_order < {end:?}"));
            }
        }

        let order_by = match direction {
            PaginateDirection::Forward => "ASC",
            PaginateDirection::Backward => "DESC",
        };
        let sql = format!(
            "SELECT *
            FROM messages
            WHERE {}
            ORDER BY dst_chain_order {} LIMIT {}",
            where_ops.join(" AND "),
            order_by,
            limit,
        );

        log::debug!("account_messages: SQL: {sql}");
        let result =
            sqlx::query_as(&sql).fetch_all(pool).await.map_err(|e| anyhow::format_err!("{}", e));

        match result {
            Err(e) => {
                anyhow::bail!("ERROR: {e}");
            }
            Ok(mut value) => {
                log::debug!("OK: {} rows", value.len());
                value.iter_mut().for_each(|m: &mut Message| {
                    m.id = format!("message/{}", m.id);
                    m.body = if let Some(boc) = &m.boc {
                        let params =
                            ParamsOfParse { boc: tvm_types::base64_encode(boc).to_string() };
                        let res = tvm_client::boc::parse_message(an_client.clone(), params);
                        match res {
                            Ok(value) => {
                                let body = serde_json::from_value::<MessageBody>(value.parsed)
                                    .unwrap()
                                    .body;
                                match body {
                                    Some(body) => tvm_types::base64_decode(body).ok(),
                                    _ => None,
                                }
                            }
                            Err(_) => None,
                        }
                        // log::trace!("parse_message(): {:?}",
                        // res.parsed["body"]);
                    } else {
                        None
                    };
                });
                Ok(match direction {
                    PaginateDirection::Forward => value,
                    PaginateDirection::Backward => value.into_iter().rev().collect(),
                })
            }
        }
    }
}
