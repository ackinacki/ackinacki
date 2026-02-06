// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;

use async_graphql::futures_util::TryStreamExt;
use sqlx::prelude::FromRow;
use sqlx::QueryBuilder;

use crate::defaults;
use crate::helpers::u64_to_string;
use crate::schema::db::DBConnector;
use crate::schema::graphql::query::PaginateDirection;
use crate::schema::graphql::query::PaginationArgs;
use crate::schema::graphql_ext::blockchain_api::account::BlockchainMasterSeqNoFilter;
use crate::schema::graphql_ext::blockchain_api::account::BlockchainMessageTypeFilterEnum;

#[allow(dead_code)]
#[derive(Clone, Debug, FromRow)]
pub struct InBlockMessage {
    pub msg_id: String,
    pub transaction_id: String,
}

#[allow(dead_code)]
pub struct AccountMessagesQueryArgs {
    allow_latest_inconsistent_data: Option<bool>,
    master_seq_no_range: Option<BlockchainMasterSeqNoFilter>,
    counterparties: Option<Vec<String>>,
    msg_type: Option<Vec<BlockchainMessageTypeFilterEnum>>,
    min_value: Option<String>,
    pub pagination: PaginationArgs,
}

impl AccountMessagesQueryArgs {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        allow_latest_inconsistent_data: Option<bool>,
        master_seq_no_range: Option<BlockchainMasterSeqNoFilter>,
        counterparties: Option<Vec<String>>,
        msg_type: Option<Vec<BlockchainMessageTypeFilterEnum>>,
        min_value: Option<String>,
        pagination: PaginationArgs,
    ) -> Self {
        Self {
            allow_latest_inconsistent_data,
            master_seq_no_range,
            counterparties,
            msg_type,
            min_value,
            pagination,
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
    pub msg_chain_order: Option<String>,
}

impl Message {
    pub async fn list(
        db_connector: &DBConnector,
        filter: String,
        order_by: String,
        limit: Option<i32>,
    ) -> anyhow::Result<Vec<Message>> {
        let limit = match limit {
            Some(v) => v as u16,
            None => defaults::QUERY_BATCH_SIZE,
        };

        let db_names = db_connector.attached_db_names();
        tracing::trace!(db_names = ?db_names, "attached DBs:");

        if db_names.is_empty() {
            return Ok(Vec::new());
        }

        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT * FROM \"{name}\".messages {filter}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT * FROM ({union_sql}) {order_by} LIMIT {limit}");
        tracing::debug!("SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let mut messages = builder
            .build_query_as()
            .fetch(&mut *conn)
            .map_ok(|b| b)
            .try_collect::<Vec<Message>>()
            .await?;
        let mut seen = HashSet::with_capacity(messages.len());
        messages.retain(|message| seen.insert(message.id.clone()));

        Ok(messages)
    }

    pub async fn in_block_msgs(
        db_connector: &DBConnector,
        block_id: String,
    ) -> anyhow::Result<Vec<InBlockMessage>> {
        let db_names = db_connector.attached_db_names();
        tracing::trace!(db_names = ?db_names, "attached DBs:");

        if db_names.is_empty() {
            return Ok(Vec::new());
        }

        let union_sql = db_names
            .into_iter()
            .map(|name| {
                format!(
                    "SELECT t.id AS transaction_id, m.id AS msg_id
                    FROM \"{name}\".blocks b, \"{name}\".transactions t, \"{name}\".messages m
                    WHERE b.id={block_id:?} AND t.block_id=b.id AND m.id=t.in_msg"
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT * FROM ({union_sql}) GROUP BY transaction_id, msg_id");
        tracing::debug!("SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let messages = builder
            .build_query_as()
            .fetch(&mut *conn)
            .map_ok(|m| {
                tracing::debug!("m: {m:?}");
                m
            })
            .try_collect::<Vec<InBlockMessage>>()
            .await?;

        tracing::debug!("in block messages: {:?}", messages);
        Ok(messages)
    }

    pub async fn account_messages(
        db_connector: &DBConnector,
        account: String,
        args: &AccountMessagesQueryArgs,
    ) -> anyhow::Result<Vec<Message>> {
        let has_inbound = args.has_ext_in() || args.has_int_in();
        let has_outbound = args.has_ext_out() || args.has_int_out();
        let limit = args.pagination.get_limit();
        let direction = args.pagination.get_direction();

        let mut where_ops = vec![];
        let mut cursor_field = "";
        {
            let mut ops = vec![];
            if has_inbound {
                ops.push(format!("dst={account:?}"));
                cursor_field = "dst_chain_order";
            }
            if has_outbound {
                ops.push(format!("src={account:?}"));
                cursor_field = "src_chain_order";
            }
            if has_inbound && has_outbound {
                cursor_field = "COALESCE(dst_chain_order,src_chain_order)";
            }
            if !ops.is_empty() {
                where_ops.push(format!("({})", ops.join(" OR ")));
            }
        };

        if let Some(msg_types) = &args.msg_type {
            if !msg_types.is_empty() {
                let u8ed = msg_types
                    .iter()
                    .map(|t| (<BlockchainMessageTypeFilterEnum as Into<u8>>::into(*t)).to_string())
                    .collect::<Vec<String>>();
                where_ops.push(format!("msg_type IN ({})", u8ed.join(",")));
            }
        }

        if let Some(after) = &args.pagination.after {
            if !after.is_empty() {
                where_ops.push(format!("{cursor_field} > {after:?}"));
            }
        }
        if let Some(before) = &args.pagination.before {
            if !before.is_empty() {
                where_ops.push(format!("{cursor_field} < {before:?}"));
            }
        }

        if let Some(seq_no_range) = &args.master_seq_no_range {
            if let Some(start) = seq_no_range.start {
                let start = u64_to_string(start as u64);
                where_ops.push(format!("dst_chain_order >= {start:?}"));
            }
            if let Some(end) = seq_no_range.end {
                let end = u64_to_string(end as u64);
                where_ops.push(format!("dst_chain_order < {end:?}"));
            }
        }

        let order_by_sort = match direction {
            PaginateDirection::Forward => "ASC",
            PaginateDirection::Backward => "DESC",
        };

        let db_names = db_connector.attached_db_names();
        tracing::trace!(db_names = ?db_names, "attached DBs:");

        if db_names.is_empty() {
            return Ok(Vec::new());
        }

        let filter = format!("WHERE {}", where_ops.join(" AND "));
        let order_by = format!("ORDER BY {cursor_field} {order_by_sort}");

        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT * FROM \"{name}\".messages {filter}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT * FROM ({union_sql}) {order_by} LIMIT {limit}");
        tracing::debug!("account_messages: SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let result = builder
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| anyhow::format_err!("{e}"));

        match result {
            Err(e) => {
                anyhow::bail!("ERROR: {e}");
            }
            Ok(value) => {
                tracing::debug!("OK: {} rows", value.len());
                Ok(match direction {
                    PaginateDirection::Forward => value,
                    PaginateDirection::Backward => value.into_iter().rev().collect(),
                })
            }
        }
    }

    pub async fn account_events(
        db_connector: &DBConnector,
        account: String,
        pagination: &PaginationArgs,
    ) -> anyhow::Result<Vec<Message>> {
        let limit = pagination.get_limit();
        let direction = pagination.get_direction();

        let order_by_sort = match direction {
            PaginateDirection::Forward => "ASC",
            PaginateDirection::Backward => "DESC",
        };

        let mut where_ops = vec![format!("src={account:?}")];

        if !cfg!(feature = "store_events_only") {
            where_ops.push("msg_type=2".to_string());
        }

        let cursor_field = "msg_chain_order";

        if let Some(after) = &pagination.after {
            if !after.is_empty() {
                where_ops.push(format!("{cursor_field} > {after:?}"));
            }
        }

        if let Some(before) = &pagination.before {
            if !before.is_empty() {
                where_ops.push(format!("{cursor_field} < {before:?}"));
            }
        }

        let db_names = db_connector.attached_db_names();
        tracing::trace!(db_names = ?db_names, "attached DBs:");

        if db_names.is_empty() {
            return Ok(Vec::new());
        }

        let filter = format!("WHERE {}", where_ops.join(" AND "));
        let order_by = format!("ORDER BY {cursor_field} {order_by_sort}");

        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT * FROM \"{name}\".messages {filter}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT * FROM ({union_sql}) {order_by} LIMIT {limit}");

        tracing::debug!("account_events: SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        QueryBuilder::new(sql)
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map(|list| match direction {
                PaginateDirection::Forward => list,
                PaginateDirection::Backward => list.into_iter().rev().collect(),
            })
            .map_err(|e| anyhow::format_err!("{e}"))
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::Message;
    use crate::schema::db::DBConnector;
    use crate::web;

    async fn setup_db_connector() -> Arc<DBConnector> {
        let db_path = PathBuf::from("./tests/fixtures/dup_messages/bm-archive.db");
        let archive = "./tests/fixtures/dup_messages/bm-archive-1.db".to_string();
        let pool = web::open_db(db_path.clone()).await.expect("create pool");
        let db_connector = DBConnector::new(pool, db_path);
        db_connector.update_attachments(vec![archive]).await.expect("should attach arcive DB");

        db_connector
    }

    #[tokio::test]
    async fn list_deduplicates_by_id_across_attached_dbs() {
        // crate::helpers::init_tracing();
        let db_connector = setup_db_connector().await;

        let list = Message::list(
            &db_connector,
            "".to_string(),
            " ORDER BY rowid ASC ".to_string(),
            Some(30),
        )
        .await
        .expect("list messages");

        assert_eq!(list.len(), 25);
    }
}
