// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::futures_util::TryStreamExt;
use sqlx::prelude::FromRow;
use sqlx::QueryBuilder;

use crate::defaults;
use crate::helpers::u64_to_string;
use crate::schema::db::DBConnector;
use crate::schema::graphql::query::PaginateDirection;
use crate::schema::graphql::query::PaginationArgs;
use crate::schema::graphql_ext::blockchain_api::account::BlockchainMasterSeqNoFilter;
use crate::schema::graphql_ext::blockchain_api::transactions::BlockchainTransactionsQueryArgs;

#[allow(dead_code)]
pub struct AccountTransactionsQueryArgs {
    allow_latest_inconsistent_data: Option<bool>,
    block_seq_no_range: Option<BlockchainMasterSeqNoFilter>,
    aborted: Option<bool>,
    min_balance_delta: Option<String>,
    max_balance_delta: Option<String>,
    pub pagination: PaginationArgs,
}

impl AccountTransactionsQueryArgs {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        allow_latest_inconsistent_data: Option<bool>,
        block_seq_no_range: Option<BlockchainMasterSeqNoFilter>,
        aborted: Option<bool>,
        min_balance_delta: Option<String>,
        max_balance_delta: Option<String>,
        pagination: PaginationArgs,
    ) -> Self {
        Self {
            allow_latest_inconsistent_data,
            block_seq_no_range,
            aborted,
            min_balance_delta,
            max_balance_delta,
            pagination,
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, FromRow, Debug)]
pub struct Transaction {
    #[sqlx(skip)]
    pub rowid: i64, // id INTEGER PRIMARY KEY,
    pub id: String,                          // transaction_id TEXT NOT NULL UNIQUE,
    pub aborted: bool,                       // aborted INTEGER NOT NULL,
    pub account_addr: String,                // account_addr TEXT NOT NULL,
    pub action_success: bool,                // action_success INTEGER NOT NULL,
    pub action_valid: bool,                  // action_valid INTEGER NOT NULL,
    pub action_no_funds: bool,               // action_no_funds INTEGER NOT NULL,
    pub action_status_change: u8,            // action_status_change INTEGER NOT NULL,
    pub action_result_code: i32,             // action_result_code INTEGER NOT NULL,
    pub action_tot_actions: i16,             // action_tot_actions INTEGER NOT NULL,
    pub action_spec_actions: i16,            // action_spec_actions INTEGER NOT NULL,
    pub action_skipped_actions: i16,         // action_skipped_actions INTEGER NOT NULL,
    pub action_msgs_created: i16,            // action_msgs_created INTEGER NOT NULL,
    pub action_list_hash: String,            // action_list_hash INTEGER NOT NULL,
    pub action_tot_msg_size_cells: f64,      // action_tot_msg_size_cells INTEGER NOT NULL,
    pub action_tot_msg_size_bits: f64,       // action_tot_msg_size_bits INTEGER NOT NULL,
    pub balance_delta: String,               // balance_delta INTEGER NOT NULL,
    pub block_id: String,                    // block_id TEXT NOT NULL,
    pub boc: Vec<u8>,                        // boc TEXT NOT NULL,
    pub chain_order: String,                 // chain_order TEXT NOT NULL
    pub credit: Option<String>,              // credit TEXT NOT NULL,
    pub credit_first: bool,                  // credit_first INTEGER NOT NULL,
    pub compute_account_activated: bool,     // compute_account_activated INTEGER NOT NULL,
    pub compute_exit_code: i32,              // compute_exit_code INTEGER NOT NULL,
    pub compute_gas_fees: String,            // compute_gas_fees INTEGER NOT NULL,
    pub compute_gas_used: f64,               // compute_gas_used INTEGER NOT NULL,
    pub compute_gas_limit: f64,              // compute_gas_limit INTEGER NOT NULL,
    pub compute_mode: i8,                    // compute_mode INTEGER NOT NULL,
    pub compute_msg_state_used: bool,        // compute_msg_state_used INTEGER NOT NULL,
    pub compute_success: bool,               // compute_success INTEGER NOT NULL,
    pub compute_type: u8,                    // compute_type INTEGER NOT NULL,
    pub compute_vm_final_state_hash: String, // compute_vm_final_state_hash TEXT NOT NULL,
    pub compute_vm_init_state_hash: String,  // compute_vm_init_state_hash TEXT NOT NULL,
    #[sqlx(try_from = "u32")]
    pub compute_vm_steps: u64, // compute_vm_steps INTEGER NOT NULL,
    pub destroyed: bool,                     // destroyed INTEGER NOT NULL,
    pub end_status: u8,                      // end_status INTEGER NOT NULL,
    pub in_msg: String,                      // in_msg TEXT NOT NULL,
    pub lt: String,                          // lt INTEGER NOT NULL,
    pub new_hash: String,                    // new_hash TEXT NOT NULL,
    #[sqlx(try_from = "u32")]
    pub now: u64, // now INTEGER NOT NULL,
    pub old_hash: String,                    // old_hash TEXT NOT NULL,
    pub orig_status: u8,                     // orig_status INTEGER NOT NULL,
    pub out_msgs: String,                    // out_msgs TEXT,
    pub outmsg_cnt: u16,                     // outmsg_cnt INTEGER NOT NULL,
    pub prev_trans_hash: String,             // prev_trans_hash TEXT NOT NULL,
    pub prev_trans_lt: String,               // prev_trans_lt TEXT NOT NULL,
    pub proof: Option<Vec<u8>>,
    pub status: u8,                             // status INTEGER NOT NULL,
    pub storage_fees_collected: Option<String>, // storage_fees_collected INTEGER NOT NULL,
    pub storage_status_change: Option<u8>,      // storage_status_change INTEGER NOT NULL,
    pub total_fees: String,                     // total_fees INTEGER NOT NULL,
    pub tr_type: u8,                            // tr_type INTEGER NOT NULL,
    pub workchain_id: i32,                      // workchain_id INTEGER NOT NULL,
}

impl Transaction {
    pub async fn list(
        db_connector: &DBConnector,
        filter: String,
        order_by: String,
        limit: Option<i32>,
    ) -> anyhow::Result<Vec<Transaction>> {
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
            .map(|name| format!("SELECT * FROM \"{name}\".transactions {filter}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT * FROM ({union_sql}) {order_by} LIMIT {limit}");
        tracing::debug!("SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let transactions = builder
            .build_query_as()
            .fetch(&mut *conn)
            .map_ok(|b| b)
            .try_collect::<Vec<Transaction>>()
            .await?;

        Ok(transactions)
    }

    pub async fn blockchain_transactions(
        db_connector: &DBConnector,
        args: &BlockchainTransactionsQueryArgs,
    ) -> anyhow::Result<Vec<Transaction>> {
        let direction = args.pagination.get_direction();
        let limit = args.pagination.get_limit();

        let mut where_ops: Vec<String> = vec![];

        if let Some(after) = &args.pagination.after {
            if !after.is_empty() {
                where_ops.push(format!("chain_order > {after:?}"));
            }
        }

        if let Some(before) = &args.pagination.before {
            if !before.is_empty() {
                where_ops.push(format!("chain_order < {before:?}"));
            }
        }

        if let Some(code_hash) = &args.code_hash {
            where_ops.push(format!("code_hash == {code_hash:?}"));
        }

        if let Some(min_balance_delta) = &args.min_balance_delta {
            where_ops.push(format!("balance_delta+0 >= {}", min_balance_delta.parse::<u128>()?));
        }

        if let Some(max_balance_delta) = &args.max_balance_delta {
            where_ops.push(format!("balance_delta+0 <= {}", max_balance_delta.parse::<u128>()?));
        }

        let order_by = match direction {
            PaginateDirection::Forward => "ASC",
            PaginateDirection::Backward => "DESC",
        };

        let where_clause = if !where_ops.is_empty() {
            format!("WHERE {}", where_ops.join(" AND "))
        } else {
            "".to_string()
        };

        let db_names = db_connector.attached_db_names();
        tracing::trace!(db_names = ?db_names, "attached DBs:");

        if db_names.is_empty() {
            return Ok(Vec::new());
        }

        let filter = where_clause;
        let order_by = format!("ORDER BY chain_order {order_by}");

        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT * FROM \"{name}\".transactions {filter}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT * FROM ({union_sql}) {order_by} LIMIT {limit}");

        tracing::trace!(target: "blockchain_api", "SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let result: Result<Vec<Transaction>, anyhow::Error> = builder
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| anyhow::format_err!("{e}"));

        if let Err(e) = result {
            anyhow::bail!("ERROR: {e}");
        }

        let list = result.unwrap();
        tracing::debug!("OK: {} rows", list.len());
        let mut ids = Vec::new();
        list.iter().for_each(|t: &Transaction| {
            ids.push(t.id.clone());
        });

        Ok(match direction {
            PaginateDirection::Forward => list,
            PaginateDirection::Backward => list.into_iter().rev().collect(),
        })
    }

    pub async fn account_transactions(
        db_connector: &DBConnector,
        account: String,
        args: &AccountTransactionsQueryArgs,
    ) -> anyhow::Result<Vec<Self>> {
        let mut where_ops = vec![];
        where_ops.push(format!("account_addr={account:?}"));

        if let Some(aborted) = args.aborted {
            where_ops.push(format!("aborted={}", aborted as u8));
        }

        if let Some(after) = &args.pagination.after {
            if !after.is_empty() {
                where_ops.push(format!("chain_order > {after:?}"));
            }
        }

        if let Some(before) = &args.pagination.before {
            if !before.is_empty() {
                where_ops.push(format!("chain_order < {before:?}"));
            }
        }

        if let Some(seq_no_range) = &args.block_seq_no_range {
            if let Some(start) = seq_no_range.start {
                let start = u64_to_string(start as u64);
                where_ops.push(format!("chain_order >= {start:?}"));
            }
            if let Some(end) = seq_no_range.end {
                let end = u64_to_string(end as u64);
                where_ops.push(format!("chain_order < {end:?}"));
            }
        }

        if let Some(min_balance_delta) = &args.min_balance_delta {
            where_ops.push(format!("balance_delta+0 >= {}", min_balance_delta.parse::<u128>()?));
        }

        if let Some(max_balance_delta) = &args.max_balance_delta {
            where_ops.push(format!("balance_delta+0 <= {}", max_balance_delta.parse::<u128>()?));
        }

        let where_clause = if !where_ops.is_empty() {
            format!("WHERE {}", where_ops.join(" AND "))
        } else {
            "".to_string()
        };

        let order_by = match args.pagination.get_direction() {
            PaginateDirection::Forward => "ASC",
            PaginateDirection::Backward => "DESC",
        };

        let db_names = db_connector.attached_db_names();
        tracing::trace!(db_names = ?db_names, "attached DBs:");

        if db_names.is_empty() {
            return Ok(Vec::new());
        }

        let filter = where_clause;
        let order_by = format!("ORDER BY chain_order {order_by}");
        let limit = args.pagination.get_limit();

        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT * FROM \"{name}\".transactions {filter}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT * FROM ({union_sql}) {order_by} LIMIT {limit}");

        tracing::trace!(target: "blockchain_api.account.transactions", "SQL: {sql}");

        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);

        let mut conn = db_connector.get_connection().await?;
        let result: Result<Vec<Transaction>, anyhow::Error> = builder
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| anyhow::format_err!("{e}"));

        if let Err(e) = result {
            anyhow::bail!("ERROR: {e}");
        }

        let list = result.unwrap();
        tracing::debug!("OK: {} rows", list.len());
        let mut ids = Vec::new();
        list.iter().for_each(|t: &Transaction| {
            ids.push(t.id.clone());
        });

        Ok(match args.pagination.get_direction() {
            PaginateDirection::Forward => list,
            PaginateDirection::Backward => list.into_iter().rev().collect(),
        })
    }

    pub async fn by_in_message(
        db_connector: &DBConnector,
        msg_id: &str,
        _fields: Option<Vec<String>>,
    ) -> anyhow::Result<Option<Transaction>> {
        let db_names = db_connector.attached_db_names();
        tracing::trace!(db_names = ?db_names, "attached DBs:");

        if db_names.is_empty() {
            return Ok(None);
        }

        let msg_id = msg_id.strip_prefix("message/").unwrap_or(msg_id);
        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT * FROM \"{name}\".transactions WHERE in_msg={msg_id:?}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT * FROM ({union_sql}) LIMIT 1");
        tracing::debug!("SQL: {sql}");

        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);

        let mut conn = db_connector.get_connection().await?;
        Ok(builder.build_query_as().fetch_optional(&mut *conn).await?)
    }
}
