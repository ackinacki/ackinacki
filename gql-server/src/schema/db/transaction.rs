// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::futures_util::TryStreamExt;
use sqlx::prelude::FromRow;
use sqlx::QueryBuilder;

use crate::defaults;
use crate::helpers::u64_to_string;
use crate::schema::db::projection::SqlProjection;
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
#[sqlx(default)]
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

impl Default for Transaction {
    fn default() -> Self {
        Self {
            rowid: 0,
            id: String::new(),
            aborted: false,
            account_addr: String::new(),
            action_success: false,
            action_valid: false,
            action_no_funds: false,
            action_status_change: 0,
            action_result_code: 0,
            action_tot_actions: 0,
            action_spec_actions: 0,
            action_skipped_actions: 0,
            action_msgs_created: 0,
            action_list_hash: String::new(),
            action_tot_msg_size_cells: 0.0,
            action_tot_msg_size_bits: 0.0,
            balance_delta: String::new(),
            block_id: String::new(),
            boc: Vec::new(),
            chain_order: String::new(),
            credit: None,
            credit_first: false,
            compute_account_activated: false,
            compute_exit_code: 0,
            compute_gas_fees: String::new(),
            compute_gas_used: 0.0,
            compute_gas_limit: 0.0,
            compute_mode: 0,
            compute_msg_state_used: false,
            compute_success: false,
            compute_type: 0,
            compute_vm_final_state_hash: String::new(),
            compute_vm_init_state_hash: String::new(),
            compute_vm_steps: 0,
            destroyed: false,
            end_status: 0,
            in_msg: String::new(),
            lt: String::new(),
            new_hash: String::new(),
            now: 0,
            old_hash: String::new(),
            orig_status: 0,
            out_msgs: String::new(),
            outmsg_cnt: 0,
            prev_trans_hash: String::new(),
            prev_trans_lt: String::new(),
            proof: None,
            status: 0,
            storage_fees_collected: None,
            storage_status_change: Some(0),
            total_fees: String::new(),
            tr_type: 0,
            workchain_id: 0,
        }
    }
}

impl Transaction {
    const ACTION_COLUMNS: [&'static str; 12] = [
        "action_list_hash",
        "action_msgs_created",
        "action_no_funds",
        "action_result_code",
        "action_skipped_actions",
        "action_spec_actions",
        "action_status_change",
        "action_success",
        "action_tot_actions",
        "action_tot_msg_size_bits",
        "action_tot_msg_size_cells",
        "action_valid",
    ];
    const COMPUTE_COLUMNS: [&'static str; 12] = [
        "compute_account_activated",
        "compute_type",
        "compute_exit_code",
        "compute_gas_fees",
        "compute_gas_limit",
        "compute_gas_used",
        "compute_mode",
        "compute_msg_state_used",
        "compute_success",
        "compute_vm_final_state_hash",
        "compute_vm_init_state_hash",
        "compute_vm_steps",
    ];
    const DIRECT_COLUMNS: [&'static str; 23] = [
        "aborted",
        "account_addr",
        "balance_delta",
        "block_id",
        "boc",
        "chain_order",
        "credit_first",
        "destroyed",
        "id",
        "lt",
        "new_hash",
        "now",
        "old_hash",
        "outmsg_cnt",
        "prev_trans_hash",
        "prev_trans_lt",
        "proof",
        "status",
        "total_fees",
        "workchain_id",
        "in_msg",
        "out_msgs",
        "credit",
    ];

    fn direct_column(field: &str) -> Option<&'static str> {
        Self::DIRECT_COLUMNS.iter().copied().find(|column| *column == field)
    }

    pub fn projection_for_fields<I>(fields: I) -> SqlProjection
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let mut projection = SqlProjection::new();
        for field in fields {
            let field = field.as_ref();
            match field {
                "action" => projection.extend(Self::ACTION_COLUMNS),
                "compute" => projection.extend(Self::COMPUTE_COLUMNS),
                "end_status" | "end_status_name" => projection.add("end_status"),
                "orig_status" | "orig_status_name" => projection.add("orig_status"),
                "status_name" => projection.add("status"),
                "storage" => projection.extend(["storage_fees_collected", "storage_status_change"]),
                "tr_type" | "tr_type_name" => projection.add("tr_type"),
                "in_message" => projection.add("in_msg"),
                "out_messages" => projection.add("out_msgs"),
                "bounce"
                | "code_hash"
                | "ext_in_msg_fee"
                | "installed"
                | "master_seq_no"
                | "now_string"
                | "prepare_transaction"
                | "split_info"
                | "tt" => {}
                _ if let Some(column) = Self::direct_column(field) => projection.add(column),
                _ => panic!("Unsupported SQL projection field: transaction.{field}"),
            }
        }
        projection.ensure_minimum(["id"]);
        projection
    }

    pub fn connection_projection_for_fields<I>(fields: I) -> SqlProjection
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        let mut projection = Self::projection_for_fields(fields);
        projection.add("chain_order");
        projection
    }

    pub fn graphql_transaction_projection() -> SqlProjection {
        let mut projection = SqlProjection::new();
        projection.add("id");
        projection.extend(Self::DIRECT_COLUMNS);
        projection.extend(Self::ACTION_COLUMNS);
        projection.extend(Self::COMPUTE_COLUMNS);
        projection.extend([
            "end_status",
            "orig_status",
            "storage_fees_collected",
            "storage_status_change",
            "tr_type",
        ]);
        projection
    }

    pub async fn list(
        db_connector: &DBConnector,
        projection: &SqlProjection,
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

        let mut projection = projection.clone();
        projection.add("rowid");
        let select = projection.select_list();
        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT {select} FROM \"{name}\".transactions {filter}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT {select} FROM ({union_sql}) {order_by} LIMIT {limit}");
        tracing::debug!("SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        conn.set_sql(&sql);
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
        projection: &SqlProjection,
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
            where_ops.push(format!("code_hash = {code_hash:?}"));
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

        let mut projection = projection.clone();
        projection.add("chain_order");
        let select = projection.select_list();
        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT {select} FROM \"{name}\".transactions {filter}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT {select} FROM ({union_sql}) {order_by} LIMIT {limit}");

        tracing::trace!(target: "blockchain_api", "SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        conn.set_sql(&sql);
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
        projection: &SqlProjection,
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

        let mut projection = projection.clone();
        projection.add("chain_order");
        let select = projection.select_list();
        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT {select} FROM \"{name}\".transactions {filter}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT {select} FROM ({union_sql}) {order_by} LIMIT {limit}");

        tracing::trace!(target: "blockchain_api.account.transactions", "SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        conn.set_sql(&sql);
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

        Ok(match args.pagination.get_direction() {
            PaginateDirection::Forward => list,
            PaginateDirection::Backward => list.into_iter().rev().collect(),
        })
    }

    pub async fn by_in_message(
        db_connector: &DBConnector,
        projection: &SqlProjection,
        msg_id: &str,
        _fields: Option<Vec<String>>,
    ) -> anyhow::Result<Option<Transaction>> {
        let mut result = Self::by_in_messages(db_connector, projection, &[msg_id]).await?;
        let key = msg_id.strip_prefix("message/").unwrap_or(msg_id);
        Ok(result.remove(key))
    }

    /// Batch-fetch transactions by their inbound message IDs.
    /// Returns a map from stripped msg_id (without `message/` prefix) to Transaction.
    pub async fn by_in_messages(
        db_connector: &DBConnector,
        projection: &SqlProjection,
        msg_ids: &[&str],
    ) -> anyhow::Result<std::collections::HashMap<String, Transaction>> {
        use std::collections::HashMap;

        let db_names = db_connector.attached_db_names();
        if db_names.is_empty() || msg_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let stripped: Vec<&str> =
            msg_ids.iter().map(|id| id.strip_prefix("message/").unwrap_or(id)).collect();

        let in_list = stripped.iter().map(|id| format!("{id:?}")).collect::<Vec<_>>().join(",");

        let mut projection = projection.clone();
        projection.add("in_msg");
        let select = projection.select_list();
        let union_sql = db_names
            .into_iter()
            .map(|name| {
                format!("SELECT {select} FROM \"{name}\".transactions WHERE in_msg IN ({in_list})")
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT {select} FROM ({union_sql})");
        tracing::debug!("SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        conn.set_sql(&sql);
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);

        let rows: Vec<Transaction> =
            builder.build_query_as().fetch(&mut *conn).try_collect().await?;

        let mut map = HashMap::with_capacity(rows.len());
        for trx in rows {
            map.entry(trx.in_msg.clone()).or_insert(trx);
        }

        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use migration_tool::DbInfo;
    use migration_tool::DbMaintenance;
    use migration_tool::DbMaintenanceOptions;
    use migration_tool::MigrateTo;
    use rusqlite::params;
    use rusqlite::Connection;
    use testdir::testdir;

    use super::Transaction;
    use crate::defaults;
    use crate::schema::db::DBConnector;
    use crate::web;

    fn insert_transaction(db_path: &std::path::Path, id: &str, in_msg: &str) {
        let conn = Connection::open(db_path).expect("open db");
        conn.execute(
            "INSERT INTO transactions (
                id, block_id, status, compute_success, compute_msg_state_used,
                compute_account_activated, compute_gas_fees, compute_gas_used,
                compute_gas_limit, compute_mode, compute_exit_code, compute_vm_steps,
                compute_vm_init_state_hash, compute_vm_final_state_hash, compute_type,
                action_success, action_valid, action_no_funds, action_status_change,
                action_result_code, action_tot_actions, action_spec_actions,
                action_skipped_actions, action_msgs_created, action_list_hash,
                action_tot_msg_size_cells, action_tot_msg_size_bits,
                credit_first, aborted, destroyed, tr_type,
                lt, prev_trans_hash, prev_trans_lt, now, outmsg_cnt,
                orig_status, end_status, in_msg, out_msgs,
                account_addr, workchain_id, total_fees, balance_delta,
                old_hash, new_hash, chain_order, boc
            ) VALUES (
                ?1, 'block1', 3, 1, 0,
                0, '0', 0.0,
                0.0, 0, 0, 0,
                'hash0', 'hash1', 0,
                1, 1, 0, 0,
                0, 1, 0,
                0, 1, 'list_hash',
                0.0, 0.0,
                1, 0, 0, 0,
                '10', 'prev_hash', '10', 1000, 1,
                0, 1, ?2, '',
                'addr1', 0, '100', '50',
                'old', 'new', 'co1', X'00'
            )",
            params![id, in_msg],
        )
        .expect("insert transaction");
    }

    async fn setup_connector_with_transactions() -> Arc<DBConnector> {
        let root = testdir!();
        let db_dir = root.join(format!("db-{}", std::process::id()));
        std::fs::create_dir_all(&db_dir).expect("create db dir");
        let db_maintenance = DbMaintenance::new(&DbInfo::BM_ARCHIVE, &db_dir);
        db_maintenance
            .migrate(MigrateTo::Latest, DbMaintenanceOptions { silent: true })
            .expect("migrate");
        let main_db = db_maintenance.path;

        insert_transaction(&main_db, "trx-1", "msg-aaa");
        insert_transaction(&main_db, "trx-2", "msg-bbb");
        insert_transaction(&main_db, "trx-3", "msg-ccc");

        let pool = web::open_db(
            PathBuf::from(&main_db),
            15,
            std::time::Duration::from_secs(defaults::DEFAULT_ACQUIRE_TIMEOUT_SECS),
            crate::schema::db::build_read_pragmas(
                defaults::DEFAULT_SQLITE_MMAP_SIZE,
                defaults::DEFAULT_SQLITE_CACHE_SIZE,
            ),
        )
        .await
        .expect("open db");
        DBConnector::new(pool, main_db, defaults::MAX_POOL_CONNECTIONS)
    }

    #[tokio::test]
    async fn by_in_messages_returns_matching_transactions() {
        let connector = setup_connector_with_transactions().await;
        let projection = Transaction::projection_for_fields(["id"]);

        let result = Transaction::by_in_messages(&connector, &projection, &["msg-aaa", "msg-ccc"])
            .await
            .expect("batch");

        assert_eq!(result.len(), 2);
        assert_eq!(result.get("msg-aaa").unwrap().id, "trx-1");
        assert_eq!(result.get("msg-ccc").unwrap().id, "trx-3");
        assert!(!result.contains_key("msg-bbb"));
    }

    #[tokio::test]
    async fn by_in_messages_strips_message_prefix() {
        let connector = setup_connector_with_transactions().await;
        let projection = Transaction::projection_for_fields(["id"]);

        let result = Transaction::by_in_messages(&connector, &projection, &["message/msg-aaa"])
            .await
            .expect("batch with prefix");

        assert_eq!(result.len(), 1);
        assert_eq!(result.get("msg-aaa").unwrap().id, "trx-1");
    }

    #[tokio::test]
    async fn by_in_messages_empty_input() {
        let connector = setup_connector_with_transactions().await;
        let projection = Transaction::projection_for_fields(["id"]);

        let result =
            Transaction::by_in_messages(&connector, &projection, &[]).await.expect("empty");
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn by_in_message_delegates_to_batch() {
        let connector = setup_connector_with_transactions().await;
        let projection = Transaction::projection_for_fields(["id"]);

        let result = Transaction::by_in_message(&connector, &projection, "msg-bbb", None)
            .await
            .expect("single");
        assert_eq!(result.unwrap().id, "trx-2");

        let missing = Transaction::by_in_message(&connector, &projection, "msg-zzz", None)
            .await
            .expect("missing");
        assert!(missing.is_none());
    }

    #[test]
    fn transaction_projection_maps_compute_group() {
        let projection = Transaction::projection_for_fields(["id", "compute"]);
        assert!(projection.columns().contains(&"compute_gas_used"));
        assert!(projection.columns().contains(&"compute_success"));
    }

    #[test]
    fn transaction_projection_adds_nested_message_keys() {
        let projection = Transaction::projection_for_fields(["id", "in_message", "out_messages"]);
        assert_eq!(projection.columns(), &["id", "in_msg", "out_msgs"]);
    }

    #[test]
    fn transaction_connection_projection_adds_chain_order() {
        let projection = Transaction::connection_projection_for_fields(["id"]);
        assert_eq!(projection.columns(), &["id", "chain_order"]);
    }

    #[test]
    fn transaction_projection_empty_uses_id_minimum() {
        let projection = Transaction::projection_for_fields(std::iter::empty::<&str>());
        assert_eq!(projection.columns(), &["id"]);
    }

    #[test]
    #[should_panic(expected = "Unsupported SQL projection field: transaction.unknown_field")]
    fn transaction_projection_unknown_field_panics() {
        Transaction::projection_for_fields(["unknown_field"]);
    }

    #[test]
    fn transaction_projection_full_graphql_projection_includes_order_and_nested_columns() {
        let projection = Transaction::graphql_transaction_projection();
        assert!(projection.columns().contains(&"chain_order"));
        assert!(projection.columns().contains(&"in_msg"));
        assert!(projection.columns().contains(&"out_msgs"));
    }
}
