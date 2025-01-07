// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::futures_util::TryStreamExt;
use sqlx::prelude::FromRow;
use sqlx::SqlitePool;

use crate::defaults;
use crate::schema::graphql::blockchain_api::query::PaginateDirection;
use crate::schema::graphql::blockchain_api::query::PaginationArgs;

#[allow(dead_code)]
#[derive(Clone, FromRow)]
pub struct Account {
    #[sqlx(skip)]
    rowid: i64, // id INTEGER PRIMARY KEY,
    pub id: String,      // account_id TEXT NOT NULL UNIQUE,
    pub acc_type: u8,    // acc_type INTEGER NOT NULL,
    pub balance: String, // balance TEXT NOT NULL,
    pub balance_other: Option<Vec<u8>>,
    pub bits: String,            // bits INTEGER NOT NULL,
    pub boc: Option<Vec<u8>>,    // boc TEXT NOT NULL,
    pub cells: String,           // cells INTEGER NOT NULL,
    pub code: Option<Vec<u8>>,   // code TEXT NOT NULL,
    pub code_hash: String,       // code_hash TEXT NOT NULL,
    pub data: Option<Vec<u8>>,   // data TEXT NOT NULL,
    pub data_hash: String,       // data_hash TEXT NOT NULL,
    pub dapp_id: Option<String>, // dapp_id TEXT,
    pub due_payment: Option<String>,
    pub init_code_hash: String, // init_code_hash TEXT NOT NULL,
    #[sqlx(try_from = "u32")]
    pub last_paid: u64, // last_paid INTEGER NOT NULL,
    pub last_trans_chain_order: String, // last_trans_chain_order TEXT NOT NULL
    pub last_trans_lt: String,  // last_trans_lt INTEGER NOT NULL,
    pub last_trans_hash: String, // last_trans_hash INTEGER NOT NULL,
    pub prev_code_hash: Option<String>,
    pub proof: Option<Vec<u8>>,
    pub public_cells: String, // public_cells INTEGER NOT NULL,
    pub split_depth: Option<i64>,
    pub state_hash: Option<String>,
    pub workchain_id: i32, // workchain_id INTEGER NOT NULL,
}

pub struct BlockchainAccountsQueryArgs {
    pub code_hash: Option<String>,
    pub pagination: PaginationArgs,
}

impl Account {
    pub async fn list(
        pool: &SqlitePool,
        where_clause: String,
        order_by: String,
        limit: Option<i32>,
    ) -> anyhow::Result<Vec<Account>> {
        let limit = match limit {
            Some(v) => v as u16,
            None => defaults::QUERY_BATCH_SIZE,
        };

        let sql = format!("SELECT * FROM accounts {} {} LIMIT {}", where_clause, order_by, limit);
        tracing::debug!("SQL: {sql}");
        let accounts =
            sqlx::query_as(&sql).fetch(pool).map_ok(|b| b).try_collect::<Vec<Account>>().await?;

        Ok(accounts)
    }

    pub async fn by_address(
        pool: &SqlitePool,
        address: Option<String>,
    ) -> anyhow::Result<Option<Account>> {
        if address.is_none() {
            return Ok(None);
        }

        let sql = format!("SELECT * FROM accounts WHERE id={:?}", address.unwrap());
        tracing::debug!("SQL: {sql}");
        let account = sqlx::query_as(&sql).fetch_optional(pool).await?;

        Ok(account)
    }

    pub(crate) async fn blockchain_accounts(
        pool: &SqlitePool,
        args: &BlockchainAccountsQueryArgs,
    ) -> anyhow::Result<Vec<Account>> {
        let mut with_selects = Vec::new();
        let mut where_ops = Vec::new();
        let mut from_clause = "accounts".to_string();

        let mut bind_after = None;
        if let Some(after) = &args.pagination.after {
            if !after.is_empty() {
                with_selects.push(
                    "WITH after_accounts AS (SELECT rowid AS after_rowid FROM accounts WHERE id = ?)".to_string(),
                );
                where_ops.push("rowid > after_rowid".to_string());
                from_clause.push_str(", after_accounts");
                bind_after = Some(after.as_str());
            }
        }

        let mut bind_before = None;
        if let Some(before) = &args.pagination.before {
            if !before.is_empty() {
                with_selects.push(
                    "WITH before_accounts AS (SELECT rowid AS before_rowid FROM accounts WHERE id = ?)".to_string(),
                );
                from_clause.push_str(", before_accounts");
                where_ops.push("rowid < before_rowid".to_string());
                bind_before = Some(before.as_str());
            }
        }

        let mut bind_code_hash = None;
        if let Some(code_hash) = &args.code_hash {
            if !code_hash.is_empty() {
                where_ops.push("code_hash = ?".to_string());
                bind_code_hash = Some(code_hash.as_str());
            }
        }

        let order_by = match args.pagination.get_direction() {
            PaginateDirection::Forward => "ASC",
            PaginateDirection::Backward => "DESC",
        };

        let where_clause = if !where_ops.is_empty() {
            format!("WHERE {}", where_ops.join(" AND "))
        } else {
            "".to_string()
        };

        let sql = format!(
            "{} SELECT * FROM {from_clause} {where_clause} ORDER BY rowid {order_by} LIMIT {}",
            with_selects.join(", "),
            args.pagination.get_limit(),
        );

        tracing::trace!(target: "blockchain_api", "SQL: {sql}");

        let mut query = sqlx::query_as(&sql);
        if let Some(after) = bind_after {
            query = query.bind(after);
        }
        if let Some(before) = bind_before {
            query = query.bind(before);
        }
        if let Some(bind_code_hash) = bind_code_hash {
            query = query.bind(bind_code_hash);
        }
        let result: Result<Vec<Account>, anyhow::Error> =
            query.fetch_all(pool).await.map_err(|e| anyhow::format_err!("{}", e));

        match result {
            Err(e) => {
                anyhow::bail!("ERROR: {e}");
            }
            Ok(list) => {
                tracing::debug!("OK: {} rows", list.len());
                Ok(match args.pagination.get_direction() {
                    PaginateDirection::Forward => list,
                    PaginateDirection::Backward => list.into_iter().rev().collect(),
                })
            }
        }
    }
}
