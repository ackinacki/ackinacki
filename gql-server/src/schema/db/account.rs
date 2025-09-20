// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use anyhow::bail;
use async_graphql::futures_util::TryStreamExt;
use sqlx::prelude::FromRow;
use sqlx::QueryBuilder;
use sqlx::SqlitePool;
use tvm_block::AccountStatus;
use tvm_block::Deserializable;
use tvm_block::Serializable;
use tvm_client::account::get_account;
use tvm_client::account::ParamsOfGetAccount;
use tvm_client::net::ErrorCode;
use tvm_client::ClientContext;
use tvm_types::base64_decode;
use tvm_types::write_boc;

use crate::defaults;
use crate::schema::graphql::query::PaginateDirection;
use crate::schema::graphql::query::PaginationArgs;

#[allow(dead_code)]
#[derive(Clone, FromRow)]
pub struct Account {
    #[sqlx(skip)]
    pub rowid: i64, // id INTEGER PRIMARY KEY,
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

        let sql = format!("SELECT * FROM accounts {where_clause} {order_by} LIMIT {limit}");
        tracing::debug!("SQL: {sql}");

        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let accounts = builder
            .build_query_as()
            .fetch(pool)
            .map_ok(|b| b)
            .try_collect::<Vec<Account>>()
            .await?;

        Ok(accounts)
    }

    pub async fn by_address(
        _pool: &SqlitePool,
        client: &Arc<ClientContext>,
        address: Option<String>,
    ) -> anyhow::Result<Option<Account>> {
        if address.is_none() {
            return Ok(None);
        }

        let Some(address) = address else { bail!("Address required!") };

        let params = ParamsOfGetAccount { address: address.clone() };

        match get_account(client.clone(), params).await {
            Ok(got_acc) => {
                let boc_base64 = got_acc.boc;

                let acc = tvm_block::Account::construct_from_base64(&boc_base64)
                    .map_err(|e| anyhow::anyhow!("Failed to construct account from boc: {e}"))?;

                let boc = base64_decode(boc_base64)
                    .map_err(|e| anyhow::anyhow!("Failed to decode received data: {e}"))?;

                let balance = acc
                    .balance()
                    .map(|bal| format!("{:x}", bal.grams.as_u128()))
                    .unwrap_or_default();

                let balance_other = acc
                    .balance()
                    .and_then(|bal| bal.other.serialize().ok())
                    .and_then(|cell| write_boc(&cell).ok());

                let code_hash =
                    acc.get_code_hash().map(|hash| hash.as_hex_string()).unwrap_or_default();

                let (code, data) = if matches!(acc.status(), AccountStatus::AccStateActive) {
                    if let Some(state) = acc.state_init() {
                        let code = state
                            .code()
                            .filter(|cell| !cell.is_pruned())
                            .and_then(|cell| write_boc(cell).ok());

                        let data = state
                            .data()
                            .filter(|cell| !cell.is_pruned())
                            .and_then(|cell| write_boc(cell).ok());

                        (code, data)
                    } else {
                        (None, None)
                    }
                } else {
                    (None, None)
                };

                let account = Account {
                    id: address,
                    acc_type: serialize_account_status(&acc.status()),
                    balance,
                    balance_other,
                    boc: Some(boc),
                    code,
                    code_hash,
                    data,
                    dapp_id: got_acc.dapp_id,
                    last_paid: acc.last_paid() as u64,
                    last_trans_lt: acc.last_tr_time().map_or("".to_owned(), |v| format!("{v:x}")),
                    // Next properties left not initialized
                    rowid: 0,
                    prev_code_hash: None,
                    proof: None,
                    public_cells: "".to_string(),
                    split_depth: None,
                    state_hash: None,
                    last_trans_hash: "".to_string(),
                    last_trans_chain_order: "".to_string(),
                    workchain_id: 0,
                    bits: "".to_string(),
                    cells: "".to_string(),
                    due_payment: None,
                    init_code_hash: "".to_string(),
                    data_hash: "".to_string(),
                };

                Ok(Some(account))
            }
            Err(err) => {
                if err.code == ErrorCode::NotFound as u32 {
                    return Ok(None);
                }
                bail!("failed to get account {address}: {err}");
            }
        }
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

        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);

        let mut query = builder.build_query_as();
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

// Helpers: copy from acki-nacki/database/src
fn serialize_account_status(status: &AccountStatus) -> u8 {
    match status {
        AccountStatus::AccStateUninit => 0b00,
        AccountStatus::AccStateFrozen => 0b10,
        AccountStatus::AccStateActive => 0b01,
        AccountStatus::AccStateNonexist => 0b11,
    }
}
