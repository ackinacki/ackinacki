// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::futures_util::TryStreamExt;
use sqlx::prelude::FromRow;
use sqlx::SqlitePool;

use crate::defaults;

#[allow(dead_code)]
#[derive(Clone, FromRow)]
pub struct Account {
    #[sqlx(skip)]
    rowid: i64, // id INTEGER PRIMARY KEY,
    pub id: String,              // account_id TEXT NOT NULL UNIQUE,
    pub acc_type: u8,            // acc_type INTEGER NOT NULL,
    pub balance: String,         // balance TEXT NOT NULL,
    pub bits: String,            // bits INTEGER NOT NULL,
    pub boc: String,             // boc TEXT NOT NULL,
    pub cells: String,           // cells INTEGER NOT NULL,
    pub code: String,            // code TEXT NOT NULL,
    pub code_hash: String,       // code_hash TEXT NOT NULL,
    pub data: String,            // data TEXT NOT NULL,
    pub data_hash: String,       // data_hash TEXT NOT NULL,
    pub dapp_id: Option<String>, // dapp_id TEXT,
    pub init_code_hash: String,  // init_code_hash TEXT NOT NULL,
    #[sqlx(try_from = "u32")]
    pub last_paid: u64, // last_paid INTEGER NOT NULL,
    pub last_trans_chain_order: String, // last_trans_chain_order TEXT NOT NULL
    pub last_trans_lt: String,   // last_trans_lt INTEGER NOT NULL,
    pub last_trans_hash: String, // last_trans_hash INTEGER NOT NULL,
    pub public_cells: String,    // public_cells INTEGER NOT NULL,
    pub workchain_id: i32,       // workchain_id INTEGER NOT NULL,
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
        log::debug!("SQL: {sql}");
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
        log::debug!("SQL: {sql}");
        let account = sqlx::query_as(&sql).fetch_optional(pool).await?;

        Ok(account)
    }
}
