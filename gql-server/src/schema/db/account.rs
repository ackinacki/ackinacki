// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use anyhow::bail;
use async_graphql::futures_util::TryStreamExt;
use sqlx::prelude::FromRow;
use sqlx::QueryBuilder;
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
use crate::schema::db::projection::SqlProjection;
use crate::schema::db::DBConnector;
use crate::schema::graphql::query::PaginateDirection;
use crate::schema::graphql::query::PaginationArgs;

#[allow(dead_code)]
#[derive(Clone, Default, FromRow)]
#[sqlx(default)]
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

const LEGACY_DAPP_ID: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn account_id_from_address(address: &str) -> &str {
    address.split_once(':').map(|(_, account_id)| account_id).unwrap_or(address)
}

impl Account {
    const DIRECT_COLUMNS: &'static [&'static str] = &[
        "acc_type",
        "balance",
        "balance_other",
        "bits",
        "boc",
        "cells",
        "code",
        "code_hash",
        "data",
        "data_hash",
        "dapp_id",
        "due_payment",
        "id",
        "init_code_hash",
        "last_paid",
        "last_trans_chain_order",
        "last_trans_hash",
        "last_trans_lt",
        "prev_code_hash",
        "proof",
        "public_cells",
        "split_depth",
        "state_hash",
        "workchain_id",
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
                "id" | "address" => projection.add("id"),
                "acc_type_name" => projection.add("acc_type"),
                "last_trans_hash" => projection.add("last_trans_hash"),
                "last_trans_lt" => projection.add("last_trans_lt"),
                "library" | "library_hash" | "tick" | "tock" => {}
                _ if let Some(column) = Self::direct_column(field) => projection.add(column),
                _ => panic!("Unsupported SQL projection field: account.{field}"),
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
        projection.add("rowid");
        projection
    }

    pub fn graphql_account_projection() -> SqlProjection {
        let mut projection = SqlProjection::new();
        for column in Self::DIRECT_COLUMNS {
            projection.add(column);
        }
        projection
    }

    pub async fn list(
        db_connector: &DBConnector,
        projection: &SqlProjection,
        where_clause: String,
        order_by: String,
        limit: Option<i32>,
    ) -> anyhow::Result<Vec<Account>> {
        let limit = match limit {
            Some(v) => v as u16,
            None => defaults::QUERY_BATCH_SIZE,
        };

        let select = projection.select_list();
        let sql = format!("SELECT {select} FROM accounts {where_clause} {order_by} LIMIT {limit}");
        tracing::debug!("SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        conn.set_sql(&sql);
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let accounts = builder
            .build_query_as()
            .fetch(&mut *conn)
            .map_ok(|b| b)
            .try_collect::<Vec<Account>>()
            .await?;

        Ok(accounts)
    }

    pub async fn by_address(
        db_connector: &Arc<DBConnector>,
        client: &Arc<ClientContext>,
        address: Option<String>,
    ) -> anyhow::Result<Option<Account>> {
        let Some(address) = address else {
            return Ok(None);
        };
        let account_id = account_id_from_address(&address).to_string();
        let account =
            Self::by_account_id(db_connector, client, account_id, LEGACY_DAPP_ID.to_string())
                .await?;
        Ok(account.map(|mut account| {
            account.id = address;
            account
        }))
    }

    pub async fn by_account_id(
        _db_connector: &Arc<DBConnector>,
        client: &Arc<ClientContext>,
        account_id: String,
        dapp_id: String,
    ) -> anyhow::Result<Option<Account>> {
        let params = ParamsOfGetAccount { account_id: account_id.clone(), dapp_id };

        match get_account(client.clone(), params).await {
            Ok(got_acc) => {
                let account_id = got_acc.account_id;
                let dapp_id = got_acc.dapp_id;
                let boc_base64 = got_acc.boc;

                let tvm_acc = tvm_block::Account::construct_from_base64(&boc_base64)
                    .map_err(|e| anyhow::anyhow!("Failed to construct account from boc: {e}"))?;

                let boc = base64_decode(boc_base64)
                    .map_err(|e| anyhow::anyhow!("Failed to decode received data: {e}"))?;

                let balance = tvm_acc
                    .balance()
                    .map(|bal| format!("{:x}", bal.grams.as_u128()))
                    .unwrap_or_default();

                let balance_other = tvm_acc
                    .balance()
                    .and_then(|bal| bal.other.serialize().ok())
                    .and_then(|cell| write_boc(&cell).ok());

                let code_hash =
                    tvm_acc.get_code_hash().map(|hash| hash.as_hex_string()).unwrap_or_default();

                let (code, data) = if matches!(tvm_acc.status(), AccountStatus::AccStateActive) {
                    if let Some(state) = tvm_acc.state_init() {
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
                    id: account_id,
                    acc_type: serialize_account_status(&tvm_acc.status()),
                    balance,
                    balance_other,
                    boc: Some(boc),
                    code,
                    code_hash,
                    data,
                    dapp_id: Some(dapp_id),
                    last_paid: tvm_acc.last_paid() as u64,
                    last_trans_lt: tvm_acc
                        .last_tr_time()
                        .map_or("".to_owned(), |v| format!("{v:x}")),
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
                if err.code() == ErrorCode::NotFound as u32 {
                    return Ok(None);
                }
                bail!("failed to get account {account_id}: {err}");
            }
        }
    }

    pub(crate) async fn blockchain_accounts(
        db_connector: &DBConnector,
        projection: &SqlProjection,
        args: &BlockchainAccountsQueryArgs,
    ) -> anyhow::Result<Vec<Account>> {
        let mut with_selects = Vec::new();
        let mut where_ops = Vec::new();
        let mut from_clause = "accounts".to_string();

        let mut bind_after = None;
        if let Some(after) = &args.pagination.after {
            if !after.is_empty() {
                with_selects.push(
                    "after_accounts AS (SELECT rowid AS after_rowid FROM accounts WHERE id = ?)"
                        .to_string(),
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
                    "before_accounts AS (SELECT rowid AS before_rowid FROM accounts WHERE id = ?)"
                        .to_string(),
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

        let select = projection.select_list();
        let with_clause = if with_selects.is_empty() {
            String::new()
        } else {
            format!("WITH {} ", with_selects.join(", "))
        };
        let sql = format!(
            "{with_clause}SELECT {select} FROM {from_clause} {where_clause} ORDER BY rowid {order_by} LIMIT {}",
            args.pagination.get_limit(),
        );

        tracing::trace!(target: "blockchain_api", "SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        conn.set_sql(&sql);
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
            query.fetch_all(&mut *conn).await.map_err(|e| anyhow::format_err!("{e}"));

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

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use rusqlite::params;
    use rusqlite::Connection;
    use testdir::testdir;

    use super::Account;
    use crate::defaults;
    use crate::schema::db::DBConnector;
    use crate::schema::graphql::query::PaginationArgs;
    use crate::web;

    #[test]
    fn account_connection_projection_adds_id_cursor() {
        let projection = Account::connection_projection_for_fields(["id", "balance"]);
        assert_eq!(projection.columns(), &["id", "balance", "rowid"]);
    }

    #[test]
    fn account_projection_empty_adds_minimum_identity_column() {
        let projection = Account::projection_for_fields(std::iter::empty::<&str>());
        assert_eq!(projection.columns(), &["id"]);
    }

    #[test]
    #[should_panic(expected = "Unsupported SQL projection field: account.unknown_field")]
    fn account_projection_unknown_field_panics() {
        Account::projection_for_fields(["unknown_field"]);
    }

    fn create_accounts_table(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS accounts (
                id TEXT NOT NULL UNIQUE,
                code_hash TEXT
            );",
        )
        .expect("create accounts");
    }

    fn unique_suffix() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        format!("{}-{nanos}", std::process::id())
    }

    fn insert_account(db_path: &Path, id: &str) {
        let conn = Connection::open(db_path).expect("open db");
        create_accounts_table(&conn);
        conn.execute(
            "INSERT INTO accounts (id, code_hash) VALUES (?1, ?2)",
            params![id, "code-hash"],
        )
        .expect("insert account");
    }

    async fn setup_db_connector() -> Arc<DBConnector> {
        let root = testdir!();
        let db_path = root.join(format!("accounts-{}.db", unique_suffix()));

        insert_account(&db_path, "acc-1");
        insert_account(&db_path, "acc-2");
        insert_account(&db_path, "acc-3");
        insert_account(&db_path, "acc-4");

        let pool = web::open_db(
            PathBuf::from(&db_path),
            15,
            std::time::Duration::from_secs(defaults::DEFAULT_ACQUIRE_TIMEOUT_SECS),
            crate::schema::db::build_read_pragmas(
                defaults::DEFAULT_SQLITE_MMAP_SIZE,
                defaults::DEFAULT_SQLITE_CACHE_SIZE,
            ),
        )
        .await
        .expect("open accounts db");
        DBConnector::new(pool, db_path, defaults::MAX_POOL_CONNECTIONS)
    }

    #[tokio::test]
    async fn account_projection_blockchain_accounts_accepts_after_and_before_cursors() {
        let db_connector = setup_db_connector().await;
        let projection = Account::connection_projection_for_fields(["id"]);
        let args = super::BlockchainAccountsQueryArgs {
            code_hash: None,
            pagination: PaginationArgs {
                first: Some(10),
                after: Some("acc-1".to_string()),
                last: None,
                before: Some("acc-4".to_string()),
            },
        };

        let rows = Account::blockchain_accounts(&db_connector, &projection, &args)
            .await
            .expect("query accounts with both cursors");

        assert_eq!(rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(), ["acc-2", "acc-3"]);
    }
}
