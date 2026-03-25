// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;

use sqlx::prelude::FromRow;
use sqlx::QueryBuilder;

use crate::helpers::sql_quote;
use crate::helpers::u64_to_hexed_blob_literal;
use crate::schema::db::DBConnector;
use crate::schema::graphql::query::PaginateDirection;
use crate::schema::graphql_ext::blockchain_api::bk_set_updates::BlockchainBkSetUpdatesQueryArgs;

#[derive(Clone, Debug, FromRow)]
pub struct BkSetUpdate {
    pub block_id: String,
    pub bk_set_update: Vec<u8>,
    pub chain_order: String,
    pub height: Option<Vec<u8>>,
    pub thread_id: Option<String>,
}

impl BkSetUpdate {
    pub async fn blockchain_bk_set_updates(
        db_connector: &DBConnector,
        args: &BlockchainBkSetUpdatesQueryArgs,
    ) -> anyhow::Result<Vec<BkSetUpdate>> {
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
        if let Some(thread_id) = &args.thread_id {
            where_ops.push(format!("thread_id = {}", sql_quote(thread_id)));
        }
        if let Some(height_start) = args.height_start {
            where_ops.push(format!("height >= {}", u64_to_hexed_blob_literal(height_start)));
        }
        if let Some(height_end) = args.height_end {
            where_ops.push(format!("height <= {}", u64_to_hexed_blob_literal(height_end)));
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

        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT * FROM \"{name}\".bk_set_updates {where_clause}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let sql =
            format!("SELECT * FROM ({union_sql}) ORDER BY chain_order {order_by} LIMIT {limit}");
        tracing::trace!(target: "blockchain_api", "SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let rows: Vec<BkSetUpdate> = builder.build_query_as().fetch_all(&mut *conn).await?;

        // Same row can exist in `main` and one of attached DBs; keep one per block_id.
        let mut dedup = Vec::new();
        let mut seen = HashSet::new();
        for row in rows {
            if seen.insert(row.block_id.clone()) {
                dedup.push(row);
            }
        }

        Ok(match direction {
            PaginateDirection::Forward => dedup,
            PaginateDirection::Backward => dedup.into_iter().rev().collect(),
        })
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

    use super::BkSetUpdate;
    use crate::schema::db::DBConnector;
    use crate::schema::graphql::query::PaginationArgs;
    use crate::schema::graphql_ext::blockchain_api::bk_set_updates::BlockchainBkSetUpdatesQueryArgs;
    use crate::web;

    fn create_bk_set_updates_table(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS bk_set_updates (
                rowid INTEGER PRIMARY KEY,
                block_id TEXT NOT NULL UNIQUE,
                thread_id TEXT NOT NULL,
                height BLOB NOT NULL,
                chain_order TEXT NOT NULL,
                bk_set_update BLOB NOT NULL
            );",
        )
        .expect("create bk_set_updates");
    }

    fn unique_suffix() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        format!("{}-{nanos}", std::process::id())
    }

    fn insert_bk_set_update(
        db_path: &Path,
        block_id: &str,
        thread_id: &str,
        height: u64,
        chain_order: &str,
        payload: &[u8],
    ) {
        let conn = Connection::open(db_path).expect("open db");
        create_bk_set_updates_table(&conn);
        conn.execute(
            "INSERT INTO bk_set_updates (block_id, thread_id, height, chain_order, bk_set_update)
            VALUES (?1, ?2, ?3, ?4, ?5)",
            params![block_id, thread_id, height.to_be_bytes().to_vec(), chain_order, payload],
        )
        .expect("insert bk_set_update");
    }

    async fn setup_db_connector() -> Arc<DBConnector> {
        let root = testdir!();
        let suffix = unique_suffix();
        let main_db = root.join(format!("bm-archive-{suffix}.db"));
        let archive_db = root.join(format!("bm-archive-{suffix}-1.db"));

        insert_bk_set_update(&main_db, "blk-2", "thread-A", 2, "200", &[2]);
        insert_bk_set_update(&main_db, "blk-3", "thread-B", 3, "300", &[3]);

        // Older duplicate of blk-2.
        insert_bk_set_update(&archive_db, "blk-2", "thread-A", 2, "200", &[2]);
        insert_bk_set_update(&archive_db, "blk-1", "thread-A", 1, "100", &[1]);

        let pool = web::open_db(PathBuf::from(&main_db)).await.expect("open main db");
        let db_connector = DBConnector::new(pool, main_db.clone());
        db_connector
            .update_attachments(vec![archive_db.to_string_lossy().into_owned()])
            .await
            .expect("attach archive");
        db_connector
    }

    #[tokio::test]
    async fn blockchain_bk_set_updates_orders_and_deduplicates() {
        let db_connector = setup_db_connector().await;
        let args = BlockchainBkSetUpdatesQueryArgs {
            pagination: PaginationArgs { first: Some(10), after: None, last: None, before: None },
            thread_id: None,
            height_start: None,
            height_end: None,
        };

        let rows = BkSetUpdate::blockchain_bk_set_updates(&db_connector, &args)
            .await
            .expect("query bk_set_updates");

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].block_id, "blk-1");
        assert_eq!(rows[1].block_id, "blk-2");
        assert_eq!(rows[1].bk_set_update, vec![2]);
        assert_eq!(rows[2].block_id, "blk-3");
    }

    #[tokio::test]
    async fn blockchain_bk_set_updates_applies_after_filter() {
        let db_connector = setup_db_connector().await;
        let args = BlockchainBkSetUpdatesQueryArgs {
            pagination: PaginationArgs {
                first: Some(10),
                after: Some("200".to_string()),
                last: None,
                before: None,
            },
            thread_id: None,
            height_start: None,
            height_end: None,
        };

        let rows = BkSetUpdate::blockchain_bk_set_updates(&db_connector, &args)
            .await
            .expect("query bk_set_updates");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].block_id, "blk-3");
    }

    #[tokio::test]
    async fn blockchain_bk_set_updates_filters_by_thread_and_height_range() {
        let db_connector = setup_db_connector().await;
        let args = BlockchainBkSetUpdatesQueryArgs {
            pagination: PaginationArgs { first: Some(10), after: None, last: None, before: None },
            thread_id: Some("thread-A".to_string()),
            height_start: Some(2),
            height_end: Some(2),
        };

        let rows = BkSetUpdate::blockchain_bk_set_updates(&db_connector, &args)
            .await
            .expect("query bk_set_updates");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].block_id, "blk-2");
    }

    #[tokio::test]
    async fn blockchain_bk_set_updates_filters_by_single_height_bound() {
        let db_connector = setup_db_connector().await;

        let args_start = BlockchainBkSetUpdatesQueryArgs {
            pagination: PaginationArgs { first: Some(10), after: None, last: None, before: None },
            thread_id: Some("thread-A".to_string()),
            height_start: Some(2),
            height_end: None,
        };
        let rows_start = BkSetUpdate::blockchain_bk_set_updates(&db_connector, &args_start)
            .await
            .expect("query bk_set_updates by start bound");
        assert_eq!(rows_start.len(), 1);
        assert_eq!(rows_start[0].block_id, "blk-2");

        let args_end = BlockchainBkSetUpdatesQueryArgs {
            pagination: PaginationArgs { first: Some(10), after: None, last: None, before: None },
            thread_id: Some("thread-A".to_string()),
            height_start: None,
            height_end: Some(1),
        };
        let rows_end = BkSetUpdate::blockchain_bk_set_updates(&db_connector, &args_end)
            .await
            .expect("query bk_set_updates by end bound");
        assert_eq!(rows_end.len(), 1);
        assert_eq!(rows_end[0].block_id, "blk-1");
    }
}
