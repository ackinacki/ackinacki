// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::futures_util::TryStreamExt;
use sqlx::prelude::FromRow;
use sqlx::SqlitePool;

use crate::defaults;
use crate::helpers::u64_to_string;
use crate::schema::graphql::blockchain_api::blocks::BlockchainBlocksQueryArgs;
use crate::schema::graphql::blockchain_api::query::PaginateDirection;

#[allow(dead_code)]
#[derive(Clone, Debug, FromRow)]
pub struct Block {
    #[sqlx(skip)]
    pub rowid: i64, // id INTEGER PRIMARY KEY,
    pub id: String,                            // block_id TEXT NOT NULL UNIQUE,
    pub after_merge: Option<i64>,              // after_merge INTEGER,
    pub after_split: Option<i64>,              // after_split INTEGER,
    pub aggregated_signature: Option<Vec<u8>>, // aggregated_signature BLOB,
    pub before_split: Option<i64>,             // before_split INTEGER,
    pub boc: Option<Vec<u8>>,
    pub chain_order: Option<String>,
    pub end_lt: Option<String>, // end_lt TEXT,
    pub file_hash: Option<String>,
    pub flags: Option<i64>,                         // flags INTEGER,
    pub gen_catchain_seqno: Option<i64>,            // gen_catchain_seqno INTEGER,
    pub gen_software_version: Option<i64>,          // gen_software_version INTEGER,
    pub gen_software_capabilities: Option<String>,  // gen_software_capabilities TEXT,
    pub gen_utime: Option<i64>,                     // gen_utime INTEGER,
    pub gen_utime_ms_part: Option<i64>,             // gen_utime_ms_part INTEGER,
    pub gen_validator_list_hash_short: Option<i64>, // gen_validator_list_hash_short INTEGER,
    pub global_id: Option<i64>,                     // global_id INTEGER,
    pub in_msgs: Option<String>,                    // in_msgs TEXT,
    pub key_block: Option<i64>,                     // key_block INTEGER,
    pub min_ref_mc_seqno: Option<i64>,              // min_ref_mc_seqno INTEGER,
    pub out_msgs: Option<String>,                   // out_msgs TEXT,
    pub parent: String,                             // parent TEXT NOT NULL,
    pub prev_alt_ref_seq_no: Option<i64>,
    pub prev_alt_ref_end_lt: Option<String>,
    pub prev_alt_ref_file_hash: Option<String>,
    pub prev_alt_ref_root_hash: Option<String>,
    pub prev_key_block_seqno: Option<i64>, // prev_key_block_seqno INTEGER,
    pub prev_ref_seq_no: Option<i64>,
    pub prev_ref_end_lt: Option<String>,
    pub prev_ref_file_hash: Option<String>,
    pub prev_ref_root_hash: Option<String>,
    pub producer_id: Option<String>,
    pub root_hash: Option<String>,
    pub seq_no: i64,                                  // seq_no INTEGER NOT NULL,
    pub signature_occurrences: Option<Vec<u8>>,       // signature_occurrences BLOB,
    pub shard: Option<String>,                        // shard INTEGER,
    pub share_state_resource_address: Option<String>, // share_state_resource_address TEXT,
    pub start_lt: Option<String>,                     // start_lt TEXT,
    pub status: Option<i64>,                          // status INTEGER NOT NULL,
    pub tr_count: Option<i64>,
    pub version: Option<i64>,      // version INTEGER,
    pub want_merge: Option<i64>,   // want_merge INTEGER,
    pub want_split: Option<i64>,   // want_split INTEGER,
    pub workchain_id: Option<i64>, // workchain_id INTEGER,
    pub data: Option<Vec<u8>>,
    pub thread_id: Option<String>,
}

impl Block {
    pub async fn list(
        pool: &SqlitePool,
        where_clause: String,
        order_by: String,
        limit: Option<i32>,
    ) -> anyhow::Result<Vec<Block>> {
        let limit = match limit {
            Some(v) => v as u16,
            None => defaults::QUERY_BATCH_SIZE,
        };

        let sql = format!("SELECT * FROM blocks {where_clause} {order_by} LIMIT {limit}");
        tracing::debug!("SQL: {sql}");
        let res = sqlx::query_as(&sql).fetch(pool).map_ok(|b| b).try_collect::<Vec<Block>>().await;

        let blocks = if let Err(err) = res {
            tracing::error!("ERROR: {:?}", err);
            anyhow::bail!("ERROR: {err}");
        } else {
            let blocks = res.unwrap();
            tracing::debug!("list(): {:?}", blocks);
            blocks
        };
        Ok(blocks)
    }

    pub async fn latest_block(pool: &SqlitePool) -> anyhow::Result<Option<Block>> {
        let block = sqlx::query_as!(Block, "SELECT * FROM blocks ORDER BY gen_utime DESC LIMIT 1")
            .fetch_optional(pool)
            .await?;

        Ok(block)
    }

    pub async fn blockchain_blocks(
        pool: &SqlitePool,
        args: &BlockchainBlocksQueryArgs,
    ) -> anyhow::Result<Vec<Block>> {
        let direction = args.pagination.get_direction();
        let limit = args.pagination.get_limit();

        let mut where_ops: Vec<String> = vec![];

        if let Some(after) = &args.pagination.after {
            if !after.is_empty() {
                where_ops.push(format!("chain_order > {:?}", after));
            }
        }
        if let Some(before) = &args.pagination.before {
            if !before.is_empty() {
                where_ops.push(format!("chain_order < {:?}", before));
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

        if let Some(min_tr_count) = args.min_tr_count {
            where_ops.push(format!("tr_count >= {min_tr_count}"));
        }

        if let Some(max_tr_count) = args.max_tr_count {
            where_ops.push(format!("tr_count <= {max_tr_count}"));
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

        let sql = format!(
            "SELECT * FROM blocks {where_clause} ORDER BY chain_order {} LIMIT {}",
            order_by, limit,
        );

        tracing::trace!(target: "blockchain_api", "SQL: {sql}");

        let result: Result<Vec<Block>, anyhow::Error> =
            sqlx::query_as(&sql).fetch_all(pool).await.map_err(|e| anyhow::format_err!("{}", e));

        match result {
            Err(e) => {
                anyhow::bail!("ERROR: {e}");
            }
            Ok(list) => {
                tracing::debug!("OK: {} rows", list.len());
                Ok(match direction {
                    PaginateDirection::Forward => list,
                    PaginateDirection::Backward => list.into_iter().rev().collect(),
                })
            }
        }
    }

    // pub async fn by_seq_no(pool: &SqlitePool, seq_no: i64) ->
    // anyhow::Result<Option<Block>> {     let block =
    // sqlx::query_as_unchecked!(         Block,
    //         "SELECT * FROM blocks WHERE seq_no = ?",
    //         seq_no
    //     )
    //     .fetch_optional(pool)
    //     .await?;

    //     Ok(block)
    // }
}
