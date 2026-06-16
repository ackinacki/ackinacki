// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt;

use async_graphql::futures_util::TryStreamExt;
use sqlx::prelude::FromRow;
use sqlx::QueryBuilder;

use crate::defaults;
use crate::helpers::sql_quote;
use crate::helpers::u64_to_string;
use crate::schema::db::projection::SqlProjection;
use crate::schema::db::DBConnector;
use crate::schema::graphql::query::PaginateDirection;
use crate::schema::graphql_ext::blockchain_api::blocks::BlockchainBlocksQueryArgs;

#[allow(dead_code)]
#[derive(Clone, Default, FromRow)]
#[sqlx(default)]
pub struct Block {
    #[sqlx(skip)]
    pub rowid: i64, // id INTEGER PRIMARY KEY,
    pub id: String,                            // block_id TEXT NOT NULL UNIQUE,
    pub after_merge: Option<i64>,              // after_merge INTEGER,
    pub after_split: Option<i64>,              // after_split INTEGER,
    pub aggregated_signature: Option<Vec<u8>>, // aggregated_signature BLOB,
    pub before_split: Option<i64>,             // before_split INTEGER,
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
    pub height: Option<Vec<u8>>,
    pub envelope_hash: Option<Vec<u8>>,
}

struct HexBytesOpt<'a>(Option<&'a [u8]>);

impl fmt::Debug for HexBytesOpt<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => f.write_str("None"),
            Some(bytes) => {
                f.write_str("Some(0x")?;
                for byte in bytes {
                    write!(f, "{:02x}", byte)?;
                }
                f.write_str(")")
            }
        }
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Block")
            .field("id", &self.id)
            .field("seq_no", &self.seq_no)
            .field("thread_id", &self.thread_id)
            .field("height", &HexBytesOpt(self.height.as_deref()))
            .field("envelope_hash", &HexBytesOpt(self.envelope_hash.as_deref()))
            .finish()
    }
}

impl Block {
    const DIRECT_COLUMNS: &'static [&'static str] = &[
        "after_merge",
        "after_split",
        "aggregated_signature",
        "before_split",
        "chain_order",
        "data",
        "end_lt",
        "envelope_hash",
        "file_hash",
        "flags",
        "gen_catchain_seqno",
        "gen_software_capabilities",
        "gen_software_version",
        "gen_utime",
        "gen_utime_ms_part",
        "gen_validator_list_hash_short",
        "global_id",
        "height",
        "in_msgs",
        "key_block",
        "min_ref_mc_seqno",
        "parent",
        "prev_alt_ref_seq_no",
        "prev_alt_ref_end_lt",
        "prev_alt_ref_file_hash",
        "prev_alt_ref_root_hash",
        "prev_key_block_seqno",
        "prev_ref_seq_no",
        "prev_ref_end_lt",
        "prev_ref_file_hash",
        "prev_ref_root_hash",
        "producer_id",
        "root_hash",
        "seq_no",
        "shard",
        "share_state_resource_address",
        "signature_occurrences",
        "start_lt",
        "thread_id",
        "tr_count",
        "version",
        "want_merge",
        "want_split",
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
                "id" | "hash" => projection.add("id"),
                "status" | "status_name" => projection.add("status"),
                "prev_ref" => projection.extend([
                    "prev_ref_seq_no",
                    "prev_ref_end_lt",
                    "prev_ref_file_hash",
                    "prev_ref_root_hash",
                ]),
                "prev_alt_ref" => projection.extend([
                    "prev_alt_ref_seq_no",
                    "prev_alt_ref_end_lt",
                    "prev_alt_ref_file_hash",
                    "prev_alt_ref_root_hash",
                ]),
                "out_msg_descr" | "out_msgs" => projection.add("out_msgs"),
                "in_msg_descr" => projection.add("in_msgs"),
                "gen_utime_string" => projection.add("gen_utime"),
                "directives" => projection.add("share_state_resource_address"),
                "attestations" => projection.add("id"),
                "account_blocks" | "created_by" | "master" | "master_ref" | "master_seq_no"
                | "prev_vert_alt_ref" | "prev_vert_ref" | "rand_seed" | "value_flow"
                | "vert_seq_no" => {}
                _ if let Some(column) = Self::direct_column(field) => projection.add(column),
                _ => panic!("Unsupported SQL projection field: block.{field}"),
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

    pub fn graphql_block_projection() -> SqlProjection {
        let mut projection = SqlProjection::new();
        projection.add("id");
        for column in Self::DIRECT_COLUMNS {
            projection.add(column);
        }
        projection.add("status");
        projection
    }

    pub async fn list(
        db_connector: &DBConnector,
        projection: &SqlProjection,
        where_clause: String,
        order_by: String,
        limit: Option<i32>,
    ) -> anyhow::Result<Vec<Block>> {
        let limit = match limit {
            Some(v) => v as u16,
            None => defaults::QUERY_BATCH_SIZE,
        };

        let db_names = db_connector.attached_db_names();
        tracing::trace!(db_names = ?db_names, "attached DBs:");

        if db_names.is_empty() {
            return Ok(Vec::new());
        }

        let select = projection.select_list();
        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT {select} FROM \"{name}\".blocks {where_clause}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT {select} FROM ({union_sql}) {order_by} LIMIT {limit}");
        tracing::debug!("SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        conn.set_sql(&sql);
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);

        let blocks = builder
            .build_query_as()
            .fetch(&mut *conn)
            .map_ok(|b| b)
            .try_collect::<Vec<Block>>()
            .await?;

        Ok(blocks)
    }

    pub async fn latest_block(
        db_connector: &DBConnector,
        projection: &SqlProjection,
    ) -> anyhow::Result<Option<Block>> {
        let select = projection.select_list();
        let sql = format!("SELECT {select} FROM blocks ORDER BY chain_order DESC LIMIT 1");
        tracing::debug!("SQL: {sql}");
        let mut conn = db_connector.get_connection().await?;
        conn.set_sql(&sql);
        let block = QueryBuilder::new(sql).build_query_as().fetch_optional(&mut *conn).await?;

        Ok(block)
    }

    pub async fn blockchain_blocks(
        db_connector: &DBConnector,
        projection: &SqlProjection,
        args: &BlockchainBlocksQueryArgs,
    ) -> anyhow::Result<Vec<Block>> {
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

        if let Some(thread_id) = &args.thread_id {
            where_ops.push(format!("thread_id = {}", sql_quote(thread_id)));
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

        let select = projection.select_list();
        let filter = where_clause;
        let order_by = format!("ORDER BY chain_order {order_by}");

        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT {select} FROM \"{name}\".blocks {filter}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT {select} FROM ({union_sql}) {order_by} LIMIT {limit}");

        tracing::trace!(target: "blockchain_api", "SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        conn.set_sql(&sql);
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let result: Result<Vec<Block>, anyhow::Error> = builder
            .build_query_as()
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| anyhow::format_err!("{e}"));

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

    // pub async fn by_seq_no(db_connector: &DBConnector, seq_no: i64) ->
    // anyhow::Result<Option<Block>> {     let block =
    // sqlx::query_as_unchecked!(         Block,
    //         "SELECT id FROM blocks WHERE seq_no = ?",
    //         seq_no
    //     )
    //     .fetch_optional(pool)
    //     .await?;

    //     Ok(block)
    // }
}

#[cfg(test)]
mod tests {
    use super::Block;

    #[test]
    fn block_projection_maps_direct_and_derived_fields() {
        let projection = Block::projection_for_fields(["id", "hash", "status_name", "height"]);
        assert_eq!(projection.columns(), &["id", "status", "height"]);
    }

    #[test]
    fn block_projection_maps_grouped_prev_ref() {
        let projection = Block::projection_for_fields(["prev_ref"]);
        assert_eq!(
            projection.columns(),
            &["prev_ref_seq_no", "prev_ref_end_lt", "prev_ref_file_hash", "prev_ref_root_hash"]
        );
    }

    #[test]
    fn block_connection_projection_adds_cursor_columns() {
        let projection = Block::connection_projection_for_fields(["id"]);
        assert_eq!(projection.columns(), &["id", "chain_order"]);
    }

    #[test]
    fn block_projection_maps_directives() {
        let projection = Block::projection_for_fields(["directives"]);
        assert_eq!(projection.columns(), &["share_state_resource_address"]);
    }

    #[test]
    fn block_projection_supports_computed_attestations() {
        let projection = Block::projection_for_fields(["attestations"]);
        assert_eq!(projection.columns(), &["id"]);
    }

    #[test]
    fn block_projection_adds_id_for_attestations_with_other_fields() {
        let projection = Block::projection_for_fields(["chain_order", "attestations"]);
        assert_eq!(projection.columns(), &["chain_order", "id"]);
    }

    #[test]
    fn block_projection_empty_fields_uses_minimum_id() {
        let projection = Block::projection_for_fields(std::iter::empty::<&str>());
        assert_eq!(projection.columns(), &["id"]);
    }

    #[test]
    #[should_panic(expected = "Unsupported SQL projection field: block.unknown_field")]
    fn block_projection_unknown_field_panics_with_context() {
        Block::projection_for_fields(["unknown_field"]);
    }

    #[test]
    fn block_projection_full_graphql_projection_includes_order_only_columns() {
        let projection = Block::graphql_block_projection();

        assert!(projection.columns().contains(&"root_hash"));
        assert!(projection.columns().contains(&"data"));
        assert!(projection.columns().contains(&"in_msgs"));
    }
}
