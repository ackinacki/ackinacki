// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::Arc;

use async_graphql::dataloader::Loader;
use async_graphql::Error;
use futures::TryStreamExt;
use sqlx::QueryBuilder;

use crate::helpers::sql_quote;
use crate::helpers::u64_to_hexed_blob_buf;
use crate::schema::db;
use crate::schema::db::DBConnector;

pub struct BlockLoader {
    pub db_connector: Arc<DBConnector>,
}

impl Loader<(String, u64)> for BlockLoader {
    type Error = Error;
    type Value = super::Block;

    async fn load(
        &self,
        keys: &[(String, u64)],
    ) -> anyhow::Result<HashMap<(String, u64), Self::Value>, Self::Error> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let db_names = self.db_connector.attached_db_names();

        if db_names.is_empty() {
            return Ok(HashMap::new());
        }

        let tuples_sql = keys
            .iter()
            .map(|(thread_id, height)| {
                let blob = u64_to_hexed_blob_buf(*height);
                let height_lit = unsafe { std::str::from_utf8_unchecked(&blob) };
                format!("({}, {})", sql_quote(thread_id), height_lit)
            })
            .collect::<Vec<_>>()
            .join(",");

        let union_sql = db_names
            .into_iter()
            .map(|name| {
                format!(
                    "SELECT * FROM \"{name}\".blocks WHERE (thread_id, height) IN ({tuples_sql})"
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT * FROM ({union_sql})");
        tracing::trace!(target: "data_loader", "SQL: {sql}");
        let mut conn = self.db_connector.get_connection().await?;
        let blocks = QueryBuilder::new(sql)
            .build_query_as()
            .fetch(&mut *conn)
            .try_filter_map(|block: db::Block| async move {
                let thread_id = match block.thread_id.clone() {
                    Some(v) => v,
                    None => return Ok(None),
                };
                let height = match block.height.as_deref().and_then(|v| {
                    let arr: [u8; 8] = v.try_into().ok()?;
                    Some(u64::from_be_bytes(arr))
                }) {
                    Some(v) => v,
                    None => return Ok(None),
                };
                let block: Self::Value = block.into();
                Ok(Some(((thread_id, height), block)))
            })
            .try_collect::<HashMap<(String, u64), Self::Value>>()
            .await?;

        Ok(blocks)
    }
}

impl Loader<String> for BlockLoader {
    type Error = Error;
    type Value = super::Block;

    async fn load(
        &self,
        keys: &[String],
    ) -> anyhow::Result<HashMap<String, Self::Value>, Self::Error> {
        let ids = keys.iter().map(|m| format!("{m:?}")).collect::<Vec<_>>().join(",");

        let db_names = self.db_connector.attached_db_names();

        if db_names.is_empty() {
            return Ok(HashMap::new());
        }

        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT * FROM \"{name}\".blocks WHERE id IN ({ids})"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        let sql = format!("SELECT * FROM ({union_sql})");
        tracing::trace!(target: "data_loader", "SQL: {sql}");
        let mut conn = self.db_connector.get_connection().await?;
        let blocks = QueryBuilder::new(sql)
            .build_query_as()
            .fetch(&mut *conn)
            .map_ok(|block: db::Block| {
                let block: Self::Value = block.into();
                let block_id = block.id.clone();
                (block_id, block)
            })
            .try_collect::<HashMap<String, Self::Value>>()
            .await?;

        Ok(blocks)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use async_graphql::dataloader::Loader;
    use rusqlite::params;
    use rusqlite::Connection;
    use testdir::testdir;

    use super::BlockLoader;
    use crate::schema::db::DBConnector;
    use crate::web;

    // todo_ use fixtures
    fn create_blocks_db(path: &Path, id: &str, thread_id: &str, height: u64) {
        let conn = Connection::open(path).expect("open sqlite db");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS blocks (
                rowid INTEGER PRIMARY KEY,
                id TEXT NOT NULL UNIQUE,
                status INTEGER NOT NULL,
                seq_no INTEGER NOT NULL,
                parent TEXT NOT NULL,
                producer_id TEXT,
                thread_id TEXT,
                gen_utime INTEGER,
                chain_order TEXT,
                boc BLOB,
                aggregated_signature BLOB,
                signature_occurrences BLOB,
                share_state_resource_address TEXT,
                global_id INTEGER,
                version INTEGER,
                after_merge INTEGER,
                before_split INTEGER,
                after_split INTEGER,
                want_split INTEGER,
                want_merge INTEGER,
                key_block INTEGER,
                flags INTEGER,
                workchain_id INTEGER,
                gen_utime_ms_part INTEGER,
                start_lt TEXT,
                end_lt TEXT,
                gen_validator_list_hash_short INTEGER,
                gen_catchain_seqno INTEGER,
                min_ref_mc_seqno INTEGER,
                prev_key_block_seqno INTEGER,
                gen_software_version INTEGER,
                gen_software_capabilities TEXT,
                data BLOB,
                file_hash TEXT,
                root_hash TEXT,
                prev_alt_ref_seq_no INTEGER,
                prev_alt_ref_end_lt TEXT,
                prev_alt_ref_file_hash TEXT,
                prev_alt_ref_root_hash TEXT,
                prev_ref_seq_no INTEGER,
                prev_ref_end_lt TEXT,
                prev_ref_file_hash TEXT,
                prev_ref_root_hash TEXT,
                in_msgs TEXT,
                out_msgs TEXT,
                shard TEXT,
                tr_count INTEGER,
                height BLOB CHECK (length(height) = 8),
                envelope_hash BLOB CHECK (length(envelope_hash) = 32)
            );",
        )
        .expect("create blocks table");

        conn.execute(
            "INSERT OR IGNORE INTO blocks (id, status, seq_no, parent, thread_id, height)
             VALUES (?1, 1, 1, 'p', ?2, ?3)",
            params![id, thread_id, height.to_be_bytes().to_vec()],
        )
        .expect("insert block");
    }

    async fn setup_loader() -> (BlockLoader, (String, u64), (String, u64)) {
        let root = testdir!();

        let main_db = root.join("main.db");
        let archive_db = root.join("archive_one.db");

        let key_main = (
            "00000000000000000000000000000000000000000000000000000000000000000000".to_string(),
            31178796_u64,
        );
        let key_archive = (
            "00000000000000000000000000000000000000000000000000000000000000000000".to_string(),
            31178792_u64,
        );

        create_blocks_db(
            &main_db,
            "9c435c860c8252a1415da449213d00ba23f391e77c7e1f8cc7b54840d1b61b94",
            &key_main.0,
            key_main.1,
        );
        create_blocks_db(
            &archive_db,
            "852e35f89fcd0d3f3f7574b9c4b29221042d21e4a1bb106c9171ff31fd28ff94",
            &key_archive.0,
            key_archive.1,
        );

        let pool = web::open_db(PathBuf::from(&main_db)).await.expect("open main db");
        let db_connector = DBConnector::new(pool, main_db.clone());
        db_connector
            .update_attachments(vec![archive_db.to_string_lossy().into_owned()])
            .await
            .expect("attach archive db");

        (BlockLoader { db_connector: Arc::clone(&db_connector) }, key_main, key_archive)
    }

    #[tokio::test]
    async fn loads_blocks_by_thread_id_and_height_from_all_attached_dbs() {
        let (loader, key_main, key_archive) = setup_loader().await;

        let loaded = loader
            .load(&[key_main.clone(), key_archive.clone(), ("missing".to_string(), 999)])
            .await
            .expect("load blocks");

        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded.get(&key_main).map(|b| b.id.as_str()),
            Some("9c435c860c8252a1415da449213d00ba23f391e77c7e1f8cc7b54840d1b61b94")
        );
        assert_eq!(
            loaded.get(&key_archive).map(|b| b.id.as_str()),
            Some("852e35f89fcd0d3f3f7574b9c4b29221042d21e4a1bb106c9171ff31fd28ff94")
        );
    }
}
