// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;

use sqlx::prelude::FromRow;
use sqlx::QueryBuilder;

use crate::schema::db::DBConnector;

#[derive(Clone, Debug, FromRow)]
pub struct Attestation {
    pub block_id: String,
    pub parent_block_id: String,
    pub envelope_hash: Vec<u8>,
    pub target_type: i64,
    pub aggregated_signature: Vec<u8>,
    pub signature_occurrences: Vec<u8>,
}

impl Attestation {
    pub async fn by_block_id(
        db_connector: &DBConnector,
        block_id: &str,
    ) -> anyhow::Result<Vec<Attestation>> {
        Self::list_by(db_connector, "block_id", block_id).await
    }

    pub async fn by_source_block_id(
        db_connector: &DBConnector,
        block_id: &str,
    ) -> anyhow::Result<Vec<Attestation>> {
        Self::list_by(db_connector, "source_block_id", block_id).await
    }

    async fn list_by(
        db_connector: &DBConnector,
        field: &str,
        value: &str,
    ) -> anyhow::Result<Vec<Attestation>> {
        let db_names = db_connector.attached_db_names();
        tracing::trace!(db_names = ?db_names, "attached DBs:");

        if db_names.is_empty() {
            return Ok(Vec::new());
        }

        let union_sql = db_names
            .into_iter()
            .map(|name| format!("SELECT * FROM \"{name}\".attestations WHERE {field}={value:?}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let sql = format!(
            "SELECT * FROM ({union_sql}) ORDER BY source_chain_order DESC, target_type ASC"
        );
        tracing::trace!(target: "blockchain_api", "SQL: {sql}");

        let mut conn = db_connector.get_connection().await?;
        let mut builder: QueryBuilder<sqlx::Sqlite> = QueryBuilder::new(sql);
        let rows: Vec<Attestation> = builder.build_query_as().fetch_all(&mut *conn).await?;

        // Same row can exist in `main` and one of attached DBs; keep one per (block_id, target_type).
        let mut dedup = Vec::new();
        let mut seen = HashSet::new();
        for row in rows {
            let key = (row.block_id.clone(), row.target_type);
            if seen.insert(key) {
                dedup.push(row);
            }
        }

        dedup.sort_by_key(|x| x.target_type);
        Ok(dedup)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    use rusqlite::params;
    use rusqlite::Connection;
    use testdir::testdir;

    use super::Attestation;
    use crate::schema::db::DBConnector;
    use crate::web;

    fn create_attestations_table(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS attestations (
                rowid INTEGER PRIMARY KEY,
                block_id TEXT NOT NULL,
                parent_block_id TEXT NOT NULL,
                envelope_hash BLOB NOT NULL,
                target_type INTEGER NOT NULL,
                aggregated_signature BLOB NOT NULL,
                signature_occurrences BLOB NOT NULL,
                source_block_id TEXT NOT NULL,
                source_chain_order TEXT NOT NULL,
                UNIQUE(block_id, target_type)
            );",
        )
        .expect("create attestations");
    }

    fn unique_suffix() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        format!("{}-{nanos}", std::process::id())
    }

    fn insert_attestation(
        db_path: &Path,
        block_id: &str,
        target_type: i64,
        source_block_id: &str,
        source_chain_order: &str,
        agg_sig: &[u8],
    ) {
        let conn = Connection::open(db_path).expect("open db");
        create_attestations_table(&conn);
        let mut occurrences = HashMap::<u16, u16>::new();
        occurrences.insert(1, 2);
        conn.execute(
            "INSERT INTO attestations (
                block_id,parent_block_id,envelope_hash,target_type,aggregated_signature,signature_occurrences,source_block_id,source_chain_order
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                block_id,
                "parent",
                vec![7_u8; 32],
                target_type,
                agg_sig,
                bincode::serialize(&occurrences).expect("serialize occurrences"),
                source_block_id,
                source_chain_order
            ],
        )
        .expect("insert attestation");
    }

    async fn setup_db_connector() -> Arc<DBConnector> {
        let root = testdir!();
        let suffix = unique_suffix();
        let main_db = root.join(format!("bm-archive-{suffix}.db"));
        let archive_db = root.join(format!("bm-archive-{suffix}-1.db"));

        insert_attestation(&main_db, "block-A", 0, "source-A", "200", &[9, 9]);
        insert_attestation(&main_db, "block-A", 1, "source-A", "210", &[8, 8]);

        // Older duplicate of primary attestation from archive.
        insert_attestation(&archive_db, "block-A", 0, "source-A", "100", &[1, 1]);

        let pool = web::open_db(PathBuf::from(&main_db)).await.expect("open main db");
        let db_connector = DBConnector::new(pool, main_db.clone());
        db_connector
            .update_attachments(vec![archive_db.to_string_lossy().into_owned()])
            .await
            .expect("attach archive");
        db_connector
    }

    #[tokio::test]
    async fn by_block_id_deduplicates_per_target_type_and_keeps_newest() {
        let db_connector = setup_db_connector().await;
        let rows =
            Attestation::by_block_id(&db_connector, "block-A").await.expect("query attestations");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].target_type, 0);
        assert_eq!(rows[0].aggregated_signature, vec![9, 9]);
        assert_eq!(rows[1].target_type, 1);
    }

    #[tokio::test]
    async fn by_source_block_id_returns_all_attestations_for_source() {
        let db_connector = setup_db_connector().await;
        let rows = Attestation::by_source_block_id(&db_connector, "source-A")
            .await
            .expect("query attestations by source");

        assert_eq!(rows.len(), 2);
    }
}
