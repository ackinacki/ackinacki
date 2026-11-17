// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! Reads of the history proofs store (`bm-history-proofs.db`, attached as
//! `history_proofs_db`): thread-0 blocks and bridge events kept after the
//! main DB and the archives dropped them.

use std::collections::HashMap;

use proof_data::FormatError;
use sqlx::prelude::FromRow;
use sqlx::QueryBuilder;

use crate::defaults::HISTORY_PROOFS_DB_ALIAS;
use crate::defaults::THREAD_ZERO;
use crate::schema::db::DBConnector;

const BLOCK_COLUMNS: &str = "height, block_id, envelope_hash, tracked_ext_out_messages_root, \
     tracked_ext_out_message_hashes, history_proofs, block_merkle_leaves, gen_utime";

/// A row of `blocks` in the store.
#[derive(Clone, Debug, FromRow)]
pub struct HistoryBlockRow {
    pub height: i64,
    pub block_id: Vec<u8>,
    pub envelope_hash: Vec<u8>,
    pub tracked_ext_out_messages_root: Vec<u8>,
    pub tracked_ext_out_message_hashes: Option<Vec<u8>>,
    pub history_proofs: Option<Vec<u8>>,
    pub block_merkle_leaves: Option<Vec<u8>>,
    pub gen_utime: Option<i64>,
}

impl HistoryBlockRow {
    pub fn height(&self) -> u64 {
        self.height as u64
    }

    pub async fn by_height(db: &DBConnector, height: u64) -> anyhow::Result<Option<Self>> {
        if !db.history_store_attached() {
            return Ok(None);
        }
        let sql = format!(
            "SELECT {BLOCK_COLUMNS} FROM \"{HISTORY_PROOFS_DB_ALIAS}\".blocks WHERE height = "
        );
        let mut conn = db.get_connection().await?;
        conn.set_sql(&sql);
        let mut query = QueryBuilder::new(sql);
        query.push_bind(height as i64);
        Ok(query.build_query_as().fetch_optional(&mut *conn).await?)
    }

    /// `id_hex` is the block id as `block(hash)` takes it; not hex → `None`.
    pub async fn by_block_id(db: &DBConnector, id_hex: &str) -> anyhow::Result<Option<Self>> {
        if !db.history_store_attached() {
            return Ok(None);
        }
        let Some(id) = decode_hash(id_hex) else {
            return Ok(None);
        };
        let sql = format!(
            "SELECT {BLOCK_COLUMNS} FROM \"{HISTORY_PROOFS_DB_ALIAS}\".blocks WHERE block_id = "
        );
        let mut conn = db.get_connection().await?;
        conn.set_sql(&sql);
        let mut query = QueryBuilder::new(sql);
        query.push_bind(id.to_vec());
        Ok(query.build_query_as().fetch_optional(&mut *conn).await?)
    }

    pub async fn many_by_height(
        db: &DBConnector,
        heights: &[u64],
    ) -> anyhow::Result<HashMap<u64, Self>> {
        if !db.history_store_attached() || heights.is_empty() {
            return Ok(HashMap::new());
        }
        let list = heights.iter().map(|h| (*h as i64).to_string()).collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT {BLOCK_COLUMNS} FROM \"{HISTORY_PROOFS_DB_ALIAS}\".blocks WHERE height IN ({list})"
        );
        let mut conn = db.get_connection().await?;
        conn.set_sql(&sql);
        let rows: Vec<Self> = QueryBuilder::new(sql).build_query_as().fetch_all(&mut *conn).await?;
        Ok(rows.into_iter().map(|row| (row.height(), row)).collect())
    }

    /// Format checks before a row is served: hash lengths, the layer set for
    /// the height, the tracked hashes and the Merkle leaves.
    pub fn validate(&self) -> Result<(), FormatError> {
        for (field, value) in [
            ("block_id", &self.block_id),
            ("envelope_hash", &self.envelope_hash),
            ("tracked_ext_out_messages_root", &self.tracked_ext_out_messages_root),
        ] {
            if value.len() != 32 {
                return Err(FormatError::BadLength { field, expected: 32, got: value.len() });
            }
        }
        proof_data::layers::check_history_proofs_for_height(
            self.height(),
            self.history_proofs.as_deref(),
        )?;
        if let Some(tracked) = &self.tracked_ext_out_message_hashes {
            proof_data::tracked::decode_tracked_hashes(tracked)?;
        }
        if let Some(leaves) = &self.block_merkle_leaves {
            proof_data::leaves::parse_block_merkle_leaves(leaves)?;
        }
        Ok(())
    }
}

/// A row of `events` in the store: a bridge withdrawal or a RootPN voucher.
#[derive(Clone, Debug, FromRow)]
pub struct EventDbRow {
    pub msg_hash: Vec<u8>,
    pub height: i64,
    pub block_id: Vec<u8>,
    pub transaction_id: String,
    pub boc: Vec<u8>,
    pub src: String,
    pub dst: String,
    pub created_at: Option<i64>,
    pub src_dapp_id: Option<String>,
    pub msg_type: Option<i64>,
}

impl EventDbRow {
    pub async fn by_hash(db: &DBConnector, msg_hash_hex: &str) -> anyhow::Result<Option<Self>> {
        if !db.history_store_attached() {
            return Ok(None);
        }
        let Some(hash) = decode_hash(msg_hash_hex) else {
            return Ok(None);
        };
        let sql = format!(
            "SELECT msg_hash, height, block_id, transaction_id, boc, src, dst, created_at, \
             src_dapp_id, msg_type FROM \"{HISTORY_PROOFS_DB_ALIAS}\".events WHERE msg_hash = "
        );
        let mut conn = db.get_connection().await?;
        conn.set_sql(&sql);
        let mut query = QueryBuilder::new(sql);
        query.push_bind(hash.to_vec());
        Ok(query.build_query_as().fetch_optional(&mut *conn).await?)
    }

    /// The vouchers with `skUCommit` `sk`, by height. The caller checks that
    /// the store is attached.
    pub async fn vouchers_by_sk_u_commit(
        db: &DBConnector,
        sk: [u8; 32],
    ) -> anyhow::Result<Vec<Self>> {
        let sql = format!(
            "SELECT msg_hash, height, block_id, transaction_id, boc, src, dst, created_at, \
             src_dapp_id, msg_type FROM \"{HISTORY_PROOFS_DB_ALIAS}\".events \
             WHERE kind = 'voucher' AND sk_u_commit = "
        );
        let mut conn = db.get_connection().await?;
        conn.set_sql(&sql);
        let mut query = QueryBuilder::new(sql);
        query.push_bind(sk.to_vec());
        query.push(" ORDER BY height, msg_hash");
        Ok(query.build_query_as().fetch_all(&mut *conn).await?)
    }
}

fn decode_hash(value: &str) -> Option<[u8; 32]> {
    let mut out = [0u8; 32];
    (value.len() == 64).then_some(())?;
    faster_hex::hex_decode(value.as_bytes(), &mut out).ok()?;
    Some(out)
}

/// The proof fields of a thread-0 block, from the main DB, an archive or the
/// store. Hash fields are `None` on legacy rows written before they existed.
#[derive(Clone, Debug)]
pub struct ProofRow {
    pub height: u64,
    pub block_id: Vec<u8>,
    pub envelope_hash: Option<Vec<u8>>,
    pub tracked_ext_out_messages_root: Option<Vec<u8>>,
    pub history_proofs: Option<Vec<u8>>,
}

#[derive(FromRow)]
struct ArchiveProofRow {
    id: String,
    height: Vec<u8>,
    envelope_hash: Option<Vec<u8>>,
    tracked_ext_out_messages_root: Option<Vec<u8>>,
    history_proofs: Option<Vec<u8>>,
}

/// Proof fields of the thread-0 blocks at `heights`: the main DB and the
/// archives first (only the columns layer trees need, not `data`), then the
/// store for the heights they do not have.
pub async fn proof_rows(
    db: &DBConnector,
    heights: &[u64],
) -> anyhow::Result<HashMap<u64, ProofRow>> {
    let mut found = HashMap::new();
    if heights.is_empty() {
        return Ok(found);
    }
    let list = heights
        .iter()
        .map(|height| crate::helpers::u64_to_hexed_blob_literal(*height))
        .collect::<Vec<_>>()
        .join(",");
    let union_sql = db
        .attached_db_names()
        .into_iter()
        .rev()
        .map(|name| {
            format!(
                "SELECT id, height, envelope_hash, tracked_ext_out_messages_root, history_proofs \
                 FROM \"{name}\".blocks WHERE thread_id = '{THREAD_ZERO}' AND height IN ({list})"
            )
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    {
        let mut conn = db.get_connection().await?;
        conn.set_sql(&union_sql);
        let rows: Vec<ArchiveProofRow> =
            QueryBuilder::new(union_sql).build_query_as().fetch_all(&mut *conn).await?;
        for row in rows {
            let Ok(height) = <[u8; 8]>::try_from(row.height.as_slice()) else {
                continue;
            };
            let height = u64::from_be_bytes(height);
            let mut block_id = vec![0u8; row.id.len() / 2];
            if faster_hex::hex_decode(row.id.as_bytes(), &mut block_id).is_err() {
                block_id.clear();
            }
            // `main` comes first; a later copy in an archive is the same block.
            found.entry(height).or_insert(ProofRow {
                height,
                block_id,
                envelope_hash: row.envelope_hash,
                tracked_ext_out_messages_root: row.tracked_ext_out_messages_root,
                history_proofs: row.history_proofs,
            });
        }
    }
    let rest: Vec<u64> = heights.iter().copied().filter(|h| !found.contains_key(h)).collect();
    for (height, row) in HistoryBlockRow::many_by_height(db, &rest).await? {
        found.insert(
            height,
            ProofRow {
                height,
                block_id: row.block_id,
                envelope_hash: Some(row.envelope_hash),
                tracked_ext_out_messages_root: Some(row.tracked_ext_out_messages_root),
                history_proofs: row.history_proofs,
            },
        );
    }
    Ok(found)
}
