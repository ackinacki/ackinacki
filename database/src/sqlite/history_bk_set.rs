// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! BK set updates and attestations in the history proofs store (thread 0):
//! a BK set update belongs to its block, the attestations of key and rotation
//! blocks are kept, which copy wins, and how an attestation is checked against
//! the row of the block it signs. Live writes, `import` and the verifier all use these rules.

use std::collections::BTreeSet;

use rusqlite::params;
use rusqlite::OptionalExtension;

use super::history_proofs::decode_hex32;
use super::history_proofs::HistoryRow;
use super::history_proofs::ATTESTATION_WAIT_BLOCKS;
use super::history_proofs::THREAD_ZERO_HEX;
use super::history_ranges;
use super::history_ranges::RangeTable;
use super::ArchAttestation;
use super::ArchBkSetUpdate;

/// Key blocks of the bridge bundles: thread-0 blocks at heights divisible by
/// this. The store keeps their attestations.
pub const KEY_BLOCK_STRIDE: u64 = 1024;

/// Checks that `update` is the BK set update of `row`, or that a row without
/// one is not shaped like a rotation block (leaves off a layer boundary).
pub fn check_bk_set_update_of(
    row: &HistoryRow,
    update: Option<&ArchBkSetUpdate>,
) -> Result<(), String> {
    let block_id = hex::encode(row.block_id);
    let Some(u) = update else {
        if row.block_merkle_leaves.is_some()
            && !row.height.is_multiple_of(proof_data::layers::WINDOW)
        {
            return Err(format!(
                "block {block_id} at height {} has block_merkle_leaves off a layer boundary but no BK set update",
                row.height
            ));
        }
        return Ok(());
    };
    if u.block_id != block_id {
        return Err(format!("BK set update of block {}, not of block {block_id}", u.block_id));
    }
    let height = u64::from_be_bytes(u.height);
    if height != row.height {
        return Err(format!(
            "BK set update at height {height}, block {block_id} is at height {}",
            row.height
        ));
    }
    if u.thread_id != THREAD_ZERO_HEX {
        return Err(format!("BK set update of block {block_id} is of thread {}", u.thread_id));
    }
    if row.block_merkle_leaves.is_none() {
        return Err(format!(
            "block {block_id} at height {} has a BK set update but no block_merkle_leaves",
            row.height
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BkSetUpdateStatus {
    /// Neither the height nor the block id has a BK set update.
    Absent,
    /// The same BK set update is stored.
    Present,
    /// Another BK set update holds the height or the block id.
    Conflict,
}

/// What happened to the BK set update of a written block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BkSetUpdateWrite {
    Inserted,
    Present,
}

const BK_SET_UPDATE_COLUMNS: &str =
    "block_height, block_id, thread_id, height, chain_order, bk_set_update";

/// `(block_height, update)` of a row selected with `BK_SET_UPDATE_COLUMNS`.
fn read_bk_set_update(r: &rusqlite::Row<'_>) -> rusqlite::Result<(u64, ArchBkSetUpdate)> {
    Ok((
        r.get::<_, i64>(0)? as u64,
        ArchBkSetUpdate {
            block_id: r.get(1)?,
            thread_id: r.get(2)?,
            height: r.get(3)?,
            chain_order: r.get(4)?,
            bk_set_update: r.get(5)?,
        },
    ))
}

/// Compares `u` with the stored BK set updates; writes nothing.
pub fn bk_set_update_status(
    conn: &rusqlite::Connection,
    height: u64,
    u: &ArchBkSetUpdate,
) -> rusqlite::Result<BkSetUpdateStatus> {
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT {BK_SET_UPDATE_COLUMNS} FROM bk_set_updates WHERE block_height = ?1 OR block_id = ?2"
    ))?;
    let stored = stmt
        .query_map(params![height as i64, u.block_id], read_bk_set_update)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(match stored.as_slice() {
        [] => BkSetUpdateStatus::Absent,
        [(stored_height, stored)] if *stored_height == height && stored == u => {
            BkSetUpdateStatus::Present
        }
        _ => BkSetUpdateStatus::Conflict,
    })
}

pub fn insert_bk_set_update(
    conn: &rusqlite::Connection,
    height: u64,
    u: &ArchBkSetUpdate,
) -> rusqlite::Result<()> {
    conn.prepare_cached(
        "INSERT INTO bk_set_updates (block_height, block_id, thread_id, height, chain_order, bk_set_update)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?
    .execute(params![height as i64, u.block_id, u.thread_id, u.height, u.chain_order, u.bk_set_update])?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MismatchKind {
    /// The attestation's `envelope_hash` is not the one of the row it signs
    /// (or of the stored copy).
    EnvelopeHash,
    /// The attestation's `parent_block_id` is not the block id of the row
    /// below (or of the stored copy).
    Parent,
    /// `block_id` is not 32 bytes of lowercase hex.
    BadBlockId,
    /// An attestation from an export file of a block this store does not have,
    /// while another block is stored at the height it signs: the chains differ.
    OtherBlock,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttestationWrite {
    Inserted,
    /// A copy from a later carrier block replaced the stored one.
    Replaced,
    /// The stored copy is the same or later; it stays.
    Kept,
    /// Not an attestation of a stored key or rotation block.
    NotKept,
    /// Not written.
    Mismatch(MismatchKind, String),
}

/// Writes an attestation if the block it signs is stored and is a key block
/// or a rotation block. The bridge reads attestations by `block_id` only
/// (`Block.attestations`); the other attestations a rotation block carries
/// stay in the archives.
///
/// It must match the signed row (envelope hash, parent) and a stored copy.
/// Of two copies, the one with the greater `source_chain_order` is kept.
pub fn write_attestation(
    conn: &rusqlite::Connection,
    a: &ArchAttestation,
) -> rusqlite::Result<AttestationWrite> {
    // Lowercase only: gql and the checks compare block ids as `hex()` text.
    let lowercase = !a.block_id.bytes().any(|b| b.is_ascii_uppercase());
    let Some(block_id) = decode_hex32(&a.block_id).filter(|_| lowercase) else {
        return Ok(AttestationWrite::Mismatch(
            MismatchKind::BadBlockId,
            format!("attestation block_id {:?} is not 32 bytes of lowercase hex", a.block_id),
        ));
    };
    let Some((height, envelope_hash)) = block_by_id(conn, &block_id)? else {
        return Ok(AttestationWrite::NotKept);
    };
    if let Some((kind, reason)) =
        mismatch_with_row(conn, &a.envelope_hash, &a.parent_block_id, height, &envelope_hash)?
    {
        return Ok(AttestationWrite::Mismatch(
            kind,
            format!("attestation {} {}: {reason}", a.block_id, a.target_type),
        ));
    }
    if !height.is_multiple_of(KEY_BLOCK_STRIDE) && !has_bk_set_update_at(conn, height)? {
        return Ok(AttestationWrite::NotKept);
    }

    let stored = conn
        .prepare_cached(
            "SELECT envelope_hash, parent_block_id, source_chain_order
             FROM attestations WHERE block_id = ?1 AND target_type = ?2",
        )?
        .query_row(params![a.block_id, a.target_type], |r| {
            Ok((r.get::<_, [u8; 32]>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
        })
        .optional()?;
    let Some((stored_envelope, stored_parent, stored_order)) = stored else {
        conn.prepare_cached(
            "INSERT INTO attestations (block_id, parent_block_id, envelope_hash, target_type,
                 aggregated_signature, signature_occurrences, source_block_id, source_chain_order,
                 target_height)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )?
        .execute(params![
            a.block_id,
            a.parent_block_id,
            a.envelope_hash,
            a.target_type,
            a.aggregated_signature,
            a.signature_occurrences,
            a.source_block_id,
            a.source_chain_order,
            height as i64,
        ])?;
        return Ok(AttestationWrite::Inserted);
    };
    if stored_envelope != a.envelope_hash {
        return Ok(AttestationWrite::Mismatch(
            MismatchKind::EnvelopeHash,
            format!(
                "attestation {} {}: envelope_hash {}, the stored copy has {}",
                a.block_id,
                a.target_type,
                hex::encode(a.envelope_hash),
                hex::encode(stored_envelope)
            ),
        ));
    }
    // Both copies passed the row checks; without the row below, their parents
    // were not checked and can still differ.
    if stored_parent != a.parent_block_id {
        return Ok(AttestationWrite::Mismatch(
            MismatchKind::Parent,
            format!(
                "attestation {} {}: parent {}, the stored copy has {stored_parent}",
                a.block_id, a.target_type, a.parent_block_id
            ),
        ));
    }
    if a.source_chain_order <= stored_order {
        return Ok(AttestationWrite::Kept);
    }
    conn.prepare_cached(
        "UPDATE attestations SET aggregated_signature = ?3, signature_occurrences = ?4,
             source_block_id = ?5, source_chain_order = ?6
         WHERE block_id = ?1 AND target_type = ?2",
    )?
    .execute(params![
        a.block_id,
        a.target_type,
        a.aggregated_signature,
        a.signature_occurrences,
        a.source_block_id,
        a.source_chain_order,
    ])?;
    Ok(AttestationWrite::Replaced)
}

/// `write_attestation` for an attestation from an export file, with the height
/// of the block it signs on the source. An attestation of a block this store
/// does not have is `NotKept` only while that height has no row; a row of
/// another block there means the chains differ (`MismatchKind::OtherBlock`).
pub fn write_exported_attestation(
    conn: &rusqlite::Connection,
    a: &ArchAttestation,
    target_height: u64,
) -> rusqlite::Result<AttestationWrite> {
    let write = write_attestation(conn, a)?;
    let (AttestationWrite::NotKept, Some(block_id)) = (&write, decode_hex32(&a.block_id)) else {
        return Ok(write);
    };
    let stored: Option<[u8; 32]> = conn
        .prepare_cached("SELECT block_id FROM blocks WHERE height = ?1")?
        .query_row(params![target_height as i64], |r| r.get(0))
        .optional()?;
    Ok(match stored {
        Some(stored) if stored != block_id => AttestationWrite::Mismatch(
            MismatchKind::OtherBlock,
            format!(
                "attestation {} {}: signs height {target_height}, where this store has block {}",
                a.block_id,
                a.target_type,
                hex::encode(stored)
            ),
        ),
        _ => write,
    })
}

/// Run in the transaction that inserts a new `blocks` row: the attestations
/// of the block above whose parent is not this row are deleted (stored while
/// this row was missing, their parent could not be checked). Returns
/// `"<block_id> <target_type>: <reason>"` per deleted attestation.
pub fn detach_attestations_above(
    conn: &rusqlite::Connection,
    row: &HistoryRow,
) -> rusqlite::Result<Vec<String>> {
    let block_id = hex::encode(row.block_id);
    let above = conn
        .prepare_cached(
            "SELECT block_id, target_type, parent_block_id FROM attestations
             WHERE target_height = ?1 AND parent_block_id != ?2",
        )?
        .query_map(params![row.height as i64 + 1, block_id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, u8>(1)?, r.get::<_, String>(2)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut detached = vec![];
    for (above_id, target_type, parent) in above {
        delete_attestation(conn, &above_id, target_type)?;
        detached.push(format!(
            "{above_id} {target_type}: parent {parent}, the row at height {} is block {block_id}",
            row.height
        ));
    }
    Ok(detached)
}

/// BK set updates of `start ..= end`, by height.
pub fn bk_set_updates_in_range(
    conn: &rusqlite::Connection,
    start: u64,
    end: u64,
) -> rusqlite::Result<Vec<(u64, ArchBkSetUpdate)>> {
    conn.prepare_cached(&format!(
        "SELECT {BK_SET_UPDATE_COLUMNS} FROM bk_set_updates
         WHERE block_height BETWEEN ?1 AND ?2 ORDER BY block_height"
    ))?
    .query_map(params![start as i64, end as i64], read_bk_set_update)?
    .collect()
}

/// Attestations exported with `start ..= end`: of the blocks of the range and
/// of the `ATTESTATION_WAIT_BLOCKS` blocks below it (their attestations come
/// in the blocks of the range: a gap after a restart holds the carriers of the
/// key block stored just before it). The target applies its rules to each, so
/// a copy it has is kept.
pub fn attestations_to_export(
    conn: &rusqlite::Connection,
    start: u64,
    end: u64,
) -> rusqlite::Result<Vec<(ArchAttestation, u64)>> {
    conn.prepare_cached(
        "SELECT block_id, parent_block_id, envelope_hash, target_type, aggregated_signature,
                signature_occurrences, source_block_id, source_chain_order, target_height
         FROM attestations WHERE target_height BETWEEN ?1 AND ?2
         ORDER BY block_id, target_type",
    )?
    .query_map(params![start.saturating_sub(ATTESTATION_WAIT_BLOCKS) as i64, end as i64], |r| {
        Ok((
            ArchAttestation {
                block_id: r.get(0)?,
                parent_block_id: r.get(1)?,
                envelope_hash: r.get(2)?,
                target_type: r.get(3)?,
                aggregated_signature: r.get(4)?,
                signature_occurrences: r.get(5)?,
                source_block_id: r.get(6)?,
                source_chain_order: r.get(7)?,
            },
            r.get::<_, i64>(8)? as u64,
        ))
    })?
    .collect()
}

/// Inserts an attestation as it is (an export file); one already stored
/// under the same `(block_id, target_type)` is kept.
pub fn copy_attestation(
    conn: &rusqlite::Connection,
    a: &ArchAttestation,
    target_height: u64,
) -> rusqlite::Result<()> {
    conn.prepare_cached(
        "INSERT INTO attestations (block_id, parent_block_id, envelope_hash, target_type,
             aggregated_signature, signature_occurrences, source_block_id, source_chain_order,
             target_height)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT (block_id, target_type) DO NOTHING",
    )?
    .execute(params![
        a.block_id,
        a.parent_block_id,
        a.envelope_hash,
        a.target_type,
        a.aggregated_signature,
        a.signature_occurrences,
        a.source_block_id,
        a.source_chain_order,
        target_height as i64,
    ])?;
    Ok(())
}

/// The highest height whose attestations should have arrived: the last
/// `ATTESTATION_WAIT_BLOCKS` heights of the store may still get them. `None`
/// for an empty or younger store.
fn attestation_limit(conn: &rusqlite::Connection) -> rusqlite::Result<Option<u64>> {
    Ok(history_ranges::first_last(conn)?
        .and_then(|(_, last)| last.checked_sub(ATTESTATION_WAIT_BLOCKS)))
}

/// Stored key and rotation blocks of `[from, to]` without a PRIMARY
/// attestation, ascending. Heights within the last `ATTESTATION_WAIT_BLOCKS`
/// of the store are not listed. Height 0 is not a key block: nothing attests
/// it.
pub fn missing_attestations(
    conn: &rusqlite::Connection,
    from: u64,
    to: u64,
) -> rusqlite::Result<Vec<u64>> {
    let Some(limit) = attestation_limit(conn)? else {
        return Ok(vec![]);
    };
    let (from, to) = (from.max(1), to.min(limit));
    if from > to {
        return Ok(vec![]);
    }
    let mut heights = BTreeSet::new();
    for (start, end) in history_ranges::ranges(conn, RangeTable::Coverage)? {
        let (low, high) = (start.max(from), end.min(to));
        let mut key = low.div_ceil(KEY_BLOCK_STRIDE) * KEY_BLOCK_STRIDE;
        while key <= high {
            heights.insert(key);
            key += KEY_BLOCK_STRIDE;
        }
    }
    let range = params![from as i64, to as i64];
    let mut rotations = conn.prepare_cached(
        "SELECT block_height FROM bk_set_updates WHERE block_height BETWEEN ?1 AND ?2",
    )?;
    for height in rotations.query_map(range, |r| r.get::<_, i64>(0))? {
        heights.insert(height? as u64);
    }
    let mut attested = conn.prepare_cached(
        "SELECT target_height FROM attestations
         WHERE target_type = 0 AND target_height BETWEEN ?1 AND ?2",
    )?;
    for height in attested.query_map(range, |r| r.get::<_, i64>(0))? {
        heights.remove(&(height? as u64));
    }
    Ok(heights.into_iter().collect())
}

/// The number of `missing_attestations` over the whole store, without
/// listing the key heights.
pub fn missing_attestations_count(conn: &rusqlite::Connection) -> rusqlite::Result<u64> {
    let Some(limit) = attestation_limit(conn)? else {
        return Ok(0);
    };
    let mut expected = 0;
    for (start, end) in history_ranges::ranges(conn, RangeTable::Coverage)? {
        let (low, high) = (start.max(1), end.min(limit));
        if low <= high {
            expected += high / KEY_BLOCK_STRIDE - (low - 1) / KEY_BLOCK_STRIDE;
        }
    }
    let stride = KEY_BLOCK_STRIDE as i64;
    let rotations: i64 = conn.query_row(
        "SELECT COUNT(*) FROM bk_set_updates
         WHERE block_height BETWEEN 1 AND ?1 AND block_height % ?2 != 0",
        params![limit as i64, stride],
        |r| r.get(0),
    )?;
    let attested: i64 = conn.query_row(
        "SELECT COUNT(*) FROM attestations a
         WHERE a.target_type = 0 AND a.target_height BETWEEN 1 AND ?1
           AND (a.target_height % ?2 = 0
                OR EXISTS (SELECT 1 FROM bk_set_updates u WHERE u.block_height = a.target_height))",
        params![limit as i64, stride],
        |r| r.get(0),
    )?;
    Ok((expected + rotations as u64).saturating_sub(attested as u64))
}

/// Rows that do not match the block rows; only a manual edit of the file
/// makes them. `(BK set updates, attestations)`:
/// - a BK set update without a block row of the same height and block id, or
///   whose row has no `block_merkle_leaves`;
/// - an attestation whose row is missing or has another block id or envelope
///   hash, or whose row below has another block id than its
///   `parent_block_id`.
pub fn inconsistent_bk_set_rows(conn: &rusqlite::Connection) -> rusqlite::Result<(u64, u64)> {
    let updates: i64 = conn.query_row(
        "SELECT COUNT(*) FROM bk_set_updates u LEFT JOIN blocks b ON b.height = u.block_height
         WHERE b.height IS NULL OR lower(hex(b.block_id)) != u.block_id
            OR b.block_merkle_leaves IS NULL",
        [],
        |r| r.get(0),
    )?;
    let attestations: i64 = conn.query_row(
        "SELECT COUNT(*) FROM attestations a
         LEFT JOIN blocks b ON b.height = a.target_height
         LEFT JOIN blocks p ON p.height = a.target_height - 1
         WHERE b.height IS NULL OR lower(hex(b.block_id)) != a.block_id
            OR b.envelope_hash != a.envelope_hash
            OR (p.height IS NOT NULL AND lower(hex(p.block_id)) != a.parent_block_id)",
        [],
        |r| r.get(0),
    )?;
    Ok((updates as u64, attestations as u64))
}

/// `(kind, reason)` when an attestation with `envelope_hash` and `parent`
/// does not sign the row at `height` with `row_envelope_hash`.
fn mismatch_with_row(
    conn: &rusqlite::Connection,
    envelope_hash: &[u8; 32],
    parent: &str,
    height: u64,
    row_envelope_hash: &[u8; 32],
) -> rusqlite::Result<Option<(MismatchKind, String)>> {
    if envelope_hash != row_envelope_hash {
        return Ok(Some((
            MismatchKind::EnvelopeHash,
            format!(
                "envelope_hash {}, the row at height {height} has {}",
                hex::encode(envelope_hash),
                hex::encode(row_envelope_hash)
            ),
        )));
    }
    let Some(below) = height.checked_sub(1) else {
        return Ok(None);
    };
    let below_id = conn
        .prepare_cached("SELECT block_id FROM blocks WHERE height = ?1")?
        .query_row(params![below as i64], |r| r.get::<_, [u8; 32]>(0))
        .optional()?;
    match below_id {
        Some(below_id) if hex::encode(below_id) != parent => Ok(Some((
            MismatchKind::Parent,
            format!(
                "parent {parent}, the row at height {below} is block {}",
                hex::encode(below_id)
            ),
        ))),
        _ => Ok(None),
    }
}

/// `(height, envelope_hash)` of the stored row with `block_id`.
fn block_by_id(
    conn: &rusqlite::Connection,
    block_id: &[u8; 32],
) -> rusqlite::Result<Option<(u64, [u8; 32])>> {
    conn.prepare_cached("SELECT height, envelope_hash FROM blocks WHERE block_id = ?1")?
        .query_row(params![block_id], |r| Ok((r.get::<_, i64>(0)? as u64, r.get(1)?)))
        .optional()
}

fn has_bk_set_update_at(conn: &rusqlite::Connection, height: u64) -> rusqlite::Result<bool> {
    conn.prepare_cached("SELECT 1 FROM bk_set_updates WHERE block_height = ?1")?
        .query_row(params![height as i64], |_| Ok(()))
        .optional()
        .map(|found| found.is_some())
}

fn delete_attestation(
    conn: &rusqlite::Connection,
    block_id: &str,
    target_type: u8,
) -> rusqlite::Result<()> {
    conn.prepare_cached("DELETE FROM attestations WHERE block_id = ?1 AND target_type = ?2")?
        .execute(params![block_id, target_type])?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use rusqlite::params;
    use rusqlite::OptionalExtension;
    use testdir::testdir;

    use super::*;
    use crate::sqlite::history_proofs::history_row_from_arch_block;
    use crate::sqlite::history_proofs::tests::arch_block;
    use crate::sqlite::history_proofs::tests::event;
    use crate::sqlite::history_proofs::tests::id;
    use crate::sqlite::history_proofs::tests::row;
    use crate::sqlite::history_proofs::AttestationWrites;
    use crate::sqlite::history_proofs::BlockWrite;
    use crate::sqlite::history_proofs::HistoryProofsStore;
    use crate::sqlite::history_proofs::ImportItem;
    use crate::sqlite::history_proofs::ImportMode;
    use crate::sqlite::history_proofs::Written;

    const PRIMARY: u8 = 0;
    const FALLBACK: u8 = 1;

    /// A thread-0 block that changes the BK set: it keeps its leaves.
    pub(crate) fn rotation(height: u64) -> HistoryRow {
        let mut r = row(height);
        r.block_merkle_leaves = Some(vec![0x77; 512]);
        r
    }

    pub(crate) fn update(height: u64) -> ArchBkSetUpdate {
        ArchBkSetUpdate {
            block_id: hex::encode(id(height)),
            thread_id: THREAD_ZERO_HEX.to_string(),
            height: height.to_be_bytes(),
            chain_order: format!("{height:08}"),
            bk_set_update: vec![0xbb; 40],
        }
    }

    /// An attestation of the row `row(target)`, carried in block `id(carrier)`.
    pub(crate) fn attestation(
        target: u64,
        target_type: u8,
        carrier: u64,
        chain_order: &str,
    ) -> ArchAttestation {
        ArchAttestation {
            block_id: hex::encode(id(target)),
            parent_block_id: hex::encode(id(target - 1)),
            envelope_hash: [0xe0; 32],
            target_type,
            aggregated_signature: vec![0xa0; 8],
            signature_occurrences: vec![0x0c; 4],
            source_block_id: hex::encode(id(carrier)),
            source_chain_order: chain_order.to_string(),
        }
    }

    /// The stored copy of `(block_id, target_type)` and its `target_height`.
    pub(crate) fn stored(
        store: &HistoryProofsStore,
        block_id: &str,
        target_type: u8,
    ) -> Option<(ArchAttestation, Option<i64>)> {
        store
            .connection()
            .query_row(
                "SELECT block_id, parent_block_id, envelope_hash, target_type, aggregated_signature,
                        signature_occurrences, source_block_id, source_chain_order, target_height
                 FROM attestations WHERE block_id = ?1 AND target_type = ?2",
                params![block_id, target_type],
                |r| {
                    Ok((
                        ArchAttestation {
                            block_id: r.get(0)?,
                            parent_block_id: r.get(1)?,
                            envelope_hash: r.get(2)?,
                            target_type: r.get(3)?,
                            aggregated_signature: r.get(4)?,
                            signature_occurrences: r.get(5)?,
                            source_block_id: r.get(6)?,
                            source_chain_order: r.get(7)?,
                        },
                        r.get(8)?,
                    ))
                },
            )
            .optional()
            .unwrap()
    }

    fn count(store: &HistoryProofsStore, table: &str) -> i64 {
        store
            .connection()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }

    fn written(bk_set_update: Option<BkSetUpdateWrite>, inserted: u64) -> Written {
        Written {
            bk_set_update,
            attestations: AttestationWrites { inserted, ..Default::default() },
            events_inserted: crate::sqlite::history_proofs::EventCounts::default(),
            skipped_events: vec![],
        }
    }

    /// Rows `row(h)` for `heights`, in import batches.
    pub(crate) fn fill(store: &mut HistoryProofsStore, heights: impl IntoIterator<Item = u64>) {
        let items: Vec<ImportItem> = heights.into_iter().map(|h| (row(h), vec![]).into()).collect();
        for chunk in items.chunks(5_000) {
            store.import_batch(chunk, ImportMode::All).unwrap();
        }
    }

    /// Rows 1..=3200 with rotation block `rotation_height` and a PRIMARY
    /// attestation of key block 1024 only.
    fn store_with_rotation(dir: &std::path::Path, rotation_height: u64) -> HistoryProofsStore {
        let mut store = HistoryProofsStore::open(dir).unwrap();
        fill(&mut store, (1..=3200).filter(|h| *h != rotation_height));
        store
            .insert_block_with(&rotation(rotation_height), &[], Some(&update(rotation_height)), &[])
            .unwrap();
        let attested = attestation(1024, PRIMARY, 1025, "00001025");
        assert_eq!(
            write_attestation(store.connection(), &attested).unwrap(),
            AttestationWrite::Inserted
        );
        store
    }

    #[test]
    fn missing_attestations_lists_key_and_rotation_blocks() -> anyhow::Result<()> {
        let store = store_with_rotation(&testdir!(), 2500);
        let conn = store.connection();
        // Last height 3200: heights above 2944 may still get their attestations.
        assert_eq!(missing_attestations(conn, 0, u64::MAX)?, vec![2048, 2500]);
        assert_eq!(missing_attestations(conn, 2049, 3200)?, vec![2500]);
        assert_eq!(missing_attestations(conn, 0, 2047)?, Vec::<u64>::new());
        Ok(())
    }

    /// The window ends 256 heights below the last stored height, inclusive;
    /// the list and the count cover every coverage range and skip the gaps.
    #[test]
    fn missing_attestations_window_and_ranges() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        let rotations = [3100, 3144, 3145];
        fill(&mut store, (1000..=1100).chain(3000..=3400).filter(|h| !rotations.contains(h)));
        for h in rotations {
            store.insert_block_with(&rotation(h), &[], Some(&update(h)), &[])?;
        }
        let conn = store.connection();
        // Last height 3400: the window ends at 3144. Key block 2048 is in the gap.
        assert_eq!(missing_attestations(conn, 0, u64::MAX)?, vec![1024, 3072, 3100, 3144]);
        assert_eq!(missing_attestations_count(conn)?, 4);
        assert_eq!(missing_attestations(conn, 3144, 3144)?, vec![3144]);
        assert_eq!(missing_attestations(conn, 3145, u64::MAX)?, Vec::<u64>::new());
        assert_eq!(missing_attestations(conn, 1025, 3071)?, Vec::<u64>::new());
        Ok(())
    }

    #[test]
    fn fallback_only_is_missing() -> anyhow::Result<()> {
        let store = store_with_rotation(&testdir!(), 2500);
        let fallback = attestation(2048, FALLBACK, 2049, "00002049");
        assert_eq!(write_attestation(store.connection(), &fallback)?, AttestationWrite::Inserted);
        assert_eq!(missing_attestations(store.connection(), 0, u64::MAX)?, vec![2048, 2500]);
        let primary = attestation(2500, PRIMARY, 2501, "00002501");
        assert_eq!(write_attestation(store.connection(), &primary)?, AttestationWrite::Inserted);
        assert_eq!(missing_attestations(store.connection(), 0, u64::MAX)?, vec![2048]);
        Ok(())
    }

    #[test]
    fn rotation_key_block_counted_once() -> anyhow::Result<()> {
        let store = store_with_rotation(&testdir!(), 2048);
        assert_eq!(missing_attestations(store.connection(), 0, u64::MAX)?, vec![2048]);
        assert_eq!(missing_attestations_count(store.connection())?, 1);
        Ok(())
    }

    #[test]
    fn young_store_has_no_missing_attestations() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut store = HistoryProofsStore::open(&dir)?;
        assert_eq!(missing_attestations(store.connection(), 0, u64::MAX)?, Vec::<u64>::new());
        assert_eq!(missing_attestations_count(store.connection())?, 0);
        fill(&mut store, 1..=200);
        assert_eq!(missing_attestations(store.connection(), 0, u64::MAX)?, Vec::<u64>::new());
        assert_eq!(missing_attestations_count(store.connection())?, 0);
        Ok(())
    }

    #[test]
    fn count_matches_list() -> anyhow::Result<()> {
        let store = store_with_rotation(&testdir!(), 2500);
        let conn = store.connection();
        assert_eq!(
            missing_attestations_count(conn)?,
            missing_attestations(conn, 0, u64::MAX)?.len() as u64
        );
        Ok(())
    }

    #[test]
    fn inconsistent_rows_are_counted() -> anyhow::Result<()> {
        let store = store_with_rotation(&testdir!(), 2500);
        let conn = store.connection();
        assert_eq!(inconsistent_bk_set_rows(conn)?, (0, 0));
        conn.execute(
            "UPDATE bk_set_updates SET block_id = ?1 WHERE block_height = 2500",
            [hex::encode(id(9999))],
        )?;
        assert_eq!(inconsistent_bk_set_rows(conn)?, (1, 0));
        conn.execute(
            "UPDATE attestations SET parent_block_id = ?1 WHERE target_height = 1024",
            [hex::encode(id(9999))],
        )?;
        assert_eq!(inconsistent_bk_set_rows(conn)?, (1, 1));
        Ok(())
    }

    #[test]
    fn new_store_has_bk_set_tables() -> anyhow::Result<()> {
        let store = HistoryProofsStore::open(&testdir!())?;
        let conn = store.connection();
        let tables: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name IN ('bk_set_updates', 'attestations')",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(tables, 2);
        let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        assert_eq!(version, crate::sqlite::history_proofs::SCHEMA_VERSION);
        Ok(())
    }

    #[test]
    fn bk_set_update_written_with_its_block() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        let u = update(2000);
        assert_eq!(
            store.insert_block_with(&rotation(2000), &[], Some(&u), &[])?,
            BlockWrite::Inserted(written(Some(BkSetUpdateWrite::Inserted), 0))
        );
        assert_eq!(
            store.insert_block_with(&rotation(2000), &[], Some(&u), &[])?,
            BlockWrite::Duplicate(written(Some(BkSetUpdateWrite::Present), 0))
        );
        let mut other_blob = u.clone();
        other_blob.bk_set_update = vec![0xcc; 40];
        assert_eq!(
            store.insert_block_with(&rotation(2000), &[], Some(&other_blob), &[])?,
            BlockWrite::BkSetUpdateConflict { height: 2000 }
        );
        let conn = store.connection();
        assert_eq!(bk_set_update_status(conn, 2000, &u)?, BkSetUpdateStatus::Present);
        assert_eq!(bk_set_update_status(conn, 2000, &other_blob)?, BkSetUpdateStatus::Conflict);
        let mut moved = u.clone();
        moved.height = 2001u64.to_be_bytes();
        assert_eq!(bk_set_update_status(conn, 2001, &moved)?, BkSetUpdateStatus::Conflict);
        assert_eq!(bk_set_update_status(conn, 3000, &update(3000))?, BkSetUpdateStatus::Absent);
        assert_eq!(count(&store, "bk_set_updates"), 1);
        assert_eq!(store.row(2000)?, Some(rotation(2000)));
        Ok(())
    }

    #[test]
    fn bk_set_update_of_another_block_rejected() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        let mut of_another_block = update(2000);
        of_another_block.block_id = hex::encode(id(2001));
        let mut at_another_height = update(2000);
        at_another_height.height = 2001u64.to_be_bytes();
        let mut of_another_thread = update(2000);
        of_another_thread.thread_id = format!("{}01", &THREAD_ZERO_HEX[..66]);
        for u in [of_another_block, at_another_height, of_another_thread] {
            let result = store.insert_block_with(&rotation(2000), &[], Some(&u), &[])?;
            assert!(matches!(result, BlockWrite::BkSetUpdateRejected(_)), "{u:?}: {result:?}");
        }
        let without_leaves = store.insert_block_with(&row(2000), &[], Some(&update(2000)), &[])?;
        assert!(matches!(without_leaves, BlockWrite::BkSetUpdateRejected(_)), "{without_leaves:?}");
        assert_eq!(store.row(2000)?, None);
        assert_eq!(count(&store, "bk_set_updates"), 0);
        assert_eq!(count(&store, "coverage"), 0);
        Ok(())
    }

    #[test]
    fn rotation_shaped_row_without_bk_set_update_rejected() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        let result = store.insert_block_with(&rotation(2000), &[], None, &[])?;
        assert!(matches!(result, BlockWrite::BkSetUpdateRejected(_)), "{result:?}");
        assert_eq!(store.row(2000)?, None);
        assert_eq!(
            store.insert_block_with(&rotation(2048), &[], None, &[])?,
            BlockWrite::Inserted(written(None, 0))
        );
        Ok(())
    }

    #[test]
    fn conflicting_block_writes_nothing() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.insert_block(&row(10), &[])?;
        let mut changed = rotation(10);
        changed.gen_utime = Some(1);
        assert_eq!(
            store.insert_block_with(
                &changed,
                &[event(10, 1)],
                Some(&update(10)),
                &[attestation(9, PRIMARY, 10, "00000010")],
            )?,
            BlockWrite::Conflict { existing_height: 10, existing_block_id: id(10) }
        );
        assert_eq!(count(&store, "events"), 0);
        assert_eq!(count(&store, "bk_set_updates"), 0);
        assert_eq!(count(&store, "attestations"), 0);
        assert_eq!(store.row(10)?, Some(row(10)));
        Ok(())
    }

    #[test]
    fn leaves_kept_by_bk_set_update() {
        let mut block = arch_block(300, vec![0]);
        let plain = history_row_from_arch_block(&block).unwrap().unwrap();
        assert_eq!(plain.block_merkle_leaves, None);
        block.bk_set_update = Some(update(300));
        let kept = history_row_from_arch_block(&block).unwrap().unwrap();
        assert_eq!(kept.block_merkle_leaves, Some(vec![0x77; 512]));
    }

    #[test]
    fn atomic_on_sqlite_error() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.connection().execute_batch(
            "CREATE TRIGGER t BEFORE INSERT ON attestations BEGIN SELECT RAISE(ABORT, 'boom'); END",
        )?;
        store.insert_block(&row(2047), &[])?;
        store.insert_block(&row(2048), &[])?;
        let carried = attestation(2048, PRIMARY, 3000, "00003000");
        assert!(store
            .insert_block_with(&rotation(3000), &[], Some(&update(3000)), &[carried])
            .is_err());
        assert_eq!(store.row(3000)?, None);
        assert_eq!(count(&store, "bk_set_updates"), 0);
        assert_eq!(count(&store, "attestations"), 0);
        assert_eq!(
            history_ranges::ranges(store.connection(), RangeTable::Coverage)?,
            vec![(2047, 2048)]
        );
        Ok(())
    }

    /// gql and the checks compare block ids as lowercase hex: an uppercase
    /// copy would be stored and never found.
    #[test]
    fn uppercase_block_id_refused() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        fill(&mut store, [2047, 2048]);
        let mut a = attestation(2048, PRIMARY, 2049, "00002049");
        a.block_id = a.block_id.to_uppercase();
        assert!(matches!(
            write_attestation(store.connection(), &a)?,
            AttestationWrite::Mismatch(MismatchKind::BadBlockId, _)
        ));
        assert_eq!(count(&store, "attestations"), 0);
        Ok(())
    }

    /// An exported attestation of an unknown block: not kept in a gap, a
    /// mismatch where another block is stored at the height it signs.
    #[test]
    fn exported_attestation_of_another_block() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        fill(&mut store, [2047, 2049]);
        let a = attestation(2048, PRIMARY, 2049, "00002049");
        assert_eq!(
            write_exported_attestation(store.connection(), &a, 2048)?,
            AttestationWrite::NotKept
        );
        let mut other = row(2048);
        other.block_id = id(9999);
        store.insert_block(&other, &[])?;
        assert!(matches!(
            write_exported_attestation(store.connection(), &a, 2048)?,
            AttestationWrite::Mismatch(MismatchKind::OtherBlock, _)
        ));
        let mut own = attestation(2048, PRIMARY, 2049, "00002049");
        own.block_id = hex::encode(id(9999));
        assert_eq!(
            write_exported_attestation(store.connection(), &own, 2048)?,
            AttestationWrite::Inserted
        );
        Ok(())
    }

    #[test]
    fn key_block_attestation_kept() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.insert_block(&row(2047), &[])?;
        store.insert_block(&row(2048), &[])?;
        let a = attestation(2048, PRIMARY, 2049, "00002049");
        assert_eq!(write_attestation(store.connection(), &a)?, AttestationWrite::Inserted);
        assert_eq!(stored(&store, &a.block_id, PRIMARY), Some((a, Some(2048))));
        Ok(())
    }

    #[test]
    fn plain_block_attestation_not_kept() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.insert_block(&row(2000), &[])?;
        store.insert_block(&row(2001), &[])?;
        let a = attestation(2001, PRIMARY, 2002, "00002002");
        assert_eq!(write_attestation(store.connection(), &a)?, AttestationWrite::NotKept);
        assert_eq!(count(&store, "attestations"), 0);
        Ok(())
    }

    #[test]
    fn rotation_block_attestation_kept() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.insert_block(&row(1999), &[])?;
        store.insert_block_with(&rotation(2000), &[], Some(&update(2000)), &[])?;
        let a = attestation(2000, PRIMARY, 2001, "00002001");
        assert_eq!(write_attestation(store.connection(), &a)?, AttestationWrite::Inserted);
        assert_eq!(stored(&store, &a.block_id, PRIMARY), Some((a, Some(2000))));
        Ok(())
    }

    #[test]
    fn rotation_block_does_not_keep_what_it_carries() {
        // A rotation block keeps the attestations of key and rotation blocks
        // only, like any other carrier (v4 §6: the prover reads attestations
        // by block_id).
        let mut store = HistoryProofsStore::open(&testdir!()).unwrap();
        store.insert_block(&row(2998), &[]).unwrap();
        store.insert_block(&row(2999), &[]).unwrap();
        let BlockWrite::Inserted(written) = store
            .insert_block_with(
                &rotation(3000),
                &[],
                Some(&update(3000)),
                &[
                    attestation(2999, PRIMARY, 3000, "00003000"),
                    attestation(2048, PRIMARY, 3000, "00003000"),
                ],
            )
            .unwrap()
        else {
            panic!("rotation 3000 must be inserted");
        };
        assert_eq!(written.attestations.not_kept, 2, "{written:?}");
        assert_eq!(count(&store, "attestations"), 0);
    }

    #[test]
    fn attestation_mismatch_not_written() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.insert_block(&row(2047), &[])?;
        store.insert_block(&row(2048), &[])?;
        let conn = store.connection();
        let good = attestation(2048, PRIMARY, 2049, "00000010");

        let mut wrong_envelope = good.clone();
        wrong_envelope.envelope_hash = [0xee; 32];
        let result = write_attestation(conn, &wrong_envelope)?;
        assert!(
            matches!(result, AttestationWrite::Mismatch(MismatchKind::EnvelopeHash, _)),
            "{result:?}"
        );
        let mut wrong_parent = good.clone();
        wrong_parent.parent_block_id = hex::encode(id(9999));
        let result = write_attestation(conn, &wrong_parent)?;
        assert!(
            matches!(result, AttestationWrite::Mismatch(MismatchKind::Parent, _)),
            "{result:?}"
        );
        let mut bad_id = good.clone();
        bad_id.block_id = "not hex".to_string();
        let result = write_attestation(conn, &bad_id)?;
        assert!(
            matches!(result, AttestationWrite::Mismatch(MismatchKind::BadBlockId, _)),
            "{result:?}"
        );
        assert_eq!(count(&store, "attestations"), 0);

        assert_eq!(write_attestation(conn, &good)?, AttestationWrite::Inserted);
        let mut later_wrong_envelope = attestation(2048, PRIMARY, 2050, "00000020");
        later_wrong_envelope.envelope_hash = [0xee; 32];
        let result = write_attestation(conn, &later_wrong_envelope)?;
        assert!(
            matches!(result, AttestationWrite::Mismatch(MismatchKind::EnvelopeHash, _)),
            "{result:?}"
        );
        assert_eq!(stored(&store, &good.block_id, PRIMARY), Some((good, Some(2048))));
        Ok(())
    }

    /// A copy of the attestation of key block 2048 carried in a block with
    /// chain order `order`.
    fn copy(order: &str) -> ArchAttestation {
        let mut a = attestation(2048, PRIMARY, 3000 + order.parse::<u64>().unwrap(), order);
        a.aggregated_signature = order.as_bytes().to_vec();
        a.signature_occurrences = vec![order.as_bytes()[7]; 3];
        a
    }

    #[test]
    fn later_copy_replaces_earlier_does_not() -> anyhow::Result<()> {
        let dir = testdir!();
        let mut finals = vec![];
        for (name, arrivals, expected) in [
            (
                "forward",
                ["00000010", "00000020", "00000015"],
                [AttestationWrite::Inserted, AttestationWrite::Replaced, AttestationWrite::Kept],
            ),
            (
                "reverse",
                ["00000020", "00000015", "00000010"],
                [AttestationWrite::Inserted, AttestationWrite::Kept, AttestationWrite::Kept],
            ),
        ] {
            let dir = dir.join(name);
            std::fs::create_dir_all(&dir)?;
            let mut store = HistoryProofsStore::open(&dir)?;
            store.insert_block(&row(2047), &[])?;
            store.insert_block(&row(2048), &[])?;
            for (order, expected) in arrivals.iter().zip(expected) {
                assert_eq!(
                    write_attestation(store.connection(), &copy(order))?,
                    expected,
                    "{name} {order}"
                );
            }
            finals.push(stored(&store, &copy("00000020").block_id, PRIMARY));
        }
        assert_eq!(finals[0], Some((copy("00000020"), Some(2048))));
        assert_eq!(finals[0], finals[1]);
        Ok(())
    }

    #[test]
    fn same_copy_again_is_kept() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.insert_block(&row(2047), &[])?;
        store.insert_block(&row(2048), &[])?;
        let a = attestation(2048, PRIMARY, 2049, "00002049");
        assert_eq!(write_attestation(store.connection(), &a)?, AttestationWrite::Inserted);
        assert_eq!(write_attestation(store.connection(), &a)?, AttestationWrite::Kept);
        assert_eq!(stored(&store, &a.block_id, PRIMARY), Some((a, Some(2048))));
        Ok(())
    }

    #[test]
    fn primary_and_fallback_are_separate() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.insert_block(&row(2047), &[])?;
        store.insert_block(&row(2048), &[])?;
        let primary = attestation(2048, PRIMARY, 2049, "00002049");
        let fallback = attestation(2048, FALLBACK, 2050, "00002050");
        assert_eq!(write_attestation(store.connection(), &primary)?, AttestationWrite::Inserted);
        assert_eq!(write_attestation(store.connection(), &fallback)?, AttestationWrite::Inserted);
        assert_eq!(count(&store, "attestations"), 2);
        assert_eq!(stored(&store, &primary.block_id, PRIMARY), Some((primary, Some(2048))));
        assert_eq!(stored(&store, &fallback.block_id, FALLBACK), Some((fallback, Some(2048))));
        Ok(())
    }

    #[test]
    fn parent_checked_when_row_below_arrives() -> anyhow::Result<()> {
        let mut store = HistoryProofsStore::open(&testdir!())?;
        store.insert_block(&row(2048), &[])?;
        let a = attestation(2048, PRIMARY, 2049, "00002049");
        assert_eq!(write_attestation(store.connection(), &a)?, AttestationWrite::Inserted);
        assert_eq!(stored(&store, &a.block_id, PRIMARY), Some((a.clone(), Some(2048))));

        let mut below = row(2047);
        below.block_id = id(9999);
        let BlockWrite::Inserted(written) = store.insert_block_with(&below, &[], None, &[])? else {
            panic!("row 2047 must be inserted");
        };
        assert_eq!(written.attestations.detached.len(), 1, "{written:?}");
        assert!(written.attestations.detached[0].starts_with(&a.block_id), "{written:?}");
        assert_eq!(count(&store, "attestations"), 0);
        Ok(())
    }
}
