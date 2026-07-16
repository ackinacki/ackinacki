// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use async_graphql::ComplexObject;
use async_graphql::Context;
use async_graphql::Enum;
use async_graphql::SimpleObject;
use chrono::DateTime;
use faster_hex::hex_string;

use crate::helpers::decode_u64_string;
use crate::helpers::format_big_int;
use crate::helpers::ToBool;
use crate::helpers::ToFloat;
use crate::helpers::ToInt;
use crate::schema::db::message::InBlockMessage;
use crate::schema::db::{self};

pub mod filter;
pub mod resolver;

pub use filter::BlockFilter;
pub use resolver::BlockLoader;

use super::message::InMsg;
use super::message::OutMsg;
use crate::schema::db::DBConnector;
use crate::schema::graphql::currency::OtherCurrency;
use crate::schema::graphql::formats::BigIntFormat;
use crate::schema::graphql_ext::blockchain_api::attestations::BlockAttestation;

type Boolean = bool;
type Int = i32;
type Float = f64;

const BLOCK_MERKLE_LEAF_COUNT: usize = 16;
const BLOCK_MERKLE_HASH_SIZE: usize = 32;
const BLOCK_MERKLE_LEAVES_SIZE: usize = BLOCK_MERKLE_LEAF_COUNT * BLOCK_MERKLE_HASH_SIZE;

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
pub enum BlockProcessingStatusEnum {
    Unknown,
    Proposed,
    Finalized,
    Refused,
}

impl From<Option<i64>> for BlockProcessingStatusEnum {
    fn from(val: Option<i64>) -> Self {
        match val.unwrap_or(0) {
            1 => BlockProcessingStatusEnum::Proposed,
            2 => BlockProcessingStatusEnum::Finalized,
            3 => BlockProcessingStatusEnum::Refused,
            _ => BlockProcessingStatusEnum::Unknown,
        }
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
#[derive(Default)]
pub struct ExtBlkRef {
    #[graphql(skip)]
    end_lt: Option<String>,
    file_hash: Option<String>,
    root_hash: Option<String>,
    seq_no: Option<f64>,
}

#[ComplexObject]
impl ExtBlkRef {
    #[graphql(name = "end_lt")]
    async fn end_lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.end_lt.clone(), format)
    }
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
struct BlockMasterShardHashesDescr {
    gen_utime: Option<f64>,
    root_hash: Option<String>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
struct BlockMasterShardHashes {
    workchain_id: Option<Int>,
    shard: Option<String>,
    descr: Option<BlockMasterShardHashesDescr>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
struct BlockMaster {
    shard_hashes: Option<BlockMasterShardHashes>,
}

#[derive(SimpleObject, Clone, Debug)]
pub struct Directives {
    share_state_resource_address: Option<String>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
pub struct BlockValueFlow {
    #[graphql(skip)]
    created: Option<String>,
    created_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    exported: Option<String>,
    exported_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    fees_collected: Option<String>,
    fees_collected_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    fees_imported: Option<String>,
    fees_imported_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    from_prev_blk: Option<String>,
    from_prev_blk_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    imported: Option<String>,
    imported_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    minted: Option<String>,
    minted_other: Option<Vec<OtherCurrency>>,
    #[graphql(skip)]
    to_next_blk: Option<String>,
    to_next_blk_other: Option<Vec<OtherCurrency>>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
pub struct BlockAccountBlocksTransactions {
    #[graphql(skip)]
    lt: Option<String>,
    #[graphql(skip)]
    total_fees: Option<String>,
    total_fees_other: Option<Vec<OtherCurrency>>,
    transaction_id: Option<String>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
pub struct BlockAccountBlocks {
    account_addr: Option<String>,
    new_hash: Option<String>,
    old_hash: Option<String>,
    tr_count: Option<i32>,
    transactions: Option<Vec<BlockAccountBlocksTransactions>>,
}

/// One entry in `Block.history_proofs`. Produced from the block's
/// `CommonSection.history_proofs` map.
#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
pub struct HistoryProofEntry {
    /// 1-based layer number (1..=10), strictly contiguous starting at 1.
    pub layer: Int,
    /// 32-byte layer root hash, lowercase hex, no `0x` prefix, length 64.
    pub root_hash: String,
}

/// One canonically ordered tracked external outbound message-hash group committed by
/// `Block.tracked_ext_out_messages_root`.
#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
pub struct TrackedExtOutMessageHashesEntry {
    /// 128 lowercase hex chars: `dapp_id || account_id`.
    pub routing: String,
    /// Ordered 32-byte external outbound message hashes, lowercase hex.
    pub message_hashes: Vec<String>,
}

#[derive(SimpleObject, Clone, Debug)]
#[graphql(complex, rename_fields = "snake_case")]
pub struct Block {
    pub id: String,
    account_blocks: Option<Vec<BlockAccountBlocks>>,
    after_merge: Option<Boolean>,
    after_split: Option<Boolean>,
    aggregated_signature: Vec<u8>,
    before_split: Option<Boolean>,
    /// Collection-unique field for pagination and sorting. This field is
    /// designed to retain logical order.
    pub chain_order: Option<String>,
    /// Public key of the collator who produced this block.
    created_by: Option<String>,
    /// Base64-encoded payload of the `AckiNackiBlock`. Replaces the legacy
    /// `boc` field that used to store the raw TVM block BOC.
    ///
    /// The bytes returned here come straight out of SQLite without any
    /// server-side decompression. The block manager normally writes a
    /// zstd-compressed payload, but it falls back to the raw bytes when
    /// compression fails so that no block is ever lost, and pre-compression
    /// legacy rows may also exist. **Clients must therefore tolerate both
    /// shapes**:
    ///
    /// 1. attempt `zstd::decode_all` on the bytes that come out of base64;
    /// 2. on success, the result is the bincode-serialized `AckiNackiBlock`;
    /// 3. on failure (invalid zstd frame), use the bytes as-is — they are
    ///    already the raw bincode-serialized `AckiNackiBlock`.
    ///
    /// Reference writer:
    /// `database::sqlite::sqlite_helper::zstd_compress`.
    data: String,
    directives: Directives,
    #[graphql(skip)]
    end_lt: Option<String>,
    envelope_hash: Option<String>,
    /// Root of tracked external outbound messages from the block common section.
    /// This is one of the three public inputs used to recompute the historical
    /// block leaf together with `block_id` and `envelope_hash`.
    tracked_ext_out_messages_root: Option<String>,
    /// Canonically ordered tracked external outbound message hashes committed by
    /// `tracked_ext_out_messages_root`.
    ///
    /// This field contains message hashes only, not full message BOCs. Use the
    /// existing message API to fetch message content by hash if needed.
    ///
    /// Entries are sorted by `routing`. Each `routing` is `dapp_id || account_id`
    /// encoded as 128 lowercase hex chars. NULL on legacy rows or malformed
    /// stored payloads.
    tracked_ext_out_message_hashes: Option<Vec<TrackedExtOutMessageHashesEntry>>,
    /// Shard block file hash.
    file_hash: String,
    flags: Option<Int>,
    gen_catchain_seqno: Option<Float>,
    #[graphql(skip)]
    gen_software_capabilities: Option<String>,
    gen_software_version: Option<Float>,
    /// uint 32 generation time stamp.
    gen_utime: Option<Int>,
    gen_utime_string: Option<String>,
    gen_validator_list_hash_short: Option<Float>,
    /// uint32 global block ID.
    global_id: Option<Int>,
    pub hash: Option<String>,
    height: Option<u64>,
    in_msg_descr: Option<Vec<Option<InMsg>>>,
    /// true if this block is a key block.
    key_block: Option<Boolean>,
    master: Option<BlockMaster>,
    master_ref: Option<ExtBlkRef>,
    /// seq_no of masterchain block which commited the block.
    master_seq_no: Option<u64>,
    /// Returns last known master block at the time of shard generation.
    min_ref_mc_seqno: Option<Float>,
    pub out_msg_descr: Option<Vec<Option<OutMsg>>>,
    #[graphql(skip)]
    pub out_msgs: Option<String>,
    // #[graphql(skip)]
    prev_alt_ref: Option<ExtBlkRef>,
    /// Returns a number of a previous key block.
    prev_key_block_seqno: Option<Float>,
    // #[graphql(skip)]
    prev_ref: Option<ExtBlkRef>,
    prev_vert_alt_ref: Option<ExtBlkRef>,
    /// External block reference for previous block in case of vertical blocks.
    prev_vert_ref: Option<ExtBlkRef>,
    producer_id: Option<String>,
    rand_seed: String,
    seq_no: i64,
    shard: Option<String>,
    //-- signatures(timeout: Int, "**DEPRECATED**" when: BlockFilter): BlockSignatures,
    signature_occurrences: Vec<u8>,
    #[graphql(skip)]
    start_lt: Option<String>,
    //-- state_update: BlockStateUpdate,
    /// Returns block processing status:
    /// - 0 – unknown
    /// - 1 – proposed
    /// - 2 – finalized
    /// - 3 – refused
    status: u8,
    /// Returns block processing status name.
    status_name: BlockProcessingStatusEnum,
    thread_id: Option<String>,
    tr_count: Option<i32>,
    value_flow: Option<BlockValueFlow>,
    /// uin32 block version identifier.
    version: Option<Float>,
    vert_seq_no: Option<Float>,
    want_merge: Option<Boolean>,
    want_split: Option<Boolean>,
    /// int64 workchain identifier.
    workchain_id: Option<i64>,
    /// Acki Nacki block ID = SHA-256 root of the 16-leaf Merkle tree
    /// (`AckiNackiBlock::identifier()` / `merkle_block_id`).
    /// Alias of `id`, exposed as the Acki Nacki block ID.
    /// Hex string, length 64.
    block_id: String,
    /// Ordered Acki Nacki block Merkle leaf hashes.
    ///
    /// This field is not the Merkle root. The Merkle root is exposed
    /// separately as `block_id`.
    ///
    /// The database stores the leaves as `L0 || L1 || ... || L15`, but GQL
    /// returns them as sixteen ordered 32-byte lowercase hex strings:
    /// - index 0: Poseidon(layer history proof preimage)
    /// - index 1: SHA-256(bincode(CommonSection))
    /// - index 2: Poseidon(old BK set commitment)
    /// - index 3: Poseidon(new BK set commitment)
    /// - index 4: TVM block representation hash
    /// - index 5: SHA-256(bincode(durable_state_update))
    /// - index 6: SHA-256(tx_cnt.to_be_bytes())
    /// - index 7: Poseidon Merkle root of `proof_block_refs`
    /// - index 8: `tracked_ext_out_messages_root`
    /// - indexes 9..15: zero padding
    ///
    /// NULL on legacy rows or malformed stored payloads.
    block_merkle_tree_leaves: Option<Vec<String>>,
    /// Ordered block references committed by `block_merkle_tree_leaves[7]`.
    ///
    /// Entry 0 is the mandatory parent block ID. Entries 1.. are
    /// `CommonSection.refs` in canonical block order. Empty on legacy rows or
    /// malformed stored payloads.
    proof_block_refs: Vec<String>,
    /// Layer-hash roots from the block's `CommonSection.history_proofs` map.
    /// Empty array on non-key blocks. On key blocks, layer numbers are
    /// strictly contiguous starting at 1 (the resolver enforces this).
    history_proofs: Vec<HistoryProofEntry>,
}

impl From<db::Block> for Block {
    fn from(block: db::Block) -> Self {
        let data = tvm_types::base64_encode(block.data.as_deref().unwrap_or_default());
        let prev_alt_ref = if block.prev_alt_ref_root_hash.is_some() {
            Some(ExtBlkRef {
                end_lt: block.prev_alt_ref_end_lt.map(|v| decode_u64_string(&v).to_string()),
                file_hash: block.prev_alt_ref_file_hash,
                root_hash: block.prev_alt_ref_root_hash,
                seq_no: block.prev_alt_ref_seq_no.to_float(),
            })
        } else {
            None
        };
        let height: Option<u64> = block.height.and_then(|v| {
            let arr: [u8; 8] = v.try_into().ok()?;
            Some(u64::from_be_bytes(arr))
        });
        Self {
            id: block.id.clone(),
            account_blocks: None,
            after_merge: block.after_merge.to_bool(),
            after_split: block.after_split.to_bool(),
            aggregated_signature: block.aggregated_signature.unwrap_or_default(),
            before_split: block.before_split.to_bool(),
            chain_order: block.chain_order,
            created_by: None,
            data,
            directives: Directives {
                share_state_resource_address: block.share_state_resource_address,
            },
            end_lt: None,
            envelope_hash: block.envelope_hash.as_deref().map(hex_string),
            tracked_ext_out_messages_root: block
                .tracked_ext_out_messages_root
                .as_deref()
                .map(hex_string),
            tracked_ext_out_message_hashes: decode_tracked_ext_out_message_hashes(
                block.tracked_ext_out_message_hashes.as_deref(),
            ),
            file_hash: "".to_string(),
            flags: block.flags.to_int(),
            gen_catchain_seqno: block.gen_catchain_seqno.to_float(),
            gen_software_capabilities: block.gen_software_capabilities,
            gen_software_version: block.gen_software_version.to_float(),
            gen_utime: block.gen_utime.to_int(),
            gen_utime_string: block
                .gen_utime
                .and_then(|ts| DateTime::from_timestamp(ts, 0))
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S.%z").to_string()),
            gen_validator_list_hash_short: block.gen_validator_list_hash_short.to_float(),
            global_id: block.global_id.to_int(),
            hash: Some(block.id.clone()),
            height,
            in_msg_descr: Some(vec![]),
            key_block: block.key_block.to_bool(),
            master: None,
            master_ref: None,
            master_seq_no: None,
            min_ref_mc_seqno: block.min_ref_mc_seqno.to_float(),
            out_msg_descr: None,
            out_msgs: block.out_msgs,
            prev_alt_ref,
            prev_key_block_seqno: block.prev_key_block_seqno.to_float(),
            prev_ref: Some(ExtBlkRef {
                end_lt: block.prev_ref_end_lt.map(|v| decode_u64_string(&v).to_string()),
                file_hash: block.prev_ref_file_hash,
                root_hash: block.prev_ref_root_hash,
                seq_no: block.prev_ref_seq_no.to_float(),
            }),
            prev_vert_ref: None,
            prev_vert_alt_ref: None,
            producer_id: block.producer_id,
            rand_seed: "".to_string(),
            seq_no: block.seq_no,
            shard: block.shard,
            signature_occurrences: block.signature_occurrences.unwrap_or_default(),
            start_lt: Some(block.start_lt.unwrap_or("".to_string())),
            status: block.status.unwrap_or(0) as u8,
            status_name: block.status.into(),
            thread_id: block.thread_id,
            tr_count: block.tr_count.to_int(),
            value_flow: None,
            version: block.version.to_float(),
            vert_seq_no: None,
            want_merge: block.want_merge.to_bool(),
            want_split: block.want_split.to_bool(),
            workchain_id: block.workchain_id,
            block_id: block.id,
            block_merkle_tree_leaves: decode_block_merkle_tree_leaves(
                block.block_merkle_leaves.as_deref(),
            ),
            proof_block_refs: decode_proof_block_refs(block.proof_block_refs.as_deref()),
            history_proofs: decode_history_proofs(block.history_proofs.as_deref()),
        }
    }
}

fn decode_block_merkle_tree_leaves(leaves: Option<&[u8]>) -> Option<Vec<String>> {
    let leaves = leaves.filter(|b| b.len() == BLOCK_MERKLE_LEAVES_SIZE)?;
    Some(leaves.chunks_exact(BLOCK_MERKLE_HASH_SIZE).map(hex_string).collect())
}

fn decode_proof_block_refs(raw: Option<&[u8]>) -> Vec<String> {
    decode_proof_block_refs_inner(raw).unwrap_or_default()
}

fn decode_proof_block_refs_inner(raw: Option<&[u8]>) -> Option<Vec<String>> {
    let mut cursor = raw?;
    let ref_count = read_u32_be(&mut cursor)? as usize;
    if ref_count == 0 {
        return None;
    }
    let mut refs = Vec::with_capacity(ref_count);
    for _ in 0..ref_count {
        refs.push(hex_string(take_exact(&mut cursor, 32)?));
    }
    cursor.is_empty().then_some(refs)
}

fn decode_tracked_ext_out_message_hashes(
    raw: Option<&[u8]>,
) -> Option<Vec<TrackedExtOutMessageHashesEntry>> {
    let raw = raw?;
    let mut cursor = raw;
    let entry_count = read_u32_be(&mut cursor)? as usize;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        let routing = take_exact(&mut cursor, 64)?;
        let message_count = read_u32_be(&mut cursor)? as usize;
        let mut message_hashes = Vec::with_capacity(message_count);
        for _ in 0..message_count {
            message_hashes.push(hex_string(take_exact(&mut cursor, 32)?));
        }
        entries
            .push(TrackedExtOutMessageHashesEntry { routing: hex_string(routing), message_hashes });
    }
    cursor.is_empty().then_some(entries)
}

fn read_u32_be(cursor: &mut &[u8]) -> Option<u32> {
    let bytes = take_exact(cursor, 4)?;
    Some(u32::from_be_bytes(bytes.try_into().ok()?))
}

fn take_exact<'a>(cursor: &mut &'a [u8], len: usize) -> Option<&'a [u8]> {
    if cursor.len() < len {
        return None;
    }
    let (head, tail) = cursor.split_at(len);
    *cursor = tail;
    Some(head)
}

/// Decode the history_proofs blob format (`n: u8 || n * (layer: u8, hash: [u8; 32])`)
/// produced by `node/src/database/serialize_block.rs`.
///
/// Returns an empty vector on NULL, malformed input, or non-contiguous layer
/// numbers (the resolver-side check that complements the circuit-side
/// constraint at `circuit.rs:182–185`).
fn decode_history_proofs(raw: Option<&[u8]>) -> Vec<HistoryProofEntry> {
    let Some(raw) = raw else { return Vec::new() };
    if raw.is_empty() {
        return Vec::new();
    }
    let n = raw[0] as usize;
    if n > 10 || raw.len() != 1 + n * 33 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let off = 1 + i * 33;
        let layer = raw[off];
        // Enforce contiguous 1..=n
        if layer as usize != i + 1 {
            return Vec::new();
        }
        let hash = &raw[off + 1..off + 33];
        out.push(HistoryProofEntry { layer: layer as Int, root_hash: hex_string(hash) });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_block_merkle_tree_leaves() {
        let mut leaves = Vec::with_capacity(BLOCK_MERKLE_LEAVES_SIZE);
        for i in 0..BLOCK_MERKLE_LEAF_COUNT as u8 {
            leaves.extend_from_slice(&[i; BLOCK_MERKLE_HASH_SIZE]);
        }

        let tree = decode_block_merkle_tree_leaves(Some(&leaves)).expect("tree");
        assert_eq!(
            tree,
            vec![
                "0000000000000000000000000000000000000000000000000000000000000000",
                "0101010101010101010101010101010101010101010101010101010101010101",
                "0202020202020202020202020202020202020202020202020202020202020202",
                "0303030303030303030303030303030303030303030303030303030303030303",
                "0404040404040404040404040404040404040404040404040404040404040404",
                "0505050505050505050505050505050505050505050505050505050505050505",
                "0606060606060606060606060606060606060606060606060606060606060606",
                "0707070707070707070707070707070707070707070707070707070707070707",
                "0808080808080808080808080808080808080808080808080808080808080808",
                "0909090909090909090909090909090909090909090909090909090909090909",
                "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a",
                "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b",
                "0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c",
                "0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d",
                "0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e",
                "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f",
            ]
        );
    }

    #[test]
    fn ignores_malformed_block_merkle_parts() {
        assert!(decode_block_merkle_tree_leaves(None).is_none());

        let malformed_leaves = [0xcdu8; 511];
        assert!(decode_block_merkle_tree_leaves(Some(&malformed_leaves)).is_none());
    }

    #[test]
    fn decodes_proof_block_refs() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&3u32.to_be_bytes());
        raw.extend_from_slice(&[0x11; 32]);
        raw.extend_from_slice(&[0x22; 32]);
        raw.extend_from_slice(&[0x33; 32]);

        assert_eq!(
            decode_proof_block_refs(Some(&raw)),
            vec!["11".repeat(32), "22".repeat(32), "33".repeat(32)]
        );
    }

    #[test]
    fn ignores_malformed_proof_block_refs() {
        assert!(decode_proof_block_refs(None).is_empty());
        assert!(decode_proof_block_refs(Some(&[])).is_empty());

        let zero_count = 0u32.to_be_bytes();
        assert!(decode_proof_block_refs(Some(&zero_count)).is_empty());

        let mut trailing = Vec::new();
        trailing.extend_from_slice(&1u32.to_be_bytes());
        trailing.extend_from_slice(&[0x11; 32]);
        trailing.push(0xff);
        assert!(decode_proof_block_refs(Some(&trailing)).is_empty());
    }

    #[test]
    fn decodes_tracked_ext_out_message_hashes() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&2u32.to_be_bytes());

        raw.extend_from_slice(&[0x11; 32]);
        raw.extend_from_slice(&[0x22; 32]);
        raw.extend_from_slice(&2u32.to_be_bytes());
        raw.extend_from_slice(&[0x33; 32]);
        raw.extend_from_slice(&[0x44; 32]);

        raw.extend_from_slice(&[0x55; 32]);
        raw.extend_from_slice(&[0x66; 32]);
        raw.extend_from_slice(&1u32.to_be_bytes());
        raw.extend_from_slice(&[0x77; 32]);

        let entries = decode_tracked_ext_out_message_hashes(Some(&raw)).expect("entries");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].routing, format!("{}{}", "11".repeat(32), "22".repeat(32)));
        assert_eq!(entries[0].message_hashes, vec!["33".repeat(32), "44".repeat(32)]);
        assert_eq!(entries[1].routing, format!("{}{}", "55".repeat(32), "66".repeat(32)));
        assert_eq!(entries[1].message_hashes, vec!["77".repeat(32)]);
    }

    #[test]
    fn decodes_empty_tracked_ext_out_message_hashes() {
        let raw = 0u32.to_be_bytes();
        let entries = decode_tracked_ext_out_message_hashes(Some(&raw)).expect("entries");
        assert!(entries.is_empty());
    }

    #[test]
    fn rejects_malformed_tracked_ext_out_message_hashes() {
        assert!(decode_tracked_ext_out_message_hashes(None).is_none());
        assert!(decode_tracked_ext_out_message_hashes(Some(&[])).is_none());

        let mut truncated = Vec::new();
        truncated.extend_from_slice(&1u32.to_be_bytes());
        truncated.extend_from_slice(&[0x11; 63]);
        assert!(decode_tracked_ext_out_message_hashes(Some(&truncated)).is_none());

        let mut trailing = Vec::new();
        trailing.extend_from_slice(&0u32.to_be_bytes());
        trailing.push(0xff);
        assert!(decode_tracked_ext_out_message_hashes(Some(&trailing)).is_none());
    }
}

#[ComplexObject]
impl Block {
    #[graphql(name = "end_lt")]
    /// Logical creation time automatically set by the block formation end.
    async fn end_lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.end_lt.clone(), format)
    }

    #[graphql(name = "gen_software_capabilities")]
    async fn gen_software_capabilities(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.gen_software_capabilities.clone(), format)
    }

    // #[graphql(name = "prev_ref")]
    // /// External block reference for previous block.
    // async fn prev_ref(&self) -> Option<ExtBlkRef> {
    //     // let prev_block = db::Block::by_seq_no(pool, seq_no)
    //     // ExtBlkRef {

    //     // }
    //     Default::default()
    // }

    // #[graphql(name = "prev_alt_ref")]
    // /// External block reference for previous block in case of shard merge.
    // async fn prev_alt_ref(&self) -> Option<ExtBlkRef> {
    //     // let prev_block = db::Block::by_seq_no(pool, seq_no)
    //     // ExtBlkRef {

    //     // }
    //     Default::default()
    // }

    #[graphql(name = "start_lt")]
    /// Logical creation time automatically set by the block formation start.
    /// Logical time is a component of the Acki Nacki Blockchain that also plays
    /// an important role in message delivery is the logical time, usually
    /// denoted by Lt. It is a non-negative 64-bit integer, assigned to
    /// certain events. For more details, see the Acki Nacki blockchain
    /// specification.
    async fn start_lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.start_lt.clone(), format)
    }

    async fn attestations(
        &self,
        ctx: &Context<'_>,
    ) -> async_graphql::Result<Vec<BlockAttestation>> {
        let db_connector = ctx.data::<Arc<DBConnector>>()?;
        let projection = db::attestation::Attestation::graphql_attestation_projection();
        let rows = db::attestation::Attestation::by_block_id(db_connector, &projection, &self.id)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        rows.into_iter()
            .map(BlockAttestation::try_from_db)
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|e| async_graphql::Error::new(e.to_string()))
    }
}

#[ComplexObject]
impl BlockValueFlow {
    async fn created(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.created.clone(), format)
    }

    async fn exported(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.exported.clone(), format)
    }

    #[graphql(name = "fees_collected")]
    async fn fees_collected(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.fees_collected.clone(), format)
    }

    #[graphql(name = "fees_imported")]
    async fn fees_imported(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.fees_imported.clone(), format)
    }

    #[graphql(name = "from_prev_blk")]
    async fn from_prev_blk(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.from_prev_blk.clone(), format)
    }

    async fn imported(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.imported.clone(), format)
    }

    async fn minted(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.minted.clone(), format)
    }

    #[graphql(name = "to_next_blk")]
    async fn to_next_blk(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.to_next_blk.clone(), format)
    }
}

#[ComplexObject]
impl BlockAccountBlocksTransactions {
    async fn lt(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.lt.clone(), format)
    }

    #[graphql(name = "total_fees")]
    async fn total_fees(&self, format: Option<BigIntFormat>) -> Option<String> {
        format_big_int(self.total_fees.clone(), format)
    }
}

impl Block {
    pub fn set_in_msg_descr(&mut self, in_msgs: Vec<InBlockMessage>) {
        self.in_msg_descr = Some(in_msgs.into_iter().map(|v| Some(v.into())).collect())
    }

    // pub fn set_out_msg_descr(&mut self, out_msgs: Vec<Message>) {
    //     self.out_msg_descr = Some(out_msgs.into_iter().map(|v|
    // Some(v.into())).collect()) }
}
