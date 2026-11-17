// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! The closed list of fields served for objects read from the history proofs
//! store. A field added to a type later is refused for such objects until it
//! is added here: the store does not hold it, and a default value would lie.

use std::fmt::Display;

use async_graphql::ErrorExtensions;
use async_graphql::Lookahead;

pub const BLOCK_FIELDS: &[&str] = &[
    "id",
    "hash",
    "block_id",
    "thread_id",
    "height",
    "seq_no",
    "gen_utime",
    "envelope_hash",
    "tracked_ext_out_messages_root",
    "tracked_ext_out_message_hashes",
    "history_proofs",
    "block_merkle_tree_leaves",
    "key_block",
    "attestations",
    "__typename",
];
/// Fields of `Block.attestations` on a block from the history proofs store.
pub const ATTESTATION_FIELDS: &[&str] = &[
    "block_id",
    "parent_block_id",
    "envelope_hash",
    "target_type",
    "aggregated_signature",
    "signature_occurrences",
    "__typename",
];
pub const MESSAGE_FIELDS: &[&str] = &[
    "id",
    "boc",
    "src",
    "dst",
    "created_at",
    "src_dapp_id",
    "block_id",
    "msg_type",
    "src_transaction",
    "__typename",
];
pub const TRANSACTION_FIELDS: &[&str] = &["id", "block_id", "__typename"];

pub const CODE_FIELD_UNAVAILABLE: &str = "HISTORY_FIELD_UNAVAILABLE";
pub const CODE_DATA_CORRUPT: &str = "HISTORY_DATA_CORRUPT";

/// Checks the fields selected under `sel` (fragments included) against
/// `allowed`, and the sub-selections of the fields named in `nested`.
pub fn check_selection(
    sel: Lookahead<'_>,
    allowed: &[&str],
    nested: &[(&str, &[&str])],
) -> async_graphql::Result<()> {
    let mut refused = Vec::new();
    for field in sel.selection_fields() {
        for child in field.selection_set() {
            let name = child.name();
            if !allowed.contains(&name) {
                refused.push(name.to_string());
                continue;
            }
            if let Some((_, sub_allowed)) = nested.iter().find(|(nested, _)| *nested == name) {
                for grandchild in child.selection_set() {
                    if !sub_allowed.contains(&grandchild.name()) {
                        refused.push(format!("{name}.{}", grandchild.name()));
                    }
                }
            }
        }
    }
    if refused.is_empty() {
        return Ok(());
    }
    refused.sort();
    refused.dedup();
    Err(async_graphql::Error::new(format!(
        "fields not available for data served from the history proofs store: {}",
        refused.join(", ")
    ))
    .extend_with(|_, e| e.set("code", CODE_FIELD_UNAVAILABLE)))
}

/// A stored row that does not pass the format checks.
pub fn data_corrupt(height: u64, e: impl Display) -> async_graphql::Error {
    async_graphql::Error::new(format!(
        "history proofs store row at height {height} is corrupt: {e}"
    ))
    .extend_with(|_, err| err.set("code", CODE_DATA_CORRUPT))
}
