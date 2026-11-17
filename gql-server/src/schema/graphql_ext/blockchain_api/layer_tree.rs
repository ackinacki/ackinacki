// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! `blockchain.layerTree`: the 130 leaves of a history proof layer tree, read
//! from the main DB, the archives and the history proofs store, checked
//! against the root the node wrote before they are returned.

use std::collections::HashMap;

use async_graphql::ErrorExtensions;
use async_graphql::SimpleObject;
use faster_hex::hex_string;
use history_proof::RowView;
use proof_data::layer_tree::LayerTreeSpec;
use proof_data::layers::check_history_proofs_for_height;

use crate::defaults::THREAD_ZERO;
use crate::helpers::pad_thread_id;
use crate::schema::db::history_store::proof_rows;
use crate::schema::db::history_store::ProofRow;
use crate::schema::db::DBConnector;
use crate::schema::graphql_ext::history_allowlist::data_corrupt;

pub const CODE_BAD_ARGUMENT: &str = "LAYER_TREE_BAD_ARGUMENT";
pub const CODE_MISSING_DATA: &str = "LAYER_TREE_MISSING_DATA";
pub const CODE_ROOT_MISMATCH: &str = "LAYER_TREE_ROOT_MISMATCH";

/// A history proof layer tree of thread 0.
#[derive(SimpleObject, Clone, Debug)]
#[graphql(rename_fields = "snake_case")]
pub struct LayerTree {
    /// A 68-character thread identifier.
    pub thread_id: String,
    /// The boundary height the tree's root is recorded at.
    pub height: u64,
    /// Layer number, 1..=10.
    pub layer: u8,
    /// `history_proofs[layer]` of the block at `height`, lowercase hex.
    pub root_hash: String,
    /// 130 leaves, lowercase hex: the L(layer+1) root at the last L(layer+1)
    /// boundary below `height` (zero when there is none), the L(layer) root
    /// at `height - 128^layer` (zero when `height == 128^layer`), then 128 data
    /// leaves (block leaves for layer 1, L(layer-1) roots above).
    pub leaves: Vec<String>,
}

fn error(code: &'static str, message: String) -> async_graphql::Error {
    async_graphql::Error::new(message).extend_with(|_, e| e.set("code", code))
}

/// A row as the tree code reads it. `Ok(None)` when it cannot give a leaf: a
/// legacy row without hashes, or one without layer roots where the height
/// needs them. A row whose fields do not parse is corrupt.
fn row_view(row: &ProofRow) -> async_graphql::Result<Option<RowView>> {
    let as_hash = |field: &'static str, value: &[u8]| -> async_graphql::Result<[u8; 32]> {
        value.try_into().map_err(|_| {
            data_corrupt(row.height, format!("{field} has {} bytes, expected 32", value.len()))
        })
    };
    let (Some(envelope_hash), Some(tracked_root)) =
        (&row.envelope_hash, &row.tracked_ext_out_messages_root)
    else {
        return Ok(None);
    };
    let blob = row.history_proofs.as_deref().filter(|blob| !matches!(blob, [] | [0]));
    let layers = match blob {
        None if proof_data::layers::required_layers(row.height) > 0 => return Ok(None),
        blob => check_history_proofs_for_height(row.height, blob)
            .map_err(|err| data_corrupt(row.height, err))?,
    };
    Ok(Some(RowView {
        block_id: as_hash("block_id", &row.block_id)?,
        envelope_hash: as_hash("envelope_hash", envelope_hash)?,
        tracked_root: as_hash("tracked_ext_out_messages_root", tracked_root)?,
        layers,
    }))
}

pub async fn layer_tree(
    db: &DBConnector,
    thread_id: String,
    height: u64,
    layer: u8,
) -> async_graphql::Result<LayerTree> {
    let thread_id = pad_thread_id(thread_id);
    if thread_id != THREAD_ZERO {
        return Err(error(CODE_BAD_ARGUMENT, "layer trees exist for thread 0 only".to_string()));
    }
    let spec = LayerTreeSpec::new(height, layer)
        .map_err(|err| error(CODE_BAD_ARGUMENT, err.to_string()))?;

    let mut heights: Vec<u64> = spec.data.iter().copied().filter(|h| *h > 0).collect();
    heights.extend([Some(height), spec.leaf0, spec.leaf1].into_iter().flatten());
    let rows = proof_rows(db, &heights)
        .await
        .map_err(|err| async_graphql::Error::new(format!("{err:#}")))?;
    let mut views: HashMap<u64, RowView> = HashMap::new();
    for (row_height, row) in &rows {
        if let Some(view) = row_view(row)? {
            views.insert(*row_height, view);
        }
    }
    let Some(root) = views
        .get(&height)
        .and_then(|view| view.layers.iter().find(|(n, _)| *n == layer).map(|(_, root)| *root))
    else {
        return Err(missing(&[height]));
    };

    let (leaves, computed) = tokio::task::spawn_blocking(move || {
        let hasher = history_proof::PoseidonHasher::new();
        history_proof::assemble_layer_tree_leaves(&hasher, &spec, |h| views.get(&h).cloned()).map(
            |leaves| {
                let computed = history_proof::layer_tree_root(&hasher, &leaves);
                (leaves, computed)
            },
        )
    })
    .await
    .map_err(|err| async_graphql::Error::new(format!("layer tree task failed: {err}")))?
    .map_err(|missing_heights| missing(&missing_heights))?;

    if computed != root {
        return Err(error(
            CODE_ROOT_MISMATCH,
            format!(
                "L{layer} root at {height} is {}, the tree of the stored rows gives {}",
                hex_string(&root),
                hex_string(&computed)
            ),
        ));
    }
    Ok(LayerTree {
        thread_id,
        height,
        layer,
        root_hash: hex_string(&root),
        leaves: leaves.iter().map(|leaf| hex_string(leaf)).collect(),
    })
}

fn missing(heights: &[u64]) -> async_graphql::Error {
    let list = heights.iter().map(u64::to_string).collect::<Vec<_>>().join(", ");
    error(
        CODE_MISSING_DATA,
        format!(
            "no row or no needed layer root at heights: {list}; retry later or ask another server"
        ),
    )
}
