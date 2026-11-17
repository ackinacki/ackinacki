// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! Layer trees assembled from stored rows must give the roots the node wrote.
//! The fixture is produced by the node's own cursor code:
//! `cargo test -p node --release --lib regenerate_history_chain_fixture -- --ignored`.

use std::collections::BTreeMap;
use std::collections::HashMap;

use history_proof::assemble_layer_tree_leaves;
use history_proof::compute_block_leaf_hash;
use history_proof::compute_block_leaf_hash_with;
use history_proof::compute_ext_out_messages_root;
use history_proof::ext_out_messages_root_from_groups;
use history_proof::layer_tree_root;
use history_proof::PoseidonHasher;
use history_proof::RowView;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;
use proof_data::layer_tree::LayerTreeSpec;
use proof_data::layers::check_history_proofs_for_height;
use proof_data::tracked::TrackedGroup;
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    rows: Vec<FixtureRow>,
}

#[derive(Deserialize)]
struct FixtureRow {
    height: u64,
    block_id: String,
    envelope_hash: String,
    tracked_ext_out_messages_root: String,
    history_proofs: Option<String>,
}

fn hash(hex_str: &str) -> [u8; 32] {
    hex::decode(hex_str).unwrap().try_into().unwrap()
}

fn rows() -> HashMap<u64, RowView> {
    let fixture: Fixture =
        serde_json::from_str(include_str!("fixtures/history_chain.json")).unwrap();
    fixture
        .rows
        .into_iter()
        .map(|row| {
            let blob = row.history_proofs.as_deref().map(|b| hex::decode(b).unwrap());
            let layers = check_history_proofs_for_height(row.height, blob.as_deref()).unwrap();
            (
                row.height,
                RowView {
                    block_id: hash(&row.block_id),
                    envelope_hash: hash(&row.envelope_hash),
                    tracked_root: hash(&row.tracked_ext_out_messages_root),
                    layers,
                },
            )
        })
        .collect()
}

fn layer_root(rows: &HashMap<u64, RowView>, height: u64, layer: u8) -> [u8; 32] {
    rows[&height].layers.iter().find(|(n, _)| *n == layer).unwrap().1
}

fn assembled_root(rows: &HashMap<u64, RowView>, height: u64, layer: u8) -> [u8; 32] {
    let hasher = PoseidonHasher::new();
    let spec = LayerTreeSpec::new(height, layer).unwrap();
    let leaves = assemble_layer_tree_leaves(&hasher, &spec, |h| rows.get(&h).cloned()).unwrap();
    assert_eq!(leaves.len(), 130);
    layer_tree_root(&hasher, &leaves)
}

#[test]
fn fixture_l1_roots_match_assembled_trees() {
    let rows = rows();
    for t in [128, 256, 16_384, 16_512] {
        assert_eq!(assembled_root(&rows, t, 1), layer_root(&rows, t, 1), "L1 at {t}");
    }
}

#[test]
fn fixture_l2_roots_match() {
    let rows = rows();
    for (t, n) in [(16_384, 2), (2_097_152, 2), (2_097_152, 3)] {
        assert_eq!(assembled_root(&rows, t, n), layer_root(&rows, t, n), "L{n} at {t}");
    }
}

#[test]
fn assemble_reports_missing_heights() {
    let mut rows = rows();
    rows.remove(&16_300);
    let hasher = PoseidonHasher::new();
    let spec = LayerTreeSpec::new(16_384, 1).unwrap();
    assert_eq!(
        assemble_layer_tree_leaves(&hasher, &spec, |h| rows.get(&h).cloned()),
        Err(vec![16_300])
    );
    // A row without the needed layer is reported like a missing row.
    rows.get_mut(&16_256).unwrap().layers.clear();
    assert_eq!(
        assemble_layer_tree_leaves(&hasher, &spec, |h| rows.get(&h).cloned()),
        Err(vec![16_256, 16_300])
    );
}

#[test]
fn block_leaf_with_matches_plain() {
    let hasher = PoseidonHasher::new();
    let (a, b, c) = ([1u8; 32], [2u8; 32], [3u8; 32]);
    assert_eq!(
        compute_block_leaf_hash_with(&hasher, &a, &b, &c),
        compute_block_leaf_hash(&a, &b, &c)
    );
}

#[test]
fn groups_root_matches_btreemap_root() {
    let first = AccountIdentifier::new([0x1a; 32]).routing(DAppIdentifier::new([0; 32]));
    let second = AccountIdentifier::new([0x10; 32]).routing(DAppIdentifier::new([4; 32]));
    let mut map = BTreeMap::new();
    map.insert(first, vec![[0x11; 32], [0x12; 32]]);
    map.insert(second, vec![[0x21; 32]]);
    let groups: Vec<TrackedGroup> = map
        .iter()
        .map(|(routing, hashes)| {
            let (dapp_id, account_id) = routing.unpack_for_hash();
            TrackedGroup { dapp_id, account_id, hashes: hashes.clone() }
        })
        .collect();
    let hasher = PoseidonHasher::new();
    assert_eq!(
        ext_out_messages_root_from_groups(&hasher, &groups),
        compute_ext_out_messages_root(&map)
    );
    assert_eq!(ext_out_messages_root_from_groups(&hasher, &[]), [0u8; 32]);
    let empty_group = TrackedGroup { dapp_id: [0; 32], account_id: [1; 32], hashes: vec![] };
    assert_eq!(ext_out_messages_root_from_groups(&hasher, &[empty_group]), [0u8; 32]);
}

/// Missing inputs are found from the rows alone, before any Poseidon hashing:
/// the verifier re-checks windows it cannot complete on every pass.
#[test]
fn missing_heights_found_without_hashing() {
    let mut rows = rows();
    let spec = LayerTreeSpec::new(16_512, 1).unwrap();
    assert_eq!(
        history_proof::layer_tree_missing_heights(&spec, |h| rows.get(&h).cloned()),
        Vec::<u64>::new()
    );
    rows.remove(&16_400);
    rows.remove(&16_384);
    assert_eq!(
        history_proof::layer_tree_missing_heights(&spec, |h| rows.get(&h).cloned()),
        vec![16_384, 16_400]
    );
}

/// A row whose second read fails (a store read error turned into "no row")
/// must not become a zero leaf: the tree would get a false root mismatch.
#[test]
fn each_row_is_read_once() {
    let rows = rows();
    let hasher = PoseidonHasher::new();
    for (height, layer) in [(16_384, 1), (16_384, 2)] {
        let spec = LayerTreeSpec::new(height, layer).unwrap();
        let reads = std::cell::RefCell::new(HashMap::<u64, usize>::new());
        let flaky = |h: u64| {
            let mut reads = reads.borrow_mut();
            let count = reads.entry(h).or_default();
            *count += 1;
            (*count == 1).then(|| rows.get(&h).cloned()).flatten()
        };
        let leaves = assemble_layer_tree_leaves(&hasher, &spec, flaky).unwrap();
        assert_eq!(
            layer_tree_root(&hasher, &leaves),
            layer_root(&rows, height, layer),
            "L{layer} at {height}"
        );
    }
}
