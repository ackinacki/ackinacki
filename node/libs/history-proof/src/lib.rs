use std::collections::BTreeMap;

use node_types::AccountRouting;
use tvm_vm::executor::zk_stuff::bn254::poseidon::poseidon_bytes_flat;

pub const HISTORY_PROOF_WINDOW_SIZE: usize = 128;
pub const MAX_HISTORY_PROOF_LAYERS: usize = 10;
const HISTORY_PROOF_PREIMAGE_SIZE: usize = 1 + MAX_HISTORY_PROOF_LAYERS * 33;
const REFERENCED_PARENT_BLOCK_TAG: &[u8] = b"acki-nacki:referenced-block:parent:v1";
const REFERENCED_REF_BLOCK_TAG: &[u8] = b"acki-nacki:referenced-block:ref:v1";

pub type LayerNumber = u8;

// Keep construction through `new` while storing no per-hash mutable state.
pub struct PoseidonHasher(());

impl Default for PoseidonHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl PoseidonHasher {
    pub fn new() -> Self {
        Self(())
    }

    pub fn digest(&self, bytes: &[u8]) -> [u8; 32] {
        // The SDK initializes the round constants once in its LazyLock and
        // clones the prepared sponge for each digest. Rebuilding the template
        // here costs milliseconds, including for blocks with no transactions.
        // Each call still has an independent hashing state, also across threads.
        poseidon_bytes_flat(bytes).expect("Poseidon hash failed")
    }
}

pub fn dense_leaf_hash(hasher: &PoseidonHasher, value1: &[u8; 32], value2: &[u8; 32]) -> [u8; 32] {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(value1);
    buf[32..].copy_from_slice(value2);
    hasher.digest(&buf)
}

fn dense_combine(hasher: &PoseidonHasher, left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut buf = [0u8; 64];
    buf[..32].copy_from_slice(left);
    buf[32..].copy_from_slice(right);
    hasher.digest(&buf)
}

pub fn dense_merkle_tree(hasher: &PoseidonHasher, leaves: &[[u8; 32]]) -> Vec<[u8; 32]> {
    assert!(!leaves.is_empty());
    let width = leaves.len().next_power_of_two();
    let tree_size = 2 * width - 1;
    let mut tree = vec![[0u8; 32]; tree_size];

    let leaf_start = width - 1;
    for (i, leaf) in leaves.iter().enumerate() {
        tree[leaf_start + i] = *leaf;
    }

    for i in (0..leaf_start).rev() {
        let left = 2 * i + 1;
        let right = 2 * i + 2;
        tree[i] = dense_combine(hasher, &tree[left], &tree[right]);
    }

    tree
}

pub fn dense_merkle_root(hasher: &PoseidonHasher, leaves: &[[u8; 32]]) -> [u8; 32] {
    dense_merkle_tree(hasher, leaves)[0]
}

pub fn dense_merkle_proof(
    hasher: &PoseidonHasher,
    leaves: &[[u8; 32]],
    pos: usize,
) -> Vec<[u8; 32]> {
    let tree = dense_merkle_tree(hasher, leaves);
    let width = leaves.len().next_power_of_two();
    let mut idx = width - 1 + pos;
    let mut proof = Vec::new();

    while idx > 0 {
        let sibling = if idx % 2 == 1 { idx + 1 } else { idx - 1 };
        proof.push(tree[sibling]);
        idx = (idx - 1) / 2;
    }

    proof
}

pub fn dense_merkle_verify(
    hasher: &PoseidonHasher,
    root: &[u8; 32],
    leaf: &[u8; 32],
    pos: usize,
    proof: &[[u8; 32]],
) -> bool {
    let width = 1usize << proof.len();
    let mut idx = width - 1 + pos;
    let mut current = *leaf;

    for sibling in proof {
        current = if idx % 2 == 1 {
            dense_combine(hasher, &current, sibling)
        } else {
            dense_combine(hasher, sibling, &current)
        };
        idx = (idx - 1) / 2;
    }

    current == *root
}

pub fn compute_block_leaf_hash(
    block_id: &[u8; 32],
    envelope_hash: &[u8; 32],
    tracked_ext_out_messages_root: &[u8; 32],
) -> [u8; 32] {
    compute_block_leaf_hash_with(
        &PoseidonHasher::new(),
        block_id,
        envelope_hash,
        tracked_ext_out_messages_root,
    )
}

/// `compute_block_leaf_hash` with a caller-owned hasher: creating a hasher
/// costs far more than one hash, so loops over many blocks reuse one.
pub fn compute_block_leaf_hash_with(
    hasher: &PoseidonHasher,
    block_id: &[u8; 32],
    envelope_hash: &[u8; 32],
    tracked_ext_out_messages_root: &[u8; 32],
) -> [u8; 32] {
    hash_three(hasher, block_id, envelope_hash, tracked_ext_out_messages_root)
}

pub fn compute_ext_message_leaf_hash(
    account_dapp_id: &[u8; 32],
    account_id: &[u8; 32],
    ext_message_hash: &[u8; 32],
) -> [u8; 32] {
    hash_three(&PoseidonHasher::new(), account_dapp_id, account_id, ext_message_hash)
}

fn hash_three(hasher: &PoseidonHasher, a: &[u8; 32], b: &[u8; 32], c: &[u8; 32]) -> [u8; 32] {
    let mut buf = [0u8; 96];
    buf[..32].copy_from_slice(a);
    buf[32..64].copy_from_slice(b);
    buf[64..96].copy_from_slice(c);
    hasher.digest(&buf)
}

/// `compute_ext_out_messages_root` over groups decoded from a stored
/// `tracked_ext_out_message_hashes` blob (same order: the blob is written from
/// the same sorted map). Zero when there are no hashes.
pub fn ext_out_messages_root_from_groups(
    hasher: &PoseidonHasher,
    groups: &[proof_data::tracked::TrackedGroup],
) -> [u8; 32] {
    let leaf_hashes: Vec<[u8; 32]> = groups
        .iter()
        .flat_map(|group| {
            group
                .hashes
                .iter()
                .map(|message| hash_three(hasher, &group.dapp_id, &group.account_id, message))
        })
        .collect();
    if leaf_hashes.is_empty() {
        return [0u8; 32];
    }
    dense_merkle_root(hasher, &leaf_hashes)
}

/// The fields of a stored thread-0 block that layer trees are built from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowView {
    pub block_id: [u8; 32],
    pub envelope_hash: [u8; 32],
    pub tracked_root: [u8; 32],
    pub layers: proof_data::layers::LayerRoots,
}

impl RowView {
    fn layer_root(&self, layer: LayerNumber) -> Option<[u8; 32]> {
        self.layers.iter().find(|(n, _)| *n == layer).map(|(_, root)| *root)
    }
}

/// Number of leaves of a layer tree: the higher-layer root, the previous
/// same-layer root and one window of data.
pub const LAYER_TREE_LEAF_COUNT: usize = 2 + HISTORY_PROOF_WINDOW_SIZE;

/// Heights whose row is missing or lacks the layer root the tree described by
/// `spec` needs, sorted. Reads rows only, no hashing: `assemble_layer_tree_leaves`
/// runs it before any hashing.
pub fn layer_tree_missing_heights(
    spec: &proof_data::layer_tree::LayerTreeSpec,
    get: impl Fn(u64) -> Option<RowView>,
) -> Vec<u64> {
    let has_layer = |height: u64, layer: LayerNumber| {
        get(height).is_some_and(|row| row.layer_root(layer).is_some())
    };
    let mut missing = Vec::new();
    for (height, layer) in [(spec.leaf0, spec.layer + 1), (spec.leaf1, spec.layer)] {
        if let Some(height) = height {
            if !has_layer(height, layer) {
                missing.push(height);
            }
        }
    }
    for &height in &spec.data {
        let present = if spec.layer == 1 {
            height == 0 || get(height).is_some()
        } else {
            has_layer(height, spec.layer - 1)
        };
        if !present {
            missing.push(height);
        }
    }
    missing.sort_unstable();
    missing.dedup();
    missing
}

/// Builds the 130 leaves of the layer tree described by `spec` from stored
/// rows, in the order the node hashes them, or returns the sorted heights
/// whose row is missing or lacks the needed layer root (found before any
/// hashing). Height 0 is a zero leaf. The rows are read through `get` first,
/// and only those reads are used: a second read that failed (a store error
/// read as "no row") would otherwise become a zero leaf and a false root
/// mismatch.
pub fn assemble_layer_tree_leaves(
    hasher: &PoseidonHasher,
    spec: &proof_data::layer_tree::LayerTreeSpec,
    get: impl Fn(u64) -> Option<RowView>,
) -> Result<Vec<[u8; 32]>, Vec<u64>> {
    let mut read: std::collections::HashMap<u64, RowView> = std::collections::HashMap::new();
    for height in spec.leaf0.into_iter().chain(spec.leaf1).chain(spec.data.iter().copied()) {
        if let std::collections::hash_map::Entry::Vacant(entry) = read.entry(height) {
            if let Some(row) = get(height) {
                entry.insert(row);
            }
        }
    }
    let missing = layer_tree_missing_heights(spec, |height| read.get(&height).cloned());
    if !missing.is_empty() {
        return Err(missing);
    }
    let layer_root = |height: Option<u64>, layer: LayerNumber| {
        height
            .and_then(|height| read.get(&height))
            .and_then(|row| row.layer_root(layer))
            .unwrap_or([0u8; 32])
    };
    let mut leaves = Vec::with_capacity(LAYER_TREE_LEAF_COUNT);
    leaves.push(layer_root(spec.leaf0, spec.layer + 1));
    leaves.push(layer_root(spec.leaf1, spec.layer));
    for &height in &spec.data {
        let leaf = if spec.layer == 1 {
            match read.get(&height).filter(|_| height != 0) {
                Some(row) => compute_block_leaf_hash_with(
                    hasher,
                    &row.block_id,
                    &row.envelope_hash,
                    &row.tracked_root,
                ),
                None => [0u8; 32],
            }
        } else {
            layer_root(Some(height), spec.layer - 1)
        };
        leaves.push(leaf);
    }
    Ok(leaves)
}

/// Root of a layer tree from its 130 leaves (padded with zero leaves to 256).
pub fn layer_tree_root(hasher: &PoseidonHasher, leaves: &[[u8; 32]]) -> [u8; 32] {
    assert_eq!(leaves.len(), LAYER_TREE_LEAF_COUNT, "a layer tree has 130 leaves");
    dense_merkle_root(hasher, leaves)
}

pub fn compute_ext_out_messages_root(
    message_hashes: &BTreeMap<AccountRouting, Vec<[u8; 32]>>,
) -> [u8; 32] {
    if message_hashes.is_empty() {
        return [0u8; 32];
    }
    let mut leaf_hashes = vec![];
    let hasher = PoseidonHasher::new();
    for (account_routing, messages) in message_hashes {
        let (dapp, acc) = account_routing.unpack_for_hash();
        for message in messages {
            leaf_hashes.push(compute_ext_message_leaf_hash(&dapp, &acc, message));
        }
    }

    dense_merkle_root(&hasher, &leaf_hashes)
}

pub fn compute_referenced_block_leaf_hash(index: usize, block_id: &[u8; 32]) -> [u8; 32] {
    let tag = if index == 0 { REFERENCED_PARENT_BLOCK_TAG } else { REFERENCED_REF_BLOCK_TAG };
    let hasher = PoseidonHasher::new();
    let mut buf = Vec::with_capacity(tag.len() + block_id.len());
    buf.extend_from_slice(tag);
    buf.extend_from_slice(block_id);
    hasher.digest(&buf)
}

pub fn compute_referenced_blocks_root<'a>(
    proof_block_refs: impl IntoIterator<Item = &'a [u8; 32]>,
) -> [u8; 32] {
    let leaf_hashes = proof_block_refs
        .into_iter()
        .enumerate()
        .map(|(index, block_id)| compute_referenced_block_leaf_hash(index, block_id))
        .collect::<Vec<_>>();
    if leaf_hashes.is_empty() {
        return [0u8; 32];
    }
    dense_merkle_root(&PoseidonHasher::new(), &leaf_hashes)
}

pub fn history_proofs_l0<'a>(
    history_proofs: impl IntoIterator<Item = (LayerNumber, &'a [u8; 32])>,
) -> [u8; 32] {
    let mut roots = [None; MAX_HISTORY_PROOF_LAYERS];
    let mut count = 0usize;
    for (layer, root_hash) in history_proofs {
        if (1..=MAX_HISTORY_PROOF_LAYERS as u8).contains(&layer) {
            roots[layer as usize - 1] = Some(*root_hash);
        }
        count += 1;
    }

    let mut preimage = Vec::with_capacity(HISTORY_PROOF_PREIMAGE_SIZE);
    preimage.push(count as u8);
    for (index, root_hash) in roots.iter().enumerate() {
        preimage.push(index as u8 + 1);
        if let Some(root_hash) = root_hash {
            preimage.extend_from_slice(root_hash);
        } else {
            preimage.extend_from_slice(&[0u8; 32]);
        }
    }
    debug_assert_eq!(preimage.len(), HISTORY_PROOF_PREIMAGE_SIZE);

    PoseidonHasher::new().digest(&preimage)
}

#[cfg(test)]
mod tests {
    use node_types::AccountIdentifier;
    use node_types::DAppIdentifier;

    use super::*;

    fn routing(dapp: u8, account: u8) -> AccountRouting {
        AccountIdentifier::new([account; 32]).routing(DAppIdentifier::new([dapp; 32]))
    }

    #[test]
    fn cached_poseidon_matches_fresh_sponge_across_threads() {
        use tvm_vm::executor::zk_stuff::bn254::poseidon::PoseidonSponge;

        // Cover field chunk boundaries and the fixed history-proof preimage.
        // Reference results use the previous, freshly constructed sponge path.
        let inputs: Vec<_> = [0, 30, 31, 32, 64, 331]
            .into_iter()
            .map(|len| (0..len).map(|i| (i % 251) as u8).collect::<Vec<_>>())
            .collect();
        let reference = PoseidonSponge::new();
        let expected: Vec<_> =
            inputs.iter().map(|input| reference.hash_bytes_flat(input).unwrap()).collect();
        let hasher = PoseidonHasher::new();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..8 {
                        for (input, expected) in inputs.iter().zip(&expected) {
                            assert_eq!(&hasher.digest(input), expected);
                        }
                    }
                });
            }
        });
    }

    #[test]
    fn dense_merkle_proof_verifies_for_all_leaves() {
        let hasher = PoseidonHasher::new();
        let leaves = [[1u8; 32], [2u8; 32], [3u8; 32], [4u8; 32]];
        let root = dense_merkle_root(&hasher, &leaves);
        for (pos, leaf) in leaves.iter().enumerate() {
            let proof = dense_merkle_proof(&hasher, &leaves, pos);
            assert!(dense_merkle_verify(&hasher, &root, leaf, pos, &proof));
        }
    }

    #[test]
    fn block_leaf_hash_depends_on_all_inputs() {
        let block_id = [1u8; 32];
        let envelope_hash = [2u8; 32];
        let tracked_ext_out_messages_root = [3u8; 32];
        let base =
            compute_block_leaf_hash(&block_id, &envelope_hash, &tracked_ext_out_messages_root);

        assert_ne!(
            base,
            compute_block_leaf_hash(&[4u8; 32], &envelope_hash, &tracked_ext_out_messages_root)
        );
        assert_ne!(
            base,
            compute_block_leaf_hash(&block_id, &[5u8; 32], &tracked_ext_out_messages_root)
        );
        assert_ne!(base, compute_block_leaf_hash(&block_id, &envelope_hash, &[6u8; 32]));
    }

    #[test]
    fn ext_out_messages_root_is_stable_for_different_insertion_order() {
        let first = routing(1, 1);
        let second = routing(2, 2);

        let mut forward = BTreeMap::new();
        forward.insert(first, vec![[0x11; 32], [0x12; 32]]);
        forward.insert(second, vec![[0x21; 32]]);

        let mut reverse = BTreeMap::new();
        reverse.insert(second, vec![[0x21; 32]]);
        reverse.insert(first, vec![[0x11; 32], [0x12; 32]]);

        assert_eq!(
            compute_ext_out_messages_root(&forward),
            compute_ext_out_messages_root(&reverse)
        );
    }

    #[test]
    fn history_proofs_l0_is_layer_order_independent() {
        let first = [0x11; 32];
        let second = [0x22; 32];
        assert_eq!(
            history_proofs_l0([(1, &first), (2, &second)]),
            history_proofs_l0([(2, &second), (1, &first)])
        );
    }

    #[test]
    fn referenced_blocks_root_depends_on_order_and_position() {
        let parent = [0x11; 32];
        let first_ref = [0x22; 32];
        let second_ref = [0x33; 32];

        let base = compute_referenced_blocks_root([&parent, &first_ref, &second_ref]);
        assert_ne!(base, compute_referenced_blocks_root([&parent, &second_ref, &first_ref]));
        assert_ne!(base, compute_referenced_blocks_root([&first_ref, &parent, &second_ref]));
        assert_ne!(
            compute_referenced_blocks_root([&parent]),
            compute_referenced_blocks_root([&parent, &first_ref])
        );
        assert_ne!(
            compute_referenced_block_leaf_hash(0, &parent),
            compute_referenced_block_leaf_hash(1, &parent)
        );
    }

    #[test]
    fn empty_referenced_blocks_root_is_zero_for_legacy_rows() {
        let empty: [&[u8; 32]; 0] = [];
        assert_eq!(compute_referenced_blocks_root(empty), [0u8; 32]);
    }
}
