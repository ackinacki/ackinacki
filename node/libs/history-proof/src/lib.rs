use std::collections::BTreeMap;

use node_types::AccountRouting;
use tvm_vm::executor::zk_stuff::bn254::poseidon::PoseidonSponge;

pub const HISTORY_PROOF_WINDOW_SIZE: usize = 128;
pub const MAX_HISTORY_PROOF_LAYERS: usize = 10;
const HISTORY_PROOF_PREIMAGE_SIZE: usize = 1 + MAX_HISTORY_PROOF_LAYERS * 33;
const REFERENCED_PARENT_BLOCK_TAG: &[u8] = b"acki-nacki:referenced-block:parent:v1";
const REFERENCED_REF_BLOCK_TAG: &[u8] = b"acki-nacki:referenced-block:ref:v1";

pub type LayerNumber = u8;

pub struct PoseidonHasher {
    sponge: PoseidonSponge,
}

impl Default for PoseidonHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl PoseidonHasher {
    pub fn new() -> Self {
        PoseidonHasher { sponge: PoseidonSponge::new() }
    }

    pub fn digest(&self, bytes: &[u8]) -> [u8; 32] {
        self.sponge.hash_bytes_flat(bytes).expect("Poseidon hash failed")
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
    let hasher = PoseidonHasher::new();
    let mut buf = [0u8; 96];
    buf[..32].copy_from_slice(block_id);
    buf[32..64].copy_from_slice(envelope_hash);
    buf[64..96].copy_from_slice(tracked_ext_out_messages_root);
    hasher.digest(&buf)
}

pub fn compute_ext_message_leaf_hash(
    account_dapp_id: &[u8; 32],
    account_id: &[u8; 32],
    ext_message_hash: &[u8; 32],
) -> [u8; 32] {
    let hasher = PoseidonHasher::new();
    let mut buf = [0u8; 96];
    buf[..32].copy_from_slice(account_dapp_id);
    buf[32..64].copy_from_slice(account_id);
    buf[64..96].copy_from_slice(ext_message_hash);
    hasher.digest(&buf)
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
