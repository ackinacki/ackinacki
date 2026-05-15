mod get;
mod split;
mod update;

#[cfg(test)]
mod tests;

// Public API
// Shared helpers used by update.rs and split.rs
use std::sync::Arc;

use blake3::Hasher;
pub use get::get;
use node_types::Blake3Hashable;
pub use split::merge;
pub use split::merge_batch;
pub use split::split;
pub use update::update;
pub use update::update_single;

use crate::node::BranchData;
use crate::node::Node;
use crate::MultiMapValue;

/// Node pointer equality check (same allocation).
pub(crate) fn node_ptr_eq<V: MultiMapValue>(a: &Node<V>, b: &Node<V>) -> bool {
    match (a, b) {
        (Node::Empty, Node::Empty) => true,
        (Node::Leaf(a), Node::Leaf(b)) => Arc::ptr_eq(a, b),
        (Node::Branch(a), Node::Branch(b)) => Arc::ptr_eq(a, b),
        (Node::Ext(a), Node::Ext(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

/// Compressing branch constructor: single-child branch → Ext node.
pub(crate) fn make_branch<V: MultiMapValue>(bitmap: u16, mut children: [Node<V>; 16]) -> Node<V> {
    if bitmap == 0 {
        return Node::Empty;
    }
    if bitmap.count_ones() == 1 {
        let nib = bitmap.trailing_zeros() as u8;
        let child = std::mem::take(&mut children[nib as usize]);
        return compress_single_child(nib, child);
    }
    Node::new_branch(bitmap, children)
}

/// Single nibble → Ext([nib], child), fusing with child Ext if possible.
pub(crate) fn compress_single_child<V: MultiMapValue>(nib: u8, child: Node<V>) -> Node<V> {
    if let Node::Ext(data) = &child {
        let cc = data.nibble_count as usize;
        let mut merged = [0u8; 64];
        merged[0] = nib;
        merged[1..1 + cc].copy_from_slice(&data.nibbles[..cc]);
        return Node::new_ext(&merged[..1 + cc], data.child.clone());
    }
    Node::new_ext(&[nib], child)
}

/// Flatten `Ext(a, Ext(b, x))` → `Ext(a||b, x)`.
pub(crate) fn merge_ext_if_possible<V: MultiMapValue>(node: Node<V>) -> Node<V> {
    if let Node::Ext(data) = &node {
        if let Node::Ext(child_data) = &data.child {
            let nc = data.nibble_count as usize;
            let cc = child_data.nibble_count as usize;
            let total = nc + cc;
            let mut merged = [0u8; 64];
            merged[..nc].copy_from_slice(&data.nibbles[..nc]);
            merged[nc..total].copy_from_slice(&child_data.nibbles[..cc]);
            return Node::new_ext(&merged[..total], child_data.child.clone());
        }
    }
    node
}

/// Materialize an Ext node into a Branch consuming 1 nibble.
/// Uses unhashed constructors — the intermediate node is temporary and its hash is never read.
pub(crate) fn ensure_branch_at_depth<V: MultiMapValue>(root: &Node<V>) -> Node<V> {
    match root {
        Node::Empty => Node::Empty,
        Node::Branch(_) => root.clone(),
        Node::Ext(data) => {
            let count = data.nibble_count as usize;
            if count == 0 {
                return data.child.clone();
            }
            let first = data.nibbles[0];
            let tail_child = if count == 1 {
                data.child.clone()
            } else {
                // Ext tail must be hashed — it may be kept in the final tree
                Node::new_ext(&data.nibbles[1..count], data.child.clone())
            };
            let mut children: [Node<V>; 16] = Default::default();
            children[first as usize] = tail_child;
            // Branch wrapper is always temporary (destructured for children), safe to skip hash
            Node::new_branch_unhashed(children)
        }
        Node::Leaf(_) => root.clone(),
    }
}

/// Rebuild a Branch node with one child changed. Uses `make_branch` (compressing).
pub(crate) fn rebuild_branch_with_child<V: MultiMapValue>(
    br: &Arc<BranchData<V>>,
    nib: u8,
    new_child: Node<V>,
) -> Node<V> {
    let bit = 1u16 << (nib as u16);
    let existed = (br.bitmap & bit) != 0;

    if !existed && new_child.is_empty() {
        return Node::Branch(Arc::clone(br));
    }

    let mut out = br.children.clone();
    out[nib as usize] = new_child;

    let mut out_bitmap: u16 = 0;
    for n in 0..16u16 {
        if !out[n as usize].is_empty() {
            out_bitmap |= 1u16 << n;
        }
    }

    if out_bitmap == 0 {
        Node::Empty
    } else {
        make_branch(out_bitmap, out)
    }
}

#[inline]
pub fn boundary_mask(r: usize) -> u8 {
    debug_assert!((1..=3).contains(&r));
    (0xFu8 << (4 - r)) & 0xF
}

#[inline]
pub fn prefix_bits_match(prefix: &[u8; 32], key: &[u8; 32], bits: u8) -> bool {
    let bits = bits as usize;
    let full = bits / 8;
    let rem = bits % 8;

    if full > 0 && prefix[..full] != key[..full] {
        return false;
    }
    if rem == 0 {
        return true;
    }
    let mask = 0xFFu8 << (8 - rem);
    (prefix[full] & mask) == (key[full] & mask)
}

#[inline]
pub fn nibble_from_key(key: &[u8; 32], i: usize) -> u8 {
    let b = key[i / 2];
    if (i & 1) == 0 {
        (b >> 4) & 0x0F
    } else {
        b & 0x0F
    }
}

#[inline]
pub fn nibble_at(key: &[u8; 32], depth: usize) -> u8 {
    let b = key[depth / 2];
    if (depth & 1) == 0 {
        (b >> 4) & 0x0F
    } else {
        b & 0x0F
    }
}

/// H(0x01 || value) — leaf depends only on value
#[inline(always)]
pub fn hash_leaf_value<V: Blake3Hashable>(value: &V) -> [u8; 32] {
    value.hash(0x01)
}

/// Sparse branch hash:
/// H(0x02 || bitmap_le(u16) || repeated (nibble(u8) || child_hash([u8;32])) in nibble order)
pub fn hash_branch_sparse(bitmap: u16, nibs: &[u8], child_hashes: &[[u8; 32]]) -> [u8; 32] {
    debug_assert_eq!(nibs.len(), child_hashes.len());

    let mut h = Hasher::new();
    h.update(&[0x02]);
    h.update(&bitmap.to_le_bytes());

    for i in 0..nibs.len() {
        h.update(&[nibs[i]]);
        h.update(&child_hashes[i]);
    }

    *h.finalize().as_bytes()
}

/// Extension node hash:
/// H(0x03 || len(u8) || packed_nibbles || child_hash)
///
/// Packed_nibbles contains 2 nibbles per byte (high then low), length = ceil(len/2).
pub fn hash_ext(len: u8, packed_nibbles: &[u8], child_hash: &[u8; 32]) -> [u8; 32] {
    debug_assert_eq!(packed_nibbles.len(), (len as usize).div_ceil(2));

    let mut h = Hasher::new();
    h.update(&[0x03]);
    h.update(&[len]);
    h.update(packed_nibbles);
    h.update(child_hash);

    *h.finalize().as_bytes()
}

/// Empty branch (bitmap=0)
pub fn hash_branch_empty() -> [u8; 32] {
    hash_branch_sparse(0, &[], &[])
}
