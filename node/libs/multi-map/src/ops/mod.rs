mod get;
mod split;
mod update;

#[cfg(test)]
mod tests;

// Public API
// Shared helpers used by update.rs and split.rs
use std::sync::Arc;

pub use get::get;
pub use split::merge;
pub use split::split;
pub use update::update;

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
