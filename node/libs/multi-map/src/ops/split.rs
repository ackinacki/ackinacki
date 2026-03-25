use trie_map::trie::arena::boundary_mask;
use trie_map::trie::arena::nibble_at;
use trie_map::trie::arena::nibble_from_key;
use trie_map::MapKeyPath;

use super::ensure_branch_at_depth;
use super::make_branch;
use super::merge_ext_if_possible;
use super::rebuild_branch_with_child;
use crate::node::Node;
use crate::MultiMapValue;

/// Normalize trie structure: collapse single-child branches, merge adjacent Exts, remove empties.
fn canonicalize<V: MultiMapValue>(node: &Node<V>) -> Node<V> {
    match node {
        Node::Empty => Node::Empty,

        Node::Leaf(_) => node.clone(),

        Node::Ext(data) => {
            let child2 = canonicalize(&data.child);
            match child2 {
                Node::Empty => Node::Empty,
                c => {
                    let count = data.nibble_count as usize;
                    let ext = Node::new_ext(&data.nibbles[..count], c);
                    merge_ext_if_possible(ext)
                }
            }
        }

        Node::Branch(data) => {
            let mut out: [Node<V>; 16] = Default::default();
            let mut out_bitmap: u16 = 0;

            for nib in 0..16u8 {
                let bit = 1u16 << (nib as u16);
                if (data.bitmap & bit) == 0 {
                    continue;
                }
                let c = canonicalize(&data.children[nib as usize]);
                if !c.is_empty() {
                    out_bitmap |= bit;
                    out[nib as usize] = c;
                }
            }

            if out_bitmap == 0 {
                Node::Empty
            } else {
                make_branch(out_bitmap, out)
            }
        }
    }
}

/// Find the subtree root for a given bit-prefix path.
fn subtree_root_for_view<V: MultiMapValue>(root: &Node<V>, path: MapKeyPath) -> Node<V> {
    let d = (path.len as usize) / 4;
    let r = (path.len as usize) % 4;

    let mut current = root.clone();
    for depth in 0..d {
        let br = ensure_branch_at_depth(&current);
        let br_data = match &br {
            Node::Branch(data) => data,
            _ => return Node::Empty,
        };
        let nib = nibble_at(&path.prefix.0, depth);
        current = br_data.children[nib as usize].clone();
        if current.is_empty() {
            return Node::Empty;
        }
    }

    if r == 0 {
        return current;
    }

    // Non-nibble-aligned boundary
    let br = ensure_branch_at_depth(&current);
    let br_data = match &br {
        Node::Branch(data) => data,
        _ => return Node::Empty,
    };

    let mask = boundary_mask(r);
    let want = nibble_from_key(&path.prefix.0, d) & mask;

    let mut out: [Node<V>; 16] = Default::default();
    let mut out_bitmap: u16 = 0;

    for nib in 0..16u8 {
        let bit = 1u16 << (nib as u16);
        if (br_data.bitmap & bit) == 0 {
            continue;
        }
        if (nib & mask) == want {
            out_bitmap |= bit;
            out[nib as usize] = br_data.children[nib as usize].clone();
        }
    }

    if out_bitmap == 0 {
        Node::Empty
    } else {
        make_branch(out_bitmap, out)
    }
}

/// Remove the prefix subtree from root (COW).
fn prune_prefix<V: MultiMapValue>(root: &Node<V>, path: MapKeyPath) -> Node<V> {
    let d = (path.len as usize) / 4;
    let r = (path.len as usize) % 4;
    prune_rec(root, 0, d, r, &path.prefix.0)
}

fn prune_rec<V: MultiMapValue>(
    node: &Node<V>,
    depth: usize,
    d: usize,
    r: usize,
    prefix_key: &[u8; 32],
) -> Node<V> {
    if node.is_empty() {
        return Node::Empty;
    }
    if depth > d {
        return node.clone();
    }

    let br = ensure_branch_at_depth(node);
    let br_data = match &br {
        Node::Branch(data) => data,
        _ => return node.clone(),
    };

    if depth < d {
        let nib = nibble_at(prefix_key, depth);
        if br_data.children[nib as usize].is_empty() {
            return br;
        }
        let new_child = prune_rec(&br_data.children[nib as usize], depth + 1, d, r, prefix_key);
        return rebuild_branch_with_child(br_data, nib, new_child);
    }

    // depth == d
    if r == 0 {
        return Node::Empty;
    }

    let mask = boundary_mask(r);
    let want = nibble_from_key(prefix_key, depth) & mask;

    let mut out: [Node<V>; 16] = Default::default();
    let mut out_bitmap: u16 = 0;

    for nib in 0..16u8 {
        let bit = 1u16 << (nib as u16);
        if (br_data.bitmap & bit) == 0 {
            continue;
        }
        if (nib & mask) != want {
            out_bitmap |= bit;
            out[nib as usize] = br_data.children[nib as usize].clone();
        }
    }

    if out_bitmap == 0 {
        Node::Empty
    } else {
        make_branch(out_bitmap, out)
    }
}

/// Graft a subtree back into root at the given prefix position (COW).
fn graft_prefix<V: MultiMapValue>(
    root: &Node<V>,
    sub_root: &Node<V>,
    sub_path: MapKeyPath,
) -> Node<V> {
    let d = (sub_path.len as usize) / 4;
    let r = (sub_path.len as usize) % 4;
    graft_rec(root, 0, d, r, &sub_path.prefix.0, sub_root)
}

fn graft_rec<V: MultiMapValue>(
    node: &Node<V>,
    depth: usize,
    d: usize,
    r: usize,
    prefix_key: &[u8; 32],
    sub_root: &Node<V>,
) -> Node<V> {
    if depth == d {
        if r == 0 {
            return sub_root.clone();
        }

        // Sub-nibble boundary: merge bitmaps
        let base_br = ensure_branch_at_depth(node);
        let sub_br = ensure_branch_at_depth(sub_root);

        let sub_data = match &sub_br {
            Node::Branch(data) => data,
            _ => return node.clone(),
        };

        // Start with existing children
        let mut nib_to_child: [Node<V>; 16] = Default::default();

        if let Node::Branch(base_data) = &base_br {
            nib_to_child = base_data.children.clone();
        }

        // Sub overwrites existing
        for nib in 0..16u8 {
            if sub_data.bitmap & (1u16 << nib) != 0 {
                nib_to_child[nib as usize] = sub_data.children[nib as usize].clone();
            }
        }

        // Compute bitmap from actual children
        let mut out_bitmap: u16 = 0;
        for nib in 0..16u8 {
            if !nib_to_child[nib as usize].is_empty() {
                out_bitmap |= 1u16 << nib;
            }
        }

        return if out_bitmap == 0 { Node::Empty } else { make_branch(out_bitmap, nib_to_child) };
    }

    // depth < d: descend one nibble
    let nib = nibble_at(prefix_key, depth);
    let here_br = ensure_branch_at_depth(node);

    match &here_br {
        Node::Empty => {
            let child = graft_rec(&Node::Empty, depth + 1, d, r, prefix_key, sub_root);
            if child.is_empty() {
                return Node::Empty;
            }
            let bitmap = 1u16 << (nib as u16);
            let mut children: [Node<V>; 16] = Default::default();
            children[nib as usize] = child;
            make_branch(bitmap, children)
        }
        Node::Branch(here_data) => {
            let old_child = &here_data.children[nib as usize];
            let new_child = graft_rec(old_child, depth + 1, d, r, prefix_key, sub_root);
            rebuild_branch_with_child(here_data, nib, new_child)
        }
        _ => {
            let child = graft_rec(&Node::Empty, depth + 1, d, r, prefix_key, sub_root);
            if child.is_empty() {
                return here_br;
            }
            let bitmap = 1u16 << (nib as u16);
            let mut children: [Node<V>; 16] = Default::default();
            children[nib as usize] = child;
            make_branch(bitmap, children)
        }
    }
}

/// Split a trie: extract the prefix-subtree and return (without_prefix, with_prefix).
pub fn split<V: MultiMapValue>(
    root: &Node<V>,
    #[allow(unused)] root_path: MapKeyPath,
    key_path: MapKeyPath,
) -> (Node<V>, Node<V>) {
    let sub_root = subtree_root_for_view(root, key_path);
    let pruned = prune_prefix(root, key_path);

    let sub_root = canonicalize(&sub_root);
    let pruned = canonicalize(&pruned);

    (pruned, sub_root)
}

/// Merge: graft `b` (with its path) back into `a`.
pub fn merge<V: MultiMapValue>(
    a: &Node<V>,
    #[allow(unused)] a_path: MapKeyPath,
    b: &Node<V>,
    b_path: MapKeyPath,
) -> Node<V> {
    let merged = graft_prefix(a, b, b_path);
    canonicalize(&merged)
}
