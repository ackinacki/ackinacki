use super::boundary_mask;
use super::ensure_branch_at_depth;
use super::make_branch;
use super::merge_ext_if_possible;
use super::nibble_at;
use super::nibble_from_key;
use super::node_ptr_eq;
use super::prefix_bits_match;
use super::rebuild_branch_with_child;
use crate::node::NodeAug;
use crate::MapKeyPath;
use crate::MultiMapAugmentation;
use crate::MultiMapValue;

/// Normalize trie structure: collapse single-child branches, merge adjacent `Ext`s, remove empties.
/// Uses hash-based short-circuit: if a node's hash is non-zero (i.e., it was properly hashed
/// during construction, not created by `new_branch_unhashed`/`new_ext_unhashed`), its subtree
/// is already canonical and can be returned as-is.
fn canonicalize<V: MultiMapValue, A: MultiMapAugmentation>(node: &NodeAug<V, A>) -> NodeAug<V, A> {
    match node {
        NodeAug::Empty => NodeAug::Empty,

        NodeAug::Leaf(_) => node.clone(),

        NodeAug::Ext(data) => {
            // If this ext was properly constructed (non-zero hash), it and its
            // subtree are already canonical — skip the recursive walk.
            if data.hash != [0u8; 32] {
                return node.clone();
            }
            let child2 = canonicalize(&data.child);
            match child2 {
                NodeAug::Empty => NodeAug::Empty,
                c => {
                    let count = data.nibble_count as usize;
                    let ext = NodeAug::new_ext(&data.nibbles[..count], c);
                    merge_ext_if_possible(ext)
                }
            }
        }

        NodeAug::Branch(data) => {
            // If this branch was properly constructed (non-zero hash), it and its
            // subtree are already canonical — skip the recursive walk.
            if data.hash != [0u8; 32] {
                return node.clone();
            }
            let mut out: [NodeAug<V, A>; 16] = Default::default();
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
                NodeAug::Empty
            } else {
                make_branch(out_bitmap, out)
            }
        }
    }
}

/// Find the subtree root for a given bit-prefix path inside the current root-path view.
fn subtree_root_for_view<V: MultiMapValue, A: MultiMapAugmentation>(
    root: &NodeAug<V, A>,
    path: MapKeyPath,
    start_depth: usize,
) -> NodeAug<V, A> {
    let d = (path.len as usize) / 4;
    let r = (path.len as usize) % 4;

    let mut current = root.clone();
    for depth in start_depth..d {
        let br = ensure_branch_at_depth(&current);
        let br_data = match &br {
            NodeAug::Branch(data) => data,
            _ => return NodeAug::Empty,
        };
        let nib = nibble_at(&path.prefix.0, depth);
        current = br_data.children[nib as usize].clone();
        if current.is_empty() {
            return NodeAug::Empty;
        }
    }

    if r == 0 {
        return current;
    }

    // Non-nibble-aligned boundary
    let br = ensure_branch_at_depth(&current);
    let br_data = match &br {
        NodeAug::Branch(data) => data,
        _ => return NodeAug::Empty,
    };

    let mask = boundary_mask(r);
    let want = nibble_from_key(&path.prefix.0, d) & mask;

    let mut out: [NodeAug<V, A>; 16] = Default::default();
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
        NodeAug::Empty
    } else {
        make_branch(out_bitmap, out)
    }
}

/// Remove the prefix subtree from root (COW).
fn prune_prefix_from_view<V: MultiMapValue, A: MultiMapAugmentation>(
    root: &NodeAug<V, A>,
    root_path: MapKeyPath,
    path: MapKeyPath,
) -> NodeAug<V, A> {
    let d = (path.len as usize) / 4;
    let r = (path.len as usize) % 4;
    let start_depth = (root_path.len as usize) / 4;
    prune_rec(root, start_depth, d, r, &path.prefix.0)
}

fn prune_rec<V: MultiMapValue, A: MultiMapAugmentation>(
    node: &NodeAug<V, A>,
    depth: usize,
    d: usize,
    r: usize,
    prefix_key: &[u8; 32],
) -> NodeAug<V, A> {
    if node.is_empty() {
        return NodeAug::Empty;
    }
    if depth > d {
        return node.clone();
    }

    // Nibble-aligned prefix boundary reached: the whole subtree rooted here
    // matches the prefix and must be removed. This is handled BEFORE
    // materializing a branch because at full key depth (`d == 64`) `node` is a
    // bare `Leaf`; `ensure_branch_at_depth` passes a `Leaf` through unchanged
    // and the `match ... { _ => return node.clone() }` below would then retain
    // it — leaving the pruned key live on the "without" side and duplicating it
    // across the split (a full-depth `len == 256` split of an L1 dapp leaf
    // duplicates that dapp's entire account submap into both threads).
    if depth == d && r == 0 {
        return NodeAug::Empty;
    }

    let br = ensure_branch_at_depth(node);
    let br_data = match &br {
        NodeAug::Branch(data) => data,
        _ => return node.clone(),
    };

    if depth < d {
        let nib = nibble_at(prefix_key, depth);
        if br_data.children[nib as usize].is_empty() {
            // `br` can be a temporary unhashed view of an extension.
            // A missing prefix must leave the original subtree unchanged.
            return node.clone();
        }
        let new_child = prune_rec(&br_data.children[nib as usize], depth + 1, d, r, prefix_key);
        if node_ptr_eq(&br_data.children[nib as usize], &new_child) {
            return node.clone();
        }
        return rebuild_branch_with_child(br_data, nib, new_child);
    }

    // depth == d with a sub-nibble boundary (r != 0). The r == 0 case is
    // handled above.
    let mask = boundary_mask(r);
    let want = nibble_from_key(prefix_key, depth) & mask;

    let mut out: [NodeAug<V, A>; 16] = Default::default();
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
        NodeAug::Empty
    } else {
        make_branch(out_bitmap, out)
    }
}

/// Graft a subtree back into root at the given prefix position (COW).
fn graft_prefix<V: MultiMapValue, A: MultiMapAugmentation>(
    root: &NodeAug<V, A>,
    root_path: MapKeyPath,
    sub_root: &NodeAug<V, A>,
    sub_path: MapKeyPath,
) -> NodeAug<V, A> {
    let common_len = root_path.len.min(sub_path.len);
    if common_len > 0 && !prefix_bits_match(&root_path.prefix.0, &sub_path.prefix.0, common_len) {
        return root.clone();
    }
    if sub_path.len < root_path.len {
        // The replacement is rooted above this view. Restrict it to the
        // receiver's scope rather than reinterpreting its nodes at a new depth.
        return subtree_root_for_view(sub_root, root_path, (sub_path.len / 4) as usize);
    }
    if sub_path.len == root_path.len {
        return sub_root.clone();
    }

    let d = (sub_path.len as usize) / 4;
    let r = (sub_path.len as usize) % 4;
    let start_depth = (root_path.len as usize) / 4;
    graft_rec(root, start_depth, d, r, &sub_path.prefix.0, sub_root)
}

fn graft_rec<V: MultiMapValue, A: MultiMapAugmentation>(
    node: &NodeAug<V, A>,
    depth: usize,
    d: usize,
    r: usize,
    prefix_key: &[u8; 32],
    sub_root: &NodeAug<V, A>,
) -> NodeAug<V, A> {
    if depth == d {
        if r == 0 {
            return sub_root.clone();
        }

        // Sub-nibble boundary: merge bitmaps
        let base_br = ensure_branch_at_depth(node);
        let sub_br = ensure_branch_at_depth(sub_root);

        let sub_data = match &sub_br {
            NodeAug::Branch(data) => data,
            _ => return node.clone(),
        };

        // Start with existing children
        let mut nib_to_child: [NodeAug<V, A>; 16] = Default::default();

        if let NodeAug::Branch(base_data) = &base_br {
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

        return if out_bitmap == 0 {
            NodeAug::Empty
        } else {
            make_branch(out_bitmap, nib_to_child)
        };
    }

    // depth < d: descend one nibble
    let nib = nibble_at(prefix_key, depth);
    let here_br = ensure_branch_at_depth(node);

    match &here_br {
        NodeAug::Empty => {
            let child = graft_rec(&NodeAug::Empty, depth + 1, d, r, prefix_key, sub_root);
            if child.is_empty() {
                return NodeAug::Empty;
            }
            let bitmap = 1u16 << (nib as u16);
            let mut children: [NodeAug<V, A>; 16] = Default::default();
            children[nib as usize] = child;
            make_branch(bitmap, children)
        }
        NodeAug::Branch(here_data) => {
            let old_child = &here_data.children[nib as usize];
            let new_child = graft_rec(old_child, depth + 1, d, r, prefix_key, sub_root);
            if node_ptr_eq(old_child, &new_child) {
                return node.clone();
            }
            rebuild_branch_with_child(here_data, nib, new_child)
        }
        _ => {
            let child = graft_rec(&NodeAug::Empty, depth + 1, d, r, prefix_key, sub_root);
            if child.is_empty() {
                return here_br;
            }
            let bitmap = 1u16 << (nib as u16);
            let mut children: [NodeAug<V, A>; 16] = Default::default();
            children[nib as usize] = child;
            make_branch(bitmap, children)
        }
    }
}

/// Split a trie: extract the prefix-subtree and return (without_prefix, with_prefix).
pub fn split<V: MultiMapValue, A: MultiMapAugmentation>(
    root: &NodeAug<V, A>,
    root_path: MapKeyPath,
    key_path: MapKeyPath,
) -> (NodeAug<V, A>, NodeAug<V, A>) {
    let common_len = root_path.len.min(key_path.len);
    if common_len > 0 && !prefix_bits_match(&root_path.prefix.0, &key_path.prefix.0, common_len) {
        return (canonicalize(root), NodeAug::Empty);
    }

    if key_path.len <= root_path.len {
        let sub_root = graft_prefix(&NodeAug::Empty, key_path, root, root_path);
        return (NodeAug::Empty, canonicalize(&sub_root));
    }

    let start_depth = (root_path.len as usize) / 4;
    let sub_root = subtree_root_for_view(root, key_path, start_depth);
    let pruned = prune_prefix_from_view(root, root_path, key_path);

    let sub_root = canonicalize(&sub_root);
    let pruned = canonicalize(&pruned);

    (pruned, sub_root)
}

/// Graft `b` back into `a` at `b`'s prefix path.
///
/// This is the *inverse of split*: if `(a', b) = split(a, b_path)`, then
/// `graft(a', _, b, b_path) == a`. It overwrites whatever `a` had at the
/// `b_path` position with `b`'s root. This is NOT a union of two tries —
/// see the note at `b_path.len == 0` below.
///
/// Pre-condition: `a` must have been pruned at `b_path` (i.e. no live
/// leaves under that prefix). If `a` already has data under `b_path`,
/// those entries are lost. In particular, calling `graft(a, _, b, EMPTY)`
/// with two full L1 roots replaces `a` with `b` entirely (`graft_rec`
/// returns `sub_root.clone()` when `depth == d == 0` and `r == 0`). For
/// combining two overlapping tries use a true union, not this function.
pub fn graft<V: MultiMapValue, A: MultiMapAugmentation>(
    a: &NodeAug<V, A>,
    a_path: MapKeyPath,
    b: &NodeAug<V, A>,
    b_path: MapKeyPath,
) -> NodeAug<V, A> {
    let grafted = graft_prefix(a, a_path, b, b_path);
    canonicalize(&grafted)
}

/// Graft multiple subtrees into `a` in a single pass.
/// Avoids the O(n²) cost of repeated graft and canonicalize calls.
///
/// Same pre-condition as [`graft`]: each subtree's prefix must be a
/// pruned slot in `a`. Conflicting entries are overwritten (the later
/// subtree wins for any overlap), so order matters when the inputs are
/// not disjoint by construction.
pub fn graft_batch<V: MultiMapValue, A: MultiMapAugmentation>(
    a: &NodeAug<V, A>,
    a_path: MapKeyPath,
    subtrees: &[(NodeAug<V, A>, MapKeyPath)],
) -> NodeAug<V, A> {
    if let Some(assembled) = assemble_disjoint_nibble_subtrees(a, a_path, subtrees) {
        return assembled;
    }

    let mut grafted = a.clone();
    for (b, b_path) in subtrees {
        grafted = graft_prefix(&grafted, a_path, b, *b_path);
    }
    canonicalize(&grafted)
}

/// Build a canonical trie directly from disjoint, nibble-aligned subtrees.
///
/// Partition assembly starts from an empty map and already has these stronger
/// invariants. Building the trie bottom-up avoids repeatedly walking and
/// rebuilding the shared prefixes of every subtree. Inputs outside this fast
/// path fall back to the general later-wins graft implementation above.
fn assemble_disjoint_nibble_subtrees<V: MultiMapValue, A: MultiMapAugmentation>(
    a: &NodeAug<V, A>,
    a_path: MapKeyPath,
    subtrees: &[(NodeAug<V, A>, MapKeyPath)],
) -> Option<NodeAug<V, A>> {
    if !a.is_empty() || !a_path.len.is_multiple_of(4) || a_path.len > 256 {
        return None;
    }

    let mut ordered = Vec::with_capacity(subtrees.len());
    for (root, path) in subtrees {
        if root.is_empty()
            || root.hash() == [0; 32]
            || path.len % 4 != 0
            || path.len > 256
            || path.len < a_path.len
            || !prefix_bits_match(&a_path.prefix.0, &path.prefix.0, a_path.len)
        {
            return None;
        }
        ordered.push((root, *path));
    }

    if ordered.is_empty() {
        return Some(NodeAug::Empty);
    }

    ordered.sort_unstable_by(|(_, left), (_, right)| compare_nibble_paths(*left, *right));
    for pair in ordered.windows(2) {
        let left = pair[0].1;
        let right = pair[1].1;
        if left.len <= right.len && prefix_bits_match(&left.prefix.0, &right.prefix.0, left.len) {
            return None;
        }
    }

    Some(assemble_sorted_subtrees(&ordered, (a_path.len / 4) as usize))
}

fn compare_nibble_paths(left: MapKeyPath, right: MapKeyPath) -> std::cmp::Ordering {
    let left_depth = (left.len / 4) as usize;
    let right_depth = (right.len / 4) as usize;
    for depth in 0..left_depth.min(right_depth) {
        match nibble_at(&left.prefix.0, depth).cmp(&nibble_at(&right.prefix.0, depth)) {
            std::cmp::Ordering::Equal => {}
            ordering => return ordering,
        }
    }
    left_depth.cmp(&right_depth)
}

fn assemble_sorted_subtrees<V: MultiMapValue, A: MultiMapAugmentation>(
    subtrees: &[(&NodeAug<V, A>, MapKeyPath)],
    depth: usize,
) -> NodeAug<V, A> {
    debug_assert!(!subtrees.is_empty());

    if subtrees.len() == 1 {
        let (root, path) = subtrees[0];
        let end = (path.len / 4) as usize;
        if end == depth {
            return root.clone();
        }
        let mut nibbles = [0u8; 64];
        for (slot, nibble_depth) in nibbles.iter_mut().zip(depth..end) {
            *slot = nibble_at(&path.prefix.0, nibble_depth);
        }
        return merge_ext_if_possible(NodeAug::new_ext(&nibbles[..end - depth], root.clone()));
    }

    let minimum_end =
        subtrees.iter().map(|(_, path)| (path.len / 4) as usize).min().expect("non-empty slice");
    let first_path = subtrees[0].1;
    let mut common_end = depth;
    while common_end < minimum_end {
        let nibble = nibble_at(&first_path.prefix.0, common_end);
        if subtrees.iter().skip(1).any(|(_, path)| nibble_at(&path.prefix.0, common_end) != nibble)
        {
            break;
        }
        common_end += 1;
    }

    if common_end > depth {
        let child = assemble_sorted_subtrees(subtrees, common_end);
        let mut nibbles = [0u8; 64];
        for (slot, nibble_depth) in nibbles.iter_mut().zip(depth..common_end) {
            *slot = nibble_at(&first_path.prefix.0, nibble_depth);
        }
        return merge_ext_if_possible(NodeAug::new_ext(&nibbles[..common_end - depth], child));
    }

    let mut children: [NodeAug<V, A>; 16] = Default::default();
    let mut bitmap = 0u16;
    let mut start = 0usize;
    while start < subtrees.len() {
        let nibble = nibble_at(&subtrees[start].1.prefix.0, depth);
        let mut end = start + 1;
        while end < subtrees.len() && nibble_at(&subtrees[end].1.prefix.0, depth) == nibble {
            end += 1;
        }
        children[nibble as usize] = assemble_sorted_subtrees(&subtrees[start..end], depth + 1);
        bitmap |= 1u16 << nibble;
        start = end;
    }
    make_branch(bitmap, children)
}
