use super::boundary_mask;
use super::ensure_branch_at_depth;
use super::make_branch;
use super::nibble_at;
use super::nibble_from_key;
use super::node_ptr_eq;
use super::prefix_bits_match;
use super::rebuild_branch_with_child;
use crate::node::Node;
use crate::MapKey;
use crate::MapKeyPath;
use crate::MultiMapValue;

struct Update<V> {
    key: MapKey,
    val: Option<V>,
    seq: u32,
}

fn normalize_updates<V: MultiMapValue>(
    root_path: &MapKeyPath,
    updates: &[(MapKey, Option<V>)],
) -> Vec<Update<V>> {
    let start = (root_path.len as usize) / 4;
    let r = (root_path.len as usize) % 4;

    let mut out: Vec<Update<V>> = Vec::with_capacity(updates.len());

    for (seq, (k, v)) in updates.iter().enumerate() {
        if root_path.len > 0 && !prefix_bits_match(&root_path.prefix.0, &k.0, root_path.len) {
            continue;
        }
        if r != 0 {
            let mask = boundary_mask(r);
            let pn = nibble_from_key(&root_path.prefix.0, start) & mask;
            let kn = nibble_from_key(&k.0, start) & mask;
            if pn != kn {
                continue;
            }
        }
        out.push(Update { key: *k, val: v.clone(), seq: seq as u32 });
    }

    if out.is_empty() {
        return out;
    }

    out.sort_unstable_by(|a, b| {
        let kc = a.key.0.cmp(&b.key.0);
        if kc != std::cmp::Ordering::Equal {
            kc
        } else {
            a.seq.cmp(&b.seq)
        }
    });

    let mut w = 0usize;
    let mut i = 0usize;
    while i < out.len() {
        let mut j = i + 1;
        while j < out.len() && out[j].key.0 == out[i].key.0 {
            j += 1;
        }
        let last = j - 1;
        if w != last {
            out.swap(w, last);
        }
        w += 1;
        i = j;
    }
    out.truncate(w);

    out.sort_unstable_by_key(|a| a.key.0);
    out
}

fn split_by_nibble<V>(items: &[Update<V>], depth: usize) -> [(usize, usize); 16] {
    let mut ranges = [(0usize, 0usize); 16];
    let mut i = 0usize;

    while i < items.len() {
        let nib = nibble_at(&items[i].key.0, depth) as usize;
        let start = i;
        i += 1;
        while i < items.len() && nibble_at(&items[i].key.0, depth) as usize == nib {
            i += 1;
        }
        ranges[nib] = (start, i);
    }
    ranges
}

/// Build a fresh trie from sorted, deduped updates.
fn build_from_updates<V: MultiMapValue>(depth: usize, items: &mut [Update<V>]) -> Node<V> {
    if items.is_empty() {
        return Node::Empty;
    }
    if depth == 64 {
        debug_assert_eq!(items.len(), 1);
        return items[0].val.take().map_or(Node::Empty, Node::new_leaf);
    }

    let ranges = split_by_nibble(items, depth);

    let mut children: [Node<V>; 16] = Default::default();
    let mut bitmap: u16 = 0;

    for nib in 0..16usize {
        let (a, b) = ranges[nib];
        if a == b {
            continue;
        }
        let child = build_from_updates(depth + 1, &mut items[a..b]);
        if !child.is_empty() {
            bitmap |= 1u16 << (nib as u16);
            children[nib] = child;
        }
    }

    if bitmap == 0 {
        Node::Empty
    } else {
        make_branch(bitmap, children)
    }
}

/// COW merge: apply updates to existing trie.
fn apply_range<V: MultiMapValue>(old: &Node<V>, depth: usize, items: &mut [Update<V>]) -> Node<V> {
    if items.is_empty() {
        return old.clone();
    }
    if depth == 64 {
        debug_assert_eq!(items.len(), 1);
        return items[0].val.take().map_or(Node::Empty, Node::new_leaf);
    }

    if old.is_empty() {
        return build_from_updates(depth, items);
    }

    let br_ref = ensure_branch_at_depth(old);
    let br_data = match &br_ref {
        Node::Branch(data) => data,
        _ => return build_from_updates(depth, items),
    };

    let ranges = split_by_nibble(items, depth);

    // Fast path: if only one nibble is touched, avoid cloning all 16 children
    let mut touched_nib = 255u8;
    let mut touched_count = 0u8;
    for nib in 0..16u8 {
        let (a, b) = ranges[nib as usize];
        if a != b {
            touched_nib = nib;
            touched_count += 1;
            if touched_count > 1 {
                break;
            }
        }
    }

    if touched_count == 1 {
        let nib = touched_nib;
        let (a, b) = ranges[nib as usize];
        let new_c = apply_range(&br_data.children[nib as usize], depth + 1, &mut items[a..b]);
        if node_ptr_eq(&br_data.children[nib as usize], &new_c) {
            return br_ref;
        }
        return rebuild_branch_with_child(br_data, nib, new_c);
    }

    // Multi-nibble path: clone all children
    let mut nib_to_child: [Node<V>; 16] = br_data.children.clone();
    let mut changed = false;

    for nib in 0..16usize {
        let (a, b) = ranges[nib];
        if a == b {
            continue;
        }
        let new_c = apply_range(&nib_to_child[nib], depth + 1, &mut items[a..b]);
        if !node_ptr_eq(&nib_to_child[nib], &new_c) {
            nib_to_child[nib] = new_c;
            changed = true;
        }
    }

    if !changed {
        return br_ref;
    }

    let mut bitmap: u16 = 0;
    for nib in 0..16u16 {
        if !nib_to_child[nib as usize].is_empty() {
            bitmap |= 1u16 << nib;
        }
    }

    if bitmap == 0 {
        Node::Empty
    } else {
        make_branch(bitmap, nib_to_child)
    }
}

/// Public entry point: apply a batch of inserts/deletes to a trie.
pub fn update<V: MultiMapValue>(
    root: &Node<V>,
    root_path: &MapKeyPath,
    updates: &[(MapKey, Option<V>)],
) -> Node<V> {
    let mut items = normalize_updates(root_path, updates);
    if items.is_empty() {
        return root.clone();
    }

    let start = (root_path.len as usize) / 4;
    apply_range(root, start, &mut items)
}

/// Fast path for inserting or replacing a single key-value pair.
/// Skips normalize_updates, sort, dedup, split_by_nibble — just walks the trie directly.
pub fn update_single<V: MultiMapValue>(
    root: &Node<V>,
    root_path: &MapKeyPath,
    key: &MapKey,
    value: Option<V>,
) -> Node<V> {
    let start = (root_path.len as usize) / 4;
    apply_single(root, start, key, value)
}

/// COW single-key update: walk directly by nibble, no sorting or range splitting.
fn apply_single<V: MultiMapValue>(
    old: &Node<V>,
    depth: usize,
    key: &MapKey,
    value: Option<V>,
) -> Node<V> {
    if depth == 64 {
        return value.map_or(Node::Empty, Node::new_leaf);
    }

    let nib = nibble_at(&key.0, depth);

    match old {
        Node::Empty => {
            // Insert into empty: build a path of ext + leaf
            match value {
                None => Node::Empty,
                Some(v) => {
                    let leaf = Node::new_leaf(v);
                    // Remaining nibbles after consuming `nib` at `depth`: depth+1 .. 63
                    let remaining = 63 - depth; // number of nibbles for the ext
                    if remaining == 0 {
                        // We're at depth 63, just one nibble branches to the leaf
                        let mut children: [Node<V>; 16] = Default::default();
                        children[nib as usize] = leaf;
                        make_branch(1u16 << nib, children)
                    } else {
                        // Build ext covering nibbles [depth+1 .. 63], child is leaf
                        let mut nibbles = [0u8; 64];
                        #[allow(clippy::needless_range_loop)]
                        for i in 0..remaining {
                            nibbles[i] = nibble_at(&key.0, depth + 1 + i);
                        }
                        let ext_child = Node::new_ext(&nibbles[..remaining], leaf);
                        let mut children: [Node<V>; 16] = Default::default();
                        children[nib as usize] = ext_child;
                        make_branch(1u16 << nib, children)
                    }
                }
            }
        }

        Node::Leaf(_) => {
            // Leaf at non-max depth shouldn't happen in a well-formed trie
            // Fall back to batch update
            let updates = [(*key, value)];
            let mut items =
                normalize_updates(&MapKeyPath { prefix: MapKey([0; 32]), len: 0 }, &updates);
            apply_range(old, depth, &mut items)
        }

        Node::Ext(data) => {
            let ext_count = data.nibble_count as usize;
            // Check if our key matches the ext prefix
            let mut match_len = 0;
            while match_len < ext_count {
                if data.nibbles[match_len] != nibble_at(&key.0, depth + match_len) {
                    break;
                }
                match_len += 1;
            }

            if match_len == ext_count {
                // Full match: recurse into child
                let new_child = apply_single(&data.child, depth + ext_count, key, value);
                if node_ptr_eq(&data.child, &new_child) {
                    return old.clone();
                }
                if new_child.is_empty() {
                    return Node::Empty;
                }
                return Node::new_ext(&data.nibbles[..ext_count], new_child);
            }

            // Partial match: need to split the ext
            // Fall back to batch update for this case (rare in practice for updates on existing keys)
            let updates = [(*key, value)];
            let mut items =
                normalize_updates(&MapKeyPath { prefix: MapKey([0; 32]), len: 0 }, &updates);
            apply_range(old, depth, &mut items)
        }

        Node::Branch(br_data) => {
            let old_child = &br_data.children[nib as usize];
            let new_child = apply_single(old_child, depth + 1, key, value);
            if node_ptr_eq(old_child, &new_child) {
                return old.clone();
            }
            rebuild_branch_with_child(br_data, nib, new_child)
        }
    }
}
