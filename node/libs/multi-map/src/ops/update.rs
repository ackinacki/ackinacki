use trie_map::trie::arena::boundary_mask;
use trie_map::trie::arena::nibble_at;
use trie_map::trie::arena::nibble_from_key;
use trie_map::trie::arena::prefix_bits_match;
use trie_map::MapKey;
use trie_map::MapKeyPath;

use super::ensure_branch_at_depth;
use super::make_branch;
use super::node_ptr_eq;
use super::rebuild_branch_with_child;
use crate::node::Node;
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

    out.sort_unstable_by(|a, b| a.key.0.cmp(&b.key.0));
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
