use crate::node::LeafDataAug;
use crate::node::NodeAug;
use crate::ops::boundary_mask;
use crate::ops::nibble_at;
use crate::ops::nibble_from_key;
use crate::ops::prefix_bits_match;
use crate::MapKey;
use crate::MapKeyPath;
use crate::MultiMapAugmentation;
use crate::MultiMapValue;

/// Look up a key in the trie. Returns `Some(value)` if found, `None` otherwise.
pub fn get<V: MultiMapValue, A: MultiMapAugmentation>(
    root: &NodeAug<V, A>,
    root_path: &MapKeyPath,
    key: &MapKey,
) -> Option<V> {
    get_leaf(root, root_path, key).map(|leaf| leaf.value.clone())
}

/// Look up the leaf, including its derived augmentation, without cloning it.
pub fn get_leaf<'a, V: MultiMapValue, A: MultiMapAugmentation>(
    root: &'a NodeAug<V, A>,
    root_path: &MapKeyPath,
    key: &MapKey,
) -> Option<&'a LeafDataAug<V, A>> {
    if root_path.len > 0 && !prefix_bits_match(&root_path.prefix.0, &key.0, root_path.len) {
        return None;
    }

    let start = (root_path.len / 4) as usize;
    let r = (root_path.len % 4) as usize;

    if r != 0 {
        let mask = boundary_mask(r);
        let key_nib = nibble_from_key(&key.0, start);
        let prefix_nib = nibble_from_key(&root_path.prefix.0, start);
        if (key_nib & mask) != (prefix_nib & mask) {
            return None;
        }
    }

    let mut current: &NodeAug<V, A> = root;
    if current.is_empty() {
        return None;
    }
    let mut depth = start;

    while depth < 64 {
        match current {
            NodeAug::Empty => return None,
            NodeAug::Branch(data) => {
                let nib = nibble_at(&key.0, depth);
                let child = &data.children[nib as usize];
                if child.is_empty() {
                    return None;
                }
                current = child;
                depth += 1;
            }
            NodeAug::Ext(data) => {
                let count = data.nibble_count as usize;
                if depth + count > 64 {
                    return None;
                }
                for (i, n) in data.nibbles.iter().enumerate().take(count) {
                    if *n != nibble_at(&key.0, depth + i) {
                        return None;
                    }
                }
                depth += count;
                current = &data.child;
            }
            NodeAug::Leaf(_) => return None,
        }
    }

    match current {
        NodeAug::Leaf(data) => Some(data),
        _ => None,
    }
}
