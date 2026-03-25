use trie_map::trie::arena::boundary_mask;
use trie_map::trie::arena::nibble_at;
use trie_map::trie::arena::nibble_from_key;
use trie_map::trie::arena::prefix_bits_match;
use trie_map::MapKey;
use trie_map::MapKeyPath;

use crate::node::Node;
use crate::MultiMapValue;

/// Look up a key in the trie. Returns `Some(value)` if found, `None` otherwise.
pub fn get<V: MultiMapValue>(root: &Node<V>, root_path: &MapKeyPath, key: &MapKey) -> Option<V> {
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

    let mut current: &Node<V> = root;
    if current.is_empty() {
        return None;
    }
    let mut depth = start;

    while depth < 64 {
        match current {
            Node::Empty => return None,
            Node::Branch(data) => {
                let nib = nibble_at(&key.0, depth);
                let child = &data.children[nib as usize];
                if child.is_empty() {
                    return None;
                }
                current = child;
                depth += 1;
            }
            Node::Ext(data) => {
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
            Node::Leaf(_) => return None,
        }
    }

    match current {
        Node::Leaf(data) => Some(data.value.clone()),
        _ => None,
    }
}
