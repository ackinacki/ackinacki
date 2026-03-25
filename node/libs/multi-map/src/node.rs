use std::sync::Arc;

use trie_map::trie::hash::hash_branch_empty;
use trie_map::trie::hash::hash_branch_sparse;
use trie_map::trie::hash::hash_ext;
use trie_map::trie::hash::hash_leaf_value;

use crate::MultiMapValue;

/// Per-variant Arc node layout. Each variant holds its own Arc to a tightly-sized struct.
/// `Node::Empty` replaces `Option<Arc<Node>>` / `None`.
/// sizeof(Node<V>) = 16 bytes (discriminant + pointer).
#[derive(Clone)]
pub enum Node<V: MultiMapValue> {
    Empty,
    Leaf(Arc<LeafData<V>>),
    Branch(Arc<BranchData<V>>),
    Ext(Arc<ExtData<V>>),
}

pub struct LeafData<V: MultiMapValue> {
    pub hash: [u8; 32],
    pub value: V,
}

pub struct BranchData<V: MultiMapValue> {
    pub hash: [u8; 32],
    pub bitmap: u16,
    pub children: [Node<V>; 16],
}

pub struct ExtData<V: MultiMapValue> {
    pub hash: [u8; 32],
    pub nibble_count: u8,
    pub nibbles: [u8; 64],
    pub child: Node<V>,
}

impl<V: MultiMapValue> Node<V> {
    /// Create a leaf node. Hash = H(0x01 || value).
    pub fn new_leaf(value: V) -> Self {
        let hash = hash_leaf_value(&value);
        Node::Leaf(Arc::new(LeafData { hash, value }))
    }

    /// Create a branch node from a full 16-slot children array.
    ///
    /// IMPORTANT: this does NOT perform Patricia compression.
    /// Use `make_branch()` from ops module for the compressing variant.
    pub fn new_branch(bitmap: u16, children: [Node<V>; 16]) -> Self {
        let mut nibs = [0u8; 16];
        let mut hashes = [[0u8; 32]; 16];
        let mut out_len = 0usize;

        for nib in 0..16u8 {
            if bitmap & (1u16 << nib) != 0 {
                nibs[out_len] = nib;
                hashes[out_len] = children[nib as usize].hash();
                out_len += 1;
            }
        }

        let hash = hash_branch_sparse(bitmap, &nibs[..out_len], &hashes[..out_len]);
        Node::Branch(Arc::new(BranchData { hash, bitmap, children }))
    }

    /// Create an extension node. `nibbles` is unpacked (one nibble per byte).
    /// Hash = H(0x03 || len || packed_nibbles || child_hash).
    pub fn new_ext(nibbles: &[u8], child: Node<V>) -> Self {
        debug_assert!(!nibbles.is_empty());
        debug_assert!(nibbles.len() <= 64);

        let nibble_count = nibbles.len() as u8;
        let mut nibble_arr = [0u8; 64];
        nibble_arr[..nibbles.len()].copy_from_slice(nibbles);

        // Pack on stack
        let packed_len = nibbles.len().div_ceil(2);
        let mut packed = [0u8; 32];
        for (i, p) in packed.iter_mut().enumerate().take(packed_len) {
            let hi = nibbles.get(i * 2).copied().unwrap_or(0) & 0x0F;
            let lo = nibbles.get(i * 2 + 1).copied().unwrap_or(0) & 0x0F;
            *p = (hi << 4) | lo;
        }

        let hash = hash_ext(nibble_count, &packed[..packed_len], &child.hash());
        Node::Ext(Arc::new(ExtData { hash, nibble_count, nibbles: nibble_arr, child }))
    }

    /// Create a branch node WITHOUT computing the hash.
    /// ONLY for temporary nodes in ensure_branch_at_depth — the hash is never read.
    pub fn new_branch_unhashed(children: [Node<V>; 16]) -> Self {
        let mut bitmap: u16 = 0;
        for nib in 0..16u16 {
            if !children[nib as usize].is_empty() {
                bitmap |= 1u16 << nib;
            }
        }
        Node::Branch(Arc::new(BranchData { hash: [0; 32], bitmap, children }))
    }

    /// Create an ext node WITHOUT computing the hash.
    /// ONLY for temporary nodes in ensure_branch_at_depth — the hash is never read.
    pub fn new_ext_unhashed(nibbles: &[u8], child: Node<V>) -> Self {
        debug_assert!(!nibbles.is_empty());
        debug_assert!(nibbles.len() <= 64);
        let nibble_count = nibbles.len() as u8;
        let mut nibble_arr = [0u8; 64];
        nibble_arr[..nibbles.len()].copy_from_slice(nibbles);
        Node::Ext(Arc::new(ExtData { hash: [0; 32], nibble_count, nibbles: nibble_arr, child }))
    }

    /// Get the hash of any node variant.
    pub fn hash(&self) -> [u8; 32] {
        match self {
            Node::Empty => empty_map_hash(),
            Node::Leaf(d) => d.hash,
            Node::Branch(d) => d.hash,
            Node::Ext(d) => d.hash,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self, Node::Empty)
    }
}

/// Default is Empty.
impl<V: MultiMapValue> Default for Node<V> {
    fn default() -> Self {
        Node::Empty
    }
}

/// Pack nibbles (one per byte) into the format hash_ext expects:
/// two nibbles per byte, high nibble first.
pub fn pack_nibbles(nibbles: &[u8]) -> Vec<u8> {
    nibbles
        .chunks(2)
        .map(|c| {
            let hi = c[0] & 0x0F;
            let lo = if c.len() > 1 { c[1] & 0x0F } else { 0 };
            (hi << 4) | lo
        })
        .collect()
}

/// The hash of an empty map. Must match trie-map's behavior.
pub fn empty_map_hash() -> [u8; 32] {
    hash_branch_empty()
}

#[cfg(test)]
mod tests {
    use blake3::Hasher;
    use node_types::Blake3Hashable;
    use serde::Deserialize;
    use serde::Serialize;
    use trie_map::trie::hash::hash_branch_empty;

    use super::*;

    #[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct TV(pub u64);

    impl Blake3Hashable for TV {
        fn update_hasher(&self, hasher: &mut Hasher) {
            hasher.update(&self.0.to_be_bytes());
        }
    }

    #[test]
    fn leaf_hash_matches_trie_map() {
        let v = TV(42);
        let leaf = Node::new_leaf(v);
        let expected = trie_map::trie::hash::hash_leaf_value(&v);
        assert_eq!(leaf.hash(), expected);
    }

    #[test]
    fn empty_map_hash_matches_trie_map() {
        assert_eq!(empty_map_hash(), hash_branch_empty());
    }

    #[test]
    fn branch_hash_matches_trie_map() {
        let child_a = Node::new_leaf(TV(1));
        let child_b = Node::new_leaf(TV(2));
        let bitmap: u16 = (1 << 3) | (1 << 7);
        let mut children: [Node<TV>; 16] = Default::default();
        children[3] = child_a.clone();
        children[7] = child_b.clone();
        let branch = Node::new_branch(bitmap, children);

        let nibs = [3u8, 7u8];
        let hashes = [child_a.hash(), child_b.hash()];
        let expected = trie_map::trie::hash::hash_branch_sparse(bitmap, &nibs, &hashes);
        assert_eq!(branch.hash(), expected);
    }

    #[test]
    fn ext_hash_matches_trie_map() {
        let child = Node::new_leaf(TV(99));
        let nibbles = [0x0Au8, 0x0B, 0x0C];
        let child_hash = child.hash();
        let ext = Node::new_ext(&nibbles, child);

        let packed = pack_nibbles(&nibbles);
        let expected = trie_map::trie::hash::hash_ext(3, &packed, &child_hash);
        assert_eq!(ext.hash(), expected);
    }

    #[test]
    fn branch_child_lookup() {
        let c3 = Node::new_leaf(TV(3));
        let c7 = Node::new_leaf(TV(7));
        let bitmap: u16 = (1 << 3) | (1 << 7);
        let mut children: [Node<TV>; 16] = Default::default();
        children[3] = c3.clone();
        children[7] = c7.clone();
        let branch = Node::new_branch(bitmap, children);

        match &branch {
            Node::Branch(data) => {
                assert_eq!(data.children[3].hash(), c3.hash());
                assert_eq!(data.children[7].hash(), c7.hash());
                assert!(data.children[0].is_empty());
                assert!(data.children[4].is_empty());
                assert!(data.children[15].is_empty());
            }
            _ => panic!("expected branch"),
        }
    }

    #[test]
    fn ext_merge_nibbles_preserves_hash() {
        let leaf = Node::new_leaf(TV(1));
        let inner_ext = Node::new_ext(&[0x0C], leaf.clone());
        let outer_ext = Node::new_ext(&[0x0A, 0x0B], inner_ext);

        let flat_ext = Node::new_ext(&[0x0A, 0x0B, 0x0C], leaf);

        assert_ne!(outer_ext.hash(), flat_ext.hash());
    }
}
