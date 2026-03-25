use serde::Deserialize;
use serde::Serialize;

use crate::trie::hash::hash_branch_empty;
use crate::trie::hash::hash_ext;
use crate::trie::hash::hash_leaf_value;
use crate::trie::nibble::TrieNibble;
use crate::trie::nibble::TrieNibble4;
use crate::trie::node::*;
use crate::trie::tx::is_tx_node;
use crate::trie::tx::Tx;
use crate::trie::tx::TxArena;
use crate::trie::tx::TX_TAG;
use crate::MapKey;
use crate::MapKeyPath;
use crate::MapValue;

#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "V: Serialize", deserialize = "V: for<'de2> Deserialize<'de2>"))]
pub struct Arena<V: MapValue> {
    pub nodes: Vec<Node>,             // NodeId index
    pub values: Vec<V>,               // leaf value pool
    pub branch_children: Vec<NodeId>, // packed children for branches
    pub ext_paths: Vec<u8>,           // packed nibbles (2 per byte), append-only

    pub empty_branch_hash: [u8; 32], // hash of empty subtree
}

impl<V: MapValue> Default for Arena<V> {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::needless_range_loop)]
impl<V: MapValue> Arena<V> {
    pub fn new() -> Self {
        Self::with_raw_parts(
            vec![Node::with_empty(hash_branch_empty())],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
    }

    pub fn with_raw_parts(
        nodes: Vec<Node>,
        values: Vec<V>,
        branch_children: Vec<NodeId>,
        ext_path: Vec<u8>,
    ) -> Self {
        Self {
            nodes,
            values,
            branch_children,
            ext_paths: ext_path,
            empty_branch_hash: hash_branch_empty(),
        }
    }

    #[inline]
    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id as usize]
    }

    pub fn alloc_leaf(&mut self, value: V) -> NodeId {
        let idx = self.values.len() as u32;
        self.values.push(value);

        let hash = hash_leaf_value(&value);
        let id = self.nodes.len() as u32;
        self.nodes.push(Node::with_leaf(hash, idx));
        id
    }

    #[inline]
    pub fn ext_get_nibble(&self, base: usize, i: usize) -> u8 {
        let b = self.ext_paths[base + (i / 2)];
        if (i & 1) == 0 {
            (b >> 4) & 0x0F
        } else {
            b & 0x0F
        }
    }

    pub fn alloc_ext(&mut self, ext_base: u32, ext_len: u8, child: NodeId) -> NodeId {
        debug_assert!(ext_len > 0);
        let child_hash = self.node(child).hash;
        let bytes = ext_bytes_len(ext_len);
        let packed = &self.ext_paths[ext_base as usize..ext_base as usize + bytes];
        let hash = hash_ext(ext_len, packed, &child_hash);

        let id = self.nodes.len() as u32;
        self.nodes.push(Node::with_ext(hash, ext_base, child, ext_len));
        id
    }

    #[inline]
    pub fn branch_child_get(&self, node: &Node, nib: u8) -> NodeId {
        debug_assert!(node.tag == TAG_BRANCH);
        let (bitmap, len, base) = node.branch();
        let bit = 1u16 << (nib as u16);
        if (bitmap & bit) == 0 {
            return NODE_ID_NONE;
        }
        let lower = bitmap & (bit - 1);
        let idx = pop_count16(lower) as usize;
        let slice = &self.branch_children[base..base + (len as usize)];
        slice[idx]
    }

    fn ext_matches_key(&self, base: usize, len: u8, key: &[u8; 32], depth: usize) -> bool {
        for i in 0..(len as usize) {
            let a = self.ext_get_nibble(base, i);
            let b = nibble_at(key, depth + i);
            if a != b {
                return false;
            }
        }
        true
    }

    /// Iterative get from a root (Patricia-aware).
    pub(crate) fn get_from(&self, root_path: &MapKeyPath, root: NodeId, key: &MapKey) -> Option<V> {
        // filter by bit-prefix view
        if root_path.len > 0 && !prefix_bits_match(&root_path.prefix.0, &key.0, root_path.len) {
            return None;
        }

        let start = (root_path.len as usize) / 4;
        let r = (root_path.len as usize) % 4;

        // boundary inside nibble `start`
        if r != 0 {
            let mask = boundary_mask(r);
            let pn = nibble_from_key(&root_path.prefix.0, start) & mask;
            let kn = nibble_from_key(&key.0, start) & mask;
            if pn != kn {
                return None;
            }
        }

        let mut node_id = root;
        let mut depth = start;

        while depth < 64 {
            if node_id == NODE_ID_NONE {
                return None;
            }
            let n = self.node(node_id);

            match n.tag {
                TAG_BRANCH => {
                    let nib = nibble_at(&key.0, depth);
                    node_id = self.branch_child_get(n, nib);
                    depth += 1;
                }
                TAG_EXT => {
                    let (base, len, child) = n.ext();
                    if depth + (len as usize) > 64 {
                        return None;
                    }
                    if !self.ext_matches_key(base, len, &key.0, depth) {
                        return None;
                    }
                    depth += len as usize;
                    node_id = child;
                }
                _ => return None,
            }
        }

        if node_id == NODE_ID_NONE {
            return None;
        }
        let n = self.node(node_id);
        if n.tag != TAG_LEAF {
            return None;
        }
        Some(self.values[n.leaf()])
    }

    pub fn update(
        &mut self,
        root: NodeId,
        root_path: MapKeyPath,
        updates: &[(MapKey, Option<V>)],
    ) -> (NodeId, MapKeyPath) {
        let mut tx = TxArena::new(self);
        let tx_root = tx.update(root, root_path, updates);
        let [new_root] = self.commit(tx.tx, [tx_root]);
        (new_root, root_path)
    }

    pub(crate) fn split(
        &mut self,
        root: NodeId,
        root_path: MapKeyPath,
        key_path: MapKeyPath,
    ) -> ((NodeId, MapKeyPath), (NodeId, MapKeyPath)) {
        let mut tx = TxArena::new(self);
        let ((tx_without_branch, without_branch_key_path), (tx_branch, branch_key_path)) =
            tx.split(root, root_path, key_path);
        let [without_branch, branch] = self.commit(tx.tx, [tx_without_branch, tx_branch]);

        ((without_branch, without_branch_key_path), (branch, branch_key_path))
    }

    pub(crate) fn merge(
        &mut self,
        a: (NodeId, MapKeyPath),
        b: (NodeId, MapKeyPath),
    ) -> (NodeId, MapKeyPath) {
        let mut tx = TxArena::new(self);
        let (tx_merged, merged_key_path) = tx.merge(a, b);
        let [merged] = self.commit(tx.tx, [tx_merged]);
        (merged, merged_key_path)
    }

    fn commit<const N: usize>(&mut self, tx: Tx<V>, roots: [NodeId; N]) -> [NodeId; N] {
        #[inline(always)]
        fn tx_index(id: NodeId) -> usize {
            (id & !TX_TAG) as usize
        }

        // ---------- Phase 1: mark reachable + build postorder ----------
        let mut reachable = vec![false; tx.nodes.len()];
        let mut postorder: Vec<usize> = Vec::new();
        let mut stack: Vec<(NodeId, bool)> = Vec::new(); // (node_id, expanded)

        for &r in &roots {
            stack.push((r, false));
        }

        while let Some((id, expanded)) = stack.pop() {
            if id == NODE_ID_NONE || !is_tx_node(id) {
                continue;
            }
            let idx = tx_index(id);

            if expanded {
                postorder.push(idx);
                continue;
            }

            if reachable[idx] {
                continue;
            }
            reachable[idx] = true;

            // postorder: push self as expanded, then children
            stack.push((id, true));

            let node = &tx.nodes[idx];
            match node.tag {
                TAG_BRANCH => {
                    let (_, len, base) = node.branch();
                    let kids = &tx.branch_children[base..base + (len as usize)];
                    for &c in kids {
                        if is_tx_node(c) {
                            stack.push((c, false));
                        }
                    }
                }
                TAG_EXT => {
                    let (_, _, child) = node.ext();
                    if is_tx_node(child) {
                        stack.push((child, false));
                    }
                }
                TAG_LEAF | TAG_EMPTY => {}
                _ => unreachable!(),
            }
        }

        // Optional: reserve pretty accurately (inexpensive scan)
        let mut leaf_cnt = 0usize;
        let mut branch_child_cnt = 0usize;
        let mut ext_bytes_cnt = 0usize;

        for &i in &postorder {
            let n = &tx.nodes[i];
            match n.tag {
                TAG_LEAF => leaf_cnt += 1,
                TAG_BRANCH => {
                    let (_, len, _) = n.branch();
                    branch_child_cnt += len as usize;
                }
                TAG_EXT => {
                    let (base, len, _) = n.ext();
                    let _ = base; // the base is in tx.ext_paths
                    ext_bytes_cnt += ext_bytes_len(len);
                }
                TAG_EMPTY => {}
                _ => unreachable!(),
            }
        }

        self.nodes.reserve(postorder.len());
        self.values.reserve(leaf_cnt);
        self.branch_children.reserve(branch_child_cnt);
        self.ext_paths.reserve(ext_bytes_cnt);

        // ---------- Phase 2: copy in postorder, filling remap ----------
        let mut remap: Vec<NodeId> = vec![NODE_ID_NONE; tx.nodes.len()];

        let resolve_child = |child: NodeId, remap: &Vec<NodeId>| -> NodeId {
            if child == NODE_ID_NONE {
                NODE_ID_NONE
            } else if is_tx_node(child) {
                remap[tx_index(child)]
            } else {
                child
            }
        };

        for &i in &postorder {
            let node = &tx.nodes[i];

            match node.tag {
                TAG_EMPTY => {
                    // Arena already has a canonical empty node at 0 (per your new()).
                    remap[i] = 0;
                }

                TAG_LEAF => {
                    let tx_val_idx = node.leaf();

                    // NOTE: this is Clone-based. Your current commit() already required Clone
                    // via extend_from_slice, so the behavior matches.
                    let v = tx.values[tx_val_idx];

                    let new_val_idx = self.values.len() as u32;
                    self.values.push(v);

                    let new_id = self.nodes.len() as u32;
                    self.nodes.push(Node::with_leaf(node.hash, new_val_idx));
                    remap[i] = new_id;
                }

                TAG_EXT => {
                    let (base, len, child) = node.ext();
                    let bytes = ext_bytes_len(len);

                    let new_base = self.ext_paths.len() as u32;
                    self.ext_paths.extend_from_slice(&tx.ext_paths[base..base + bytes]);

                    let child2 = resolve_child(child, &remap);

                    let new_id = self.nodes.len() as u32;
                    self.nodes.push(Node::with_ext(node.hash, new_base, child2, len));
                    remap[i] = new_id;
                }

                TAG_BRANCH => {
                    let (bitmap, len, base) = node.branch();
                    let kids = &tx.branch_children[base..base + (len as usize)];

                    let new_base = self.branch_children.len() as u32;
                    for &c in kids {
                        self.branch_children.push(resolve_child(c, &remap));
                    }

                    let new_id = self.nodes.len() as u32;
                    self.nodes.push(Node::with_branch(node.hash, bitmap, new_base, len));
                    remap[i] = new_id;
                }

                _ => unreachable!(),
            }
        }

        // ---------- resolve roots ----------
        let mut out = roots;
        for r in &mut out {
            if *r == NODE_ID_NONE {
                continue;
            }
            if is_tx_node(*r) {
                *r = remap[tx_index(*r)];
            }
        }
        out
    }
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
pub fn boundary_mask(r: usize) -> u8 {
    debug_assert!((1..=3).contains(&r));
    (0xFu8 << (4 - r)) & 0xF
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
pub fn ext_bytes_len(len_nibbles: u8) -> usize {
    (len_nibbles as usize).div_ceil(2)
}

#[inline]
pub fn nibble_at(key: &[u8; 32], depth: usize) -> u8 {
    TrieNibble4::get(key, depth)
}
