use crate::trie::arena::boundary_mask;
use crate::trie::arena::ext_bytes_len;
use crate::trie::arena::nibble_at;
use crate::trie::arena::nibble_from_key;
use crate::trie::arena::prefix_bits_match;
use crate::trie::arena::Arena;
use crate::trie::hash::hash_branch_sparse;
use crate::trie::hash::hash_ext;
use crate::trie::hash::hash_leaf_value;
use crate::trie::node::*;
use crate::MapKey;
use crate::MapKeyPath;
use crate::MapValue;

#[derive(Debug)]
struct Update<V> {
    key: MapKey,
    val: Option<V>, // Some(v)=set, None=delete
    seq: u32,       // order for last-write-wins
}

pub struct Tx<V: MapValue> {
    pub nodes: Vec<Node>,
    pub values: Vec<V>,
    pub branch_children: Vec<NodeId>,
    pub ext_paths: Vec<u8>,
}

impl<V: MapValue> Tx<V> {
    #[inline]
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            values: Vec::new(),
            branch_children: Vec::new(),
            ext_paths: Vec::new(),
        }
    }
}

pub struct TxArena<'a, V: MapValue> {
    pub arena_nodes: &'a [Node],
    pub arena_branch_children: &'a [NodeId],
    pub arena_ext_paths: &'a [u8],
    pub tx: Tx<V>,
}

pub(crate) const TX_TAG: NodeId = 0x80000000;

#[inline]
pub(crate) fn is_tx_node(id: NodeId) -> bool {
    id & TX_TAG != 0
}

#[inline]
pub(crate) fn tx_node_id(id: NodeId) -> NodeId {
    id | TX_TAG
}

#[allow(clippy::needless_range_loop)]
impl<'a, V: MapValue> TxArena<'a, V> {
    pub fn new(arena: &'a Arena<V>) -> Self {
        Self {
            arena_nodes: &arena.nodes,
            arena_branch_children: &arena.branch_children,
            arena_ext_paths: &arena.ext_paths,
            tx: Tx::new(),
        }
    }

    fn push_value(&mut self, value: V) -> u32 {
        let value_index = self.tx.values.len() as u32;
        self.tx.values.push(value);
        value_index
    }

    #[inline]
    pub fn node(&self, id: NodeId) -> &Node {
        if id == NODE_ID_NONE {
            panic!("invalid node id");
        }
        if is_tx_node(id) {
            &self.tx.nodes[(id & !TX_TAG) as usize]
        } else {
            &self.arena_nodes[id as usize]
        }
    }

    fn push_node(&mut self, node: Node) -> NodeId {
        let id = tx_node_id(self.tx.nodes.len() as u32);
        self.tx.nodes.push(node);
        id
    }

    pub fn alloc_leaf(&mut self, value: V) -> NodeId {
        let hash = hash_leaf_value(&value);
        let value_index = self.push_value(value);
        self.push_node(Node::with_leaf(hash, value_index))
    }

    #[inline]
    fn ext_paths(&self, node_id: NodeId, base: usize) -> &[u8] {
        if is_tx_node(node_id) {
            &self.tx.ext_paths[base..]
        } else {
            &self.arena_ext_paths[base..]
        }
    }

    #[inline]
    pub fn ext_get_nibble(&self, node_id: NodeId, base: usize, i: usize) -> u8 {
        let b = self.ext_paths(node_id, base)[i / 2];
        if (i & 1) == 0 {
            (b >> 4) & 0x0F
        } else {
            b & 0x0F
        }
    }

    fn ext_push_nibbles(&mut self, nibs: &[u8]) -> u32 {
        debug_assert!(!nibs.is_empty());
        let base = self.tx.ext_paths.len() as u32;
        let bytes = nibs.len().div_ceil(2);
        self.tx.ext_paths.reserve(bytes);

        for i in (0..nibs.len()).step_by(2) {
            let hi = nibs[i] & 0x0F;
            let lo = if i + 1 < nibs.len() { nibs[i + 1] & 0x0F } else { 0u8 };
            self.tx.ext_paths.push((hi << 4) | lo);
        }
        base
    }

    pub fn alloc_ext(&mut self, ext_base: u32, ext_len: u8, child: NodeId) -> NodeId {
        debug_assert!(ext_len > 0);
        let child_hash = self.node(child).hash;
        let bytes = ext_bytes_len(ext_len);
        let packed = &self.tx.ext_paths[ext_base as usize..ext_base as usize + bytes];
        let hash = hash_ext(ext_len, packed, &child_hash);
        self.push_node(Node::with_ext(hash, ext_base, child, ext_len))
    }

    pub fn alloc_ext_from_nibbles(&mut self, nibs: &[u8], child: NodeId) -> NodeId {
        debug_assert!(!nibs.is_empty());
        let base = self.ext_push_nibbles(nibs);
        self.alloc_ext(base, nibs.len() as u8, child)
    }

    /// Merge EXT+EXT: `Ext(a, Ext(b, child)) => Ext(a||b, child)`
    fn merge_ext_if_possible(&mut self, ext_id: NodeId) -> NodeId {
        let ext_node = *self.node(ext_id);
        if ext_node.tag != TAG_EXT {
            return ext_id;
        }
        let (ext_base, ext_len, child) = ext_node.ext();
        if child == NODE_ID_NONE {
            return NODE_ID_NONE;
        }
        let child_node = *self.node(child);
        if child_node.tag != TAG_EXT {
            return ext_id;
        }
        let (child_base, child_len, next_child) = child_node.ext();

        let total = (ext_len as usize) + (child_len as usize);
        let mut nibs = [0u8; 64];
        for i in 0..(ext_len as usize) {
            nibs[i] = self.ext_get_nibble(ext_id, ext_base, i);
        }
        for i in 0..(child_len as usize) {
            nibs[ext_len as usize + i] = self.ext_get_nibble(child, child_base, i);
        }

        self.alloc_ext_from_nibbles(&nibs[..total], next_child)
    }

    /// Compress a single-child branch into an EXT of 1 nibble (and merge EXT+EXT).
    fn compress_single_child_branch(&mut self, bitmap: u16, child: NodeId) -> NodeId {
        debug_assert!(bitmap.count_ones() == 1);
        if child == NODE_ID_NONE {
            return NODE_ID_NONE;
        }
        let nib = bitmap.trailing_zeros() as u8;
        debug_assert!(nib < 16);

        // create Ext([nib], child) but if the child is EXT, merge into one EXT
        let tmp = [nib];
        let ext = self.alloc_ext_from_nibbles(&tmp, child);
        self.merge_ext_if_possible(ext)
    }

    pub fn alloc_branch_sparse(&mut self, bitmap: u16, children: &[NodeId]) -> NodeId {
        debug_assert!(children.len() <= 16);

        // if it's a single-child branch, store it as EXT immediately
        if bitmap.count_ones() == 1 {
            debug_assert_eq!(children.len(), 1);
            return self.compress_single_child_branch(bitmap, children[0]);
        }

        let base = self.tx.branch_children.len() as u32;
        self.tx.branch_children.extend_from_slice(children);

        let (nibs, hs, out_len) = self.gather_nibs_hashes(bitmap, children);

        let hash = hash_branch_sparse(bitmap, &nibs[..out_len], &hs[..out_len]);
        self.push_node(Node::with_branch(hash, bitmap, base, children.len() as u16))
    }

    fn gather_nibs_hashes(
        &mut self,
        bitmap: u16,
        children: &[NodeId],
    ) -> ([u8; 16], [[u8; 32]; 16], usize) {
        let mut nibs = [0u8; 16];
        let mut hs = [[0u8; 32]; 16];
        let mut out_len = 0usize;

        let mut j = 0usize;
        for nib in 0..16u8 {
            let bit = 1u16 << (nib as u16);
            if (bitmap & bit) != 0 {
                let cid = children[j];
                nibs[out_len] = nib;
                hs[out_len] = self.node(cid).hash;
                out_len += 1;
                j += 1;
            }
        }
        (nibs, hs, out_len)
    }

    #[inline]
    fn branch_children(&self, node_id: NodeId, base: usize, len: u16) -> &[NodeId] {
        if is_tx_node(node_id) {
            &self.tx.branch_children[base..base + len as usize]
        } else {
            &self.arena_branch_children[base..base + len as usize]
        }
    }

    #[inline]
    pub fn branch_child_get(&self, node_id: NodeId, node: &Node, nib: u8) -> NodeId {
        debug_assert!(node.tag == TAG_BRANCH);
        let (bitmap, len, base) = node.branch();
        let bit = 1u16 << (nib as u16);
        if (bitmap & bit) == 0 {
            return NODE_ID_NONE;
        }
        let lower = bitmap & (bit - 1);
        let idx = pop_count16(lower) as usize;
        let slice = &self.branch_children(node_id, base, len);
        slice[idx]
    }

    /// Ensure that the returned node behaves as a BRANCH consuming exactly 1 nibble:
    /// - If the node is BRANCH => returns it
    /// - If the node is EXT => materializes a BRANCH with the first nibble of EXT
    /// - If the node is EMPTY/NONE => returns NONE
    fn ensure_branch_at_depth(&mut self, node_id: NodeId) -> NodeId {
        if node_id == NODE_ID_NONE {
            return NODE_ID_NONE;
        }
        let n = *self.node(node_id);
        match n.tag {
            TAG_BRANCH => node_id,
            TAG_EXT => {
                let (base, len, child) = n.ext();
                if len == 0 {
                    return child;
                }
                let first = self.ext_get_nibble(node_id, base, 0);
                let tail_len = len - 1;

                let tail_child = if tail_len == 0 {
                    child
                } else {
                    // build tail ext (base+0.5 bytes shift is not possible => copy nibs)
                    let mut nibs = [0u8; 64];
                    for i in 0..(tail_len as usize) {
                        nibs[i] = self.ext_get_nibble(node_id, base, i + 1);
                    }
                    self.alloc_ext_from_nibbles(&nibs[..tail_len as usize], child)
                };

                let bitmap = 1u16 << (first as u16);
                let tmp = [tail_child];
                self.alloc_branch_sparse_raw(bitmap, &tmp)
            }
            TAG_EMPTY => NODE_ID_NONE,
            _ => NODE_ID_NONE,
        }
    }

    /// Rebuild a branch with one nibble child updated, stack-only (no heap alloc).
    fn rebuild_branch_with_child_no_alloc(
        &mut self,
        old_id: NodeId,
        old: &Node,
        nib: u8,
        new_child: NodeId,
    ) -> NodeId {
        let (old_bitmap, old_len, old_base) = old.branch();
        let bit = 1u16 << (nib as u16);
        let existed = (old_bitmap & bit) != 0;

        let old_children = &self.branch_children(old_id, old_base, old_len);

        // Fast path: no change
        if !existed && new_child == NODE_ID_NONE {
            return old_id;
        }

        let mut out = [NODE_ID_NONE; 16];
        let mut out_len = 0usize;

        if existed {
            let idx = pop_count16(old_bitmap & (bit - 1)) as usize;
            if new_child == NODE_ID_NONE {
                // remove
                for (i, &cid) in old_children.iter().enumerate() {
                    if i == idx {
                        continue;
                    }
                    out[out_len] = cid;
                    out_len += 1;
                }
                let new_bitmap = old_bitmap & !bit;
                if new_bitmap == 0 {
                    NODE_ID_NONE
                } else {
                    self.alloc_branch_sparse(new_bitmap, &out[..out_len])
                }
            } else {
                // replace
                for (i, &cid) in old_children.iter().enumerate() {
                    out[out_len] = if i == idx { new_child } else { cid };
                    out_len += 1;
                }
                self.alloc_branch_sparse(old_bitmap, &out[..out_len])
            }
        } else {
            // insert
            debug_assert!(new_child != NODE_ID_NONE);

            let idx = pop_count16(old_bitmap & (bit - 1)) as usize;
            let n = old_children.len();

            out[..idx].copy_from_slice(&old_children[..idx]);
            out[idx] = new_child;
            out[idx + 1..idx + 1 + (n - idx)].copy_from_slice(&old_children[idx..]);

            let out_len = n + 1;

            let new_bitmap = old_bitmap | bit;
            self.alloc_branch_sparse(new_bitmap, &out[..out_len])
        }
    }

    fn resolve_child(nib_to_child: &mut [NodeId; 16], branch_map: u16, branch_children: &[NodeId]) {
        let mut j = 0usize;
        for nib in 0..16u8 {
            let bit = 1u16 << (nib as u16);
            if (branch_map & bit) == 0 {
                continue;
            }
            nib_to_child[nib as usize] = branch_children[j];
            j += 1;
        }
    }

    /// Find subtree root node-id for a bit-prefix view.
    fn subtree_root_for_view(&mut self, full_root: NodeId, path: MapKeyPath) -> NodeId {
        if full_root == NODE_ID_NONE {
            return NODE_ID_NONE;
        }
        let d = (path.len as usize) / 4;
        let r = (path.len as usize) % 4;

        // Walk full nibbles from 0 to d-1, materializing EXT when needed
        let mut node_id = full_root;
        for depth in 0..d {
            node_id = self.ensure_branch_at_depth(node_id);
            if node_id == NODE_ID_NONE {
                return NODE_ID_NONE;
            }
            let node = *self.node(node_id);
            if node.tag != TAG_BRANCH {
                return NODE_ID_NONE;
            }
            let nib = nibble_at(&path.prefix.0, depth);
            node_id = self.branch_child_get(node_id, &node, nib);
        }

        if r == 0 {
            return node_id;
        }

        // Not nibble-aligned: we need a BRANCH at depth d (materialize EXT if needed).
        node_id = self.ensure_branch_at_depth(node_id);
        if node_id == NODE_ID_NONE {
            return NODE_ID_NONE;
        }
        let node = *self.node(node_id);
        if node.tag != TAG_BRANCH {
            return NODE_ID_NONE;
        }

        let mask = boundary_mask(r);
        let want = nibble_from_key(&path.prefix.0, d) & mask;

        let (bitmap, len, base) = node.branch();
        let all_children = &self.branch_children(node_id, base, len);

        let mut out = [NODE_ID_NONE; 16];
        let mut out_len = 0usize;
        let mut out_bitmap: u16 = 0;

        let mut j = 0usize;
        for nib in 0..16u8 {
            let bit = 1u16 << (nib as u16);
            if (bitmap & bit) == 0 {
                continue;
            }
            let child_id = all_children[j];
            j += 1;

            if (nib & mask) == want {
                out[out_len] = child_id;
                out_len += 1;
                out_bitmap |= bit;
            }
        }

        if out_bitmap == 0 {
            NODE_ID_NONE
        } else {
            self.alloc_branch_sparse(out_bitmap, &out[..out_len])
        }
    }

    /// Prune (delete) the entire prefix-subtree from a full-root.
    fn prune_prefix(&mut self, root: NodeId, path: MapKeyPath) -> NodeId {
        let d = (path.len as usize) / 4;
        let r = (path.len as usize) % 4;
        self.prune_rec(root, 0, d, r, &path.prefix.0)
    }

    fn prune_rec(
        &mut self,
        node_id: NodeId,
        depth: usize,
        d: usize,
        r: usize,
        prefix_key: &[u8; 32],
    ) -> NodeId {
        if node_id == NODE_ID_NONE {
            return NODE_ID_NONE;
        }
        if depth > d {
            return node_id;
        }

        // materialize EXT into BRANCH at this depth (so pruning logic stays nibble-based)
        let node_id = self.ensure_branch_at_depth(node_id);
        if node_id == NODE_ID_NONE {
            return NODE_ID_NONE;
        }

        let node = *self.node(node_id);
        if node.tag != TAG_BRANCH {
            return node_id;
        }

        if depth < d {
            let nib = nibble_at(prefix_key, depth);
            let child = self.branch_child_get(node_id, &node, nib);
            let new_child = self.prune_rec(child, depth + 1, d, r, prefix_key);
            return self.rebuild_branch_with_child_no_alloc(node_id, &node, nib, new_child);
        }

        // depth == d: boundary action
        if r == 0 {
            return NODE_ID_NONE;
        }

        let mask = boundary_mask(r);
        let want = nibble_from_key(prefix_key, depth) & mask;

        let (bitmap, len, base) = node.branch();
        let old_children = &self.branch_children(node_id, base, len);

        let mut out = [NODE_ID_NONE; 16];
        let mut out_len = 0usize;
        let mut out_bitmap: u16 = 0;

        let mut j = 0usize;
        for nib in 0..16u8 {
            let bit = 1u16 << (nib as u16);
            if (bitmap & bit) == 0 {
                continue;
            }
            let cid = old_children[j];
            j += 1;

            if (nib & mask) != want {
                out[out_len] = cid;
                out_len += 1;
                out_bitmap |= bit;
            }
        }

        if out_bitmap == 0 {
            NODE_ID_NONE
        } else {
            self.alloc_branch_sparse(out_bitmap, &out[..out_len])
        }
    }

    /// Graft a prefix-subtree back into a pruned full-root.
    fn graft_prefix(&mut self, root: NodeId, sub_root: NodeId, sub_path: MapKeyPath) -> NodeId {
        let d = (sub_path.len as usize) / 4;
        let r = (sub_path.len as usize) % 4;
        self.graft_rec(root, 0, d, r, &sub_path.prefix.0, sub_root)
    }

    fn graft_rec(
        &mut self,
        node_id: NodeId,
        depth: usize,
        d: usize,
        r: usize,
        prefix_key: &[u8; 32],
        sub_root: NodeId,
    ) -> NodeId {
        if depth == d {
            if r == 0 {
                return sub_root;
            }

            // boundary inside nibble: need branch at depth d
            let base_id = self.ensure_branch_at_depth(node_id);
            let sub_id = self.ensure_branch_at_depth(sub_root);

            if sub_id == NODE_ID_NONE {
                return node_id;
            }
            let sub_node = *self.node(sub_id);
            if sub_node.tag != TAG_BRANCH {
                return node_id;
            }

            let (sub_bitmap, sub_len, sub_base) = sub_node.branch();
            let sub_children = &self.branch_children(sub_id, sub_base, sub_len);

            let mut existing_map = 0u16;
            let mut existing_children_slice: &[NodeId] = &[];
            if base_id != NODE_ID_NONE {
                let base_node = *self.node(base_id);
                if base_node.tag == TAG_BRANCH {
                    let (base_bitmap, base_len, base_base) = base_node.branch();
                    existing_map = base_bitmap;
                    existing_children_slice = self.branch_children(base_id, base_base, base_len);
                }
            }

            let mut final_bitmap = existing_map | sub_bitmap;

            let mut nib_to_child = [NODE_ID_NONE; 16];
            if existing_map != 0 {
                Self::resolve_child(&mut nib_to_child, existing_map, existing_children_slice);
            }
            Self::resolve_child(&mut nib_to_child, sub_bitmap, sub_children);

            let mut out = [NODE_ID_NONE; 16];
            let mut out_len = 0usize;
            for nib in 0..16u8 {
                let bit = 1u16 << (nib as u16);
                if (final_bitmap & bit) == 0 {
                    continue;
                }
                let cid = nib_to_child[nib as usize];
                if cid == NODE_ID_NONE {
                    final_bitmap &= !bit;
                    continue;
                }
                out[out_len] = cid;
                out_len += 1;
            }

            return if final_bitmap == 0 {
                NODE_ID_NONE
            } else {
                self.alloc_branch_sparse(final_bitmap, &out[..out_len])
            };
        }

        // depth < d: descend one nibble (materialize EXT when needed)
        let nib = nibble_at(prefix_key, depth);

        let here_id = self.ensure_branch_at_depth(node_id);
        if here_id == NODE_ID_NONE {
            // treat as empty; only create a path if the subtree wants something below
            let child = self.graft_rec(NODE_ID_NONE, depth + 1, d, r, prefix_key, sub_root);
            if child == NODE_ID_NONE {
                return NODE_ID_NONE;
            }
            let bitmap = 1u16 << (nib as u16);
            let tmp = [child];
            return self.alloc_branch_sparse(bitmap, &tmp);
        }

        let here_node = *self.node(here_id);
        if here_node.tag != TAG_BRANCH {
            let child = self.graft_rec(NODE_ID_NONE, depth + 1, d, r, prefix_key, sub_root);
            if child == NODE_ID_NONE {
                return here_id;
            }
            let bitmap = 1u16 << (nib as u16);
            let tmp = [child];
            return self.alloc_branch_sparse(bitmap, &tmp);
        }

        let old_child = self.branch_child_get(here_id, &here_node, nib);
        let new_child = self.graft_rec(old_child, depth + 1, d, r, prefix_key, sub_root);
        self.rebuild_branch_with_child_no_alloc(here_id, &here_node, nib, new_child)
    }

    pub(crate) fn split(
        &mut self,
        root: NodeId,
        root_path: MapKeyPath,
        key_path: MapKeyPath,
    ) -> ((NodeId, MapKeyPath), (NodeId, MapKeyPath)) {
        let sub_root = self.subtree_root_for_view(root, key_path);
        let pruned_root = self.prune_prefix(root, key_path);

        // normalize both outputs (important!)
        let sub_root = self.canonicalize(sub_root);
        let pruned_root = self.canonicalize(pruned_root);

        let without_branch = (pruned_root, root_path);
        let branch = (sub_root, key_path);
        (without_branch, branch)
    }

    pub(crate) fn merge(
        &mut self,
        a: (NodeId, MapKeyPath),
        b: (NodeId, MapKeyPath),
    ) -> (NodeId, MapKeyPath) {
        let merged_root = self.graft_prefix(a.0, b.0, b.1);
        let merged_root = self.canonicalize(merged_root);
        (merged_root, a.1)
    }

    fn canonicalize(&mut self, id: NodeId) -> NodeId {
        if id == NODE_ID_NONE {
            return NODE_ID_NONE;
        }
        let node = *self.node(id);
        match node.tag {
            TAG_EMPTY => NODE_ID_NONE,

            TAG_LEAF => id,

            TAG_EXT => {
                let (base, len, child) = node.ext();
                let child2 = self.canonicalize(child);
                if child2 == NODE_ID_NONE {
                    return NODE_ID_NONE;
                }

                // rebuild ext with the same path but normalized child
                let mut nibs = [0u8; 64];
                for i in 0..(len as usize) {
                    nibs[i] = self.ext_get_nibble(id, base, i);
                }
                let ext = self.alloc_ext_from_nibbles(&nibs[..len as usize], child2);
                self.merge_ext_if_possible(ext)
            }

            TAG_BRANCH => {
                let (bitmap, len, base) = node.branch();
                let old_children = &self.branch_children(id, base, len);

                // build nibble->child map, normalize each child, then pack in nibble order
                let mut nib_to_child = [NODE_ID_NONE; 16];
                Self::resolve_child(&mut nib_to_child, bitmap, old_children);

                let mut out = [NODE_ID_NONE; 16];
                let mut out_len = 0usize;
                let mut out_bitmap: u16 = 0;

                for nib in 0..16u8 {
                    let cid = nib_to_child[nib as usize];
                    if cid == NODE_ID_NONE {
                        continue;
                    }
                    let cid2 = self.canonicalize(cid);
                    if cid2 == NODE_ID_NONE {
                        continue;
                    }
                    out_bitmap |= 1u16 << (nib as u16);
                    out[out_len] = cid2;
                    out_len += 1;
                }

                if out_bitmap == 0 {
                    NODE_ID_NONE
                } else {
                    // alloc_branch_sparse already compresses single-child -> EXT
                    self.alloc_branch_sparse(out_bitmap, &out[..out_len])
                }
            }

            _ => unreachable!(),
        }
    }

    /// Allocate a branch node WITHOUT Patricia compression.
    /// Used internally to materialize EXT into a real BRANCH for split/prune/graft logic.
    fn alloc_branch_sparse_raw(&mut self, bitmap: u16, children: &[NodeId]) -> NodeId {
        debug_assert!(children.len() <= 16);

        let base = self.tx.branch_children.len() as u32;
        self.tx.branch_children.extend_from_slice(children);
        let (nibs, hs, out_len) = self.gather_nibs_hashes(bitmap, children);

        let hash = hash_branch_sparse(bitmap, &nibs[..out_len], &hs[..out_len]);
        self.push_node(Node::with_branch(hash, bitmap, base, children.len() as u16))
    }

    // new update

    fn normalize_updates(
        &self,
        root_path: &MapKeyPath,
        updates: &[(MapKey, Option<V>)],
    ) -> Vec<Update<V>> {
        let start = (root_path.len as usize) / 4;
        let r = (root_path.len as usize) % 4;

        let mut out: Vec<Update<V>> = Vec::with_capacity(updates.len());

        for (seq, (k, v)) in updates.iter().enumerate() {
            // 1) prefix-bits filter
            if root_path.len > 0 && !prefix_bits_match(&root_path.prefix.0, &k.0, root_path.len) {
                continue;
            }

            // 2) boundary inside nibble `start`
            if r != 0 {
                let mask = boundary_mask(r);
                let pn = nibble_from_key(&root_path.prefix.0, start) & mask;
                let kn = nibble_from_key(&k.0, start) & mask;
                if pn != kn {
                    continue;
                }
            }

            out.push(Update { key: *k, val: *v, seq: seq as u32 });
        }

        if out.is_empty() {
            return out;
        }

        // Sort by (key, seq) so we can keep the last seq per key
        out.sort_unstable_by(|a, b| {
            let kc = a.key.0.cmp(&b.key.0);
            if kc != std::cmp::Ordering::Equal {
                kc
            } else {
                a.seq.cmp(&b.seq)
            }
        });

        // Dedup by key keeping LAST (max seq) in each group.
        // Because seq is ascending within equal keys, the last is at the end of the group.
        let mut w = 0usize;
        let mut i = 0usize;
        while i < out.len() {
            let mut j = i + 1;
            while j < out.len() && out[j].key.0 == out[i].key.0 {
                j += 1;
            }
            // keep last in `[i..j)`
            let last = j - 1;
            if w != last {
                out.swap(w, last);
            }
            w += 1;
            i = j;
        }
        out.truncate(w);

        // Re-sort by key only (now unique keys)
        out.sort_unstable_by(|a, b| a.key.0.cmp(&b.key.0));
        out
    }

    #[inline]
    fn split_by_nibble(items: &[Update<V>], depth: usize) -> [(usize, usize); 16] {
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

    fn build_from_updates(&mut self, depth: usize, items: &mut [Update<V>]) -> NodeId {
        if items.is_empty() {
            return NODE_ID_NONE;
        }
        if depth == 64 {
            // unique keys => exactly 1 element here
            let v = items[0].val.take();
            return match v {
                Some(v) => self.alloc_leaf(v),
                None => NODE_ID_NONE,
            };
        }

        let ranges = Self::split_by_nibble(items, depth);

        let mut out = [NODE_ID_NONE; 16];
        let mut out_len = 0usize;
        let mut bitmap: u16 = 0;

        for nib in 0..16usize {
            let (a, b) = ranges[nib];
            if a == b {
                continue;
            }
            let child = self.build_from_updates(depth + 1, &mut items[a..b]);
            if child != NODE_ID_NONE {
                bitmap |= 1u16 << (nib as u16);
                out[out_len] = child;
                out_len += 1;
            }
        }

        if bitmap == 0 {
            NODE_ID_NONE
        } else {
            self.alloc_branch_sparse(bitmap, &out[..out_len]) // already compresses single-child to EXT
        }
    }

    fn apply_range(&mut self, old_id: NodeId, depth: usize, items: &mut [Update<V>]) -> NodeId {
        if items.is_empty() {
            return old_id;
        }
        if depth == 64 {
            let v = items[0].val.take();
            return match v {
                Some(v) => self.alloc_leaf(v),
                None => NODE_ID_NONE,
            };
        }

        if old_id == NODE_ID_NONE {
            return self.build_from_updates(depth, items);
        }

        // Make sure we behave like a BRANCH consuming 1 nibble at this depth
        let br_id = self.ensure_branch_at_depth(old_id);
        if br_id == NODE_ID_NONE {
            return self.build_from_updates(depth, items);
        }
        let br = *self.node(br_id);
        if br.tag != TAG_BRANCH {
            return self.build_from_updates(depth, items);
        }

        let (bitmap, len, base) = br.branch();
        let old_children = self.branch_children(br_id, base, len);

        let mut nib_to_child = [NODE_ID_NONE; 16];
        Self::resolve_child(&mut nib_to_child, bitmap, old_children);

        let ranges = Self::split_by_nibble(items, depth);

        let mut changed = false;

        for nib in 0..16usize {
            let (a, b) = ranges[nib];
            if a == b {
                continue; // no updates for this nibble => keep old child as-is
            }
            let old_c = nib_to_child[nib];
            let new_c = self.apply_range(old_c, depth + 1, &mut items[a..b]);
            if new_c != old_c {
                nib_to_child[nib] = new_c;
                changed = true;
            }
        }

        if !changed {
            return br_id;
        }

        // repack children
        let mut out = [NODE_ID_NONE; 16];
        let mut out_len = 0usize;
        let mut out_bitmap: u16 = 0;

        for nib in 0..16usize {
            let cid = nib_to_child[nib];
            if cid != NODE_ID_NONE {
                out_bitmap |= 1u16 << (nib as u16);
                out[out_len] = cid;
                out_len += 1;
            }
        }

        if out_bitmap == 0 {
            NODE_ID_NONE
        } else {
            self.alloc_branch_sparse(out_bitmap, &out[..out_len])
        }
    }

    pub fn update(
        &mut self,
        root: NodeId,
        root_path: MapKeyPath,
        updates: &[(MapKey, Option<V>)],
    ) -> NodeId {
        // normalize: filter+sort+dedup(last-write-wins)
        let mut items = self.normalize_updates(&root_path, updates);

        if items.is_empty() {
            return root;
        }

        // conservative reserve
        let n = items.len();
        self.tx.nodes.reserve(1 + n * 8);
        self.tx.values.reserve(n);
        self.tx.branch_children.reserve(n * 4);
        self.tx.ext_paths.reserve(n * 16);

        let start = (root_path.len as usize) / 4;

        self.apply_range(root, start, &mut items[..])
    }
}
