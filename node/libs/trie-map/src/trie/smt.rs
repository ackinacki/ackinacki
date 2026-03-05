use core::fmt::Display;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::Deserialize;
use serde::Serialize;

use crate::durable::DurableMapStat;
use crate::trie::arena::ext_bytes_len;
use crate::trie::arena::Arena;
use crate::trie::node::Node;
use crate::trie::node::NodeId;
use crate::trie::node::NODE_ID_NONE;
use crate::trie::node::TAG_BRANCH;
use crate::trie::node::TAG_EMPTY;
use crate::trie::node::TAG_EXT;
use crate::trie::node::TAG_LEAF;
use crate::MapHash;
use crate::MapKey;
use crate::MapKeyPath;
use crate::MapRepository;
use crate::MapValue;

#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "V: Serialize", deserialize = "V: for<'de2> Deserialize<'de2>"))]
pub struct TrieMapSnapshot<V: MapValue> {
    pub nodes: Vec<Node>,
    pub values: Vec<V>,
    pub branch_children: Vec<NodeId>,
    pub ext_paths: Vec<u8>,
    pub root: NodeId,
    pub root_path: MapKeyPath,
}

#[derive(Clone)]
pub struct TrieMapRepository<V: MapValue> {
    pub arena: Arc<RwLock<Arena<V>>>,
}

#[derive(Clone, Debug, Copy)]
pub struct TrieMapRef {
    pub root: NodeId,
    pub root_path: MapKeyPath,
}

impl TrieMapRef {
    pub fn new(root: NodeId, root_path: MapKeyPath) -> Self {
        Self { root, root_path }
    }
}

impl<V: MapValue> Default for TrieMapRepository<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: MapValue> TrieMapRepository<V> {
    pub fn new() -> Self {
        Self::with_arena(Arena::new())
    }

    pub fn new_map() -> TrieMapRef {
        TrieMapRef { root: NODE_ID_NONE, root_path: MapKeyPath { prefix: MapKey([0; 32]), len: 0 } }
    }

    pub fn with_arena(arena: Arena<V>) -> Self {
        Self { arena: Arc::new(RwLock::new(arena)) }
    }

    pub fn get_stat(&self, roots: impl IntoIterator<Item = NodeId>) -> DurableMapStat {
        let arena = self.arena.read();
        let reachable = Self::reachable_from_roots(&arena, roots);
        DurableMapStat {
            total_nodes: arena.nodes.len(),
            reachable_nodes: reachable.count_ones(),
            values_count: arena.values.len(),
            branch_children_count: arena.branch_children.len(),
            ext_paths_bytes: arena.ext_paths.len(),
        }
    }

    pub fn export_snapshot(&self, map: &TrieMapRef) -> TrieMapSnapshot<V> {
        let arena = self.arena.read();

        if map.root == NODE_ID_NONE {
            return TrieMapSnapshot {
                nodes: vec![Node::with_empty(arena.empty_branch_hash)],
                values: Vec::new(),
                branch_children: Vec::new(),
                ext_paths: Vec::new(),
                root: NODE_ID_NONE,
                root_path: map.root_path,
            };
        }

        // DFS to collect all reachable nodes and build remap
        let mut visited = std::collections::HashSet::new();
        let mut stack = vec![map.root];
        let mut visit_order = Vec::new();

        while let Some(id) = stack.pop() {
            if id == NODE_ID_NONE || !visited.insert(id) {
                continue;
            }
            visit_order.push(id);
            let node = &arena.nodes[id as usize];
            match node.tag {
                TAG_EMPTY | TAG_LEAF => {}
                TAG_EXT => {
                    let (_, _, child) = node.ext();
                    if child != NODE_ID_NONE {
                        stack.push(child);
                    }
                }
                TAG_BRANCH => {
                    let (_, len, base) = node.branch();
                    for i in 0..(len as usize) {
                        let child = arena.branch_children[base + i];
                        if child != NODE_ID_NONE {
                            stack.push(child);
                        }
                    }
                }
                _ => {}
            }
        }

        // Build remap: old_node_id -> new_local_index (1-based, 0 = empty)
        let mut remap = std::collections::HashMap::new();
        remap.insert(NODE_ID_NONE, NODE_ID_NONE);

        let mut snap_nodes = vec![Node::with_empty(arena.empty_branch_hash)]; // index 0 = empty
        let mut snap_values: Vec<V> = Vec::new();
        let mut snap_branch_children: Vec<NodeId> = Vec::new();
        let mut snap_ext_paths: Vec<u8> = Vec::new();

        for &old_id in &visit_order {
            let node = &arena.nodes[old_id as usize];
            let new_id = snap_nodes.len() as u32;
            remap.insert(old_id, new_id);

            match node.tag {
                TAG_EMPTY => {
                    remap.insert(old_id, NODE_ID_NONE);
                    continue; // don't add another empty, use canonical 0
                }
                TAG_LEAF => {
                    let val_idx = node.leaf();
                    let new_val_idx = snap_values.len() as u32;
                    snap_values.push(arena.values[val_idx]);
                    snap_nodes.push(Node::with_leaf(node.hash, new_val_idx));
                }
                TAG_BRANCH => {
                    let (bitmap, len, base) = node.branch();
                    let new_base = snap_branch_children.len() as u32;
                    for i in 0..(len as usize) {
                        // Placeholder children; will remap after
                        snap_branch_children.push(arena.branch_children[base + i]);
                    }
                    snap_nodes.push(Node::with_branch(node.hash, bitmap, new_base, len));
                }
                TAG_EXT => {
                    let (base, len, child) = node.ext();
                    let bytes = ext_bytes_len(len);
                    let new_base = snap_ext_paths.len() as u32;
                    snap_ext_paths.extend_from_slice(&arena.ext_paths[base..base + bytes]);
                    // child is placeholder, will remap after
                    snap_nodes.push(Node::with_ext(node.hash, new_base, child, len));
                }
                _ => {}
            }
        }

        // Now remap all child references in snapshot nodes
        for node in snap_nodes.iter_mut().skip(1) {
            match node.tag {
                TAG_BRANCH => {
                    let (_, len, base) = node.branch();
                    for i in 0..(len as usize) {
                        let old_child = snap_branch_children[base + i];
                        snap_branch_children[base + i] =
                            *remap.get(&old_child).unwrap_or(&NODE_ID_NONE);
                    }
                }
                TAG_EXT => {
                    let (ext_base, _len, old_child) = node.ext();
                    let new_child = *remap.get(&old_child).unwrap_or(&NODE_ID_NONE);
                    node.set_ext(ext_base, new_child);
                }
                _ => {}
            }
        }

        let new_root = *remap.get(&map.root).unwrap_or(&NODE_ID_NONE);

        TrieMapSnapshot {
            nodes: snap_nodes,
            values: snap_values,
            branch_children: snap_branch_children,
            ext_paths: snap_ext_paths,
            root: new_root,
            root_path: map.root_path,
        }
    }

    pub fn import_snapshot(&self, snapshot: TrieMapSnapshot<V>) -> TrieMapRef {
        if snapshot.root == NODE_ID_NONE {
            return TrieMapRef { root: NODE_ID_NONE, root_path: snapshot.root_path };
        }

        let mut arena = self.arena.write();

        let node_offset = arena.nodes.len() as u32;
        let value_offset = arena.values.len() as u32;
        let bc_offset = arena.branch_children.len() as u32;
        let ext_offset = arena.ext_paths.len() as u32;

        // Append values, branch_children, ext_paths
        arena.values.extend_from_slice(&snapshot.values);
        arena.branch_children.extend_from_slice(&snapshot.branch_children);
        arena.ext_paths.extend_from_slice(&snapshot.ext_paths);

        // Remap and append nodes (skip index 0 which is canonical empty)
        for (i, node) in snapshot.nodes.into_iter().enumerate() {
            if i == 0 {
                continue; // skip canonical empty
            }
            let mut n = node;
            match n.tag {
                TAG_LEAF => {
                    let old_val = n.leaf() as u32;
                    n.set_leaf((old_val + value_offset) as usize);
                }
                TAG_BRANCH => {
                    let (bitmap, len, base) = n.branch();
                    let new_base = base as u32 + bc_offset;
                    n.set_branch(new_base as usize);
                    // Remap children in the arena's branch_children
                    for j in 0..(len as usize) {
                        let idx = new_base as usize + j;
                        let old_child = arena.branch_children[idx];
                        arena.branch_children[idx] = if old_child == NODE_ID_NONE {
                            NODE_ID_NONE
                        } else {
                            // old_child is a snapshot-local index (>=1), remap to global
                            old_child - 1 + node_offset
                        };
                    }
                    // Rebuild node with correct base (bitmap and len unchanged)
                    n = Node::with_branch(n.hash, bitmap, new_base, len);
                }
                TAG_EXT => {
                    let (base, len, child) = n.ext();
                    let new_base = base as u32 + ext_offset;
                    let new_child =
                        if child == NODE_ID_NONE { NODE_ID_NONE } else { child - 1 + node_offset };
                    n = Node::with_ext(n.hash, new_base, new_child, len);
                }
                TAG_EMPTY => {
                    continue; // should not happen for i>0 normally
                }
                _ => {}
            }
            arena.nodes.push(n);
        }

        // Remap root: snapshot root is a local index (>=1), remap to global
        let new_root = if snapshot.root == NODE_ID_NONE {
            NODE_ID_NONE
        } else {
            snapshot.root - 1 + node_offset
        };

        TrieMapRef { root: new_root, root_path: snapshot.root_path }
    }

    pub fn collect_values(&self, map: &TrieMapRef) -> Vec<(MapKey, V)> {
        let arena = self.arena.read();
        let mut result = Vec::new();

        if map.root == NODE_ID_NONE {
            return result;
        }

        // Stack entries: (node_id, nibble_path)
        // nibble_path tracks the key nibbles accumulated so far
        let mut stack: Vec<(NodeId, Vec<u8>)> = vec![(map.root, Vec::new())];

        // Compute starting nibbles from root_path
        let start_nibbles = (map.root_path.len as usize) / 4;

        while let Some((node_id, nibbles)) = stack.pop() {
            if node_id == NODE_ID_NONE {
                continue;
            }
            let node = &arena.nodes[node_id as usize];
            match node.tag {
                TAG_EMPTY => {}
                TAG_LEAF => {
                    // Reconstruct full MapKey from root_path prefix + accumulated nibbles
                    let mut key_bytes = map.root_path.prefix.0;
                    // Write accumulated nibbles into key starting at start_nibbles
                    for (i, &nib) in nibbles.iter().enumerate() {
                        let pos = start_nibbles + i;
                        let byte_idx = pos / 2;
                        if pos % 2 == 0 {
                            key_bytes[byte_idx] = (key_bytes[byte_idx] & 0x0F) | (nib << 4);
                        } else {
                            key_bytes[byte_idx] = (key_bytes[byte_idx] & 0xF0) | (nib & 0x0F);
                        }
                    }
                    let val_idx = node.leaf();
                    result.push((MapKey(key_bytes), arena.values[val_idx]));
                }
                TAG_BRANCH => {
                    let (bitmap, _len, mut base) = node.branch();
                    for nib in 0..16u8 {
                        if bitmap & (1u16 << nib) != 0 {
                            let child_id = arena.branch_children[base];
                            let mut child_nibbles = nibbles.clone();
                            child_nibbles.push(nib);
                            stack.push((child_id, child_nibbles));
                            base += 1;
                        }
                    }
                }
                TAG_EXT => {
                    let (base, len, child) = node.ext();
                    let mut child_nibbles = nibbles.clone();
                    for j in 0..(len as usize) {
                        child_nibbles.push(arena.ext_get_nibble(base, j));
                    }
                    stack.push((child, child_nibbles));
                }
                _ => {}
            }
        }

        result
    }

    fn reachable_from_roots(arena: &Arena<V>, roots: impl IntoIterator<Item = NodeId>) -> BitSet {
        let n = arena.nodes.len();
        let mut seen = BitSet::new(n);
        let mut stack = Vec::with_capacity(1024);

        for r in roots {
            if r != NODE_ID_NONE {
                stack.push(r);
            }
        }

        while let Some(id) = stack.pop() {
            let idx = id as usize;
            if idx >= n || seen.get(idx) {
                continue;
            }
            seen.set(idx);

            let node = &arena.nodes[idx];
            match node.tag {
                TAG_EMPTY | TAG_LEAF => {}
                TAG_EXT => {
                    let (_base, _len, child) = node.ext();
                    if child != NODE_ID_NONE {
                        stack.push(child);
                    }
                }
                TAG_BRANCH => {
                    let (_bitmap, len, base) = node.branch();
                    let len = len as usize;
                    if base + len <= arena.branch_children.len() {
                        stack.extend_from_slice(&arena.branch_children[base..base + len]);
                    }
                }
                _ => {}
            }
        }

        seen
    }
}

struct BitSet {
    bits: Vec<u64>,
}

impl BitSet {
    fn new(size: usize) -> Self {
        let words = (size + 63) >> 6;
        Self { bits: vec![0; words] }
    }

    #[inline(always)]
    fn set(&mut self, i: usize) {
        unsafe {
            *self.bits.get_unchecked_mut(i >> 6) |= 1u64 << (i & 63);
        }
    }

    #[inline(always)]
    fn get(&self, i: usize) -> bool {
        unsafe { (*self.bits.get_unchecked(i >> 6) >> (i & 63)) & 1 == 1 }
    }

    fn count_ones(&self) -> usize {
        self.bits.iter().map(|w| w.count_ones() as usize).sum()
    }
}
impl<V: MapValue + Display> TrieMapRepository<V> {
    pub fn print_tree(&self, map: &TrieMapRef) {
        Self::print_node(&self.arena.read(), map.root, 0, "".to_string());
    }

    pub fn print_node(arena: &Arena<V>, node_id: NodeId, indent: usize, prefix: String) {
        let node = arena.node(node_id);
        match node.tag {
            TAG_EMPTY => {
                println!("{:indent$}{prefix}Empty", "", indent = indent);
            }
            TAG_LEAF => {
                let value_index = node.leaf();
                println!(
                    "{:indent$}{prefix}Leaf({node_id}) values[{value_index}] = {}",
                    "",
                    arena.values[value_index],
                    indent = indent
                );
            }
            TAG_BRANCH => {
                let (bitmap, _len, mut base) = node.branch();
                println!("{:indent$}{prefix}Branch({node_id})", "", indent = indent);
                for nib in 0..16 {
                    if bitmap & (1u16 << nib) != 0 {
                        let child_id = arena.branch_children[base];
                        Self::print_node(arena, child_id, indent + 2, format!("[{nib:x}] "));
                        base += 1;
                    }
                }
            }
            TAG_EXT => {
                let (base, len, child) = node.ext();
                println!(
                    "{:indent$}{prefix}Ext({node_id}) [{}]",
                    "",
                    ext_path_info(arena, base, len as usize),
                    indent = indent
                );
                Self::print_node(arena, child, indent + 2, "".to_string());
            }
            _ => unreachable!(),
        }
    }
}

fn ext_path_info<V: MapValue>(arena: &Arena<V>, base: usize, len: usize) -> String {
    let mut s = String::new();
    for i in 0..len {
        s.push_str(&format!("{:01x}", arena.ext_get_nibble(base, i)));
    }
    s
}

impl<V: MapValue> MapRepository for TrieMapRepository<V> {
    type MapRef = TrieMapRef;
    type Value = V;

    fn new_map() -> Self::MapRef {
        Self::new_map()
    }

    fn map_hash(&self, map: &Self::MapRef) -> MapHash {
        let arena = self.arena.read();
        MapHash(arena.node(map.root).hash)
    }

    fn map_key_path(&self, map: &Self::MapRef) -> MapKeyPath {
        map.root_path
    }

    fn map_get(&self, map: &Self::MapRef, key: &MapKey) -> Option<Self::Value> {
        let arena = self.arena.read();
        arena.get_from(&map.root_path, map.root, key)
    }

    fn map_batch_get(&self, map: &Self::MapRef, keys: &[MapKey]) -> Vec<Option<Self::Value>> {
        let arena = self.arena.read();
        keys.iter().map(|k| arena.get_from(&map.root_path, map.root, k)).collect()
    }

    fn map_update(
        &self,
        map: &Self::MapRef,
        updates: &[(MapKey, Option<Self::Value>)],
    ) -> Self::MapRef {
        let mut arena = self.arena.write();
        let (root, root_path) = arena.update(map.root, map.root_path, updates);
        Self::MapRef { root, root_path }
    }

    fn map_split(&self, map: &Self::MapRef, key_path: MapKeyPath) -> (Self::MapRef, Self::MapRef) {
        let mut arena = self.arena.write();
        let (without_branch, branch) = arena.split(map.root, map.root_path, key_path);
        (
            Self::MapRef { root: without_branch.0, root_path: without_branch.1 },
            Self::MapRef { root: branch.0, root_path: branch.1 },
        )
    }

    fn merge(&self, a: &Self::MapRef, b: &Self::MapRef) -> Self::MapRef {
        let mut arena = self.arena.write();
        let (root, root_path) = arena.merge((a.root, a.root_path), (b.root, b.root_path));
        Self::MapRef { root, root_path }
    }
}
