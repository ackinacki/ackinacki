use std::sync::Arc;

use trie_map::trie::node::Node as ArenaNode;
use trie_map::trie::node::NodeId;
use trie_map::trie::node::NODE_ID_NONE;
use trie_map::trie::node::TAG_BRANCH;
use trie_map::trie::node::TAG_EXT;
use trie_map::trie::node::TAG_LEAF;
use trie_map::MapValue;
use trie_map::TrieMapSnapshot;

use crate::node::BranchData;
use crate::node::ExtData;
use crate::node::LeafData;
use crate::node::Node;
use crate::repo::MultiMapRef;

/// Convert a `MultiMapRef<V>` (Arc tree) to a `TrieMapSnapshot<V>` (flat arena).
pub fn to_trie_snapshot<V: MapValue>(map: &MultiMapRef<V>) -> TrieMapSnapshot<V> {
    let mut nodes = Vec::new();
    let mut values = Vec::new();
    let mut branch_children = Vec::new();
    let mut ext_paths = Vec::new();

    // Index 0 = canonical empty node
    nodes.push(ArenaNode::with_empty(map.root.hash()));

    let root =
        convert_to_arena(&map.root, &mut nodes, &mut values, &mut branch_children, &mut ext_paths);

    TrieMapSnapshot { nodes, values, branch_children, ext_paths, root, root_path: map.root_path }
}

fn convert_to_arena<V: MapValue>(
    node: &Node<V>,
    nodes: &mut Vec<ArenaNode>,
    values: &mut Vec<V>,
    branch_children: &mut Vec<NodeId>,
    ext_paths: &mut Vec<u8>,
) -> NodeId {
    match node {
        Node::Empty => NODE_ID_NONE,
        Node::Leaf(data) => {
            let value_index = values.len() as u32;
            values.push(data.value);
            let id = nodes.len() as NodeId;
            nodes.push(ArenaNode::with_leaf(data.hash, value_index));
            id
        }
        Node::Branch(data) => {
            let base = branch_children.len() as u32;
            let mut len = 0u16;

            // Count set bits and reserve placeholder slots
            for nib in 0..16u8 {
                if data.bitmap & (1u16 << nib) != 0 {
                    branch_children.push(NODE_ID_NONE);
                    len += 1;
                }
            }

            // Recurse into children and fill in the actual child IDs
            let mut slot = 0usize;
            for nib in 0..16u8 {
                if data.bitmap & (1u16 << nib) != 0 {
                    let child_id = convert_to_arena(
                        &data.children[nib as usize],
                        nodes,
                        values,
                        branch_children,
                        ext_paths,
                    );
                    branch_children[base as usize + slot] = child_id;
                    slot += 1;
                }
            }

            let id = nodes.len() as NodeId;
            nodes.push(ArenaNode::with_branch(data.hash, data.bitmap, base, len));
            id
        }
        Node::Ext(data) => {
            let nibbles = &data.nibbles[..data.nibble_count as usize];
            let ext_base = ext_paths.len() as u32;

            // Pack nibbles: two per byte, high nibble first
            let packed_len = nibbles.len().div_ceil(2);
            for i in 0..packed_len {
                let hi = nibbles.get(i * 2).copied().unwrap_or(0) & 0x0F;
                let lo = nibbles.get(i * 2 + 1).copied().unwrap_or(0) & 0x0F;
                ext_paths.push((hi << 4) | lo);
            }

            let child_id = convert_to_arena(&data.child, nodes, values, branch_children, ext_paths);
            let id = nodes.len() as NodeId;
            nodes.push(ArenaNode::with_ext(data.hash, ext_base, child_id, data.nibble_count));
            id
        }
    }
}

/// Convert a `TrieMapSnapshot<V>` (flat arena) to a `MultiMapRef<V>` (Arc tree).
pub fn from_trie_snapshot<V: MapValue>(snapshot: TrieMapSnapshot<V>) -> MultiMapRef<V> {
    let root = convert_from_arena(snapshot.root, &snapshot);
    MultiMapRef { root, root_path: snapshot.root_path }
}

fn convert_from_arena<V: MapValue>(id: NodeId, snapshot: &TrieMapSnapshot<V>) -> Node<V> {
    if id == NODE_ID_NONE {
        return Node::Empty;
    }
    let arena_node = &snapshot.nodes[id as usize];
    match arena_node.tag {
        TAG_LEAF => {
            let value_index = arena_node.leaf();
            Node::Leaf(Arc::new(LeafData {
                hash: arena_node.hash,
                value: snapshot.values[value_index],
            }))
        }
        TAG_BRANCH => {
            let (bitmap, _len, base) = arena_node.branch();
            let mut children: [Node<V>; 16] = Default::default();
            let mut slot = 0usize;
            for nib in 0..16u8 {
                if bitmap & (1u16 << nib) != 0 {
                    let child_id = snapshot.branch_children[base + slot];
                    children[nib as usize] = convert_from_arena(child_id, snapshot);
                    slot += 1;
                }
            }
            Node::Branch(Arc::new(BranchData { hash: arena_node.hash, bitmap, children }))
        }
        TAG_EXT => {
            let (ext_base, ext_len, child_id) = arena_node.ext();
            let packed_len = (ext_len as usize).div_ceil(2);
            let packed = &snapshot.ext_paths[ext_base..ext_base + packed_len];
            let mut nibbles = [0u8; 64];
            for (i, &byte) in packed.iter().enumerate() {
                nibbles[i * 2] = byte >> 4;
                if i * 2 + 1 < ext_len as usize {
                    nibbles[i * 2 + 1] = byte & 0x0F;
                }
            }
            let child = convert_from_arena(child_id, snapshot);
            Node::Ext(Arc::new(ExtData {
                hash: arena_node.hash,
                nibble_count: ext_len,
                nibbles,
                child,
            }))
        }
        _ => Node::Empty,
    }
}

#[cfg(test)]
mod tests {
    use blake3::Hasher;
    use node_types::Blake3Hashable;
    use serde::Deserialize;
    use serde::Serialize;
    use trie_map::MapKey;

    use super::*;
    use crate::repo::MultiMapRepository;

    #[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct TV(pub u64);

    impl Blake3Hashable for TV {
        fn update_hasher(&self, hasher: &mut Hasher) {
            hasher.update(&self.0.to_be_bytes());
        }
    }

    fn key(i: usize) -> MapKey {
        let mut h = Hasher::new();
        h.update(b"key:");
        h.update(&(i as u64).to_le_bytes());
        MapKey(*h.finalize().as_bytes())
    }

    #[test]
    fn roundtrip_empty() {
        let repo = MultiMapRepository::<TV>::new();
        let m = MultiMapRepository::<TV>::new_map();
        let snap = to_trie_snapshot(&m);
        let m2 = from_trie_snapshot(snap);
        assert_eq!(repo.map_hash(&m), repo.map_hash(&m2));
    }

    #[test]
    fn roundtrip_with_data() {
        let repo = MultiMapRepository::<TV>::new();
        let m = MultiMapRepository::<TV>::new_map();
        let updates: Vec<_> = (0..200).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = repo.map_update(&m, &updates);
        let orig_hash = repo.map_hash(&m);

        let snap = to_trie_snapshot(&m);
        let m2 = from_trie_snapshot(snap);

        assert_eq!(repo.map_hash(&m2), orig_hash);
        for i in 0..200 {
            assert_eq!(repo.map_get(&m2, &key(i)), Some(TV(i as u64)));
        }
    }

    #[test]
    fn roundtrip_after_deletes() {
        let repo = MultiMapRepository::<TV>::new();
        let m = MultiMapRepository::<TV>::new_map();
        let inserts: Vec<_> = (0..100).map(|i| (key(i), Some(TV(i as u64)))).collect();
        let m = repo.map_update(&m, &inserts);
        let deletes: Vec<_> = (0..100).step_by(3).map(|i| (key(i), None)).collect();
        let m = repo.map_update(&m, &deletes);
        let orig_hash = repo.map_hash(&m);

        let snap = to_trie_snapshot(&m);
        let m2 = from_trie_snapshot(snap);

        assert_eq!(repo.map_hash(&m2), orig_hash);
    }
}
