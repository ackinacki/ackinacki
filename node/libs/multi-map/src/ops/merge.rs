use super::ensure_branch_at_depth;
use super::make_branch;
use crate::node::NodeAug;
use crate::MultiMapAugmentation;
use crate::MultiMapValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DisjointMergeError {
    IncompatibleRootPath,
    OverlappingLeaf,
    MalformedTree,
}

/// Checked union of two canonical tries representing the same root-path view.
///
/// The function rejects a leaf present in both inputs. It operates only on
/// nodes, cached hashes, and augmentations; leaf values are never inspected.
pub fn merge_disjoint<V: MultiMapValue, A: MultiMapAugmentation>(
    left: &NodeAug<V, A>,
    right: &NodeAug<V, A>,
    start_depth: usize,
) -> Result<NodeAug<V, A>, DisjointMergeError> {
    merge_at_depth(left, right, start_depth)
}

fn merge_at_depth<V: MultiMapValue, A: MultiMapAugmentation>(
    left: &NodeAug<V, A>,
    right: &NodeAug<V, A>,
    depth: usize,
) -> Result<NodeAug<V, A>, DisjointMergeError> {
    if left.is_empty() {
        return Ok(right.clone());
    }
    if right.is_empty() {
        return Ok(left.clone());
    }
    if depth == 64 {
        return match (left, right) {
            (NodeAug::Leaf(_), NodeAug::Leaf(_)) => Err(DisjointMergeError::OverlappingLeaf),
            _ => Err(DisjointMergeError::MalformedTree),
        };
    }

    let left_branch = ensure_branch_at_depth(left);
    let right_branch = ensure_branch_at_depth(right);
    let (NodeAug::Branch(left_data), NodeAug::Branch(right_data)) = (&left_branch, &right_branch)
    else {
        return Err(DisjointMergeError::MalformedTree);
    };

    let mut children: [NodeAug<V, A>; 16] = Default::default();
    let mut bitmap = 0u16;
    for (nibble, slot) in children.iter_mut().enumerate() {
        let child =
            merge_at_depth(&left_data.children[nibble], &right_data.children[nibble], depth + 1)?;
        if !child.is_empty() {
            bitmap |= 1 << nibble;
            *slot = child;
        }
    }

    Ok(make_branch(bitmap, children))
}
