use std::collections::HashSet;
use std::collections::VecDeque;

use node_types::TemporaryBlockId;

use crate::node::unprocessed_blocks_collection::FilterPrehistoric;
use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

pub fn invalidate_branch(
    branch_root_block_state: BlockState,
    block_state_repository: &BlockStateRepository,
    filter: &FilterPrehistoric,
) {
    if let Some(seq_no) = branch_root_block_state.guarded(|e| *e.block_seq_no()) {
        if *filter.block_seq_no() >= seq_no {
            return;
        }
    }
    let mut to_process = VecDeque::from([branch_root_block_state]);
    while let Some(next) = to_process.pop_front() {
        assert!(!next.guarded(|e| e.is_finalized()));
        let (children, incomplete_children) = next.guarded_mut(|e| {
            let mut children = HashSet::new();
            if e.is_invalidated() {
                // We expect this branch to be invalidated already with the same call.
                return (children, HashSet::new());
            }
            e.set_invalidated().unwrap();
            for (_key, hashset) in e.known_children.iter() {
                children = children.union(hashset).cloned().collect();
            }

            // Collect incomplete children for temporary subtree invalidation
            let mut incomplete = HashSet::new();
            for (_key, temp_ids) in e.known_children_incomplete.iter() {
                incomplete.extend(temp_ids.iter().copied());
            }

            (children, incomplete)
        });
        for child_id in children.iter() {
            let child = block_state_repository.get(child_id).unwrap();
            to_process.push_back(child);
        }

        // Invalidate temporary children by removing them.
        // Temporary states don't have is_invalidated() — just remove them.
        for temp_id in incomplete_children.iter() {
            invalidate_temporary_subtree(temp_id, block_state_repository);
        }
    }
}

/// Recursively remove a temporary state and all its temporary descendants.
fn invalidate_temporary_subtree(
    temp_id: &TemporaryBlockId,
    block_state_repository: &BlockStateRepository,
) {
    if let Some(temp_arc) = block_state_repository.get_temporary(temp_id) {
        let children_incomplete = {
            let temp = temp_arc.read();
            temp.known_children_incomplete()
                .values()
                .flat_map(|s| s.iter().copied())
                .collect::<Vec<_>>()
        };
        for child_temp_id in children_incomplete {
            invalidate_temporary_subtree(&child_temp_id, block_state_repository);
        }
    }
}
