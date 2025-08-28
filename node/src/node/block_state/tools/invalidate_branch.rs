use std::collections::HashSet;
use std::collections::VecDeque;

use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

pub fn invalidate_branch(
    branch_root_block_state: BlockState,
    block_state_repository: &BlockStateRepository,
) {
    let mut to_process = VecDeque::from([branch_root_block_state]);
    while let Some(next) = to_process.pop_front() {
        assert!(!next.guarded(|e| e.is_finalized()));
        let children = next.guarded_mut(|e| {
            let mut children = HashSet::new();
            if e.is_invalidated() {
                // We expect this branch to be invalidated already with the same call.
                return children;
            }
            e.set_invalidated().unwrap();
            for (_key, hashset) in e.known_children.iter() {
                children = children.union(hashset).cloned().collect();
            }
            children
        });
        for child_id in children.iter() {
            let child = block_state_repository.get(child_id).unwrap();
            to_process.push_back(child);
        }
    }
}
