use std::collections::HashSet;

use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;

pub(super) fn find_next_prefinalized(
    block_state: &BlockState,
    thread_identifier: &ThreadIdentifier,
    block_state_repository: &BlockStateRepository,
) -> Option<BlockState> {
    let children: HashSet<BlockIdentifier> =
        block_state.guarded(|e| e.known_children(thread_identifier).cloned())?;
    let mut candidate = None;
    for child_id in children.iter() {
        let child = block_state_repository.get(child_id).unwrap();
        if child.guarded(|e| e.is_finalized()) {
            return Some(child);
        }
        if child.guarded(|e| e.is_prefinalized() && !e.is_invalidated()) {
            assert!(
                candidate.is_none(),
                "Somehow we have a situation of more than 1 block prefinalized"
            );
            candidate = Some(child);
        }
    }
    candidate
}

pub(super) fn find_last_prefinalized(
    thread_identifier: &ThreadIdentifier,
    block_repository: &RepositoryImpl,
    block_state_repository: &BlockStateRepository,
) -> anyhow::Result<BlockState> {
    let last_finalized_block_id = match block_repository
        .select_thread_last_finalized_block(thread_identifier)?
    {
        Some((id, _)) => id,
        None => {
            // Assumption:
            // It means this thread has no blocks at all.
            // Lookup for the root block for this thread.
            let root_block_id = thread_identifier.spawning_block_id();
            let root_block_state = block_state_repository.get(&root_block_id).unwrap();

            // Safety check. Ensure that the assumption is correct.
            let children =
                root_block_state.guarded(|e| e.known_children(thread_identifier).cloned());
            if let Some(children) = children {
                // ensure no child is finalized
                for child in children.iter() {
                    if block_state_repository.get(child).unwrap().guarded(|e| e.is_finalized()) {
                        // There is a slim chance for the race condition.
                        if block_repository
                            .select_thread_last_finalized_block(thread_identifier)
                            .unwrap()
                            .is_none()
                        {
                            panic!("The assumption above is wrong. needs an investigation");
                        }
                    }
                }
            }
            // end of the safety check.
            root_block_id
        }
    };
    // Descend down to the last non prefinalized
    let mut cursor = block_state_repository.get(&last_finalized_block_id).unwrap();
    loop {
        let Some(next) = find_next_prefinalized(&cursor, thread_identifier, block_state_repository)
        else {
            tracing::trace!(
                "find_last_prefinalized: result: {} {}",
                cursor.block_identifier(),
                cursor.guarded(|e| (*e.block_seq_no()).expect("Must be set"))
            );
            return Ok(cursor);
        };
        cursor = next;
    }
}
