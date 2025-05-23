use crate::node::block_state::repository::BlockStateRepository;
use crate::node::BlockState;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

pub fn set_bk_set(block_state: &BlockState, repo: &BlockStateRepository) {
    if block_state.guarded(|e| e.bk_set().is_some()) {
        // Already set
        return;
    }
    let Some(parent_id) = block_state.guarded(|e| e.parent_block_identifier().clone()) else {
        // Parent of this block is not known yet.
        return;
    };
    let Ok(parent_block_state) = repo.get(&parent_id) else {
        tracing::trace!("Unexpected failure: failed to load parent state");
        return;
    };
    let Some(bk_set) = parent_block_state.guarded(|e| e.descendant_bk_set().clone()) else {
        // Parent block has no descendant bk set ready.
        return;
    };

    block_state.guarded_mut(|e| {
        if e.bk_set().is_none() {
            let _ = e.set_bk_set(bk_set);
        }
    });
}
