use crate::node::block_state::repository::BlockStateRepository;
use crate::node::BlockState;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

pub fn set_bk_set(block_state: &BlockState, repo: &BlockStateRepository) -> bool {
    if block_state.guarded(|e| e.bk_set().is_some()) {
        // Already set
        return true;
    }
    let Some(parent_id) = block_state.guarded(|e| e.parent_block_identifier().clone()) else {
        // Parent of this block is not known yet.
        tracing::trace!("Parent of this block is not known yet");
        return false;
    };
    let Ok(parent_block_state) = repo.get(&parent_id) else {
        tracing::trace!("Unexpected failure: failed to load parent state");
        return false;
    };
    let (Some(bk_set), Some(future_bk_set)) = parent_block_state
        .guarded(|e| (e.descendant_bk_set().clone(), e.descendant_future_bk_set().clone()))
    else {
        // Parent block has no descendant bk set ready.
        tracing::trace!("Parent block has no descendant bk set ready");
        return false;
    };
    if block_state.guarded(|e| e.block_seq_no().map(|v| v % 200 == 0).unwrap_or(false)) {
        let seq_no = block_state.guarded(|e| *e.block_seq_no()).unwrap_or(BlockSeqNo::default());
        tracing::trace!("Full bk set {seq_no} {block_state:?}: {bk_set}");
        tracing::trace!("Full future bk set {seq_no} {block_state:?}: {future_bk_set}");
    }

    block_state.guarded_mut(|e| {
        if e.bk_set().is_none() {
            let _ = e.set_bk_set(bk_set);
        }
        if e.future_bk_set().is_none() {
            let _ = e.set_future_bk_set(future_bk_set);
        }
    });
    true
}
