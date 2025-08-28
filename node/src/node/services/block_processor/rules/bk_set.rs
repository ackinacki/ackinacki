use crate::helper::metrics::BlockProductionMetrics;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::BlockState;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

pub fn set_bk_set(
    block_state: &BlockState,
    repo: &BlockStateRepository,
    metrics: Option<&BlockProductionMetrics>,
) -> bool {
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

    let bk_set_len = bk_set.len();
    let (did_update, thread_id) = block_state.guarded_mut(|e| {
        let mut did_update = false;
        if e.bk_set().is_none() {
            let _ = e.set_bk_set(bk_set);
            did_update = true;
        }
        if e.future_bk_set().is_none() {
            let _ = e.set_future_bk_set(future_bk_set);
        }
        (did_update, *e.thread_identifier())
    });
    if did_update {
        if let Some(thread_id) = thread_id {
            metrics.inspect(|m| m.report_bk_set_size(bk_set_len as u64, &thread_id));
        }
    }
    true
}
