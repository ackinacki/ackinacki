use crate::block_keeper_system::bk_set::update_block_keeper_set_from_common_section;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::node::block_state::repository::BlockState;
use crate::repository::Repository;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

pub fn set_descendant_bk_set<T: Repository>(block_state: &BlockState, blocks_repository: &T) {
    if block_state.guarded(|e| e.descendant_bk_set().is_some()) {
        // already set
        return;
    }
    let Some(bk_set) = block_state.guarded(|e| e.bk_set().clone()) else {
        // bk set for the block is not set yet.
        return;
    };

    if !block_state.guarded(|e| e.is_stored()) {
        // no block available. skip till it's available.
        return;
    }

    let Some(candidate_block) =
        blocks_repository.get_block(block_state.block_identifier()).ok().flatten()
    else {
        tracing::trace!("Failed to load candidate block even though it is marked to be saved. Skipping for another attempt. {}", block_state.block_identifier());
        return;
    };
    match update_block_keeper_set_from_common_section(candidate_block.data(), bk_set.clone()) {
        Err(e) => {
            tracing::trace!(
                "Failed to update block keeper set from the common section: {}; {}, {:?}",
                e,
                candidate_block.data(),
                &bk_set
            );
        }
        Ok(None) => {
            block_state.guarded_mut(|e| {
                if e.descendant_bk_set().is_none() {
                    let _ = e.set_descendant_bk_set(bk_set);
                }
            });
        }
        Ok(Some(next_bk_set)) => {
            block_state.guarded_mut(|e| {
                if e.descendant_bk_set().is_none() {
                    let _ = e.set_descendant_bk_set(next_bk_set);
                }
            });
        }
    }
}
