use crate::block_keeper_system::bk_set::update_block_keeper_set_from_common_section;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::block_state::repository::BlockState;
use crate::types::AckiNackiBlock;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

pub fn set_descendant_bk_set(
    block_state: &BlockState,
    candidate_block: &Envelope<GoshBLS, AckiNackiBlock>,
) {
    if block_state
        .guarded(|e| e.descendant_bk_set().is_some() && e.descendant_future_bk_set().is_some())
    {
        // already set
        return;
    }
    let (Some(bk_set), Some(future_bk_set)) =
        block_state.guarded(|e| (e.bk_set().clone(), e.future_bk_set().clone()))
    else {
        // bk set for the block is not set yet.
        return;
    };

    if !block_state.guarded(|e| e.is_stored()) {
        // no block available. skip till it's available.
        return;
    }

    match update_block_keeper_set_from_common_section(
        candidate_block.data(),
        bk_set.clone(),
        future_bk_set.clone(),
    ) {
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
                if e.descendant_future_bk_set().is_none() {
                    let _ = e.set_descendant_future_bk_set(future_bk_set);
                }
            });
        }
        Ok(Some((next_bk_set, next_future_bk_set))) => {
            block_state.guarded_mut(|e| {
                if e.descendant_bk_set().is_none() {
                    let _ = e.set_descendant_bk_set(next_bk_set);
                }
                if e.descendant_future_bk_set().is_none() {
                    let _ = e.set_descendant_future_bk_set(next_future_bk_set);
                }
            });
        }
    }
}
