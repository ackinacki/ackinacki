use typed_builder::TypedBuilder;
use std::sync::Arc;
use crate::types::AckiNackiBlock;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::bls::GoshBLS;
use crate::bls::envelope::Envelope;


// Note: MUST BE UNCLONABLE. This ensures no inner clones will ever happen.
pub struct ThreadState {
    // Finalized block can not rollback ever
    last_finalized: Checkpoint,

    // Prefinalized block can be rolled back on NACK.
    last_prefinalized: Checkpoint,


}


impl ThreadState {
    // This function must NEVER fail. It is crucial to have all precautions set in place.
    pub fn move_finalized(self, next_finalized_block: Envelope<GoshBLS, AckiNackiBlock>) -> Self {
        let ThreadState {
            last_finalized,
        } = self;
        //
        let next_checkpoint = last_finalized.apply(next_finalized_block).unwrap();
        Self {
            last_finalized: next_checkpoint,
        }
    }

    pub fn move_prefinalized(self, next_prefinalized_block: Envelope<GoshBLS, AckiNackiBlock>) -> Self {
    }
}
