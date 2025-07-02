use derive_getters::Getters;

use crate::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use crate::types::ThreadIdentifier;

#[derive(Getters)]
pub struct BlockFinalizedEvent {
    thread_identifier: ThreadIdentifier,
}

#[derive(Getters)]
pub struct BlockPrefinalizedEvent {
    thread_identifier: ThreadIdentifier,
}

pub enum ChainPulseEvent {
    BlockFinalized(BlockFinalizedEvent),
    BlockPrefinalized(BlockPrefinalizedEvent),
    StartThread {
        thread_id: ThreadIdentifier,
        block_candidates: UnfinalizedCandidateBlockCollection,
    },
}

impl ChainPulseEvent {
    pub fn block_finalized(thread_identifier: ThreadIdentifier) -> Self {
        ChainPulseEvent::BlockFinalized(BlockFinalizedEvent { thread_identifier })
    }

    pub fn start_thread(
        thread_id: ThreadIdentifier,
        block_candidates: UnfinalizedCandidateBlockCollection,
    ) -> Self {
        ChainPulseEvent::StartThread { thread_id, block_candidates }
    }
}
