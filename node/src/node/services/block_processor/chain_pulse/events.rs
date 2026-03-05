use derive_getters::Getters;
use node_types::ThreadIdentifier;

use crate::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use crate::types::BlockHeight;

#[derive(Getters)]
pub struct BlockFinalizedEvent {
    thread_identifier: ThreadIdentifier,
    block_height: Option<BlockHeight>,
}

#[derive(Getters)]
pub struct BlockPrefinalizedEvent {
    thread_identifier: ThreadIdentifier,
    block_height: Option<BlockHeight>,
}

#[derive(Getters)]
pub struct BlockAppliedEvent {
    thread_identifier: ThreadIdentifier,
    block_height: Option<BlockHeight>,
}

pub enum ChainPulseEvent {
    BlockFinalized(BlockFinalizedEvent),
    BlockPrefinalized(BlockPrefinalizedEvent),
    BlockApplied(BlockAppliedEvent),
    StartThread {
        thread_id: ThreadIdentifier,
        block_candidates: UnfinalizedCandidateBlockCollection,
    },
    StopThread {
        thread_id: ThreadIdentifier,
    },
}

impl ChainPulseEvent {
    pub fn block_finalized(
        thread_identifier: ThreadIdentifier,
        block_height: Option<BlockHeight>,
    ) -> Self {
        ChainPulseEvent::BlockFinalized(BlockFinalizedEvent { thread_identifier, block_height })
    }

    pub fn block_prefinalized(
        thread_identifier: ThreadIdentifier,
        block_height: Option<BlockHeight>,
    ) -> Self {
        ChainPulseEvent::BlockPrefinalized(BlockPrefinalizedEvent {
            thread_identifier,
            block_height,
        })
    }

    pub fn block_applied(
        thread_identifier: ThreadIdentifier,
        block_height: Option<BlockHeight>,
    ) -> Self {
        ChainPulseEvent::BlockApplied(BlockAppliedEvent { thread_identifier, block_height })
    }

    pub fn start_thread(
        thread_id: ThreadIdentifier,
        block_candidates: UnfinalizedCandidateBlockCollection,
    ) -> Self {
        ChainPulseEvent::StartThread { thread_id, block_candidates }
    }

    pub fn stop_thread(thread_id: ThreadIdentifier) -> Self {
        ChainPulseEvent::StopThread { thread_id }
    }
}
