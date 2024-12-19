use typed_builder::TypedBuilder;

use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

#[derive(Clone, TypedBuilder)]
pub struct BlockFinalizedEventData {
    pub parent_block_identifier: BlockIdentifier,
    pub block_identifier: BlockIdentifier,
    pub thread_identifier: ThreadIdentifier,
    pub block_seq_no: BlockSeqNo,
    pub refs: Vec<BlockIdentifier>,
}

#[derive(Clone)]
pub enum Event {
    BlockFinalized(BlockFinalizedEventData),
}
