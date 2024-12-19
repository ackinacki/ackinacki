// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

#[derive(Clone, Debug)]
pub struct BlockData {
    pub parent_block_identifier: BlockIdentifier,
    pub block_identifier: BlockIdentifier,
    pub thread_identifier: ThreadIdentifier,
    pub block_seq_no: BlockSeqNo,
    pub refs: Vec<BlockIdentifier>,
}
