// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;

#[derive(Clone)]
pub enum Event {
    BlockMustBeInvalidated(ThreadIdentifier, BlockIdentifier),
    BlockDependenciesMet(ThreadIdentifier, BlockIdentifier),
}
