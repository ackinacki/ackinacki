// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use thiserror::Error;

use crate::types::BlockIdentifier;

#[derive(Error, Debug)]
pub enum BlockAppendError {
    #[error("Thread tail does not match block parent.")]
    TailMistmatch,

    #[error("One of the refs has a conflict with this chain or other ref this block depends on. This block dependencies are impossible.")]
    ConflictingStates,

    #[error("A referenced block was marked as invalid. Probably due to a fork.")]
    InvalidatedRef,

    #[error("A thread chain state for a referenced block is not available.")]
    MissingReferencedState,

    #[error("Parent block is not ready.")]
    ParentBlockMissing,
}

#[derive(Error, Debug, Clone)]
pub enum BlockFinalizationHandlingError {
    #[error("Critical error. Must be investigated. A block marked as invalidated was sent for finalization.")]
    CriticalInvalidatedBlockIsFinalized,
}

#[derive(Error, Debug, Clone)]
pub enum BlockInvalidationHandlingError {
    #[error("Critical error. Must be investigated. A block already marked as finalized was sent for invalidation.")]
    CriticalFinalizedBlockIsInvalidated,
}

#[derive(Error, Debug, Clone)]
pub enum CanReferenceQueryHandlingError {
    #[error("Dependency tracking service does not have a record for the parent block.")]
    QueriedParentBlockStateNotFound(BlockIdentifier),

    #[error("Dependency tracking service does not have a record for the referenced block.")]
    ReferencedBlockStateNotFound(BlockIdentifier),
}
