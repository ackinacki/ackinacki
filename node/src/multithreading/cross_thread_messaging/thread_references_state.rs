// Re-export types from the thread-reference-state crate
pub use thread_reference_state::BlockSeqNo;
pub use thread_reference_state::CanRefQueryResult as CanRefQueryResultGeneric;
pub use thread_reference_state::ReferencedBlock as ReferencedBlockGeneric;
pub use thread_reference_state::ResultingState as ResultingStateGeneric;
pub use thread_reference_state::ThreadReferencesState as ThreadReferencesStateGeneric;

// Concrete type aliases using node_types identifiers and the local BlockSeqNo.
pub type ThreadReferencesState =
    thread_reference_state::ThreadReferencesState<BlockId, BlockSeqNo, ThreadId>;
pub type ReferencedBlock = thread_reference_state::ReferencedBlock<BlockId, BlockSeqNo, ThreadId>;
pub type ResultingState = thread_reference_state::ResultingState<BlockId>;
pub type CanRefQueryResult = thread_reference_state::CanRefQueryResult<BlockId>;

type BlockId = node_types::BlockIdentifier;
type ThreadId = node_types::ThreadIdentifier;
