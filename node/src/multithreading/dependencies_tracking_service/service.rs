// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::mpsc::Sender;

use super::events::Event;
use super::thread_chain::OtherThreadChainErr;
use super::thread_chain::OtherThreadChainResult;
use super::thread_chain::ThreadChain;
use super::thread_chain::ThreadChainValidationFailed;
use super::BlockAppendError;
use super::BlockData;
use super::BlockFinalizationHandlingError;
use super::BlockInvalidationHandlingError;
use super::CanReferenceQueryHandlingError;
use super::FinalizedBlocksSet;
use super::InvalidatedBlocksSet;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;

// Note: why I don't like this solution.
// It requires to update all optimistic states for all blocks.
// This means single finalized block would require an update in 6 * 100 states.
// Not good at all. One thing is obvious - it must be stored in a separate object
// rather than together in the optimistic state.
// There's also a problem with the state durability. The node may require full
// state resync in case this state fails to be saved on disk.
// Is there alternatives? Not that I can think of right now. Might come up with
// something later. Yet, this solution should work ok'ish for now

// Note:
// 100 threads x 100 blocks. Assuming it will be enough for quite some time.
// If block falls behind the capacity it may not be referenced.
// TODO: implement the right solution.
const FINALIZED_BLOCKS_SET_CAPACITY: usize = 10000usize;
const INVALIDATED_BLOCKS_SET_CAPACITY: usize = 10000usize;

#[allow(dead_code)]
pub struct DependenciesTrackingService {
    states: HashMap<BlockIdentifier, ThreadChain>,
    subscribers: Vec<Sender<Event>>,
    finalized_blocks: FinalizedBlocksSet,
    invalidated_blocks: InvalidatedBlocksSet,
}

impl DependenciesTrackingService {
    pub fn start(_: ()) -> Self {
        Self {
            states: HashMap::default(),
            subscribers: vec![],
            finalized_blocks: FinalizedBlocksSet::new(FINALIZED_BLOCKS_SET_CAPACITY),
            invalidated_blocks: InvalidatedBlocksSet::new(INVALIDATED_BLOCKS_SET_CAPACITY),
        }
    }

    pub fn init_thread(
        &mut self,
        thread_identifier: ThreadIdentifier,
        block_identifier: BlockIdentifier,
    ) {
        tracing::trace!("dependency tracking service init: {thread_identifier:?} {block_identifier:?}, self.states: {:?}", self.states);
        let synced_thread_chain = ThreadChain {
            validation_graph: HashMap::from_iter(vec![(
                block_identifier.clone(),
                vec![(thread_identifier, block_identifier.clone())],
            )]),
            tail: (thread_identifier, block_identifier.clone()),
        };
        self.states.insert(block_identifier, synced_thread_chain);
    }

    pub fn on_block_finalized(
        &mut self,
        parent_block_identifier: BlockIdentifier,
        thread_identifier: ThreadIdentifier,
        block_identifier: BlockIdentifier,
    ) -> anyhow::Result<Vec<Event>, BlockFinalizationHandlingError> {
        let _state = self.states.remove(&block_identifier);
        // TODO: decide if we want to add safety checks like check if state exists
        // and ready for finalization. Safety checks may have negative impact though.
        // For example what happens if we send the same block twice for finalization?
        self.finalized_blocks.insert(block_identifier.clone());

        if self.invalidated_blocks.contains(&block_identifier) {
            return Err(BlockFinalizationHandlingError::CriticalInvalidatedBlockIsFinalized);
        }
        let mut events = vec![];
        for thread_chain in self.states.values_mut() {
            match thread_chain.on_block_finalized(
                parent_block_identifier.clone(),
                thread_identifier,
                block_identifier.clone(),
            ) {
                Ok(()) => {
                    if thread_chain.is_tail_ready_to_be_finalized() {
                        events.push(Event::BlockDependenciesMet(
                            thread_chain.tail_thread_id(),
                            thread_chain.tail_block_id(),
                        ));
                    }
                }
                Err(ThreadChainValidationFailed::AnotherChainFinalizedAtFork) => {
                    events.push(Event::BlockMustBeInvalidated(
                        thread_chain.tail_thread_id(),
                        thread_chain.tail_block_id(),
                    ));
                }
            }
        }

        Ok(events)
    }

    pub fn on_block_invalidated(
        &mut self,
        block_data: BlockData,
    ) -> anyhow::Result<Vec<Event>, BlockInvalidationHandlingError> {
        if self.finalized_blocks.contains(&block_data.block_identifier) {
            return Err(BlockInvalidationHandlingError::CriticalFinalizedBlockIsInvalidated);
        }
        self.invalidated_blocks.insert(block_data.block_identifier.clone());
        self.states.remove(&block_data.block_identifier);

        todo!("check what should be invalidated after this");
    }

    pub fn can_reference(
        &self,
        parent: BlockIdentifier,
        references: Vec<BlockIdentifier>,
    ) -> anyhow::Result<bool, CanReferenceQueryHandlingError> {
        // Note:
        // There must be another check that there's no other block
        // already created in the same thread that is a descendant
        // of the given parent.
        // It must be checked in the producer itself though.

        if let Some(main_thread) = self.states.get(&parent) {
            let mut referenced_states = vec![];
            for e in references {
                let referenced_state = self.states.get(&e).ok_or(
                    CanReferenceQueryHandlingError::ReferencedBlockStateNotFound(e.clone()),
                )?;
                referenced_states.push(referenced_state);
            }
            Ok(ThreadChain::can_merge(main_thread, referenced_states))
        } else {
            Err(CanReferenceQueryHandlingError::QueriedParentBlockStateNotFound(parent))
        }
    }

    pub fn append(&mut self, block_data: BlockData) -> anyhow::Result<(), BlockAppendError> {
        tracing::trace!(
            "dependency tracking service append: {block_data:?}, self.states: {:?}, self.finalized_blocks: {:?}",
            self.states,
            self.finalized_blocks
        );
        if let Some(parent_chain) = self.states.get(&block_data.parent_block_identifier) {
            tracing::trace!("dependency tracking service. continue existing");
            let generated_chain = parent_chain.try_append(&block_data, |e| self.other(e))?;
            self.states.insert(block_data.block_identifier.clone(), generated_chain);
            tracing::trace!("dependency tracking service append success - self: {:?}", self.states);
            Ok(())
        } else if self.finalized_blocks.contains(&block_data.parent_block_identifier) {
            tracing::trace!("dependency tracking service. parent finalized. probably a new thread");
            let finalized_parent_thread_chain = ThreadChain {
                validation_graph: HashMap::default(),
                tail: (block_data.thread_identifier, block_data.parent_block_identifier.clone()),
            };
            let mut generated_chain =
                finalized_parent_thread_chain.try_append(&block_data, |e| self.other(e))?;
            generated_chain.validation_graph.remove(&block_data.parent_block_identifier);
            tracing::trace!("dependency tracking service append success - self: {:?}", self.states);
            self.states.insert(block_data.block_identifier.clone(), generated_chain);
            Ok(())
        } else {
            Err(BlockAppendError::ParentBlockMissing)
        }
    }

    // TODO: this function name can be confusing, need to rename it
    fn other(&self, block_id: &BlockIdentifier) -> OtherThreadChainResult {
        if let Some(state) = self.states.get(block_id) {
            return Ok(state.clone());
        }

        if self.finalized_blocks.contains(block_id) {
            return Err(OtherThreadChainErr::Finalized);
        }
        // TODO: include invalidated chain result

        Err(OtherThreadChainErr::NotFound)
    }
}
