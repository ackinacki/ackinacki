// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::mpsc::Sender;

use anyhow::bail;
use anyhow::ensure;

use super::thread_chain::OtherThreadChain;
use super::thread_chain::ThreadChain;
use super::thread_chain::ThreadChainValidationFailed;
use super::FinalizedBlocksSet;
use super::InvalidatedBlocksSet;
use crate::bls::GoshBLS;
use crate::types::AckiNackiBlock;
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

#[derive(Clone)]
pub enum Event {
    BlockMustBeInvalidated(ThreadIdentifier, BlockIdentifier),
    BlockDependenciesMet(ThreadIdentifier, BlockIdentifier),
}

pub struct DependenciesTrackingServiceContext {
    states: HashMap<BlockIdentifier, ThreadChain>,
    subscribers: Vec<Sender<Event>>,
    finalized_blocks: FinalizedBlocksSet,
    invalidated_blocks: InvalidatedBlocksSet,
}

impl DependenciesTrackingServiceContext {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
            subscribers: vec![],
            finalized_blocks: FinalizedBlocksSet::new(FINALIZED_BLOCKS_SET_CAPACITY),
            invalidated_blocks: InvalidatedBlocksSet::new(INVALIDATED_BLOCKS_SET_CAPACITY),
        }
    }

    pub fn subscribe(&mut self, subscriber: Sender<Event>) {
        self.subscribers.push(subscriber);
    }

    pub fn on_block_finalized(&mut self, block: &AckiNackiBlock<GoshBLS>) -> anyhow::Result<()> {
        let _state = self.states.remove(&block.identifier());
        // TODO: decide if we want to add safety checks like check if state exists
        // and ready for finalization. Safety checks may have negative impact though.
        // For example what happens if we send the same block twice for finalization?
        self.finalized_blocks.insert(block.identifier());
        ensure!(
            !self.invalidated_blocks.contains(&block.identifier()),
            "Broken state detected: Invalidated block is finalized."
        );
        let mut events = vec![];
        for thread_chain in self.states.values_mut() {
            match thread_chain.on_block_finalized(block) {
                Ok(()) => {
                    if thread_chain.is_tail_ready_to_be_finalized() {
                        events.push(Event::BlockDependenciesMet(
                            thread_chain.tail_thread_id(),
                            thread_chain.tail_block_id(),
                        ));
                    }
                }
                Err(e) => match e.downcast_ref::<ThreadChainValidationFailed>() {
                    Some(ThreadChainValidationFailed::AnotherChainFinalizedAtFork) => {
                        events.push(Event::BlockMustBeInvalidated(
                            thread_chain.tail_thread_id(),
                            thread_chain.tail_block_id(),
                        ));
                    }
                    None => bail!(e),
                },
            }
        }

        for e in events {
            self.dispatch(e)?;
        }
        Ok(())
    }

    pub fn on_block_invalidated(&mut self, block: &AckiNackiBlock<GoshBLS>) -> anyhow::Result<()> {
        ensure!(
            !self.finalized_blocks.contains(&block.identifier()),
            "Broken state detected: A block that was finalized is invalidated now."
        );
        self.invalidated_blocks.insert(block.identifier());
        self.states.remove(&block.identifier());

        todo!();
    }

    pub fn can_reference(
        &self,
        parent: BlockIdentifier,
        _thread: ThreadIdentifier,
        references: Vec<BlockIdentifier>,
    ) -> bool {
        let main_thread = self.states.get(&parent).unwrap();
        let mut referenced_states = vec![];
        for e in references {
            referenced_states.push(self.states.get(&e).unwrap());
        }
        ThreadChain::can_merge(main_thread, referenced_states)
        // Note:
        // There must be another check that there's no other block
        // already created in the same thread that is a descendant
        // of the given parent.
        // It must be checked in the producer itself though.
    }

    pub fn append(&mut self, block: &AckiNackiBlock<GoshBLS>) -> anyhow::Result<()> {
        if let Some(parent_chain) = self.states.get(&block.parent()) {
            let generated_chain = parent_chain.try_append(block, |e| self.other(e))?;
            self.states.insert(block.identifier(), generated_chain);
        } else {
            bail!("Parent is not ready");
        }
        Ok(())
    }

    fn other(&self, block_id: &BlockIdentifier) -> anyhow::Result<OtherThreadChain> {
        if let Some(state) = self.states.get(block_id) {
            return Ok(OtherThreadChain::Active(state.clone()));
        }

        if self.finalized_blocks.contains(block_id) {
            return Ok(OtherThreadChain::Finalized);
        }

        Ok(OtherThreadChain::NotFound)
    }

    fn dispatch(&self, event: Event) -> anyhow::Result<()> {
        for subscriber in &self.subscribers {
            match subscriber.send(event.clone()) {
                Ok(()) => {}
                Err(_e) => todo!("remove channel in case of receiver is no longer alive"),
            }
        }
        Ok(())
    }
}
