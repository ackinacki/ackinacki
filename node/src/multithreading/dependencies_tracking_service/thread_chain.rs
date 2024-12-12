// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::vec::Vec;

use anyhow::bail;
use anyhow::ensure;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::bls::GoshBLS;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;

#[derive(Error, Debug)]
enum BlockAppendError {
    #[error("Thread tail does not match block parent.")]
    TailMistmatch,

    #[error("One of the refs has a conflict with this chain or other ref this block depends on. This block dependencies are impossible.")]
    ConflictingStates,

    #[error("A referenced block was marked as invalid. Probably due to a fork.")]
    InvalidatedRef,

    #[error("A thread chain state for a referenced block is not available.")]
    MissingReferencedState,
}

#[derive(Error, Debug)]
pub enum ThreadChainValidationFailed {
    #[error("It appears that this chain is no longer valid since fork was resolved in a favour of another chain.")]
    AnotherChainFinalizedAtFork,
}

pub enum OtherThreadChain {
    NotFound,
    #[allow(dead_code)]
    Invalidated,
    Finalized,
    Active(ThreadChain),
}

// Note:
// It is not the best implementation in terms of performance. I would even agree
// that it is terrible.
// However at the current moment we require an implementation that does work
// without bugs. Therefore readability is the top priority.

/// Represents a chain of blocks from finalized to the last block in a thread.
/// It ignores other possible forks and is used in the optimistic state.
/// Each block finalized either confirms this state or invalidates it.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThreadChain {
    // Stores edges in a directional graph from root nodes (finalized blocks, inclusive)
    // to the last block in this thread (inclusive).
    // Key is a parent and Value is a list of children relationship.
    // Notes:
    // - Vec should not have same thread identifiers added twice since it will be
    //   an invalid graph (a fork).
    // - It contains tail node aswell. Just to be sure it didn't fork on that particular
    //   block.
    validation_graph: HashMap<BlockIdentifier, Vec<(ThreadIdentifier, BlockIdentifier)>>,
    tail: (ThreadIdentifier, BlockIdentifier),
}

impl ThreadChain {
    /// Continues thread chain validation process.
    pub fn on_block_finalized(&mut self, block: &AckiNackiBlock<GoshBLS>) -> anyhow::Result<()> {
        // Note: parent must be in the list of finalized to continue validation.
        // This chain has no dependency on the block if parent is not in the list.
        // This logic assumes correct order of block finalizations:
        // - a child can not be finalized before parent.
        // - a dependant block can not be finalized before all its refs finalized.
        // A parent block is removed if all its descendants were validated.
        let parent_id = block.parent();
        if let Some(mut descendants) = self.validation_graph.remove(&parent_id) {
            let thread_id = block.get_common_section().thread_id;
            let block_id = block.identifier();
            if let Some(index) = descendants.iter().position(|&(e, _)| e == thread_id) {
                let (_, expected_block_id) = descendants.remove(index);
                ensure!(
                    expected_block_id == block_id,
                    ThreadChainValidationFailed::AnotherChainFinalizedAtFork
                );
            }
            if !descendants.is_empty() {
                self.validation_graph.insert(parent_id, descendants);
            }
        }
        Ok(())
    }

    pub fn tail_block_id(&self) -> BlockIdentifier {
        self.tail.1.clone()
    }

    pub fn tail_thread_id(&self) -> ThreadIdentifier {
        self.tail.0
    }

    /// Checks the state to ensure that the tail block has all it's dependencies finalized.
    pub fn is_tail_ready_to_be_finalized(&self) -> bool {
        self.validation_graph
            .values()
            .all(|descendants| descendants.len() == 1 && descendants[0] == self.tail)
    }

    pub fn can_merge<'a, T>(main_thread: &Self, other: T) -> bool
    where
        T: std::iter::IntoIterator<Item = &'a Self>,
    {
        let mut validation_graph = main_thread.validation_graph.clone();
        for referenced_chain in other.into_iter() {
            for (block_id, descendants) in &referenced_chain.validation_graph {
                if !merge_descendants(&mut validation_graph, block_id, descendants) {
                    return false;
                }
            }
        }
        true
    }

    /// Tries to create a new thread state object for the descendant block.
    pub fn try_append<F>(
        &self,
        block: &AckiNackiBlock<GoshBLS>,
        mut other: F,
    ) -> anyhow::Result<Self>
    where
        F: std::ops::FnMut(&BlockIdentifier) -> anyhow::Result<OtherThreadChain>,
    {
        let parent_id = block.parent();
        ensure!(self.tail.1 == parent_id, BlockAppendError::TailMistmatch);
        let thread_id = block.get_common_section().thread_id;
        let tail = (thread_id, block.identifier());
        let mut state =
            Self { validation_graph: self.validation_graph.clone(), tail: tail.clone() };
        // Append an edge from the old tail to the appended block
        state.validation_graph.insert(parent_id, vec![tail.clone()]);

        // Out of all other states select states that are referenced from the block
        for referenced_block_id in block.get_common_section().refs.iter() {
            match other(referenced_block_id)? {
                OtherThreadChain::Invalidated => bail!(BlockAppendError::InvalidatedRef),
                OtherThreadChain::NotFound => bail!(BlockAppendError::MissingReferencedState),
                OtherThreadChain::Finalized => {
                    state.validation_graph.insert(referenced_block_id.clone(), vec![tail.clone()]);
                }
                OtherThreadChain::Active(referenced_chain) => {
                    // Merge those dependencies into the resulting state.
                    for (block_id, descendants) in &referenced_chain.validation_graph {
                        ensure!(
                            merge_descendants(&mut state.validation_graph, block_id, descendants),
                            BlockAppendError::ConflictingStates
                        );
                    }
                    // And add an edge from the referenced tail to the new block.
                    {
                        ensure!(
                            merge_descendants(
                                &mut state.validation_graph,
                                referenced_block_id,
                                &[tail.clone()]
                            ),
                            BlockAppendError::ConflictingStates
                        );
                    }
                }
            }
        }
        Ok(state)
    }
}

fn merge_descendants(
    validation_graph: &mut HashMap<BlockIdentifier, Vec<(ThreadIdentifier, BlockIdentifier)>>,
    block_id: &BlockIdentifier,
    descendants: &[(ThreadIdentifier, BlockIdentifier)],
) -> bool {
    // Note: a dirty way to ensure there are no forks in the descendants of the node.
    // We remove duplicates first and memo the size.
    // After that we remove pairs by the thread id. If there were more than one entry
    // per thread it's a fork. So we take a control size and check two values afterwards.
    let mut merged = 0usize;
    let mut control = 0usize;
    validation_graph
        .entry(block_id.clone())
        .and_modify(|e| {
            e.extend_from_slice(descendants);
            e.sort();
            e.dedup();
            merged = e.len();
            e.dedup_by_key(|(e, _): &mut (ThreadIdentifier, BlockIdentifier)| *e);
            control = e.len();
        })
        .or_insert(descendants.to_vec());
    merged == control
}

#[cfg(test)]
mod tests {
    #[allow(dead_code)]
    fn it_must_continue_on_a_block_finalized_that_is_in_a_root() {
        // pass
    }
}
