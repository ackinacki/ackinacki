use std::collections::HashMap;
use std::collections::VecDeque;

use serde::Deserialize;
use serde::Serialize;
use tracing::instrument;
use typed_builder::TypedBuilder;

use crate::repository::CrossThreadRefData;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct ReferencedBlock {
    pub block_thread_identifier: ThreadIdentifier,
    pub block_identifier: BlockIdentifier,
    pub block_seq_no: BlockSeqNo,
}

impl From<(ThreadIdentifier, BlockIdentifier, BlockSeqNo)> for ReferencedBlock {
    fn from(params: (ThreadIdentifier, BlockIdentifier, BlockSeqNo)) -> Self {
        Self {
            block_thread_identifier: params.0,
            block_identifier: params.1,
            block_seq_no: params.2,
        }
    }
}

#[derive(TypedBuilder, Clone, Serialize, Deserialize, Debug)]
pub struct ThreadReferencesState {
    // Note that ThreadIdentifier is duplicated as a key and in the value.
    // It was intentionally done this way, since it is possible to have a thread
    // starting block (thread split) and no first block of the new thread produced
    // (or referenced) yet.
    // It DOES NOT keep references to dead threads
    // TODO: convert it to a load on demand.
    all_thread_refs: HashMap<ThreadIdentifier, ReferencedBlock>,
}

#[derive(Debug)]
pub struct ResultingState {
    pub implicitly_referenced_blocks: Vec<BlockIdentifier>,
    // Note: it is a squashed state when a query
    // had several references to the same thread.
    pub explicitly_referenced_blocks: Vec<BlockIdentifier>,
}

#[derive(Debug)]
pub enum CanRefQueryResult {
    No,
    Yes(ResultingState),
}

impl ThreadReferencesState {
    pub fn update(
        &mut self,
        thread: ThreadIdentifier,
        referenced_block: impl Into<ReferencedBlock>,
    ) {
        self.all_thread_refs.insert(thread, referenced_block.into());
    }

    #[instrument(skip_all)]
    pub fn can_reference<F>(
        &self,
        explicit_references: Vec<BlockIdentifier>,
        mut get_ref_data: F,
    ) -> anyhow::Result<CanRefQueryResult>
    where
        F: FnMut(&BlockIdentifier) -> anyhow::Result<CrossThreadRefData>,
    {
        tracing::trace!("can_reference: {:?} self: {:?}", explicit_references, self);
        if explicit_references.is_empty() {
            return Ok(CanRefQueryResult::Yes(ResultingState {
                explicitly_referenced_blocks: vec![],
                implicitly_referenced_blocks: vec![],
            }));
        }
        let mut tails = self.all_thread_refs.clone();
        let mut all_referenced_blocks = vec![];
        let mut stack: VecDeque<BlockIdentifier> = explicit_references.into();

        while let Some(cursor) = stack.pop_front() {
            let trail = walk_back_into_history(&cursor, &mut get_ref_data, &tails)?;

            for block in trail.into_iter().rev() {
                let referenced_block: ReferencedBlock = block.as_reference_state_data().into();
                let thread_id = *block.block_thread_identifier();
                match tails.get(&thread_id) {
                    Some(existing) if *block.block_seq_no() > existing.block_seq_no => {
                        tails.insert(thread_id, referenced_block.clone());
                    }
                    None => {
                        tails.insert(thread_id, referenced_block.clone());
                    }
                    _ => {}
                }

                all_referenced_blocks.push(referenced_block);
                stack.extend(block.refs().iter().cloned());
            }
        }

        let previously_referenced_blocks: Vec<_> =
            self.all_thread_refs.values().map(|e| e.block_identifier.clone()).collect();

        let explicitly_referenced_blocks: Vec<_> = tails
            .values()
            .filter(|e| !previously_referenced_blocks.contains(&e.block_identifier))
            .map(|e| e.block_identifier.clone())
            .collect();

        let implicitly_referenced_blocks: Vec<BlockIdentifier> = all_referenced_blocks
            .into_iter()
            .filter(|e| !explicitly_referenced_blocks.contains(&e.block_identifier))
            .map(|e| e.block_identifier)
            .collect();

        let result = ResultingState { explicitly_referenced_blocks, implicitly_referenced_blocks };

        tracing::trace!("can_reference: Yes({result:?})");
        Ok(CanRefQueryResult::Yes(result))
    }

    pub fn move_refs<F>(
        &mut self,
        refs: Vec<BlockIdentifier>,
        mut get_ref_data: F,
    ) -> anyhow::Result<Vec<CrossThreadRefData>>
    where
        F: FnMut(&BlockIdentifier) -> anyhow::Result<CrossThreadRefData>,
    {
        match self.can_reference(refs, &mut get_ref_data)? {
            CanRefQueryResult::No => anyhow::bail!("Can not reference to the given set of refs"),
            CanRefQueryResult::Yes(ResultingState {
                explicitly_referenced_blocks,
                implicitly_referenced_blocks,
            }) => {
                // Notes:
                // - Out of all referenced blocks we must keep references
                //   to blocks with split if no later block was referenced
                //   (store "phantoms" in all_thread_refs)
                // - we must delete references from all_thread_refs that are
                //   the last block in a thread(!) and a successor thread
                //   has a block referencing to the killed thread(!).
                let all_refs = {
                    let mut e = explicitly_referenced_blocks.clone();
                    // Must clone for append operation since it
                    // moves all elements from the other vector.
                    e.append(&mut implicitly_referenced_blocks.clone());
                    e
                };
                {
                    // Add phantoms: blocks that spawn new threads must be placed into the new thread too
                    let get_ref_data = &mut get_ref_data;
                    for referenced_block in all_refs.iter().map(get_ref_data) {
                        let referenced_block = referenced_block?;
                        let block_identifier = referenced_block.block_identifier().clone();
                        let block_seq_no = *referenced_block.block_seq_no();
                        let block_thread_identifier = *referenced_block.block_thread_identifier();
                        for spawned_thread in referenced_block.spawned_threads() {
                            self.all_thread_refs.insert(
                                spawned_thread,
                                (block_thread_identifier, block_identifier.clone(), block_seq_no)
                                    .into(),
                            );
                        }
                    }
                };
                // Inserts all new key-values from the iterator and replaces values
                // with existing keys with new values returned from the iterator.
                self.all_thread_refs.extend({
                    let mut e = all_refs
                        .clone()
                        .into_iter()
                        .map(|e| get_ref_data(&e).expect("must be here"))
                        .map(|e| e.as_reference_state_data())
                        .collect();
                    // ensures moved
                    keep_tails(&mut e);
                    e.into_iter().map(|e| (e.0, e.into()))
                });
                if cfg!(feature = "allow-threads-merge") {
                    #[cfg(feature = "allow-threads-merge")]
                    compile_error!(
                        "needs implementation for the bullet 2 in the notes section above"
                    );
                }
                all_refs
                    .into_iter()
                    .map(|e| get_ref_data(&e))
                    .collect::<anyhow::Result<Vec<CrossThreadRefData>>>()
            }
        }
    }
}

fn walk_back_into_history<F>(
    cursor: &BlockIdentifier,
    mut read: F,
    cutoff: &HashMap<ThreadIdentifier, ReferencedBlock>,
) -> anyhow::Result<Vec<CrossThreadRefData>>
where
    F: FnMut(&BlockIdentifier) -> anyhow::Result<CrossThreadRefData>,
{
    let mut cursor = read(cursor)?;
    let mut trail = vec![];
    loop {
        if let Some(thread_last_block) = cutoff.get(cursor.block_thread_identifier()) {
            if &thread_last_block.block_seq_no >= cursor.block_seq_no() {
                if &thread_last_block.block_seq_no == cursor.block_seq_no() {
                    // Sanity check.
                    assert_eq!(&thread_last_block.block_identifier, cursor.block_identifier());
                }
                return Ok(trail);
            }
        }
        let parent_block_id = cursor.parent_block_identifier().clone();
        trail.push(cursor);
        cursor = read(&parent_block_id)?;
    }
}

fn keep_tails(referenced_blocks: &mut Vec<(ThreadIdentifier, BlockIdentifier, BlockSeqNo)>) {
    let mut tails =
        HashMap::<ThreadIdentifier, (ThreadIdentifier, BlockIdentifier, BlockSeqNo)>::new();
    for (thread, id, seq_no) in referenced_blocks.iter_mut() {
        tails
            .entry(*thread)
            .and_modify(|e| {
                if *seq_no > e.2 {
                    *e = (*thread, id.clone(), *seq_no);
                }
            })
            .or_insert((*thread, id.clone(), *seq_no));
    }
    *referenced_blocks = tails.values().cloned().collect();
}
