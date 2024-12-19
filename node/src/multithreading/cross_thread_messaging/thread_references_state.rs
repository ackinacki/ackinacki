use std::collections::HashMap;
use std::collections::HashSet;

use serde::Deserialize;
use serde::Serialize;
use typed_builder::TypedBuilder;

use crate::repository::CrossThreadRefData;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

#[derive(TypedBuilder, Clone, Serialize, Deserialize)]
pub struct ThreadReferencesState {
    // Note that ThreadIdentifier is duplicated as a key and in the value.
    // It was intentionally done this way, since it is possible to have a thread
    // starting block (thread split) and no first block of the new thread produced
    // (or referenced) yet.
    pub all_thread_refs: HashMap<ThreadIdentifier, (ThreadIdentifier, BlockIdentifier, BlockSeqNo)>,
}

pub struct ResultingState {
    pub implicitly_referenced_blocks: Vec<(ThreadIdentifier, BlockIdentifier, BlockSeqNo)>,
    // Note: it is a squashed state when a query
    // had several references to the same thread.
    pub explicitly_referenced_blocks: Vec<(ThreadIdentifier, BlockIdentifier, BlockSeqNo)>,
}

pub enum CanRefQueryResult {
    No,
    Yes(ResultingState),
}

impl ThreadReferencesState {
    pub fn can_reference<F>(
        &self,
        explicit_references: Vec<(ThreadIdentifier, BlockIdentifier, BlockSeqNo)>,
        mut get_ref_data: F,
    ) -> anyhow::Result<CanRefQueryResult>
    where
        F: FnMut(&BlockIdentifier) -> anyhow::Result<CrossThreadRefData>,
    {
        if explicit_references.is_empty() {
            return Ok(CanRefQueryResult::Yes(ResultingState {
                explicitly_referenced_blocks: vec![],
                implicitly_referenced_blocks: vec![],
            }));
        }
        let min_referenced_block_seq_no: BlockSeqNo = self
            .all_thread_refs
            .values()
            .map(|e| {
                let block_seq_no: BlockSeqNo = e.2;
                block_seq_no
            })
            .min()
            .expect("Impossible state reached. No references in the state");
        let mut visited_blocks = HashSet::<BlockIdentifier>::new();
        {
            // It is easier to backfill thread blocks here and check visited later
            // rather than do the same in the back tracking loop.
            let mut backfilling: Vec<(ThreadIdentifier, BlockIdentifier, BlockSeqNo)> =
                self.all_thread_refs.values().cloned().collect();
            while let Some(cursor) = backfilling.pop() {
                if visited_blocks.contains(&cursor.1) {
                    continue;
                }
                visited_blocks.insert(cursor.1.clone());
                if min_referenced_block_seq_no > cursor.2 {
                    continue;
                }
                let ref_data = get_ref_data(&cursor.1)?;
                backfilling.push(
                    get_ref_data(ref_data.parent_block_identifier())?.as_reference_state_data(),
                );
                for referenced_block in ref_data.refs() {
                    backfilling.push(get_ref_data(referenced_block)?.as_reference_state_data());
                }
            }
        }

        let mut implicitly_referenced_blocks = vec![];
        let mut backtracking_to_any_known_thread = explicit_references.clone();
        let explicitly_referenced_blocks = {
            let mut e = explicit_references;
            keep_tails(&mut e);
            HashSet::<(ThreadIdentifier, BlockIdentifier, BlockSeqNo)>::from_iter(e)
        };

        while let Some(cursor) = backtracking_to_any_known_thread.pop() {
            let cursor_block_id: BlockIdentifier = cursor.1.clone();
            if visited_blocks.contains(&cursor_block_id) {
                continue;
            }
            visited_blocks.insert(cursor_block_id.clone());
            if !explicitly_referenced_blocks.contains(&cursor) {
                implicitly_referenced_blocks.push(cursor.clone());
            }
            let cursor_block_seq_no: &BlockSeqNo = &cursor.2;
            if &min_referenced_block_seq_no > cursor_block_seq_no {
                // Its a clear cut off for referenced threads.
                // In case some chain goes below this limit we can clearly say that
                // a particular thread can not be referenced by the given references
                return Ok(CanRefQueryResult::No);
            }
            let ref_data = get_ref_data(&cursor_block_id)?;
            backtracking_to_any_known_thread
                .push(get_ref_data(ref_data.parent_block_identifier())?.as_reference_state_data());
            for referenced_block in ref_data.refs() {
                backtracking_to_any_known_thread
                    .push(get_ref_data(referenced_block)?.as_reference_state_data());
            }
        }

        let result = ResultingState {
            implicitly_referenced_blocks,
            explicitly_referenced_blocks: explicitly_referenced_blocks.into_iter().collect(),
        };
        Ok(CanRefQueryResult::Yes(result))
    }

    pub fn move_refs<F>(
        &mut self,
        refs: Vec<(ThreadIdentifier, BlockIdentifier, BlockSeqNo)>,
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
                let phantoms = {
                    let mut e = vec![];
                    let get_ref_data = &mut get_ref_data;
                    for referenced_block in all_refs.iter().map(|e| get_ref_data(&e.1)) {
                        let referenced_block = referenced_block?;
                        let block_identifier = referenced_block.block_identifier().clone();
                        let block_seq_no = *referenced_block.block_seq_no();
                        for spawned_thread in referenced_block.spawned_threads() {
                            e.push((spawned_thread, block_identifier.clone(), block_seq_no));
                        }
                    }
                    e
                };
                // Inserts all new key-values from the iterator and replaces values
                // with existing keys with new values returned from the iterator.
                self.all_thread_refs.extend({
                    let mut e = all_refs.clone();
                    // ensures moved
                    let mut phantoms = phantoms;
                    e.append(&mut phantoms);
                    keep_tails(&mut e);
                    e.into_iter().map(|e| (e.0, e))
                });
                if cfg!(feature = "allow-threads-merge") {
                    #[cfg(feature = "allow-threads-merge")]
                    compile_error!(
                        "needs implementation for the bullet 2 in the notes section above"
                    );
                }
                all_refs
                    .into_iter()
                    .map(|e| get_ref_data(&e.1))
                    .collect::<anyhow::Result<Vec<CrossThreadRefData>>>()
            }
        }
    }
}

fn keep_tails(referenced_blocks: &mut Vec<(ThreadIdentifier, BlockIdentifier, BlockSeqNo)>) {
    referenced_blocks.sort_by(|a, b| {
        let thread_comparison = a.0.cmp(&b.0);
        if thread_comparison != std::cmp::Ordering::Equal {
            return thread_comparison;
        }
        // Make it a descending order for the next dedup operation
        b.1.cmp(&a.1)
    });
    referenced_blocks.dedup_by(|a, b| a.0 == b.0);
}
