use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use serde::Deserialize;
use serde::Serialize;
use tracing::instrument;
use typed_builder::TypedBuilder;

/// Block sequence number type. This is the same as TVM block seq no (u32).
#[derive(Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, Default, PartialOrd, Ord)]
pub struct BlockSeqNo(u32);

impl BlockSeqNo {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn value(&self) -> u32 {
        self.0
    }

    pub fn saturating_sub(self, other: u32) -> Self {
        Self(self.0.saturating_sub(other))
    }
}

impl From<u32> for BlockSeqNo {
    fn from(seq_no: u32) -> Self {
        Self(seq_no)
    }
}

impl From<BlockSeqNo> for u32 {
    fn from(val: BlockSeqNo) -> Self {
        val.0
    }
}

impl std::fmt::Display for BlockSeqNo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Debug for BlockSeqNo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Sub for BlockSeqNo {
    type Output = i64;

    fn sub(self, other: Self) -> Self::Output {
        i64::from(self.0) - i64::from(other.0)
    }
}

impl std::ops::Sub<&BlockSeqNo> for BlockSeqNo {
    type Output = i64;

    fn sub(self, other: &Self) -> Self::Output {
        i64::from(self.0) - i64::from(other.0)
    }
}

impl std::ops::Add<u32> for BlockSeqNo {
    type Output = Self;

    fn add(self, other: u32) -> Self::Output {
        Self(self.0 + other)
    }
}

impl std::ops::Rem<u32> for BlockSeqNo {
    type Output = u32;

    fn rem(self, rhs: u32) -> Self::Output {
        self.0 % rhs
    }
}

pub fn next_seq_no(seq_no: BlockSeqNo) -> BlockSeqNo {
    BlockSeqNo(seq_no.0 + 1)
}

/// Trait for cross-thread reference data that can be used with ThreadReferencesState.
/// This trait abstracts the minimum interface needed for reference tracking.
pub trait CrossThreadRefDataTrait {
    fn block_identifier(&self) -> &BlockIdentifier;
    fn block_seq_no(&self) -> &BlockSeqNo;
    fn block_thread_identifier(&self) -> &ThreadIdentifier;
    fn parent_block_identifier(&self) -> &BlockIdentifier;
    fn refs(&self) -> &Vec<BlockIdentifier>;
    fn spawned_threads(&self) -> Vec<ThreadIdentifier>;
    fn as_reference_state_data(&self) -> (ThreadIdentifier, BlockIdentifier, BlockSeqNo);
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct ReferencedBlock {
    pub block_thread_identifier: ThreadIdentifier,
    pub block_identifier: BlockIdentifier,
    pub block_seq_no: BlockSeqNo,
}

impl<T> From<(ThreadIdentifier, BlockIdentifier, T)> for ReferencedBlock
where
    T: Into<BlockSeqNo>,
{
    fn from(params: (ThreadIdentifier, BlockIdentifier, T)) -> Self {
        Self {
            block_thread_identifier: params.0,
            block_identifier: params.1,
            block_seq_no: params.2.into(),
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
    pub fn all_thread_refs(&self) -> &HashMap<ThreadIdentifier, ReferencedBlock> {
        &self.all_thread_refs
    }

    pub fn update(
        &mut self,
        thread: ThreadIdentifier,
        referenced_block: impl Into<ReferencedBlock>,
    ) {
        self.all_thread_refs.insert(thread, referenced_block.into());
    }

    #[instrument(skip_all)]
    pub fn can_reference<T, F>(
        &self,
        explicit_references: Vec<BlockIdentifier>,
        mut get_ref_data: F,
    ) -> anyhow::Result<CanRefQueryResult>
    where
        T: CrossThreadRefDataTrait,
        F: FnMut(&BlockIdentifier) -> anyhow::Result<T>,
    {
        tracing::trace!("can_reference: {:?} self: {:?}", explicit_references, self);
        if explicit_references.is_empty() {
            return Ok(CanRefQueryResult::Yes(ResultingState {
                explicitly_referenced_blocks: vec![],
                implicitly_referenced_blocks: vec![],
            }));
        }
        let mut tails = self.all_thread_refs.clone();
        let mut stack: VecDeque<BlockIdentifier> = explicit_references.into();
        let mut all_referenced_blocks = vec![];
        let previously_referenced_set: HashSet<_> =
            self.all_thread_refs.values().map(|e| e.block_identifier).collect();

        let mut explicitly_referenced_set = HashSet::new();
        while let Some(cursor) = stack.pop_front() {
            let trail = walk_back_into_history(&cursor, &mut get_ref_data, &tails)?;
            for block in trail.into_iter().rev() {
                let referenced_block: ReferencedBlock = block.as_reference_state_data().into();
                let thread_id = *block.block_thread_identifier();
                let should_update = match tails.get(&thread_id) {
                    Some(existing) => *block.block_seq_no() > existing.block_seq_no,
                    None => true,
                };
                if should_update {
                    tails.insert(thread_id, referenced_block.clone());
                    if !previously_referenced_set.contains(&referenced_block.block_identifier) {
                        explicitly_referenced_set.insert(referenced_block.block_identifier);
                    }
                }
                all_referenced_blocks.push(referenced_block);
                stack.extend(block.refs().iter().cloned());
            }
        }
        let explicitly_referenced_blocks: Vec<_> = explicitly_referenced_set.into_iter().collect();
        let explicit_set: HashSet<_> = explicitly_referenced_blocks.iter().cloned().collect();
        let implicitly_referenced_blocks: Vec<_> = all_referenced_blocks
            .into_iter()
            .filter(|e| !explicit_set.contains(&e.block_identifier))
            .map(|e| e.block_identifier)
            .collect();

        let result = ResultingState { explicitly_referenced_blocks, implicitly_referenced_blocks };

        tracing::trace!("can_reference: Yes({result:?})");
        Ok(CanRefQueryResult::Yes(result))
    }

    pub fn move_refs<T, F>(
        &mut self,
        refs: Vec<BlockIdentifier>,
        mut get_ref_data: F,
    ) -> anyhow::Result<Vec<T>>
    where
        T: CrossThreadRefDataTrait,
        F: FnMut(&BlockIdentifier) -> anyhow::Result<T>,
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
                        let block_identifier = *referenced_block.block_identifier();
                        let block_seq_no = *referenced_block.block_seq_no();
                        let block_thread_identifier = *referenced_block.block_thread_identifier();
                        for spawned_thread in referenced_block.spawned_threads() {
                            self.all_thread_refs.insert(
                                spawned_thread,
                                (block_thread_identifier, block_identifier, block_seq_no).into(),
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
                all_refs.into_iter().map(|e| get_ref_data(&e)).collect::<anyhow::Result<Vec<T>>>()
            }
        }
    }
}

fn walk_back_into_history<T, F>(
    cursor: &BlockIdentifier,
    mut read: F,
    cutoff: &HashMap<ThreadIdentifier, ReferencedBlock>,
) -> anyhow::Result<Vec<T>>
where
    T: CrossThreadRefDataTrait,
    F: FnMut(&BlockIdentifier) -> anyhow::Result<T>,
{
    let mut cursor = read(cursor)?;
    let mut trail = vec![];
    loop {
        if let Some(thread_last_block) = cutoff.get(cursor.block_thread_identifier()) {
            if &thread_last_block.block_seq_no >= cursor.block_seq_no() {
                if &thread_last_block.block_seq_no == cursor.block_seq_no() {
                    // Sanity check.
                    // Note: Changed panic to error. This can happen when this node has produced a block, and fork was finalized.
                    // Production process was triggered to stop, but it can race to start new block production and produce block (after the one that has lost the fork choice).
                    // In this case producing block will be based on the invalid state.
                    anyhow::ensure!(
                        &thread_last_block.block_identifier == cursor.block_identifier(),
                        "Wrong state refs"
                    );
                }
                return Ok(trail);
            }
        }
        let parent_block_id = *cursor.parent_block_identifier();
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
                    *e = (*thread, *id, *seq_no);
                }
            })
            .or_insert((*thread, *id, *seq_no));
    }
    *referenced_blocks = tails.values().cloned().collect();
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    // Mock implementation of CrossThreadRefDataTrait for testing
    #[derive(Clone, Debug)]
    struct MockRefData {
        block_identifier: BlockIdentifier,
        block_seq_no: BlockSeqNo,
        block_thread_identifier: ThreadIdentifier,
        parent_block_identifier: BlockIdentifier,
        refs: Vec<BlockIdentifier>,
        spawned_threads: Vec<ThreadIdentifier>,
    }

    impl CrossThreadRefDataTrait for MockRefData {
        fn block_identifier(&self) -> &BlockIdentifier {
            &self.block_identifier
        }

        fn block_seq_no(&self) -> &BlockSeqNo {
            &self.block_seq_no
        }

        fn block_thread_identifier(&self) -> &ThreadIdentifier {
            &self.block_thread_identifier
        }

        fn parent_block_identifier(&self) -> &BlockIdentifier {
            &self.parent_block_identifier
        }

        fn refs(&self) -> &Vec<BlockIdentifier> {
            &self.refs
        }

        fn spawned_threads(&self) -> Vec<ThreadIdentifier> {
            self.spawned_threads.clone()
        }

        fn as_reference_state_data(&self) -> (ThreadIdentifier, BlockIdentifier, BlockSeqNo) {
            (self.block_thread_identifier, self.block_identifier, self.block_seq_no)
        }
    }

    // Helper to create test identifiers
    fn test_block_id(id: u8) -> BlockIdentifier {
        BlockIdentifier::new([id; 32])
    }

    fn test_thread_id(id: u8) -> ThreadIdentifier {
        ThreadIdentifier::from([id; 34])
    }

    // Tests for BlockSeqNo
    #[test]
    fn test_block_seq_no_basic_operations() {
        let seq1 = BlockSeqNo::new(10);
        let seq2 = BlockSeqNo::new(5);

        assert_eq!(seq1.value(), 10);
        assert_eq!(seq2.value(), 5);
        assert!(seq1 > seq2);
        assert!(seq1 >= seq2);
        assert!(seq2 < seq1);
        assert_eq!(seq1, BlockSeqNo::from(10));
    }

    #[test]
    fn test_block_seq_no_arithmetic() {
        let seq = BlockSeqNo::new(100);

        // Addition
        assert_eq!(seq + 50, BlockSeqNo::new(150));

        // Subtraction (returns i64)
        assert_eq!(seq - BlockSeqNo::new(30), 70i64);
        assert_eq!(BlockSeqNo::new(30) - seq, -70i64);

        // Remainder
        assert_eq!(seq % 30, 10u32);

        // Saturating subtraction
        assert_eq!(seq.saturating_sub(30), BlockSeqNo::new(70));
        assert_eq!(seq.saturating_sub(200), BlockSeqNo::new(0));
    }

    #[test]
    fn test_block_seq_no_conversions() {
        let seq = BlockSeqNo::new(42);
        let u: u32 = seq.into();
        assert_eq!(u, 42);

        let seq2: BlockSeqNo = 42u32.into();
        assert_eq!(seq, seq2);
    }

    #[test]
    fn test_block_seq_no_display() {
        let seq = BlockSeqNo::new(123);
        assert_eq!(format!("{}", seq), "123");
        assert_eq!(format!("{:?}", seq), "123");
    }

    #[test]
    fn test_block_seq_no_default() {
        let seq = BlockSeqNo::default();
        assert_eq!(seq.value(), 0);
    }

    #[test]
    fn test_next_seq_no() {
        let seq = BlockSeqNo::new(10);
        let next = next_seq_no(seq);
        assert_eq!(next, BlockSeqNo::new(11));
    }

    // Tests for ReferencedBlock
    #[test]
    fn test_referenced_block_from_tuple() {
        let thread_id = test_thread_id(1);
        let block_id = test_block_id(1);
        let seq_no = BlockSeqNo::new(10);

        let ref_block: ReferencedBlock = (thread_id, block_id, seq_no).into();

        assert_eq!(ref_block.block_thread_identifier, thread_id);
        assert_eq!(ref_block.block_identifier, block_id);
        assert_eq!(ref_block.block_seq_no, seq_no);
    }

    #[test]
    fn test_referenced_block_from_tuple_with_u32() {
        let thread_id = test_thread_id(1);
        let block_id = test_block_id(1);
        let seq_no = 10u32;

        let ref_block: ReferencedBlock = (thread_id, block_id, seq_no).into();

        assert_eq!(ref_block.block_thread_identifier, thread_id);
        assert_eq!(ref_block.block_identifier, block_id);
        assert_eq!(ref_block.block_seq_no, BlockSeqNo::new(10));
    }

    #[test]
    fn test_referenced_block_equality() {
        let thread_id = test_thread_id(1);
        let block_id = test_block_id(1);

        let ref1: ReferencedBlock = (thread_id, block_id, 10u32).into();
        let ref2: ReferencedBlock = (thread_id, block_id, 10u32).into();
        let ref3: ReferencedBlock = (thread_id, block_id, 11u32).into();

        assert_eq!(ref1, ref2);
        assert_ne!(ref1, ref3);
    }

    // Tests for ThreadReferencesState
    #[test]
    fn test_thread_references_state_empty() {
        let state = ThreadReferencesState::builder().all_thread_refs(HashMap::new()).build();

        assert_eq!(state.all_thread_refs().len(), 0);
    }

    #[test]
    fn test_thread_references_state_update() {
        let mut state = ThreadReferencesState::builder().all_thread_refs(HashMap::new()).build();

        let thread_id = test_thread_id(1);
        let block_id = test_block_id(1);
        let seq_no = BlockSeqNo::new(10);

        state.update(thread_id, (thread_id, block_id, seq_no));

        assert_eq!(state.all_thread_refs().len(), 1);
        let ref_block = state.all_thread_refs().get(&thread_id).unwrap();
        assert_eq!(ref_block.block_identifier, block_id);
        assert_eq!(ref_block.block_seq_no, seq_no);
    }

    #[test]
    fn test_thread_references_state_update_overwrites() {
        let mut state = ThreadReferencesState::builder().all_thread_refs(HashMap::new()).build();

        let thread_id = test_thread_id(1);
        let block_id1 = test_block_id(1);
        let block_id2 = test_block_id(2);

        state.update(thread_id, (thread_id, block_id1, 10u32));
        state.update(thread_id, (thread_id, block_id2, 20u32));

        assert_eq!(state.all_thread_refs().len(), 1);
        let ref_block = state.all_thread_refs().get(&thread_id).unwrap();
        assert_eq!(ref_block.block_identifier, block_id2);
        assert_eq!(ref_block.block_seq_no, BlockSeqNo::new(20));
    }

    #[test]
    fn test_can_reference_empty_refs() {
        let state = ThreadReferencesState::builder().all_thread_refs(HashMap::new()).build();

        let mut _call_count = 0;
        let result = state.can_reference::<MockRefData, _>(vec![], |_| {
            _call_count += 1;
            unreachable!("Should not be called for empty refs")
        });

        assert!(result.is_ok());
        match result.unwrap() {
            CanRefQueryResult::Yes(resulting_state) => {
                assert!(resulting_state.explicitly_referenced_blocks.is_empty());
                assert!(resulting_state.implicitly_referenced_blocks.is_empty());
            }
            CanRefQueryResult::No => panic!("Expected Yes result"),
        }
    }

    #[test]
    fn test_can_reference_single_block_no_history() {
        let thread_id = test_thread_id(1);
        let block_id = test_block_id(1);
        let parent_id = test_block_id(0);

        // Create genesis block and current block
        let blocks = HashMap::from([
            (
                parent_id,
                MockRefData {
                    block_identifier: parent_id,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: parent_id, // Genesis points to itself
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id,
                MockRefData {
                    block_identifier: block_id,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: parent_id,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
        ]);

        // Set state with genesis block as cutoff
        let mut initial_refs = HashMap::new();
        initial_refs.insert(thread_id, (thread_id, parent_id, BlockSeqNo::new(0)).into());

        let state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        let result = state.can_reference::<MockRefData, _>(vec![block_id], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
        });

        assert!(result.is_ok());
        match result.unwrap() {
            CanRefQueryResult::Yes(resulting_state) => {
                assert_eq!(resulting_state.explicitly_referenced_blocks.len(), 1);
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_id),
                    "Should contain block_id"
                );
                assert!(
                    !resulting_state.explicitly_referenced_blocks.contains(&parent_id),
                    "Should NOT contain parent_id (genesis/cutoff)"
                );
                assert!(resulting_state.implicitly_referenced_blocks.is_empty());
            }
            CanRefQueryResult::No => panic!("Expected Yes result"),
        }
    }

    #[test]
    fn test_can_reference_with_history() {
        let thread_id = test_thread_id(1);
        let block_id_0 = test_block_id(0);
        let block_id_1 = test_block_id(1);
        let block_id_2 = test_block_id(2);
        let block_id_3 = test_block_id(3);

        // Block 3 -> Block 2 -> Block 1 -> Block 0 (genesis)
        let blocks = HashMap::from([
            (
                block_id_0,
                MockRefData {
                    block_identifier: block_id_0,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: block_id_0, // Genesis points to itself
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id_1,
                MockRefData {
                    block_identifier: block_id_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: block_id_0,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id_2,
                MockRefData {
                    block_identifier: block_id_2,
                    block_seq_no: BlockSeqNo::new(2),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: block_id_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id_3,
                MockRefData {
                    block_identifier: block_id_3,
                    block_seq_no: BlockSeqNo::new(3),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: block_id_2,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
        ]);

        // Set genesis as cutoff
        let mut initial_refs = HashMap::new();
        initial_refs.insert(thread_id, (thread_id, block_id_0, BlockSeqNo::new(0)).into());

        let state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        let result = state.can_reference::<MockRefData, _>(vec![block_id_3], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
        });

        assert!(result.is_ok());
        match result.unwrap() {
            CanRefQueryResult::Yes(resulting_state) => {
                // Blocks 1, 2, and 3 should be explicitly referenced (not 0, which is the cutoff)
                assert_eq!(
                    resulting_state.explicitly_referenced_blocks.len(),
                    3,
                    "Expected 3 explicit blocks"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_id_1),
                    "Block 1 should be explicitly referenced"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_id_2),
                    "Block 2 should be explicitly referenced"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_id_3),
                    "Block 3 should be explicitly referenced"
                );
                assert!(
                    !resulting_state.explicitly_referenced_blocks.contains(&block_id_0),
                    "Block 0 should NOT be referenced (genesis/cutoff)"
                );

                // No implicit references (no cross-thread refs)
                assert!(
                    resulting_state.implicitly_referenced_blocks.is_empty(),
                    "Expected no implicit blocks in simple history chain"
                );
            }
            CanRefQueryResult::No => panic!("Expected Yes result"),
        }
    }

    #[test]
    fn test_can_reference_with_cutoff() {
        let thread_id = test_thread_id(1);
        let block_id_1 = test_block_id(1);
        let block_id_2 = test_block_id(2);
        let block_id_3 = test_block_id(3);

        // Block 3 -> Block 2 -> Block 1
        let blocks = HashMap::from([
            (
                block_id_3,
                MockRefData {
                    block_identifier: block_id_3,
                    block_seq_no: BlockSeqNo::new(3),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: block_id_2,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id_2,
                MockRefData {
                    block_identifier: block_id_2,
                    block_seq_no: BlockSeqNo::new(2),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: block_id_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id_1,
                MockRefData {
                    block_identifier: block_id_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: test_block_id(0),
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
        ]);

        // State already has block 1 referenced
        let mut initial_refs = HashMap::new();
        initial_refs.insert(thread_id, (thread_id, block_id_1, BlockSeqNo::new(1)).into());

        let state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        let result = state.can_reference::<MockRefData, _>(vec![block_id_3], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
        });

        assert!(result.is_ok());
        match result.unwrap() {
            CanRefQueryResult::Yes(resulting_state) => {
                // Only blocks 2 and 3 should be explicitly referenced (1 is already referenced as cutoff)
                assert_eq!(
                    resulting_state.explicitly_referenced_blocks.len(),
                    2,
                    "Expected 2 explicit blocks (block 1 is cutoff)"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_id_2),
                    "Block 2 should be explicitly referenced"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_id_3),
                    "Block 3 should be explicitly referenced"
                );
                assert!(
                    !resulting_state.explicitly_referenced_blocks.contains(&block_id_1),
                    "Block 1 should NOT be referenced (cutoff)"
                );

                // No implicit references (no cross-thread refs, all in same chain)
                assert!(
                    resulting_state.implicitly_referenced_blocks.is_empty(),
                    "Expected no implicit blocks in single-thread chain"
                );
            }
            CanRefQueryResult::No => panic!("Expected Yes result"),
        }
    }

    #[test]
    fn test_can_reference_with_cross_thread_refs() {
        let thread_id_1 = test_thread_id(1);
        let thread_id_2 = test_thread_id(2);
        let genesis_1 = test_block_id(10);
        let genesis_2 = test_block_id(20);
        let block_id_1_1 = test_block_id(1);
        let block_id_2_1 = test_block_id(2);
        let block_id_2_2 = test_block_id(3);

        // Thread 2, Block 2 references Thread 2, Block 1
        // Thread 2, Block 1 references Thread 1, Block 1
        let blocks = HashMap::from([
            (
                genesis_1,
                MockRefData {
                    block_identifier: genesis_1,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_id_1,
                    parent_block_identifier: genesis_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                genesis_2,
                MockRefData {
                    block_identifier: genesis_2,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_id_2,
                    parent_block_identifier: genesis_2,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id_1_1,
                MockRefData {
                    block_identifier: block_id_1_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_id_1,
                    parent_block_identifier: genesis_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id_2_1,
                MockRefData {
                    block_identifier: block_id_2_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_id_2,
                    parent_block_identifier: genesis_2,
                    refs: vec![block_id_1_1],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id_2_2,
                MockRefData {
                    block_identifier: block_id_2_2,
                    block_seq_no: BlockSeqNo::new(2),
                    block_thread_identifier: thread_id_2,
                    parent_block_identifier: block_id_2_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
        ]);

        // Set genesis blocks as cutoffs
        let mut initial_refs = HashMap::new();
        initial_refs.insert(thread_id_1, (thread_id_1, genesis_1, BlockSeqNo::new(0)).into());
        initial_refs.insert(thread_id_2, (thread_id_2, genesis_2, BlockSeqNo::new(0)).into());

        let state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        let result = state.can_reference::<MockRefData, _>(vec![block_id_2_2], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
        });

        assert!(result.is_ok());
        match result.unwrap() {
            CanRefQueryResult::Yes(resulting_state) => {
                // Total: 3 blocks should be referenced
                let total_refs = resulting_state.explicitly_referenced_blocks.len()
                    + resulting_state.implicitly_referenced_blocks.len();
                assert_eq!(total_refs, 3, "Expected 3 total referenced blocks");

                println!("Potato2 {:?}", resulting_state.implicitly_referenced_blocks);
                // Explicitly: block_id_2_2 and block_id_2_1 (directly in query chain)
                assert_eq!(
                    resulting_state.explicitly_referenced_blocks.len(),
                    3,
                    "Expected 3 explicit blocks"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_id_2_2),
                    "Block 2.2 should be explicitly referenced"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_id_2_1),
                    "Block 2.1 should be explicitly referenced"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_id_1_1),
                    "Block 1.1 should be explicitly referenced"
                );
                assert!(
                    !resulting_state.explicitly_referenced_blocks.contains(&genesis_1),
                    "Genesis 1 should NOT be explicitly referenced"
                );
                assert!(
                    !resulting_state.explicitly_referenced_blocks.contains(&genesis_2),
                    "Genesis 2 should NOT be explicitly referenced"
                );

                // Implicitly: block_id_1_1 (pulled in via cross-thread ref from block_id_2_1)
                assert_eq!(
                    resulting_state.implicitly_referenced_blocks.len(),
                    0,
                    "Implicit blocks should be empty"
                );
            }
            CanRefQueryResult::No => panic!("Expected Yes result"),
        }
    }

    #[test]
    fn test_move_refs_basic() {
        let thread_id = test_thread_id(1);
        let genesis_id = test_block_id(0);
        let block_id = test_block_id(1);

        let blocks = HashMap::from([
            (
                genesis_id,
                MockRefData {
                    block_identifier: genesis_id,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: genesis_id,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id,
                MockRefData {
                    block_identifier: block_id,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: genesis_id,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
        ]);

        // Set genesis as cutoff
        let mut initial_refs = HashMap::new();
        initial_refs.insert(thread_id, (thread_id, genesis_id, BlockSeqNo::new(0)).into());

        let mut state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        let result = state.move_refs::<MockRefData, _>(vec![block_id], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
        });

        assert!(result.is_ok());
        let returned_blocks = result.unwrap();
        assert_eq!(returned_blocks.len(), 1, "Expected 1 returned block");
        assert_eq!(
            returned_blocks[0].block_identifier, block_id,
            "Returned block should be block_id"
        );

        // Genesis should NOT be in returned blocks
        assert!(
            !returned_blocks.iter().any(|b| b.block_identifier == genesis_id),
            "Genesis block should NOT be in returned blocks"
        );

        // Check that state was updated
        assert_eq!(state.all_thread_refs().len(), 1);
        let ref_block = state.all_thread_refs().get(&thread_id).unwrap();
        assert_eq!(ref_block.block_identifier, block_id);
    }

    #[test]
    fn test_move_refs_with_spawned_threads() {
        let thread_id_1 = test_thread_id(1);
        let thread_id_2 = test_thread_id(2);
        let genesis_id = test_block_id(0);
        let block_id = test_block_id(1);

        let blocks = HashMap::from([
            (
                genesis_id,
                MockRefData {
                    block_identifier: genesis_id,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_id_1,
                    parent_block_identifier: genesis_id,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id,
                MockRefData {
                    block_identifier: block_id,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_id_1,
                    parent_block_identifier: genesis_id,
                    refs: vec![],
                    spawned_threads: vec![thread_id_2],
                },
            ),
        ]);

        // Set genesis as cutoff
        let mut initial_refs = HashMap::new();
        initial_refs.insert(thread_id_1, (thread_id_1, genesis_id, BlockSeqNo::new(0)).into());

        let mut state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        let result = state.move_refs::<MockRefData, _>(vec![block_id], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
        });

        assert!(result.is_ok());
        let returned_blocks = result.unwrap();

        // Should return 1 block (the one we queried)
        assert_eq!(returned_blocks.len(), 1, "Expected 1 returned block");
        assert_eq!(
            returned_blocks[0].block_identifier, block_id,
            "Returned block should be block_id"
        );

        // Genesis should NOT be in returned blocks
        assert!(
            !returned_blocks.iter().any(|b| b.block_identifier == genesis_id),
            "Genesis block should NOT be in returned blocks"
        );

        // Both thread 1 and spawned thread 2 should have references
        assert_eq!(state.all_thread_refs().len(), 2);
        assert!(state.all_thread_refs().contains_key(&thread_id_1));
        assert!(state.all_thread_refs().contains_key(&thread_id_2));

        // The spawned thread should reference the same block
        let ref_block_2 = state.all_thread_refs().get(&thread_id_2).unwrap();
        assert_eq!(ref_block_2.block_identifier, block_id);
    }

    #[test]
    fn test_keep_tails_function() {
        let thread_id_1 = test_thread_id(1);
        let thread_id_2 = test_thread_id(2);
        let block_id_1 = test_block_id(1);
        let block_id_2 = test_block_id(2);
        let block_id_3 = test_block_id(3);

        let mut blocks = vec![
            (thread_id_1, block_id_1, BlockSeqNo::new(1)),
            (thread_id_1, block_id_2, BlockSeqNo::new(2)),
            (thread_id_1, block_id_3, BlockSeqNo::new(3)),
            (thread_id_2, block_id_1, BlockSeqNo::new(1)),
            (thread_id_2, block_id_2, BlockSeqNo::new(2)),
        ];

        keep_tails(&mut blocks);

        // Should keep only the highest seq_no for each thread
        assert_eq!(blocks.len(), 2);

        let thread_1_blocks: Vec<_> = blocks.iter().filter(|(t, _, _)| *t == thread_id_1).collect();
        assert_eq!(thread_1_blocks.len(), 1);
        assert_eq!(thread_1_blocks[0].1, block_id_3);
        assert_eq!(thread_1_blocks[0].2, BlockSeqNo::new(3));

        let thread_2_blocks: Vec<_> = blocks.iter().filter(|(t, _, _)| *t == thread_id_2).collect();
        assert_eq!(thread_2_blocks.len(), 1);
        assert_eq!(thread_2_blocks[0].1, block_id_2);
        assert_eq!(thread_2_blocks[0].2, BlockSeqNo::new(2));
    }

    #[test]
    fn test_keep_tails_single_thread() {
        let thread_id = test_thread_id(1);
        let block_id = test_block_id(1);

        let mut blocks = vec![(thread_id, block_id, BlockSeqNo::new(5))];

        keep_tails(&mut blocks);

        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].2, BlockSeqNo::new(5));
    }

    #[test]
    fn test_keep_tails_empty() {
        let mut blocks: Vec<(ThreadIdentifier, BlockIdentifier, BlockSeqNo)> = vec![];
        keep_tails(&mut blocks);
        assert_eq!(blocks.len(), 0);
    }

    #[test]
    fn test_walk_back_history_with_cutoff_error() {
        let thread_id = test_thread_id(1);
        let block_id_1 = test_block_id(1);
        let block_id_2 = test_block_id(2);
        let wrong_block_id = test_block_id(99);

        // Block 2 should link to block 1, but we have wrong_block_id in cutoff
        let blocks = HashMap::from([(
            block_id_2,
            MockRefData {
                block_identifier: block_id_2,
                block_seq_no: BlockSeqNo::new(2),
                block_thread_identifier: thread_id,
                parent_block_identifier: block_id_1,
                refs: vec![],
                spawned_threads: vec![],
            },
        )]);

        let mut cutoff = HashMap::new();
        cutoff.insert(thread_id, (thread_id, wrong_block_id, BlockSeqNo::new(2)).into());

        let result = walk_back_into_history(
            &block_id_2,
            |id| blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found")),
            &cutoff,
        );

        // Should return error because block_identifier doesn't match
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Wrong state refs"));
    }

    #[test]
    fn test_serialization_deserialization() {
        let thread_id = test_thread_id(1);
        let block_id = test_block_id(1);
        let seq_no = BlockSeqNo::new(10);

        let ref_block: ReferencedBlock = (thread_id, block_id, seq_no).into();

        let serialized = bincode::serialize(&ref_block).unwrap();
        let deserialized: ReferencedBlock = bincode::deserialize(&serialized).unwrap();

        assert_eq!(ref_block, deserialized);
    }

    #[test]
    fn test_thread_references_state_serialization() {
        let thread_id = test_thread_id(1);
        let block_id = test_block_id(1);

        let mut refs = HashMap::new();
        refs.insert(thread_id, (thread_id, block_id, 10u32).into());

        let state = ThreadReferencesState::builder().all_thread_refs(refs).build();

        let serialized = bincode::serialize(&state).unwrap();
        let deserialized: ThreadReferencesState = bincode::deserialize(&serialized).unwrap();

        assert_eq!(state.all_thread_refs().len(), deserialized.all_thread_refs().len());
        let original_ref = state.all_thread_refs().get(&thread_id).unwrap();
        let deserialized_ref = deserialized.all_thread_refs().get(&thread_id).unwrap();
        assert_eq!(original_ref, deserialized_ref);
    }

    #[test]
    fn test_block_seq_no_ordering() {
        let seq1 = BlockSeqNo::new(10);
        let seq2 = BlockSeqNo::new(20);
        let seq3 = BlockSeqNo::new(15);

        let mut vec = vec![seq2, seq1, seq3];
        vec.sort();

        assert_eq!(vec, vec![seq1, seq3, seq2]);
    }

    #[test]
    fn test_block_seq_no_hashing() {
        use std::collections::HashSet;

        let seq1 = BlockSeqNo::new(10);
        let seq2 = BlockSeqNo::new(10);
        let seq3 = BlockSeqNo::new(20);

        let mut set = HashSet::new();
        set.insert(seq1);
        set.insert(seq2);
        set.insert(seq3);

        assert_eq!(set.len(), 2); // seq1 and seq2 are the same
    }

    #[test]
    fn test_can_reference_multiple_explicit_refs() {
        let thread_id = test_thread_id(1);
        let genesis_id = test_block_id(0);
        let block_id_1 = test_block_id(1);
        let block_id_2 = test_block_id(2);
        let block_id_3 = test_block_id(3);

        let blocks = HashMap::from([
            (
                genesis_id,
                MockRefData {
                    block_identifier: genesis_id,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: genesis_id,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id_1,
                MockRefData {
                    block_identifier: block_id_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: genesis_id,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id_2,
                MockRefData {
                    block_identifier: block_id_2,
                    block_seq_no: BlockSeqNo::new(2),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: genesis_id,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_id_3,
                MockRefData {
                    block_identifier: block_id_3,
                    block_seq_no: BlockSeqNo::new(3),
                    block_thread_identifier: thread_id,
                    parent_block_identifier: genesis_id,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
        ]);

        // Set genesis as cutoff
        let mut initial_refs = HashMap::new();
        initial_refs.insert(thread_id, (thread_id, genesis_id, BlockSeqNo::new(0)).into());

        let state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        let result = state
            .can_reference::<MockRefData, _>(vec![block_id_1, block_id_2, block_id_3], |id| {
                blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
            });

        assert!(result.is_ok());
        match result.unwrap() {
            CanRefQueryResult::Yes(resulting_state) => {
                // All 3 queried blocks should be explicitly referenced
                assert_eq!(
                    resulting_state.explicitly_referenced_blocks.len(),
                    3,
                    "Expected 3 explicit blocks"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_id_1),
                    "Block 1 should be explicitly referenced"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_id_2),
                    "Block 2 should be explicitly referenced"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_id_3),
                    "Block 3 should be explicitly referenced"
                );
                assert!(
                    !resulting_state.explicitly_referenced_blocks.contains(&genesis_id),
                    "Genesis block should NOT be explicitly referenced"
                );

                // No implicit references (all blocks directly queried, no cross-thread refs)
                assert!(
                    resulting_state.implicitly_referenced_blocks.is_empty(),
                    "Expected no implicit blocks"
                );
                assert!(
                    !resulting_state.implicitly_referenced_blocks.contains(&genesis_id),
                    "Genesis block should NOT be implicitly referenced"
                );
            }
            CanRefQueryResult::No => panic!("Expected Yes result"),
        }
    }

    // ========================================
    // Complex Runtime Scenario Tests
    // ========================================

    #[test]
    fn test_scenario_single_block_spawns_multiple_threads() {
        // ASCII Diagram:
        // =============
        //
        //      Genesis_1 (Thread 1)
        //            |
        //       Block_1.1 (spawns T2, T3, T4)
        //         / | \
        //        /  |  \
        //       T2  T3  T4
        //     (phantom references to Block_1.1)
        //
        // Test: Single block creates 3 simultaneous thread spawns
        // Validates: Multiple phantom references from one spawn operation
        let thread_1 = test_thread_id(1);
        let thread_2 = test_thread_id(2);
        let thread_3 = test_thread_id(3);
        let thread_4 = test_thread_id(4);

        let genesis_1 = test_block_id(10);
        let block_1_1 = test_block_id(11);

        let blocks = HashMap::from([
            (
                genesis_1,
                MockRefData {
                    block_identifier: genesis_1,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: genesis_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_1_1,
                MockRefData {
                    block_identifier: block_1_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: genesis_1,
                    refs: vec![],
                    spawned_threads: vec![thread_2, thread_3, thread_4], // Spawns 3 threads
                },
            ),
        ]);

        let mut initial_refs = HashMap::new();
        initial_refs.insert(thread_1, (thread_1, genesis_1, BlockSeqNo::new(0)).into());

        let mut state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        let result = state.move_refs::<MockRefData, _>(vec![block_1_1], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
        });

        assert!(result.is_ok());
        let returned_blocks = result.unwrap();

        // Should return 1 block (block_1_1)
        assert_eq!(returned_blocks.len(), 1, "Expected 1 returned block");
        assert_eq!(
            returned_blocks[0].block_identifier, block_1_1,
            "Returned block should be block_1_1"
        );

        // Genesis should NOT be in returned blocks
        assert!(
            !returned_blocks.iter().any(|b| b.block_identifier == genesis_1),
            "Genesis block should NOT be in returned blocks"
        );

        // Thread 1 and all 3 spawned threads should have references
        assert_eq!(state.all_thread_refs().len(), 4);
        assert!(state.all_thread_refs().contains_key(&thread_1));
        assert!(state.all_thread_refs().contains_key(&thread_2));
        assert!(state.all_thread_refs().contains_key(&thread_3));
        assert!(state.all_thread_refs().contains_key(&thread_4));

        // All spawned threads should reference the spawning block
        for spawned_thread in [thread_2, thread_3, thread_4] {
            let ref_block = state.all_thread_refs().get(&spawned_thread).unwrap();
            assert_eq!(ref_block.block_identifier, block_1_1);
            assert_eq!(ref_block.block_seq_no, BlockSeqNo::new(1));
        }
    }

    #[test]
    fn test_scenario_cascade_thread_splits() {
        // ASCII Diagram:
        // =============
        //
        //      Genesis_1 (Thread 1)
        //            |
        //       Block_1.1 (spawns T2)
        //            |
        //            └─> Genesis_2 (Thread 2, phantom ref)
        //                     |
        //                 Block_2.1
        //                     |
        //                 Block_2.2 (spawns T3)
        //                     |
        //                     └─> Thread 3 (phantom ref)
        //
        // Test: Cascading thread spawns (T1 → T2 → T3)
        // Validates: Multi-level thread spawning and phantom propagation
        let thread_1 = test_thread_id(1);
        let thread_2 = test_thread_id(2);
        let thread_3 = test_thread_id(3);

        let genesis_1 = test_block_id(10);
        let genesis_2 = test_block_id(20);
        let block_1_1 = test_block_id(11); // Thread 1, Block 1
        let block_2_1 = test_block_id(21); // Thread 2, Block 1 (bridging block)
        let block_2_2 = test_block_id(22); // Thread 2, Block 2

        let blocks = HashMap::from([
            (
                genesis_1,
                MockRefData {
                    block_identifier: genesis_1,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: genesis_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_1_1,
                MockRefData {
                    block_identifier: block_1_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: genesis_1,
                    refs: vec![],
                    spawned_threads: vec![thread_2], // Thread 1 spawns Thread 2
                },
            ),
            (
                genesis_2,
                MockRefData {
                    block_identifier: genesis_2,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_2,
                    parent_block_identifier: genesis_2,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_2_1,
                MockRefData {
                    block_identifier: block_2_1,
                    block_seq_no: BlockSeqNo::new(2),
                    block_thread_identifier: thread_2,
                    parent_block_identifier: genesis_2,
                    refs: vec![block_1_1], // References Thread 1
                    spawned_threads: vec![],
                },
            ),
            (
                block_2_2,
                MockRefData {
                    block_identifier: block_2_2,
                    block_seq_no: BlockSeqNo::new(3),
                    block_thread_identifier: thread_2,
                    parent_block_identifier: block_2_1,
                    refs: vec![],
                    spawned_threads: vec![thread_3], // Thread 2 spawns Thread 3
                },
            ),
        ]);

        let mut initial_refs = HashMap::new();
        initial_refs.insert(thread_1, (thread_1, genesis_1, BlockSeqNo::new(0)).into());
        initial_refs.insert(thread_2, (thread_2, genesis_2, BlockSeqNo::new(0)).into());

        let mut state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        // First, reference block_1_1 (which spawns thread_2)
        let result = state.move_refs::<MockRefData, _>(vec![block_1_1], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
        });
        assert!(result.is_ok());
        let returned_blocks_1 = result.unwrap();

        // Should return 1 block (block_1_1)
        assert_eq!(returned_blocks_1.len(), 1, "Expected 1 block in first move_refs");
        assert_eq!(
            returned_blocks_1[0].block_identifier, block_1_1,
            "First move_refs should return block_1_1"
        );

        // Now 2 threads: thread_1 (updated), thread_2 (spawned with phantom)
        assert_eq!(state.all_thread_refs().len(), 2);

        // Now reference block_2_2 (which pulls in block_2_1 and block_1_1, and spawns thread_3)
        let result = state.move_refs::<MockRefData, _>(vec![block_2_2], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found: {:?}", id))
        });
        if let Err(ref e) = result {
            eprintln!("Error in second move_refs: {}", e);
            eprintln!("State before: {:?}", state.all_thread_refs());
        }
        assert!(result.is_ok());
        let returned_blocks_2 = result.unwrap();

        // Should return 3 blocks: block_2_2, block_2_1 (parent), block_1_1 (cross-thread ref)
        assert_eq!(returned_blocks_2.len(), 2, "Expected 2 blocks in second move_refs");
        let returned_ids: Vec<_> = returned_blocks_2.iter().map(|b| b.block_identifier).collect();
        assert!(returned_ids.contains(&block_2_2), "Should return block_2_2");
        assert!(returned_ids.contains(&block_2_1), "Should return block_2_1");

        // Genesis blocks should NOT be in returned blocks
        assert!(!returned_ids.contains(&genesis_1), "Genesis 1 should NOT be in returned blocks");
        assert!(!returned_ids.contains(&genesis_2), "Genesis 2 should NOT be in returned blocks");

        // Now 3 threads: thread_1, thread_2 (updated to block_2_2), thread_3 (spawned)
        assert_eq!(state.all_thread_refs().len(), 3);
        assert!(state.all_thread_refs().contains_key(&thread_1));
        assert!(state.all_thread_refs().contains_key(&thread_2));
        assert!(state.all_thread_refs().contains_key(&thread_3));

        // Thread 1 should still reference block_1_1
        let ref_block_1 = state.all_thread_refs().get(&thread_1).unwrap();
        assert_eq!(ref_block_1.block_identifier, block_1_1);

        // Thread 2 should reference block_2_2 (latest)
        let ref_block_2 = state.all_thread_refs().get(&thread_2).unwrap();
        assert_eq!(ref_block_2.block_identifier, block_2_2);

        // Thread 3 should reference block_2_2 (phantom from spawn)
        let ref_block_3 = state.all_thread_refs().get(&thread_3).unwrap();
        assert_eq!(ref_block_3.block_identifier, block_2_2);
    }

    #[test]
    fn test_scenario_multiple_threads_converge() {
        // ASCII Diagram:
        // =============
        //
        //       Genesis_1 (Thread 1)
        //            |
        //       Block_1.1
        //            |
        //       Block_1.2
        //         /     \
        //        /       \
        //   Block_2.1   Block_3.1 (both reference T1)
        //   (Thread 2)  (Thread 3)
        //        \       /
        //         \     /
        //          \   /
        //       Block_4.1 (Thread 4)
        //       (refs T2 & T3)
        //
        // Test: Multiple threads reference common ancestor, then converge
        // Validates: Diamond-like pattern, transitive references
        let thread_1 = test_thread_id(1);
        let thread_2 = test_thread_id(2);
        let thread_3 = test_thread_id(3);
        let thread_4 = test_thread_id(4);

        let genesis_1 = test_block_id(10);
        let genesis_2 = test_block_id(20);
        let genesis_3 = test_block_id(30);
        let genesis_4 = test_block_id(40);

        let block_1_1 = test_block_id(11);
        let block_1_2 = test_block_id(12);
        let block_2_1 = test_block_id(21);
        let block_3_1 = test_block_id(31);
        let block_4_1 = test_block_id(41);

        let blocks = HashMap::from([
            // Thread 1
            (
                genesis_1,
                MockRefData {
                    block_identifier: genesis_1,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: genesis_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_1_1,
                MockRefData {
                    block_identifier: block_1_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: genesis_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_1_2,
                MockRefData {
                    block_identifier: block_1_2,
                    block_seq_no: BlockSeqNo::new(2),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: block_1_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            // Thread 2 - references Thread 1, Block 1
            (
                genesis_2,
                MockRefData {
                    block_identifier: genesis_2,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_2,
                    parent_block_identifier: genesis_2,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_2_1,
                MockRefData {
                    block_identifier: block_2_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_2,
                    parent_block_identifier: genesis_2,
                    refs: vec![block_1_1],
                    spawned_threads: vec![],
                },
            ),
            // Thread 3 - references Thread 1, Block 2
            (
                genesis_3,
                MockRefData {
                    block_identifier: genesis_3,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_3,
                    parent_block_identifier: genesis_3,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_3_1,
                MockRefData {
                    block_identifier: block_3_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_3,
                    parent_block_identifier: genesis_3,
                    refs: vec![block_1_2],
                    spawned_threads: vec![],
                },
            ),
            // Thread 4 - references both Thread 2 and Thread 3
            (
                genesis_4,
                MockRefData {
                    block_identifier: genesis_4,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_4,
                    parent_block_identifier: genesis_4,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_4_1,
                MockRefData {
                    block_identifier: block_4_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_4,
                    parent_block_identifier: genesis_4,
                    refs: vec![block_2_1, block_3_1], // References both Thread 2 and 3
                    spawned_threads: vec![],
                },
            ),
        ]);

        let mut initial_refs = HashMap::new();
        initial_refs.insert(thread_1, (thread_1, genesis_1, BlockSeqNo::new(0)).into());
        initial_refs.insert(thread_2, (thread_2, genesis_2, BlockSeqNo::new(0)).into());
        initial_refs.insert(thread_3, (thread_3, genesis_3, BlockSeqNo::new(0)).into());
        initial_refs.insert(thread_4, (thread_4, genesis_4, BlockSeqNo::new(0)).into());

        let state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        // Reference block_4_1, which should pull in blocks from multiple threads
        let result = state.can_reference::<MockRefData, _>(vec![block_4_1], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
        });

        assert!(result.is_ok());
        match result.unwrap() {
            CanRefQueryResult::Yes(resulting_state) => {
                // Should reference: block_4_1, block_2_1, block_3_1, block_1_1, block_1_2
                assert_eq!(resulting_state.explicitly_referenced_blocks.len(), 5);
                assert!(resulting_state.explicitly_referenced_blocks.contains(&block_4_1));
                assert!(resulting_state.explicitly_referenced_blocks.contains(&block_2_1));
                assert!(resulting_state.explicitly_referenced_blocks.contains(&block_3_1));
                assert!(resulting_state.explicitly_referenced_blocks.contains(&block_1_1));
                assert!(resulting_state.explicitly_referenced_blocks.contains(&block_1_2));

                // Genesis blocks should NOT be referenced
                for genesis in [genesis_1, genesis_2, genesis_3, genesis_4] {
                    assert!(
                        !resulting_state.explicitly_referenced_blocks.contains(&genesis),
                        "Genesis block {:?} should NOT be explicitly referenced",
                        genesis
                    );
                    assert!(
                        !resulting_state.implicitly_referenced_blocks.contains(&genesis),
                        "Genesis block {:?} should NOT be implicitly referenced",
                        genesis
                    );
                }
            }
            CanRefQueryResult::No => panic!("Expected Yes result"),
        }
    }

    #[test]
    fn test_scenario_parallel_thread_development() {
        // ASCII Diagram:
        // =============
        //
        //   Thread 1    Thread 2    Thread 3    Thread 4
        //   Genesis_1   Genesis_2   Genesis_3   Genesis_4
        //      |           |           |           |
        //   Block_1.1   Block_2.1   Block_3.1   Block_4.1
        //      |           |           |           |
        //   Block_1.2   Block_2.2   Block_3.2   Block_4.2
        //      |           |           |           |
        //   Block_1.3   Block_2.3   Block_3.3   Block_4.3
        //       \          |           |          /
        //        \         |           |         /
        //         \        |           |        /
        //          \       |           |       /
        //           \______\_________/_______/
        //                      |
        //              Convergence_Block (Thread 5)
        //              (refs all 4 threads)
        //
        // Test: Large-scale parallel development with final convergence
        // Validates: 4 threads × 3 blocks each = 12 blocks, then 1 convergence
        let threads: Vec<ThreadIdentifier> = (1..=4).map(test_thread_id).collect();
        let mut blocks = HashMap::new();

        // Create genesis blocks for all threads
        for (idx, thread) in threads.iter().enumerate() {
            let genesis_id = test_block_id((idx * 10) as u8);
            blocks.insert(
                genesis_id,
                MockRefData {
                    block_identifier: genesis_id,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: *thread,
                    parent_block_identifier: genesis_id,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            );
        }

        // Create 3 blocks for each thread
        let mut latest_blocks = Vec::new();
        for (thread_idx, thread) in threads.iter().enumerate() {
            let genesis_id = test_block_id((thread_idx * 10) as u8);
            let mut parent = genesis_id;

            for block_num in 1..=3 {
                let block_id = test_block_id((thread_idx * 10) as u8 + block_num);
                blocks.insert(
                    block_id,
                    MockRefData {
                        block_identifier: block_id,
                        block_seq_no: BlockSeqNo::new(block_num as u32),
                        block_thread_identifier: *thread,
                        parent_block_identifier: parent,
                        refs: vec![],
                        spawned_threads: vec![],
                    },
                );
                parent = block_id;

                if block_num == 3 {
                    latest_blocks.push(block_id);
                }
            }
        }

        // Create a convergence block in thread 5 that references all latest blocks
        let thread_5 = test_thread_id(5);
        let genesis_5 = test_block_id(50);
        let convergence_block = test_block_id(51);

        blocks.insert(
            genesis_5,
            MockRefData {
                block_identifier: genesis_5,
                block_seq_no: BlockSeqNo::new(0),
                block_thread_identifier: thread_5,
                parent_block_identifier: genesis_5,
                refs: vec![],
                spawned_threads: vec![],
            },
        );

        blocks.insert(
            convergence_block,
            MockRefData {
                block_identifier: convergence_block,
                block_seq_no: BlockSeqNo::new(1),
                block_thread_identifier: thread_5,
                parent_block_identifier: genesis_5,
                refs: latest_blocks.clone(), // References all 4 threads
                spawned_threads: vec![],
            },
        );

        // Setup initial state with all genesis blocks
        let mut initial_refs = HashMap::new();
        for (thread_idx, thread) in threads.iter().enumerate() {
            let genesis_id = test_block_id((thread_idx * 10) as u8);
            initial_refs.insert(*thread, (*thread, genesis_id, BlockSeqNo::new(0)).into());
        }
        initial_refs.insert(thread_5, (thread_5, genesis_5, BlockSeqNo::new(0)).into());

        let state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        let result = state.can_reference::<MockRefData, _>(vec![convergence_block], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
        });

        assert!(result.is_ok());
        match result.unwrap() {
            CanRefQueryResult::Yes(resulting_state) => {
                // Should reference: convergence block + 3 blocks per thread * 4 threads = 13 blocks
                assert_eq!(resulting_state.explicitly_referenced_blocks.len(), 13);

                // Total: convergence block + 3 blocks per thread * 4 threads = 13 blocks
                let total_refs = resulting_state.explicitly_referenced_blocks.len()
                    + resulting_state.implicitly_referenced_blocks.len();
                assert_eq!(total_refs, 13, "Expected 13 total referenced blocks");

                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&convergence_block),
                    "Convergence block should be explicitly referenced (query target)"
                );

                // Verify all latest blocks from each thread are included (implicitly)
                for latest in &latest_blocks {
                    assert!(
                        resulting_state.explicitly_referenced_blocks.contains(latest),
                        "Latest block {:?} should be explicitly referenced (cross-thread ref)",
                        latest
                    );
                }

                // Genesis blocks should NOT be referenced (5 threads, 5 genesis blocks)
                for thread_idx in 0..=4 {
                    let genesis_id = test_block_id((thread_idx * 10) as u8);
                    assert!(
                        !resulting_state.explicitly_referenced_blocks.contains(&genesis_id),
                        "Genesis {:?} should NOT be explicitly referenced",
                        genesis_id
                    );
                    assert!(
                        !resulting_state.implicitly_referenced_blocks.contains(&genesis_id),
                        "Genesis {:?} should NOT be implicitly referenced",
                        genesis_id
                    );
                }
            }
            CanRefQueryResult::No => panic!("Expected Yes result"),
        }
    }

    #[test]
    fn test_scenario_thread_split_chain() {
        // ASCII Diagram:
        // =============
        //
        //      Genesis_1 (Thread 1)
        //            |
        //       Block_1.1 (spawns T2) ────┐
        //            |                     │
        //            |                     ▼
        //       Block_1.2 (spawns T3) ──┐ Thread 2
        //            |                   │ (phantom ref to Block_1.1)
        //            |                   │
        //       Block_1.3 (spawns T4) ┐ ▼
        //                              │ Thread 3
        //                              │ (phantom ref to Block_1.2)
        //                              │
        //                              ▼
        //                            Thread 4
        //                            (phantom ref to Block_1.3)
        //
        // Test: Sequential blocks each spawn a new thread
        // Validates: Multiple spawns in a single thread chain
        //           Phantom references at different sequence numbers
        let thread_1 = test_thread_id(1);
        let thread_2 = test_thread_id(2);
        let thread_3 = test_thread_id(3);
        let thread_4 = test_thread_id(4);

        let genesis_1 = test_block_id(10);
        let block_1_1 = test_block_id(11);
        let block_1_2 = test_block_id(12);
        let block_1_3 = test_block_id(13);

        let blocks = HashMap::from([
            (
                genesis_1,
                MockRefData {
                    block_identifier: genesis_1,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: genesis_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_1_1,
                MockRefData {
                    block_identifier: block_1_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: genesis_1,
                    refs: vec![],
                    spawned_threads: vec![thread_2], // Block 1 spawns Thread 2
                },
            ),
            (
                block_1_2,
                MockRefData {
                    block_identifier: block_1_2,
                    block_seq_no: BlockSeqNo::new(2),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: block_1_1,
                    refs: vec![],
                    spawned_threads: vec![thread_3], // Block 2 spawns Thread 3
                },
            ),
            (
                block_1_3,
                MockRefData {
                    block_identifier: block_1_3,
                    block_seq_no: BlockSeqNo::new(3),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: block_1_2,
                    refs: vec![],
                    spawned_threads: vec![thread_4], // Block 3 spawns Thread 4
                },
            ),
        ]);

        let mut initial_refs = HashMap::new();
        initial_refs.insert(thread_1, (thread_1, genesis_1, BlockSeqNo::new(0)).into());

        let mut state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        // Move refs for all three blocks
        let result = state
            .move_refs::<MockRefData, _>(vec![block_1_1, block_1_2, block_1_3], |id| {
                blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
            });

        assert!(result.is_ok());
        let returned_blocks = result.unwrap();

        // Should return all 3 queried blocks (all in same thread chain)
        assert_eq!(returned_blocks.len(), 3, "Expected 3 returned blocks");
        let returned_ids: Vec<_> = returned_blocks.iter().map(|b| b.block_identifier).collect();
        assert!(returned_ids.contains(&block_1_1), "Should return block_1_1");
        assert!(returned_ids.contains(&block_1_2), "Should return block_1_2");
        assert!(returned_ids.contains(&block_1_3), "Should return block_1_3");

        // Genesis should NOT be in returned blocks
        assert!(
            !returned_ids.contains(&genesis_1),
            "Genesis block should NOT be in returned blocks"
        );

        // Should have 4 threads: original + 3 spawned
        assert_eq!(state.all_thread_refs().len(), 4);
        assert!(state.all_thread_refs().contains_key(&thread_1));
        assert!(state.all_thread_refs().contains_key(&thread_2));
        assert!(state.all_thread_refs().contains_key(&thread_3));
        assert!(state.all_thread_refs().contains_key(&thread_4));

        // Thread 1 should reference block_1_3 (latest)
        let ref_1 = state.all_thread_refs().get(&thread_1).unwrap();
        assert_eq!(ref_1.block_identifier, block_1_3);

        // Thread 2 should reference block_1_1 (phantom from spawn)
        let ref_2 = state.all_thread_refs().get(&thread_2).unwrap();
        assert_eq!(ref_2.block_identifier, block_1_1);

        // Thread 3 should reference block_1_2 (phantom from spawn)
        let ref_3 = state.all_thread_refs().get(&thread_3).unwrap();
        assert_eq!(ref_3.block_identifier, block_1_2);

        // Thread 4 should reference block_1_3 (phantom from spawn)
        let ref_4 = state.all_thread_refs().get(&thread_4).unwrap();
        assert_eq!(ref_4.block_identifier, block_1_3);
    }

    #[test]
    fn test_scenario_complex_dag_with_multiple_splits() {
        // ASCII Diagram:
        // =============
        //
        //          Genesis_1 (Thread 1)
        //                |
        //           Block_1.1 (spawns T2, T3)
        //             /    \
        //            /      \
        //      Thread 2    Thread 3
        //          |            |
        //      Block_2.1    Block_3.1
        //     (refs T1)    (refs T1, T2)
        //          |            |
        //      Block_2.2        |
        //     (refs T1,         |
        //      spawns T4)       |
        //          |            |
        //      Thread 4         |
        //          |            |
        //      Block_4.1        |
        //     (refs T2, T3) <---+
        //
        // Test: Complex DAG with nested spawns and cross-references
        // Validates: 4 threads, multiple splits, extensive cross-referencing
        //           6 blocks with complex dependencies

        let thread_1 = test_thread_id(1);
        let thread_2 = test_thread_id(2);
        let thread_3 = test_thread_id(3);
        let thread_4 = test_thread_id(4);

        let genesis_1 = test_block_id(10);
        let genesis_2 = test_block_id(20);
        let genesis_3 = test_block_id(30);
        let genesis_4 = test_block_id(40);

        let block_1_1 = test_block_id(11);
        let block_1_2 = test_block_id(12);
        let block_2_1 = test_block_id(21);
        let block_2_2 = test_block_id(22);
        let block_3_1 = test_block_id(31);
        let block_4_1 = test_block_id(41);

        let blocks = HashMap::from([
            // Thread 1
            (
                genesis_1,
                MockRefData {
                    block_identifier: genesis_1,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: genesis_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_1_1,
                MockRefData {
                    block_identifier: block_1_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: genesis_1,
                    refs: vec![],
                    spawned_threads: vec![thread_2, thread_3], // Spawns 2 threads
                },
            ),
            (
                block_1_2,
                MockRefData {
                    block_identifier: block_1_2,
                    block_seq_no: BlockSeqNo::new(2),
                    block_thread_identifier: thread_1,
                    parent_block_identifier: block_1_1,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            // Thread 2
            (
                genesis_2,
                MockRefData {
                    block_identifier: genesis_2,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_2,
                    parent_block_identifier: genesis_2,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_2_1,
                MockRefData {
                    block_identifier: block_2_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_2,
                    parent_block_identifier: genesis_2,
                    refs: vec![block_1_1], // References Thread 1
                    spawned_threads: vec![],
                },
            ),
            (
                block_2_2,
                MockRefData {
                    block_identifier: block_2_2,
                    block_seq_no: BlockSeqNo::new(2),
                    block_thread_identifier: thread_2,
                    parent_block_identifier: block_2_1,
                    refs: vec![block_1_2], // References Thread 1 again
                    spawned_threads: vec![thread_4], // Spawns Thread 4
                },
            ),
            // Thread 3
            (
                genesis_3,
                MockRefData {
                    block_identifier: genesis_3,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_3,
                    parent_block_identifier: genesis_3,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_3_1,
                MockRefData {
                    block_identifier: block_3_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_3,
                    parent_block_identifier: genesis_3,
                    refs: vec![block_1_1, block_2_1], // References both Thread 1 and 2
                    spawned_threads: vec![],
                },
            ),
            // Thread 4
            (
                genesis_4,
                MockRefData {
                    block_identifier: genesis_4,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: thread_4,
                    parent_block_identifier: genesis_4,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            ),
            (
                block_4_1,
                MockRefData {
                    block_identifier: block_4_1,
                    block_seq_no: BlockSeqNo::new(1),
                    block_thread_identifier: thread_4,
                    parent_block_identifier: genesis_4,
                    refs: vec![block_2_2, block_3_1], // References Thread 2 and 3
                    spawned_threads: vec![],
                },
            ),
        ]);

        let mut initial_refs = HashMap::new();
        initial_refs.insert(thread_1, (thread_1, genesis_1, BlockSeqNo::new(0)).into());
        initial_refs.insert(thread_2, (thread_2, genesis_2, BlockSeqNo::new(0)).into());
        initial_refs.insert(thread_3, (thread_3, genesis_3, BlockSeqNo::new(0)).into());
        initial_refs.insert(thread_4, (thread_4, genesis_4, BlockSeqNo::new(0)).into());

        let state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        // Query if we can reference block_4_1 (which pulls in everything)
        let result = state.can_reference::<MockRefData, _>(vec![block_4_1], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
        });

        assert!(result.is_ok());
        match result.unwrap() {
            CanRefQueryResult::Yes(resulting_state) => {
                // Total: all blocks except genesis blocks = 6 blocks
                // block_4_1, block_3_1, block_2_2, block_2_1, block_1_2, block_1_1
                let total_refs = resulting_state.explicitly_referenced_blocks.len()
                    + resulting_state.implicitly_referenced_blocks.len();
                assert_eq!(total_refs, 6, "Expected 6 total referenced blocks");

                // Explicitly: only block_4_1 (directly queried)
                assert_eq!(
                    resulting_state.explicitly_referenced_blocks.len(),
                    6,
                    "Expected 6 explicit blocks"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_4_1),
                    "Block 4.1 should be explicitly referenced (query target)"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_3_1),
                    "Block 3.1 should be explicitly referenced (cross-thread ref from 4.1)"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_2_2),
                    "Block 2.2 should be explicitly referenced (cross-thread ref from 4.1)"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_2_1),
                    "Block 2.1 should be explicitly referenced (parent of 2.2)"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_1_2),
                    "Block 1.2 should be explicitly referenced (cross-thread ref from 2.2)"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&block_1_1),
                    "Block 1.1 should be explicitly referenced (cross-thread ref from 2.1 or 3.1)"
                );

                // Implicitly: block_3_1, block_2_2 (cross-refs from 4.1),
                //             block_2_1, block_1_1 (from cross-refs and history),
                //             block_1_2 (cross-ref from 2.2)
                assert_eq!(
                    resulting_state.implicitly_referenced_blocks.len(),
                    0,
                    "Implicit blocks should be empty"
                );

                // Genesis blocks should NOT be referenced
                for genesis in [genesis_1, genesis_2, genesis_3, genesis_4] {
                    assert!(
                        !resulting_state.explicitly_referenced_blocks.contains(&genesis),
                        "Genesis {:?} should NOT be explicitly referenced",
                        genesis
                    );
                    assert!(
                        !resulting_state.implicitly_referenced_blocks.contains(&genesis),
                        "Genesis {:?} should NOT be implicitly referenced",
                        genesis
                    );
                }
            }
            CanRefQueryResult::No => panic!("Expected Yes result"),
        }
    }

    #[test]
    fn test_scenario_diamond_dag_with_parallel_splits_and_joins() {
        // Complex DAG Structure - "Diamond with Parallel Lanes"
        //
        // This test simulates a realistic blockchain scenario with:
        // - 1 genesis block (Thread 1)
        // - Multiple parallel thread splits (spawning Threads 2-6)
        // - Parallel development in each thread
        // - Multiple convergence points
        // - Final convergence back to Thread 1
        //
        // ASCII Diagram:
        // ===============
        //
        //                      Genesis_1 (Thread 1, seq 0)
        //                                 |
        //                            Block_1.1 (seq 1)
        //                           [spawns T2, T3]
        //                         /               \
        //                        /                 \
        //                       ▼                   ▼
        //            ┌────── Thread 2           Thread 3 ──────┐
        //            │       (phantom)          (phantom)      │
        //            │          |                   |          │
        //            │      Block_2.1            Block_3.1     │
        //            │   (seq 1, spawns T4)  (seq 1, spawns T5)│
        //            │          |                   |          │
        //            │      Block_2.2            Block_3.2     │
        //            │       (seq 2)              (seq 2)      │
        //            │      /                          \       │
        //            │     /                            \      │
        //            │    ▼                              ▼     │
        //            │ Thread 4                      Thread 5  │
        //            │ (phantom)                     (phantom) │
        //            │    |                              |     │
        //            │ Block_4.1                     Block_5.1 │
        //            │ (seq 1)                       (seq 1)   │
        //            │    |                              |     │
        //            │ Block_4.2                     Block_5.2 │
        //            │ (seq 2)                       (seq 2)   │
        //            │    |                              |     │
        //            └────┼                              ┼─────┘
        //                 │                              │
        //                 └──────────┬───────────────────┘
        //                            ▼
        //                       Thread 6
        //                    (convergence)
        //                         |
        //                    Block_6.1
        //              (seq 1, refs b2.2, b3.2,
        //                     b4.2, b5.2)
        //                         |
        //                    Block_6.2
        //                      (seq 2)
        //                         |
        //                         ▼
        //                    Block_1.2
        //                (Thread 1, seq 2)
        //                 [Final Block]
        //                  (refs b6.2)
        //
        // Flow Summary:
        // 1. Thread 1 starts, spawns T2 and T3
        // 2. Thread 2 develops (2 blocks), spawns T4
        // 3. Thread 3 develops (2 blocks), spawns T5
        // 4. Thread 4 develops independently (2 blocks)
        // 5. Thread 5 develops independently (2 blocks)
        // 6. All 4 threads (T2, T3, T4, T5) converge in Thread 6
        // 7. Thread 6 develops (2 blocks)
        // 8. Thread 1 creates final convergence block referencing all threads
        //
        // Total: 6 threads, 14 blocks (excluding genesis blocks)

        // Setup thread identifiers
        let t1 = test_thread_id(1);
        let t2 = test_thread_id(2);
        let t3 = test_thread_id(3);
        let t4 = test_thread_id(4);
        let t5 = test_thread_id(5);
        let t6 = test_thread_id(6);

        // Setup block identifiers
        let gen_1 = test_block_id(10);
        let gen_2 = test_block_id(20);
        let gen_3 = test_block_id(30);
        let gen_4 = test_block_id(40);
        let gen_5 = test_block_id(50);
        let gen_6 = test_block_id(60);

        let b1_1 = test_block_id(11); // Thread 1, Block 1
        let b1_2 = test_block_id(12); // Thread 1, Final block

        let b2_1 = test_block_id(21); // Thread 2, Block 1
        let b2_2 = test_block_id(22); // Thread 2, Block 2

        let b3_1 = test_block_id(31); // Thread 3, Block 1
        let b3_2 = test_block_id(32); // Thread 3, Block 2

        let b4_1 = test_block_id(41); // Thread 4, Block 1
        let b4_2 = test_block_id(42); // Thread 4, Block 2

        let b5_1 = test_block_id(51); // Thread 5, Block 1
        let b5_2 = test_block_id(52); // Thread 5, Block 2

        let b6_1 = test_block_id(61); // Thread 6, Block 1
        let b6_2 = test_block_id(62); // Thread 6, Block 2

        let mut blocks = HashMap::new();

        // Genesis blocks for all threads
        for (idx, thread) in [t1, t2, t3, t4, t5, t6].iter().enumerate() {
            let gen_id = [gen_1, gen_2, gen_3, gen_4, gen_5, gen_6][idx];
            blocks.insert(
                gen_id,
                MockRefData {
                    block_identifier: gen_id,
                    block_seq_no: BlockSeqNo::new(0),
                    block_thread_identifier: *thread,
                    parent_block_identifier: gen_id,
                    refs: vec![],
                    spawned_threads: vec![],
                },
            );
        }

        // Thread 1: Initial split
        blocks.insert(
            b1_1,
            MockRefData {
                block_identifier: b1_1,
                block_seq_no: BlockSeqNo::new(1),
                block_thread_identifier: t1,
                parent_block_identifier: gen_1,
                refs: vec![],
                spawned_threads: vec![t2, t3], // First split: spawns T2 and T3
            },
        );

        // Thread 2: Develops and spawns T4
        blocks.insert(
            b2_1,
            MockRefData {
                block_identifier: b2_1,
                block_seq_no: BlockSeqNo::new(1),
                block_thread_identifier: t2,
                parent_block_identifier: gen_2,
                refs: vec![b1_1],          // References parent thread
                spawned_threads: vec![t4], // Second split: spawns T4
            },
        );
        blocks.insert(
            b2_2,
            MockRefData {
                block_identifier: b2_2,
                block_seq_no: BlockSeqNo::new(2),
                block_thread_identifier: t2,
                parent_block_identifier: b2_1,
                refs: vec![],
                spawned_threads: vec![],
            },
        );

        // Thread 3: Develops and spawns T5
        blocks.insert(
            b3_1,
            MockRefData {
                block_identifier: b3_1,
                block_seq_no: BlockSeqNo::new(1),
                block_thread_identifier: t3,
                parent_block_identifier: gen_3,
                refs: vec![b1_1],          // References parent thread
                spawned_threads: vec![t5], // Third split: spawns T5
            },
        );
        blocks.insert(
            b3_2,
            MockRefData {
                block_identifier: b3_2,
                block_seq_no: BlockSeqNo::new(2),
                block_thread_identifier: t3,
                parent_block_identifier: b3_1,
                refs: vec![],
                spawned_threads: vec![],
            },
        );

        // Thread 4: Independent development
        blocks.insert(
            b4_1,
            MockRefData {
                block_identifier: b4_1,
                block_seq_no: BlockSeqNo::new(1),
                block_thread_identifier: t4,
                parent_block_identifier: gen_4,
                refs: vec![b2_1], // References parent thread
                spawned_threads: vec![],
            },
        );
        blocks.insert(
            b4_2,
            MockRefData {
                block_identifier: b4_2,
                block_seq_no: BlockSeqNo::new(2),
                block_thread_identifier: t4,
                parent_block_identifier: b4_1,
                refs: vec![],
                spawned_threads: vec![],
            },
        );

        // Thread 5: Independent development
        blocks.insert(
            b5_1,
            MockRefData {
                block_identifier: b5_1,
                block_seq_no: BlockSeqNo::new(1),
                block_thread_identifier: t5,
                parent_block_identifier: gen_5,
                refs: vec![b3_1], // References parent thread
                spawned_threads: vec![],
            },
        );
        blocks.insert(
            b5_2,
            MockRefData {
                block_identifier: b5_2,
                block_seq_no: BlockSeqNo::new(2),
                block_thread_identifier: t5,
                parent_block_identifier: b5_1,
                refs: vec![],
                spawned_threads: vec![],
            },
        );

        // Thread 6: First convergence - joins T2, T3, T4, T5
        blocks.insert(
            b6_1,
            MockRefData {
                block_identifier: b6_1,
                block_seq_no: BlockSeqNo::new(1),
                block_thread_identifier: t6,
                parent_block_identifier: gen_6,
                refs: vec![b2_2, b3_2, b4_2, b5_2], // Convergence: refs all 4 threads
                spawned_threads: vec![],
            },
        );
        blocks.insert(
            b6_2,
            MockRefData {
                block_identifier: b6_2,
                block_seq_no: BlockSeqNo::new(2),
                block_thread_identifier: t6,
                parent_block_identifier: b6_1,
                refs: vec![],
                spawned_threads: vec![],
            },
        );

        // Thread 1: Final convergence - references all threads including T6
        blocks.insert(
            b1_2,
            MockRefData {
                block_identifier: b1_2,
                block_seq_no: BlockSeqNo::new(2),
                block_thread_identifier: t1,
                parent_block_identifier: b1_1,
                refs: vec![b6_2], // Final convergence through T6
                spawned_threads: vec![],
            },
        );

        // Setup initial state with all genesis blocks
        let mut initial_refs = HashMap::new();
        for (idx, thread) in [t1, t2, t3, t4, t5, t6].iter().enumerate() {
            let gen_id = [gen_1, gen_2, gen_3, gen_4, gen_5, gen_6][idx];
            initial_refs.insert(*thread, (*thread, gen_id, BlockSeqNo::new(0)).into());
        }

        let state = ThreadReferencesState::builder().all_thread_refs(initial_refs).build();

        // Test the final convergence block
        let result = state.can_reference::<MockRefData, _>(vec![b1_2], |id| {
            blocks.get(id).cloned().ok_or_else(|| anyhow::anyhow!("Block not found"))
        });

        assert!(result.is_ok());
        match result.unwrap() {
            CanRefQueryResult::Yes(resulting_state) => {
                // Should reference all non-genesis blocks: 14 blocks total
                // b1_1, b1_2, b2_1, b2_2, b3_1, b3_2, b4_1, b4_2, b5_1, b5_2, b6_1, b6_2
                assert!(
                    resulting_state.explicitly_referenced_blocks.len() >= 12,
                    "Expected at least 12 blocks, got {}",
                    resulting_state.explicitly_referenced_blocks.len()
                );

                // Verify key blocks are included
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&b1_1),
                    "Missing b1_1"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&b1_2),
                    "Missing b1_2 (final block)"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&b6_2),
                    "Missing b6_2 (convergence)"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&b6_1),
                    "Missing b6_1"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&b2_2),
                    "Missing b2_2"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&b3_2),
                    "Missing b3_2"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&b4_2),
                    "Missing b4_2"
                );
                assert!(
                    resulting_state.explicitly_referenced_blocks.contains(&b5_2),
                    "Missing b5_2"
                );

                // Verify all threads are represented
                let mut threads_seen = HashSet::new();
                for block_id in &resulting_state.explicitly_referenced_blocks {
                    if let Some(block) = blocks.get(block_id) {
                        threads_seen.insert(block.block_thread_identifier);
                    }
                }
                assert!(threads_seen.contains(&t1), "Thread 1 not seen");
                assert!(threads_seen.contains(&t2), "Thread 2 not seen");
                assert!(threads_seen.contains(&t3), "Thread 3 not seen");
                assert!(threads_seen.contains(&t4), "Thread 4 not seen");
                assert!(threads_seen.contains(&t5), "Thread 5 not seen");
                assert!(threads_seen.contains(&t6), "Thread 6 not seen");

                // Genesis blocks should NOT be referenced
                for genesis in [gen_1, gen_2, gen_3, gen_4, gen_5, gen_6] {
                    assert!(
                        !resulting_state.explicitly_referenced_blocks.contains(&genesis),
                        "Genesis {:?} should NOT be explicitly referenced",
                        genesis
                    );
                    assert!(
                        !resulting_state.implicitly_referenced_blocks.contains(&genesis),
                        "Genesis {:?} should NOT be implicitly referenced",
                        genesis
                    );
                }

                println!(
                    "✓ Diamond DAG test passed: {} blocks referenced across 6 threads",
                    resulting_state.explicitly_referenced_blocks.len()
                );
            }
            CanRefQueryResult::No => panic!("Expected Yes result"),
        }
    }
}
