use std::collections::HashMap;
use std::collections::HashSet;

use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::ThreadAccountsHash;
use node_types::ThreadIdentifier;

use crate::live_metrics::LiveAccumulatedUpdateCounter;
use crate::thread_accounts::ArchiveOperation;
use crate::ThreadAccount;

/// Tracks blockchain thread state transition.
#[derive(Clone, Debug)]
pub struct ThreadTransition {
    /// Block ID the store should currently be at.
    pub expected_block_id: BlockIdentifier,
    /// Block ID after this update is applied.
    pub new_block_id: BlockIdentifier,
}

/// Complete snapshot of account data for a single blockchain thread.
/// Used by thread_init to populate an Uninitialized thread.
#[derive(Clone, Default)]
pub struct ThreadSnapshot {
    pub entries: HashMap<AccountRouting, ThreadAccount>,
}

impl ThreadSnapshot {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, routing: AccountRouting, value: ThreadAccount) {
        self.entries.insert(routing, value);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// A batch of account updates to be applied atomically via A/B commit.
#[derive(Clone, Default, Debug)]
pub struct AccumulatedUpdate {
    /// Account-level operations (shared across all threads in this update).
    pub operations: HashMap<AccountRouting, ArchiveOperation>,

    /// For each existing blockchain thread being updated: expected → new block_id.
    pub thread_transitions: HashMap<ThreadIdentifier, ThreadTransition>,

    /// Blockchain threads emerging in this update.
    pub emerging_threads: HashMap<ThreadIdentifier, BlockIdentifier>,

    /// Blockchain threads collapsing in this update.
    pub collapsing_threads: HashSet<ThreadIdentifier>,

    /// State map hashes that become archived after this update is applied.
    /// Used by optimize_parents to prune in-memory parent chains.
    pub archived_state_hashes: Vec<ThreadAccountsHash>,

    pub(crate) _live_counter: LiveAccumulatedUpdateCounter,
}

impl AccumulatedUpdate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from a single thread's diff (convenience for the commit loop).
    pub fn with_single_thread(
        thread_id: ThreadIdentifier,
        transition: ThreadTransition,
        operations: HashMap<AccountRouting, ArchiveOperation>,
    ) -> Self {
        let mut update = Self::new();
        update.thread_transitions.insert(thread_id, transition);
        update.operations = operations;
        update
    }

    pub fn is_empty(&self) -> bool {
        self.thread_transitions.is_empty()
            && self.operations.is_empty()
            && self.collapsing_threads.is_empty()
            && self.emerging_threads.is_empty()
    }

    pub fn insert(&mut self, routing: AccountRouting, account: ArchiveOperation) {
        self.operations.insert(routing, account);
    }

    pub fn transition_thread(
        &mut self,
        thread_id: ThreadIdentifier,
        expected_block_id: BlockIdentifier,
        new_block_id: BlockIdentifier,
    ) {
        self.thread_transitions
            .insert(thread_id, ThreadTransition { expected_block_id, new_block_id });
    }

    pub fn emerge_thread(
        &mut self,
        thread_id: ThreadIdentifier,
        initial_block_id: BlockIdentifier,
    ) {
        self.emerging_threads.insert(thread_id, initial_block_id);
    }

    pub fn collapse_thread(&mut self, thread_id: ThreadIdentifier) {
        self.collapsing_threads.insert(thread_id);
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }
}
