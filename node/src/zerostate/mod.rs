// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

mod parse;
pub mod serialize;
mod update;

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use tvm_block::ShardStateUnsplit;
use versioned_struct::versioned;
use versioned_struct::Transitioning;

use crate::block_keeper_system::BlockKeeperSet;
use crate::block_keeper_system::BlockKeeperSetOld;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

#[versioned]
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ZeroState {
    states: HashMap<ThreadIdentifier, OptimisticStateImpl>,
    #[legacy]
    block_keeper_set: HashMap<ThreadIdentifier, BlockKeeperSetOld>,
    #[future]
    block_keeper_set: HashMap<ThreadIdentifier, BlockKeeperSet>,
}

impl Transitioning for ZeroState {
    type Old = ZeroStateOld;

    fn from(old: Self::Old) -> Self {
        let block_keeper_set =
            old.block_keeper_set.into_iter().map(|(k, v)| (k, v.into())).collect();
        Self { block_keeper_set, states: old.states }
    }
}

impl ZeroState {
    pub(crate) fn get_shard_state(
        &mut self,
        thread_identifier: &ThreadIdentifier,
    ) -> anyhow::Result<Arc<ShardStateUnsplit>> {
        self.state_mut(thread_identifier).map(|opt_state| opt_state.get_shard_state())
    }

    pub fn get_threads_table(&self) -> &ThreadsTable {
        // Note: Assumed that all states in zerostate have the same threads table
        &self.states.iter().next().expect("Zerostate doesn't contain states").1.threads_table
    }

    pub fn get_block_keeper_set(&self) -> anyhow::Result<BlockKeeperSet> {
        // BK set organization changed gue to dynamic thread ID generation and we've decided to have
        // single common BK set and use thread ID salt in leader group generation.
        assert!(self.block_keeper_set.len() == 1);
        Ok(self.block_keeper_set.values().next().unwrap().clone())
    }

    pub fn unwrapped_block_keeper_sets(&self) -> &HashMap<ThreadIdentifier, BlockKeeperSet> {
        &self.block_keeper_set
    }

    pub fn states(&self) -> &HashMap<ThreadIdentifier, OptimisticStateImpl> {
        &self.states
    }

    pub fn states_mut(&mut self) -> &mut HashMap<ThreadIdentifier, OptimisticStateImpl> {
        &mut self.states
    }

    pub fn state_mut(
        &mut self,
        thread_identifier: &ThreadIdentifier,
    ) -> anyhow::Result<&mut OptimisticStateImpl> {
        self.states.get_mut(thread_identifier).ok_or(anyhow::format_err!(
            "Zerostate does not contain state (mut) for requested thread"
        ))
    }

    pub fn state(
        &self,
        thread_identifier: &ThreadIdentifier,
    ) -> anyhow::Result<&OptimisticStateImpl> {
        self.states
            .get(thread_identifier)
            .ok_or(anyhow::format_err!("Zerostate does not contain state for requested thread"))
    }

    pub fn init_thread(&mut self, thread_identifier: ThreadIdentifier) {
        let state = OptimisticStateImpl { thread_id: thread_identifier, ..Default::default() };
        self.states.insert(thread_identifier, state);
    }

    pub fn set_threads_table(&mut self, threads_table: ThreadsTable) {
        for (_, state) in self.states.iter_mut() {
            state.threads_table = threads_table.clone();
        }
    }

    pub fn list_threads(&self) -> impl Iterator<Item = &'_ ThreadIdentifier> {
        self.states.keys()
    }
}
