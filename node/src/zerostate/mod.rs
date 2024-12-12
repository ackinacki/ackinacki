// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

mod parse;
pub mod serialize;
mod update;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use tvm_block::ShardStateUnsplit;

use crate::block_keeper_system::BlockKeeperSet;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ZeroState {
    states: HashMap<ThreadIdentifier, OptimisticStateImpl>,
    block_keeper_sets: HashMap<ThreadIdentifier, BlockKeeperSet>,
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

    pub fn get_block_keeper_sets(
        &self,
    ) -> anyhow::Result<HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>> {
        Ok(self
            .block_keeper_sets
            .clone()
            .into_iter()
            .map(|(k, v)| {
                let mut map = BTreeMap::new();
                map.insert(BlockSeqNo::from(0), v);
                (k, map)
            })
            .collect())
    }

    pub fn unwrapped_block_keeper_sets(&self) -> &HashMap<ThreadIdentifier, BlockKeeperSet> {
        &self.block_keeper_sets
    }

    pub fn states_mut(&mut self) -> &mut HashMap<ThreadIdentifier, OptimisticStateImpl> {
        &mut self.states
    }

    pub fn state_mut(
        &mut self,
        thread_identifier: &ThreadIdentifier,
    ) -> anyhow::Result<&mut OptimisticStateImpl> {
        self.states
            .get_mut(thread_identifier)
            .ok_or(anyhow::format_err!("Zerostate does not contain state for requested thread"))
    }

    pub fn state(
        &self,
        thread_identifier: &ThreadIdentifier,
    ) -> anyhow::Result<&OptimisticStateImpl> {
        self.states
            .get(thread_identifier)
            .ok_or(anyhow::format_err!("Zerostate does not contain state for requested thread"))
    }

    pub fn add_thread_state(&mut self, thread_identifier: ThreadIdentifier) {
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
