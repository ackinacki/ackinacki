use std::collections::HashMap;

use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;
mod aggregated_thread_load;
use crate::bitmask::mask::Bitmask;
mod in_thread_accounts_load;
mod threads_merge;
mod threads_split;
use aggregated_thread_load::AggregatedLoad;
use aggregated_thread_load::Load;

use crate::repository::optimistic_state::OptimisticState;
use crate::types::AccountRouting;
use crate::types::AckiNackiBlock;

// Note:
// MAX_LOAD_DISPROPORTION can not be less than 2!
// Otherwise it may create infinite splits and collapses on threads table.
// TODO: write an example here.
const MAX_LOAD_DISPROPORTION: Load = 2;
// number of messages in a queue to start splitting a thread
const LOAD_SPLIT_THRESHOLD: Load = 5000;

#[derive(Debug)]
pub struct Proposal {
    pub proposed_threads_table: ThreadsTable,
}

#[derive(Debug)]
pub enum ThreadAction {
    ContinueAsIs,
    Split(Proposal),
    Collapse(Proposal),
}

#[derive(Debug)]
pub enum CheckError {
    StatsAreNotReady,
    ThreadIsNotInTheTable,
}

// Note: This service:
// - monitors load on each thread in the network.
// - decides if a given thread must go.
// - decides if a given thread must be split.
// - uses in-thread-load subservice to generate
//   a new mask for an approximate balance split.
pub struct LoadBalancingService {
    thread_load_map: HashMap<ThreadIdentifier, AggregatedLoad>,
}

impl LoadBalancingService {
    pub fn start() -> Self {
        Self { thread_load_map: HashMap::default() }
    }

    #[allow(clippy::explicit_counter_loop)]
    pub fn check(
        &self,
        block_identifier: &BlockIdentifier,
        thread_identifier: &ThreadIdentifier,
        threads_table: &ThreadsTable,
        max_table_size: usize,
    ) -> anyhow::Result<ThreadAction, CheckError> {
        assert!(max_table_size > 0, "Empty threads table is not allowed");
        let current_load: Load = self.read_load(thread_identifier)?;
        let mut min_load: Load = current_load;
        let mut min_load_thread_id = *thread_identifier;
        let mut max_load: Load = current_load;
        let mut max_load_thread_id = *thread_identifier;
        let mut last_thread_id: Option<ThreadIdentifier> = None;
        let mut pre_last_thread_id: Option<ThreadIdentifier> = None;
        let mut this_thread_row_index: Option<usize> = None;
        let mut this_thread_bitmask: Option<Bitmask<AccountRouting>> = None;
        let mut i: usize = 0;
        for (bitmask, thread) in threads_table.rows() {
            let thread_load = self.read_load(thread)?;
            if thread_load > max_load {
                max_load = thread_load;
                max_load_thread_id = *thread;
            }

            if thread == thread_identifier {
                this_thread_row_index = Some(i);
                this_thread_bitmask = Some(bitmask.clone());
            }
            if min_load >= thread_load {
                // prefer to shift down
                min_load = thread_load;
                min_load_thread_id = *thread;
            }
            pre_last_thread_id = last_thread_id;
            last_thread_id = Some(*thread);
            i += 1;
        }
        let this_thread_row_index =
            this_thread_row_index.ok_or(CheckError::ThreadIsNotInTheTable)?;
        // Checked at the statement above
        let this_thread_bitmask = this_thread_bitmask.unwrap();
        let last_thread_id = last_thread_id.expect("Threads table can not be empty");

        tracing::trace!(
            "Thread {} load: {}. Max load: {}; min load: {}, threads_table.len - {}; max_table_size - {}",
            thread_identifier,
            current_load,
            max_load,
            min_load,
            threads_table.len(),
            max_table_size
        );
        if threads_table.len() > max_table_size {
            threads_merge::try_threads_merge(
                thread_identifier,
                this_thread_row_index,
                &min_load_thread_id,
                pre_last_thread_id,
                &last_thread_id,
                threads_table,
            )
        } else if max_load >= LOAD_SPLIT_THRESHOLD {
            threads_split::try_threads_split(
                block_identifier,
                thread_identifier,
                this_thread_row_index,
                self.thread_load_map
                    .get(thread_identifier)
                    .ok_or(CheckError::ThreadIsNotInTheTable)?,
                &this_thread_bitmask,
                max_load,
                min_load,
                MAX_LOAD_DISPROPORTION,
                &max_load_thread_id,
                threads_table,
                max_table_size,
            )
        } else {
            Ok(ThreadAction::ContinueAsIs)
        }
    }

    pub fn handle_block_finalized<TOptimisticState>(
        &mut self,
        block: &AckiNackiBlock,
        block_state: &mut TOptimisticState,
    ) where
        TOptimisticState: OptimisticState,
    {
        // Note: It does not create unprepared threads.
        // Reason: It must be managed from outside to decide if thread is no longer in use.
        // And since the cleanup is managed from outside it makes sense to prepare threads
        // from the outside control as well.
        let thread_identifier: ThreadIdentifier = block.get_common_section().thread_id;
        if let Some(e) = self.thread_load_map.get_mut(&thread_identifier) {
            e.append_from(block, block_state);
        } else {
            // TODO: This must be a serious error, yet the node can continue in a release build
            panic!("DEBUG: thread must be ready: {:?}", &self.thread_load_map);
        }
    }

    fn read_load(&self, thread_identifier: &ThreadIdentifier) -> anyhow::Result<Load, CheckError> {
        tracing::trace!("read_load: {:?}", thread_identifier);
        let load =
            self.thread_load_map.get(thread_identifier).ok_or(CheckError::StatsAreNotReady)?;
        // tracing::trace!("read_load: load: {:?}", load);
        if !load.is_ready() {
            return Err(CheckError::StatsAreNotReady);
        }
        Ok(load.load_value())
    }
}
impl crate::multithreading::threads_tracking_service::Subscriber for LoadBalancingService {
    fn handle_start_thread(
        &mut self,
        _parent_split_block: &BlockIdentifier,
        thread_identifier: &ThreadIdentifier,
    ) {
        tracing::trace!("load balancing handle_start_thread called");
        self.thread_load_map.insert(*thread_identifier, AggregatedLoad::new());
    }

    fn handle_stop_thread(
        &mut self,
        // TODO: remove
        _last_thread_block: &BlockIdentifier,
        thread_identifier: &ThreadIdentifier,
    ) {
        self.thread_load_map.remove(thread_identifier);
    }
}
