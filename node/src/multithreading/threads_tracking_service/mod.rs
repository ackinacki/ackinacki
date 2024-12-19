use std::collections::HashMap;
use std::collections::HashSet;

use thiserror::Error;

use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

mod subscribers;
pub use subscribers::Subscriber;

// Note:
// The only responsibility of this service is to track threads table updates
// and notify if any thread is being created or destroyed.
// Note:
// This implementation assumes that blocks will be finalized in the right order

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct BlockData {
    pub parent_block_identifier: BlockIdentifier,
    pub block_identifier: BlockIdentifier,
    pub thread_identifier: ThreadIdentifier,
    pub threads_list: HashSet<ThreadIdentifier>,
}

#[derive(Clone, Debug)]
pub struct AppendedBlockData {
    pub parent_block_identifier: BlockIdentifier,
    pub block_identifier: BlockIdentifier,
    pub thread_identifier: ThreadIdentifier,
    pub threads_table: Option<ThreadsTable>,
}

#[derive(Clone)]
pub struct CheckpointBlockData {
    pub parent_block_identifier: BlockIdentifier,
    pub block_identifier: BlockIdentifier,
    pub thread_identifier: ThreadIdentifier,
    pub threads_table: ThreadsTable,
}

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("There is no data stored for the given block id. If could be a critical issue depending on the command being executed.")]
    BlockDataMissing,
    #[error("It appears that a block referenced did not have this thread as a dependency. It might be that this dependency was erased before due to a bug. This issue must be investigated.")]
    UnexpectedDependency,
    #[error("Parent block missing. This could be a critical issue in some cases.")]
    ParentBlockMissing,

    #[error("Service is in inconsistent state.")]
    CriticalInconsistency,
}

#[derive(Debug)]
pub struct ThreadsTrackingService {
    tables: HashMap<BlockIdentifier, HashSet<ThreadIdentifier>>,
    block_data: HashMap<BlockIdentifier, BlockData>,

    // This tracks how long a table has to be stored.
    // As long as all its descendant threads have at least one block
    // finalized it can drop its data from the tables map.
    block_tracked_dependencies: HashMap<BlockIdentifier, HashSet<ThreadIdentifier>>,
}

impl ThreadsTrackingService {
    pub fn start() -> Self {
        Self {
            tables: HashMap::default(),
            block_data: HashMap::default(),
            block_tracked_dependencies: HashMap::default(),
        }
    }

    pub fn init_thread<T>(
        &mut self,
        block_identifier: BlockIdentifier,
        threads_set: HashSet<ThreadIdentifier>,
        subscribers: &mut T,
    ) where
        T: Subscriber,
    {
        tracing::trace!(
            "ThreadsTrackingService: init block_id: {:?}, threads_set: {:?}",
            block_identifier,
            threads_set
        );
        self.block_tracked_dependencies.insert(block_identifier.clone(), threads_set.clone());
        self.tables.insert(block_identifier.clone(), threads_set.clone());
        for thread in threads_set.iter() {
            subscribers.handle_start_thread(&block_identifier, thread);
        }
    }

    pub fn handle_block_appended(
        &mut self,
        block_data: AppendedBlockData,
    ) -> anyhow::Result<(), CommandError> {
        tracing::trace!(
            "ThreadsTrackingService: append: block_data: {:?}, self: {:?}",
            block_data,
            self
        );
        let parent_table = self
            .tables
            .get(&block_data.parent_block_identifier)
            .ok_or(CommandError::ParentBlockMissing)?;
        let AppendedBlockData {
            parent_block_identifier,
            block_identifier,
            thread_identifier,
            threads_table,
        } = block_data;
        let table = threads_table
            .map(|e| e.list_threads().copied().collect::<HashSet<ThreadIdentifier>>())
            .unwrap_or(parent_table.clone());
        self.block_data.insert(
            block_identifier.clone(),
            BlockData {
                parent_block_identifier: parent_block_identifier.clone(),
                block_identifier: block_identifier.clone(),
                thread_identifier,
                threads_list: table.clone(),
            },
        );
        self.block_tracked_dependencies.insert(block_identifier.clone(), table.clone());
        self.tables.insert(block_identifier.clone(), table);
        Ok(())
    }

    pub fn handle_checkpoint_block_appended(
        &mut self,
        block_data: CheckpointBlockData,
    ) -> anyhow::Result<(), CommandError> {
        let CheckpointBlockData {
            parent_block_identifier,
            block_identifier,
            thread_identifier,
            threads_table,
        } = block_data;
        let table = threads_table.list_threads().copied().collect::<HashSet<ThreadIdentifier>>();
        self.block_data.insert(
            block_identifier.clone(),
            BlockData {
                parent_block_identifier: parent_block_identifier.clone(),
                block_identifier: block_identifier.clone(),
                thread_identifier,
                threads_list: table.clone(),
            },
        );
        self.block_tracked_dependencies.insert(block_identifier.clone(), table.clone());
        self.tables.insert(block_identifier.clone(), table);
        Ok(())
    }

    pub fn handle_block_invalidated(
        &mut self,
        block_identifier: BlockIdentifier,
    ) -> anyhow::Result<(), CommandError> {
        self.erase_block(&block_identifier)?;
        Ok(())
    }

    pub fn handle_block_finalized<T>(
        &mut self,
        block_identifier: BlockIdentifier,
        thread_identifier: ThreadIdentifier,
        subscribers: &mut T,
    ) -> anyhow::Result<(), CommandError>
    where
        T: Subscriber,
    {
        tracing::trace!(
            "threads tracking service: handle_block_finalized: ({})->{}",
            &thread_identifier,
            &block_identifier,
        );
        let block_data =
            self.block_data.get(&block_identifier).ok_or(CommandError::BlockDataMissing)?;
        let erase_parent = {
            if block_data.parent_block_identifier == block_identifier {
                assert!(block_identifier == BlockIdentifier::default(), "Zerostate edge case");
                false
            } else {
                let parent_depenencies =
                    self.block_tracked_dependencies.get_mut(&block_data.parent_block_identifier);
                if let Some(parent_depenencies) = parent_depenencies {
                    if !parent_depenencies.remove(&thread_identifier) {
                        return Err(CommandError::UnexpectedDependency);
                    }

                    parent_depenencies.is_empty()
                } else {
                    false
                }
            }
        };
        tracing::trace!("threads tracking service: handle_block_finalized: checkpoint 1",);
        let this_table =
            self.tables.get(&block_identifier).ok_or(CommandError::CriticalInconsistency)?;
        tracing::trace!("threads tracking service: handle_block_finalized: checkpoint 2",);
        // Note: new threads
        // There could be few options why a new thread was added.
        // It could be that it was spawned here or it was a syncronization
        // to another thread. For the simplification is_spawning_block function is added.
        let new_threads = {
            let mut threads = this_table.clone();
            threads.retain(|e| e.is_spawning_block(&block_identifier));
            threads
        };
        for thread in new_threads.iter() {
            subscribers.handle_start_thread(&block_identifier, thread);
        }
        tracing::trace!("threads tracking service: handle_block_finalized: checkpoint 3",);
        // Note: removed threads
        // It is possible that it was a last block in this thread. However
        // it is also possible that this was a result of a previous syncronization.
        // This is easy to check though. We allow threads to "self-destroy" and
        // no other option. So, we only notify of "self-destroyed" threads.
        if !this_table.contains(&block_data.thread_identifier) {
            subscribers.handle_stop_thread(&block_identifier, &thread_identifier);
        }

        let parent_block_identifier = block_data.parent_block_identifier.clone();
        if erase_parent {
            tracing::trace!("threads tracking service: erase_parent {}", &parent_block_identifier,);
            self.erase_block(&parent_block_identifier)?;
        }
        Ok(())
    }

    fn erase_block(
        &mut self,
        block_identifier: &BlockIdentifier,
    ) -> anyhow::Result<(), CommandError> {
        self.block_data.remove(block_identifier);
        self.block_tracked_dependencies.remove(block_identifier);
        self.tables.remove(block_identifier);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mockall::mock;

    use super::*;

    mock! {
        // Structure to mock
        SubscriberImpl {}

        // First trait to implement
        impl Subscriber for SubscriberImpl {
            fn handle_start_thread(
                &mut self,
                parent_split_block: &BlockIdentifier,
                thread_identifier: &ThreadIdentifier,
            );
            fn handle_stop_thread(
                &mut self,
                last_thread_block: &BlockIdentifier,
                thread_identifier: &ThreadIdentifier,
            );
        }
    }

    #[test]
    pub fn in_must_be_able_to_use_tuple_subscribers_as_event_handler() {
        let mut some_subscriber = MockSubscriberImpl::new();
        let mut some_other_subscriber = MockSubscriberImpl::new();
        let mut service = ThreadsTrackingService::start();
        let _ = service.handle_block_finalized(
            BlockIdentifier::default(),
            ThreadIdentifier::default(),
            &mut (&mut some_subscriber, &mut some_other_subscriber),
        );
    }
}
