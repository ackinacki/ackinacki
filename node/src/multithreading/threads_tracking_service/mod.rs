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
pub struct ThreadsTrackingService {}

impl ThreadsTrackingService {
    pub fn start() -> Self {
        Self {}
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
        for thread in threads_set.iter() {
            subscribers.handle_start_thread(&block_identifier, thread, None);
        }
    }

    pub fn handle_block_invalidated(
        &mut self,
        _block_identifier: BlockIdentifier,
    ) -> anyhow::Result<(), CommandError> {
        Ok(())
    }

    pub fn handle_block_finalized<T>(
        &mut self,
        block_identifier: BlockIdentifier,
        thread_identifier: ThreadIdentifier,
        threads_table_after_this_block: ThreadsTable,
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
        let this_table = threads_table_after_this_block
            .list_threads()
            .cloned()
            .collect::<Vec<ThreadIdentifier>>();
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
            subscribers.handle_start_thread(
                &block_identifier,
                thread,
                Some(threads_table_after_this_block.clone()),
            );
        }
        tracing::trace!("threads tracking service: handle_block_finalized: checkpoint 3",);
        // Note: removed threads
        // It is possible that it was a last block in this thread. However
        // it is also possible that this was a result of a previous syncronization.
        // This is easy to check though. We allow threads to "self-destroy" and
        // no other option. So, we only notify of "self-destroyed" threads.
        if !this_table.contains(&thread_identifier) {
            subscribers.handle_stop_thread(&block_identifier, &thread_identifier);
        }

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

        // // First trait to implement
        impl Subscriber for SubscriberImpl {
            fn handle_start_thread(
                &mut self,
                parent_split_block: &BlockIdentifier,
                thread_identifier: &ThreadIdentifier,
                _threads_table: Option<ThreadsTable>,
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
        some_subscriber.expect_handle_start_thread().times(1).return_const(());
        some_other_subscriber.expect_handle_start_thread().times(1).return_const(());
        some_subscriber.expect_handle_stop_thread().times(0).return_const(());
        some_other_subscriber.expect_handle_stop_thread().times(0).return_const(());
        let mut service = ThreadsTrackingService::start();
        let _ = service.handle_block_finalized(
            BlockIdentifier::default(),
            ThreadIdentifier::default(),
            ThreadsTable::default(),
            &mut (&mut some_subscriber, &mut some_other_subscriber),
        );
    }
}
