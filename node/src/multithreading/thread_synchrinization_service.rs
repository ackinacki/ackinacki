// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;

// This service must be able to tell threads that there are blocks
// that have messages or accounts for it.
// The easiest solution would be to have a map of thread to
// a vector of tuples of account route to a triplet of a message source.
// This way we can use threads table and thread id to identify source messages.
// A top level map is used to optimize search by skipping threads that were
// already detected having outbound messages (once an outbound message from
// a particular thread was detected we can move to another thread and search
// from there.
// Problem: this will grow.
// It also seems have not much of a benefit vs always referencing all finalized
// blocks from other threads.
// Even more. Referencing only finalized blocks should reduce amount of time
// BK would not be able to verify block in time due to a reference missing.
// ---
// Let's make an initial implementation working with finalized blocks referencing
// for cross-thread messaging and make an implementation working  with optimistic
// thread states later. Test it on real networks and compare results.
//

pub struct ThreadSyncService {
    // Note: Since this map contains only finalized blocks we don't care about
    // possible forks since they must be resolved before finalization.
    // Note: This table may grow in case of thread collapsing and splitting.
    // However it is not a concern in this implementation.
    last_finalized_blocks: HashMap<ThreadIdentifier, BlockIdentifier>,
}

impl ThreadSyncService {
    pub fn on_block_finalized(
        &mut self,
        block_identifier: &BlockIdentifier,
        thread_identifier: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        self.last_finalized_blocks.insert(*thread_identifier, block_identifier.clone());
        Ok(())
    }

    pub fn list_blocks_sending_messages_to_thread(
        &mut self,
        thread_identifier: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<BlockIdentifier>> {
        let other_threads_last_blocks = self
            .last_finalized_blocks
            .iter()
            .filter_map(|(k, v)| if k != thread_identifier { Some(v.clone()) } else { None })
            .collect();
        Ok(other_threads_last_blocks)
    }

    pub fn start(_: ()) -> Self {
        Self { last_finalized_blocks: HashMap::new() }
    }
}
