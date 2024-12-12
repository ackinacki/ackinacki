// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::mpsc::Receiver;
use std::sync::Arc;

use parking_lot::Mutex;
use typed_builder::TypedBuilder;

use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;

// Data for threads sync
#[derive(Clone, Debug)]
pub struct ThreadSyncInfo {
    // Source thread
    pub source_thread_id: ThreadIdentifier,
    // Identifier of the block that has crossthread messages
    pub block_id: BlockIdentifier,
    // Vec of identifiers of destination threads
    pub destination_thread_ids: Vec<ThreadIdentifier>,
}

// Service that spawns senders for all thread nodes, receives ThreadSyncInfo from them and stores
// infos for other threads to take it.
#[derive(TypedBuilder)]
pub struct ThreadSyncService {
    common_receiver: Receiver<ThreadSyncInfo>,
    #[builder(default)]
    saved_sync_infos: HashMap<ThreadIdentifier, Arc<Mutex<Vec<ThreadSyncInfo>>>>,
}

impl ThreadSyncService {
    pub fn add_thread(&mut self, thread_id: ThreadIdentifier) -> Arc<Mutex<Vec<ThreadSyncInfo>>> {
        assert!(!self.saved_sync_infos.contains_key(&thread_id));
        let cache = Arc::new(Mutex::new(vec![]));
        self.saved_sync_infos.insert(thread_id, cache.clone());
        cache
    }

    pub fn execute(&self) -> anyhow::Result<()> {
        loop {
            match self.common_receiver.recv() {
                Err(e) => {
                    tracing::error!("ThreadSyncService: common receiver was disconnected: {e}");
                    anyhow::bail!(e)
                }
                Ok(sync_info) => {
                    tracing::trace!("ThreadSyncService received {sync_info:?}");
                    for thread_id in &sync_info.destination_thread_ids {
                        let mut cache = self
                            .saved_sync_infos
                            .get(thread_id)
                            .expect("Thread sync service received sync for unexpected thread")
                            .lock();
                        tracing::trace!(
                            "ThreadSyncService received push to thread buffer: {sync_info:?}"
                        );
                        cache.push(sync_info.clone());
                    }
                }
            }
        }
    }
}
