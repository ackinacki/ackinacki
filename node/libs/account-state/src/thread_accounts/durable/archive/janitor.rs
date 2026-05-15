#![allow(dead_code)]
use std::sync::Arc;

/// Background janitor for deleting stale epoch records.
/// After reset(), records with epoch < current_data_epoch should be deleted.
pub struct StateJanitor {}

pub struct JanitorConfig {
    pub aerospike_address: String,
    pub delete_parallelism: usize,
    pub poll_interval_ms: u64,
}

impl StateJanitor {
    pub fn new(_config: JanitorConfig) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {}))
    }

    /// Enqueue a full scan of the accounts set, deleting all records
    /// whose epoch bin is less than `current_data_epoch`.
    pub fn enqueue_drop_stale_epoch(&self, _node_id: String, _current_data_epoch: u64) {
        tracing::trace!(
            target: "monit",
            "Janitor: enqueue_drop_stale_epoch called (not yet implemented)"
        );
    }

    /// Block until the deletion queue is empty.
    pub fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
