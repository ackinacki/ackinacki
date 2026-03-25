// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::sync::Arc;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use telemetry_utils::mpsc::InstrumentedSender;

use crate::node::services::sync::StateSyncService;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;

pub struct StateSyncServiceStub {}

impl StateSyncService for StateSyncServiceStub {
    type Repository = RepositoryImpl;

    fn is_load_thread_available(&self) -> bool {
        todo!()
    }

    fn save_state_for_sharing(
        &self,
        _block_id: &BlockIdentifier,
        _thread_id: &ThreadIdentifier,
        _min_state: Option<Arc<OptimisticStateImpl>>,
        _finalizing_block_id: BlockIdentifier,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn add_load_state_task(
        &mut self,
        _resource_address: BTreeMap<ThreadIdentifier, BlockIdentifier>,
        _repository: RepositoryImpl,
        _output: InstrumentedSender<anyhow::Result<BTreeMap<ThreadIdentifier, BlockIdentifier>>>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn flush(&self) -> anyhow::Result<()> {
        todo!()
    }
}
impl Default for StateSyncServiceStub {
    fn default() -> Self {
        Self::new()
    }
}

impl StateSyncServiceStub {
    pub fn new() -> Self {
        Self {}
    }
}
