// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::Arc;

use telemetry_utils::mpsc::InstrumentedSender;

use crate::node::services::sync::StateSyncService;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;

pub struct StateSyncServiceStub {}

impl StateSyncService for StateSyncServiceStub {
    type Repository = RepositoryImpl;

    fn reset_sync(&self) {
        todo!()
    }

    fn save_state_for_sharing(&self, _state: Arc<OptimisticStateImpl>) -> anyhow::Result<()> {
        todo!()
    }

    fn add_load_state_task(
        &mut self,
        _resource_address: HashMap<ThreadIdentifier, BlockIdentifier>,
        _repository: RepositoryImpl,
        _output: InstrumentedSender<anyhow::Result<()>>,
    ) -> anyhow::Result<()> {
        Ok(())
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
