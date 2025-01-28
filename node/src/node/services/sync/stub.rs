// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::mpsc::Sender;

use crate::block_keeper_system::BlockKeeperSet;
use crate::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BlockStatistics;
use crate::node::services::sync::StateSyncService;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;

pub struct StateSyncServiceStub {}

impl StateSyncService for StateSyncServiceStub {
    type Repository = RepositoryImpl;
    type ResourceAddress = String;

    fn add_share_state_task(
        &mut self,
        _state: <Self::Repository as Repository>::OptimisticState,
        _cross_thread_ref_data: Vec<CrossThreadRefData>,
        _finalized_block_stats: BlockStatistics,
        _bk_set: BlockKeeperSet,
    ) -> anyhow::Result<Self::ResourceAddress> {
        todo!()
    }

    fn add_load_state_task(
        &mut self,
        _resource_address: Self::ResourceAddress,
        _output: Sender<anyhow::Result<(Self::ResourceAddress, Vec<u8>)>>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn generate_resource_address(
        &self,
        _state: &<Self::Repository as Repository>::OptimisticState,
    ) -> anyhow::Result<Self::ResourceAddress> {
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
