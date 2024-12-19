// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::mpsc::Sender;

use crate::node::services::sync::StateSyncService;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::block_keeper_ring::BlockKeeperRing;
use crate::types::ThreadIdentifier;

pub struct StateSyncServiceStub {}

impl StateSyncService for StateSyncServiceStub {
    type Repository = RepositoryImpl;
    type ResourceAddress = String;

    fn add_share_state_task(
        &mut self,
        _state: <Self::Repository as Repository>::OptimisticState,
        _block_producer_groups: HashMap<
            ThreadIdentifier,
            Vec<<Self::Repository as Repository>::NodeIdentifier>,
        >,
        _block_keeper_set: BlockKeeperRing,
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
