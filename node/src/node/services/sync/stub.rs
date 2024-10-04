// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::mpsc::Sender;

use crate::node::services::sync::StateSyncService;

pub struct StateSyncServiceStub {}

impl StateSyncService for StateSyncServiceStub {
    type ResourceAddress = String;

    fn add_share_state_task(&mut self, _data: Vec<u8>) -> anyhow::Result<Self::ResourceAddress> {
        Ok("".to_string())
    }

    fn add_load_state_task(
        &mut self,
        _resource_address: Self::ResourceAddress,
        _output: Sender<anyhow::Result<(Self::ResourceAddress, Vec<u8>)>>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn generate_resource_address(&self, _data: &[u8]) -> anyhow::Result<Self::ResourceAddress> {
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
