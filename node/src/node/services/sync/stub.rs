// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use telemetry_utils::mpsc::InstrumentedSender;

use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::message_storage::MessageDurableStorage;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::services::sync::StateSyncService;
use crate::node::shared_services::SharedServices;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;

pub struct StateSyncServiceStub {}

impl StateSyncService for StateSyncServiceStub {
    type Repository = RepositoryImpl;
    type ResourceAddress = String;

    fn add_share_state_task(
        &mut self,
        _finalized_block: &Envelope<GoshBLS, AckiNackiBlock>,
        _message_db: &MessageDurableStorage,
        _repository: &RepositoryImpl,
        _block_state_repository: &BlockStateRepository,
        _shared_services: &SharedServices,
    ) -> anyhow::Result<Self::ResourceAddress> {
        todo!()
    }

    fn add_load_state_task(
        &mut self,
        _resource_address: Self::ResourceAddress,
        _output: InstrumentedSender<anyhow::Result<(Self::ResourceAddress, Vec<u8>)>>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn generate_resource_address(
        &self,
        _block_id: &BlockIdentifier,
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
