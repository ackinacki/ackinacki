// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::time::Duration;
use std::time::Instant;

#[cfg(test)]
use crate::block::block_stub;
#[cfg(test)]
use crate::block::block_stub::BlockIdentifierStub;
#[cfg(test)]
use crate::block::producer::process::BlockProducerProcess;
#[cfg(test)]
use crate::block::producer::producer_stub::BlockProducerStub;
use crate::block::producer::BlockProducer;
#[cfg(test)]
use crate::block::MockBlockStruct;
use crate::block_keeper_system::BlockKeeperData;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
#[cfg(test)]
use crate::repository::stub_repository::OptimisticStateStub;
#[cfg(test)]
use crate::repository::stub_repository::RepositoryStub;

#[cfg(test)]
pub struct BlockProducerProcessStub {}

#[cfg(test)]
impl BlockProducerProcess for BlockProducerProcessStub {
    type Block = MockBlockStruct;
    type BlockIdentifier = BlockIdentifierStub;
    type BlockProducer = BlockProducerStub;
    type CandidateBlock = Envelope<GoshBLS, Self::Block>;
    type OptimisticState = OptimisticStateStub;
    type Repository = RepositoryStub;

    fn start_thread_production(
        &mut self,
        _thread_id: &<Self::BlockProducer as BlockProducer>::ThreadIdentifier,
        _prev_block_id: &Self::BlockIdentifier,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn stop_thread_production(
        &mut self,
        _thread_id: &<Self::BlockProducer as BlockProducer>::ThreadIdentifier,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn get_produced_blocks(
        &mut self,
        _thread_id: &<Self::BlockProducer as BlockProducer>::ThreadIdentifier,
    ) -> Vec<(MockBlockStruct, OptimisticStateStub, usize)> {
        todo!()
    }

    fn get_production_iteration_start(&self) -> Instant {
        todo!()
    }

    fn set_timeout(&mut self, _timeout: Duration) {
        todo!()
    }

    fn write_block_to_db(
        &self,
        _block: Envelope<GoshBLS, block_stub::MockBlockStruct>,
        _optimistic_state: Self::OptimisticState,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn send_epoch_message(
        &self,
        _thread_id: &<Self::BlockProducer as BlockProducer>::ThreadIdentifier,
        _data: BlockKeeperData,
    ) {
        todo!()
    }
}
