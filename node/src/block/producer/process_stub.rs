// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::time::Duration;
use std::time::Instant;

#[cfg(test)]
use crate::block::producer::process::BlockProducerProcess;
#[cfg(test)]
use crate::block::producer::producer_stub::BlockProducerStub;
use crate::block_keeper_system::BlockKeeperData;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
#[cfg(test)]
use crate::repository::stub_repository::OptimisticStateStub;
#[cfg(test)]
use crate::repository::stub_repository::RepositoryStub;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;

#[cfg(test)]
pub struct BlockProducerProcessStub {}

#[cfg(test)]
impl BlockProducerProcess for BlockProducerProcessStub {
    type BLSSignatureScheme = GoshBLS;
    type BlockProducer = BlockProducerStub;
    type CandidateBlock =
        Envelope<GoshBLS, AckiNackiBlock<<Self as BlockProducerProcess>::BLSSignatureScheme>>;
    type OptimisticState = OptimisticStateStub;
    type Repository = RepositoryStub;

    fn start_thread_production(
        &mut self,
        _thread_id: &ThreadIdentifier,
        _prev_block_id: &BlockIdentifier,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn stop_thread_production(&mut self, _thread_id: &ThreadIdentifier) -> anyhow::Result<()> {
        todo!()
    }

    fn get_produced_blocks(
        &mut self,
        _thread_id: &ThreadIdentifier,
    ) -> Vec<(
        AckiNackiBlock<<Self as BlockProducerProcess>::BLSSignatureScheme>,
        OptimisticStateStub,
        usize,
    )> {
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
        _block: Envelope<
            GoshBLS,
            AckiNackiBlock<<Self as BlockProducerProcess>::BLSSignatureScheme>,
        >,
        _optimistic_state: Self::OptimisticState,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn send_epoch_message(&self, _thread_id: &ThreadIdentifier, _data: BlockKeeperData) {
        todo!()
    }

    fn add_state_to_cache(&mut self, _thread_id: ThreadIdentifier, _state: Self::OptimisticState) {
        todo!()
    }
}
