// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use http_server::ExtMsgFeedback;
use parking_lot::Mutex;
use tvm_types::UInt256;

#[cfg(test)]
use crate::block::producer::process::BlockProducerProcess;
#[cfg(test)]
use crate::block::producer::producer_stub::BlockProducerStub;
use crate::block_keeper_system::BlockKeeperData;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::associated_types::AckData;
use crate::node::associated_types::NackData;
use crate::node::block_state::repository::BlockStateRepository;
#[cfg(test)]
use crate::repository::stub_repository::OptimisticStateStub;
#[cfg(test)]
use crate::repository::stub_repository::RepositoryStub;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::utilities::FixedSizeHashSet;

#[cfg(test)]
pub struct BlockProducerProcessStub {}

#[cfg(test)]
impl BlockProducerProcess for BlockProducerProcessStub {
    type Ack = Envelope<Self::BLSSignatureScheme, AckData>;
    type BLSSignatureScheme = GoshBLS;
    type BlockProducer = BlockProducerStub;
    type CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>;
    type Nack = Envelope<Self::BLSSignatureScheme, NackData>;
    type OptimisticState = OptimisticStateStub;
    type Repository = RepositoryStub;

    fn start_thread_production(
        &mut self,
        _thread_id: &ThreadIdentifier,
        _prev_block_id: &BlockIdentifier,
        _feedback_sender: Sender<Vec<ExtMsgFeedback>>,
        _received_acks: Arc<Mutex<Vec<Envelope<Self::BLSSignatureScheme, AckData>>>>,
        _received_nacks: Arc<Mutex<Vec<Envelope<Self::BLSSignatureScheme, NackData>>>>,
        _block_state: BlockStateRepository,
        _nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn stop_thread_production(&mut self, _thread_id: &ThreadIdentifier) -> anyhow::Result<()> {
        todo!()
    }

    fn get_produced_blocks(
        &mut self,
        _thread_id: &ThreadIdentifier,
    ) -> Vec<(AckiNackiBlock, OptimisticStateStub, usize)> {
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
        _block: Envelope<GoshBLS, AckiNackiBlock>,
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
