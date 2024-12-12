// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::mpsc::Receiver;

use tvm_types::Cell;

use crate::block::producer::builder::structs::ActiveThread;
use crate::block::producer::BlockProducer;
use crate::bls::GoshBLS;
use crate::message::message_stub::MessageStub;
use crate::repository::stub_repository::OptimisticStateStub;
use crate::types::AckiNackiBlock;

#[cfg(test)]
pub struct BlockProducerStub {}

#[cfg(test)]
impl BlockProducer for BlockProducerStub {
    type Message = MessageStub;
    type OptimisticState = OptimisticStateStub;

    fn produce<'a, I>(
        self,
        _initial_state: Self::OptimisticState,
        _refs: I,
        _control_rx_stop: Receiver<()>,
    ) -> anyhow::Result<(AckiNackiBlock<GoshBLS>, Self::OptimisticState, Vec<(Cell, ActiveThread)>)>
    where
        I: std::iter::Iterator<Item = &'a Self::OptimisticState> + Clone,
        <Self as BlockProducer>::OptimisticState: 'a,
    {
        todo!()
    }

    fn generate_verify_block<'a, I>(
        self,
        _block: &AckiNackiBlock<GoshBLS>,
        _initial_state: Self::OptimisticState,
        _refs: I,
    ) -> anyhow::Result<(AckiNackiBlock<GoshBLS>, Self::OptimisticState)>
    where
        // TODO: remove Clone and change to Into<>
        I: std::iter::Iterator<Item = &'a Self::OptimisticState> + Clone,
        <Self as BlockProducer>::OptimisticState: 'a,
    {
        todo!()
    }
}
