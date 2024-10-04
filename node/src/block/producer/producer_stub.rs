// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::mpsc::Receiver;

use tvm_block::ShardStateUnsplit;

use crate::block::producer::BlockProducer;
use crate::block::MockBlockStruct;
#[cfg(test)]
use crate::message::message_stub::MessageStub;
#[cfg(test)]
use crate::transaction::MockTransaction;

#[cfg(test)]
pub struct BlockProducerStub {}

#[cfg(test)]
impl BlockProducer for BlockProducerStub {
    type Block = MockBlockStruct;
    type Cell = ();
    type Message = MessageStub;
    type ShardState = ShardStateUnsplit;
    type ThreadIdentifier = u64;
    type Transaction = MockTransaction;

    fn produce(
        &mut self,
        _control_rx_stop: Receiver<()>,
    ) -> anyhow::Result<(Self::Block, Self::ShardState, Self::Cell)> {
        todo!()
    }

    fn generate_verify_block(
        &self,
        _block: Self::Block,
    ) -> anyhow::Result<(Self::Block, Self::ShardState, Self::Cell)> {
        todo!()
    }
}
