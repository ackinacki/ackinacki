// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use http_server::ExtMsgFeedbackList;
use node_types::ParentRef;
use node_types::TemporaryBlockId;
use node_types::ThreadIdentifier;
use telemetry_utils::mpsc::InstrumentedReceiver;
use tvm_types::Cell;

use crate::block::producer::builder::ActiveThread;
use crate::block::producer::execution_time::ExecutionTimeLimits;
use crate::block::producer::BlockProducer;
use crate::external_messages::Stamp;
use crate::message::message_stub::MessageStub;
use crate::repository::stub_repository::OptimisticStateStub;
use crate::repository::CrossThreadRefData;
use crate::storage::MessageDurableStorage;
use crate::types::BlockRound;
use crate::versioning::ProtocolVersion;

#[cfg(test)]
pub struct BlockProducerStub {}

#[cfg(test)]
impl BlockProducer for BlockProducerStub {
    type Message = MessageStub;
    type OptimisticState = OptimisticStateStub;

    fn produce<'a, I>(
        self,
        _thread_identifier: ThreadIdentifier,
        _initial_state: Self::OptimisticState,
        _refs: I,
        _control_rx_stop: InstrumentedReceiver<()>,
        _db: MessageDurableStorage,
        _time_limits: &ExecutionTimeLimits,
        _block_round: BlockRound,
        _parent_block_state: ParentRef,
        _protocol_version: ProtocolVersion,
    ) -> anyhow::Result<(
        super::single_block_producer::Block,
        Self::OptimisticState,
        Vec<(Cell, ActiveThread)>,
        CrossThreadRefData,
        Vec<Stamp>,
        ExtMsgFeedbackList,
        TemporaryBlockId,
    )>
    where
        I: std::iter::Iterator<Item = &'a CrossThreadRefData> + Clone,
        CrossThreadRefData: 'a,
    {
        todo!()
    }
}
