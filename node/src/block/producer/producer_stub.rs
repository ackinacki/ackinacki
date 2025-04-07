// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use http_server::ExtMsgFeedbackList;
use telemetry_utils::mpsc::InstrumentedReceiver;
use tvm_types::Cell;

use crate::block::producer::builder::ActiveThread;
use crate::block::producer::execution_time::ExecutionTimeLimits;
use crate::block::producer::BlockProducer;
use crate::message::message_stub::MessageStub;
use crate::message_storage::MessageDurableStorage;
use crate::repository::stub_repository::OptimisticStateStub;
use crate::repository::CrossThreadRefData;
use crate::types::AckiNackiBlock;
use crate::types::ThreadIdentifier;

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
    ) -> anyhow::Result<(
        AckiNackiBlock,
        Self::OptimisticState,
        Vec<(Cell, ActiveThread)>,
        CrossThreadRefData,
        usize,
        ExtMsgFeedbackList,
    )>
    where
        I: std::iter::Iterator<Item = &'a CrossThreadRefData> + Clone,
        CrossThreadRefData: 'a,
    {
        todo!()
    }
}
