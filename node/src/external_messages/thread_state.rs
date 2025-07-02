// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use parking_lot::Mutex;
use tvm_block::Message;
use typed_builder::TypedBuilder;

use super::queue::ExternalMessagesQueue;
use crate::external_messages::Stamp;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message::WrappedMessage;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[derive(TypedBuilder)]
#[builder(
    build_method(vis="pub", into=anyhow::Result<ExternalMessagesThreadState>),
    builder_method(vis="pub"),
    builder_type(vis="pub", name=ExternalMessagesThreadStateBuilder),
    field_defaults(setter(prefix="with_")),
)]
struct ExternalMessagesThreadStateConfig {
    report_metrics: Option<BlockProductionMetrics>,
    thread_id: ThreadIdentifier,
    cache_size: usize,
}

impl std::convert::From<ExternalMessagesThreadStateConfig>
    for anyhow::Result<ExternalMessagesThreadState>
{
    fn from(
        config: ExternalMessagesThreadStateConfig,
    ) -> anyhow::Result<ExternalMessagesThreadState> {
        let ExternalMessagesThreadStateConfig { report_metrics, thread_id, cache_size } = config;
        let queue: ExternalMessagesQueue = ExternalMessagesQueue::empty();
        Ok(ExternalMessagesThreadState {
            queue: Arc::new(Mutex::new(queue)),
            report_metrics,
            thread_id,
            cache_size,
        })
    }
}

// Note (obsolete?):
// It is important to keep external messages and their progress in sync.
// If one is stored in a durable storage the other one must be stored aswell.
// This is the main reason why progress is not stored in BlockState directly,
// and rather stored together with the external messages.
// The other side effect is that the progress for external messages
// is different for each thread while block may exist in several threads
// simultaneously (thread starting block).
#[derive(Clone)]
pub struct ExternalMessagesThreadState {
    queue: Arc<Mutex<ExternalMessagesQueue>>,
    report_metrics: Option<BlockProductionMetrics>,
    // For reporting only.
    thread_id: ThreadIdentifier,
    cache_size: usize,
}

impl ExternalMessagesThreadState {
    pub fn builder() -> ExternalMessagesThreadStateBuilder {
        ExternalMessagesThreadStateConfig::builder()
    }

    pub fn push_external_messages(&self, messages: &[WrappedMessage]) -> anyhow::Result<()> {
        tracing::trace!("add_external_messages {:?}", messages.len());
        let report_len = self.queue.guarded_mut(|e| -> anyhow::Result<usize> {
            let rest = self.cache_size.saturating_sub(e.messages().len()).min(messages.len());
            if rest > 0 {
                e.push_external_messages(messages.split_at(rest).0);
                tracing::trace!("cur cache_size len {:?}", e.messages().len());
            }
            Ok(e.messages().len())
        })?;
        self.report_metrics
            .as_ref()
            .inspect(|e| e.report_ext_msg_queue_size(report_len, &self.thread_id));
        Ok(())
    }

    pub fn erase_processed(&mut self, processed: &Vec<Stamp>) -> anyhow::Result<()> {
        tracing::trace!("erase_processed ext messages {}", processed.len());

        let report_len = self.queue.guarded_mut(|q| {
            q.erase_processed(processed);
            q.messages().len()
        });

        self.report_metrics
            .as_ref()
            .inspect(|e| e.report_ext_msg_queue_size(report_len, &self.thread_id));

        Ok(())
    }

    pub fn get_remaining_external_messages(&self) -> anyhow::Result<Vec<(Stamp, Message)>> {
        tracing::trace!("get_remaining_externals");

        self.queue.guarded(|q| Ok(q.unprocessed_messages()))
    }
}
