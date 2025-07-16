// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use chrono::Utc;
use http_server::ExtMsgFeedbackList;
use parking_lot::Mutex;
use telemetry_utils::mpsc::InstrumentedSender;
use tvm_block::Message;
use tvm_types::AccountId;
use typed_builder::TypedBuilder;

use crate::block::producer::builder::build_actions::create_queue_overflow_feedback;
use crate::external_messages::queue::ExternalMessagesQueue;
use crate::external_messages::Stamp;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message::WrappedMessage;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

impl AllowGuardedMut for ExternalMessagesQueue {}

#[derive(TypedBuilder)]
#[builder(
    build_method(vis="pub", into=anyhow::Result<ExternalMessagesThreadState>),
    builder_method(vis="pub"),
    builder_type(vis="pub", name=ExternalMessagesThreadStateBuilder),
    field_defaults(setter(prefix="with_")),
)]
pub struct ExternalMessagesThreadStateConfig {
    report_metrics: Option<BlockProductionMetrics>,
    thread_id: ThreadIdentifier,
    cache_size: usize,
    feedback_sender: InstrumentedSender<ExtMsgFeedbackList>,
}

impl From<ExternalMessagesThreadStateConfig> for anyhow::Result<ExternalMessagesThreadState> {
    fn from(config: ExternalMessagesThreadStateConfig) -> Self {
        tracing::trace!(target: "ext_messages", "configured cache_size: {}", config.cache_size);
        Ok(ExternalMessagesThreadState {
            queue: Arc::new(Mutex::new(ExternalMessagesQueue::empty())),
            report_metrics: config.report_metrics,
            thread_id: config.thread_id,
            cache_size: config.cache_size,
            feedback_sender: config.feedback_sender,
        })
    }
}

#[derive(Clone)]
pub struct ExternalMessagesThreadState {
    queue: Arc<Mutex<ExternalMessagesQueue>>,
    report_metrics: Option<BlockProductionMetrics>,
    // For reporting only.
    thread_id: ThreadIdentifier,
    cache_size: usize,
    feedback_sender: InstrumentedSender<ExtMsgFeedbackList>,
}

impl ExternalMessagesThreadState {
    pub fn builder() -> ExternalMessagesThreadStateBuilder {
        ExternalMessagesThreadStateConfig::builder()
    }

    pub fn push_external_messages(&self, messages: &[WrappedMessage]) -> anyhow::Result<()> {
        tracing::trace!("add_external_messages: {}", messages.len());

        let now = Utc::now();

        let (report_len, unused) = self.queue.guarded_mut(|q| {
            let remaining = self.cache_size.saturating_sub(q.messages().len());

            let (to_push, unused) = messages.split_at(remaining.min(messages.len()));

            q.push_external_messages(to_push, now);
            (q.messages().len(), unused.to_vec())
        });

        if !unused.is_empty() {
            let overflow_feedbacks: Vec<_> = unused
                .into_iter()
                .map(|msg| create_queue_overflow_feedback(msg.message, &self.thread_id))
                .collect::<Result<_, _>>()?;

            let _ = self.feedback_sender.send(ExtMsgFeedbackList(overflow_feedbacks));
        }

        if let Some(metrics) = &self.report_metrics {
            metrics.report_ext_msg_queue_size(report_len, &self.thread_id);
        }

        Ok(())
    }

    pub fn erase_processed(&self, processed: &[Stamp]) -> anyhow::Result<()> {
        tracing::trace!("erase_processed ext messages: {}", processed.len());

        let report_len = self.queue.guarded_mut(|q| {
            q.erase_processed(processed);
            q.messages().len()
        });

        tracing::trace!(target: "ext_messages", "on erase: queue_size={}", report_len);

        if let Some(metrics) = &self.report_metrics {
            metrics.report_ext_msg_queue_size(report_len, &self.thread_id);
        }

        Ok(())
    }

    pub fn get_remaining_external_messages(
        &self,
    ) -> anyhow::Result<HashMap<AccountId, VecDeque<(Stamp, Message)>>> {
        tracing::trace!("get_remaining_externals");
        self.queue.guarded(|q| Ok(q.unprocessed_messages()))
    }
}
