// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use chrono::Utc;
use http_server::ExtMsgFeedbackList;
use node_types::ThreadIdentifier;
use parking_lot::Mutex;
use telemetry_utils::mpsc::InstrumentedSender;
use typed_builder::TypedBuilder;

use crate::block::producer::builder::build_actions::create_not_block_producer_feedback;
use crate::block::producer::builder::build_actions::create_queue_overflow_feedback;
use crate::external_messages::queue::ExtMessageDst;
use crate::external_messages::queue::ExternalMessagesQueue;
use crate::external_messages::QueuedExtMessage;
use crate::external_messages::Stamp;
use crate::helper::metrics::BlockProductionMetrics;
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
    is_producing: Arc<AtomicBool>,
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
            is_producing: config.is_producing,
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
    is_producing: Arc<AtomicBool>,
}

impl ExternalMessagesThreadState {
    pub fn builder() -> ExternalMessagesThreadStateBuilder {
        ExternalMessagesThreadStateConfig::builder()
    }

    pub fn push_external_messages(&self, ext_messages: &[QueuedExtMessage]) -> anyhow::Result<()> {
        tracing::trace!("add_external_messages: {}", ext_messages.len());

        if !self.is_producing.load(Ordering::Acquire) {
            self.clear_queue_for_non_producer()?;

            let feedbacks: Vec<_> = ext_messages
                .iter()
                .map(|msg| create_not_block_producer_feedback(msg.clone(), &self.thread_id))
                .collect::<Result<_, _>>()?;

            if !feedbacks.is_empty() {
                let _ = self.feedback_sender.send(ExtMsgFeedbackList(feedbacks));
            }

            return Ok(());
        }

        let now = Utc::now();

        let (report_len, unused) = self.queue.guarded_mut(|q| {
            let remaining = self.cache_size.saturating_sub(q.messages().len());

            let (to_push, unused) = ext_messages.split_at(remaining.min(ext_messages.len()));

            q.push_external_messages(to_push, now);
            (q.messages().len(), unused.to_vec())
        });

        if !unused.is_empty() {
            let overflow_feedbacks: Vec<_> = unused
                .into_iter()
                .map(|msg| create_queue_overflow_feedback(msg, &self.thread_id))
                .collect::<Result<_, _>>()?;

            let _ = self.feedback_sender.send(ExtMsgFeedbackList(overflow_feedbacks));
        }

        if let Some(metrics) = &self.report_metrics {
            metrics.report_ext_msg_queue_size(report_len, &self.thread_id);
        }

        Ok(())
    }

    pub fn clear_queue_for_non_producer(&self) -> anyhow::Result<()> {
        if self.is_producing.load(Ordering::Acquire) {
            return Ok(());
        }

        let drained = self.queue.guarded_mut(|q| q.drain_all());

        if drained.is_empty() {
            return Ok(());
        }

        tracing::info!(
            target: "ext_messages",
            "Clearing {} ext messages from queue for non-producer thread {:?}",
            drained.len(),
            self.thread_id
        );

        let feedbacks: Vec<_> = drained
            .into_values()
            .map(|msg| create_not_block_producer_feedback(msg, &self.thread_id))
            .collect::<Result<_, _>>()?;

        if !feedbacks.is_empty() {
            let _ = self.feedback_sender.send(ExtMsgFeedbackList(feedbacks));
        }

        if let Some(metrics) = &self.report_metrics {
            metrics.report_ext_msg_queue_size(0, &self.thread_id);
        }

        Ok(())
    }

    pub fn erase_processed(&self, processed: &[Stamp]) {
        tracing::trace!("erase_processed ext messages: {}", processed.len());

        let report_len = self.queue.guarded_mut(|q| {
            q.erase_processed(processed);
            q.messages().len()
        });

        tracing::trace!(target: "ext_messages", "on erase: queue_size={}", report_len);

        if let Some(metrics) = &self.report_metrics {
            metrics.report_ext_msg_queue_size(report_len, &self.thread_id);
        }
    }

    pub fn get_remaining_external_messages(
        &self,
    ) -> HashMap<ExtMessageDst, VecDeque<(Stamp, QueuedExtMessage)>> {
        tracing::trace!("get_remaining_externals");
        self.queue.guarded(|q| q.unprocessed_messages())
    }
}
