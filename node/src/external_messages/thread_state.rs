// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeSet as StdBTreeSet;
use std::collections::HashSet;
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
use crate::external_messages::ExtMessages;
use crate::external_messages::ExtMessagesLimits;
use crate::external_messages::ExtMessagesSelectionCursor;
use crate::external_messages::QueuedExtMessage;
use crate::external_messages::Stamp;
use crate::helper::metrics::BlockProductionMetrics;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

impl AllowGuardedMut for ExtMessages {}

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
    limits: ExtMessagesLimits,
    feedback_sender: InstrumentedSender<ExtMsgFeedbackList>,
    is_producing: Arc<AtomicBool>,
}

impl From<ExternalMessagesThreadStateConfig> for anyhow::Result<ExternalMessagesThreadState> {
    fn from(config: ExternalMessagesThreadStateConfig) -> Self {
        tracing::trace!(target: "ext_messages", "configured limits: {:?}", config.limits);
        Ok(ExternalMessagesThreadState {
            queue: Arc::new(Mutex::new(ExtMessages::empty(config.limits))),
            report_metrics: config.report_metrics,
            thread_id: config.thread_id,
            feedback_sender: config.feedback_sender,
            is_producing: config.is_producing,
        })
    }
}

#[derive(Clone)]
pub struct ExternalMessagesThreadState {
    queue: Arc<Mutex<ExtMessages>>,
    report_metrics: Option<BlockProductionMetrics>,
    // For reporting only.
    thread_id: ThreadIdentifier,
    feedback_sender: InstrumentedSender<ExtMsgFeedbackList>,
    is_producing: Arc<AtomicBool>,
}

impl ExternalMessagesThreadState {
    pub fn builder() -> ExternalMessagesThreadStateBuilder {
        ExternalMessagesThreadStateConfig::builder()
    }

    pub fn queue_handle(&self) -> Arc<Mutex<ExtMessages>> {
        self.queue.clone()
    }

    fn report_queue_state(
        &self,
        queue_len: usize,
        dapp_queue_sizes: &[(node_types::DAppIdentifier, usize)],
    ) {
        let by_dapp = dapp_queue_sizes
            .iter()
            .map(|(dapp_id, len)| format!("{}={}", dapp_id.to_hex_string(), len))
            .collect::<Vec<_>>()
            .join(",");

        tracing::info!(
            target: "ext_messages",
            "ext_messages_queue_by_dapp: total={}, by_dapp={}",
            queue_len,
            by_dapp
        );
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

        let (report_len, dapp_queue_sizes, unused) = self.queue.guarded_mut(|q| {
            let unused = q.push_external_messages(ext_messages, now);
            (q.len(), q.dapp_queue_sizes(), unused)
        });

        self.report_queue_state(report_len, &dapp_queue_sizes);

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
            .into_iter()
            .map(|(_, msg)| msg)
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

    pub fn erase_processed(&self, processed: &[Stamp]) -> Vec<(Stamp, QueuedExtMessage)> {
        tracing::trace!("erase_processed ext messages: {}", processed.len());

        let (report_len, dapp_queue_sizes, removed) = self.queue.guarded_mut(|q| {
            let removed = q.erase_processed(processed);
            (q.len(), q.dapp_queue_sizes(), removed)
        });

        tracing::trace!(target: "ext_messages", "on erase: queue_size={}", report_len);
        self.report_queue_state(report_len, &dapp_queue_sizes);

        if let Some(metrics) = &self.report_metrics {
            metrics.report_ext_msg_queue_size(report_len, &self.thread_id);
        }

        removed
    }

    pub fn restore_processed(&self, processed: &[(Stamp, QueuedExtMessage)]) {
        if processed.is_empty() {
            return;
        }

        let (report_len, dapp_queue_sizes) = self.queue.guarded_mut(|q| {
            q.restore_processed(processed);
            (q.len(), q.dapp_queue_sizes())
        });

        tracing::trace!(
            target: "ext_messages",
            "restored {} ext messages after production restart, queue_size={}",
            processed.len(),
            report_len
        );
        self.report_queue_state(report_len, &dapp_queue_sizes);

        if let Some(metrics) = &self.report_metrics {
            metrics.report_ext_msg_queue_size(report_len, &self.thread_id);
        }
    }

    pub fn len(&self) -> usize {
        self.queue.guarded(|q| q.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn next_message(
        &self,
        active_destinations: &HashSet<ExtMessageDst>,
        requested_stamps: &StdBTreeSet<Stamp>,
        selection_cursor: &mut ExtMessagesSelectionCursor,
    ) -> Option<(Stamp, QueuedExtMessage)> {
        self.queue
            .guarded(|q| q.next_message(active_destinations, requested_stamps, selection_cursor))
    }
}
