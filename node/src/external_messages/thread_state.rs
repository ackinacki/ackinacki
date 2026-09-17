// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeSet as StdBTreeSet;
use std::collections::HashSet;
use std::env;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;

use chrono::Utc;
use http_server::ExtMsgFeedbackList;
use node_types::DAppIdentifier;
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

const EXT_MESSAGES_QUEUE_SIZE_METRIC_DAPPS_ENV: &str = "EXT_MESSAGES_QUEUE_SIZE_METRIC_DAPPS";
const DEFAULT_EXT_MESSAGES_QUEUE_SIZE_METRIC_DAPPS: &[DAppIdentifier] = &[
    DAppIdentifier::ZERO,
    dapp_identifier_with_last_byte(1),
    dapp_identifier_with_last_byte(2),
    dapp_identifier_with_last_byte(4),
];

static EXT_MESSAGES_QUEUE_SIZE_METRIC_DAPPS: OnceLock<StdBTreeSet<DAppIdentifier>> =
    OnceLock::new();

const fn dapp_identifier_with_last_byte(last_byte: u8) -> DAppIdentifier {
    let mut bytes = [0u8; 32];
    bytes[31] = last_byte;
    DAppIdentifier::new(bytes)
}

fn ext_messages_queue_size_metric_dapps() -> &'static StdBTreeSet<DAppIdentifier> {
    EXT_MESSAGES_QUEUE_SIZE_METRIC_DAPPS.get_or_init(|| {
        env::var(EXT_MESSAGES_QUEUE_SIZE_METRIC_DAPPS_ENV)
            .ok()
            .and_then(|value| {
                let dapps = parse_metric_dapps(&value);
                if dapps.is_empty() {
                    tracing::warn!(
                        target: "ext_messages",
                        "{} is set but does not contain valid DApp ids; using defaults",
                        EXT_MESSAGES_QUEUE_SIZE_METRIC_DAPPS_ENV
                    );
                    None
                } else {
                    Some(dapps)
                }
            })
            .unwrap_or_else(default_metric_dapps)
    })
}

fn default_metric_dapps() -> StdBTreeSet<DAppIdentifier> {
    DEFAULT_EXT_MESSAGES_QUEUE_SIZE_METRIC_DAPPS.iter().copied().collect()
}

fn parse_metric_dapps(value: &str) -> StdBTreeSet<DAppIdentifier> {
    value
        .split(',')
        .filter_map(|raw| {
            let token = raw.trim();
            if token.is_empty() {
                return None;
            }
            if let Ok(last_byte) = token.parse::<u8>() {
                return Some(dapp_identifier_with_last_byte(last_byte));
            }
            match DAppIdentifier::from_str(token) {
                Ok(dapp_id) => Some(dapp_id),
                Err(err) => {
                    tracing::warn!(
                        target: "ext_messages",
                        "Ignoring invalid DApp id in {}: {token}: {err}",
                        EXT_MESSAGES_QUEUE_SIZE_METRIC_DAPPS_ENV
                    );
                    None
                }
            }
        })
        .collect()
}

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
            reported_dapp_metrics: Arc::new(Mutex::new(StdBTreeSet::new())),
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
    reported_dapp_metrics: Arc<Mutex<StdBTreeSet<DAppIdentifier>>>,
}

impl ExternalMessagesThreadState {
    pub fn builder() -> ExternalMessagesThreadStateBuilder {
        ExternalMessagesThreadStateConfig::builder()
    }

    pub fn queue_handle(&self) -> Arc<Mutex<ExtMessages>> {
        self.queue.clone()
    }

    fn report_queue_state(&self, queue_len: usize, dapp_queue_sizes: &[(DAppIdentifier, usize)]) {
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

    fn report_queue_metrics(
        &self,
        queue_len: usize,
        low_priority_queue_len: usize,
        total_limit: usize,
        dapp_queue_sizes: &[(DAppIdentifier, usize)],
    ) {
        if let Some(metrics) = &self.report_metrics {
            metrics.report_ext_msg_queue_size(queue_len, &self.thread_id);
            let low_priority_percentage = if queue_len == 0 {
                0.0
            } else {
                low_priority_queue_len as f64 * 100.0 / queue_len as f64
            };
            metrics.report_ext_msg_queue_low_priority_percentage(
                low_priority_percentage,
                &self.thread_id,
            );
            let low_priority_total_limit_percentage = if total_limit == 0 {
                0.0
            } else {
                low_priority_queue_len as f64 * 100.0 / total_limit as f64
            };
            metrics.report_ext_msg_queue_low_priority_total_limit_percentage(
                low_priority_total_limit_percentage,
                &self.thread_id,
            );
            let metric_dapps = ext_messages_queue_size_metric_dapps();

            let current_dapps: StdBTreeSet<_> = dapp_queue_sizes
                .iter()
                .map(|(dapp_id, _)| *dapp_id)
                .filter(|dapp_id| metric_dapps.contains(dapp_id))
                .collect();
            let mut reported_dapps = self.reported_dapp_metrics.lock();
            for (dapp_id, len) in dapp_queue_sizes {
                if !metric_dapps.contains(dapp_id) {
                    continue;
                }
                metrics.report_ext_msg_queue_size_by_dapp(
                    *len,
                    &self.thread_id,
                    dapp_id.to_hex_string(),
                );
            }
            for dapp_id in reported_dapps.difference(&current_dapps) {
                metrics.report_ext_msg_queue_size_by_dapp(
                    0,
                    &self.thread_id,
                    dapp_id.to_hex_string(),
                );
            }
            *reported_dapps = current_dapps;
        }
    }

    pub fn push_external_messages(&self, ext_messages: &[QueuedExtMessage]) -> anyhow::Result<()> {
        tracing::trace!("add_external_messages: {}", ext_messages.len());

        if let Some(metrics) = &self.report_metrics {
            let low_priority_len = ext_messages.iter().filter(|msg| msg.is_low_priority()).count();
            metrics.report_ext_msg_received(ext_messages.len() as u64, &self.thread_id);
            metrics.report_ext_msg_low_priority_received(low_priority_len as u64, &self.thread_id);
        }

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

        let (report_len, low_priority_queue_len, total_limit, dapp_queue_sizes, unused) =
            self.queue.guarded_mut(|q| {
                let unused = q.push_external_messages(ext_messages, now);
                (q.len(), q.low_priority_len(), q.limits().total, q.dapp_queue_sizes(), unused)
            });

        self.report_queue_state(report_len, &dapp_queue_sizes);

        let low_priority_filtered = unused.iter().filter(|msg| msg.is_low_priority()).count();
        if low_priority_filtered > 0 {
            self.report_metrics.as_ref().inspect(|metrics| {
                metrics.report_ext_msg_low_priority_filtered(
                    low_priority_filtered as u64,
                    &self.thread_id,
                )
            });
        }

        if !unused.is_empty() {
            let overflow_feedbacks: Vec<_> = unused
                .into_iter()
                .map(|msg| create_queue_overflow_feedback(msg, &self.thread_id))
                .collect::<Result<_, _>>()?;

            let _ = self.feedback_sender.send(ExtMsgFeedbackList(overflow_feedbacks));
        }

        self.report_queue_metrics(
            report_len,
            low_priority_queue_len,
            total_limit,
            &dapp_queue_sizes,
        );

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

        let total_limit = self.queue.guarded(|q| q.limits().total);
        self.report_queue_metrics(0, 0, total_limit, &[]);

        Ok(())
    }

    pub fn erase_processed(&self, processed: &[Stamp]) -> Vec<(Stamp, QueuedExtMessage)> {
        tracing::trace!("erase_processed ext messages: {}", processed.len());

        let (report_len, low_priority_queue_len, total_limit, dapp_queue_sizes, removed) =
            self.queue.guarded_mut(|q| {
                let removed = q.erase_processed(processed);
                (q.len(), q.low_priority_len(), q.limits().total, q.dapp_queue_sizes(), removed)
            });

        tracing::trace!(target: "ext_messages", "on erase: queue_size={}", report_len);
        self.report_queue_state(report_len, &dapp_queue_sizes);

        self.report_queue_metrics(
            report_len,
            low_priority_queue_len,
            total_limit,
            &dapp_queue_sizes,
        );

        removed
    }

    pub fn restore_processed(&self, processed: &[(Stamp, QueuedExtMessage)]) {
        if processed.is_empty() {
            return;
        }

        let (report_len, low_priority_queue_len, total_limit, dapp_queue_sizes) =
            self.queue.guarded_mut(|q| {
                q.restore_processed(processed);
                (q.len(), q.low_priority_len(), q.limits().total, q.dapp_queue_sizes())
            });

        tracing::trace!(
            target: "ext_messages",
            "restored {} ext messages after production restart, queue_size={}",
            processed.len(),
            report_len
        );
        self.report_queue_state(report_len, &dapp_queue_sizes);

        self.report_queue_metrics(
            report_len,
            low_priority_queue_len,
            total_limit,
            &dapp_queue_sizes,
        );
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

#[cfg(test)]
mod tests {
    use node_types::DAppIdentifier;

    use super::dapp_identifier_with_last_byte;
    use super::parse_metric_dapps;

    #[test]
    fn parse_metric_dapps_accepts_decimal_last_byte_shortcuts() {
        let dapps = parse_metric_dapps("0, 1, 4");

        assert!(dapps.contains(&DAppIdentifier::ZERO));
        assert!(dapps.contains(&dapp_identifier_with_last_byte(1)));
        assert!(dapps.contains(&dapp_identifier_with_last_byte(4)));
        assert_eq!(dapps.len(), 3);
    }

    #[test]
    fn parse_metric_dapps_accepts_full_hex_ids_and_ignores_invalid_tokens() {
        let dapp_hex = format!("{}01", "00".repeat(31));
        let dapps = parse_metric_dapps(&format!("invalid,{dapp_hex}"));

        assert_eq!(dapps, [dapp_identifier_with_last_byte(1)].into());
    }
}
