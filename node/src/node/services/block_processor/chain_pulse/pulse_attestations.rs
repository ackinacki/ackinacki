use std::collections::HashSet;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use network::channel::NetDirectSender;
use typed_builder::TypedBuilder;

use crate::helper::metrics::BlockProductionMetrics;
use crate::node::services::block_processor::chain_tracker::_find_shortest_gap;
use crate::node::unprocessed_blocks_collection::UnfinalizedBlocksSnapshot;
use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::types::bp_selector::BlockGap;
use crate::types::next_seq_no;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;

#[allow(dead_code)]
#[derive(TypedBuilder)]
pub struct PulseAttestations {
    node_id: NodeIdentifier,
    thread_identifier: ThreadIdentifier,

    send_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,

    #[builder(setter(skip), default=None)]
    last_finalized_block_seq_no: Option<BlockSeqNo>,

    #[builder(setter(skip), default=None)]
    last_requested_range: Option<(BlockSeqNo, BlockSeqNo)>,

    #[builder(setter(skip), default=None)]
    last_requested_time: Option<Instant>,

    #[builder(setter(skip), default_code = "Instant::now()")]
    last_pulse: Instant,

    trigger_by_time: Option<Duration>,

    trigger_by_length: Option<usize>,

    request_retry_timeout: Duration,

    missing_blocks_were_requested: Arc<AtomicBool>,

    trigger_increase_block_gap: Option<Duration>,

    #[builder(setter(skip), default_code = "Instant::now()")]
    last_gap_increase: Instant,

    block_gap: BlockGap,

    #[builder(setter(skip), default=None)]
    min_generations_to_request: Option<usize>,
}

impl PulseAttestations {
    pub fn pulse(&mut self, last_finalized_block: &BlockState) -> anyhow::Result<()> {
        let next_seq_no = last_finalized_block.guarded(|e| e.block_seq_no().unwrap());
        if let Some(ref prev) = self.last_finalized_block_seq_no {
            if &next_seq_no == prev {
                return Ok(());
            }
            anyhow::ensure!(next_seq_no > prev);
        }
        self.last_finalized_block_seq_no = Some(next_seq_no);
        self.min_generations_to_request = last_finalized_block
            .guarded(|e| e.attestation_target().map(|x| *x.primary().generation_deadline() + 1));
        self.last_pulse = Instant::now();
        // drop timers and ranges
        self.last_requested_time = None;
        self.last_requested_range = None;
        tracing::trace!("clear blocks_were_requested");
        self.missing_blocks_were_requested.store(false, atomic::Ordering::Relaxed);
        Ok(())
    }

    #[allow(clippy::mutable_key_type)]
    pub fn evaluate(
        &mut self,
        candidates: &UnfinalizedBlocksSnapshot,
        _block_state_repository: &BlockStateRepository,
        metrics: Option<&BlockProductionMetrics>,
    ) -> anyhow::Result<()> {
        let is_triggering = {
            let is_time_triggered = if let Some(time) = self.trigger_by_time {
                self.last_pulse.elapsed() > time
            } else {
                false
            };
            let is_queue_triggered = if let Some(length) = self.trigger_by_length {
                candidates.blocks().len() > length
            } else {
                false
            };
            is_time_triggered || is_queue_triggered
        };
        if is_triggering {
            // self.try_request_missing(candidates, block_state_repository, metrics.clone())?;
        }
        let gap = if let Some(time) = self.trigger_increase_block_gap {
            if self.last_gap_increase.elapsed() > time {
                self.last_gap_increase = Instant::now();
                let gap = self.block_gap.fetch_add(1, atomic::Ordering::Relaxed);
                tracing::trace!("Increase block gap for {:?}: {}", self.thread_identifier, gap);
                gap
            } else {
                self.block_gap.load(atomic::Ordering::Relaxed)
            }
        } else {
            self.block_gap.load(atomic::Ordering::Relaxed)
        };

        metrics.inspect(|m| {
            m.report_finalization_gap(gap, &self.thread_identifier);
        });

        Ok(())
    }

    #[allow(clippy::mutable_key_type)]
    fn _try_request_missing(
        &mut self,
        candidates: &UnfinalizedBlocksSnapshot,
        block_state_repository: &BlockStateRepository,
        metrics: Option<BlockProductionMetrics>,
    ) -> anyhow::Result<()> {
        if let Some(last_requested_time) = self.last_requested_time {
            if last_requested_time.elapsed() < self.request_retry_timeout {
                return Ok(());
            }
        }
        let mut attestation_interested_parties = {
            candidates.blocks().iter().fold(HashSet::new(), |mut all, (_, (e, _))| {
                all.extend(e.guarded(|s| s.known_attestation_interested_parties().clone()));
                all
            })
        };
        attestation_interested_parties.remove(&self.node_id);
        if attestation_interested_parties.is_empty() {
            return Ok(());
        }

        if let Some(requested) = self.last_requested_range {
            let request_range_start = requested.1;
            let request_range_end = _find_shortest_gap(
                &self.thread_identifier,
                candidates,
                block_state_repository,
                request_range_start,
                metrics.clone(),
            );
            if let Some(request_range_end) = request_range_end {
                return self._make_request(
                    attestation_interested_parties,
                    request_range_start,
                    request_range_end,
                    self.min_generations_to_request,
                    metrics.clone(),
                );
            }
        }
        // Note: this next_seq_no use is correct for multithreaded chains environment
        let request_range_start = next_seq_no(self.last_finalized_block_seq_no.unwrap_or_default());
        let Some(request_range_end) = _find_shortest_gap(
            &self.thread_identifier,
            candidates,
            block_state_repository,
            request_range_start,
            metrics.clone(),
        ) else {
            return Ok(());
        };
        tracing::trace!("set blocks_were_requested");
        self.missing_blocks_were_requested.store(true, atomic::Ordering::Relaxed);
        self._make_request(
            attestation_interested_parties,
            request_range_start,
            request_range_end,
            self.min_generations_to_request,
            metrics.clone(),
        )
    }

    fn _make_request(
        &mut self,
        attestation_interested_parties: HashSet<NodeIdentifier>,
        request_range_start: BlockSeqNo,
        request_range_end: BlockSeqNo,
        request_at_least_this_number_of_blocks: Option<usize>,
        metrics: Option<BlockProductionMetrics>,
    ) -> anyhow::Result<()> {
        if request_range_end > request_range_start {
            if let Some(metrics) = &metrics {
                let gap_len = request_range_end - request_range_start;
                metrics.report_blocks_requested(gap_len as u64, &self.thread_identifier);
            }
        }
        for remote in attestation_interested_parties.into_iter() {
            crate::node::send::send_blocks_range_request(
                &self.send_tx,
                remote,
                self.node_id.clone(),
                self.thread_identifier,
                request_range_start,
                request_range_end,
                request_at_least_this_number_of_blocks,
            )?;
        }
        self.last_requested_time = Some(Instant::now());
        self.last_requested_range = Some((request_range_start, request_range_end));
        Ok(())
    }
}
