use std::collections::HashSet;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use typed_builder::TypedBuilder;

use crate::node::associated_types::NodeIdentifier;
use crate::node::services::block_processor::chain_tracker::find_shortest_gap;
use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::node::NetworkMessage;
use crate::types::next_seq_no;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;

#[derive(TypedBuilder)]
pub struct PulseAttestations {
    node_id: NodeIdentifier,
    thread_identifier: ThreadIdentifier,

    send_tx: Sender<(NodeIdentifier, NetworkMessage)>,

    #[builder(setter(skip), default=None)]
    last_finalized_block: Option<BlockSeqNo>,

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

    block_gap: Arc<parking_lot::Mutex<usize>>,
}

impl PulseAttestations {
    pub fn pulse(&mut self, last_finalized_block: BlockSeqNo) -> anyhow::Result<()> {
        anyhow::ensure!(Some(last_finalized_block) != self.last_finalized_block);
        self.last_finalized_block = Some(last_finalized_block);
        self.last_pulse = Instant::now();
        // drop timers and ranges
        self.last_requested_time = None;
        self.last_requested_range = None;
        self.missing_blocks_were_requested.store(false, atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn evaluate(
        &mut self,
        candidates: &[BlockState],
        blocks_states: &BlockStateRepository,
    ) -> anyhow::Result<()> {
        let is_triggering = {
            let is_time_triggered = if let Some(time) = self.trigger_by_time {
                self.last_pulse.elapsed() > time
            } else {
                false
            };
            let is_queue_triggered = if let Some(length) = self.trigger_by_length {
                candidates.len() > length
            } else {
                false
            };
            is_time_triggered || is_queue_triggered
        };
        if is_triggering {
            self.try_request_missing(candidates, blocks_states)?;
        }
        if let Some(time) = self.trigger_increase_block_gap {
            if self.last_gap_increase.elapsed() > time {
                self.last_gap_increase = Instant::now();
                let mut gap = self.block_gap.lock();
                *gap += 1;
                tracing::trace!("Increase block gap for {:?}: {}", self.thread_identifier, *gap);
            }
        }
        Ok(())
    }

    fn try_request_missing(
        &mut self,
        candidates: &[BlockState],
        blocks_states: &BlockStateRepository,
    ) -> anyhow::Result<()> {
        if let Some(last_requested_time) = self.last_requested_time {
            if last_requested_time.elapsed() < self.request_retry_timeout {
                return Ok(());
            }
        }
        let mut attestation_interested_parties = {
            candidates.iter().fold(HashSet::new(), |mut all, e| {
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
            let request_range_end = find_shortest_gap(
                &self.thread_identifier,
                candidates,
                blocks_states,
                request_range_start,
            );
            if let Some(request_range_end) = request_range_end {
                return self.make_request(
                    attestation_interested_parties,
                    request_range_start,
                    request_range_end,
                );
            }
        }
        // Note: this next_seq_no use is correct for multithreaded chains environment
        let request_range_start = next_seq_no(self.last_finalized_block.unwrap_or_default());
        let Some(request_range_end) = find_shortest_gap(
            &self.thread_identifier,
            candidates,
            blocks_states,
            request_range_start,
        ) else {
            return Ok(());
        };
        self.missing_blocks_were_requested.store(true, atomic::Ordering::Relaxed);
        self.make_request(attestation_interested_parties, request_range_start, request_range_end)
    }

    fn make_request(
        &mut self,
        attestation_interested_parties: HashSet<NodeIdentifier>,
        request_range_start: BlockSeqNo,
        request_range_end: BlockSeqNo,
    ) -> anyhow::Result<()> {
        for remote in attestation_interested_parties.into_iter() {
            crate::node::send::send_blocks_range_request(
                &self.send_tx,
                remote,
                self.node_id.clone(),
                self.thread_identifier,
                request_range_start,
                request_range_end,
            )?;
        }
        self.last_requested_time = Some(Instant::now());
        self.last_requested_range = Some((request_range_start, request_range_end));
        Ok(())
    }
}
