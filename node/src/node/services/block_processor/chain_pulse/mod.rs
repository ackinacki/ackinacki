use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use network::channel::NetBroadcastSender;
use network::channel::NetDirectSender;
use typed_builder::TypedBuilder;

use crate::node::associated_types::NodeIdentifier;
use crate::node::unprocessed_blocks_collection::UnfinalizedBlocksSnapshot;
use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::node::NetworkMessage;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::bp_selector::BlockGap;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;

pub mod events;
mod pulse_attestations;
mod pulse_candidates;

#[derive(TypedBuilder)]
#[builder(
    build_method(vis="pub", into=ChainPulse),
    builder_method(vis=""),
    builder_type(vis="pub", name=ChainPulseBuilder),
)]
struct ChainPulseSettings {
    node_id: NodeIdentifier,

    thread_identifier: ThreadIdentifier,

    direct_send_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,

    broadcast_send_tx: NetBroadcastSender<NetworkMessage>,

    #[builder(setter(strip_option), default=None)]
    trigger_attestation_resend_by_time: Option<Duration>,

    #[builder(setter(strip_option), default=None)]
    trigger_attestation_resend_by_chain_length: Option<usize>,

    retry_request_missing_block_timeout: Duration,

    resend_timeout_blocks_stuck: Duration,
    resend_timeout_blocks_stuck_extra_offset_per_candidate: Duration,

    missing_blocks_were_requested: Arc<AtomicBool>,

    #[builder(setter(strip_option), default=None)]
    trigger_increase_block_gap: Option<Duration>,

    finalization_stopped_time_trigger: Duration,
    finalization_has_not_started_time_trigger: Duration,

    block_gap: BlockGap,

    last_finalized_block: Option<BlockState>,
    // chain_pulse_monitor: std::sync::mpsc::Sender<events::ChainPulseEvent>,
}

pub struct ChainPulse {
    // thread_identifier: ThreadIdentifier,
    attestations: pulse_attestations::PulseAttestations,
    candidates: pulse_candidates::PulseCandidateBlocks,
    // chain_pulse_monitor: std::sync::mpsc::Sender<events::ChainPulseEvent>,
    last_finalized_block: Option<BlockState>,
}

impl From<ChainPulseSettings> for ChainPulse {
    fn from(settings: ChainPulseSettings) -> Self {
        Self {
            // thread_identifier: settings.thread_identifier,
            attestations: pulse_attestations::PulseAttestations::builder()
                .node_id(settings.node_id.clone())
                .thread_identifier(settings.thread_identifier)
                .send_tx(settings.direct_send_tx.clone())
                .request_retry_timeout(settings.retry_request_missing_block_timeout)
                .missing_blocks_were_requested(settings.missing_blocks_were_requested.clone())
                .trigger_by_time(settings.trigger_attestation_resend_by_time)
                .trigger_by_length(settings.trigger_attestation_resend_by_chain_length)
                .trigger_increase_block_gap(settings.trigger_increase_block_gap)
                .block_gap(settings.block_gap.clone())
                .build(),
            candidates: pulse_candidates::PulseCandidateBlocks::builder()
                .node_id(settings.node_id.clone())
                .thread_identifier(settings.thread_identifier)
                .broadcast_tx(settings.broadcast_send_tx)
                .resend_timeout(settings.resend_timeout_blocks_stuck)
                .resend_extra_timeout_per_candidate(
                    settings.resend_timeout_blocks_stuck_extra_offset_per_candidate,
                )
                .trigger_by_finalization_stopped_timer(settings.finalization_stopped_time_trigger)
                .trigger_by_no_finalized_since_start_timer(
                    settings.finalization_has_not_started_time_trigger,
                )
                .direct_send_tx(settings.direct_send_tx.clone())
                .build(),
            // chain_pulse_monitor: settings.chain_pulse_monitor,
            last_finalized_block: settings.last_finalized_block,
        }
    }
}

impl ChainPulse {
    pub fn builder() -> ChainPulseBuilder {
        ChainPulseSettings::builder()
    }

    pub fn pulse(&mut self, last_finalized_block: &BlockState) -> anyhow::Result<()> {
        let next_seq_no = last_finalized_block
            .guarded(|e| *e.block_seq_no())
            .ok_or(anyhow::format_err!("Can fail on restart"))?;
        if let Some(ref prev) = self.last_finalized_block {
            if prev == last_finalized_block {
                return Ok(());
            }
            let prev_seq_no = prev.guarded(|e| e.block_seq_no().unwrap());
            anyhow::ensure!(next_seq_no > prev_seq_no);
        }
        // match self.chain_pulse_monitor.send(events::ChainPulseEvent::block_finalized(
        //     self.thread_identifier,
        //     Some(block_height),
        // )) {
        //     Ok(()) => {}
        //     Err(e) => {
        //         if SHUTDOWN_FLAG.get() != Some(&true) {
        //             anyhow::bail!("Failed to send block finalized: {e}");
        //         }
        //     }
        // }
        let r1 = self.attestations.pulse(last_finalized_block);
        let r2 = self.candidates.pulse(next_seq_no);
        // Don't care if one fails. Make sure both had a chance to execute.
        // In case of an error return one.
        r1?;
        r2?;
        self.last_finalized_block = Some(last_finalized_block.clone());
        Ok(())
    }

    #[allow(clippy::mutable_key_type)]
    pub fn evaluate(
        &mut self,
        candidates: &UnfinalizedBlocksSnapshot,
        block_state_repository: &BlockStateRepository,
        block_repository: &RepositoryImpl,
    ) -> anyhow::Result<()> {
        let metrics = block_repository.get_metrics();
        let r1 = self.attestations.evaluate(candidates, block_state_repository, metrics);
        let r2 = self.candidates.evaluate(candidates, block_repository);
        r1?;
        r2?;
        Ok(())
    }
}
