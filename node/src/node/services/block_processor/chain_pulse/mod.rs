use std::sync::atomic::AtomicBool;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use typed_builder::TypedBuilder;

use crate::node::associated_types::NodeIdentifier;
use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::node::NetworkMessage;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

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

    direct_send_tx: Sender<(NodeIdentifier, NetworkMessage)>,

    broadcast_send_tx: Sender<NetworkMessage>,

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

    block_gap: Arc<parking_lot::Mutex<usize>>,
}

pub struct ChainPulse {
    attestations: pulse_attestations::PulseAttestations,
    candidates: pulse_candidates::PulseCandidateBlocks,
}

impl From<ChainPulseSettings> for ChainPulse {
    fn from(settings: ChainPulseSettings) -> Self {
        Self {
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
                .build(),
        }
    }
}

impl ChainPulse {
    pub fn builder() -> ChainPulseBuilder {
        ChainPulseSettings::builder()
    }

    pub fn pulse(&mut self, last_finalized_block: BlockSeqNo) -> anyhow::Result<()> {
        let r1 = self.attestations.pulse(last_finalized_block);
        let r2 = self.candidates.pulse(last_finalized_block);
        // Don't care if one fails. Make sure both had a chance to execute.
        // In case of an error return one.
        r1?;
        r2?;
        Ok(())
    }

    pub fn evaluate(
        &mut self,
        candidates: &[BlockState],
        blocks_states: &BlockStateRepository,
        block_repository: &RepositoryImpl,
    ) -> anyhow::Result<()> {
        let r1 = self.attestations.evaluate(candidates, blocks_states);
        let r2 = self.candidates.evaluate(candidates, block_repository);
        r1?;
        r2?;
        Ok(())
    }
}
