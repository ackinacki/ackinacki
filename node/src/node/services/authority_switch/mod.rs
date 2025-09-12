use std::net::SocketAddr;
use std::ops::Sub;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use network::channel::NetBroadcastSender;
use network::channel::NetDirectSender;
use parking_lot::Mutex;
use telemetry_utils::instrumented_channel_ext::WrappedItem;
use telemetry_utils::instrumented_channel_ext::XInstrumentedReceiver;
use telemetry_utils::instrumented_channel_ext::XInstrumentedSender;
use typed_builder::TypedBuilder;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::helper::SHUTDOWN_FLAG;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::services::block_processor::chain_pulse::events::ChainPulseEvent;
use crate::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use crate::node::AuthoritySwitch;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::protocol::authority_switch::action_lock::OnNextRoundIncomingRequestResult;
use crate::protocol::authority_switch::action_lock::ThreadAuthority;
use crate::protocol::authority_switch::network_message::NextRoundSuccess;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[derive(TypedBuilder)]
pub struct AuthoritySwitchService {
    self_addr: SocketAddr,
    rx: XInstrumentedReceiver<(NetworkMessage, SocketAddr)>,
    self_node_tx: XInstrumentedSender<(NetworkMessage, SocketAddr)>,
    network_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
    thread_id: ThreadIdentifier,
    unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection,
    thread_authority: Arc<Mutex<ThreadAuthority>>,
    network_broadcast_tx: NetBroadcastSender<NetworkMessage>,
    block_state_repository: BlockStateRepository,
    chain_pulse_monitor: Sender<ChainPulseEvent>,
    sync_timeout_duration: Duration,
}

impl AuthoritySwitchService {
    pub fn run(&mut self) -> anyhow::Result<()> {
        let mut last_state_sync_executed: std::time::Instant =
            std::time::Instant::now().sub(self.sync_timeout_duration);
        loop {
            match self.rx.recv() {
                Ok((msg, sender)) => {
                    match msg {
                        NetworkMessage::AuthoritySwitchProtocol(auth_switch) => match auth_switch {
                            AuthoritySwitch::Request(next_round) => {
                                tracing::trace!("Received NetworkMessage::AuthoritySwitchProtocol(AuthoritySwitch::Request: {next_round:?})");

                                if let Some(attestation) =
                                    next_round.locked_block_attestation().clone()
                                {
                                    let _ = self.self_node_tx.send(WrappedItem {
                                        payload: (
                                            NetworkMessage::BlockAttestation((
                                                attestation,
                                                self.thread_id,
                                            )),
                                            self.self_addr,
                                        ),
                                        label: self.thread_id.to_string(),
                                    });
                                }
                                for attestation in next_round.attestations_for_ancestors().iter() {
                                    let _ = self.self_node_tx.send(WrappedItem {
                                        payload: (
                                            NetworkMessage::BlockAttestation((
                                                attestation.clone(),
                                                self.thread_id,
                                            )),
                                            self.self_addr,
                                        ),
                                        label: self.thread_id.to_string(),
                                    });
                                }
                                let unprocessed_cache = self.unprocessed_blocks_cache.clone();
                                let action = self.thread_authority.guarded_mut(|e| {
                                    e.on_next_round_incoming_request(
                                        next_round,
                                        Some(sender),
                                        unprocessed_cache,
                                    )
                                });
                                match action {
                                    OnNextRoundIncomingRequestResult::StartingBlockProducer => {}
                                    OnNextRoundIncomingRequestResult::DoNothing => {}
                                    OnNextRoundIncomingRequestResult::Broadcast(e) => {
                                        tracing::trace!(
                                            "Broadcast AuthoritySwitch::Switched({e:?})"
                                        );
                                        match self.network_broadcast_tx.send(
                                            NetworkMessage::AuthoritySwitchProtocol(
                                                AuthoritySwitch::Switched(e.clone()),
                                            ),
                                        ) {
                                            Ok(_) => {}
                                            _ => {
                                                if SHUTDOWN_FLAG.get() != Some(&true) {
                                                    panic!(
                                                        "Failed to broadcast AuthoritySwitch::Switched"
                                                    );
                                                }
                                            }
                                        }
                                        self.on_authority_switch_success(e)?;
                                    }
                                    OnNextRoundIncomingRequestResult::Reject((e, source)) => {
                                        match self.network_direct_tx.send((
                                            source.into(),
                                            NetworkMessage::AuthoritySwitchProtocol(
                                                AuthoritySwitch::Reject(e),
                                            ),
                                        )) {
                                            Ok(()) => {}
                                            _ => {
                                                if SHUTDOWN_FLAG.get() != Some(&true) {
                                                    panic!(
                                                        "Failed to send AuthoritySwitch::Reject"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    OnNextRoundIncomingRequestResult::RejectTooOldRequest(
                                        source,
                                    ) => {
                                        match self.network_direct_tx.send((
                                            source,
                                            NetworkMessage::AuthoritySwitchProtocol(
                                                AuthoritySwitch::RejectTooOld(self.thread_id),
                                            ),
                                        )) {
                                            Ok(()) => {}
                                            _ => {
                                                if SHUTDOWN_FLAG.get() != Some(&true) {
                                                    panic!(
                                                        "Failed to send AuthoritySwitch::RejectTooOld"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            AuthoritySwitch::Switched(next_round_success) => {
                                //
                                let Ok(proposed_block) =
                                    next_round_success.data().proposed_block().get_envelope()
                                else {
                                    tracing::trace!("Failed to parse a proposed block");
                                    continue;
                                };
                                let parent_block_identifier = proposed_block.data().parent();
                                let parent_block_state = self
                                    .block_state_repository
                                    .get(&parent_block_identifier)
                                    .unwrap();
                                let Some(bk_set) =
                                    parent_block_state.guarded(|e| e.descendant_bk_set().clone())
                                else {
                                    tracing::trace!("Failed to get descendant bk_set");
                                    continue;
                                };
                                match next_round_success
                                    .verify_signatures(bk_set.get_pubkeys_by_signers())
                                {
                                    Ok(false) => {
                                        tracing::trace!("Invalid signature");
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::trace!("Signature verification failed: {e:?}");
                                        continue;
                                    }
                                    Ok(true) => {
                                        // Consistency check
                                        let is_correct = {
                                            let signature_occurrences =
                                                next_round_success.clone_signature_occurrences();

                                            let Some(expected_signer_index) = bk_set
                                                .get_by_node_id(
                                                    next_round_success.data().node_identifier(),
                                                )
                                                .map(|e| e.signer_index)
                                            else {
                                                tracing::trace!(
                                                "Incorrect or malicious message: incorrect node id"
                                            );
                                                continue;
                                            };
                                            signature_occurrences.len() == 1
                                                && signature_occurrences.keys().cloned().last()
                                                    == Some(expected_signer_index)
                                        };
                                        if !is_correct {
                                            tracing::trace!("Incorrect or malicious message");
                                            continue;
                                        }
                                    }
                                }
                                self.on_authority_switch_success(next_round_success)?;
                            }
                            AuthoritySwitch::Reject(rejection_reason) => {
                                let net_block = rejection_reason.prefinalized_block();
                                tracing::trace!(
                                    "Received: AuthoritySwitch::Reject: {} {} {:?}",
                                    net_block.seq_no,
                                    net_block.identifier,
                                    rejection_reason.proof_of_prefinalization(),
                                );
                                //
                                let Ok(prefinalized_block) = net_block.get_envelope() else {
                                    tracing::trace!("Failed to parse a prefinalized block");
                                    continue;
                                };
                                if self
                                    .block_state_repository
                                    .get(&prefinalized_block.data().identifier())
                                    .unwrap()
                                    .guarded(|e| e.is_prefinalized())
                                {
                                    tracing::trace!("Reject contains block that was already prefinalized. Skip it.");
                                    continue;
                                }
                                let parent_block_identifier = prefinalized_block.data().parent();
                                let parent_block_state = self
                                    .block_state_repository
                                    .get(&parent_block_identifier)
                                    .unwrap();
                                let Some(bk_set) =
                                    parent_block_state.guarded(|e| e.descendant_bk_set().clone())
                                else {
                                    tracing::trace!("Failed to get descendant bk_set");
                                    continue;
                                };
                                match prefinalized_block
                                    .verify_signatures(bk_set.get_pubkeys_by_signers())
                                {
                                    Ok(false) => {
                                        tracing::trace!("Invalid signature");
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::trace!("Signature verification failed: {e:?}");
                                        continue;
                                    }
                                    Ok(true) => {}
                                }
                                let _ = self.self_node_tx.send(WrappedItem {
                                    payload: (
                                        NetworkMessage::Candidate(net_block.clone()),
                                        self.self_addr,
                                    ),
                                    label: self.thread_id.to_string(),
                                });
                                // self.on_incoming_candidate_block(net_block, None)?;
                                let attestations =
                                    rejection_reason.proof_of_prefinalization().clone();
                                if attestations.data().block_id()
                                    != &prefinalized_block.data().identifier()
                                {
                                    tracing::warn!(
                                        "Malicious message: attestation is sent for a wrong block"
                                    );
                                    continue;
                                }
                                let round = prefinalized_block.data().get_common_section().round;
                                self.block_state_repository
                                    .get(&prefinalized_block.data().identifier())
                                    .unwrap()
                                    .guarded_mut(|e| e.set_block_round(round))?;
                                let block_state = self
                                    .block_state_repository
                                    .get(attestations.data().block_id())?;
                                // TODO: expedite setting the attestation target.
                                let Some(attestation_target) =
                                    block_state.guarded(|e| *e.attestation_target())
                                else {
                                    tracing::trace!("Attestation target is not set for switched block {attestations:?}");
                                    continue;
                                };
                                // TODO:
                                // check block validity
                                // call common code to prefinalize block

                                let target =
                                    *attestation_target.fallback().required_attestation_count();

                                if attestations.clone_signature_occurrences().len() >= target {
                                    block_state.guarded_mut(|e| -> anyhow::Result<()> {
                                        e.set_prefinalized(attestations.clone())?;
                                        Ok(())
                                    })?;
                                    let _ = self.chain_pulse_monitor.send(
                                        ChainPulseEvent::block_prefinalized(
                                            *rejection_reason.thread_identifier(),
                                            Some(
                                                prefinalized_block
                                                    .data()
                                                    .get_common_section()
                                                    .block_height,
                                            ),
                                        ),
                                    );
                                }
                                let _ = self.self_node_tx.send(WrappedItem {
                                    payload: (
                                        NetworkMessage::BlockAttestation((
                                            attestations,
                                            self.thread_id,
                                        )),
                                        self.self_addr,
                                    ),
                                    label: self.thread_id.to_string(),
                                });

                                let blocks = self.unprocessed_blocks_cache.clone();
                                tracing::trace!(
                                    "Successfully preprocessed: AuthoritySwitch::Reject. Calling on_block_producer_stalled: {} {} {:?}",
                                    net_block.seq_no,
                                    net_block.identifier,
                                    rejection_reason.proof_of_prefinalization(),
                                );
                                self.thread_authority.guarded_mut(|e| {
                                    let result = e.on_block_producer_stalled();
                                    tracing::trace!("on_block_producer_stalled result: {result:?}");
                                    if let Some(next_round) = result.next_round().clone() {
                                        e.on_next_round_incoming_request(
                                            next_round,
                                            Some(sender),
                                            blocks,
                                        );
                                    }
                                });
                            }
                            AuthoritySwitch::RejectTooOld(_) => {
                                if last_state_sync_executed.elapsed() > self.sync_timeout_duration {
                                    last_state_sync_executed = std::time::Instant::now();
                                    let _ = self.self_node_tx.send(WrappedItem {
                                        payload: (
                                            NetworkMessage::StartSynchronization,
                                            self.self_addr,
                                        ),
                                        label: self.thread_id.to_string(),
                                    });
                                }
                            }
                            AuthoritySwitch::Failed(e) => {
                                let net_block = e.data().proposed_block();
                                // self.on_incoming_candidate_block(net_block, None)?;
                                let _ = self.self_node_tx.send(WrappedItem {
                                    payload: (
                                        NetworkMessage::Candidate(net_block.clone()),
                                        self.self_addr,
                                    ),
                                    label: self.thread_id.to_string(),
                                });
                                if let Some(attestations) =
                                    e.data().attestations_aggregated().clone()
                                {
                                    let _ = self.self_node_tx.send(WrappedItem {
                                        payload: (
                                            NetworkMessage::BlockAttestation((
                                                attestations,
                                                self.thread_id,
                                            )),
                                            self.self_addr,
                                        ),
                                        label: self.thread_id.to_string(),
                                    });
                                }
                            }
                        },
                        msg => {
                            panic!("AuthoritySwitchService has received unexpected msg: {msg:?}");
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(%error, "AuthoritySwitchService was disconnected");
                    return Ok(());
                }
            }
        }
    }

    fn on_authority_switch_success(
        &mut self,
        next_round_success: Envelope<GoshBLS, NextRoundSuccess>,
    ) -> anyhow::Result<()> {
        tracing::trace!("Received NetworkMessage::AuthoritySwitchProtocol(AuthoritySwitch::Switched({next_round_success:?}))");
        // TODO: check signatures.
        let next_round_success = next_round_success.data();
        let proposed_block = next_round_success.proposed_block().clone();
        // let resend_node_id = Some(next_round_success.node_identifier().clone());
        // self.on_incoming_candidate_block(&proposed_block, resend_node_id)?;
        let _ = self.self_node_tx.send(WrappedItem {
            payload: (NetworkMessage::Candidate(proposed_block.clone()), self.self_addr),
            label: self.thread_id.to_string(),
        });
        let attestation = next_round_success.attestations_aggregated().clone();
        if let Some(attestation) = attestation.clone() {
            let block_state = self.block_state_repository.get(attestation.data().block_id())?;
            if let Some(attestation_target) = block_state.guarded(|e| *e.attestation_target()) {
                if attestation.clone_signature_occurrences().len()
                    >= *attestation_target.fallback().required_attestation_count()
                {
                    // TODO: check attestations before going further
                    block_state.guarded_mut(|e| {
                        e.set_prefinalized(attestation.clone())?;
                        anyhow::Ok(())
                    })?;
                    let block = proposed_block.get_envelope()?;
                    let _ = self.chain_pulse_monitor.send(ChainPulseEvent::block_prefinalized(
                        block.data().get_common_section().thread_id,
                        Some(block.data().get_common_section().block_height),
                    ));
                }
            } else {
                tracing::trace!("Attestation target is not set for switched block {attestation:?}");
                block_state.guarded_mut(|e| e.add_detached_attestations(attestation));
                return Ok(());
            }
        }
        if let Some(attestation) = attestation {
            let _ = self.self_node_tx.send(WrappedItem {
                payload: (
                    NetworkMessage::BlockAttestation((attestation, self.thread_id)),
                    self.self_addr,
                ),
                label: self.thread_id.to_string(),
            });
        }

        self.thread_authority.guarded_mut(|e| e.on_next_round_success(next_round_success));

        let proposed_block_round =
            proposed_block.get_envelope().unwrap().data().get_common_section().round;
        tracing::trace!(
            "on_authority_switch_success: proposed_block.round={}, next_round_success.round={}",
            proposed_block_round,
            next_round_success.round()
        );
        if &proposed_block_round != next_round_success.round() {
            let blocks = self.unprocessed_blocks_cache.clone();
            self.thread_authority.guarded_mut(|e| {
                let result = e.on_block_producer_stalled();
                tracing::trace!("on_block_producer_stalled result: {result:?}");
                if let Some(next_round) = result.next_round().clone() {
                    e.on_next_round_incoming_request(next_round, None, blocks);
                }
            });
        }

        Ok(())
    }
}
