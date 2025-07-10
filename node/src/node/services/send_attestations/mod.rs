use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use derive_getters::Getters;
use derive_setters::Setters;
use network::channel::NetDirectSender;
use parking_lot::Mutex;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use telemetry_utils::now_ms;
use typed_builder::TypedBuilder;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::bls::BLSSignatureScheme;
use crate::helper::block_flow_trace;
use crate::helper::metrics::BlockProductionMetrics;
use crate::node::services::PULSE_IDLE_TIMEOUT;
use crate::node::unprocessed_blocks_collection::UnfinalizedBlocksSnapshot;
use crate::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use crate::node::AttestationData;
use crate::node::Authority;
use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::node::Envelope;
use crate::node::GoshBLS;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::protocol::authority_switch::action_lock::ActionLockResult;
use crate::protocol::authority_switch::action_lock::BlockRef;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::CollectedAttestations;
use crate::types::RndSeed;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::utilities::guarded::TryGuardedMut;
use crate::utilities::thread_spawn_critical::SpawnCritical;

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
enum AttestationAction {
    ThisBlock(Envelope<GoshBLS, AttestationData>),
    // Fork {
    //     candidate_block: Envelope<GoshBLS, AckiNackiBlock>,
    //     attestation: Envelope<GoshBLS, AttestationData>,
    //     resend_candidate: bool,
    // },
}

#[derive(Getters, Setters, TypedBuilder, Clone)]
#[setters(strip_option, prefix = "set_", borrow_self)]
struct TrackedState {
    block_state: BlockState,
    #[builder(default)]
    interested_parties_received_blocks: Option<HashSet<NodeIdentifier>>,
    #[builder(default)]
    block_applied_timestamp: Option<std::time::Instant>,
    #[builder(default)]
    first_send_timestamp: Option<std::time::Instant>,
    #[builder(default)]
    last_send_timestamp: Option<std::time::Instant>,
    #[builder(default)]
    last_send_destinations: Option<HashSet<NodeIdentifier>>,
}

#[derive(TypedBuilder)]
pub struct AttestationSendService {
    pub pulse_timeout: std::time::Duration,

    resend_attestation_timeout: std::time::Duration,

    node_id: NodeIdentifier,

    thread_id: ThreadIdentifier,

    bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,

    block_state_repository: BlockStateRepository,

    #[builder(default)]
    tracking: HashMap<BlockIdentifier, TrackedState>,

    #[builder(default)]
    block_state_repository_last_modified: u32,

    network_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,

    #[builder(default=std::time::Instant::now())]
    start_time: std::time::Instant,

    // This random IS NOT a part of any security feature.
    // It's only purpose to randomly attach a candidate block to an attestation of a fork
    #[builder(default=SmallRng::from_entropy())]
    _rng: SmallRng,

    metrics: Option<BlockProductionMetrics>,
    authority: Arc<Mutex<Authority>>,
}

impl AttestationSendService {
    #[allow(clippy::mutable_key_type)]
    pub fn evaluate(
        &mut self,
        candidates: &UnfinalizedBlocksSnapshot,
        loopback_attestations: Arc<Mutex<CollectedAttestations>>,
        candidate_block_repository: &impl Repository<CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>>,
        deadline: std::time::Instant,
    ) -> std::time::Instant {
        let last_modified =
            self.block_state_repository.notifications().load(std::sync::atomic::Ordering::Relaxed);
        if last_modified == self.block_state_repository_last_modified
            && deadline > std::time::Instant::now()
        {
            // If repository was not modified and next pulse deadline is not reached
            // do nothing and return next deadline that equal to twice pulse idle time
            return deadline;
        }
        self.block_state_repository_last_modified = last_modified;
        self.append_for_tracking(candidates);
        self.stop_tracking_finalized_and_invalidated_candidates();
        self.timestamp_applied();
        self.update_interested_parties_received_blocks(candidates);
        self.pulse(loopback_attestations, candidate_block_repository)
    }

    fn timestamp_applied(&mut self) {
        let mut to_set_timestamp = vec![];
        for (block_id, ref state) in self.tracking.iter_mut() {
            if state.block_applied_timestamp().is_none()
                && state.block_state().guarded(|e| e.is_block_already_applied())
            {
                to_set_timestamp.push(block_id.clone());
            }
        }
        for block_id in to_set_timestamp.into_iter() {
            if let Some(e) = self.tracking.get_mut(&block_id) {
                e.set_block_applied_timestamp(std::time::Instant::now());
            }
        }
    }

    fn try_get_attestation(
        &self,
        state: &TrackedState,
    ) -> anyhow::Result<Envelope<GoshBLS, AttestationData>> {
        if let Some(attn) = state.block_state.guarded(|e| e.attestation().clone()) {
            return Ok(attn);
        };
        match self.generate_attestation(&state.block_state) {
            Ok(attestation) => Ok(state.block_state.guarded_mut(|e| {
                if e.attestation().is_none() {
                    e.set_attestation(attestation.clone()).expect("failed to set attestation");
                }
                e.attestation().clone().unwrap()
            })),
            Err(error) => {
                tracing::trace!(
                    "Failed to generate attestation for {}: {:?}",
                    state.block_state.block_identifier(),
                    error
                );
                Err(error)
            }
        }
    }

    fn pulse(
        &mut self,
        loopback_attestations: Arc<Mutex<CollectedAttestations>>,
        _candidate_block_repository: &impl Repository<
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
        >,
    ) -> std::time::Instant {
        let mut to_send: Vec<(HashSet<NodeIdentifier>, AttestationAction)> = vec![];
        // TODO: fix. it's a dirty hack for the borrow on tracking
        let first_sent = self
            .tracking
            .iter()
            .filter_map(|(k, v)| v.first_send_timestamp().as_ref().map(|time| (k.clone(), *time)))
            .collect::<HashMap<BlockIdentifier, std::time::Instant>>();
        let trace_node_id = self.node_id.clone();
        let mut next_deadline = std::time::Instant::now() + PULSE_IDLE_TIMEOUT * 2;
        let mut tracking = self.tracking.clone();
        for (_block_id, state) in tracking.iter_mut() {
            tracing::trace!("AttestationSendService: process: {_block_id:?}");
            let trace_skip = |reason: &str| {
                tracing::trace!("skip send attestation: {reason}");
                block_flow_trace(
                    format!("skip send attestation: {reason}"),
                    _block_id,
                    &trace_node_id,
                    [],
                );
            };
            // let Some(attestation) = state.attestation() else {
            //     trace_skip("does not have attestation to send");
            //     continue;
            // };
            let Ok(attestation) = self.try_get_attestation(state) else {
                trace_skip("does not have attestation to send");
                continue;
            };

            let Some(block_applied_timestamp) = state.block_applied_timestamp() else {
                trace_skip("missing block applied timestamp");
                continue;
            };
            let (Some(bk_set), Some(parent_block_identifier), Some(producer), Some(thread_id)) =
                state.block_state().guarded(|e| {
                    (
                        e.bk_set().clone(),
                        e.parent_block_identifier().clone(),
                        e.producer().clone(),
                        *e.thread_identifier(),
                    )
                })
            else {
                trace_skip("missing bk_set or parent block id or producer");
                continue;
            };
            let Ok(parent_block_state) = self.block_state_repository.get(&parent_block_identifier)
            else {
                trace_skip("missing parent block state");
                continue;
            };
            let Some(parent_block_producer_selector) =
                parent_block_state.guarded(|e| e.producer_selector_data().clone())
            else {
                trace_skip("missing parent block producer selector");
                continue;
            };

            // Note: random node was chosen as BP for a default block, so no need to take this distance into account
            let distance_to_producer = if parent_block_identifier == BlockIdentifier::default()
                || thread_id.is_spawning_block(&parent_block_identifier)
            {
                0
            } else {
                let Some(distance_to_producer) =
                    parent_block_producer_selector.get_distance_from_bp(&bk_set, &producer)
                else {
                    trace_skip("missing distance to bp");
                    continue;
                };
                distance_to_producer
            };

            let earliest_to_send_attestation = {
                let first_pulse_multiplier = 0.9_f32 + distance_to_producer as f32;
                let delay = self.pulse_timeout.mul_f32(first_pulse_multiplier);
                let parent_sent_first_attestation: Option<std::time::Instant> =
                    first_sent.get(parent_block_state.block_identifier()).copied();

                if parent_sent_first_attestation.is_none() {
                    self.metrics
                        .as_ref()
                        .inspect(|m| m.report_parent_first_attestation_none(&self.thread_id));
                }

                let timestamp = {
                    let regular_case = if let Some(prev_attn_time) = parent_sent_first_attestation {
                        prev_attn_time
                        // Note: this is intentionally changed
                        // There will be a separate discussion to retaliate block jamming attack.
                        //.max(*block_applied_timestamp)
                    } else if parent_block_state
                        .guarded(|e| e.is_prefinalized() || e.is_finalized())
                    {
                        let correction = if let Some(initial_attestations_target) =
                            parent_block_state.guarded(|e| *e.initial_attestations_target())
                        {
                            self.pulse_timeout
                                * (initial_attestations_target.descendant_generations as u32)
                        } else {
                            self.pulse_timeout
                        };
                        *block_applied_timestamp - correction
                    } else {
                        trace_skip(
                            "has not sent attestations for parent and it is not prefinalized",
                        );
                        continue;
                    };

                    regular_case
                };
                timestamp + delay
            };
            let now = std::time::Instant::now();
            if earliest_to_send_attestation > now {
                tracing::trace!(
                    "AttestationSendService: pulse: earliest_to_send_attestation - now = {}",
                    earliest_to_send_attestation.duration_since(now).as_millis()
                );
                let delay = self.pulse_timeout.mul_f32(0.9_f32 + distance_to_producer as f32);
                let time_info = |time: Option<std::time::Instant>| match time {
                    Some(time) => format!("{}", (time + delay).duration_since(now).as_millis()),
                    None => "None".to_string(),
                };
                let block_applied = time_info(Some(*block_applied_timestamp));
                let parent_sent_first_attestation =
                    time_info(first_sent.get(parent_block_state.block_identifier()).copied());
                let distance_to_producer = distance_to_producer.to_string();
                block_flow_trace(
                    "skip send attestation: earliest_to_send_attestation > now",
                    _block_id,
                    &trace_node_id,
                    [
                        ("delay", &format!("{}", delay.as_millis())),
                        ("block_applied", &block_applied),
                        ("distance_to_producer", &distance_to_producer),
                        ("parent_sent_first_attestation", &parent_sent_first_attestation),
                    ],
                );
                next_deadline = next_deadline.min(earliest_to_send_attestation);
                continue;
            }
            tracing::trace!("AttestationSendService: time to send attn: {_block_id:?}");
            let last_sent_time = state.last_send_timestamp().unwrap_or_else(|| self.start_time);
            let last_destinations = state.last_send_destinations().clone().unwrap_or_default();
            let attestation_interested_parties: HashSet<NodeIdentifier> =
                state.block_state().guarded(|e| e.known_attestation_interested_parties().clone());
            let received = state.interested_parties_received_blocks().clone().unwrap_or_default();
            let awaiting_destinations = {
                if last_sent_time.elapsed() > self.resend_attestation_timeout {
                    attestation_interested_parties.clone()
                } else if attestation_interested_parties != last_destinations {
                    attestation_interested_parties
                        .difference(&received)
                        .cloned()
                        .collect::<HashSet<NodeIdentifier>>()
                } else {
                    HashSet::new()
                }
            };
            if awaiting_destinations.is_empty() {
                trace_skip("no destinations were found");
                continue;
            }
            if state.block_state().guarded(|e| e.retracted_attestation().is_some()) {
                tracing::trace!(
                    "AttestationSendService: pulse: has retracted attestation: {:?}",
                    _block_id
                );
                trace_skip("has retracted attestation");
                continue;
            }
            let block_ref = BlockRef::try_from(state.block_state()).unwrap();
            // let block_height = state.block_state().guarded(|e| e.block_height().unwrap());
            // let block_round = state.block_state().guarded(|e| e.round().unwrap());
            // let parent_block_ref = BlockRef::try_from(&parent_block_state).unwrap();
            // let (
            //     parent_block_node_producer_selector_data,
            //     parent_prefinalization_proof,
            //     parent_block_height,
            // ) = parent_block_state.guarded(|e| {
            //     (
            //         e.producer_selector_data().clone().unwrap(),
            //         e.prefinalization_proof().clone().unwrap(),
            //         e.block_height().clone().unwrap(),
            //     )
            // });
            match self
                .authority
                .guarded_mut(|e| e.try_lock_send_attestation_action(block_ref.block_identifier()))
            {
                ActionLockResult::OkSendAttestation => {
                    // TODO:
                    // Due to the protocol changes remove try_add_attestation
                    // and set attestation directly.
                    state.block_state().guarded_mut(|e| {
                        if e.attestation().is_none() {
                            e.set_attestation(attestation.clone())
                                .expect("Failed to set attestation");
                        }
                    });
                }
                ActionLockResult::Rejected => {
                    trace_skip("some other block locked");
                    continue;
                }
            }

            // Find child that was attestated and attestation was not revoked
            let (attestated_sibling, attestation) = {
                let children = parent_block_state
                    .guarded(|e| e.known_children(&self.thread_id).cloned().unwrap_or_default());
                let mut attestated_child = None;
                let mut attestation = None;

                for child_id in children.into_iter() {
                    let Ok(child) = self.block_state_repository.get(&child_id) else {
                        continue;
                    };
                    let (is_actual, this_child_attestation) = child.guarded(|e| {
                        if e.attestation().is_some() && e.retracted_attestation().is_none() {
                            (true, e.attestation().clone())
                        } else {
                            (false, None)
                        }
                    });
                    if is_actual {
                        attestated_child = Some(child);
                        attestation = this_child_attestation;
                        break;
                    }
                }
                (attestated_child, attestation)
            };
            tracing::trace!(
                "AttestationSendService: pulse: attested_sibling: {:?}",
                attestated_sibling
            );
            if attestated_sibling.is_none() {
                trace_skip("missing attested sibling");
                continue;
            }
            let attestated_sibling = attestated_sibling.unwrap();
            let attestation = attestation.unwrap();
            if attestated_sibling.block_identifier() == state.block_state().block_identifier() {
                tracing::trace!(
                    "AttestationSendService: pulse: add attestation to send: {:?}",
                    attestation
                );
                to_send.push((awaiting_destinations, AttestationAction::ThisBlock(attestation)));
            } else {
                // let Ok(Some(candidate_block)) =
                //     candidate_block_repository.get_block(attestation.data().block_id())
                // else {
                tracing::trace!("Failed to get a block");
                trace_skip("failed to get a block");
                continue;
                // };
                // let resend_candidate = 0 == self.rng.next_u32() % (bk_set.len() as u32);
                // tracing::trace!(
                //     "AttestationSendService: pulse: add attestation to send as fork: {:?}",
                //     attestation
                // );
                // to_send.push((
                //     awaiting_destinations,
                //     AttestationAction::Fork {
                //         candidate_block: candidate_block.as_ref().clone(),
                //         attestation,
                //         resend_candidate,
                //     },
                // ));
            }
            if state.first_send_timestamp().is_none() {
                state.set_first_send_timestamp(std::time::Instant::now());
            }
            state.set_last_send_timestamp(std::time::Instant::now());
            state.set_last_send_destinations(attestation_interested_parties);
        }
        self.tracking = tracking;
        for (awaiting_destinations, attestation) in to_send.into_iter() {
            tracing::trace!(
                "AttestationSendService: pulse: send attestation: {:?} {:?}",
                attestation,
                awaiting_destinations
            );
            for destination in &awaiting_destinations {
                if destination != &self.node_id {
                    let _ = self.send_block_attestation(destination.clone(), attestation.clone());
                } else {
                    match attestation {
                        AttestationAction::ThisBlock(ref attestation) => {
                            tracing::trace!(
                                "AttestationSendService: pulse: add loopback: {:?}",
                                attestation
                            );
                            loopback_attestations
                                .guarded_mut(|e| {
                                    e.add(
                                        attestation.clone(),
                                        |block_id| {
                                            let Ok(block_state) =
                                                self.block_state_repository.get(block_id)
                                            else {
                                                return None;
                                            };
                                            block_state.guarded(|e| e.bk_set().clone())
                                        },
                                        |block_id| {
                                            let Ok(block_state) =
                                                self.block_state_repository.get(block_id)
                                            else {
                                                return None;
                                            };
                                            block_state.guarded(|e| e.envelope_hash().clone())
                                        },
                                    )
                                })
                                .expect("Failed to add attestation");
                        } /*     AttestationAction::Fork {
                           *         candidate_block: _,
                           *         ref attestation,
                           *         resend_candidate: _,
                           *     } => {
                           *         tracing::trace!(
                           *             "AttestationSendService: pulse: add loopback: {:?}",
                           *             attestation
                           *         );
                           *         loopback_attestations
                           *             .guarded_mut(|e| {
                           *                 e.add(attestation.clone(), |block_id| {
                           *                     let Ok(block_state) =
                           *                         self.block_state_repository.get(block_id)
                           *                     else {
                           *                         return None;
                           *                     };
                           *                     block_state.guarded(|e| e.bk_set().clone())
                           *                 })
                           *             })
                           *             .expect("Failed to add attestation");
                           *     } */
                    }
                }
            }
        }
        next_deadline
    }

    fn send_block_attestation(
        &self,
        destination_node_id: NodeIdentifier,
        attestation: AttestationAction,
    ) -> anyhow::Result<()> {
        match attestation {
            AttestationAction::ThisBlock(attestation) => {
                tracing::info!(
                    "sending attestation for thread {:?} to node {}: {:?}",
                    self.thread_id,
                    destination_node_id,
                    attestation,
                );
                let block_id = attestation.data().block_id().clone();
                block_flow_trace(
                    "send attestation",
                    attestation.data().block_id(),
                    &self.node_id,
                    [("to", &destination_node_id.to_string())],
                );
                self.network_direct_tx.send((
                    destination_node_id,
                    NetworkMessage::BlockAttestation((attestation, self.thread_id)),
                ))?;
                self.handle_attestation_metrics(&block_id);
            } /* AttestationAction::Fork { candidate_block, attestation, resend_candidate } => {
               *     tracing::trace!(
               *         "Sending an attestation to a fork. Destination: {}, attestation: {:?}, re-send candidate block: {}",
               *         destination_node_id,
               *         attestation,
               *         resend_candidate,
               *     );
               *     if resend_candidate {
               *         self.network_direct_tx.send((
               *             destination_node_id.clone(),
               *             NetworkMessage::candidate(&candidate_block)?,
               *         ))?;
               *     }
               *     self.network_direct_tx.send((
               *         destination_node_id,
               *         NetworkMessage::BlockAttestation((attestation, self.thread_id)),
               *     ))?;
               * } */
        }
        Ok(())
    }

    // fn prepare_attestations(&mut self) {
    //     let mut to_attestate = vec![];
    //     for (_, candidate) in self.tracking.iter() {
    //         if candidate.attestation().is_some() {
    //             continue;
    //         }
    //         if candidate.block_state().guarded(|e| !e.is_block_already_applied()) {
    //             continue;
    //         }
    //         to_attestate.push(candidate.block_state().clone());
    //     }
    //     let mut attestations = vec![];
    //     for block_state in to_attestate.into_iter() {
    //         match self.generate_attestation(&block_state) {
    //             Ok(attestation) => {
    //                 attestations.push(attestation);
    //             }
    //             Err(error) => {
    //                 tracing::error!(
    //                     "Failed to generate attestation for {}: {:?}",
    //                     block_state.block_identifier(),
    //                     error
    //                 );
    //             }
    //         }
    //     }
    //     for attestation in attestations.into_iter() {
    //         if let Some(candidate) = self.tracking.get_mut(attestation.data().block_id()) {
    //             let block_id = attestation.data().block_id().clone();
    //             tracing::trace!("AttestationSendService: set_attestation {:?}", block_id);
    //             candidate.set_attestation(attestation);
    //         }
    //     }
    // }

    fn generate_attestation(
        &self,
        block_state: &BlockState,
    ) -> anyhow::Result<Envelope<GoshBLS, AttestationData>> {
        let Some(bk_data) =
            block_state.guarded(|state_in| state_in.get_bk_data_for_node_id(&self.node_id))
        else {
            anyhow::bail!("Failed to generate attestation: no bk data for node id is available");
        };

        let Some(block_seq_no) = block_state.guarded(|state_in| *state_in.block_seq_no()) else {
            anyhow::bail!("Failed to generate attestation: block seq_no is not available");
        };

        let Some(parent_id) =
            block_state.guarded(|state_in| state_in.parent_block_identifier().clone())
        else {
            anyhow::bail!("Failed to generate attestation: parent id is not available");
        };

        let Some(secret) = self.bls_keys_map.guarded(|map| map.get(&bk_data.pubkey).cloned())
        else {
            anyhow::bail!("Failed to generate attestation: missing bls key secret");
        };
        let (signer_index, secret) = (bk_data.signer_index, secret.0);

        let Some(envelope_hash) = block_state.guarded(|e| e.envelope_hash().clone()) else {
            anyhow::bail!("Failed to access envelope_hash");
        };
        let attestation_data = AttestationData::builder()
            .block_id(block_state.block_identifier().clone())
            .block_seq_no(block_seq_no)
            .parent_block_id(parent_id)
            .envelope_hash(envelope_hash)
            .build();
        tracing::trace!("Generate attestation: {:?}", attestation_data);

        let signature = <GoshBLS as BLSSignatureScheme>::sign(&secret, &attestation_data)?;
        let signature_occurrences = HashMap::from([(signer_index, 1)]);
        Ok(Envelope::<GoshBLS, AttestationData>::create(
            signature,
            signature_occurrences,
            attestation_data,
        ))
    }

    fn stop_tracking_finalized_and_invalidated_candidates(&mut self) {
        // TODO: check if `removed_block_ids` is unused !
        let mut removed_block_ids = vec![];
        self.tracking.retain(|block_id, e| {
            e.block_state().guarded(|x| {
                if !x.is_finalized() && !x.is_invalidated() {
                    true
                } else {
                    removed_block_ids.push(block_id.clone());
                    false
                }
            })
        });
        if !removed_block_ids.is_empty() {
            tracing::trace!(
                "AttestationSendService: stop_tracking_finalized_and_invalidated_candidates {:?}",
                removed_block_ids
            );
        }
    }

    #[allow(clippy::mutable_key_type)]
    fn append_for_tracking(&mut self, candidates: &UnfinalizedBlocksSnapshot) {
        for (_, (candidate, _)) in candidates.iter() {
            if !self.tracking.contains_key(candidate.block_identifier()) {
                let state = TrackedState::builder().block_state(candidate.clone()).build();
                tracing::trace!(
                    "AttestationSendService: append_for_tracking {:?}",
                    candidate.block_identifier()
                );
                self.tracking.insert(candidate.block_identifier().clone(), state);
            }
        }
    }

    #[allow(clippy::mutable_key_type)]
    fn update_interested_parties_received_blocks(
        &mut self,
        candidates: &UnfinalizedBlocksSnapshot,
    ) {
        for (_, (candidate, _)) in candidates.iter() {
            let attested_block_to_producer = self.collect_attested_blocks_to_receiver(candidate);

            if !attested_block_to_producer.is_empty() {
                self.update_tracking(attested_block_to_producer);
            }
        }
    }

    fn collect_attested_blocks_to_receiver(
        &mut self,
        candidate: &BlockState,
    ) -> HashMap<BlockIdentifier, NodeIdentifier> {
        let mut blocks_to_parties = HashMap::new();

        let Some(producer) = candidate.guarded(|e| e.producer().clone()) else {
            return blocks_to_parties;
        };
        let verified_attestations = candidate.guarded(|e| e.verified_attestations().clone());

        for (attested_blk_id, signer_ids) in verified_attestations {
            // Find out in old blocks what my "signer id" was at that time, then collect "known_attestation_interested_parties"
            if let Some((_, state)) = self.tracking.get_key_value(&attested_blk_id) {
                state.block_state().guarded(|inner| {
                    if let Some(my_idx) = inner.get_signer_index_for_node_id(&self.node_id) {
                        if signer_ids.contains(&my_idx)
                            && inner.known_attestation_interested_parties().contains(&producer)
                        {
                            blocks_to_parties.insert(attested_blk_id.clone(), producer.clone());
                        }
                    }
                });
            }
        }
        blocks_to_parties
    }

    fn update_tracking(
        &mut self,
        attested_blocks_to_parties: HashMap<BlockIdentifier, NodeIdentifier>,
    ) {
        for (block_id, producer) in attested_blocks_to_parties {
            if let Some(tracked_state) = self.tracking.get_mut(&block_id) {
                tracing::trace!(
                    "AttestationSendService: update interested_parties_received_blocks {:?} {}",
                    block_id,
                    producer
                );
                tracked_state
                    .interested_parties_received_blocks
                    .get_or_insert_with(HashSet::new)
                    .insert(producer);
            }
        }
    }

    fn handle_attestation_metrics(&self, block_id: &BlockIdentifier) {
        let _ = self.handle_attestation_metrics_inner(block_id);
    }

    fn handle_attestation_metrics_inner(&self, block_id: &BlockIdentifier) -> anyhow::Result<()> {
        let mut parent_block_identifier = None;
        // Send metrics if only we can obtain a lock
        let lock_obtained = self
            .block_state_repository
            .get(block_id)?
            .try_guarded_mut(|e| {
                // remember the parent block identifier, we need it later
                parent_block_identifier = e.parent_block_identifier().clone();

                // Set attestation_sent_ms only if it's not set
                if e.event_timestamps.attestation_sent_ms.is_none() {
                    let current_millis = now_ms();
                    e.event_timestamps.attestation_sent_ms = Some(current_millis);
                    if let Some(received) = e.event_timestamps.received_ms {
                        tracing::trace!(
                            "AttestationSendService: block_id: {:?}, attestation_delay: {} ms",
                            block_id,
                            current_millis.saturating_sub(received)
                        );
                    }

                    if let Some(metrics) = self.metrics.as_ref() {
                        // Report the duration from the moment the block is received until the attestation is sent
                        if let Some(received_ms) = e.event_timestamps.received_ms {
                            if received_ms > current_millis {
                                tracing::warn!(
                                "Attestation metric for block {} warn: received_ms > now_ms = {}",
                                block_id,
                                received_ms - current_millis
                            );
                            }

                            metrics.report_block_received_attestation_sent(
                                current_millis.saturating_sub(received_ms),
                                &self.thread_id,
                            );

                            if let Some(value) =
                                e.event_timestamps.verify_all_block_signatures_ms_total
                            {
                                metrics.report_verify_all_block_signatures(
                                    value as u64,
                                    &self.thread_id,
                                );
                            }
                        } else {
                            tracing::error!(
                                "Attestation on block: {} has not been sent, received_ms is None",
                                block_id
                            );
                        }
                        if let Some(block_applied) = e.event_timestamps.block_applied_timestamp_ms {
                            metrics.report_attestation_after_apply_delay(
                                now_ms().saturating_sub(block_applied),
                                &self.thread_id,
                            )
                        }
                    }
                }
            })
            .is_some();

        if !lock_obtained {
            tracing::warn!(
                "Attestation on block: {} has not been sent, lock not obtained",
                block_id
            );
            return Ok(());
        }

        if let Some(parent_block_id) = parent_block_identifier {
            let lock_obtained =  self.block_state_repository.get(&parent_block_id)?.try_guarded_mut(|e| {
                if let Some(parent_attestation_sent_ms) =
                    e.event_timestamps.attestation_sent_ms
                {
                    let current_millis = now_ms();
                    if parent_attestation_sent_ms > current_millis {
                        tracing::warn!(
                            "Attestation on parent block: parent_block_id={} parent_attestation_sent_ms > now_ms = {}",
                            parent_block_id,
                            parent_attestation_sent_ms - current_millis
                        );
                    }

                    if let Some(metrics) = self.metrics.as_ref() {
                        metrics.report_child_parent_attestation(
                            current_millis.saturating_sub(parent_attestation_sent_ms),
                            &self.thread_id,
                        );
                    }
                } else {
                    tracing::warn!(
                         "Attestation on parent block: {} has not been sent, its attestation_sent_ms is None",
                        parent_block_id
                    );
                }
            }).is_some();

            if !lock_obtained {
                tracing::warn!(
                    "Attestation on parent block: {} has not been sent, lock not obtained",
                    parent_block_id
                );
            }
        } else {
            tracing::warn!(
                "Attestation on parent block for child: {} has not been sent, parent not found",
                block_id
            );
        }
        Ok(())
    }
}

pub struct AttestationSendServiceHandler {
    _service_handler: std::thread::JoinHandle<()>,
}

impl AttestationSendServiceHandler {
    pub fn new(
        mut attestation_sender_service: AttestationSendService,
        repository: RepositoryImpl,
        last_block_attestations: Arc<Mutex<CollectedAttestations>>,
        block_state_repository: BlockStateRepository,
        unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection,
    ) -> Self {
        let service_handler = std::thread::Builder::new()
            .name("Attestation send service".to_string())
            .spawn_critical(move || {
                let mut attestation_deadline = Instant::now() + PULSE_IDLE_TIMEOUT * 2;
                loop {
                    let notifications =
                        block_state_repository.notifications().load(Ordering::Relaxed);
                    #[allow(clippy::mutable_key_type)]
                    let blocks_to_process: UnfinalizedBlocksSnapshot =
                        unprocessed_blocks_cache.clone_queue();
                    attestation_deadline = attestation_sender_service.evaluate(
                        &blocks_to_process,
                        last_block_attestations.clone(),
                        &repository,
                        attestation_deadline,
                    );

                    let new_notifications =
                        block_state_repository.notifications().load(Ordering::Relaxed);
                    if new_notifications == notifications {
                        if let Some(idle_time) =
                            attestation_deadline.checked_duration_since(Instant::now())
                        {
                            let delay = idle_time.min(PULSE_IDLE_TIMEOUT);
                            tracing::trace!("AttestationSendService: wait: {}", delay.as_millis());
                            std::thread::sleep(delay);
                        }
                    }
                }
            })
            .expect("Failed to create thread for attestation send service");
        Self { _service_handler: service_handler }
    }
}
