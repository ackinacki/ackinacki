use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use derive_getters::Getters;
use derive_setters::Setters;
use parking_lot::Mutex;
use rand::rngs::SmallRng;
use rand::RngCore;
use rand::SeedableRng;
use typed_builder::TypedBuilder;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::bls::BLSSignatureScheme;
use crate::node::block_state::try_add_attestation::TryAddAttestation;
use crate::node::AttestationData;
use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::node::Envelope;
use crate::node::GoshBLS;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::RndSeed;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
enum AttestationAction {
    ThisBlock(Envelope<GoshBLS, AttestationData>),
    Fork {
        candidate_block: Envelope<GoshBLS, AckiNackiBlock>,
        attestation: Envelope<GoshBLS, AttestationData>,
        resend_candidate: bool,
    },
}

#[derive(Getters, Setters, TypedBuilder)]
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
    #[builder(default)]
    attestation: Option<Envelope<GoshBLS, AttestationData>>,

    #[builder(default)]
    received_fork_resolution_for_any_of_previous_blocks_timestamp: Option<std::time::Instant>,
}

#[derive(TypedBuilder)]
pub struct AttestationSendService {
    pub pulse_timeout: std::time::Duration,

    node_id: NodeIdentifier,

    thread_id: ThreadIdentifier,

    bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,

    block_state_repository: BlockStateRepository,

    #[builder(default)]
    tracking: HashMap<BlockIdentifier, TrackedState>,

    #[builder(default)]
    block_state_repository_last_modified: u32,

    send_tx: Sender<(NodeIdentifier, NetworkMessage)>,

    #[builder(default=std::time::Instant::now())]
    start_time: std::time::Instant,

    // This random IS NOT a part of any security feature.
    // It's only purpose to randomly attach a candidate block to an attestation of a fork
    #[builder(default=SmallRng::from_entropy())]
    rng: SmallRng,
}

impl AttestationSendService {
    pub fn evaluate(
        &mut self,
        candidates: &[BlockState],
        loopback_attestations: &mut Vec<Envelope<GoshBLS, AttestationData>>,
        candidate_block_repository: &impl Repository<CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>>,
    ) {
        let last_modified =
            self.block_state_repository.notifications().load(std::sync::atomic::Ordering::Relaxed);
        if last_modified == self.block_state_repository_last_modified {
            return;
        }
        self.block_state_repository_last_modified = last_modified;
        self.append_for_tracking(candidates);
        self.stop_tracking_finalized_and_invalidated_candidates();
        self.timestamp_applied();
        self.prepare_attestations();
        self.update_interested_parties_received_blocks(candidates);
        self.timestamp_forks();
        self.pulse(loopback_attestations, candidate_block_repository);
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

    fn timestamp_forks(&mut self) {
        let mut parents_to_stamp = vec![];
        for (_block_id, ref state) in self.tracking.iter_mut() {
            if state.block_state().guarded(|e| e.resolves_forks().is_some()) {
                parents_to_stamp.push(state.block_state().clone());
            }
        }
        for state in parents_to_stamp.into_iter() {
            self.tracking
                .entry(state.block_identifier().clone())
                .and_modify(|e| {
                    if e.received_fork_resolution_for_any_of_previous_blocks_timestamp.is_none() {
                        e.received_fork_resolution_for_any_of_previous_blocks_timestamp =
                            Some(std::time::Instant::now());
                    }
                })
                .or_insert_with(|| {
                    TrackedState::builder()
                        .block_state(state)
                        .received_fork_resolution_for_any_of_previous_blocks_timestamp(Some(
                            std::time::Instant::now(),
                        ))
                        .build()
                });
        }
    }

    fn pulse(
        &mut self,
        loopback_attestations: &mut Vec<Envelope<GoshBLS, AttestationData>>,
        candidate_block_repository: &impl Repository<CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>>,
    ) {
        let mut to_send: Vec<(HashSet<NodeIdentifier>, AttestationAction)> = vec![];
        // TODO: fix. it's a dirty hack for the borrow on tracking
        let first_sent = self
            .tracking
            .iter()
            .filter_map(|(k, v)| v.first_send_timestamp().as_ref().map(|time| (k.clone(), *time)))
            .collect::<HashMap<BlockIdentifier, std::time::Instant>>();
        let fork_timestamps = self
            .tracking
            .iter()
            .filter_map(|(k, v)| {
                v.received_fork_resolution_for_any_of_previous_blocks_timestamp()
                    .as_ref()
                    .map(|t| (k.clone(), *t))
            })
            .collect::<HashMap<BlockIdentifier, std::time::Instant>>();
        for (_block_id, state) in self.tracking.iter_mut() {
            let Some(attestation) = state.attestation() else {
                continue;
            };
            let Some(block_applied_timestamp) = state.block_applied_timestamp() else {
                continue;
            };
            let (Some(bk_set), Some(parent_block_identifier), Some(producer)) =
                state.block_state().guarded(|e| {
                    (e.bk_set().clone(), e.parent_block_identifier().clone(), e.producer().clone())
                })
            else {
                continue;
            };
            let Ok(parent_block_state) = self.block_state_repository.get(&parent_block_identifier)
            else {
                continue;
            };
            let Some(parent_block_producer_selector) =
                parent_block_state.guarded(|e| e.producer_selector_data().clone())
            else {
                continue;
            };

            let Some(distance_to_producer) =
                parent_block_producer_selector.get_distance_from_bp(&bk_set, &producer)
            else {
                continue;
            };

            let earliest_to_send_attestation = {
                let first_pulse_multiplier = 0.9_f32 + distance_to_producer as f32;
                let delay = self.pulse_timeout.mul_f32(first_pulse_multiplier);
                let parent_sent_first_attestation: Option<std::time::Instant> =
                    first_sent.get(parent_block_state.block_identifier()).copied();
                let timestamp = {
                    let regular_case = if let Some(prev_attn_time) = parent_sent_first_attestation {
                        prev_attn_time.max(*block_applied_timestamp)
                    } else if parent_block_state.guarded(|e| e.is_finalized()) {
                        *block_applied_timestamp
                    } else {
                        continue;
                    };

                    if let Some(fork_resolution_time) =
                        fork_timestamps.get(parent_block_state.block_identifier())
                    {
                        regular_case.max(*fork_resolution_time)
                    } else {
                        regular_case
                    }
                };
                timestamp + delay
            };
            if earliest_to_send_attestation > std::time::Instant::now() {
                continue;
            }
            let last_sent_time = state.last_send_timestamp().unwrap_or_else(|| self.start_time);
            let last_destinations = state.last_send_destinations().clone().unwrap_or_default();
            let attestation_interested_parties: HashSet<NodeIdentifier> =
                state.block_state().guarded(|e| e.known_attestation_interested_parties().clone());
            let received = state.interested_parties_received_blocks().clone().unwrap_or_default();
            let awaiting_destinations = {
                if last_sent_time.elapsed() > self.pulse_timeout {
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
                continue;
            }
            if state.block_state().guarded(|e| e.retracted_attestation().is_some()) {
                continue;
            }
            if state.block_state().guarded(|e| e.attestation().is_none()) {
                match self.block_state_repository.try_add_attestation(attestation.clone()) {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::trace!("Try add attestation failed: {}", e);
                        continue;
                    }
                }
            }
            let _ = attestation;
            // Find child that was attestated and attestation was not revoked
            let (attestated_sibling, attestation) = {
                let children = parent_block_state.guarded(|e| e.known_children().clone());
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
            if attestated_sibling.is_none() {
                continue;
            }
            let attestated_sibling = attestated_sibling.unwrap();
            let attestation = attestation.unwrap();
            if attestated_sibling.block_identifier() == state.block_state().block_identifier() {
                to_send.push((awaiting_destinations, AttestationAction::ThisBlock(attestation)));
            } else {
                let Ok(Some(candidate_block)) =
                    candidate_block_repository.get_block(&attestation.data().block_id)
                else {
                    tracing::trace!("Failed to get a block");
                    continue;
                };
                let resend_candidate = 0 == self.rng.next_u32() % (bk_set.len() as u32);
                to_send.push((
                    awaiting_destinations,
                    AttestationAction::Fork { candidate_block, attestation, resend_candidate },
                ));
            }
            if state.first_send_timestamp().is_none() {
                state.set_first_send_timestamp(std::time::Instant::now());
            }
            state.set_last_send_timestamp(std::time::Instant::now());
            state.set_last_send_destinations(attestation_interested_parties);
        }
        for (awaiting_destinations, attestation) in to_send.into_iter() {
            for destination in &awaiting_destinations {
                if destination != &self.node_id {
                    let _ = self.send_block_attestation(destination.clone(), attestation.clone());
                } else if let AttestationAction::ThisBlock(ref attestation) = attestation {
                    loopback_attestations.push(attestation.clone());
                }
            }
        }
    }

    fn send_block_attestation(
        &self,
        destination_node_id: NodeIdentifier,
        attestation: AttestationAction,
    ) -> anyhow::Result<()> {
        match attestation {
            AttestationAction::ThisBlock(attestation) => {
                tracing::info!(
                    "sending attestation to node {}: {:?}",
                    destination_node_id,
                    attestation,
                );
                self.send_tx.send((
                    destination_node_id,
                    NetworkMessage::BlockAttestation((attestation, self.thread_id)),
                ))?;
            }
            AttestationAction::Fork { candidate_block, attestation, resend_candidate } => {
                tracing::trace!(
                    "Sending an attestation to a fork. Destination: {}, attestation: {:?}, re-send candidate block: {}",
                    destination_node_id,
                    attestation,
                    resend_candidate,
                );
                if resend_candidate {
                    self.send_tx.send((
                        destination_node_id.clone(),
                        NetworkMessage::Candidate(candidate_block),
                    ))?;
                }
                self.send_tx.send((
                    destination_node_id,
                    NetworkMessage::BlockAttestation((attestation, self.thread_id)),
                ))?;
            }
        }
        Ok(())
    }

    fn prepare_attestations(&mut self) {
        let mut to_attestate = vec![];
        for (_, candidate) in self.tracking.iter() {
            if candidate.attestation().is_some() {
                continue;
            }
            if candidate.block_state().guarded(|e| !e.is_block_already_applied()) {
                continue;
            }
            to_attestate.push(candidate.block_state().clone());
        }
        let mut attestations = vec![];
        for block_state in to_attestate.into_iter() {
            match self.generate_attestation(&block_state) {
                Ok(attestation) => {
                    attestations.push(attestation);
                }
                Err(error) => {
                    tracing::error!(
                        "Failed to generate attestation for {}: {:?}",
                        block_state.block_identifier(),
                        error
                    );
                }
            }
        }
        for attestation in attestations.into_iter() {
            if let Some(candidate) = self.tracking.get_mut(&attestation.data().block_id) {
                candidate.set_attestation(attestation);
            }
        }
    }

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

        let Some(secret) = self.bls_keys_map.guarded(|map| map.get(&bk_data.pubkey).cloned())
        else {
            anyhow::bail!("Failed to generate attestation: missing bls key secret");
        };
        let (signer_index, secret) = (bk_data.signer_index, secret.0);

        let attestation_data =
            AttestationData { block_id: block_state.block_identifier().clone(), block_seq_no };
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
    }

    fn append_for_tracking(&mut self, candidates: &[BlockState]) {
        for candidate in candidates.iter() {
            if !self.tracking.contains_key(candidate.block_identifier()) {
                let state = TrackedState::builder().block_state(candidate.clone()).build();
                self.tracking.insert(candidate.block_identifier().clone(), state);
            }
        }
    }

    fn update_interested_parties_received_blocks(&mut self, candidates: &[BlockState]) {
        for candidate in candidates.iter() {
            let attested_block_to_producer = self.collect_attested_blocks_to_recevier(candidate);

            if !attested_block_to_producer.is_empty() {
                self.update_tracking(attested_block_to_producer);
            }
        }
    }

    fn collect_attested_blocks_to_recevier(
        &mut self,
        candidate: &BlockState,
    ) -> HashMap<BlockIdentifier, NodeIdentifier> {
        let mut blocks_to_parties = HashMap::new();

        let Some(producer) = candidate.inner().lock().producer().clone() else {
            return blocks_to_parties;
        };

        for (attested_blk_id, signer_ids) in candidate.inner().lock().verified_attestations() {
            // Find out in old blocks what my "signer id" was at that time, then collect "known_attestation_interested_parties"
            if let Some((_, state)) = self.tracking.get_key_value(attested_blk_id) {
                let inner = state.block_state().inner().lock();

                if let Some(my_idx) = inner.get_signer_index_for_node_id(&self.node_id) {
                    if signer_ids.contains(&my_idx)
                        && inner.known_attestation_interested_parties().contains(&producer)
                    {
                        blocks_to_parties.insert(attested_blk_id.clone(), producer.clone());
                    }
                }
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
                tracked_state
                    .interested_parties_received_blocks
                    .get_or_insert_with(HashSet::new)
                    .insert(producer);
            }
        }
    }
}
