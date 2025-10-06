// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use tracing::instrument;
use tvm_types::UInt256;

use super::rules;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::must_save_state_on_seq_no;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::AttestationTargetType;
use crate::node::associated_types::NackReason;
use crate::node::block_state::attestation_target_checkpoints::inherit_ancestor_blocks_finalization_distances;
use crate::node::block_state::attestation_target_checkpoints::AncestorBlocksFinalizationCheckpointsConstructorResults;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::block_state::state::AttestationTarget;
use crate::node::block_state::state::AttestationTargets;
use crate::node::block_state::state::MAX_STATE_ANCESTORS;
use crate::node::block_state::tools::invalidate_branch;
use crate::node::services::block_processor::chain_pulse::events::ChainPulseEvent;
use crate::node::services::block_processor::chain_pulse::ChainPulse;
use crate::node::services::validation::feedback::AckiNackiSend;
use crate::node::shared_services::SharedServices;
use crate::node::unprocessed_blocks_collection::FilterPrehistoric;
use crate::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::node::PubKey;
use crate::node::Secret;
use crate::node::SignerIndex;
use crate::node::ValidationServiceInterface;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::CrossThreadRefDataRead;
use crate::repository::Repository;
use crate::repository::RepositoryError;
use crate::services::cross_thread_ref_data_availability_synchronization::CrossThreadRefDataAvailabilitySynchronizationServiceInterface;
use crate::types::bp_selector::BlockGap;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockIndex;
use crate::types::BlockSeqNo;
use crate::types::RndSeed;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::utilities::thread_spawn_critical::SpawnCritical;
use crate::utilities::FixedSizeHashSet;

pub const MAX_ATTESTATION_TARGET_BETA: usize = 30;
// const ALLOWED_BLOCK_PRODUCTION_TIME_LAG_MS: u64 = 50;

use network::channel::NetBroadcastSender;
use network::channel::NetDirectSender;
use telemetry_utils::mpsc::InstrumentedSender;
use telemetry_utils::now_ms;

use crate::helper::start_shutdown;
use crate::helper::SHUTDOWN_FLAG;
use crate::node::services::sync::ExternalFileSharesBased;
use crate::node::services::sync::StateSyncService;

pub struct BlockProcessorService {
    pub service_handler: std::thread::JoinHandle<()>,
    pub missing_blocks_were_requested: Arc<AtomicBool>,
}

#[derive(Clone, Copy)]
pub struct SecurityGuarantee(f64);

impl SecurityGuarantee {
    pub fn from_chance_of_successful_attack(p: f64) -> Self {
        assert!(p >= 0.0f64);
        assert!(p <= 1.0f64);
        Self(p)
    }

    pub fn chance_of_successful_attack(&self) -> f64 {
        self.0
    }
}

impl BlockProcessorService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        security_guarantee: SecurityGuarantee,
        node_id: NodeIdentifier,
        time_to_produce_block: Duration,
        save_state_frequency: u32,
        bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
        thread_identifier: ThreadIdentifier,
        block_state_repository: BlockStateRepository,
        repository: RepositoryImpl,
        mut shared_services: SharedServices,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        send_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
        broadcast_tx: NetBroadcastSender<NetworkMessage>,
        block_gap: BlockGap,
        validation_service: ValidationServiceInterface,
        share_service: ExternalFileSharesBased,
        send: AckiNackiSend,
        chain_pulse_monitor: std::sync::mpsc::Sender<ChainPulseEvent>,
        mut unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection,
        mut cross_thread_ref_data_availability_synchronization_service: CrossThreadRefDataAvailabilitySynchronizationServiceInterface,
        save_optimistic_service_sender: InstrumentedSender<Arc<OptimisticStateImpl>>,
    ) -> Self {
        let chain_pulse_last_finalized_block_id: BlockIdentifier = repository
            .select_thread_last_finalized_block(&thread_identifier)
            .unwrap_or_default()
            .unwrap_or_default()
            .0;
        let chain_pulse_last_finalized_block =
            block_state_repository.get(&chain_pulse_last_finalized_block_id).unwrap();
        let missing_blocks_were_requested = Arc::new(AtomicBool::new(false));
        let missing_blocks_were_requested_clone = missing_blocks_were_requested.clone();
        let service_handler = std::thread::Builder::new()
            .name("Block processor service".to_string())
            .spawn_critical(move || {
                let repository = repository.clone();
                let mut chain_pulse = ChainPulse::builder()
                    .node_id(node_id.clone())
                    .thread_identifier(thread_identifier)
                    .direct_send_tx(send_direct_tx)
                    .broadcast_send_tx(broadcast_tx)

                    //TODO: move into configs
                    .trigger_attestation_resend_by_time(Duration::from_secs(5))
                    .trigger_attestation_resend_by_chain_length(15usize)
                    .retry_request_missing_block_timeout(Duration::from_secs(7))
                    .resend_timeout_blocks_stuck(Duration::from_secs(10))
                    .resend_timeout_blocks_stuck_extra_offset_per_candidate(Duration::from_millis(100))
                    .finalization_stopped_time_trigger(Duration::from_secs(20))
                    .finalization_has_not_started_time_trigger(Duration::from_secs(60))
                    .missing_blocks_were_requested(missing_blocks_were_requested_clone)
                    .trigger_increase_block_gap(time_to_produce_block)
                    .block_gap(block_gap)
                    .last_finalized_block(Some(
                        block_state_repository.get(
                            &chain_pulse_last_finalized_block_id
                        )?
                    ))
                    // .chain_pulse_monitor(chain_pulse_monitor)
                    .build();
                let _ = chain_pulse.pulse(&chain_pulse_last_finalized_block);
                loop {
                    tracing::trace!("Start processing iteration");
                    if SHUTDOWN_FLAG.get() == Some(&true) {
                        return Ok(());
                    }
                    unprocessed_blocks_cache.remove_finalized_and_invalidated_blocks();
                    let saved_notification: u32 = unprocessed_blocks_cache.notifications().stamp();
                    // Await for signal that smth has changes and blocks can be checked again
                    if let Some((last_finalized_block_id, last_finalized_seq_no)) = repository.select_thread_last_finalized_block(&thread_identifier)? {

                        let last_finalized_block = block_state_repository.get(&last_finalized_block_id)?;
                        chain_pulse.pulse(&last_finalized_block)?;

                        unprocessed_blocks_cache.remove_old_blocks(&last_finalized_seq_no);
                        #[allow(clippy::mutable_key_type)]
                        let (blocks_to_process, filter) =
                            unprocessed_blocks_cache.clone_queue();

                        shared_services.metrics.as_ref().inspect(|m| {
                            m.report_unfinalized_blocks_queue(blocks_to_process.blocks().len() as u64, &thread_identifier);
                        });

                        let _ = chain_pulse.evaluate(&blocks_to_process, &block_state_repository, &repository);
                        for (block_state, block) in blocks_to_process.blocks().values() {
                            {
                                let (block_process_timestamp_was_reported, thread_identifier, received_ms) = block_state.guarded_mut(|e| {
                                    anyhow::Ok((
                                        e.event_timestamps.block_process_timestamp_was_reported,
                                        *e.thread_identifier(),
                                        e.event_timestamps.received_ms,
                                    ))
                                })?;
                                if !block_process_timestamp_was_reported {
                                    block_state.guarded_mut(|e| e.event_timestamps.block_process_timestamp_was_reported = true);
                                    shared_services.metrics.as_ref().inspect(|m| {
                                        if let Some(thread_id) = thread_identifier {
                                            if let Some(received) = &received_ms {
                                                m.report_processing_delay(
                                                    now_ms().saturating_sub(*received),
                                                    &thread_id,
                                                );
                                            }
                                        }
                                    });
                                }
                            }
                            process_candidate_block(
                                security_guarantee,
                                node_id.clone(),
                                save_state_frequency,
                                bls_keys_map.clone(),
                                block_state,
                                &block_state_repository,
                                &repository,
                                &mut shared_services,
                                nack_set_cache.clone(),
                                block,
                                &validation_service,
                                &time_to_produce_block,
                                share_service.clone(),
                                send.clone(),
                                &chain_pulse_monitor,
                                &mut cross_thread_ref_data_availability_synchronization_service,
                                &save_optimistic_service_sender,
                                &filter
                            )?;
                        }
                    }
                    unprocessed_blocks_cache.notifications().clone().wait_for_updates(saved_notification);
                }
            })
            .expect("Failed to spawn block_processor thread");
        Self { service_handler, missing_blocks_were_requested }
    }
}

fn calculate_v_parameter(
    attestation_target_in_bkset_size: usize,
    chance_of_successful_attack: f64,
    bk_set_size: usize,
) -> f64 {
    #[allow(non_snake_case)]
    let A = attestation_target_in_bkset_size as f64;
    tracing::trace!("A: {A}");
    #[allow(non_snake_case)]
    let N = bk_set_size as f64;
    tracing::trace!("N: {N}");
    // the expected fraction of Malicious Block Keepers from the total number of Block Keepers
    let m = 1.0f64 / 6.0f64;
    let p: f64 = chance_of_successful_attack;
    // expected number of acki-nacki
    let v = (N - 1.0f64) * (1.0f64 - p.powf(1.0 / (A - m * N + 1.0f64)));

    // Note: paranoid check that we have at least one acki-nacki
    // let v = if v < 1.0f64 { 1.0f64 } else { v };
    tracing::trace!("v: {v}");
    v
}

#[allow(non_snake_case, clippy::too_many_arguments)]
#[instrument(skip_all)]
fn process_candidate_block(
    security_guarantee: SecurityGuarantee,
    node_id: NodeIdentifier,
    save_state_frequency: u32,
    bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
    block_state: &BlockState,
    block_state_repository: &BlockStateRepository,
    repository: &RepositoryImpl,
    shared_services: &mut SharedServices,
    nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
    candidate_block: &Envelope<GoshBLS, AckiNackiBlock>,
    validation_service: &ValidationServiceInterface,
    time_to_produce_block: &Duration,
    share_service: ExternalFileSharesBased,
    send: AckiNackiSend,
    chain_pulse_monitor: &Sender<ChainPulseEvent>,
    cross_thread_ref_data_availability_synchronization_service: &mut CrossThreadRefDataAvailabilitySynchronizationServiceInterface,
    save_optimistic_service_sender: &InstrumentedSender<Arc<OptimisticStateImpl>>,
    filter_prehistoric: &FilterPrehistoric,
) -> anyhow::Result<()> {
    // if block_state.guarded(|e| e.is_block_already_applied()) {
    //     // This is the last flag this method sets. Skip this block checks if it is already set.
    //     return Ok(());
    // }
    let block_id = block_state.block_identifier().clone();
    let (Some(block_seq_no), Some(parent_id)) =
        block_state.guarded(|e| (*e.block_seq_no(), e.parent_block_identifier().clone()))
    else {
        return Ok(());
    };
    tracing::trace!(
        "Process block candidate: block_seq_no: {:?} block_id: {:?}",
        block_seq_no,
        block_id,
    );
    if block_state.guarded(|e| e.is_invalidated()) {
        tracing::trace!("Process block candidate: block was invalidated, skip it");
        return Ok(());
    }
    if block_state.guarded(|e| e.is_finalized()) {
        tracing::trace!("Process block candidate: block is finalized, skip it");
        return Ok(());
    }
    let Ok(parent_block_state) = block_state_repository.get(&parent_id) else {
        tracing::trace!("Unexpected failure: failed to load parent");
        return Ok(());
    };
    if block_state.guarded(|e| e.bk_set().is_none()) {
        if parent_block_state.guarded(|e| e.descendant_bk_set().is_some()) {
            if !rules::bk_set::set_bk_set(block_state, block_state_repository) {
                tracing::trace!("Failed to set BK set. Skip block");
                return Ok(());
            }
        } else {
            tracing::trace!("BK set is not set. Skip block");
            return Ok(());
        }
    }
    if block_state.guarded(|e| e.ancestors().is_none()) {
        if let Some(parent_ancestors) = parent_block_state.guarded(|e| e.ancestors().clone()) {
            block_state.guarded_mut(|e| {
                if e.ancestors().is_some() {
                    return Ok(());
                }
                let mut ancestors = Vec::with_capacity(MAX_STATE_ANCESTORS.get());
                ancestors.push(parent_id.clone());
                for ancestor_id in parent_ancestors.iter().take(MAX_STATE_ANCESTORS.get() - 1) {
                    ancestors.push(ancestor_id.clone());
                }
                e.set_ancestors(ancestors)
            })?;
        }
    }

    if !block_state.guarded(|e| e.is_stored()) {
        return Ok(());
    }

    if !block_state.guarded(|e| e.common_checks_passed() == &Some(true)) {
        let start = std::time::Instant::now();
        if !parent_block_state.guarded(|e| e.common_checks_passed() == &Some(true)) {
            tracing::trace!("Parent block has not passed common checks");
            return Ok(());
        };
        if check_common_block_params(
            candidate_block,
            &parent_block_state,
            time_to_produce_block,
            block_state,
        )? {
            let thread_id = block_state.guarded_mut(|e| {
                e.set_common_checks_passed()?;
                anyhow::Ok(*e.thread_identifier())
            })?;
            shared_services.metrics.as_ref().inspect(|m| {
                if let Some(thread_id) = thread_id {
                    m.report_common_block_checks(start.elapsed().as_millis() as u64, &thread_id);
                } else {
                    tracing::error!("Thread id not set for block {}", block_id);
                }
            });
        } else {
            tracing::trace!(target: "monit", "{block_state:?} failed to pass common checks");
            invalidate_branch(block_state.clone(), block_state_repository, filter_prehistoric);
            let _ = send.send_nack(
                block_state.clone(),
                NackReason::BadBlock { envelope: candidate_block.clone() },
            );
            return Ok(());
        }
    }

    let result = verify_all_block_signatures(block_state_repository, candidate_block, block_state);

    if let Some(status) = result {
        if !status {
            tracing::trace!("Process block candidate: blocks signature is invalid, invalidate it");
            tracing::trace!(target: "monit", "{block_state:?} signatures verification failed");
            invalidate_branch(block_state.clone(), block_state_repository, filter_prehistoric);
            return Ok(());
        }
        if !block_state.guarded(|e| e.is_signatures_verified()) {
            block_state.guarded_mut(|e| {
                e.set_signatures_verified()?;
                if e.producer().is_none() {
                    e.set_producer(
                        candidate_block.data().get_common_section().producer_id.clone(),
                    )?;
                }
                Ok::<_, anyhow::Error>(())
            })?;
        }
    } else {
        tracing::trace!("Process block candidate: can't check block signature, skip it");
        return Ok(());
    }

    if block_state.guarded(|e| e.descendant_bk_set().is_none() && e.is_signatures_verified()) {
        rules::descendant_bk_set::set_descendant_bk_set(block_state, candidate_block);
    }

    if block_state.guarded(|e| e.block_stats().is_none() && e.bk_set().is_some()) {
        if let Some(parent_stats) = parent_block_state.guarded(|e| e.block_stats().clone()) {
            let grandparent_block_id = {
                if parent_block_state.block_identifier() != &BlockIdentifier::default() {
                    let Some(grandparent_block_id) =
                        parent_block_state.guarded(|e| e.parent_block_identifier().clone())
                    else {
                        tracing::trace!(
                            "Process block candidate: grandparent block id is not set."
                        );
                        return Ok(());
                    };
                    grandparent_block_id
                } else {
                    Default::default()
                }
            };
            let blocks_finalized_by_parent =
                if parent_block_state.block_identifier() != &BlockIdentifier::default() {
                    let Some(blocks_finalized_by_parent) =
                        parent_block_state.guarded(|e| e.finalizes_blocks().clone())
                    else {
                        tracing::trace!(
                            "Process block candidate: parent block finalizes_blocks is not set."
                        );
                        return Ok(());
                    };
                    blocks_finalized_by_parent
                } else {
                    Default::default()
                };
            let mut blocks_finalized_by_parent: Vec<(BlockSeqNo, BlockIdentifier)> =
                blocks_finalized_by_parent
                    .iter()
                    .map(|index| (*index.block_seq_no(), index.block_identifier().clone()))
                    .collect();
            blocks_finalized_by_parent.sort_by(|a, b| a.0.cmp(&b.0));
            let finalized_block_distances_sorted_by_seq_no = if blocks_finalized_by_parent
                .is_empty()
            {
                vec![]
            } else if grandparent_block_id != BlockIdentifier::default() {
                let grandparent_block_state =
                    block_state_repository.get(&grandparent_block_id).unwrap();
                let Some(grandparent_checkpoints) = grandparent_block_state
                    .guarded(|e| e.ancestor_blocks_finalization_checkpoints().clone())
                else {
                    tracing::trace!("Process block candidate: grandparent ancestor_blocks_finalization_checkpoints is not set.");
                    return Ok(());
                };
                blocks_finalized_by_parent
                    .iter()
                    .map(|(_, e)| {
                        if let Some(checkpoints) = grandparent_checkpoints.fallback().get(e) {
                            if let Some(checkpoint) = checkpoints.first() {
                                return *checkpoint.current_distance() + 2; // parent + finalizing block
                            }
                        }
                        let checkpoint = grandparent_checkpoints.primary().get(e).expect(
                            "Must have a checkpoint. otherwise how did it finalize the block",
                        );
                        *checkpoint.current_distance() + 2
                    })
                    .collect::<Vec<usize>>()
            } else {
                vec![]
            };
            let moment = Instant::now();

            // TODO: need to recalculate attestation target
            let bk_set_len = block_state
                .guarded(|e| e.bk_set().clone())
                .map(|map| map.len())
                .expect("BK set must be set on this stage");
            let primary_attestation_target = (2 * bk_set_len).div_ceil(3);
            let fallback_attestation_target = (bk_set_len >> 1) + 1;
            let cur_block_stats =
                parent_stats.clone().next(&finalized_block_distances_sorted_by_seq_no, None);

            let bk_set = block_state.guarded(|e| e.bk_set().clone());

            // TODO: add test of this formula with big values
            tracing::trace!("calculate must be validator");
            let median_descendants_chain_length_to_meet_threshold =
                cur_block_stats.median_descendants_chain_length_to_meet_threshold();
            tracing::trace!("median_descendants_chain_length_to_meet_threshold: {median_descendants_chain_length_to_meet_threshold}");
            // from the spec:
            let bk_set_size = bk_set.as_ref().map(|x| x.len()).unwrap();
            let N = bk_set_size as f64;
            let v = calculate_v_parameter(
                primary_attestation_target,
                security_guarantee.chance_of_successful_attack(),
                bk_set_size,
            );
            let fallback_v = calculate_v_parameter(
                fallback_attestation_target,
                security_guarantee.chance_of_successful_attack(),
                bk_set_size,
            );
            if let Some(pubkey) =
                bk_set.as_ref().and_then(|x| x.get_by_node_id(&node_id)).map(|x| x.pubkey.clone())
            {
                if let Some(rnd) = bls_keys_map.guarded(|e| e.get(&pubkey).map(|x| x.1.clone())) {
                    let rnd = rnd ^ block_id.clone();

                    let this_block = rnd.not_a_modulus(1 << 16) as SignerIndex;
                    if (this_block as f64) * N <= (v * (SignerIndex::MAX as f64)) {
                        tracing::trace!("set_must_be_validated");
                        block_state.guarded_mut(|e| {
                            if e.must_be_validated() != &Some(true) {
                                e.set_must_be_validated()
                            } else {
                                anyhow::Ok(())
                            }
                        })?;
                    } else if (this_block as f64) * N <= (fallback_v * (SignerIndex::MAX as f64)) {
                        tracing::trace!("set_must_be_validated_in_fallback_case");
                        block_state.guarded_mut(|e| {
                            if e.must_be_validated_in_fallback_case() != &Some(true) {
                                e.set_must_be_validated_in_fallback_case()
                            } else {
                                anyhow::Ok(())
                            }
                        })?;
                    }
                } else {
                    start_shutdown();
                    tracing::error!("Node does not have valid key which was used to deploy epoch: pubkey={pubkey:?}");
                }
            }
            let attestation_target = block_state.guarded(|e| *e.attestation_target());
            let thread_identifier = block_state.guarded(|e| *e.thread_identifier());
            if attestation_target.is_none() {
                let parent_attestation_target = parent_block_state
                    .guarded(|e| *e.attestation_target())
                    .expect("Parent attestation target must be set");
                let descendant_generations = if median_descendants_chain_length_to_meet_threshold
                    < *parent_attestation_target.primary().generation_deadline()
                {
                    *parent_attestation_target.primary().generation_deadline() - 1
                } else if median_descendants_chain_length_to_meet_threshold
                    < MAX_ATTESTATION_TARGET_BETA
                {
                    median_descendants_chain_length_to_meet_threshold
                } else {
                    MAX_ATTESTATION_TARGET_BETA
                };

                shared_services.metrics.as_ref().inspect(|m| {
                    if let Some(thread_id) = thread_identifier {
                        m.report_attn_target_descendant_generations(
                            descendant_generations,
                            &thread_id,
                        );
                    } else {
                        tracing::error!("Thread id not set for block {}", block_id);
                    }
                });
                // TODO: add second attestation target check
                block_state.guarded_mut(|e| {
                    if e.attestation_target().is_none() {
                        e.set_attestation_target(
                            AttestationTargets::builder()
                                .primary(
                                    AttestationTarget::builder()
                                        .generation_deadline(descendant_generations)
                                        .required_attestation_count(primary_attestation_target)
                                        .build(),
                                )
                                .fallback(
                                    AttestationTarget::builder()
                                        .generation_deadline(2 * descendant_generations + 1)
                                        .required_attestation_count(fallback_attestation_target)
                                        .build(),
                                )
                                .build(),
                        )
                    } else {
                        anyhow::Ok(())
                    }
                })?;
            }
            block_state.guarded_mut(|e| {
                if e.block_stats().is_none() {
                    e.set_block_stats(cur_block_stats.clone())
                } else {
                    anyhow::Ok(())
                }
            })?;

            shared_services.metrics.as_ref().inspect(|m| {
                if let Some(thread_id) = thread_identifier {
                    m.report_calc_consencus_params(moment.elapsed().as_millis() as u64, &thread_id);
                } else {
                    tracing::error!("Thread id not set for block {}", block_id);
                }
            });
        }
    }

    if !block_state.guarded(|e| e.is_prefinalized()) {
        for detached_attestations in block_state.guarded(|e| e.detached_attestations().clone()) {
            if let Some(attestation_target) = block_state.guarded(|e| *e.attestation_target()) {
                if detached_attestations.clone_signature_occurrences().len()
                    >= *attestation_target.fallback().required_attestation_count()
                {
                    if let (Some(thread_id), block_height) = block_state.guarded_mut(|e| {
                        e.set_prefinalized(detached_attestations.clone())?;
                        anyhow::Ok((*e.thread_identifier(), *e.block_height()))
                    })? {
                        let _ = chain_pulse_monitor
                            .send(ChainPulseEvent::block_prefinalized(thread_id, block_height));
                    }
                }
            }
        }
    }

    if !process_block_attestations(
        block_state,
        &parent_block_state,
        block_state_repository,
        candidate_block,
        validation_service,
        chain_pulse_monitor,
        filter_prehistoric,
        shared_services,
    )? {
        tracing::trace!("Process block candidate: can't process_block_attestations, skip it");
        return Ok(());
    }

    // TODO: need to check attestations from common section and pass them to the attestation processor or attestation target service
    // TODO: need to check acks and nacks from common section

    if block_state.guarded(|e| e.has_all_cross_thread_ref_data_available().is_none()) {
        tracing::trace!("Process block candidate: check cross thread ref data for block refs");

        let moment = Instant::now();
        let missing_refs = shared_services.exec(|service| {
            let mut missing_refs = vec![];
            for block_id in &candidate_block.data().get_common_section().refs {
                if service
                    .cross_thread_ref_data_service
                    .get_cross_thread_ref_data(block_id)
                    .is_err()
                {
                    missing_refs.push(block_id.clone());
                }
            }
            missing_refs
        });
        if !missing_refs.is_empty() {
            tracing::trace!("Some refs are missing: {missing_refs:?}");
            let missing =
                missing_refs.iter().map(|id| block_state_repository.get(id).unwrap()).collect();
            cross_thread_ref_data_availability_synchronization_service
                .send_await_cross_thread_ref_data(block_state.clone(), missing);
            return Ok(());
        }
        let (thread_identifier, received_ms, block_identifier) = block_state.guarded_mut(|e| {
            e.set_has_all_cross_thread_ref_data_available()?;
            anyhow::Ok((
                *e.thread_identifier(),
                e.event_timestamps.received_ms.unwrap_or_default(),
                e.block_identifier().clone(),
            ))
        })?;
        shared_services.metrics.as_ref().inspect(|m| {
            if let Some(thread_id) = thread_identifier {
                let moment_ms = now_ms();
                if received_ms > moment_ms {
                    tracing::error!(
                        "Block received timestamp is in the future, block_id: {}",
                        block_id
                    );
                }
                m.report_check_cross_thread_ref_data(
                    moment.elapsed().as_millis() as u64,
                    moment_ms.saturating_sub(received_ms),
                    &thread_id,
                );
            } else {
                tracing::error!("Thread id not set for block {}", block_identifier);
            }
        });
    }

    if !block_state.guarded(|e| e.is_block_already_applied()) {
        let moment = Instant::now();
        tracing::trace!("Process block candidate: apply block");
        let parent_block_state = block_state_repository.get(&parent_id)?;
        if parent_block_state.guarded(|e| e.is_block_already_applied()) {
            let (thread_id, producer, block_height) = block_state.guarded(|e| {
                (
                    e.thread_identifier().expect("Must be set"),
                    e.producer().clone().expect("Must be set"),
                    (*e.block_height()).expect("Must be set"),
                )
            });
            if producer == node_id && block_state.guarded(|e| e.apply_can_be_skipped()) {
                block_state.guarded_mut(|e| {
                    e.set_applied(moment, Instant::now())?;
                    e.event_timestamps.block_applied_timestamp_ms = Some(now_ms());
                    Ok::<_, anyhow::Error>(())
                })?;

                let _ = chain_pulse_monitor
                    .send(ChainPulseEvent::block_applied(thread_id, Some(block_height)));

                return Ok(());
            }
            if block_state.guarded(|e| {
                *e.must_be_validated() == Some(true) || !e.has_bad_block_nacks_resolved()
            }) {
                validation_service.send((block_state.clone(), candidate_block.clone()));
            }
            // let (_, last_finalized_seq_no) = repository
            //     .select_thread_last_finalized_block(&thread_id)?
            //     .expect("Must be known here");
            // On the moment of block apply and block append block must have valid chain to the last finalized
            // TODO: replace this check:
            // - Invalidate children of an invalidated block
            // - Set block invalidated when adding to known children for an invalidated parent
            // if let Err(e) = block_state_repository
            // .select_unfinalized_ancestor_blocks(block_state, last_finalized_seq_no)
            // {
            // tracing::error!(
            // "Failed to build chain from the candidate block to the last finalized: {e}"
            // );
            // block_state.guarded_mut(|e| e.set_invalidated())?;
            // return Ok(());
            // }

            let optimistic_state = match repository.get_optimistic_state(
                &parent_id,
                &thread_id,
                repository.last_finalized_optimistic_state(&thread_id),
            ) {
                Ok(Some(optimistic_state)) => optimistic_state,
                Ok(None) => {
                    panic!("Failed to load optimistic state for parent block");
                }
                Err(err) => {
                    return match err.downcast_ref::<RepositoryError>() {
                        Some(RepositoryError::BlockNotFound(_)) => Ok(()),
                        Some(RepositoryError::DepthSearchMinStateLimitReached) => {
                            tracing::trace!(target: "monit", "{block_state:?} A block from an abandoned branch");
                            invalidate_branch(
                                block_state.clone(),
                                block_state_repository,
                                filter_prehistoric,
                            );
                            Ok(())
                        }
                        Some(RepositoryError::DepthSearchBlockCountLimitReached) => Err(err),
                        _ => Err(err),
                    }
                }
            };
            let mut optimistic_state = Arc::unwrap_or_clone(optimistic_state);
            let (cross_thread_ref_data, _messages) = match optimistic_state.apply_block(
                candidate_block.data(),
                shared_services,
                block_state_repository.clone(),
                nack_set_cache,
                repository.accounts_repository().clone(),
                repository.get_message_db().clone(),
            ) {
                Ok(cross_thread_ref_data) => cross_thread_ref_data,
                Err(e) => {
                    tracing::trace!(target: "monit", "{block_state:?} Failed to apply candidate block: {e}");
                    invalidate_branch(
                        block_state.clone(),
                        block_state_repository,
                        filter_prehistoric,
                    );
                    return Ok(());
                }
            };

            shared_services.metrics.as_ref().inspect(|m| {
                m.report_internal_message_queue_length(
                    optimistic_state.get_internal_message_queue_length() as u64,
                );
            });

            let common_section = candidate_block.data().get_common_section().clone();
            let parent_seq_no = parent_block_state.guarded(|e| *e.block_seq_no());
            let must_save_state = common_section.directives.share_state_resources().is_some()
                || candidate_block.data().is_thread_splitting()
                || must_save_state_on_seq_no(block_seq_no, parent_seq_no, save_state_frequency);
            let optimistic_state = Arc::new(optimistic_state);
            repository.store_optimistic_in_cache(optimistic_state.clone())?;
            if must_save_state && (common_section.producer_id != node_id) {
                let _ = save_optimistic_service_sender.send(optimistic_state);
            }

            if let Some(share_state) =
                candidate_block.data().get_common_section().directives.share_state_resources()
            {
                for (thread_id, block_id) in share_state {
                    if let Some(state) =
                        repository.get_full_optimistic_state(block_id, thread_id, None)?
                    {
                        share_service.save_state_for_sharing(state)?;
                    }
                }
            }

            block_state.guarded_mut(|e| {
                e.set_applied(moment, Instant::now())?;
                e.event_timestamps.block_applied_timestamp_ms = Some(now_ms());
                Ok::<_, anyhow::Error>(())
            })?;

            let _ = chain_pulse_monitor
                .send(ChainPulseEvent::block_applied(thread_id, Some(block_height)));

            shared_services.exec(|e| {
                e.cross_thread_ref_data_service.set_cross_thread_ref_data(cross_thread_ref_data)
            })?;
            block_state.guarded_mut(|e| e.set_has_cross_thread_ref_data_prepared())?;
            cross_thread_ref_data_availability_synchronization_service
                .send_cross_thread_ref_data_prepared(block_state.clone());
            shared_services.metrics.as_ref().inspect(|m| {
                m.report_apply_block_total(moment.elapsed().as_millis() as u64, &thread_id);
            });
        }
    }

    Ok(())
}

// Function verifies block signatures and attestation signatures from common section
pub(crate) fn verify_all_block_signatures(
    block_state_repository: &BlockStateRepository,
    candidate_block: &Envelope<GoshBLS, AckiNackiBlock>,
    block_state: &BlockState,
) -> Option<bool> {
    // TODO: verify acks and nacks in the common section

    let moment = Instant::now();

    if let Some(is_producer_signature_valid) =
        block_state.guarded(|e| *e.envelope_block_producer_signature_verified())
    {
        if !is_producer_signature_valid {
            return Some(false);
        }
    } else {
        let bk_set = block_state.guarded(|e| e.bk_set().clone())?;
        let signers_map = bk_set.get_pubkeys_by_signers();
        tracing::trace!(
            "Signature verification start: block: {}, signers: {:?}",
            candidate_block,
            signers_map
        );
        let is_valid = candidate_block
            .verify_signatures(signers_map)
            .expect("Signatures verification should not crash.");
        let stated_producer_id = candidate_block.data().get_common_section().producer_id.clone();
        let Some(stated_producer_data) = bk_set.get_by_node_id(&stated_producer_id) else {
            return Some(false);
        };
        // Ensure producer ID stated in the common section has its signature in the bls section
        if !candidate_block
            .clone_signature_occurrences()
            .get(&stated_producer_data.signer_index)
            .map(|count| *count > 0)
            .unwrap_or(false)
        {
            return Some(false);
        }
        block_state.guarded_mut(|e| {
            if let Some(race_condition) = e.envelope_block_producer_signature_verified() {
                assert!(*race_condition == is_valid);
            } else {
                let _ = e.set_envelope_block_producer_signature_verified(is_valid);
            }
        });
        if !is_valid {
            tracing::trace!("Signature verification failed: {}", candidate_block);
            return Some(false);
        }
    };
    tracing::trace!("Block signature is valid (envelope)");
    let previously_verified_attestations: HashSet<(BlockIdentifier, AttestationTargetType)> =
        block_state.guarded(|e| HashSet::from_iter(e.verified_attestations().keys().cloned()));
    if previously_verified_attestations.len()
        != candidate_block.data().get_common_section().block_attestations.len()
    {
        let mut is_all_success = true;
        let mut verified_attestations = vec![];
        for attestation in candidate_block.data().get_common_section().block_attestations.iter() {
            let attested_block_id = attestation.data().block_id().clone();
            let attestation_type = *attestation.data().target_type();
            if previously_verified_attestations.contains(&(attested_block_id, attestation_type)) {
                continue;
            }
            let Ok(ancestor_block_state) =
                block_state_repository.get(attestation.data().block_id())
            else {
                continue;
            };
            let (is_parent_invalidated, attestation_signers_map) =
                ancestor_block_state.guarded(|e| (e.is_invalidated(), e.bk_set().clone()));
            if is_parent_invalidated {
                continue;
            }
            let Some(envelope_hash) = ancestor_block_state.guarded(|e| e.envelope_hash().clone())
            else {
                tracing::trace!("Ancestor block does not have envelope hash set, skip attestation");
                continue;
            };
            if &envelope_hash != attestation.data().envelope_hash() {
                tracing::trace!("Attestation envelope hash verification failed: {:?}", attestation);
                return Some(false);
            }
            let Some(attestation_signers_map) = attestation_signers_map else { continue };

            let attestation_signers_map = attestation_signers_map.get_pubkeys_by_signers();
            let is_attestation_signatures_valid = attestation
                .verify_signatures(attestation_signers_map)
                .expect("Attestation signatures verification should not crash.");
            if !is_attestation_signatures_valid {
                tracing::trace!("Attestations signature verification failed: {}", candidate_block);
                return Some(false);
            }
            verified_attestations.push(attestation);
        }
        if block_state.guarded_mut(|e| {
            let mut check_failed = false;
            for attestation in verified_attestations {
                if e.add_verified_attestations_for(
                    attestation.data().block_id().clone(),
                    *attestation.data().target_type(),
                    HashSet::from_iter(attestation.clone_signature_occurrences().keys().cloned()),
                )
                .is_err()
                {
                    check_failed = true;
                    break;
                }
            }
            // Accumulate info about execution duration
            e.event_timestamps.verify_all_block_signatures_ms_total = Some(
                moment.elapsed().as_millis()
                    + e.event_timestamps.verify_all_block_signatures_ms_total.unwrap_or(0),
            );
            check_failed
        }) {
            // DO NOT collapse this if statement.
            is_all_success = false;
        }

        let previously_verified_attestations: HashSet<(BlockIdentifier, AttestationTargetType)> =
            block_state.guarded(|e| HashSet::from_iter(e.verified_attestations().keys().cloned()));
        if previously_verified_attestations.len()
            != candidate_block.data().get_common_section().block_attestations.len()
        {
            is_all_success = false;
        }
        return if is_all_success { Some(true) } else { None };
    }
    Some(true)
}

fn check_common_block_params(
    candidate_block: &Envelope<GoshBLS, AckiNackiBlock>,
    parent_block_state: &BlockState,
    _time_to_produce_block: &Duration,
    block_state: &BlockState,
) -> anyhow::Result<bool> {
    let (Some(_parent_time), Some(parent_seq_no), Some(parent_height)) =
        parent_block_state.guarded(|e| (*e.block_time_ms(), *e.block_seq_no(), *e.block_height()))
    else {
        anyhow::bail!("Failed to get block data to perform common check");
    };

    if candidate_block.data().seq_no() <= parent_seq_no {
        tracing::trace!(
            "Invalid block seq_no: candidate_seq_no: {} <= parent_seq_no: {parent_seq_no}",
            candidate_block.data().seq_no()
        );
        return Ok(false);
    }

    if candidate_block.data().get_common_section().block_height
        != parent_height.next(&candidate_block.data().get_common_section().thread_id)
    {
        tracing::trace!(
            "Invalid block height: candidate_height: {:?}, parent_height: {parent_height:?}",
            candidate_block.data().get_common_section().block_height
        );
        return Ok(false);
    }

    // TODO: block time can be seriously affected by block production correction and time diff between neighbor blocks can be much less than 330 ms
    // let minimal_block_time = parent_time + time_to_produce_block.as_millis() as u64
    //     - ALLOWED_BLOCK_PRODUCTION_TIME_LAG_MS;
    // let block_time = candidate_block.data().time()?;
    // if block_time < minimal_block_time {
    //     tracing::trace!("Invalid block time: block_time: {block_time}, minimal_block_time: {minimal_block_time}");
    //     return Ok(false);
    // }

    let (Some(bk_set), Some(producer_selector)) =
        block_state.guarded(|e| (e.bk_set().clone(), e.producer_selector_data().clone()))
    else {
        anyhow::bail!("Failed to get selector data to perform common check");
    };
    let is_producer_correct = producer_selector
        .is_node_bp(&bk_set, &candidate_block.data().get_common_section().producer_id);
    if is_producer_correct.is_err() || !is_producer_correct? {
        tracing::trace!("Invalid producer selector");
        return Ok(false);
    }

    let common_section = candidate_block.data().get_common_section();
    if let Some(threads_table) = &common_section.threads_table {
        let rows = threads_table.rows().map(|(a, _b)| a.clone()).collect::<Vec<_>>();
        let mut rows_dedup = rows.clone();
        rows_dedup.dedup();
        if rows_dedup.len() != rows.len() {
            // Note: panic in debug purposes now, should be changed to nack block later
            panic!("Candidate block common section contains threads table with mask duplicates");
        }
    }

    candidate_block.data().check_hash()
}

#[allow(clippy::too_many_arguments)]
fn process_block_attestations(
    block_state: &BlockState,
    parent_block_state: &BlockState,
    block_state_repository: &BlockStateRepository,
    candidate_block: &Envelope<GoshBLS, AckiNackiBlock>,
    validation_service: &ValidationServiceInterface,
    chain_pulse_monitor: &Sender<ChainPulseEvent>,
    filter_prehistoric: &FilterPrehistoric,
    shared_services: &SharedServices,
) -> anyhow::Result<bool> {
    if block_state.guarded(|e| e.has_block_attestations_processed() == &Some(true)) {
        return Ok(true);
    }
    let Some(thread_id) = block_state.guarded(|e| *e.thread_identifier()) else {
        tracing::trace!("Thread id is not ready");
        return Ok(false);
    };
    let verified_attestations = block_state.guarded(|e| e.verified_attestations().clone());

    let Ok(ancestor_distances) =
        inherit_ancestor_blocks_finalization_distances(parent_block_state, block_state)
    else {
        tracing::trace!("ancestor distances are not ready");
        return Ok(false);
    };

    let AncestorBlocksFinalizationCheckpointsConstructorResults {
        remaining_checkpoints: ancestor_blocks_finalization_distances,
        passed_primary,
        passed_fallback,
        failed,
        transitioned_to_fallback,
        passed_fallback_preattestation_checkpoint,
    } = ancestor_distances
        .into_builder()
        .update(verified_attestations.into_iter().map(|(k, v)| (k, v.len())).collect());

    if !failed.is_empty() {
        tracing::trace!(target: "monit", "process_block_attestations: attestations_target was not reached, block is considered as invalid {:?}. Missing attestations for: {failed:?}", block_state);
        invalidate_branch(block_state.clone(), block_state_repository, filter_prehistoric);
        return Ok(false);
    }
    let mut max_finalized_ancestor: Option<(BlockSeqNo, BlockIdentifier)> = None;
    for block_id in passed_primary.iter() {
        let ancestor_block_state = block_state_repository.get(block_id).unwrap();
        max_finalized_ancestor = ancestor_block_state.guarded_mut(|e| {
            if e.prefinalization_proof().is_none() {
                // TODO: this moment can be optimized be storing attestation in BlockState
                let attestation = candidate_block
                    .data()
                    .get_common_section()
                    .block_attestations
                    .iter()
                    .find(|attestation| {
                        attestation.data().block_id() == block_id
                            && *attestation.data().target_type() == AttestationTargetType::Primary
                    })
                    .cloned()
                    .expect("Attestation must be here, it is present in the verified list");
                e.set_prefinalized(attestation).expect("Failed to set prefinalizaton proof");
                let _ = chain_pulse_monitor.send(ChainPulseEvent::block_prefinalized(
                    (*e.thread_identifier()).expect("Prefinalized block must have thread id set"),
                    *e.block_height(),
                ));
            }
            if e.primary_finalization_proof().is_none() {
                let attestation = candidate_block
                    .data()
                    .get_common_section()
                    .block_attestations
                    .iter()
                    .find(|attestation| {
                        attestation.data().block_id() == block_id
                            && *attestation.data().target_type() == AttestationTargetType::Primary
                    })
                    .cloned()
                    .expect("Attestation must be here, it is present in the verified list");
                shared_services.metrics.as_ref().inspect(|m| {
                    m.report_finalized_block_attestations_cnt(
                        HashSet::<SignerIndex>::from_iter(
                            attestation.clone_signature_occurrences().keys().cloned(),
                        )
                        .len() as u64,
                        &thread_id,
                    );
                });
                e.set_primary_finalization_proof(attestation).unwrap();
            }
            if e.has_primary_attestation_target_met().is_none() {
                e.set_has_primary_attestation_target_met()
                    .expect("Failed to  set_has_primary_attestation_target_met");
            }
            let block_seq_no = e.block_seq_no().unwrap();
            if max_finalized_ancestor.is_none()
                || max_finalized_ancestor.as_ref().unwrap().0 > block_seq_no
            {
                max_finalized_ancestor = Some((block_seq_no, block_id.clone()));
            }
            max_finalized_ancestor
        });
    }
    for block_id in passed_fallback_preattestation_checkpoint {
        let ancestor_block_state = block_state_repository.get(&block_id).unwrap();
        ancestor_block_state.guarded_mut(|e| {
            if e.prefinalization_proof().is_none() {
                // TODO: this moment can be optimized be storing attestation in BlockState
                let attestation = candidate_block
                    .data()
                    .get_common_section()
                    .block_attestations
                    .iter()
                    .find(|attestation| {
                        attestation.data().block_id() == &block_id
                            && *attestation.data().target_type() == AttestationTargetType::Primary
                    })
                    .cloned()
                    .expect("Attestation must be here, it is present in the verified list");
                e.set_prefinalized(attestation).expect("Failed to set prefinalizaton proof");
                let _ = chain_pulse_monitor.send(ChainPulseEvent::block_prefinalized(
                    (*e.thread_identifier()).expect("Prefinalized block must have thread id set"),
                    *e.block_height(),
                ));
            }
        });
    }
    for block_id in transitioned_to_fallback {
        let ancestor_block_state = block_state_repository.get(&block_id).unwrap();
        ancestor_block_state.guarded_mut(|e| {
            if e.must_be_validated().is_none() {
                // Recalculate if must be validated.
                if e.must_be_validated_in_fallback_case() == &Some(true) {
                    e.set_must_be_validated().expect("Failed to set must_be_validated");
                    validation_service.send((block_state.clone(), candidate_block.clone()));
                }
            }
            e.set_requires_fallback_attestation()
                .expect("Failed to set requires_fallback_attestation");
        });
    }
    for block_id in passed_fallback.iter() {
        let ancestor_block_state = block_state_repository.get(block_id).unwrap();
        max_finalized_ancestor = ancestor_block_state.guarded_mut(|e| {
            if e.fallback_finalization_proof().is_none() {
                let attestation = candidate_block
                    .data()
                    .get_common_section()
                    .block_attestations
                    .iter()
                    .find(|attestation| {
                        attestation.data().block_id() == block_id
                            && *attestation.data().target_type() == AttestationTargetType::Fallback
                    })
                    .cloned()
                    .expect("Attestation must be here, it is present in the verified list");
                shared_services.metrics.as_ref().inspect(|m| {
                    m.report_finalized_block_attestations_cnt(
                        HashSet::<SignerIndex>::from_iter(
                            attestation.clone_signature_occurrences().keys().cloned(),
                        )
                        .len() as u64,
                        &thread_id,
                    );
                });
                e.set_fallback_finalization_proof(attestation).unwrap();
            }
            if e.has_fallback_attestation_target_met().is_none() {
                e.set_has_fallback_attestation_target_met()
                    .expect("Failed to set has_fallback_attestation_target_met");
            }
            let block_seq_no = e.block_seq_no().unwrap();
            if max_finalized_ancestor.is_none()
                || max_finalized_ancestor.as_ref().unwrap().0 > block_seq_no
            {
                max_finalized_ancestor = Some((block_seq_no, block_id.clone()));
            }
            max_finalized_ancestor
        });
    }

    let mut finalizes_blocks_with_indexes = BTreeSet::new();
    let block_attestations: Vec<Envelope<GoshBLS, AttestationData>> =
        candidate_block.data().get_common_section().block_attestations.clone();
    for block_id in passed_fallback.iter().cloned().chain(passed_primary.iter().cloned()) {
        let attn_data = block_attestations.iter().find(|attestation| { *attestation.data().block_id() == block_id }).expect("Failed to find attestation for ancestor block that is finalized by the current candidate");
        finalizes_blocks_with_indexes.insert(BlockIndex::new(
            *attn_data.data().block_seq_no(),
            attn_data.data().block_id().clone(),
        ));
    }
    block_state.guarded_mut(|e| -> anyhow::Result<()> {
        if e.finalizes_blocks()
            .clone()
            .map(|value| value != finalizes_blocks_with_indexes)
            .unwrap_or(true)
        {
            e.set_finalizes_blocks(finalizes_blocks_with_indexes)?;
            if let Some(cutoff) = max_finalized_ancestor {
                e.set_moves_attestation_list_cutoff(cutoff)?;
            }
        }
        Ok(())
    })?;
    let mut dependent_block_ids: HashSet<BlockIdentifier> =
        HashSet::from_iter(ancestor_blocks_finalization_distances.primary().keys().cloned());
    dependent_block_ids.extend(ancestor_blocks_finalization_distances.fallback().keys().cloned());
    dependent_block_ids.remove(block_state.block_identifier());
    block_state.guarded_mut(|e| {
        if e.ancestor_blocks_finalization_checkpoints().is_none() {
            e.set_ancestor_blocks_finalization_checkpoints(ancestor_blocks_finalization_distances)
        } else {
            anyhow::Ok(())
        }
    })?;
    let cur_block_producer_id = candidate_block.data().get_common_section().producer_id.clone();
    for block_id in dependent_block_ids {
        let ancestor_block_state = block_state_repository.get(&block_id).unwrap();
        ancestor_block_state.guarded_mut(|e| {
            let _ = e.try_add_attestations_interest(cur_block_producer_id.clone());
        });
    }
    block_state.guarded_mut(|e| e.set_has_block_attestations_processed()).unwrap();
    Ok(true)
}
