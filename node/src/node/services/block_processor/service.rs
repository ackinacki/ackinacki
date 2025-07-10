// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use tvm_block::ShardStateUnsplit;
use tvm_types::Cell;
use tvm_types::UInt256;

use super::rules;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::must_save_state_on_seq_no;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::block_state::state::AttestationsTarget;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocks;
use crate::node::services::block_processor::chain_pulse::events::ChainPulseEvent;
use crate::node::services::block_processor::chain_pulse::ChainPulse;
use crate::node::services::validation::feedback::AckiNackiSend;
use crate::node::shared_services::SharedServices;
use crate::node::unprocessed_blocks_collection::UnfinalizedBlocksSnapshot;
use crate::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::node::PubKey;
use crate::node::Secret;
use crate::node::SignerIndex;
use crate::node::ValidationServiceInterface;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::CrossThreadRefDataRead;
use crate::repository::Repository;
use crate::repository::RepositoryError;
use crate::types::bp_selector::BlockGap;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::RndSeed;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::utilities::thread_spawn_critical::SpawnCritical;
use crate::utilities::FixedSizeHashSet;

const MAX_ATTESTATION_TARGET_BETA: usize = 100;
// const ALLOWED_BLOCK_PRODUCTION_TIME_LAG_MS: u64 = 50;

use network::channel::NetBroadcastSender;
use network::channel::NetDirectSender;
use telemetry_utils::now_ms;

use crate::node::services::sync::ExternalFileSharesBased;
use crate::node::services::sync::StateSyncService;
use crate::node::services::PULSE_IDLE_TIMEOUT;

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
        archive: Option<
            std::sync::mpsc::Sender<(
                Envelope<GoshBLS, AckiNackiBlock>,
                Option<Arc<ShardStateUnsplit>>,
                Option<Cell>,
            )>,
        >,
        skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
        block_gap: BlockGap,
        validation_service: ValidationServiceInterface,
        share_service: ExternalFileSharesBased,
        send: AckiNackiSend,
        chain_pulse_monitor: std::sync::mpsc::Sender<ChainPulseEvent>,
        unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection,
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
                    .chain_pulse_monitor(chain_pulse_monitor)
                    .build();
                let _ = chain_pulse.pulse(&chain_pulse_last_finalized_block);
                loop {
                    let notifications =
                        block_state_repository.notifications().load(Ordering::Relaxed);
                    unprocessed_blocks_cache.remove_finalized_and_invalidated_blocks();
                    // Await for signal that smth has changes and blocks can be checked again
                    if let Some(last_finalized_block_id) = repository.select_thread_last_finalized_block(&thread_identifier)?.map(|t| t.0) {
                        let last_finalized_block = block_state_repository.get(&last_finalized_block_id)?;
                        chain_pulse.pulse(&last_finalized_block)?;

                        #[allow(clippy::mutable_key_type)]
                        let blocks_to_process: UnfinalizedBlocksSnapshot =
                            unprocessed_blocks_cache.clone_queue();

                        shared_services.metrics.as_ref().inspect(|m| {
                            m.report_unfinalized_blocks_queue(blocks_to_process.len() as u64, &thread_identifier);
                        });

                        let _ = chain_pulse.evaluate(&blocks_to_process, &block_state_repository, &repository);
                        for (block_state, block) in blocks_to_process.values() {
                            {
                                let mut lock = block_state.lock();
                                lock.set_bulk_change(true)?;
                                if !lock.event_timestamps.block_process_timestamp_was_reported {
                                    lock.event_timestamps.block_process_timestamp_was_reported = true;
                                    shared_services.metrics.as_ref().inspect(|m| {
                                        if let Some(thread_id) = lock.thread_identifier() {
                                            if let Some(received) = &lock.event_timestamps.received_ms {
                                                m.report_processing_delay(
                                                    now_ms().saturating_sub(*received),
                                                    thread_id,
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
                                &archive,
                                &skipped_attestation_ids,
                                block,
                                &validation_service,
                                &time_to_produce_block,
                                share_service.clone(),
                                send.clone(),
                            )?;
                            {
                                let mut lock = block_state.lock();
                                lock.set_bulk_change(false)?;
                            }
                        }
                    }
                    let new_notifications =
                        block_state_repository.notifications().load(Ordering::Relaxed);
                    if new_notifications == notifications {
                        std::thread::sleep(PULSE_IDLE_TIMEOUT);
                    }
                    // atomic_wait::wait(block_state_repository.notifications(), notifications);
                }
            })
            .expect("Failed to spawn block_processor thread");
        Self { service_handler, missing_blocks_were_requested }
    }
}

#[allow(non_snake_case, clippy::too_many_arguments)]
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
    archive: &Option<
        std::sync::mpsc::Sender<(
            Envelope<GoshBLS, AckiNackiBlock>,
            Option<Arc<ShardStateUnsplit>>,
            Option<Cell>,
        )>,
    >,
    skipped_attestation_ids: &Arc<Mutex<HashSet<BlockIdentifier>>>,
    candidate_block: &Envelope<GoshBLS, AckiNackiBlock>,
    validation_service: &ValidationServiceInterface,
    time_to_produce_block: &Duration,
    share_service: ExternalFileSharesBased,
    send: AckiNackiSend,
) -> anyhow::Result<()> {
    if block_state.guarded(|e| e.is_block_already_applied()) {
        // This is the last flag this method sets. Skip this block checks if it is already set.
        return Ok(());
    }
    let block_id = block_state.block_identifier().clone();
    let (Some(block_seq_no), Some(parent_id)) =
        block_state.guarded(|e| (*e.block_seq_no(), e.parent_block_identifier().clone()))
    else {
        return Ok(());
    };
    tracing::trace!(
        "Process block candidate: block_seq_no: {:?} block_id: {:?}, verified_attestations: {:?}",
        block_seq_no,
        block_id,
        block_state.guarded(|e| e.verified_attestations().clone())
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
            let metrics = shared_services.metrics.as_ref();
            if !rules::bk_set::set_bk_set(block_state, block_state_repository, metrics) {
                tracing::trace!("Failed to set BK set. Skip block");
                return Ok(());
            }
        } else {
            tracing::trace!("BK set is not set. Skip block");
            return Ok(());
        }
    }

    tracing::trace!("Process block candidate: check block signature");

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
            block_state.guarded_mut(|e| {
                shared_services.metrics.as_ref().inspect(|m| {
                    if let Some(thread_id) = e.thread_identifier() {
                        m.report_common_block_checks(start.elapsed().as_millis() as u64, thread_id);
                    } else {
                        tracing::error!("Thread id not set for block {}", block_id);
                    }
                });
                e.set_common_checks_passed()
            })?;
        } else {
            block_state.guarded_mut(|e| e.set_invalidated())?;
            let _ = send.send_nack_bad_block(block_state.clone(), candidate_block.clone());
            return Ok(());
        }
    }

    let result = verify_all_block_signatures(
        block_state_repository,
        candidate_block,
        block_state,
        skipped_attestation_ids,
    );

    if let Some(status) = result {
        if !status {
            tracing::trace!("Process block candidate: blocks signature is invalid, invalidate it");
            block_state.guarded_mut(|e| e.set_invalidated())?;
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
            let moment = Instant::now();

            // TODO: need to recalculate attestation target
            let bk_set_len = block_state
                .guarded(|e| e.bk_set().clone())
                .map(|map| map.len())
                .expect("BK set must be set on this stage");
            let attestations_target = (2 * bk_set_len).div_ceil(3);
            let min_attestations_target = (bk_set_len >> 1) + 1;

            let attestations = candidate_block
                .data()
                .get_common_section()
                .block_attestations
                .iter()
                .map(|att| {
                    (
                        att.data().block_id().clone(),
                        att.clone_signature_occurrences().keys().cloned().collect(),
                    )
                })
                .collect();

            tracing::trace!("Process block candidate: attestations: {:?}", attestations);

            let cur_block_stats = parent_stats.clone().next(
                // TODO: parent id was here check what is the right one
                block_id.clone(),
                attestations_target,
                attestations,
                None,
            );
            block_state.guarded_mut(|e| -> anyhow::Result<()> {
                // TODO: add test of this formula with big values
                tracing::trace!("calculate must be validator");
                let median_descendants_chain_length_to_meet_threshold =
                    cur_block_stats.median_descendants_chain_length_to_meet_threshold();
                tracing::trace!("median_descendants_chain_length_to_meet_threshold: {median_descendants_chain_length_to_meet_threshold}");
                // from the spec:
                let A = attestations_target as f64;
                tracing::trace!("A: {A}");
                let N = e.bk_set().as_ref().map(|x| x.len()).unwrap() as f64;
                tracing::trace!("N: {N}");
                // the expected fraction of Malicious Block Keepers from the total number of Block Keepers
                let m = 1.0f64 / 6.0f64;
                let p: f64 = security_guarantee.chance_of_successful_attack();
                // expected number of acki-nacki
                let v = (N - 1.0f64) * (1.0f64 - p.powf(1.0 / (A - m * N + 1.0f64)));

                // Note: paranoid check that we have at least one acki-nacki
                let v = if v < 1.0f64 {
                    1.0f64
                } else {
                    v
                };
                tracing::trace!("v: {v}");
                if let Some(pubkey) = e.get_bk_data_for_node_id(&node_id).map(|x| x.pubkey) {
                    let rnd: RndSeed =
                        bls_keys_map.guarded(|e| e.get(&pubkey).map(|x| x.1.clone())).unwrap();
                    tracing::trace!("rnd: {rnd:?}");
                    tracing::trace!("block_id: {block_id:?}");
                    let rnd = rnd ^ block_id.clone();
                    tracing::trace!("rnd: {rnd:?}");

                    let this_block = rnd.not_a_modulus(1 << 16) as SignerIndex;
                    tracing::trace!("this_block: {this_block}");
                    if (this_block as f64) * N <= (v * (SignerIndex::MAX as f64)) {
                        tracing::trace!("set_must_be_validated");
                        e.set_must_be_validated()?;
                    }
                }
                if e.initial_attestations_target().is_none() {
                    let parent_attestation_target = parent_block_state.guarded(|e| *e.initial_attestations_target()).expect("Parent attestation target must be set");
                    let descendant_generations = if median_descendants_chain_length_to_meet_threshold < parent_attestation_target.descendant_generations {
                        parent_attestation_target.descendant_generations - 1
                    } else if median_descendants_chain_length_to_meet_threshold < MAX_ATTESTATION_TARGET_BETA {
                        median_descendants_chain_length_to_meet_threshold
                    } else {
                        MAX_ATTESTATION_TARGET_BETA
                    };

                    shared_services.metrics.as_ref().inspect(|m| {
                        if let Some(thread_id) = e.thread_identifier() {
                            m.report_attn_target_descendant_generations(descendant_generations, thread_id);
                        } else {
                            tracing::error!("Thread id not set for block {}", block_id);
                        }
                    });
                    // TODO: add second attestation target check
                    e.set_initial_attestations_target(AttestationsTarget {
                        // Note: spec.
                        descendant_generations,
                        main_attestations_target: attestations_target,
                        fallback_attestations_target: min_attestations_target,
                    })?;
                }
                e.set_block_stats(cur_block_stats.clone())?;

                shared_services.metrics.as_ref().inspect(|m| {
                    if let Some(thread_id) = e.thread_identifier() {
                        m.report_calc_consencus_params(moment.elapsed().as_millis() as u64, thread_id);
                    } else {
                        tracing::error!("Thread id not set for block {}", block_id);
                    }
                });
                Ok(())
            })?;
        }
    }

    if !parse_verified_attestations(
        block_state,
        &parent_block_state,
        block_state_repository,
        candidate_block,
    )? {
        tracing::trace!("Process block candidate: can't parse_verified_attestations, skip it");
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
            return Ok(());
        }
        block_state.guarded_mut(|e| {
            shared_services.metrics.as_ref().inspect(|m| {
                if let Some(thread_id) = e.thread_identifier() {
                    let received_ms = e.event_timestamps.received_ms.unwrap_or_default();
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
                        thread_id,
                    );
                } else {
                    tracing::error!("Thread id not set for block {}", e.block_identifier());
                }
            });
            e.set_has_all_cross_thread_ref_data_available()
        })?;
    }

    if !block_state.guarded(|e| e.is_block_already_applied()) {
        let moment = Instant::now();
        tracing::trace!("Process block candidate: apply block");
        let parent_block_state = block_state_repository.get(&parent_id)?;
        if parent_block_state.guarded(|e| e.is_block_already_applied()) {
            let (thread_id, producer) = block_state.guarded(|e| {
                (
                    e.thread_identifier().expect("Must be set"),
                    e.producer().clone().expect("Must be set"),
                )
            });
            if producer == node_id {
                block_state.guarded_mut(|e| {
                    e.set_applied()?;
                    e.event_timestamps.block_applied_timestamp_ms = Some(now_ms());
                    Ok::<_, anyhow::Error>(())
                })?;
                return Ok(());
            }
            if block_state.guarded(|e| {
                *e.must_be_validated() == Some(true) || !e.has_bad_block_nacks_resolved()
            }) {
                validation_service.send((block_state.clone(), candidate_block.clone()));
            }
            let (_, last_finalized_seq_no) = repository
                .select_thread_last_finalized_block(&thread_id)?
                .expect("Must be known here");
            // On the moment of block apply and block append block must have valid chain to the last finalized
            if let Err(e) = block_state_repository
                .select_unfinalized_ancestor_blocks(block_state, last_finalized_seq_no)
            {
                tracing::error!(
                    "Failed to build chain from the candidate block to the last finalized: {e}"
                );
                block_state.guarded_mut(|e| e.set_invalidated())?;
                return Ok(());
            }

            let mut optimistic_state = match repository.get_optimistic_state(
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
                            block_state.guarded_mut(|e| e.set_invalidated())?;
                            Ok(())
                        }
                        Some(RepositoryError::DepthSearchBlockCountLimitReached) => Err(err),
                        _ => Err(err),
                    }
                }
            };
            let (cross_thread_ref_data, _) = match optimistic_state.apply_block(
                candidate_block.data(),
                shared_services,
                block_state_repository.clone(),
                nack_set_cache,
                repository.accounts_repository().clone(),
                repository.get_message_db().clone(),
            ) {
                Ok(cross_thread_ref_data) => cross_thread_ref_data,
                Err(e) => {
                    tracing::error!("Failed to apply candidate block: {e}");
                    block_state.guarded_mut(|e| e.set_invalidated())?;
                    return Ok(());
                }
            };
            if let Some(archive) = archive.as_ref() {
                archive.send((
                    candidate_block.clone(),
                    optimistic_state.shard_state.shard_state.clone(),
                    optimistic_state.shard_state.shard_state_cell.clone(),
                ))?;
            }
            let common_section = candidate_block.data().get_common_section().clone();
            let parent_seq_no = parent_block_state.guarded(|e| *e.block_seq_no());
            let must_save_state = common_section.directives.share_state_resources().is_some()
                || candidate_block.data().is_thread_splitting()
                || must_save_state_on_seq_no(block_seq_no, parent_seq_no, save_state_frequency);
            if must_save_state {
                repository.store_optimistic(optimistic_state)?;
            } else {
                repository.store_optimistic_in_cache(optimistic_state)?;
            }

            if let Some(share_state) =
                candidate_block.data().get_common_section().directives.share_state_resources()
            {
                for (thread_id, block_id) in share_state {
                    if let Some(state) =
                        repository.get_full_optimistic_state(block_id, thread_id, None)?
                    {
                        share_service.save_state_for_sharing(Arc::new(state))?;
                    }
                }
            }

            block_state.guarded_mut(|e| {
                e.set_applied()?;
                e.event_timestamps.block_applied_timestamp_ms = Some(now_ms());
                Ok::<_, anyhow::Error>(())
            })?;

            shared_services.on_block_appended(candidate_block.data());
            shared_services.exec(|e| {
                e.cross_thread_ref_data_service.set_cross_thread_ref_data(cross_thread_ref_data)
            })?;
            block_state.guarded_mut(|e| e.set_has_cross_thread_ref_data_prepared())?;
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
    skipped_attestation_ids: &Arc<Mutex<HashSet<BlockIdentifier>>>,
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
    let previously_verified_attestations: HashSet<BlockIdentifier> =
        block_state.guarded(|e| HashSet::from_iter(e.verified_attestations().keys().cloned()));
    if previously_verified_attestations.len()
        != candidate_block.data().get_common_section().block_attestations.len()
    {
        let skipped_attestation_ids = skipped_attestation_ids.lock().clone();
        let mut is_all_success = true;
        let mut verified_attestations = vec![];
        for attestation in candidate_block.data().get_common_section().block_attestations.iter() {
            if previously_verified_attestations.contains(attestation.data().block_id()) {
                continue;
            }
            let Ok(ancestor_block_state) =
                block_state_repository.get(attestation.data().block_id())
            else {
                continue;
            };
            if skipped_attestation_ids.contains(attestation.data().block_id()) {
                continue;
            }
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

    candidate_block.data().check_hash()
}

fn parse_verified_attestations(
    block_state: &BlockState,
    parent_block_state: &BlockState,
    block_state_repository: &BlockStateRepository,
    candidate_block: &Envelope<GoshBLS, AckiNackiBlock>,
) -> anyhow::Result<bool> {
    let Some(mut parent_distances) =
        parent_block_state.guarded(|e| e.ancestor_blocks_finalization_distances().clone())
    else {
        tracing::trace!("parse_verified_attestations: parent block ancestor_distances not found, skip the block");
        return Ok(false);
    };
    tracing::trace!(
        "parse_verified_attestations: parent block ancestor_distances: {parent_distances:?}"
    );

    if parent_block_state.guarded(|e| e.thread_identifier().expect("Must be set"))
        != block_state.guarded(|e| e.thread_identifier().expect("Must be set"))
    {
        parent_distances = HashMap::new();
    }
    let verified_attestations = block_state.guarded(|e| e.verified_attestations().clone());
    tracing::trace!(
        "parse_verified_attestations: verified_attestations: {verified_attestations:?}"
    );
    for (block_id, signers) in verified_attestations {
        let ancestor_block_state = block_state_repository.get(&block_id)?;
        let Some(attestations_target) =
            ancestor_block_state.guarded(|e| *e.initial_attestations_target())
        else {
            tracing::trace!("parse_verified_attestations: attested block {block_id:?} attestations_target not found, skip the block");
            return Ok(false);
        };

        // TODO: need to change back to the fallback_attestations_target according to the document
        if signers.len() >= attestations_target.main_attestations_target {
            tracing::trace!("parse_verified_attestations: block {block_id:?} min_attestations_target was reached");
            ancestor_block_state.guarded_mut(|e| {
                if !e.is_prefinalized() {
                    e.set_prefinalized().expect("Failed to set prefinalized");
                    // TODO: this moment can be optimized be storing attestation in BlockState
                    let attestation = candidate_block
                        .data()
                        .get_common_section()
                        .block_attestations
                        .iter()
                        .find(|attestation| attestation.data().block_id() == &block_id)
                        .cloned()
                        .expect("Attestation must be here, it is present in the verified list");
                    e.set_prefinalization_proof(attestation)
                        .expect("Failed to set prefinalizaton proof");
                }
            });
        }
        if signers.len() >= attestations_target.main_attestations_target {
            tracing::trace!(
                "parse_verified_attestations: block {block_id:?} attestations_target was reached"
            );
            ancestor_block_state.guarded_mut(|e| {
                if e.has_initial_attestations_target_met().is_none() {
                    e.set_has_initial_attestations_target_met()
                        .expect("Failed to  set_has_initial_attestations_target_met");
                }
                block_state.guarded_mut(|e| e.update_finalizes_blocks(block_id.clone()));
            });
            if parent_distances.contains_key(&block_id) {
                parent_distances.remove(&block_id);
            }
        }
    }
    for (block_id, value) in parent_distances.iter_mut() {
        *value -= 1;
        if *value == 0 {
            tracing::trace!("parse_verified_attestations: block {block_id:?} attestations_target was not reached, block is considered as invalid {:?}", block_state.block_identifier());
            block_state.guarded_mut(|e| e.set_invalidated())?;
            return Ok(false);
        }
    }
    parent_distances.insert(
        block_state.block_identifier().clone(),
        block_state.guarded(|e| {
            // e.block_stats()
            //     .clone()
            //     .expect("Block stats must be set here")
            //     .median_descendants_chain_length_to_meet_threshold()
            e.initial_attestations_target().expect("Must be set").descendant_generations
        }),
    );
    tracing::trace!(
        "parse_verified_attestations: current block ancestor_distances: {parent_distances:?}"
    );
    block_state.guarded_mut(|e| e.set_ancestor_blocks_finalization_distances(parent_distances))?;
    Ok(true)
}
