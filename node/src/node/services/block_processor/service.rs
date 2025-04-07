// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use http_server::BlockKeeperSetUpdate;
use parking_lot::Mutex;
use telemetry_utils::mpsc::InstrumentedSender;
use thiserror::Error;
use tvm_block::ShardStateUnsplit;
use tvm_types::Cell;
use tvm_types::UInt256;

use super::rules;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::must_save_state_on_seq_no;
use crate::external_messages::ExternalMessagesThreadState;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::block_state::state::AttestationsTarget;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocks;
use crate::node::services::attestations_target::service::AttestationsTargetService;
use crate::node::services::block_processor::chain_pulse::ChainPulse;
use crate::node::services::send_attestations::AttestationSendService;
use crate::node::shared_services::SharedServices;
use crate::node::unprocessed_blocks_collection::UnfinalizedBlocksSnapshot;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::node::PubKey;
use crate::node::Secret;
use crate::node::SignerIndex;
use crate::node::UnfinalizedCandidateBlockCollection;
use crate::node::ValidationServiceInterface;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::CrossThreadRefDataRead;
use crate::repository::Repository;
use crate::repository::RepositoryError;
use crate::types::bp_selector::BlockGap;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::CollectedAttestations;
use crate::types::ForkResolution;
use crate::types::RndSeed;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::utilities::thread_spawn_critical::SpawnCritical;
use crate::utilities::FixedSizeHashSet;

#[derive(Error, Debug)]
pub enum ForkResolutionVerificationError {
    #[error("Failed to load parent block state, original error {0}")]
    StateLoad(String),

    #[error("Block state doesn't have descendant_bk_set, block_id {0}")]
    BkSetAbsent(String),

    #[error("Winner attestation signatures verification failure: {0}")]
    WinnerAttestationsFailure(String),

    #[error("Lost attestation signatures verification failure: {0}")]
    LostAttestationsFailure(String),
}
use network::channel::NetBroadcastSender;
use network::channel::NetDirectSender;
use telemetry_utils::now_ms;
use ForkResolutionVerificationError::*;

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
        unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection,
        block_state_repository: BlockStateRepository,
        repository: RepositoryImpl,
        mut shared_services: SharedServices,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        send_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
        broadcast_tx: NetBroadcastSender<NetworkMessage>,
        archive: std::sync::mpsc::Sender<(
            Envelope<GoshBLS, AckiNackiBlock>,
            Option<Arc<ShardStateUnsplit>>,
            Option<Cell>,
        )>,
        skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
        bk_state_update_tx: InstrumentedSender<BlockKeeperSetUpdate>,
        block_gap: BlockGap,
        external_messages: ExternalMessagesThreadState,
        last_block_attestations: Arc<Mutex<CollectedAttestations>>,
        mut attestation_sender_service: AttestationSendService,
        validation_service: ValidationServiceInterface,
    ) -> Self {
        let chain_pulse_last_finalized_block_id: BlockIdentifier =
            repository.select_thread_last_finalized_block(&thread_identifier).unwrap().0;
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
                            &repository.select_thread_last_finalized_block(&thread_identifier)?.0
                        )?
                    ))
                    .build();
                let _ = chain_pulse.pulse(&chain_pulse_last_finalized_block);
                let mut attestations_target_service = AttestationsTargetService::builder()
                    .repository(repository.clone())
                    .block_state_repository(block_state_repository.clone())
                    .build();
                loop {
                    let notifications =
                        block_state_repository.notifications().load(Ordering::Relaxed);
                    unprocessed_blocks_cache.remove_finalized_and_invalidated_blocks();
                    // Await for signal that smth has changes and blocks can be checked again
                    let last_finalized_block_id: BlockIdentifier = repository.select_thread_last_finalized_block(&thread_identifier)?.0;
                    let last_finalized_block = block_state_repository.get(&last_finalized_block_id)?;
                    chain_pulse.pulse(&last_finalized_block)?;

                    #[allow(clippy::mutable_key_type)]
                    let blocks_to_process: UnfinalizedBlocksSnapshot =
                        unprocessed_blocks_cache.clone_queue();

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
                            &bk_state_update_tx,
                            &mut shared_services,
                            nack_set_cache.clone(),
                            &archive,
                            &skipped_attestation_ids,
                            &external_messages,
                            block,
                            &validation_service,
                            &time_to_produce_block,
                        )?;
                        {
                            let mut lock = block_state.lock();
                            lock.set_bulk_change(false)?;
                        }
                    }
                    attestations_target_service.evaluate(&blocks_to_process);

                    let new_notifications =
                        block_state_repository.notifications().load(Ordering::Relaxed);
                    if new_notifications == notifications {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    // atomic_wait::wait(block_state_repository.notifications(), notifications);
                    attestation_sender_service.evaluate(
                        &blocks_to_process,
                        last_block_attestations.clone(),
                        &repository,
                    );
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
    bk_set_update_tx: &InstrumentedSender<BlockKeeperSetUpdate>,
    shared_services: &mut SharedServices,
    nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
    archive: &std::sync::mpsc::Sender<(
        Envelope<GoshBLS, AckiNackiBlock>,
        Option<Arc<ShardStateUnsplit>>,
        Option<Cell>,
    )>,
    skipped_attestation_ids: &Arc<Mutex<HashSet<BlockIdentifier>>>,
    external_messages: &ExternalMessagesThreadState,
    candidate_block: &Envelope<GoshBLS, AckiNackiBlock>,
    validation_service: &ValidationServiceInterface,
    time_to_produce_block: &Duration,
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
        if parent_block_state.guarded(|e| e.bk_set().is_some() && e.is_signatures_verified()) {
            rules::descendant_bk_set::set_descendant_bk_set(&parent_block_state, repository);
        }
        if parent_block_state.guarded(|e| e.descendant_bk_set().is_some()) {
            rules::bk_set::set_bk_set(block_state, block_state_repository);
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
        if check_common_block_params(candidate_block, &parent_block_state, time_to_produce_block)? {
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
        rules::descendant_bk_set::set_descendant_bk_set(block_state, repository);
    }

    if block_state.guarded(|e| e.block_stats().is_none() && e.bk_set().is_some()) {
        let parent_block_state = block_state_repository.get(&parent_id)?;
        if let Some(parent_stats) = parent_block_state.guarded(|e| e.block_stats().clone()) {
            let moment = Instant::now();

            // TODO: need to recalculate attestation target
            let attestation_target = block_state
                .guarded(|e| e.bk_set().clone())
                .map(|map| 2 * map.len() / 3)
                .expect("BK set must be set on this stage");

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
                attestation_target,
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
                let A = attestation_target as f64;
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
                    if (this_block as f64) * N <= (v * (SignerIndex::MAX as f64))  {
                        tracing::trace!("set_must_be_validated");
                        e.set_must_be_validated()?;
                    }
                }
                if e.initial_attestations_target().is_none() {
                    e.set_initial_attestations_target(AttestationsTarget {
                        // Note: spec.
                        descendant_generations: 1 + median_descendants_chain_length_to_meet_threshold,
                        count: attestation_target,
                    })?;
                }
                e.set_block_stats(cur_block_stats.clone())?;

                shared_services.metrics.as_ref().inspect(|m| {
                    if let Some(thread_id) =  e.thread_identifier() {
                        m.report_calc_consencus_params(moment.elapsed().as_millis() as u64, thread_id);
                    } else {
                        tracing::error!("Thread id not set for block {}", block_id);
                    }
                });
                Ok(())
            })?;
        }
    }

    verify_fork_resolutions(
        block_state,
        block_state_repository,
        candidate_block,
        &shared_services.metrics,
    )?;

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
            if producer != node_id {
                let parent_block_external_messages_progress = external_messages
                    .get_progress(parent_block_state.block_identifier())
                    .expect("Must not fail")
                    .expect("Must be set");
                external_messages.set_progress(
                    block_state.block_identifier(),
                    parent_block_external_messages_progress,
                )?;
            } else {
                // Ensure consistency: blocks produced by node must have progress set.
                assert!(external_messages
                    .get_progress(block_state.block_identifier())
                    .unwrap()
                    .is_some());

                let bk_set_update = block_state.guarded_mut(|e| {
                    e.set_applied()?;
                    e.event_timestamps.block_applied_timestamp_ms = Some(now_ms());
                    Ok::<_, anyhow::Error>(e.bk_set().as_ref().map(|x| BlockKeeperSetUpdate {
                        node_ids: x.iter_node_ids().map(|x| x.to_string()).collect(),
                    }))
                })?;
                if let Some(update) = bk_set_update {
                    let _ = bk_set_update_tx.send(update);
                }
                return Ok(());
            }
            if block_state.guarded(|e| *e.must_be_validated() == Some(true)) {
                validation_service.send(block_state.clone());
            }

            let (_, last_finalized_seq_no) =
                repository.select_thread_last_finalized_block(&thread_id)?;
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
                nack_set_cache.clone(),
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

            archive.send((
                candidate_block.clone(),
                optimistic_state.shard_state.shard_state.clone(),
                optimistic_state.shard_state.shard_state_cell.clone(),
            ))?;
            let common_section = candidate_block.data().get_common_section().clone();
            let parent_seq_no = parent_block_state.guarded(|e| *e.block_seq_no());
            let must_save_state = common_section.directives.share_state_resource_address.is_some()
                || candidate_block.data().is_thread_splitting()
                || must_save_state_on_seq_no(block_seq_no, parent_seq_no, save_state_frequency);
            if must_save_state {
                repository.store_optimistic(optimistic_state)?;
            } else {
                repository.store_optimistic_in_cache(optimistic_state)?;
            }

            let bk_set_update = block_state.guarded_mut(|e| {
                e.set_applied()?;
                e.event_timestamps.block_applied_timestamp_ms = Some(now_ms());
                Ok::<_, anyhow::Error>(e.bk_set().as_ref().map(|x| BlockKeeperSetUpdate {
                    node_ids: x.iter_node_ids().map(|x| x.to_string()).collect(),
                }))
            })?;
            if let Some(update) = bk_set_update {
                let _ = bk_set_update_tx.send(update);
            }
            shared_services.on_block_appended(candidate_block.data());
            shared_services.exec(|e| {
                e.cross_thread_ref_data_service.set_cross_thread_ref_data(cross_thread_ref_data)
            })?;
            shared_services.metrics.as_ref().inspect(|m| {
                m.report_apply_block_total(moment.elapsed().as_millis() as u64, &thread_id);
            });
        }
    }

    Ok(())
}

fn verify_fork_resolutions(
    block_state: &BlockState,
    block_state_repository: &BlockStateRepository,
    candidate_block: &Envelope<GoshBLS, AckiNackiBlock>,
    metrics: &Option<crate::helper::metrics::BlockProductionMetrics>,
) -> anyhow::Result<()> {
    if block_state.guarded(|e| e.resolves_forks().is_some()) {
        return Ok(());
    }
    let moment = Instant::now();
    let fork_resolutions = candidate_block.data().get_common_section().fork_resolutions.clone();

    for fork_resolution in fork_resolutions.iter() {
        match check_attestation_signatures(fork_resolution, block_state_repository) {
            Ok(true) => {
                continue;
            }
            Ok(false) => {
                tracing::error!(
                    "Attestation signatures are not valid for block {}",
                    candidate_block
                );
                block_state.guarded_mut(|e| e.set_invalidated())?;
            }
            Err(error) if matches!(error, StateLoad(_) | BkSetAbsent(_)) => {
                tracing::error!(
                    "Skip attestation signatures check on this iteration for block {}, reason: {}",
                    candidate_block,
                    error
                );
            }
            Err(error) => {
                tracing::error!(
                    "Attestation signatures not valid for block {}, reason: {}",
                    candidate_block,
                    error
                );
                block_state.guarded_mut(|e| e.set_invalidated())?;
            }
        };
        break;

        // TODO: Add fork resolution verification in the next PR
        // set block invalid if any single resolution is invalid.
        // send Nack for the block (invalid fork resolution)
    }

    block_state.guarded_mut(|e| {
        metrics.as_ref().inspect(|m| {
            if let Some(thread_id) = e.thread_identifier() {
                m.report_verify_fork_resolution(moment.elapsed().as_millis() as u64, thread_id);
            } else {
                tracing::error!("Thread id not set for block {}", e.block_identifier());
            }
        });

        if e.resolves_forks().is_none() {
            e.set_resolves_forks(fork_resolutions)
        } else {
            Ok(())
        }
    })
}

fn check_attestation_signatures(
    fork_resolution: &ForkResolution,
    block_state_repository: &BlockStateRepository,
) -> Result<bool, ForkResolutionVerificationError> {
    let parent_block_id = fork_resolution.parent_block_identifier();

    let parent_block_state =
        block_state_repository.get(parent_block_id).map_err(|err| StateLoad(err.to_string()))?;

    let attestation_signers_map = parent_block_state
        .guarded(|e| e.descendant_bk_set().clone())
        .ok_or(BkSetAbsent(parent_block_id.to_string()))?
        .get_pubkeys_by_signers();

    let winner_attestations_are_valid = fork_resolution
        .winner_attestations()
        .verify_signatures(&attestation_signers_map)
        .map_err(|err| WinnerAttestationsFailure(err.to_string()))?;

    // Check lost attestations too
    let lost_attestations_are_valid = fork_resolution
        .lost_attestations()
        .iter()
        .map(|attestation| {
            attestation
                .verify_signatures(&attestation_signers_map)
                .map_err(|err| LostAttestationsFailure(err.to_string()))
        })
        .collect::<Result<Vec<bool>, ForkResolutionVerificationError>>()?
        .iter()
        .all(|result| *result);

    Ok(winner_attestations_are_valid && lost_attestations_are_valid)
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
            .verify_signatures(&signers_map)
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
            let Some(attestation_signers_map) = attestation_signers_map else { continue };

            let attestation_signers_map = attestation_signers_map.get_pubkeys_by_signers();
            let is_attestation_signatures_valid = attestation
                .verify_signatures(&attestation_signers_map)
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
    time_to_produce_block: &Duration,
) -> anyhow::Result<bool> {
    let (Some(parent_time), Some(parent_seq_no)) =
        parent_block_state.guarded(|e| (*e.block_time_ms(), *e.block_seq_no()))
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

    let minimal_block_time = parent_time + time_to_produce_block.as_millis() as u64;
    let block_time = candidate_block.data().time()?;
    let now = now_ms();

    if block_time < minimal_block_time && block_time > now {
        tracing::trace!("Invalid block time: block_time: {block_time}, minimal_block_time: {minimal_block_time}, now: {now}");
        return Ok(false);
    }

    candidate_block.data().check_hash()
}
