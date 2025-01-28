// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::Duration;

use database::documents_db::DocumentsDb;
use http_server::BlockKeeperSetUpdate;
use parking_lot::Mutex;
use tvm_types::UInt256;

use super::rules;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::database::write_to_db;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocks;
use crate::node::services::attestations_target::service::AttestationsTargetService;
use crate::node::services::block_processor::chain_pulse::ChainPulse;
use crate::node::shared_services::SharedServices;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::node::PubKey;
use crate::node::Secret;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::CrossThreadRefData;
use crate::repository::CrossThreadRefDataRead;
use crate::repository::Repository;
use crate::types::as_signatures_map::AsSignaturesMap;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::RndSeed;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::utilities::thread_spawn_critical::SpawnCritical;
use crate::utilities::FixedSizeHashSet;

// TODO: take it from config
const SAVE_STATE_FREQUENCY: u32 = 200;

pub struct BlockProcessorService {
    pub service_handler: std::thread::JoinHandle<()>,
    pub missing_blocks_were_requested: Arc<AtomicBool>,
}

impl BlockProcessorService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeIdentifier,
        time_to_produce_block: Duration,
        bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
        thread_identifier: ThreadIdentifier,
        unprocessed_blocks_cache: Arc<Mutex<Vec<BlockState>>>,
        block_state_repository: BlockStateRepository,
        repository: RepositoryImpl,
        mut shared_services: SharedServices,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        send_direct_tx: Sender<(NodeIdentifier, NetworkMessage)>,
        broadcast_tx: Sender<NetworkMessage>,
        archive: Option<Arc<dyn DocumentsDb>>,
        skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
        bk_state_update_tx: Sender<BlockKeeperSetUpdate>,
        block_gap: Arc<Mutex<usize>>,
    ) -> Self {
        let chain_pulse_last_finalized_block_seq_no =
            repository.select_thread_last_finalized_block(&thread_identifier).unwrap().1;
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
                    .build();
                let _ = chain_pulse.pulse(chain_pulse_last_finalized_block_seq_no);
                loop {
                    let notifications =
                        block_state_repository.notifications().load(Ordering::Relaxed);
                    let mut attestations_target_service = AttestationsTargetService::builder()
                        .repository(repository.clone())
                        .blocks_states(block_state_repository.clone())
                        .build();
                    // Await for signal that smth has changes and blocks can be checked again
                    let mut last_finalized_block_fallback = None;
                    let blocks_to_process: Vec<BlockState> = {
                        unprocessed_blocks_cache.guarded_mut(|cache| {
                            cache.retain(|e| {
                                e.guarded(|f| {
                                    if f.is_finalized() {
                                        let seq_no = f.block_seq_no().unwrap();
                                        if let Some(other) = last_finalized_block_fallback {
                                            if seq_no > other {
                                                last_finalized_block_fallback = Some(seq_no);
                                            }
                                        } else {
                                            last_finalized_block_fallback = Some(seq_no);
                                        }
                                        // Do not retain
                                        return false;
                                    }
                                    !f.is_invalidated()
                                })
                            });
                            cache.clone()
                        })
                    };
                    if let Some(finalized) = last_finalized_block_fallback {
                        let _ = chain_pulse.pulse(finalized);
                    }
                    let _ = chain_pulse.evaluate(&blocks_to_process, &block_state_repository, &repository);
                    for block_state in &blocks_to_process {
                        process_candidate_block(
                            node_id.clone(),
                            bls_keys_map.clone(),
                            block_state,
                            &block_state_repository,
                            &repository,
                            &bk_state_update_tx,
                            &mut shared_services,
                            nack_set_cache.clone(),
                            archive.clone(),
                            &skipped_attestation_ids,
                        )?;
                    }
                    attestations_target_service.evaluate(blocks_to_process.clone());

                    unprocessed_blocks_cache.guarded_mut(|e| {
                        e.retain(|block_state| !block_state.lock().is_invalidated())
                    });
                    atomic_wait::wait(block_state_repository.notifications(), notifications);
                }
            })
            .expect("Failed to spawn block_processor thread");
        Self { service_handler, missing_blocks_were_requested }
    }
}

#[allow(non_snake_case, clippy::too_many_arguments)]
fn process_candidate_block(
    node_id: NodeIdentifier,
    bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
    block_state: &BlockState,
    block_state_repository: &BlockStateRepository,
    repository: &RepositoryImpl,
    bk_set_update_tx: &Sender<BlockKeeperSetUpdate>,
    shared_services: &mut SharedServices,
    nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
    archive: Option<Arc<dyn DocumentsDb>>,
    skipped_attestation_ids: &Arc<Mutex<HashSet<BlockIdentifier>>>,
) -> anyhow::Result<()> {
    let block_id = block_state.block_identifier().clone();
    let Some(block_seq_no) = block_state.guarded(|e| *e.block_seq_no()) else {
        return Ok(());
    };
    let Some(parent_id) = block_state.guarded(|e| e.parent_block_identifier().clone()) else {
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
    let Ok(parent_block_state) = block_state_repository.get(&parent_id) else {
        tracing::trace!("Unexpected failure: parent failed to load");
        return Ok(());
    };
    if parent_block_state.guarded(|e| e.bk_set().is_some()) {
        if parent_block_state
            .guarded(|e| e.descendant_bk_set().is_none() && e.is_signatures_verified())
        {
            rules::descendant_bk_set::set_descendant_bk_set(&parent_block_state, repository);
        }
        rules::bk_set::set_bk_set(block_state, block_state_repository);
        rules::descendant_bk_set::set_descendant_bk_set(block_state, repository);
    }
    tracing::trace!("Process block candidate: check block signature");

    // TODO: do we have to check ackinacki block hash, tvm block hash and tvm block seq_no here?
    // candidate_block.data().check_hash()

    if !block_state.guarded(|e| e.is_stored()) {
        return Ok(());
    }
    let candidate_block =
        repository.get_block(&block_id)?.expect("Block must be in the repository");

    if let Some(status) = verify_all_block_signatures(
        block_state_repository,
        &candidate_block,
        block_state,
        skipped_attestation_ids,
    ) {
        if !status {
            tracing::trace!("Process block candidate: blocks signature is invalid, invalidate it");
            block_state.guarded_mut(|e| e.set_invalidated())?;
            return Ok(());
        }
        if !block_state.guarded(|e| e.is_signatures_verified()) {
            block_state.guarded_mut(|e| e.set_signatures_verified())?;
        }
    } else {
        tracing::trace!("Process block candidate: can't check block signature, skip it");
        return Ok(());
    }
    block_state.guarded_mut(|e| -> anyhow::Result<()> {
        if e.producer().is_none() {
            e.set_producer(candidate_block.data().get_common_section().producer_id.clone())?;
        }
        Ok(())
    })?;

    if block_state.guarded(|e| e.block_stats().is_none() && e.bk_set().is_some()) {
        let parent_block_state = block_state_repository.get(&parent_id)?;
        if let Some(parent_stats) = parent_block_state.guarded(|e| e.block_stats().clone()) {
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
                        att.data().block_id.clone(),
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
                let p: f64 = 0.000000001;
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
                    e.set_initial_attestations_target((
                        // Note: spec.
                        1 + median_descendants_chain_length_to_meet_threshold,
                        attestation_target,
                    ))?;
                }

                e.set_block_stats(cur_block_stats.clone())?;

                Ok(())
            })?;
        }
    }

    verify_fork_resolutions(block_state, &candidate_block)?;

    // TODO: need to check attestations from common section and pass them to the attestation processor or attestation target service
    // TODO: need to check acks and nacks from common section

    if block_state.guarded(|e| e.has_all_cross_thread_ref_data_available().is_none()) {
        tracing::trace!("Process block candidate: check cross thread ref data for block refs");

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
        block_state.guarded_mut(|e| e.set_has_all_cross_thread_ref_data_available())?;
    }

    if !block_state.guarded(|e| e.is_block_already_applied()) {
        tracing::trace!("Process block candidate: apply block");
        let parent_block_state = block_state_repository.get(&parent_id)?;
        if parent_block_state.guarded(|e| e.is_block_already_applied()) {
            let thread_id = block_state.guarded(|e| *e.thread_identifier()).expect("Must be set");
            let (_, last_finalized_seq_no) =
                repository.select_thread_last_finalized_block(&thread_id)?;
            // On the moment of block apply and block append block must have valid chain to the last finalized
            if let Err(e) = block_state_repository
                .select_unfinalized_ancestor_blocks(block_state.clone(), last_finalized_seq_no)
            {
                tracing::error!(
                    "Failed to build chain from the candidate block to the last finalized: {e}"
                );
                block_state.guarded_mut(|e| e.set_invalidated())?;
                return Ok(());
            }

            let mut optimistic_state = repository
                .get_optimistic_state(&parent_id, nack_set_cache.clone())?
                .expect("Failed to load state for parent block");
            if let Err(e) = optimistic_state.apply_block(
                candidate_block.data(),
                shared_services,
                block_state_repository.clone(),
                nack_set_cache,
            ) {
                tracing::error!("Failed to apply candidate block: {e}");
                block_state.guarded_mut(|e| e.set_invalidated())?;
                return Ok(());
            }

            let cross_thread_ref_data = CrossThreadRefData::from_ackinacki_block(
                candidate_block.data(),
                &mut optimistic_state,
            )?;
            if let Some(archive) = archive.clone() {
                write_to_db(
                    archive,
                    candidate_block.clone(),
                    optimistic_state.shard_state.shard_state.clone(),
                    optimistic_state.shard_state.shard_state_cell.clone(),
                )?;
            }
            let common_section = candidate_block.data().get_common_section().clone();
            let must_save_state = common_section.directives.share_state_resource_address.is_some()
                || candidate_block.data().is_thread_splitting()
                || (candidate_block.data().seq_no() % SAVE_STATE_FREQUENCY == 0);
            if must_save_state {
                repository.store_optimistic(optimistic_state)?;
            }

            let bk_set_update = block_state.guarded_mut(|e| {
                e.set_applied()?;
                Ok::<_, anyhow::Error>(e.bk_set().as_ref().map(|x| BlockKeeperSetUpdate {
                    node_ids: x.iter_node_ids().map(|x| x.to_string()).collect(),
                }))
            })?;
            bk_set_update.map(|x| bk_set_update_tx.send(x));
            shared_services.on_block_appended(candidate_block.data());
            shared_services.exec(|e| {
                e.cross_thread_ref_data_service.set_cross_thread_ref_data(cross_thread_ref_data)
            })?;
        }
    }

    Ok(())
}

fn verify_fork_resolutions(
    block_state: &BlockState,
    candidate_block: &Envelope<GoshBLS, AckiNackiBlock>,
) -> anyhow::Result<()> {
    if block_state.guarded(|e| e.resolves_forks().is_some()) {
        return Ok(());
    }
    let fork_resolutions = candidate_block.data().get_common_section().fork_resolutions.clone();
    for _fork_resolution in fork_resolutions.iter() {
        // TODO: verify fork resolution
        // set block invalid if any single resolution is invalid.
        // send Nack for the block (invalid fork resolution)
    }
    block_state.guarded_mut(|e| {
        if e.resolves_forks().is_none() {
            e.set_resolves_forks(fork_resolutions)
        } else {
            Ok(())
        }
    })
}

// Function verifies block signatures and attestation signatures from common section
pub(crate) fn verify_all_block_signatures(
    block_state_repository: &BlockStateRepository,
    candidate_block: &Envelope<GoshBLS, AckiNackiBlock>,
    block_state: &BlockState,
    skipped_attestation_ids: &Arc<Mutex<HashSet<BlockIdentifier>>>,
) -> Option<bool> {
    // TODO: verify acks and nacks in the common section
    let bk_set = block_state.guarded(|e| e.bk_set().clone())?;
    let signers_map = bk_set.get_pubkeys_by_signers();
    tracing::trace!(
        "Signature verification start: block: {}, signers: {:?}",
        candidate_block,
        signers_map
    );
    if let Some(is_producer_signature_valid) =
        block_state.guarded(|e| *e.envelope_block_producer_signature_verified())
    {
        if !is_producer_signature_valid {
            return Some(false);
        }
    } else {
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
    let mut is_all_verified = true;
    let skipped_attestation_ids = skipped_attestation_ids.lock();
    for attestation in candidate_block.data().get_common_section().block_attestations.iter() {
        let Ok(parent_block_state) = block_state_repository.get(&attestation.data().block_id)
        else {
            continue;
        };
        if skipped_attestation_ids.contains(&attestation.data().block_id) {
            continue;
        }
        let (is_parent_invalidated, attestation_signers_map) =
            parent_block_state.guarded(|e| (e.is_invalidated(), e.bk_set().clone()));
        if is_parent_invalidated {
            continue;
        }
        let Some(attestation_signers_map) = attestation_signers_map else {
            is_all_verified = false;
            continue;
        };
        let attestation_signers_map = attestation_signers_map.get_pubkeys_by_signers();
        let is_attestation_signatures_valid = attestation
            .verify_signatures(&attestation_signers_map)
            .expect("Attestation signatures verification should not crash.");
        if !is_attestation_signatures_valid {
            tracing::trace!("Attestations signature verification failed: {}", candidate_block);
            return Some(false);
        }
    }
    let mut is_all_success = true;
    if is_all_verified {
        let attestations_map =
            candidate_block.data().get_common_section().block_attestations.as_signatures_map();
        for (k, v) in attestations_map.into_iter() {
            if block_state.guarded_mut(|e| e.add_verified_attestations_for(k, v).is_err()) {
                is_all_success = false;
            }
        }
    }
    if is_all_success {
        Some(true)
    } else {
        None
    }
}
