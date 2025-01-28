// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticForwardState;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocks;
use crate::node::services::sync::StateSyncService;
use crate::node::GoshBLS;
use crate::node::Node;
use crate::node::DEFAULT_PRODUCTION_TIME_MULTIPLIER;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::as_signatures_map::AsSignaturesMap;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ForkResolution;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

impl<TStateSyncService, TBlockProducerProcess, TRandomGenerator>
Node<TStateSyncService, TBlockProducerProcess, TRandomGenerator>
    where
        TBlockProducerProcess:
        BlockProducerProcess< Repository = RepositoryImpl>,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateImpl,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = RepositoryImpl
        >,
        TRandomGenerator: rand::Rng,
{
    pub(crate) fn start_block_production(
        &mut self,
    ) -> anyhow::Result<
        Option<(
            BlockIdentifier,
            BlockSeqNo,
        )>,
    > {
        tracing::trace!("start_block_production");
        // Produce whatever threads it has to produce
        let mut producer_tails = None;
        if let Some((block_id_to_continue, block_seq_no_to_continue)) =
            self.find_thread_last_block_id_this_node_can_continue()? {
            producer_tails = Some((
                block_id_to_continue.clone(),
                block_seq_no_to_continue,
            ));
            tracing::info!(
                "Requesting producer to continue block id {:?}; seq_no {:?}, node_id {:?}",
                block_id_to_continue,
                block_seq_no_to_continue,
                self.config.local.node_id,
            );
            tracing::trace!("insert to cache_forward_optimistic {:?} {:?}", block_seq_no_to_continue, block_id_to_continue);
            self.cache_forward_optimistic = Some(OptimisticForwardState::ProducingNextBlock(
                block_id_to_continue.clone(),
                block_seq_no_to_continue,
            ));
            self.production_process.start_thread_production(
                &self.thread_id,
                &block_id_to_continue,
                self.feedback_sender.clone(),
                Arc::clone(&self.received_acks),
                Arc::clone(&self.received_nacks),
                self.blocks_states.clone(),
                Arc::clone(&self.nack_set_cache),
            )?;
        }
        Ok(producer_tails)
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn on_production_timeout(
        &mut self,
        producer_tails: &mut Option<(
            BlockIdentifier,
            BlockSeqNo,
        )>,
        share_resulting_state: &mut Option<BlockSeqNo>,
        mut memento: Option<Vec<(AckiNackiBlock, TBlockProducerProcess::OptimisticState, usize)>>,
    ) -> anyhow::Result<(bool, Option<Vec<(AckiNackiBlock, TBlockProducerProcess::OptimisticState, usize)>>)> {
        tracing::trace!("on_production_timeout start");
        let mut did_produce_something = false;

            let mut produced_data = memento.take().unwrap_or_else(||self.production_process.get_produced_blocks(&self.thread_id));
            produced_data.sort_by(|a, b| a.0.seq_no().cmp(&b.0.seq_no()));

            // While producer process was generating blocks, the node could receive valid
            // blocks, and the generated blocks could be discarded.
            // Get minimal block seq no that can be accepted from producer process.
            let minimal_seq_no_that_can_be_accepted_from_producer_process =
                self.repository.select_thread_last_finalized_block(&self.thread_id)?.1;
            tracing::trace!("on_production_timeout: minimal_seq_no_that_can_be_accepted_from_producer_process = {minimal_seq_no_that_can_be_accepted_from_producer_process:?}");
            while !produced_data.is_empty() {
                let (mut block, optimistic_state, external_messages_to_erase_count) = produced_data.first().unwrap().clone();
                tracing::info!(
                    "Got block from producer. id: {:?}; seq_no: {:?}, parent: {:?}, external_messages_to_erase_count: {}",
                    block.identifier(),
                    block.seq_no(),
                    block.parent(),
                    external_messages_to_erase_count,
                );

                if block.seq_no() <= minimal_seq_no_that_can_be_accepted_from_producer_process {
                    tracing::trace!("Produced block is older than last finalized block. Stop production for thread: {:?}", self.thread_id);
                    self.production_process.stop_thread_production(&self.thread_id)?;
                    *producer_tails = None;
                    return Ok((false, None));
                }
                did_produce_something = true;
                self.shared_services.on_block_appended(&block);
                let share_state_address = if let Some(seq_no) = *share_resulting_state {
                    if seq_no == block.seq_no() {
                        tracing::trace!("Node should share state for last block");
                        // Start share state task in a separate thread
                        let resource_address = self.state_sync_service.generate_resource_address(
                            &optimistic_state,
                        )?;

                        Some(resource_address)
                    } else {
                        None
                    }
                } else {
                    None
                };
                let block_will_share_state = share_state_address.is_some();
                if !self.update_candidate_common_section(&mut block, share_state_address, &optimistic_state)? {
                    // Memento
                    tracing::trace!("Failed to update common section 2 times in a row, stop production");
                    self.production_process.stop_thread_production(&self.thread_id)?;
                    *producer_tails = None;
                    return Ok((false, None));
                } else {
                    tracing::trace!("Speed up production");
                    self.production_timeout_multiplier = DEFAULT_PRODUCTION_TIME_MULTIPLIER;
                    self.production_process.set_timeout(self.get_production_timeout());
                    produced_data.remove(0);
                    if let Some(seq_no) = *share_resulting_state {
                        if seq_no <= block.seq_no() {
                            *share_resulting_state = None;
                        }
                    }
                }
                let must_save_state = block_will_share_state
                    || block.is_thread_splitting()
                    || (block.seq_no() % self.config.global.save_state_frequency == 0);
                if must_save_state {
                    self.repository.store_optimistic(optimistic_state.clone())?;
                }

                let parent_state = self.blocks_states.get(&block.parent())?;
                loop {
                    tracing::trace!("loop get parent bk set");
                    let notifications = self.blocks_states.notifications().load(Ordering::Relaxed);
                    if parent_state.guarded(|e| e.descendant_bk_set().is_some()) {
                        break;
                    }
                    if parent_state.guarded(|e| e.is_invalidated()) {
                        // Stop production in this case
                        self.production_process.stop_thread_production(&self.thread_id)?;
                        return Ok((false, None));
                    }
                    atomic_wait::wait(self.blocks_states.notifications(), notifications);
                }
                tracing::trace!("Got parent bk set");
                let bk_set = parent_state.guarded(|e| e.get_descendant_bk_set());

                let Some(bk_data) = bk_set.get_by_node_id(&self.config.local.node_id).cloned() else {
                    tracing::trace!("BP is not in the bk set");
                    self.production_process.stop_thread_production(&self.thread_id)?;
                    return Ok((false, None));
                };
                let secret = self.bls_keys_map.guarded(|map| map.get(&bk_data.pubkey).cloned()).expect("Failed to get signer secret for BP").0;
                let signature =
                    <GoshBLS as BLSSignatureScheme>::sign(&secret, &block)?;
                let mut signature_occurrences = HashMap::new();
                signature_occurrences.insert(bk_data.signer_index, 1);
                let envelope = <Self as NodeAssociatedTypes>::CandidateBlock::create(
                    signature,
                    signature_occurrences,
                    block.clone(),
                );

                // Check if this node has already signed block of the same height
                if self.does_this_node_have_signed_block_of_the_same_height(&envelope)? {
                    tracing::trace!("Don't accept produced block because this node has already signed a block of the same height");
                    self.production_process.stop_thread_production(&self.thread_id)?;
                    self.production_timeout_multiplier = 0;
                    return Ok((false, None));
                }

                // TODO: check whether this block changes BK set
                let producer_selector = parent_state.guarded(|e| e.producer_selector_data().clone()).expect("Failed to get parent block producer selector");
                self.blocks_states.get(&block.identifier())?.guarded_mut(|e| {
                    e.set_validated(true)?;
                    e.set_producer_selector_data(producer_selector.clone())
                })?;

                self.on_incoming_candidate_block(&envelope, None)?;

                let block_id = envelope.data().identifier();
                let block_seq_no = envelope.data().seq_no();

                self.production_process.write_block_to_db(
                    envelope.clone(),
                    optimistic_state,
                )?;
                match self.cache_forward_optimistic {
                    None | Some(OptimisticForwardState::None) => {
                        tracing::trace!("insert to cache_forward_optimistic {:?} {:?}", block_seq_no, block_id);
                        self.cache_forward_optimistic = Some(OptimisticForwardState::ProducedBlock(block_id.clone(), block_seq_no));
                    }
                    Some(OptimisticForwardState::ProducingNextBlock(
                        ref _cached_parent_id,
                        _cached_parent_seq_no,
                    )) => {
                        tracing::trace!("insert to cache_forward_optimistic {:?} {:?}", block_seq_no, block_id);
                        self.cache_forward_optimistic = Some(OptimisticForwardState::ProducedBlock(block_id.clone(), block_seq_no));
                    }
                    Some(OptimisticForwardState::ProducedBlock(_, _)) => {
                        tracing::trace!("insert to cache_forward_optimistic {:?} {:?}", block_seq_no, block_id);
                        self.cache_forward_optimistic = Some(OptimisticForwardState::ProducedBlock(block_id.clone(), block_seq_no));
                    }
                    // _ => {}
                }

                // let block_seq_no = envelope.data().seq_no();
                // let (_last_finalized_block_id, last_finalized_seq_no) =
                //     self.repository.select_thread_last_finalized_block(&thread_id)?;
                // tracing::trace!("last_finalized_seq_no={last_finalized_seq_no:?}");
                // if (block_seq_no - last_finalized_seq_no) > self.config.global.finalization_delay_to_stop {
                //     tracing::trace!(
                //         "Finalization delay is too high ({} > {}), stop production.",
                //         block_seq_no - last_finalized_seq_no,
                //         self.config.global.finalization_delay_to_stop
                //     );
                //     self.production_timeout_multiplier = 0;
                // } else if (block_seq_no - last_finalized_seq_no)
                //     > self.config.global.finalization_delay_to_slow_down
                // {
                //     tracing::trace!(
                //         "Finalization delay is too high ({} > {}), slow down production.",
                //         block_seq_no - last_finalized_seq_no,
                //         self.config.global.finalization_delay_to_slow_down
                //     );
                //     self.production_timeout_multiplier = self.config.global.slow_down_multiplier;
                //     self.production_process.set_timeout(self.get_production_timeout());
                // }
                self.repository.delete_external_messages(external_messages_to_erase_count, &self.thread_id)?;
                self.clear_block_gap();
                if self.is_this_node_in_block_keeper_set(envelope.data().parent()) != Some(true) {
                    self.production_process.stop_thread_production(&self.thread_id)?;
                }
                self.broadcast_candidate_block(envelope)?;
                // TODO: fix
                // self.update_block_keeper_set_from_common_section(&block)?;
            }

        Ok((did_produce_something, None))
    }

    fn update_candidate_common_section(
        &mut self,
        candidate_block: &mut AckiNackiBlock,
        share_state_address: Option<<TStateSyncService as StateSyncService>::ResourceAddress>,
        optimistic_state: &OptimisticStateFor<TBlockProducerProcess>,
    ) -> anyhow::Result<bool> {
        tracing::trace!("update_candidate_common_section: share_state {} block_seq_no: {:?}, attestations_len: {}, id: {:?}", share_state_address.is_some(),  candidate_block.seq_no(), self.last_block_attestations.len(), candidate_block.identifier());
        let (_, thread_last_finalized_block_seq_no) =
            self.repository.select_thread_last_finalized_block(&self.thread_id)?;

        let parent_block_state = self.blocks_states.get(&candidate_block.parent())?;
        let Ok(unfinalized_ancestor_blocks) = self.blocks_states.select_unfinalized_ancestor_blocks(
            parent_block_state.clone(),
            thread_last_finalized_block_seq_no
        ).map(|blocks| blocks.into_iter()
            .map(|e| e.guarded(|x|x.block_identifier().clone()))
            .collect::<HashSet::<BlockIdentifier>>())
        else {
            // Need to stop production here
            return Ok(false);
        };
        let (Some(parent_statistics), Some(mut parent_producer_selector), Some(bk_set)) = parent_block_state.guarded(|e|{
            (e.block_stats().clone(),
            e.producer_selector_data().clone(),
            e.descendant_bk_set().clone())
        })
        else {
            return Ok(false);
        };
        let attestations_watched_for_statistics = parent_statistics.attestations_watched();

        let trace_attestations_required = format!("{:?}", unfinalized_ancestor_blocks);
        let mut attestations_required = unfinalized_ancestor_blocks;
        attestations_required.extend(attestations_watched_for_statistics);
        let aggregated_attestations = self.aggregate_attestations(
            attestations_required,
        )?;
        let proposed_attestations = aggregated_attestations.as_signatures_map();
        tracing::trace!(
            "update_candidate_common_section ({}):  waiting for finalization: {}, proposed_attestations: {:?}",
            candidate_block.identifier(),
            trace_attestations_required,
            &proposed_attestations,
        );
        let dependants = self.attestations_target_service.find_next_block_known_dependants(
            candidate_block.parent()
        )?;
        let fork_resolutions: Vec<ForkResolution> = {
            let unprocessed_blocks_cache = self.unprocessed_blocks_cache.lock().clone();
            self.fork_resolution_service.evaluate(unprocessed_blocks_cache);
            dependants.into_iter().filter_map(|e| {
                let dependant_block_extra_attestations = aggregated_attestations.iter().filter(|x| x.data().block_id ==candidate_block.identifier()).cloned().collect::<Vec<_>>();
                self.fork_resolution_service.resolve_fork(e, &dependant_block_extra_attestations)
            }).collect()
        };

        let mut common_section = candidate_block.get_common_section().clone();
        common_section.block_attestations = aggregated_attestations;
        common_section.fork_resolutions = fork_resolutions.clone();
        // TODO: update parent_producer_selector
        // 1 step: update index if we have rotated BP:
        if parent_producer_selector.get_producer_node_id(&bk_set) != self.config.local.node_id {
            let bp_distance_for_this_node = parent_producer_selector.get_distance_from_bp(&bk_set, &self.config.local.node_id).expect("Node must present in BK set");
            // move index
            parent_producer_selector = parent_producer_selector.move_index(bp_distance_for_this_node, bk_set.len());
        }
        common_section.producer_selector = Some(parent_producer_selector);

        if let Some(resource_address) = share_state_address {
            let directive = serde_json::to_string(&resource_address)?;
            tracing::trace!("Set share state directive for block {:?} {:?}: {directive}", candidate_block.seq_no(), candidate_block.identifier());
            common_section.directives.share_state_resource_address = Some(directive);
            common_section.threads_table = Some(optimistic_state.get_produced_threads_table().clone());
        }

        candidate_block.set_common_section(common_section, true)?;

        //
        let res = self.attestations_target_service
            .evaluate_if_next_block_ancestors_required_attestations_will_be_met(
                candidate_block.parent(),
                proposed_attestations,
                &fork_resolutions,
            );
        tracing::trace!("evaluate_if_next_block_ancestors_required_attestations_will_be_met: res:{res:?}");
        if res.is_ok_and(|is_acceptable| is_acceptable)
        {
            self.last_block_attestations.clear();
            Ok(true)
        }
        else
        {
            Ok(false)
        }

    }

    pub(crate) fn get_production_timeout(&self) -> Duration {
        let res =
            Duration::from_millis(self.config.global.time_to_produce_block_millis * self.production_timeout_multiplier);
        tracing::trace!("Production timeout: {res:?}");
        res
    }

    // TODO: unite these functions to one
    fn aggregate_attestations(
        &mut self,
        required: HashSet<BlockIdentifier>,
    ) -> anyhow::Result<Vec<Envelope<GoshBLS, AttestationData>>> {
        let mut aggregated_attestations = HashMap::new();
        tracing::trace!("Aggregate attestations start len: {}", self.last_block_attestations.len());
        for attestation in &self.last_block_attestations {
            let block_id = attestation.data().block_id.clone();
            if !required.contains(&block_id) {
                continue;
            }
            tracing::trace!("Aggregate attestations block id: {:?}", block_id);
            aggregated_attestations.entry(block_id)
                .and_modify(|envelope: &mut Envelope<GoshBLS, AttestationData>| {
                    let mut merged_signatures_occurences = envelope.clone_signature_occurrences();
                    let initial_signatures_count = merged_signatures_occurences.len();
                    let incoming_signature_occurences = attestation.clone_signature_occurrences();
                    for signer_index in incoming_signature_occurences.keys() {
                        let new_count = (*merged_signatures_occurences.get(signer_index).unwrap_or(&0))
                            + (*incoming_signature_occurences.get(signer_index).unwrap());
                        merged_signatures_occurences.insert(*signer_index, new_count);
                    }
                    merged_signatures_occurences.retain(|_k, count| *count > 0);

                    if merged_signatures_occurences.len() > initial_signatures_count {
                        let aggregated_signature = envelope.aggregated_signature();
                        let merged_aggregated_signature = GoshBLS::merge(
                            aggregated_signature,
                            attestation.aggregated_signature(),
                        ).expect("Failed to merge attestations");
                        *envelope = Envelope::<GoshBLS, AttestationData>::create(
                            merged_aggregated_signature,
                            merged_signatures_occurences,
                            envelope.data().clone(),
                        );
                    }
                })
                .or_insert(attestation.clone());
        }
        tracing::trace!("Aggregate attestations result len: {:?}", aggregated_attestations.len());
        self.last_block_attestations = aggregated_attestations.clone().into_values().collect();
        Ok(aggregated_attestations.values().cloned().collect())
    }
}
