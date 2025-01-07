// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticForwardState;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::GoshBLS;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::cross_thread_ref_repository::CrossThreadRefDataHistory;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;
use crate::types::next_seq_no;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

impl<TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
Node<TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBlockProducerProcess:
        BlockProducerProcess< Repository = TRepository>,
        TValidationProcess: BlockKeeperProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
        >,
        TRepository: Repository<
            BLS = GoshBLS,
            EnvelopeSignerIndex = SignerIndex,

            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<GoshBLS, AttestationData>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = TRepository
        >,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<GoshBLS, AttestationData>,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
        >,
        TRandomGenerator: rand::Rng,
{
    pub(crate) fn start_block_production(
        &mut self,
    ) -> anyhow::Result<
        Vec<(
            ThreadIdentifier,
            BlockIdentifier,
            BlockSeqNo,
        )>,
    > {
        tracing::trace!("start_block_production");
        // Produce whatever threads it has to produce
        let mut producer_tails = vec![];
        for thread_id in self.list_threads()? {
            // TODO: this operation takes too much time and in case of non producers we simply lose time
            // let (block_id_to_continue, block_seq_no_to_continue) =
            //     self.find_thread_last_block_id_this_node_can_continue(&thread_id)?;
            // let next_thread_block_seq_no = block_seq_no_to_continue.next();
            // if self.is_this_node_a_producer_for(&thread_id, &next_thread_block_seq_no) {

            let (block_id_to_continue, block_seq_no_to_continue) =
                self.find_thread_last_block_id_this_node_can_continue(&thread_id)?;
            if self.is_this_node_a_producer_for(&thread_id, &next_seq_no(block_seq_no_to_continue)) && self.is_this_node_in_block_keeper_set(&next_seq_no(block_seq_no_to_continue), &thread_id) {
                producer_tails.push((
                    thread_id,
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
                self.cache_forward_optimistic.insert(
                    thread_id,
                    OptimisticForwardState::ProducingNextBlock(
                        block_id_to_continue.clone(),
                        block_seq_no_to_continue,
                    ),
                );
                self.production_process.start_thread_production(
                    &thread_id,
                    &block_id_to_continue,
                    Arc::clone(&self.received_acks),
                    Arc::clone(&self.received_nacks),
                    self.block_keeper_sets.clone(),
                    Arc::clone(&self.nack_set_cache),
                )?;
            //    self.received_acks.clear();
            //    self.received_nacks.clear();
            }
        }
        Ok(producer_tails)
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn on_production_timeout(
        &mut self,
        producer_tails: &mut Vec<(
            ThreadIdentifier,
            BlockIdentifier,
            BlockSeqNo,
        )>,
        share_resulting_state: &mut Option<BlockSeqNo>,
        share_producer_group: &mut bool,
    ) -> anyhow::Result<bool> {
        tracing::trace!("on_production_timeout start");
        let mut did_produce_something = false;
        let mut indexes_to_remove = vec![];
        for index in 0..producer_tails.len() {
            let thread_id = producer_tails[index].0;

            let mut produced_data = self.production_process.get_produced_blocks(&thread_id);
            produced_data.sort_by(|a, b| a.0.seq_no().cmp(&b.0.seq_no()));

            // While producer process was generating blocks, the node could receive valid
            // blocks, and the generated blocks could be discarded.
            // Get minimal block seq no that can be accepted from producer process.
            let minimal_seq_no_that_can_be_accepted_from_producer_process =
                self.repository.select_thread_last_finalized_block(&thread_id)?.1;
            tracing::trace!("on_production_timeout: minimal_seq_no_that_can_be_accepted_from_producer_process = {minimal_seq_no_that_can_be_accepted_from_producer_process:?}");

            for (mut block, optimistic_state, external_messages_to_erase_count) in produced_data {
                tracing::info!(
                    "Got block from producer id: {:?}; seq_no: {:?}, parent: {:?}, external_messages_to_erase_count: {}",
                    block.identifier(),
                    block.seq_no(),
                    block.parent(),
                    external_messages_to_erase_count,
                );

                if block.seq_no() <= minimal_seq_no_that_can_be_accepted_from_producer_process {
                    tracing::trace!("Produced block is older than last finalized block. Stop production for thread: {thread_id:?}");
                    self.production_process.stop_thread_production(&thread_id)?;
                    indexes_to_remove.push(index);
                    break;
                }
                did_produce_something = true;
                self.shared_services.on_block_appended(&block);
                let share_state_address = if let Some(seq_no) = *share_resulting_state {
                    if seq_no == block.seq_no() {
                        tracing::trace!("Node should share state for last block");
                        *share_resulting_state = None;
                        let producer_group = self.get_latest_producer_groups_for_all_threads();
                        let block_keeper_sets = self.get_block_keeper_sets_for_all_threads();

                        let cross_thread_ref_data_history = self.shared_services.exec(|e| -> anyhow::Result<Vec<CrossThreadRefData>> {
                            e.cross_thread_ref_data_service.get_history_tail(&block.identifier())
                        })?;
                        // Start share state task in a separate thread
                        let resource_address = self.state_sync_service.add_share_state_task(
                            optimistic_state.clone(),
                            producer_group,
                            block_keeper_sets,
                            cross_thread_ref_data_history
                        )?;

                        Some(resource_address)
                    } else {
                        None
                    }
                } else {
                    None
                };
                let block_will_share_state = share_state_address.is_some();
                self.update_candidate_common_section(&mut block, share_state_address, *share_producer_group, &optimistic_state)?;
                *share_producer_group = false;
                let must_save_state = block_will_share_state
                    || block.is_thread_splitting()
                    || (block.seq_no() % self.config.global.save_state_frequency == 0);
                if must_save_state {
                    self.repository.store_optimistic(optimistic_state.clone())?;
                }
                let signature =
                    <GoshBLS as BLSSignatureScheme>::sign(&self.secret, &block)?;
                let mut signature_occurrences = HashMap::new();
                let self_signer_index =
                    self.get_node_signer_index_for_block_seq_no(&block.seq_no()).expect("BP must have valid signer index");
                signature_occurrences.insert(self_signer_index, 1);
                let envelope = <Self as NodeAssociatedTypes>::CandidateBlock::create(
                    signature,
                    signature_occurrences,
                    block.clone(),
                );

                // Check if this node has already signed block of the same height
                if self.does_this_node_have_signed_block_of_the_same_height(&envelope)? {
                    tracing::trace!("Don't accept produced block because this node has already signed a block of the same height");
                    self.production_process.stop_thread_production(&thread_id)?;
                    self.production_timeout_multiplier = 0;
                    return Ok(false);
                }

                let block_id = envelope.data().identifier();
                let block_seq_no = envelope.data().seq_no();
                self.repository.store_block(envelope.clone())?;
                self.repository.mark_block_as_processed(&block_id)?;
                self.repository.mark_block_as_verified(&block_id)?;

                self.production_process.write_block_to_db(
                    envelope.clone(),
                    optimistic_state,
                )?;
                match self.cache_forward_optimistic.get(&thread_id) {
                    None | Some(OptimisticForwardState::None) => {
                        tracing::trace!("insert to cache_forward_optimistic {:?} {:?}", block_seq_no, block_id);
                        self.cache_forward_optimistic.insert(
                            thread_id,
                            OptimisticForwardState::ProducedBlock(block_id, block_seq_no),
                        );
                    }
                    Some(&OptimisticForwardState::ProducingNextBlock(
                        ref _cached_parent_id,
                        _cached_parent_seq_no,
                    )) => {
                        tracing::trace!("insert to cache_forward_optimistic {:?} {:?}", block_seq_no, block_id);
                        self.cache_forward_optimistic.insert(
                            thread_id,
                            OptimisticForwardState::ProducedBlock(block_id, block_seq_no),
                        );
                    }
                    Some(OptimisticForwardState::ProducedBlock(_, _)) => {
                        tracing::trace!("insert to cache_forward_optimistic {:?} {:?}", block_seq_no, block_id);
                        self.cache_forward_optimistic.insert(
                            thread_id,
                            OptimisticForwardState::ProducedBlock(block_id, block_seq_no),
                        );
                    }
                    // _ => {}
                }

                let block_seq_no = envelope.data().seq_no();
                let (_last_finalized_block_id, last_finalized_seq_no) =
                    self.repository.select_thread_last_finalized_block(&thread_id)?;
                tracing::trace!("last_finalized_seq_no={last_finalized_seq_no:?}");
                if (block_seq_no - last_finalized_seq_no) > self.config.global.finalization_delay_to_stop {
                    tracing::trace!(
                        "Finalization delay is too high ({} > {}), stop production.",
                        block_seq_no - last_finalized_seq_no,
                        self.config.global.finalization_delay_to_stop
                    );
                    self.production_timeout_multiplier = 0;
                } else if (block_seq_no - last_finalized_seq_no)
                    > self.config.global.finalization_delay_to_slow_down
                {
                    tracing::trace!(
                        "Finalization delay is too high ({} > {}), slow down production.",
                        block_seq_no - last_finalized_seq_no,
                        self.config.global.finalization_delay_to_slow_down
                    );
                    self.production_timeout_multiplier = self.config.global.slow_down_multiplier;
                    self.production_process.set_timeout(self.get_production_timeout());
                }
                self.repository.delete_external_messages(external_messages_to_erase_count)?;
                self.clear_block_gap(&thread_id);
                if !self.is_this_node_in_block_keeper_set(&envelope.data().seq_no(), &thread_id) {
                    self.production_process.stop_thread_production(&thread_id)?;
                }
                self.broadcast_candidate_block(envelope)?;
                self.update_block_keeper_set_from_common_section(&block, &thread_id)?;
            }
        }
        for index in indexes_to_remove {
            producer_tails.remove(index);
        }
        Ok(did_produce_something)
    }

    fn update_candidate_common_section(
        &mut self,
        candidate_block: &mut AckiNackiBlock,
        share_state_address: Option<<TStateSyncService as StateSyncService>::ResourceAddress>,
        share_producer_group: bool,
        optimistic_state: &OptimisticStateFor<TBlockProducerProcess>,
    ) -> anyhow::Result<()> {
        tracing::trace!("update_candidate_common_section: share_state {} block_seq_no: {:?}, attestations_len: {}, id: {:?}", share_state_address.is_some(),  candidate_block.seq_no(), self.last_block_attestations.len(), candidate_block.identifier());
        if share_producer_group {
            if let Some(attestations) =  self.sent_attestations.remove(&self.thread_id) {
                for (_, attestation) in attestations {
                    self.last_block_attestations.push(attestation);
                }
            }
        }
        let aggregated_attestations = self.aggregate_attestations()?;
        let mut common_section = candidate_block.get_common_section().clone();
        common_section.block_attestations = aggregated_attestations;
        common_section.producer_group = self.get_latest_producer_group(&self.thread_id);

        if let Some(resource_address) = share_state_address {
            let directive = serde_json::to_string(&resource_address)?;
            tracing::trace!("Set share state directive for block {:?} {:?}: {directive}", candidate_block.seq_no(), candidate_block.identifier());
            common_section.directives.share_state_resource_address = Some(directive);
            common_section.threads_table = Some(optimistic_state.get_produced_threads_table().clone());
        }

        candidate_block.set_common_section(common_section)?;
        Ok(())
    }

    pub(crate) fn get_production_timeout(&self) -> Duration {
        let res =
            Duration::from_millis(self.config.global.time_to_produce_block_millis * self.production_timeout_multiplier);
        tracing::trace!("Production timeout: {res:?}");
        res
    }

    // TODO: unite these functions to one
    fn aggregate_attestations(&mut self) -> anyhow::Result<Vec<Envelope<GoshBLS, AttestationData>>> {
        let mut aggregated_attestations = HashMap::new();
        tracing::trace!("Aggregate attestations start len: {}", self.last_block_attestations.len());
        for attestation in &self.last_block_attestations {
            let block_id = attestation.data().block_id.clone();
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
        self.last_block_attestations.clear();
        Ok(aggregated_attestations.values().cloned().collect())
    }
}
