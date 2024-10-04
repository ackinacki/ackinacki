// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block::Block;
use crate::block::BlockSeqNo;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockFor;
use crate::node::associated_types::BlockIdentifierFor;
use crate::node::associated_types::BlockSeqNoFor;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticForwardState;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::associated_types::ThreadIdentifierFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;

impl<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
Node<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBLSSignatureScheme: BLSSignatureScheme<PubKey = PubKey> + Clone,
        <TBLSSignatureScheme as BLSSignatureScheme>::PubKey: PartialEq,
        TBlockProducerProcess:
        BlockProducerProcess<Block = BlockFor<TBlockProducerProcess>, Repository = TRepository>,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block:
        Block<BlockIdentifier = BlockIdentifierFor<TBlockProducerProcess>>,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block:
        Block<BLSSignatureScheme = TBLSSignatureScheme>,
        <<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block as Block>::BlockSeqNo:
        Eq + Hash,
        ThreadIdentifierFor<TBlockProducerProcess>: Default,
        BlockFor<TBlockProducerProcess>: Clone + Display,
        BlockIdentifierFor<TBlockProducerProcess>: Serialize + for<'de> Deserialize<'de>,
        TValidationProcess: BlockKeeperProcess<
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            Block = BlockFor<TBlockProducerProcess>,
            BlockSeqNo = BlockSeqNoFor<TBlockProducerProcess>,
            BlockIdentifier = BlockIdentifierFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            Block = BlockFor<TBlockProducerProcess>,
        >,
        TRepository: Repository<
            BLS = TBLSSignatureScheme,
            EnvelopeSignerIndex = SignerIndex,
            ThreadIdentifier = ThreadIdentifierFor<TBlockProducerProcess>,
            Block = BlockFor<TBlockProducerProcess>,
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>,
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Block: From<<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block>,
        TRandomGenerator: rand::Rng,
{
    pub(crate) fn start_block_production(
        &mut self,
    ) -> anyhow::Result<
        Vec<(
            ThreadIdentifierFor<TBlockProducerProcess>,
            BlockIdentifierFor<TBlockProducerProcess>,
            BlockSeqNoFor<TBlockProducerProcess>,
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
            if self.is_this_node_a_producer_for(&thread_id, &block_seq_no_to_continue.next()) && self.is_this_node_in_block_keeper_set(&block_seq_no_to_continue.next()) {
                producer_tails.push((
                    thread_id.clone(),
                    block_id_to_continue.clone(),
                    block_seq_no_to_continue,
                ));
                log::info!(
                    "Requesting producer to continue block id {:?}; seq_no {:?}, node_id {:?}",
                    block_id_to_continue,
                    block_seq_no_to_continue,
                    self.config.local.node_id,
                );

                self.cache_forward_optimistic.insert(
                    thread_id.clone(),
                    OptimisticForwardState::ProducingNextBlock(
                        block_id_to_continue.clone(),
                        block_seq_no_to_continue,
                    ),
                );
                self.production_process.start_thread_production(&thread_id, &block_id_to_continue)?;
            }
        }
        Ok(producer_tails)
    }

    pub(crate) fn on_production_timeout(
        &mut self,
        producer_tails: &mut [(
            ThreadIdentifierFor<TBlockProducerProcess>,
            BlockIdentifierFor<TBlockProducerProcess>,
            BlockSeqNoFor<TBlockProducerProcess>,
        )],
        share_resulting_state: &mut Option<BlockSeqNoFor<TBlockProducerProcess>>,
        share_producer_group: &mut bool,
    ) -> anyhow::Result<bool> {
        tracing::trace!("on_production_timeout start");
        let mut did_produce_something = false;
        for (thread_id, _parent_block_id, _parent_block_seq_no) in producer_tails.iter() {
            let produced_data = self.production_process.get_produced_blocks(thread_id);
            did_produce_something = !produced_data.is_empty();
            for (mut block, optimistic_state, external_messages_to_erase_count) in produced_data {
                log::info!(
                    "Got block from producer id: {:?}; seq_no: {:?}, parent: {:?}, external_messages_to_erase_count: {}",
                    block.identifier(),
                    block.seq_no(),
                    block.parent(),
                    external_messages_to_erase_count,
                );
                self.update_block_keeper_set_from_common_section(&block)?;
                let share_state_address = if let Some(seq_no) = *share_resulting_state {
                    if seq_no == block.seq_no() {
                        *share_resulting_state = None;
                        let serialized_state = optimistic_state.serialize()?;
                        let producer_group = self.get_latest_producer_groups_for_all_threads();
                        let block_keeper_set = {
                            self.block_keeper_ring_pubkeys.lock().clone()
                        };
                        let state_snapshot = self.repository.convert_state_data_to_snapshot(
                            serialized_state,
                            producer_group,
                            block_keeper_set,
                        )?.into();

                        // Start share state task in a separate thread
                        let resource_address = self.state_sync_service.add_share_state_task(state_snapshot)?;

                        Some(resource_address)
                    } else {
                        None
                    }
                } else {
                    None
                };
                let block_will_share_state = share_state_address.is_some();
                self.update_candidate_common_section(&mut block, share_state_address, *share_producer_group)?;
                *share_producer_group = false;

                if block_will_share_state || block.seq_no().into() % self.config.global.save_state_frequency == 0 {
                    self.repository.store_optimistic(optimistic_state.clone())?;
                }
                let signature =
                    <TBLSSignatureScheme as BLSSignatureScheme>::sign(&self.secret, &block)?;
                let mut signature_occurrences = HashMap::new();
                let self_signer_index =
                    self.get_node_signer_index_for_block_seq_no(&self.config.local.node_id, &block.seq_no());
                signature_occurrences.insert(self_signer_index, 1);
                let envelope = <Self as NodeAssociatedTypes>::CandidateBlock::create(
                    signature,
                    signature_occurrences,
                    block.clone(),
                );

                let block_id = envelope.data().identifier();
                let block_seq_no = envelope.data().seq_no();
                self.repository.store_block(envelope.clone()).unwrap();

                self.production_process.write_block_to_db(
                    envelope.clone(),
                    optimistic_state,
                )?;
                match self.cache_forward_optimistic.get(thread_id) {
                    None | Some(OptimisticForwardState::None) => {
                        self.cache_forward_optimistic.insert(
                            thread_id.clone(),
                            OptimisticForwardState::ProducedBlock(block_id, block_seq_no),
                        );
                    }
                    Some(&OptimisticForwardState::ProducingNextBlock(
                        ref _cached_parent_id,
                        _cached_parent_seq_no,
                    )) => {
                        self.cache_forward_optimistic.insert(
                            thread_id.clone(),
                            OptimisticForwardState::ProducedBlock(block_id, block_seq_no),
                        );
                    }
                    Some(OptimisticForwardState::ProducedBlock(_, _)) => {
                        self.cache_forward_optimistic.insert(
                            thread_id.clone(),
                            OptimisticForwardState::ProducedBlock(block_id, block_seq_no),
                        );
                    }
                    // _ => {}
                }

                let block_seq_no = envelope.data().seq_no();
                let (_last_finalized_block_id, last_finalized_seq_no) =
                    self.repository.select_thread_last_finalized_block(thread_id)?;
                log::trace!("last_finalized_seq_no={last_finalized_seq_no:?}");
                if (block_seq_no.into() - last_finalized_seq_no.into()) > self.config.global.finalization_delay_to_stop {
                    log::trace!(
                        "Finalization delay is too high ({} > {}), stop production.",
                        block_seq_no.into() - last_finalized_seq_no.into(),
                        self.config.global.finalization_delay_to_stop
                    );
                    self.production_timeout_multiplier = 0;
                } else if (block_seq_no.into() - last_finalized_seq_no.into())
                    > self.config.global.finalization_delay_to_slow_down
                {
                    log::trace!(
                        "Finalization delay is too high ({} > {}), slow down production.",
                        block_seq_no.into() - last_finalized_seq_no.into(),
                        self.config.global.finalization_delay_to_slow_down
                    );
                    self.production_timeout_multiplier = self.config.global.slow_down_multiplier;
                    self.production_process.set_timeout(self.get_production_timeout());
                }
                self.repository.delete_external_messages(external_messages_to_erase_count)?;
                self.clear_block_gap(thread_id);
                if !self.is_this_node_in_block_keeper_set(&envelope.data().seq_no()) {
                    self.production_process.stop_thread_production(thread_id)?;
                }
                self.broadcast_candidate_block(envelope)?;
            }
        }
        Ok(did_produce_something)
    }

    fn update_candidate_common_section(
        &mut self,
        candidate_block: &mut BlockFor<TBlockProducerProcess>,
        share_state_address: Option<<TStateSyncService as StateSyncService>::ResourceAddress>,
        share_producer_group: bool,
    ) -> anyhow::Result<()> {
        tracing::trace!("update_candidate_common_section: share_state {} block_seq_no: {:?}, attestations_len: {}, id: {:?}", share_state_address.is_some(),  candidate_block.seq_no(), self.last_block_attestations.len(), candidate_block.identifier());
        if share_producer_group {
            if let Some(attestations) =  self.sent_attestations.remove(&ThreadIdentifierFor::<TBlockProducerProcess>::default()) {
                for (_, attestation) in attestations {
                    self.last_block_attestations.push(attestation);
                }
            }
        }
        let aggregated_attestations = self.aggregate_attestations()?;
        let aggregated_acks = self.aggregate_acks()?;
        let aggregated_nacks = self.aggregate_nacks()?;
        let mut common_section = candidate_block.get_common_section();
        common_section.block_attestations = aggregated_attestations;
        common_section.acks = aggregated_acks;
        common_section.nacks = aggregated_nacks;
        common_section.producer_group = self.get_latest_producer_group(&ThreadIdentifierFor::<TBlockProducerProcess>::default());

        if let Some(resource_address) = share_state_address {
            let directive = serde_json::to_string(&resource_address)?;
            tracing::trace!("Set share state directive for block {:?} {:?}: {directive}", candidate_block.seq_no(), candidate_block.identifier());
            common_section.directives.share_state_resource_address = Some(directive);
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
    fn aggregate_attestations(&mut self) -> anyhow::Result<Vec<Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>>> {
        let mut aggregated_attestations = HashMap::new();
        tracing::trace!("Aggregate attestations start len: {}", self.last_block_attestations.len());
        for attestation in &self.last_block_attestations {
            let block_id = attestation.data().block_id.clone();
            tracing::trace!("Aggregate attestations block id: {:?}", block_id);
            aggregated_attestations.entry(block_id)
                .and_modify(|envelope: &mut Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>| {
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
                        let merged_aggregated_signature = TBLSSignatureScheme::merge(
                            aggregated_signature,
                            attestation.aggregated_signature(),
                        ).expect("Failed to merge attestations");
                        *envelope = Envelope::<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>::create(
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

    fn aggregate_acks(&mut self) -> anyhow::Result<Vec<<Self as NodeAssociatedTypes>::Ack>> {
        let mut aggregated_acks = HashMap::new();
        tracing::trace!("Aggregate acks start len: {}", self.received_acks.len());
        for ack in &self.received_acks {
            let block_id = ack.data().block_id.clone();
            tracing::trace!("Aggregate acks block id: {:?}", block_id);
            aggregated_acks.entry(block_id)
                .and_modify(|aggregated_ack: &mut <Self as NodeAssociatedTypes>::Ack| {
                    let mut merged_signatures_occurences = aggregated_ack.clone_signature_occurrences();
                    let initial_signatures_count = merged_signatures_occurences.len();
                    let incoming_signature_occurences = ack.clone_signature_occurrences();
                    for signer_index in incoming_signature_occurences.keys() {
                        let new_count = (*merged_signatures_occurences.get(signer_index).unwrap_or(&0))
                            + (*incoming_signature_occurences.get(signer_index).unwrap());
                        merged_signatures_occurences.insert(*signer_index, new_count);
                    }
                    merged_signatures_occurences.retain(|_k, count| *count > 0);

                    if merged_signatures_occurences.len() > initial_signatures_count {
                        let aggregated_signature = aggregated_ack.aggregated_signature();
                        let merged_aggregated_signature = TBLSSignatureScheme::merge(
                            aggregated_signature,
                            ack.aggregated_signature(),
                        ).expect("Failed to merge attestations");
                        *aggregated_ack = <Self as NodeAssociatedTypes>::Ack::create(
                            merged_aggregated_signature,
                            merged_signatures_occurences,
                            aggregated_ack.data().clone(),
                        );
                    }
                })
                .or_insert(ack.clone());
        }
        tracing::trace!("Aggregate acks result len: {:?}", aggregated_acks.len());
        self.received_acks.clear();
        Ok(aggregated_acks.values().cloned().collect())
    }

    fn aggregate_nacks(&mut self) -> anyhow::Result<Vec<<Self as NodeAssociatedTypes>::Nack>> {
        let mut aggregated_nacks = HashMap::new();
        tracing::trace!("Aggregate nacks start len: {}", self.received_nacks.len());
        for nack in &self.received_nacks {
            let block_id = nack.data().block_id.clone();
            tracing::trace!("Aggregate nacks block id: {:?}", block_id);
            aggregated_nacks.entry(block_id)
                .and_modify(|aggregated_nack: &mut <Self as NodeAssociatedTypes>::Nack| {
                    let mut merged_signatures_occurences = aggregated_nack.clone_signature_occurrences();
                    let initial_signatures_count = merged_signatures_occurences.len();
                    let incoming_signature_occurences = nack.clone_signature_occurrences();
                    for signer_index in incoming_signature_occurences.keys() {
                        let new_count = (*merged_signatures_occurences.get(signer_index).unwrap_or(&0))
                            + (*incoming_signature_occurences.get(signer_index).unwrap());
                        merged_signatures_occurences.insert(*signer_index, new_count);
                    }
                    merged_signatures_occurences.retain(|_k, count| *count > 0);

                    if merged_signatures_occurences.len() > initial_signatures_count {
                        let aggregated_signature = aggregated_nack.aggregated_signature();
                        let merged_aggregated_signature = TBLSSignatureScheme::merge(
                            aggregated_signature,
                            nack.aggregated_signature(),
                        ).expect("Failed to merge attestations");
                        *aggregated_nack = <Self as NodeAssociatedTypes>::Nack::create(
                            merged_aggregated_signature,
                            merged_signatures_occurences,
                            aggregated_nack.data().clone(),
                        );
                    }
                })
                .or_insert(nack.clone());
        }
        tracing::trace!("Aggregate nacks result len: {:?}", aggregated_nacks.len());
        self.received_nacks.clear();
        Ok(aggregated_nacks.values().cloned().collect())
    }
}
