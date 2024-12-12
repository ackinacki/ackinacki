// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockStatus;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;
use crate::types::next_seq_no;
use crate::types::AckiNackiBlock;
use crate::types::BlockSeqNo;

const RESEND_ATTESTATION_BLOCK_DIFF: u32 = 10;

impl<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
Node<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBLSSignatureScheme: BLSSignatureScheme<PubKey = PubKey> + Clone,
        <TBLSSignatureScheme as BLSSignatureScheme>::PubKey: PartialEq,
        TBlockProducerProcess:
        BlockProducerProcess< Repository = TRepository>,
        TValidationProcess: BlockKeeperProcess<
            BLSSignatureScheme = TBLSSignatureScheme,
            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,

            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = TBLSSignatureScheme,
            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,

        >,
        TRepository: Repository<
            BLS = TBLSSignatureScheme,
            EnvelopeSignerIndex = SignerIndex,

            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<TBLSSignatureScheme, AttestationData>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = TRepository
        >,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData>,
            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,
        >,
        TRandomGenerator: rand::Rng,
{
    pub(crate) fn parse_block_attestations(
        &mut self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<BlockStatus> {
        tracing::trace!("parse_block_attestations");
        let thread_id = self.get_block_thread_id(candidate_block)?;
        let mut sent_attestations = self.sent_attestations.entry(thread_id).or_default().clone();
        tracing::trace!("sent_attestations: {:?}", sent_attestations);
        let block_attestations =
            &candidate_block.data().get_common_section()
                .block_attestations;
        tracing::trace!("block_attestations(len): {:?}", block_attestations.len());
        let attestation_limit_seq_no = self.get_attestation_limit_seq_no()?;

        for attestation in block_attestations {
            if !self.check_attestation(attestation)? {
                return Ok(BlockStatus::BadBlock);
            }
        }

        // clean old sent attestations
        for i in (0..sent_attestations.len()).rev() {
            let sent_attestation = &sent_attestations[i].1;
            if sent_attestation.data().block_seq_no <= attestation_limit_seq_no {
                let (_, removed) = sent_attestations.remove(i);
                tracing::trace!("Removed attestation for old block (older than finalized): {:?}", removed.data());
            }
        }

        for block_attestation in block_attestations.iter() {
            tracing::trace!("Processing block attestation: {:?}", block_attestation);
            for i in (0..sent_attestations.len()).rev() {
                let sent_attestation = &sent_attestations[i].1;
                if sent_attestation.data() == block_attestation.data() {
                    let block_attestation_occurrences = block_attestation.clone_signature_occurrences();
                    let incoming_attestation_contains_this_node_signature =
                        block_attestation_occurrences.get(
                            &(self.config.local.node_id as SignerIndex)
                        ).unwrap_or(&0) > &0;

                    if incoming_attestation_contains_this_node_signature {
                        let (_, removed) = sent_attestations.remove(i);
                        tracing::trace!("Removed attestation because it is present in the incoming block: {:?}", removed);
                    }
                }
            }
            self.attestation_processor.process_block_attestation(block_attestation.clone());
            // self.on_incoming_block_attestation(block_attestation)?;
        }
        let current_bp_id = self.get_latest_block_producer(&thread_id);
        tracing::trace!("sent_attestations: {:?}", sent_attestations);
        let limit_seq_no = candidate_block.data().seq_no().saturating_sub(RESEND_ATTESTATION_BLOCK_DIFF);
        for sent_attestation in sent_attestations.iter_mut().rev() {
            let attestation_seq_no = sent_attestation.0;
            if limit_seq_no > attestation_seq_no {
                let attestation = sent_attestation.1.clone();
                tracing::trace!("Resend attestation: {:?}", attestation);
                self.send_block_attestation(current_bp_id, attestation)?;
                sent_attestation.0 = candidate_block.data().seq_no();
            }
        }
        let sent = self.sent_attestations.entry(thread_id).or_default();
        *sent = sent_attestations;
        Ok(BlockStatus::Ok)
    }

    pub(crate) fn check_attestation(&self, attestation: &<Self as NodeAssociatedTypes>::BlockAttestation) -> anyhow::Result<bool> {
        tracing::trace!("Check attestation: {:?}", attestation);
        let AttestationData{ block_id, block_seq_no } = attestation.data().clone();
        let stored_block = match self.repository.get_block_from_repo_or_archive(&block_id) {
            Ok(block) => block,
            Err(_) => {
                // Node does not have block in the repo and archive, so it can't check the
                // attestation. Skip it for now, because node can be started from sync and
                // do not have some blocks.
                return Ok(true);
            }
        };
        let keys_ring =
            self.block_keeper_ring_signatures_map_for(&block_seq_no, &self.get_block_thread_id(&stored_block)?);
        let envelope_with_incoming_signatures =
            <Self as NodeAssociatedTypes>::CandidateBlock::create(
                attestation.aggregated_signature().clone(),
                attestation.clone_signature_occurrences(),
                stored_block.data().clone(),
            );
        let valid = envelope_with_incoming_signatures.verify_signatures(&keys_ring)?;
        if !valid {
            tracing::trace!("Bad attestation");
        }
        Ok(valid)
    }

    pub(crate) fn send_attestations(&mut self) -> anyhow::Result<()> {
        let mut next_attestations_to_send = vec![];
        if let Some((last_sent_attestation_seq_no, last_sent_attestation_time)) = self.last_sent_attestation {
            if last_sent_attestation_time.elapsed().as_millis() >= self.config.global.time_to_produce_block_millis as u128 {
                let next_attestation_seq_no = next_seq_no(last_sent_attestation_seq_no);
                if self.attestations_to_send.contains_key(&next_attestation_seq_no) {
                    next_attestations_to_send = self.attestations_to_send.get(&next_attestation_seq_no).cloned().unwrap();
                }
            }
        } else if !self.attestations_to_send.is_empty() {
            let (_seq_no, attestations) = self.attestations_to_send.first_key_value().unwrap();
            next_attestations_to_send = attestations.clone();
        };
        if !next_attestations_to_send.is_empty() {
            if let Some(chosen_attestation) = self.fork_choice_rule_for_attestations(&next_attestations_to_send)? {
                self.sent_attestations
                    .entry(self.thread_id)
                    .or_default()
                    .push((chosen_attestation.data().block_seq_no, chosen_attestation.clone()));
                self.attestations_to_send.remove(&chosen_attestation.data().block_seq_no);
                tracing::trace!("Insert to sent attestations: {:?}", chosen_attestation.data());
                self.send_block_attestation(self.current_block_producer_id(&self.thread_id, &chosen_attestation.data().block_seq_no), chosen_attestation)?;
                self.repository.dump_sent_attestations(self.sent_attestations.clone())?;
            }
        }
        Ok(())
    }

    pub(crate) fn resend_attestations_on_bp_change(
        &self,
        _block_seq_no: BlockSeqNo,
        bp_id: NodeIdentifier,
    ) -> anyhow::Result<()> {
        if let Some(sent_attestations) = self.sent_attestations.get(&self.thread_id) {
            for (_, attestation) in sent_attestations {
                tracing::trace!("Resend attestation to the new producer: {:?}", attestation);
                self.send_block_attestation(bp_id, attestation.clone())?;
            }
        }
        Ok(())
    }

    pub(crate) fn process_blocks_from_attestation_processor(&mut self) -> anyhow::Result<BlockStatus> {
        let block_from_attestation_processor = self.attestation_processor.get_processed_blocks();
        if !block_from_attestation_processor.is_empty() {
            tracing::trace!("Process block from attestation processor: {}", block_from_attestation_processor.len());
            for block in block_from_attestation_processor {
                let res = self.store_and_accept_candidate_block(block);
                match res {
                    Err(e) =>  {
                        tracing::error!("Failed to process block from attestation processor: {e}")
                    },
                    Ok(BlockStatus::SynchronizationRequired) => {
                        return Ok(BlockStatus::SynchronizationRequired);
                    },
                    _ => {}
                }
            }
            self.try_finalize_blocks()?;
        }
        Ok(BlockStatus::Ok)
    }
}
