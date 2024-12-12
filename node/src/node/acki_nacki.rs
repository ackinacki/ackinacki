// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockStatus;
use crate::node::associated_types::NackData;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;

const BROADCAST_ACK_BLOCK_DIFF: u32 = 10;

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
    // Validation process generated stores block validation results, here node takes them and sends
    // Acks and Nacks.
    pub(crate) fn send_acks_and_nacks_for_validated_blocks(&mut self) -> anyhow::Result<()> {
        tracing::trace!("send_acks_and_nacks_for_validated_blocks");
        // Get results from validation process
        let thread_id = self.thread_id;
        let current_bp_id = self.get_latest_block_producer(&thread_id);
        let verified_blocks = self.validation_process.get_verification_results()?;
        {
            let last_state = self.validation_process.get_last_state();
            tracing::trace!("Share state with keeper process: {:?}", last_state.get_block_id());
            self.production_process.add_state_to_cache(thread_id, last_state);
        }
        for (block_id, block_seq_no, result) in verified_blocks {
            let candidate_block = self.repository.get_block_from_repo_or_archive(&block_id)?;
            if self.is_this_node_in_block_keeper_set(&block_seq_no, &thread_id) {
                if result {
                    // If validation succeeded send Ack to BP and save it to cache
                    let block_ack = self.generate_ack(block_id.clone(), block_seq_no)?;
                    self.received_acks.push(block_ack.clone());
                    self.send_ack(current_bp_id, block_ack.clone())?;
                    self.sent_acks.insert(block_seq_no, block_ack);
                    let block_attestation = <Self as NodeAssociatedTypes>::BlockAttestation::create(
                        candidate_block.aggregated_signature().clone(),
                        candidate_block.clone_signature_occurrences(),
                        AttestationData {
                            block_id: candidate_block.data().identifier(),
                            block_seq_no: candidate_block.data().seq_no(),
                        }
                    );
                    self.sent_attestations
                        .entry(thread_id)
                        .or_default()
                        .push((candidate_block.data().seq_no(), block_attestation.clone()));
                    tracing::trace!("Insert to sent attestations: {:?} {:?}", candidate_block.data().seq_no(), block_attestation.data());
                    self.send_block_attestation(current_bp_id, block_attestation)?;
                    self.repository.dump_sent_attestations(self.sent_attestations.clone())?;
                } else {
                    // If validation failed broadcast Nack
                    let block_nack = self.generate_nack(block_id, block_seq_no)?;
                    self.received_nacks.push(block_nack.clone());
                    self.broadcast_nack(block_nack)?;
                    self.rotate_producer_group(&thread_id)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn generate_ack(
        &self,
        block_id: BlockIdentifier,
        block_seq_no: BlockSeqNo
    ) -> anyhow::Result<<Self as NodeAssociatedTypes>::Ack> {
        let ack_data = AckData { block_id, block_seq_no };
        let signature =
            <TBLSSignatureScheme as BLSSignatureScheme>::sign(&self.secret, &ack_data)?;
        let mut signature_occurrences = HashMap::new();
        signature_occurrences.insert(self.config.local.node_id as SignerIndex, 1);

        Ok(<Self as NodeAssociatedTypes>::Ack::create(
            signature,
            signature_occurrences,
            ack_data,
        ))
    }

    pub(crate) fn generate_nack(
        &self,
        block_id: BlockIdentifier,
        block_seq_no: BlockSeqNo
    ) -> anyhow::Result<<Self as NodeAssociatedTypes>::Nack> {
        let nack_data = NackData { block_id, block_seq_no };
        let signature =
            <TBLSSignatureScheme as BLSSignatureScheme>::sign(&self.secret, &nack_data)?;
        let mut signature_occurrences = HashMap::new();
        signature_occurrences.insert(self.config.local.node_id as SignerIndex, 1);

        Ok(<Self as NodeAssociatedTypes>::Nack::create(
            signature,
            signature_occurrences,
            nack_data,
        ))
    }

    pub(crate) fn parse_block_acks_and_nacks(
        &mut self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<BlockStatus> {
        tracing::trace!("parse_block_acks_and_nacks");
        tracing::trace!("sent_acks len: {}", self.sent_acks.len());
        let received_acks = &candidate_block.data().get_common_section().acks;
        tracing::trace!("received_acks len: {}", received_acks.len());
        let keys_ring =
            self.block_keeper_ring_signatures_map_for(&candidate_block.data().seq_no(), &self.get_block_thread_id(candidate_block)?);

        // clear received acks from sent acks
        for ack in received_acks {
            if !ack.verify_signatures(&keys_ring)? {
                return Ok(BlockStatus::BadBlock);
            }
            let sigs = ack.clone_signature_occurrences();
            if sigs.contains_key(&(self.config.local.node_id as SignerIndex)) {
                tracing::trace!("remove ack from cache: {:?}", ack.data());
                self.sent_acks.remove(&ack.data().block_seq_no);
            }
        }
        tracing::trace!("sent_acks after clear len: {}", self.sent_acks.len());

        // If Ack was sent long ago enough and was not added to block, broadcast it and remove from cache
        let keys: Vec<BlockSeqNo> = self.sent_acks.keys().copied().collect();
        for seq_no in keys {
            if seq_no + BROADCAST_ACK_BLOCK_DIFF < candidate_block.data().seq_no() {
                let ack = self.sent_acks.remove(&seq_no).unwrap();
                self.broadcast_ack(ack)?;
            }
        }

        let received_nacks = &candidate_block.data().get_common_section().nacks;
        tracing::trace!("received_nacks len: {}", received_nacks.len());
        for nack in received_nacks {
            if !nack.verify_signatures(&keys_ring)? {
                return Ok(BlockStatus::BadBlock);
            }
            self.on_nack(nack)?;
        }
        Ok(BlockStatus::Ok)
    }

    pub(crate) fn on_ack(&mut self, ack: &<Self as NodeAssociatedTypes>::Ack) -> anyhow::Result<()> {
        tracing::trace!("on_ack {:?}", ack);
        let block_id: &BlockIdentifier = &ack.data().block_id;
        let block_seq_no = ack.data().block_seq_no;
        self.received_acks.push(ack.clone());
        let block = match self.repository.get_block_from_repo_or_archive(block_id) {
            Err(e) => {
                tracing::trace!("ack can't be processed now, save to cache. Error: {e:?}");
                let acks = self.ack_cache.entry(block_seq_no).or_default();
                acks.push(ack.clone());
                return Ok(());
            }
            Ok(block) => block,
        };
        let block_seq_no = block.data().seq_no();
        let signatures_map = self.block_keeper_ring_signatures_map_for(&block_seq_no, &self.get_block_thread_id(&block)?);
        let _is_valid = ack
            .verify_signatures(&signatures_map)
            .expect("Signatures verification should not crash.");
        Ok(())
    }

    pub(crate) fn on_nack(&mut self, nack: &<Self as NodeAssociatedTypes>::Nack) -> anyhow::Result<()> {
        tracing::trace!("on_nack {:?}", nack);
        let block_id: &BlockIdentifier = &nack.data().block_id;
        let block_seq_no = nack.data().block_seq_no;
        self.received_nacks.push(nack.clone());
        let block = match self.repository.get_block_from_repo_or_archive(block_id) {
            Err(e) => {
                tracing::trace!("nack can't be processed now, save to cache. Error: {e:?}");
                let nacks = self.nack_cache.entry(block_seq_no).or_default();
                nacks.push(nack.clone());
                return Ok(());
            }
            Ok(block) => block,
        };
        let block_seq_no = block.data().seq_no();
        let thread_id = &self.get_block_thread_id(&block)?;
        let signatures_map = self.block_keeper_ring_signatures_map_for(&block_seq_no, thread_id);
        let is_valid = nack
            .verify_signatures(&signatures_map)
            .expect("Signatures verification should not crash.");
        if is_valid {
            // TODO: we should not blindly believe and check Nacked block
            let common_section = block.data().get_common_section();
            if self.get_latest_block_producer(&common_section.thread_id) == common_section.producer_id {
                self.rotate_producer_group(&common_section.thread_id)?;
            }
        }
        Ok(())
    }

    pub(crate) fn check_cached_acks_and_nacks(&mut self, last_processed_block: &<Self as NodeAssociatedTypes>::CandidateBlock) -> anyhow::Result<()> {
        // TODO: need to track thread id of the finalized block
        let block_seq_no = last_processed_block.data().seq_no();
        let block_id = last_processed_block.data().identifier();
        let cached_acks = self.ack_cache.get(&block_seq_no).cloned().unwrap_or_default();
        for ack in cached_acks {
            if ack.data().block_id == block_id {
                self.on_ack(&ack)?;
                break;
            }
        }
        let cached_nacks = self.nack_cache.get(&block_seq_no).cloned().unwrap_or_default();
        for nack in cached_nacks {
            if nack.data().block_id == block_id {
                self.on_nack(&nack)?;
                break;
            }
        }
        Ok(())
    }

    pub(crate) fn clear_old_acks_and_nacks(&mut self, finalized_block_seq_no: &BlockSeqNo) -> anyhow::Result<()> {
        let ack_keys: Vec<BlockSeqNo> = self.ack_cache.keys().cloned().collect();
        for key in ack_keys {
            if key <= *finalized_block_seq_no {
                let _ = self.ack_cache.remove(&key);
            }
        }
        let nack_keys: Vec<BlockSeqNo> = self.nack_cache.keys().cloned().collect();
        for key in nack_keys {
            if key <= *finalized_block_seq_no {
                let _ = self.nack_cache.remove(&key);
            }
        }
        self.received_nacks = self.received_nacks.iter().filter(|nack| nack.data().block_seq_no > *finalized_block_seq_no).cloned().collect();
        self.received_acks = self.received_acks.iter().filter(|ack| ack.data().block_seq_no > *finalized_block_seq_no).cloned().collect();
        Ok(())
    }
}
