// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;

use serde::Deserialize;
use serde::Serialize;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block::Block;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockFor;
use crate::node::associated_types::BlockIdentifierFor;
use crate::node::associated_types::BlockSeqNoFor;
use crate::node::associated_types::BlockStatus;
use crate::node::associated_types::NackData;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::associated_types::ThreadIdentifierFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;

const BROADCAST_ACK_BLOCK_DIFF: u64 = 10;

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
    // Validation process generated stores block validation results, here node takes them and sends
    // Acks and Nacks.
    pub(crate) fn send_acks_and_nacks_for_validated_blocks(&mut self) -> anyhow::Result<()> {
        tracing::trace!("send_acks_and_nacks_for_validated_blocks");
        // Get results from validation process
        let thread_id = ThreadIdentifierFor::<TBlockProducerProcess>::default();
        let current_bp_id = self.get_latest_block_producer(&thread_id);
        let verified_blocks = self.validation_process.get_verification_results()?;
        for (block_id, block_seq_no, result) in verified_blocks {
            let candidate_block = self.repository.get_block_from_repo_or_archive(&block_id)?;
            if self.is_this_node_in_block_keeper_set(&block_seq_no) {
                if result {
                    // If validation succeeded send Ack to BP and save it to cache
                    let block_ack = self.generate_ack(block_id.clone(), block_seq_no)?;
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
                        .entry(thread_id.clone())
                        .or_default()
                        .push((candidate_block.data().seq_no(), block_attestation.clone()));
                    tracing::trace!("Insert to sent attestations: {:?} {:?}", candidate_block.data().seq_no(), block_attestation.data());
                    self.send_block_attestation(current_bp_id, block_attestation)?;
                    self.repository.dump_sent_attestations(self.sent_attestations.clone())?;
                } else {
                    // If validation failed broadcast Nack
                    let block_nack = self.generate_nack(block_id, block_seq_no)?;
                    self.slash_bp_stake(&thread_id, block_seq_no, current_bp_id)?;
                    self.broadcast_nack(block_nack)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn generate_ack(
        &self,
        block_id: BlockIdentifierFor<TBlockProducerProcess>,
        block_seq_no: BlockSeqNoFor<TBlockProducerProcess>
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
        block_id: BlockIdentifierFor<TBlockProducerProcess>,
        block_seq_no: BlockSeqNoFor<TBlockProducerProcess>
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
        let received_acks = candidate_block.data().get_common_section().acks;
        tracing::trace!("received_acks len: {}", received_acks.len());
        let keys_ring =
            self.block_keeper_ring_signatures_map_for(&candidate_block.data().seq_no());

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
        let limit_seq_no = candidate_block.data().seq_no().into() - BROADCAST_ACK_BLOCK_DIFF;
        let keys: Vec<BlockSeqNoFor<TBlockProducerProcess>> = self.sent_acks.keys().copied().collect();
        for seq_no in keys {
            if seq_no.into() < limit_seq_no {
                let ack = self.sent_acks.remove(&seq_no).unwrap();
                self.broadcast_ack(ack)?;
            }
        }

        let received_nacks = candidate_block.data().get_common_section().nacks;
        tracing::trace!("received_nacks len: {}", received_nacks.len());
        for nack in received_nacks {
            if !nack.verify_signatures(&keys_ring)? {
                return Ok(BlockStatus::BadBlock);
            }
            self.on_nack(nack)?;
        }
        Ok(BlockStatus::Ok)
    }
}
