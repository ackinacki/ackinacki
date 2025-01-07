// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::node::associated_types::AttestationData;
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
    pub(crate) fn is_candidate_block_signed_by_this_node(
        &self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<bool> {
        let block_seq_no = candidate_block.data().seq_no();
        let signature_occurrences = candidate_block.clone_signature_occurrences();
        if let Some(self_signer_index) =
            self.get_node_signer_index_for_block_seq_no(&block_seq_no) {
            return Ok( {
                match signature_occurrences.get(&self_signer_index) {
                    None | Some(0) => false,
                    Some(_count) => true,
                }
            })
        }
        Ok(false)
    }

    pub(crate) fn sign_candidate_block_envelope(
        &self,
        envelope: &mut <Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<()> {
        let block_seq_no = envelope.data().seq_no();
        if let Some(self_signer_index) =
            self.get_node_signer_index_for_block_seq_no(&block_seq_no) {
            envelope.add_signature(&self_signer_index, &self.secret)?;
        }
        Ok(())
    }

    pub(crate) fn get_node_signer_index_for_block_seq_no(
        &self,
        block_seq_no: &BlockSeqNo,
    ) -> Option<SignerIndex> {
        for (seq_no, signer_index) in self.signer_index_map.iter().rev() {
            if seq_no > block_seq_no {
                continue;
            }
            return Some(*signer_index);
        }
        tracing::trace!("Failed to get signer index for block seq no {}", block_seq_no);
        None
    }

    pub(crate) fn block_keeper_ring_signatures_map_for(
        &self,
        seq_no: &BlockSeqNo,
        _thread_id: &ThreadIdentifier,
    ) -> HashMap<SignerIndex, <GoshBLS as BLSSignatureScheme>::PubKey> {
        self.block_keeper_sets.get_block_keeper_pubkeys(seq_no)
    }

    pub(crate) fn block_keeper_set_for(
        &self,
        block_seq_no: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> BlockKeeperSet {
        self.get_block_keeper_set(block_seq_no, thread_id)
    }

    pub(crate) fn is_this_node_in_block_keeper_set(
        &self,
        seq_no: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> bool {
        let block_keeper_set = self.block_keeper_ring_signatures_map_for(seq_no, thread_id);
        let res = {
            if let Some(signer_index) = self.get_node_signer_index_for_block_seq_no(seq_no) {
                block_keeper_set.contains_key(&signer_index)
            } else {
                false
            }
        };
        tracing::trace!("is_this_node_in_block_keeper_set {}",
            res
        );
        res
    }

    pub(crate) fn min_signatures_count_to_accept_broadcasted_state(
        &self,
        _block_seq_no: BlockSeqNo,
    ) -> usize {
        self.config.global.min_signatures_cnt_for_acceptance
    }


    pub(crate) fn check_block_signature(
        &self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> bool {
        let signatures_map =
            self.block_keeper_ring_signatures_map_for(&candidate_block.data().seq_no(), &self.get_block_thread_id(candidate_block).expect("Failed to get thread id for block"));
        let is_valid = candidate_block
            .verify_signatures(&signatures_map)
            .expect("Signatures verification should not crash.");
        if !is_valid {
            tracing::trace!("Signature verification failed: {}", candidate_block);
        }
        is_valid
    }
}
