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
use crate::block_keeper_system::get_block_keeper_ring_pubkeys;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockFor;
use crate::node::associated_types::BlockIdentifierFor;
use crate::node::associated_types::BlockSeqNoFor;
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
    pub(crate) fn is_candidate_block_signed_by_this_node(
        &self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<bool> {
        let block_seq_no = candidate_block.data().seq_no();
        let signature_occurrences = candidate_block.clone_signature_occurrences();
        tracing::trace!("is_candidate_block_signed_by_this_node seq_no: {block_seq_no:?} block_signatures: {signature_occurrences:?}");
        let self_signer_index =
            self.get_node_signer_index_for_block_seq_no(&self.config.local.node_id, &block_seq_no);
        let is_this_block_signed_by_this_node = {
            match signature_occurrences.get(&self_signer_index) {
                None | Some(0) => false,
                Some(_count) => true,
            }
        };
        Ok(is_this_block_signed_by_this_node)
    }

    pub(crate) fn _is_attestation_signed_by_this_node(
        &self,
        attestation: &<Self as NodeAssociatedTypes>::BlockAttestation,
    ) -> anyhow::Result<bool> {
        let block_seq_no = attestation.data().block_seq_no;
        let signature_occurrences = attestation.clone_signature_occurrences();
        tracing::trace!("is_attestation_signed_by_this_node seq_no: {block_seq_no:?} block_signatures: {signature_occurrences:?}");
        let self_signer_index =
            self.get_node_signer_index_for_block_seq_no(&self.config.local.node_id, &block_seq_no);
        let is_this_attestation_signed_by_this_node = {
            match signature_occurrences.get(&self_signer_index) {
                None | Some(0) => false,
                Some(_count) => true,
            }
        };
        Ok(is_this_attestation_signed_by_this_node)
    }

    pub(crate) fn sign_candidate_block_envelope(
        &self,
        envelope: &mut <Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<()> {
        let block_seq_no = envelope.data().seq_no();
        let self_signer_index =
            self.get_node_signer_index_for_block_seq_no(&self.config.local.node_id, &block_seq_no);
        tracing::trace!(
            "Sign candidate block (singer index = {self_signer_index}): {:?}",
            envelope.data().identifier()
        );
        envelope.add_signature(&self_signer_index, &self.secret)?;
        Ok(())
    }

    pub(crate) fn get_node_signer_index_for_block_seq_no(
        &self,
        node_id: &NodeIdentifier,
        _seq_no: &BlockSeqNoFor<TBlockProducerProcess>,
    ) -> SignerIndex {
        // TODO: fix. Stub implementation.
        *node_id as SignerIndex
    }

    pub(crate) fn block_keeper_ring_signatures_map_for(
        &self,
        _seq_no: &BlockSeqNoFor<TBlockProducerProcess>,
    ) -> HashMap<SignerIndex, <TBLSSignatureScheme as BLSSignatureScheme>::PubKey> {
        let block_keeper_ring_pubkeys = self.block_keeper_ring_pubkeys.lock();
        get_block_keeper_ring_pubkeys(&block_keeper_ring_pubkeys)
    }

    pub(crate) fn block_keeper_set_for(
        &self,
        _seq_no: &BlockSeqNoFor<TBlockProducerProcess>,
    ) -> BlockKeeperSet {
        self.block_keeper_ring_pubkeys.lock().clone()
    }

    pub(crate) fn is_this_node_in_block_keeper_set(
        &self,
        seq_no: &BlockSeqNoFor<TBlockProducerProcess>,
    ) -> bool {
        let block_keeper_set = self.block_keeper_ring_signatures_map_for(seq_no);
        block_keeper_set.contains_key(&self.get_node_signer_index_for_block_seq_no(
            &self.config.local.node_id,
            seq_no
        ))
    }

    pub(crate) fn min_signatures_count_to_accept_broadcasted_state(
        &self,
        _block_seq_no: BlockSeqNoFor<TBlockProducerProcess>,
    ) -> usize {
        self.config.global.min_signatures_cnt_for_acceptance
    }


    pub(crate) fn check_block_signature(
        &self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> bool {
        let signatures_map =
            self.block_keeper_ring_signatures_map_for(&candidate_block.data().seq_no());
        let is_valid = candidate_block
            .verify_signatures(&signatures_map)
            .expect("Signatures verification should not crash.");
        if !is_valid {
            tracing::trace!("Signature verification failed: {}", candidate_block);
        } else {
            log::info!(
                "Signatures verified: seq_no: {:?}, id: {:?}",
                candidate_block.data().seq_no(),
                candidate_block.data().identifier()
            );
        }
        is_valid
    }
}
