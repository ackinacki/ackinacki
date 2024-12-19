// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block::producer::DEFAULT_VERIFY_COMPLEXITY;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
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
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;

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
    /// Functions sends block to validation process and returns bool (true if this block will be verified
    pub(crate) fn verify_and_apply_block(&mut self, envelope: <Self as NodeAssociatedTypes>::CandidateBlock) -> anyhow::Result<bool> {
        let block = envelope.data();
        let block_id = block.identifier();
        let thread_id = block.get_common_section().thread_id;
        tracing::info!("verify_and_apply_block: {:?} {:?}", block.seq_no(), &block_id);
        // Verify block
        let is_block_already_verified = self.repository.is_block_verified(&block_id)?;
        if is_block_already_verified
            || self.is_this_node_a_producer_for(
                &thread_id,
                &block.seq_no(),
            )
            || thread_id != self.thread_id
        {
            return Ok(false);
        }

        let complexity = if block.parent() != BlockIdentifier::default() {
            self.repository.get_block(
                &block.parent()
            )?
                .ok_or(
                    anyhow::format_err!("Failed to read parent block from repo: {:?}", block.parent())
                )?
                .data()
                .get_common_section()
                .verify_complexity
        } else {
            DEFAULT_VERIFY_COMPLEXITY
        };
        let do_verify = self.is_verifier(&thread_id, &block_id, complexity);
        if do_verify {
            // TODO: assuming that consensus mod already checked that this block seq_no is
            // greater than last finalized
            tracing::trace!("Verify block {:?} on the node {:?}", block.seq_no(), self.config.local.node_id);
            self
                .validation_process
                .validate(envelope)
                .expect("Block verification should not crash");

            tracing::info!("Block {:?} verified on the node {:?}", block_id, self.config.local.node_id);
        } else {
            tracing::info!("Apply block {:?} candidate on the node {:?}", block.seq_no(), self.config.local.node_id);
            self.validation_process.apply_block(envelope).expect("Block apply should not crash");
        }
        Ok(do_verify)
    }

    pub(crate) fn is_verifier(
        &mut self,
        _thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
        verify_complexity: SignerIndex,
    ) -> bool {
        // TODO: need to check that rng is called consecutively
        let rand_id: SignerIndex = self.block_keeper_rng.gen();
        let block_id_modulus = block_id.not_a_modulus(1<<16) as SignerIndex;
        let cur_complexity = rand_id ^ block_id_modulus;
        tracing::trace!("is_verifier check: cur_complexity={cur_complexity} verify_complexity={verify_complexity}");
        cur_complexity < verify_complexity
    }


}
