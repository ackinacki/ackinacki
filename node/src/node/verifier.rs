// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Display;
use std::hash::Hash;

use serde::Deserialize;
use serde::Serialize;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block::producer::DEFAULT_VERIFY_COMPLEXITY;
use crate::block::Block;
use crate::block::BlockIdentifier;
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
    /// Functions sends block to validation process and returns bool (true if this block will be verified
    pub(crate) fn verify_and_apply_block(&mut self, envelope: <Self as NodeAssociatedTypes>::CandidateBlock) -> anyhow::Result<bool> {
        let block = envelope.data();
        let block_id = block.identifier();
        log::info!("verify_and_apply_block: {:?} {:?}", block.seq_no(), &block_id);
        // Verify block
        // TODO: get thread from block
        let is_block_already_verified = self.repository.is_block_verified(&block_id)?;
        if is_block_already_verified
            || self.is_this_node_a_producer_for(
            &ThreadIdentifierFor::<TBlockProducerProcess>::default(),
            &block.seq_no(),
        )
        {
            return Ok(false);
        }

        let complexity = if block.parent() != BlockIdentifierFor::<TBlockProducerProcess>::default() {
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
        // TODO: change thread id to a proper one
        let do_verify = self.is_verifier(&ThreadIdentifierFor::<TBlockProducerProcess>::default(), &block_id, complexity);
        if do_verify {
            // TODO: assuming that consensus mod already checked that this block seq_no is
            // greater than last finalized
            tracing::trace!("Verify block {:?} on the node {:?}", block.seq_no(), self.config.local.node_id);
            self
                .validation_process
                .validate(envelope)
                .expect("Block verification should not crash");

            log::info!("Block {:?} verified on the node {:?}", block_id, self.config.local.node_id);
        } else {
            log::info!("Apply block {:?} candidate on the node {:?}", block.seq_no(), self.config.local.node_id);
            self.validation_process.apply_block(envelope).expect("Block apply should not crash");
        }
        // Note: for not to apply block several times we mark it as verified
        // despite the fact that the node can be not a verifier
        self.repository.mark_block_as_verified(&block_id)?;
        Ok(do_verify)
    }

    pub(crate) fn is_verifier(
        &mut self,
        _thread_id: &ThreadIdentifierFor<TBlockProducerProcess>,
        block_id: &BlockIdentifierFor<TBlockProducerProcess>,
        verify_complexity: SignerIndex,
    ) -> bool {
        // TODO: need to check that rng is called consecutively
        let rand_id: SignerIndex = self.block_keeper_rng.gen();
        let block_id_modulus = block_id.modulus(1<<16) as SignerIndex;
        let cur_complexity = rand_id ^ block_id_modulus;
        tracing::trace!("is_verifier check: cur_complexity={cur_complexity} verify_complexity={verify_complexity}");
        cur_complexity < verify_complexity
    }


}
