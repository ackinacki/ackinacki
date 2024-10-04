// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

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
    pub(crate) fn list_threads(&self) -> anyhow::Result<Vec<ThreadIdentifierFor<TBlockProducerProcess>>> {
        // Note: Single thread implementation
        Ok(vec![ThreadIdentifierFor::<TBlockProducerProcess>::default()])
    }

    pub(crate) fn get_block_thread_id(
        &self,
        block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<ThreadIdentifierFor<TBlockProducerProcess>> {
        // TODO: not real implementation.
        // Issue: It is not possible to have one function to list threads
        // in a given moment, since there will be block candidates that
        // start thread splitting in one scenario and not in another.
        // Same applies for merges.
        // Yet the interface for this function should remain the same.
        // let mut result = vec![];
        // According to the latest discussions block should belong only to a single thread
        for thread_identifier in self.list_threads()? {
            if self.is_block_in_the_given_thread(block.data(), &thread_identifier) {
                // result.push(thread_identifier);
                return Ok(thread_identifier);
            }
        }
        // assert!(!result.is_empty());
        // Ok(result)
        anyhow::bail!("Failed to get thread id for block");
    }

    pub(crate) fn is_block_in_the_given_thread(
        &self,
        _block: &BlockFor<TBlockProducerProcess>,
        _thread_identifier: &ThreadIdentifierFor<TBlockProducerProcess>,
    ) -> bool {
        // Note: Stub implementation.
        // Since this example assumes only 1 thread it should always return true
        true
    }
}
