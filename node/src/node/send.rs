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
use crate::node::NetworkMessage;
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

    pub(crate) fn broadcast_node_joining(&self) -> anyhow::Result<()> {
        tracing::trace!("Broadcast NetworkMessage::NodeJoining");
        self.tx.send(NetworkMessage::NodeJoining(self.config.local.node_id))?;
        Ok(())
    }

    pub(crate) fn broadcast_candidate_block(
        &self,
        candidate_block: <Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<()> {
        log::info!(
            "broadcasting block: {}",
            candidate_block,
        );
        self.tx.send(NetworkMessage::Candidate(candidate_block))?;
        Ok(())
    }

    pub(crate) fn send_candidate_block(
        &self,
        candidate_block: <Self as NodeAssociatedTypes>::CandidateBlock,
        node_id: NodeIdentifier,
    ) -> anyhow::Result<()> {
        log::info!("sending block to node {node_id}:{}", candidate_block.data());
        self.single_tx.send((node_id, NetworkMessage::Candidate(candidate_block)))?;
        Ok(())
    }

    pub(crate) fn send_block_attestation(
        &self,
        node_id: NodeIdentifier,
        attestation: <Self as NodeAssociatedTypes>::BlockAttestation,
    ) -> anyhow::Result<()> {
        log::info!(
            "sending attestation to node {}: {:?}",
            node_id,
            attestation,
        );
        self.single_tx.send((node_id, NetworkMessage::BlockAttestation(attestation)))?;
        Ok(())
    }

    pub(crate) fn send_block_request(
        &self,
        node_id: NodeIdentifier,
        included_from: BlockSeqNoFor<TBlockProducerProcess>,
        excluded_to: BlockSeqNoFor<TBlockProducerProcess>,
    ) -> anyhow::Result<()> {
        log::info!(
            "sending block request to node {} [{:?}, {:?})",
            node_id,
            included_from,
            excluded_to,
        );
        self.single_tx.send((node_id, NetworkMessage::BlockRequest((included_from, excluded_to, self.config.local.node_id))))?;
        Ok(())
    }

    pub(crate) fn broadcast_sync_finalized(
        &self,
        block_identifier: BlockIdentifierFor<TBlockProducerProcess>,
        block_seq_no: BlockSeqNoFor<TBlockProducerProcess>,
        shared_res_address: String,
    ) -> anyhow::Result<()> {
        log::info!(
            "broadcasting SyncFinalized {:?} {:?} {}",
            block_seq_no,
            block_identifier,
            shared_res_address
        );
        self.tx.send(NetworkMessage::SyncFinalized((block_identifier, block_seq_no, shared_res_address)))?;
        Ok(())
    }

    pub(crate) fn send_sync_from(
        &self,
        node_id: NodeIdentifier,
        from_seq_no: BlockSeqNoFor<TBlockProducerProcess>,
    ) -> anyhow::Result<()> {
        log::info!(
            "sending syncFrom to node {}: {:?}",
            node_id,
            from_seq_no,
        );
        self.single_tx.send((node_id, NetworkMessage::SyncFrom(from_seq_no)))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn broadcast_ack(&self, ack: <Self as NodeAssociatedTypes>::Ack) -> anyhow::Result<()> {
        tracing::trace!("Broadcasting Ack: {:?}", ack.data());
        self.tx.send(NetworkMessage::Ack(ack))?;
        Ok(())
    }

    pub(crate) fn send_ack(
        &self,
        node_id: NodeIdentifier,
        ack: <Self as NodeAssociatedTypes>::Ack
    ) -> anyhow::Result<()> {
        log::info!(
            "sending Ack to node {}: {:?}",
            node_id,
            ack.data(),
        );
        self.single_tx.send((node_id, NetworkMessage::Ack(ack)))?;
        Ok(())
    }

    pub(crate) fn broadcast_nack(&self, nack: <Self as NodeAssociatedTypes>::Nack) -> anyhow::Result<()> {
        tracing::trace!("Broadcasting Nack: {:?}", nack.data());
        self.tx.send(NetworkMessage::Nack(nack))?;
        Ok(())
    }
}
