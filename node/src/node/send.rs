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
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;

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

    pub(crate) fn broadcast_node_joining(&self) -> anyhow::Result<()> {
        tracing::trace!("Broadcast NetworkMessage::NodeJoining");
        self.tx.send(NetworkMessage::NodeJoining((self.config.local.node_id, self.thread_id)))?;
        Ok(())
    }

    pub(crate) fn broadcast_candidate_block(
        &self,
        candidate_block: <Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<()> {
        tracing::info!(
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
        tracing::info!("sending block to node {node_id}:{}", candidate_block.data());
        self.single_tx.send((node_id, NetworkMessage::Candidate(candidate_block)))?;
        Ok(())
    }

    pub(crate) fn send_block_attestation(
        &self,
        node_id: NodeIdentifier,
        attestation: <Self as NodeAssociatedTypes>::BlockAttestation,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "sending attestation to node {}: {:?}",
            node_id,
            attestation,
        );
        self.single_tx.send((node_id, NetworkMessage::BlockAttestation((attestation, self.thread_id))))?;
        Ok(())
    }

    pub(crate) fn send_block_request(
        &self,
        node_id: NodeIdentifier,
        included_from: BlockSeqNo,
        excluded_to: BlockSeqNo,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "sending block request to node {} [{:?}, {:?})",
            node_id,
            included_from,
            excluded_to,
        );
        self.single_tx.send((node_id, NetworkMessage::BlockRequest((included_from, excluded_to, self.config.local.node_id, self.thread_id))))?;
        Ok(())
    }

    pub(crate) fn broadcast_sync_finalized(
        &self,
        block_identifier: BlockIdentifier,
        block_seq_no: BlockSeqNo,
        shared_res_address: String,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "broadcasting SyncFinalized {:?} {:?} {}",
            block_seq_no,
            block_identifier,
            shared_res_address
        );
        self.tx.send(NetworkMessage::SyncFinalized((block_identifier, block_seq_no, shared_res_address, self.thread_id)))?;
        Ok(())
    }

    pub(crate) fn send_sync_from(
        &self,
        node_id: NodeIdentifier,
        from_seq_no: BlockSeqNo,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "sending syncFrom to node {}: {:?}",
            node_id,
            from_seq_no,
        );
        self.single_tx.send((node_id, NetworkMessage::SyncFrom((from_seq_no, self.thread_id))))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn broadcast_ack(&self, ack: <Self as NodeAssociatedTypes>::Ack) -> anyhow::Result<()> {
        tracing::trace!("Broadcasting Ack: {:?}", ack.data());
        self.tx.send(NetworkMessage::Ack((ack, self.thread_id)))?;
        Ok(())
    }

    pub(crate) fn send_ack(
        &self,
        node_id: NodeIdentifier,
        ack: <Self as NodeAssociatedTypes>::Ack
    ) -> anyhow::Result<()> {
        tracing::info!(
            "sending Ack to node {}: {:?}",
            node_id,
            ack.data(),
        );
        self.single_tx.send((node_id, NetworkMessage::Ack((ack, self.thread_id))))?;
        Ok(())
    }

    pub(crate) fn broadcast_nack(&self, nack: <Self as NodeAssociatedTypes>::Nack) -> anyhow::Result<()> {
        tracing::trace!("Broadcasting Nack: {:?}", nack.data());
        self.tx.send(NetworkMessage::Nack((nack, self.thread_id)))?;
        Ok(())
    }
}
