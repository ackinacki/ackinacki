// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::mpsc::Sender;

use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::services::sync::StateSyncService;
use crate::node::GoshBLS;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

pub fn send_blocks_range_request(
    send_tx: &Sender<(NodeIdentifier, NetworkMessage)>,
    destination_node_id: NodeIdentifier,
    requesting_node_id: NodeIdentifier,
    thread_id: ThreadIdentifier,
    included_from: BlockSeqNo,
    excluded_to: BlockSeqNo,
) -> anyhow::Result<()> {
    tracing::info!(
        "sending block request to node {} [{:?}, {:?})",
        destination_node_id,
        included_from,
        excluded_to,
    );
    send_tx.send((
        destination_node_id,
        NetworkMessage::BlockRequest((included_from, excluded_to, requesting_node_id, thread_id)),
    ))?;
    Ok(())
}

impl<TStateSyncService, TBlockProducerProcess, TRandomGenerator>
Node<TStateSyncService, TBlockProducerProcess, TRandomGenerator>
    where
        TBlockProducerProcess:
        BlockProducerProcess< Repository = RepositoryImpl>,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateImpl,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = RepositoryImpl
        >,
        TRandomGenerator: rand::Rng,
{

    pub(crate) fn broadcast_node_joining(&self) -> anyhow::Result<()> {
        tracing::trace!("Broadcast NetworkMessage::NodeJoining");
        self.tx.send(NetworkMessage::NodeJoining((self.config.local.node_id.clone(), self.thread_id)))?;
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

    pub(crate) fn broadcast_candidate_block_that_was_possibly_produced_by_another_node(
        &self,
        candidate_block: <Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "rebroadcasting block: {}",
            candidate_block,
        );
        self.tx.send(NetworkMessage::ResentCandidate((candidate_block, self.config.local.node_id.clone())))?;
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

    pub(crate) fn send_block_request(
        &self,
        node_id: NodeIdentifier,
        included_from: BlockSeqNo,
        excluded_to: BlockSeqNo,
    ) -> anyhow::Result<()> {
        send_blocks_range_request(
            &self.single_tx,
            node_id,
            self.config.local.node_id.clone(),
            self.thread_id,
            included_from,
            excluded_to,
        )
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

    pub(crate) fn _send_ack(
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

    pub(crate) fn _broadcast_nack(&self, nack: <Self as NodeAssociatedTypes>::Nack) -> anyhow::Result<()> {
        tracing::trace!("Broadcasting Nack: {:?}", nack.data().block_id);
        self.tx.send(NetworkMessage::Nack((nack, self.thread_id)))?;
        Ok(())
    }
}
