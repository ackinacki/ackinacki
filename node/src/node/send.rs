// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use network::channel::NetDirectSender;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::helper::block_flow_trace;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::services::sync::StateSyncService;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

pub fn send_blocks_range_request(
    network_direct_tx: &NetDirectSender<NodeIdentifier, NetworkMessage>,
    destination_node_id: NodeIdentifier,
    requester: NodeIdentifier,
    thread_id: ThreadIdentifier,
    inclusive_from: BlockSeqNo,
    exclusive_to: BlockSeqNo,
    at_least_n_blocks: Option<usize>,
) -> anyhow::Result<()> {
    tracing::info!(
        "sending block request to node {} [{:?}, {:?}) + min_n: {:?}",
        destination_node_id,
        inclusive_from,
        exclusive_to,
        at_least_n_blocks
    );
    network_direct_tx.send((
        destination_node_id,
        NetworkMessage::BlockRequest {
            inclusive_from,
            exclusive_to,
            requester,
            thread_id,
            at_least_n_blocks,
        },
    ))?;
    Ok(())
}

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    pub(crate) fn broadcast_node_joining(&self) -> anyhow::Result<()> {
        tracing::trace!("Broadcast NetworkMessage::NodeJoining");
        self.network_broadcast_tx.send(NetworkMessage::NodeJoining((
            self.config.local.node_id.clone(),
            self.thread_id,
        )))?;
        Ok(())
    }

    pub(crate) fn _broadcast_candidate_block(
        &self,
        candidate_block: <Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<()> {
        tracing::info!("broadcasting block: {}", candidate_block,);
        self.network_broadcast_tx.send(NetworkMessage::Candidate(candidate_block))?;
        Ok(())
    }

    pub(crate) fn send_candidate_block(
        &self,
        candidate_block: <Self as NodeAssociatedTypes>::CandidateBlock,
        node_id: NodeIdentifier,
    ) -> anyhow::Result<()> {
        tracing::info!("sending block to node {node_id}:{}", candidate_block.data());
        block_flow_trace(
            "direct sending candidate",
            &candidate_block.data().identifier(),
            &self.config.local.node_id,
            [("to", &node_id.to_string())],
        );
        self.network_direct_tx.send((node_id, NetworkMessage::Candidate(candidate_block)))?;
        Ok(())
    }

    pub(crate) fn send_block_request(
        &self,
        node_id: NodeIdentifier,
        included_from: BlockSeqNo,
        excluded_to: BlockSeqNo,
        at_least_n_blocks: Option<usize>,
    ) -> anyhow::Result<()> {
        send_blocks_range_request(
            &self.network_direct_tx,
            node_id,
            self.config.local.node_id.clone(),
            self.thread_id,
            included_from,
            excluded_to,
            at_least_n_blocks,
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
        self.network_broadcast_tx.send(NetworkMessage::SyncFinalized((
            block_identifier,
            block_seq_no,
            shared_res_address,
            self.thread_id,
        )))?;
        Ok(())
    }

    pub(crate) fn send_sync_from(
        &self,
        node_id: NodeIdentifier,
        from_seq_no: BlockSeqNo,
    ) -> anyhow::Result<()> {
        tracing::info!("sending syncFrom to node {}: {:?}", node_id, from_seq_no,);
        self.network_direct_tx
            .send((node_id, NetworkMessage::SyncFrom((from_seq_no, self.thread_id))))?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn broadcast_ack(
        &self,
        ack: <Self as NodeAssociatedTypes>::Ack,
    ) -> anyhow::Result<()> {
        tracing::trace!("Broadcasting Ack: {:?}", ack.data());
        self.network_broadcast_tx.send(NetworkMessage::Ack((ack, self.thread_id)))?;
        Ok(())
    }

    pub(crate) fn _send_ack(
        &self,
        node_id: NodeIdentifier,
        ack: <Self as NodeAssociatedTypes>::Ack,
    ) -> anyhow::Result<()> {
        tracing::info!("sending Ack to node {}: {:?}", node_id, ack.data(),);
        self.network_direct_tx.send((node_id, NetworkMessage::Ack((ack, self.thread_id))))?;
        Ok(())
    }

    pub(crate) fn _broadcast_nack(
        &self,
        nack: <Self as NodeAssociatedTypes>::Nack,
    ) -> anyhow::Result<()> {
        tracing::trace!("Broadcasting Nack: {:?}", nack.data().block_id);
        self.network_broadcast_tx.send(NetworkMessage::Nack((nack, self.thread_id)))?;
        Ok(())
    }
}
