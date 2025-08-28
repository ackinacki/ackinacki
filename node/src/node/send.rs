// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use network::channel::NetDirectSender;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::helper::SHUTDOWN_FLAG;
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
    match network_direct_tx.send((
        destination_node_id,
        NetworkMessage::BlockRequest {
            inclusive_from,
            exclusive_to,
            requester,
            thread_id,
            at_least_n_blocks,
        },
    )) {
        Ok(()) => {}
        Err(e) => {
            if SHUTDOWN_FLAG.get() != Some(&true) {
                anyhow::bail!("Failed to send direct message: {e}");
            }
        }
    }
    Ok(())
}

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    pub(crate) fn broadcast_node_joining(&self) -> anyhow::Result<()> {
        tracing::trace!("Broadcast NetworkMessage::NodeJoining");
        match self
            .network_broadcast_tx
            .send(NetworkMessage::NodeJoining((self.config.local.node_id.clone(), self.thread_id)))
        {
            Ok(_) => {}
            Err(e) => {
                if SHUTDOWN_FLAG.get() != Some(&true) {
                    anyhow::bail!("Failed to broadcast node joining: {e}");
                }
            }
        }
        Ok(())
    }

    pub(crate) fn send_block_request(
        &self,
        node_id: NodeIdentifier,
        included_from: BlockSeqNo,
        excluded_to: BlockSeqNo,
        at_least_n_blocks: Option<usize>,
    ) -> anyhow::Result<()> {
        if excluded_to > included_from {
            if let Some(metrics) = &self.metrics {
                let gap_len = excluded_to - included_from;
                metrics.report_blocks_requested(gap_len as u64, &self.thread_id);
            }
        }
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
        shared_res_address: HashMap<ThreadIdentifier, BlockIdentifier>,
    ) -> anyhow::Result<()> {
        tracing::info!(
            "broadcasting SyncFinalized {:?} {:?} {:?}",
            block_seq_no,
            block_identifier,
            shared_res_address
        );
        match self.network_broadcast_tx.send(NetworkMessage::SyncFinalized((
            block_identifier,
            block_seq_no,
            shared_res_address,
            self.thread_id,
        ))) {
            Ok(_) => {}
            Err(e) => {
                if SHUTDOWN_FLAG.get() != Some(&true) {
                    anyhow::bail!("Failed to broadcast sync finalized: {e}");
                }
            }
        }
        Ok(())
    }

    pub(crate) fn send_sync_from(
        &self,
        node_id: NodeIdentifier,
        from_seq_no: BlockSeqNo,
    ) -> anyhow::Result<()> {
        tracing::info!("sending syncFrom to node {}: {:?}", node_id, from_seq_no,);
        match self
            .network_direct_tx
            .send((node_id, NetworkMessage::SyncFrom((from_seq_no, self.thread_id))))
        {
            Ok(()) => {}
            Err(e) => {
                if SHUTDOWN_FLAG.get() != Some(&true) {
                    anyhow::bail!("Failed to send sync from: {e}");
                }
            }
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn broadcast_ack(
        &self,
        ack: <Self as NodeAssociatedTypes>::Ack,
    ) -> anyhow::Result<()> {
        tracing::trace!("Broadcasting Ack: {:?}", ack.data());
        match self.network_broadcast_tx.send(NetworkMessage::Ack((ack, self.thread_id))) {
            Ok(_) => {}
            Err(e) => {
                if SHUTDOWN_FLAG.get() != Some(&true) {
                    anyhow::bail!("Failed to broadcast ack: {e}");
                }
            }
        }
        Ok(())
    }
}
