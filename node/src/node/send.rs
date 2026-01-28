// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;

use network::channel::NetBroadcastSender;
use network::channel::NetDirectSender;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::try_seal::TrySeal;
use crate::helper::metrics::BlockProductionMetrics;
use crate::helper::SHUTDOWN_FLAG;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::SyncFinalizedData;
use crate::node::network_message::NodeJoiningWithLastFinalizedData;
use crate::node::services::sync::StateSyncService;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::node::NodeIdentifier;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;

pub fn send_blocks_range_request(
    network_direct_tx: &NetDirectSender<NodeIdentifier, NetworkMessage>,
    destination_node_id: NodeIdentifier,
    requester: NodeIdentifier,
    thread_id: ThreadIdentifier,
    inclusive_from: BlockSeqNo,
    exclusive_to: BlockSeqNo,
    at_least_n_blocks: Option<usize>,
) -> anyhow::Result<()> {
    tracing::debug!(
        "sending block request to node {} [{:?}, {:?}) + min_n: {:?}",
        destination_node_id,
        inclusive_from,
        exclusive_to,
        at_least_n_blocks
    );
    match network_direct_tx.send((
        destination_node_id.into(),
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

pub(crate) fn broadcast_node_joining(
    network_broadcast_tx: &NetBroadcastSender<NodeIdentifier, NetworkMessage>,
    metrics: Option<&BlockProductionMetrics>,
    node_id: NodeIdentifier,
    thread_id: ThreadIdentifier,
    last_finalized_block_seq_no: BlockSeqNo,
) -> anyhow::Result<()> {
    tracing::debug!(target: "monit", "Broadcast NetworkMessage::NodeJoining");
    let node_joining_data = NodeJoiningWithLastFinalizedData::builder()
        .node_identifier(node_id)
        .last_finalized_block_seq_no(last_finalized_block_seq_no)
        .build();
    match network_broadcast_tx
        .send(NetworkMessage::NodeJoiningWithLastFinalized((node_joining_data, thread_id)))
    {
        Ok(_) => {
            if let Some(m) = metrics {
                m.report_broadcast_join(&thread_id);
            }
        }
        Err(e) => {
            if SHUTDOWN_FLAG.get() != Some(&true) {
                anyhow::bail!("Failed to broadcast node joining: {e}");
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
        let last_finalized_block_seq_no = self
            .repository
            .select_thread_last_finalized_block(&self.thread_id)?
            .map(|(_, seq_no)| seq_no)
            .unwrap_or_default();
        broadcast_node_joining(
            &self.network_broadcast_tx,
            self.metrics.as_ref(),
            self.config.local.node_id.clone(),
            self.thread_id,
            last_finalized_block_seq_no,
        )
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
        tracing::debug!(
            "broadcasting SyncFinalized {:?} {:?} {:?}",
            block_seq_no,
            block_identifier,
            shared_res_address
        );
        let shared_res_address = BTreeMap::from_iter(shared_res_address);
        let data = SyncFinalizedData::builder()
            .block_identifier(block_identifier.clone())
            .block_seq_no(block_seq_no)
            .thread_refs(shared_res_address)
            .build();
        let Ok(block_state) = self.block_state_repository.get(&block_identifier) else {
            tracing::trace!("Failed to load block state. Skip broadcasting");
            return Ok(());
        };
        let (Some(bk_set), Some(block_version)) =
            block_state.guarded(|e| (e.bk_set().clone(), e.block_version_state().clone()))
        else {
            tracing::trace!("Failed to get bk_set from block state. Skip broadcasting");
            return Ok(());
        };

        let secrets = self.bls_keys_map.guarded(|map| map.clone());

        let envelope = match data.try_seal(
            &self.node_credentials,
            &bk_set,
            &secrets,
            block_version.to_use(),
        ) {
            Ok(envelope) => envelope,
            Err(e) => {
                tracing::trace!("Failed to sign SyncFinalized envelope: {e}. Skip broadcasting");
                return Ok(());
            }
        };

        match self
            .network_broadcast_tx
            .send(NetworkMessage::SyncFinalized((envelope, self.thread_id)))
        {
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
        tracing::debug!("sending syncFrom to node {}: {:?}", node_id, from_seq_no,);
        match self
            .network_direct_tx
            .send((node_id.into(), NetworkMessage::SyncFrom((from_seq_no, self.thread_id))))
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
