// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::Arc;

use network::channel::NetBroadcastSender;
use network::channel::NetDirectSender;
use parking_lot::Mutex;
use typed_builder::TypedBuilder;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::helper::start_shutdown;
use crate::helper::SHUTDOWN_FLAG;
use crate::node::associated_types::AckData;
use crate::node::associated_types::NackData;
use crate::node::associated_types::NackReason;
use crate::node::BlockState;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::node::PubKey;
use crate::node::Secret;
use crate::node::SignerIndex;
use crate::types::RndSeed;
use crate::utilities::guarded::Guarded;

#[derive(TypedBuilder, Clone)]
pub struct AckiNackiSend {
    node_id: NodeIdentifier,
    bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
    ack_network_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
    nack_network_broadcast_tx: NetBroadcastSender<NetworkMessage>,
}

impl AckiNackiSend {
    pub fn send_ack(&self, block_state: BlockState) -> anyhow::Result<()> {
        let (
            block_id,
            Some(block_seq_no),
            mut known_attestation_interested_parties,
            Some(thread_identifier),
        ) = block_state.guarded(|e| {
            (
                e.block_identifier().clone(),
                *e.block_seq_no(),
                e.known_attestation_interested_parties().clone(),
                *e.thread_identifier(),
            )
        })
        else {
            anyhow::bail!("block state does not have valid data set")
        };
        let destinations = {
            known_attestation_interested_parties.remove(&self.node_id);
            known_attestation_interested_parties
        };
        // TODO: mb just return here
        anyhow::ensure!(destinations.len() > 0);

        let Some((node_epoch_signer_index, node_epoch_secret)) = self.get_signer_data(&block_state)
        else {
            tracing::warn!("Node is not in BK set for given block");
            return Ok(());
        };

        let ack_data = AckData { block_id: block_id.clone(), block_seq_no };
        let signature_occurrences = HashMap::from([(node_epoch_signer_index, 1)]);

        let signature = <GoshBLS as BLSSignatureScheme>::sign(&node_epoch_secret, &ack_data)?;

        let ack = Envelope::<GoshBLS, AckData>::create(signature, signature_occurrences, ack_data);
        let message = NetworkMessage::Ack((ack, thread_identifier));
        for destination_node_id in destinations.into_iter() {
            tracing::trace!(
                "Sending ack to node_id: {destination_node_id:?} for block_seq_no: {} block_id: {:?}",
                block_seq_no,
                block_id
            );
            match self.ack_network_direct_tx.send((destination_node_id.into(), message.clone())) {
                Ok(()) => {}
                Err(e) => {
                    if SHUTDOWN_FLAG.get() != Some(&true) {
                        anyhow::bail!("Failed to send ack: {e}");
                    }
                }
            }
        }
        Ok(())
    }

    pub fn send_nack(
        &self,
        block_state: BlockState,
        // envelope: Envelope<GoshBLS, AckiNackiBlock>,
        reason: NackReason,
    ) -> anyhow::Result<()> {
        let (block_id, Some(block_seq_no), Some(thread_id)) = block_state
            .guarded(|e| (e.block_identifier().clone(), *e.block_seq_no(), *e.thread_identifier()))
        else {
            anyhow::bail!("block state does not have valid data set")
        };
        let Some((node_epoch_signer_index, node_epoch_secret)) = self.get_signer_data(&block_state)
        else {
            tracing::warn!("Node is not in BK set for given block");
            return Ok(());
        };

        // let reason = NackReason::BadBlock { envelope };
        let nack_data = NackData { block_id: block_id.clone(), block_seq_no, reason };
        let signature = <GoshBLS as BLSSignatureScheme>::sign(&node_epoch_secret, &nack_data)?;
        let mut signature_occurrences = HashMap::new();
        signature_occurrences.insert(node_epoch_signer_index, 1);

        let nack =
            Envelope::<GoshBLS, NackData>::create(signature, signature_occurrences, nack_data);
        let message = NetworkMessage::Nack((nack, thread_id));
        tracing::trace!("Broadcasting nack for block_id: {block_id:?}");
        match self.nack_network_broadcast_tx.send(message) {
            Ok(_) => {}
            Err(e) => {
                if SHUTDOWN_FLAG.get() != Some(&true) {
                    anyhow::bail!("Failed to broadcast nack: {e}");
                }
            }
        }
        Ok(())
    }

    fn get_signer_data(&self, block_state: &BlockState) -> Option<(SignerIndex, Secret)> {
        let (node_epoch_pubkey, node_epoch_signer_index) = block_state.guarded(|e| {
            let node_epoch_bk_data = e.get_bk_data_for_node_id(&self.node_id)?;
            let node_epoch_signer_index = node_epoch_bk_data.signer_index;
            let node_epoch_pubkey = node_epoch_bk_data.pubkey;
            Some((node_epoch_pubkey, node_epoch_signer_index))
        })?;
        let node_epoch_secret = self.bls_keys_map.guarded(|e| e.get(&node_epoch_pubkey).cloned());
        if node_epoch_secret.is_none() {
            start_shutdown();
            tracing::error!("Node does not have valid key which was used to deploy epoch: pubkey={node_epoch_pubkey:?}");
        }
        let node_epoch_secret = node_epoch_secret?.0;
        Some((node_epoch_signer_index, node_epoch_secret))
    }
}
