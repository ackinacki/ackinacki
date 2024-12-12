// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::GoshBLS;
use crate::message::WrappedMessage;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::types::ThreadIdentifier;

pub struct NetworkMessageRouter {
    common_receiver: Receiver<
        NetworkMessage<GoshBLS, AckData, NackData, AttestationData, WrappedMessage, NodeIdentifier>,
    >,
    nodes: HashMap<
        ThreadIdentifier,
        Sender<
            NetworkMessage<
                GoshBLS,
                AckData,
                NackData,
                AttestationData,
                WrappedMessage,
                NodeIdentifier,
            >,
        >,
    >,
}

impl NetworkMessageRouter {
    pub fn new(
        common_receiver: Receiver<
            NetworkMessage<
                GoshBLS,
                AckData,
                NackData,
                AttestationData,
                WrappedMessage,
                NodeIdentifier,
            >,
        >,
    ) -> Self {
        Self { common_receiver, nodes: HashMap::new() }
    }

    pub fn add_sender(
        &mut self,
        thread_identifier: ThreadIdentifier,
    ) -> Receiver<
        NetworkMessage<GoshBLS, AckData, NackData, AttestationData, WrappedMessage, NodeIdentifier>,
    > {
        tracing::trace!("NetworkMessageRouter: add sender for thread: {thread_identifier:?}");
        let (incoming_messages_sender, incoming_messages_receiver) = std::sync::mpsc::channel();
        self.nodes.insert(thread_identifier, incoming_messages_sender);
        incoming_messages_receiver
    }

    pub fn execute(&self) -> anyhow::Result<()> {
        loop {
            let (thread_id, message) = match self.common_receiver.recv() {
                Err(e) => {
                    tracing::error!("NetworkMessageRouter: common receiver was disconnected: {e}");
                    anyhow::bail!(e)
                }
                Ok(message) => {
                    match &message {
                        NetworkMessage::Candidate(candidate_block) => {
                            (candidate_block.data().get_common_section().thread_id, message)
                        }
                        // Node entities share one repository, so send ext message only to one node for not to duplicate messages in repo
                        NetworkMessage::ExternalMessage(_) => {
                            (ThreadIdentifier::default(), message)
                        }
                        NetworkMessage::Ack((_, thread_id)) => (*thread_id, message),
                        NetworkMessage::Nack((_, thread_id)) => (*thread_id, message),
                        NetworkMessage::BlockAttestation((_, thread_id)) => (*thread_id, message),
                        NetworkMessage::NodeJoining((_, thread_id)) => (*thread_id, message),
                        NetworkMessage::BlockRequest((_, _, _, thread_id)) => (*thread_id, message),
                        NetworkMessage::SyncFinalized((_, _, _, thread_id)) => {
                            (*thread_id, message)
                        }
                        NetworkMessage::SyncFrom((_, thread_id)) => (*thread_id, message),
                    }
                }
            };
            tracing::trace!("NetworkMessageRouter: received message for {thread_id:?} {message:?}");
            match self.nodes.get(&thread_id) {
                Some(sender) => {
                    sender.send(message)?;
                }
                None => {
                    // TODO: Received block for unexpected thread, skip it for now
                    tracing::warn!("Received block from unexpected thread: {message:?}");
                }
            }
        }
    }
}
