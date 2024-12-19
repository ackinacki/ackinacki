use std::collections::HashMap;
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

type Payload =
    NetworkMessage<GoshBLS, AckData, NackData, AttestationData, WrappedMessage, NodeIdentifier>;

pub enum DispatchError {
    NoRoute(ThreadIdentifier, Payload),
    DestinationClosed(ThreadIdentifier, Payload),
}

pub struct Dispatcher {
    routes: HashMap<ThreadIdentifier, Sender<Payload>>,
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

// TODO: no callbacks to remove from the list is added.
impl Dispatcher {
    pub fn new() -> Self {
        Self { routes: HashMap::new() }
    }

    pub fn has_route(&mut self, thread_identifier: &ThreadIdentifier) -> bool {
        self.routes.contains_key(thread_identifier)
    }

    pub fn add_route(&mut self, thread_identifier: ThreadIdentifier, node: Sender<Payload>) {
        self.routes.insert(thread_identifier, node);
    }

    #[allow(clippy::result_large_err)]
    pub fn dispatch(&self, message: Payload) -> anyhow::Result<(), DispatchError> {
        let thread_id = match &message {
            NetworkMessage::Candidate(candidate_block) => {
                candidate_block.data().get_common_section().thread_id
            }
            // Node entities share one repository, so send ext message only to one node for not to duplicate messages in repo
            NetworkMessage::ExternalMessage(_) => ThreadIdentifier::default(),
            NetworkMessage::Ack((_, thread_id)) => *thread_id,
            NetworkMessage::Nack((_, thread_id)) => *thread_id,
            NetworkMessage::BlockAttestation((_, thread_id)) => *thread_id,
            NetworkMessage::NodeJoining((_, thread_id)) => *thread_id,
            NetworkMessage::BlockRequest((_, _, _, thread_id)) => *thread_id,
            NetworkMessage::SyncFinalized((_, _, _, thread_id)) => *thread_id,
            NetworkMessage::SyncFrom((_, thread_id)) => *thread_id,
        };
        tracing::trace!("Dispatcher: received message for {thread_id:?} {message:?}");
        match self.routes.get(&thread_id) {
            Some(sender) => {
                sender
                    .send(message)
                    .map_err(|e| DispatchError::DestinationClosed(thread_id, e.0))?;
                Ok(())
            }
            None => {
                // TODO: Received block for unexpected thread, skip it for now
                tracing::warn!("Received block from unexpected thread: {message:?}");
                Err(DispatchError::NoRoute(thread_id, message))
            }
        }
    }
}
