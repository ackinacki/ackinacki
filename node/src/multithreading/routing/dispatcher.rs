use std::collections::HashMap;

use telemetry_utils::instrumented_channel_ext::WrappedItem;
use telemetry_utils::instrumented_channel_ext::XInstrumentedSender;

use crate::node::NetworkMessage;
use crate::protocol::authority_switch;
use crate::types::ThreadIdentifier;

type Payload = NetworkMessage;

pub enum DispatchError {
    NoRoute(ThreadIdentifier, Payload),
    DestinationClosed(ThreadIdentifier, Payload),
}

pub struct Dispatcher {
    routes: HashMap<ThreadIdentifier, (XInstrumentedSender<Payload>, XInstrumentedSender<Payload>)>,
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

    pub fn add_route(
        &mut self,
        thread_identifier: ThreadIdentifier,
        node: XInstrumentedSender<Payload>,
        authority: XInstrumentedSender<Payload>,
    ) {
        self.routes.insert(thread_identifier, (node, authority));
    }

    #[allow(clippy::result_large_err)]
    pub fn dispatch(&self, message: Payload) -> anyhow::Result<(), DispatchError> {
        let (is_authority, thread_id) = match &message {
            NetworkMessage::AuthoritySwitchProtocol(e) => {
                (true, authority_switch::routing::route(e))
            }
            NetworkMessage::Candidate(net_block)
            | NetworkMessage::ResentCandidate((net_block, _)) => (false, net_block.thread_id),
            // Node entities share one repository, so send ext message only to one node for not to duplicate messages in repo
            NetworkMessage::ExternalMessage((_, thread_id)) => (false, *thread_id),
            NetworkMessage::Ack((_, thread_id)) => (false, *thread_id),
            NetworkMessage::Nack((_, thread_id)) => (false, *thread_id),
            NetworkMessage::BlockAttestation((_, thread_id)) => (false, *thread_id),
            NetworkMessage::NodeJoining((_, thread_id)) => (false, *thread_id),
            NetworkMessage::BlockRequest { thread_id, .. } => (false, *thread_id),
            NetworkMessage::SyncFinalized((_, _, _, thread_id)) => (false, *thread_id),
            NetworkMessage::SyncFrom((_, thread_id)) => (false, *thread_id),
            // ignore StartSynchronization it is used for local interaction
            NetworkMessage::StartSynchronization => {
                return Ok(());
            }
        };
        tracing::trace!("Dispatcher: received message for {thread_id:?} {message:?}");
        match self.routes.get(&thread_id) {
            Some((sender, authority)) => {
                if is_authority {
                    authority
                        .send(WrappedItem { payload: message, label: thread_id.to_string() })
                        .map_err(|e| DispatchError::DestinationClosed(thread_id, e.0.payload))?;
                } else {
                    sender
                        .send(WrappedItem { payload: message, label: thread_id.to_string() })
                        .map_err(|e| DispatchError::DestinationClosed(thread_id, e.0.payload))?;
                }
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
