use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;

use super::dispatcher::DispatchError;
use super::dispatcher::Dispatcher;
use super::poisoned_queue::PoisonedQueue as PQueue;
use crate::block::keeper::process::TVMBlockKeeperProcess;
use crate::block::producer::process::TVMBlockProducerProcess;
use crate::bls::GoshBLS;
use crate::message::WrappedMessage;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::attestation_processor::AttestationProcessorImpl;
use crate::node::services::sync::ExternalFileSharesBased;
use crate::node::NetworkMessage as NodeNetworkMessage;
use crate::node::Node as NodeImpl;
use crate::node::NodeIdentifier;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;

// TODO: make into a config.
// TODO: calculate an acceptable and balanced value.
const MAX_POISONED_QUEUE_SIZE: usize = 10000;

type NetworkMessage =
    NodeNetworkMessage<GoshBLS, AckData, NackData, AttestationData, WrappedMessage, NodeIdentifier>;

type PoisonedQueue = PQueue<NetworkMessage>;

type Node = NodeImpl<
    ExternalFileSharesBased,
    TVMBlockProducerProcess,
    TVMBlockKeeperProcess,
    RepositoryImpl,
    AttestationProcessorImpl,
    rand::prelude::SmallRng,
>;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    //    Stop,
    Route(NetworkMessage),
    StartThread(
        (
            ThreadIdentifier, // Thread to start
            BlockIdentifier,  // Thread parent block
        ),
    ),
    JoinThread(ThreadIdentifier),
}

#[derive(Clone)]
pub struct RoutingService {
    pub control: Sender<Command>,
}

impl RoutingService {
    pub fn new(
        inbound_network: Receiver<NetworkMessage>,
    ) -> (RoutingService, Receiver<Command>, std::thread::JoinHandle<Result<(), anyhow::Error>>)
    {
        let (control, handler) = std::sync::mpsc::channel();
        let forwarding_thread = {
            let control = control.clone();
            std::thread::Builder::new()
                .name("routing_service_network_messages_forwarding_loop".to_string())
                .spawn(move || {
                    Self::inner_network_messages_forwarding_loop(inbound_network, control)
                })
                .unwrap()
        };
        (RoutingService { control }, handler, forwarding_thread)
    }

    pub fn start<F>(
        channel: (RoutingService, Receiver<Command>),
        node_factory: F,
    ) -> (Self, std::thread::JoinHandle<Result<(), anyhow::Error>>)
    where
        F: FnMut(
                Option<BlockIdentifier>,
                &ThreadIdentifier,
                Receiver<NetworkMessage>,
            ) -> anyhow::Result<Node>
            + std::marker::Send
            + 'static,
    {
        let (control, handler) = channel;
        let dispatcher = Dispatcher::new();
        let inner_loop = {
            std::thread::Builder::new()
                .name("routing_service_main_loop".to_string())
                .spawn(|| Self::inner_main_loop(handler, dispatcher, node_factory))
                .unwrap()
        };
        (control, inner_loop)
    }

    pub fn join_thread(&mut self, thread_id: ThreadIdentifier) {
        let _ = self.control.send(Command::JoinThread(thread_id));
    }

    #[cfg(test)]
    pub fn stub() -> (Self, Receiver<Command>) {
        let (tx, rx) = std::sync::mpsc::channel();
        (Self { control: tx }, rx)
    }

    fn create_node_thread<F>(
        dispatcher: &mut Dispatcher,
        thread_identifier: ThreadIdentifier,
        parent_block_id: Option<BlockIdentifier>,
        node_factory: &mut F,
    ) -> anyhow::Result<Node>
    where
        F: FnMut(
                Option<BlockIdentifier>,
                &ThreadIdentifier,
                Receiver<NetworkMessage>,
            ) -> anyhow::Result<Node>
            + std::marker::Send,
    {
        tracing::trace!("NetworkMessageRouter: add sender for thread: {thread_identifier:?}");
        let (incoming_messages_sender, incoming_messages_receiver) = std::sync::mpsc::channel();
        dispatcher.add_route(thread_identifier, incoming_messages_sender);
        node_factory(parent_block_id, &thread_identifier, incoming_messages_receiver)
    }

    fn route(dispatcher: &Dispatcher, message: NetworkMessage, poisoned_queue: &mut PoisonedQueue) {
        let dispatcher_result = dispatcher.dispatch(message);
        match dispatcher_result {
            Ok(()) => {}
            Err(DispatchError::NoRoute(_thread_identifier, msg)) => {
                // TODO: Received block for unexpected thread, skip it for now
                tracing::warn!("Received block from unexpected thread: {msg:?}");
                poisoned_queue.push(msg);
            }
            Err(DispatchError::DestinationClosed(thread_identifier, _msg)) => {
                panic!("DestinationClosed {}", thread_identifier);
                // todo!();
                // tracing::trace!(
                //     "Received network message for closed destination: {thread_identifier:?}"
                // );
            }
        }
    }

    fn inner_main_loop<F>(
        control: Receiver<Command>,
        mut dispatcher: Dispatcher,
        mut node_factory: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(
                Option<BlockIdentifier>,
                &ThreadIdentifier,
                Receiver<NetworkMessage>,
            ) -> anyhow::Result<Node>
            + std::marker::Send,
    {
        use Command::*;
        let mut poisoned_queue = PoisonedQueue::new(MAX_POISONED_QUEUE_SIZE);
        std::thread::scope(|s| -> anyhow::Result<()> {
            loop {
                match control.recv() {
                    Err(e) => {
                        tracing::error!(
                            "NetworkMessageRouter: common receiver was disconnected: {e}"
                        );
                        anyhow::bail!(e)
                    }
                    Ok(command) => match command {
                        //                    Stop => break,
                        Route(message) => Self::route(&dispatcher, message, &mut poisoned_queue),
                        StartThread((thread_identifier, parent_block_identifier)) => {
                            if dispatcher.has_route(&thread_identifier) {
                                continue;
                            }
                            let mut node = Self::create_node_thread(
                                &mut dispatcher,
                                thread_identifier,
                                Some(parent_block_identifier),
                                &mut node_factory,
                            )
                            .expect("Must be able to create node instances");
                            std::thread::Builder::new()
                                .name(format!("{}", &thread_identifier))
                                .spawn_scoped(s, move || {
                                    tracing::trace!("Starting thread: {}", &thread_identifier);
                                    let thread_exit_result = node.execute();
                                    tracing::trace!(
                                        "Thread {} exited with result: {:?}",
                                        &thread_identifier,
                                        &thread_exit_result
                                    );
                                    (thread_exit_result, thread_identifier)
                                })
                                .unwrap();
                            poisoned_queue
                                .retain(|message| dispatcher.dispatch(message.clone()).is_err());
                        }
                        JoinThread(thread_identifier) => {
                            if dispatcher.has_route(&thread_identifier) {
                                continue;
                            }
                            let mut node = Self::create_node_thread(
                                &mut dispatcher,
                                thread_identifier,
                                None,
                                &mut node_factory,
                            )
                            .expect("Must be able to create node instances");
                            node.is_spawned_from_node_sync = true;
                            std::thread::Builder::new()
                                .name(format!("{}", &thread_identifier))
                                .spawn_scoped(s, move || {
                                    tracing::trace!("Starting thread: {}", &thread_identifier);
                                    let thread_exit_result = node.execute();
                                    tracing::trace!(
                                        "Thread {} exited with result: {:?}",
                                        &thread_identifier,
                                        &thread_exit_result
                                    );
                                    (thread_exit_result, thread_identifier)
                                })
                                .unwrap();
                            poisoned_queue
                                .retain(|message| dispatcher.dispatch(message.clone()).is_err());
                        }
                    },
                }
            }
            // Ok(())
        })
    }

    fn inner_network_messages_forwarding_loop(
        inbound_network: Receiver<NetworkMessage>,
        control: Sender<Command>,
    ) -> anyhow::Result<()> {
        loop {
            match inbound_network.recv() {
                Err(e) => {
                    tracing::error!("NetworkMessageRouter: common receiver was disconnected: {e}");
                    anyhow::bail!(e)
                }
                Ok(message) => control.send(Command::Route(message))?,
            }
        }
    }
}

impl crate::multithreading::threads_tracking_service::Subscriber for RoutingService {
    fn handle_start_thread(
        &mut self,
        parent_block: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
    ) {
        let _ = self.control.send(Command::StartThread((*thread_id, parent_block.clone())));
    }

    fn handle_stop_thread(&mut self, _last_block: &BlockIdentifier, _thread_id: &ThreadIdentifier) {
        // Note: No reason to add this method since the collapsing thread should just exit.
        // ...
    }
}
