use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use http_server::BlockKeeperSetUpdate;
use http_server::ExtMsgFeedback;
use http_server::ExtMsgFeedbackList;
use http_server::FeedbackError;
use http_server::FeedbackErrorCode;
use network::metrics::NetMetrics;
use network::pub_sub::connection::IncomingMessage;
use parking_lot::Mutex;
use telemetry_utils::mpsc::instrumented_channel;
use telemetry_utils::mpsc::InstrumentedReceiver;
use telemetry_utils::mpsc::InstrumentedSender;
use telemetry_utils::now_ms;
use tokio::sync::oneshot;
use tvm_block::GetRepresentationHash;

use super::dispatcher::DispatchError;
use super::dispatcher::Dispatcher;
use super::poisoned_queue::PoisonedQueue as PQueue;
use crate::helper::metrics::BlockProductionMetrics;
use crate::node::services::sync::ExternalFileSharesBased;
use crate::node::NetworkMessage;
use crate::node::Node as NodeImpl;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::utilities::thread_spawn_critical::SpawnCritical;

// TODO: make into a config.
// TODO: calculate an acceptable and balanced value.
const MAX_POISONED_QUEUE_SIZE: usize = 10000;

type FeedbackMessage = (NetworkMessage, Option<oneshot::Sender<ExtMsgFeedback>>);
type FeedbackRegistry = HashMap<String, oneshot::Sender<ExtMsgFeedback>>;

type PoisonedQueue = PQueue<NetworkMessage>;

type Node = NodeImpl<ExternalFileSharesBased, rand::prelude::SmallRng>;

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
    pub cmd_sender: InstrumentedSender<Command>,
    pub feedback_sender: InstrumentedSender<ExtMsgFeedbackList>,
}

impl RoutingService {
    pub fn new(
        inbound_network_receiver: InstrumentedReceiver<IncomingMessage>,
        inbound_ext_messages_receiver: InstrumentedReceiver<FeedbackMessage>,
        metrics: Option<BlockProductionMetrics>,
        net_metrics: Option<NetMetrics>,
    ) -> (
        RoutingService,
        InstrumentedReceiver<Command>,
        std::thread::JoinHandle<()>,
        std::thread::JoinHandle<()>,
    ) {
        let (cmd_sender, cmd_receiver) =
            instrumented_channel(metrics.clone(), crate::helper::metrics::ROUTING_COMMAND_CHANNEL);
        let metrics_clone = metrics.clone();
        let forwarding_thread = {
            let cmd_sender_clone = cmd_sender.clone();
            std::thread::Builder::new()
                .name("routing_service_network_messages_forwarding_loop".to_string())
                .spawn_critical(move || {
                    Self::inner_network_messages_forwarding_loop(
                        inbound_network_receiver,
                        cmd_sender_clone,
                        net_metrics,
                        metrics_clone,
                    )
                })
                .unwrap()
        };
        let (feedback_sender, feedback_receiver) =
            instrumented_channel(metrics.clone(), crate::helper::metrics::INBOUND_EXT_CHANNEL);
        let forwarding_ext_messages_thread = {
            let cmd_sender_clone = cmd_sender.clone();
            std::thread::Builder::new()
                .name("routing_service_external_messages_forwarding_loop".to_string())
                .spawn_critical(move || {
                    Self::inner_external_messages_forwarding_loop(
                        inbound_ext_messages_receiver,
                        feedback_receiver,
                        cmd_sender_clone,
                    )
                })
                .unwrap()
        };
        (
            RoutingService { cmd_sender, feedback_sender },
            cmd_receiver,
            forwarding_thread,
            forwarding_ext_messages_thread,
        )
    }

    pub fn start<F>(
        channel: (RoutingService, InstrumentedReceiver<Command>),
        bk_set_updates_tx: InstrumentedSender<BlockKeeperSetUpdate>,
        node_factory: F,
    ) -> (Self, std::thread::JoinHandle<()>)
    where
        F: FnMut(
                Option<BlockIdentifier>,
                &ThreadIdentifier,
                Receiver<NetworkMessage>,
                Sender<NetworkMessage>,
                InstrumentedSender<ExtMsgFeedbackList>,
                InstrumentedSender<BlockKeeperSetUpdate>,
            ) -> anyhow::Result<Node>
            + std::marker::Send
            + 'static,
    {
        let (control, handler) = channel;
        let dispatcher = Dispatcher::new();
        let inner_loop = {
            let feedback_sender = control.feedback_sender.clone();
            std::thread::Builder::new()
                .name("routing_service_main_loop".to_string())
                .spawn_critical(|| {
                    Self::inner_main_loop(
                        handler,
                        feedback_sender,
                        bk_set_updates_tx,
                        dispatcher,
                        node_factory,
                    )
                })
                .unwrap()
        };
        tracing::debug!("NetworkMessageRouter: started");
        (control, inner_loop)
    }

    pub fn join_thread(&mut self, thread_id: ThreadIdentifier) {
        let _ = self.cmd_sender.send(Command::JoinThread(thread_id));
    }

    #[cfg(test)]
    pub fn stub() -> (Self, InstrumentedReceiver<Command>) {
        let (tx, rx) = instrumented_channel(
            Option::<BlockProductionMetrics>::None,
            crate::helper::metrics::ROUTING_COMMAND_CHANNEL,
        );
        let (feedback_sender, _feedback_receiver) = instrumented_channel(
            Option::<BlockProductionMetrics>::None,
            crate::helper::metrics::INBOUND_EXT_CHANNEL,
        );
        (Self { cmd_sender: tx, feedback_sender }, rx)
    }

    fn create_node_thread<F>(
        dispatcher: &mut Dispatcher,
        feedback_sender: InstrumentedSender<ExtMsgFeedbackList>,
        thread_identifier: ThreadIdentifier,
        parent_block_id: Option<BlockIdentifier>,
        bk_set_updates_tx: InstrumentedSender<BlockKeeperSetUpdate>,
        node_factory: &mut F,
    ) -> anyhow::Result<Node>
    where
        F: FnMut(
                Option<BlockIdentifier>,
                &ThreadIdentifier,
                Receiver<NetworkMessage>,
                Sender<NetworkMessage>,
                InstrumentedSender<ExtMsgFeedbackList>,
                InstrumentedSender<BlockKeeperSetUpdate>,
            ) -> anyhow::Result<Node>
            + std::marker::Send,
    {
        tracing::trace!("NetworkMessageRouter: add sender for thread: {thread_identifier:?}");
        let (incoming_messages_sender, incoming_messages_receiver) = std::sync::mpsc::channel();
        dispatcher.add_route(thread_identifier, incoming_messages_sender.clone());
        node_factory(
            parent_block_id,
            &thread_identifier,
            incoming_messages_receiver,
            incoming_messages_sender,
            feedback_sender,
            bk_set_updates_tx,
        )
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
        control: InstrumentedReceiver<Command>,
        feedback_sender: InstrumentedSender<ExtMsgFeedbackList>,
        bk_set_updates_tx: InstrumentedSender<BlockKeeperSetUpdate>,
        mut dispatcher: Dispatcher,
        mut node_factory: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(
                Option<BlockIdentifier>,
                &ThreadIdentifier,
                Receiver<NetworkMessage>,
                Sender<NetworkMessage>,
                InstrumentedSender<ExtMsgFeedbackList>,
                InstrumentedSender<BlockKeeperSetUpdate>,
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
                    Ok(command) => {
                        match command {
                            //                    Stop => break,
                            Route(message) => {
                                Self::route(&dispatcher, message, &mut poisoned_queue)
                            }
                            StartThread((thread_identifier, parent_block_identifier)) => {
                                if dispatcher.has_route(&thread_identifier) {
                                    continue;
                                }
                                let mut node = Self::create_node_thread(
                                    &mut dispatcher,
                                    feedback_sender.clone(),
                                    thread_identifier,
                                    Some(parent_block_identifier),
                                    bk_set_updates_tx.clone(),
                                    &mut node_factory,
                                )
                                .expect("Must be able to create node instances");
                                std::thread::Builder::new()
                                    .name(format!("{}", &thread_identifier))
                                    .spawn_scoped_critical(s, move || {
                                        tracing::trace!("Starting thread: {}", &thread_identifier);
                                        node.execute()
                                    })
                                    .unwrap();
                                poisoned_queue.retain(|message| {
                                    dispatcher.dispatch(message.clone()).is_err()
                                });
                            }
                            JoinThread(thread_identifier) => {
                                if dispatcher.has_route(&thread_identifier) {
                                    continue;
                                }
                                let mut node = Self::create_node_thread(
                                    &mut dispatcher,
                                    feedback_sender.clone(),
                                    thread_identifier,
                                    None,
                                    bk_set_updates_tx.clone(),
                                    &mut node_factory,
                                )
                                .expect("Must be able to create node instances");
                                node.is_spawned_from_node_sync = true;
                                std::thread::Builder::new()
                                    .name(format!("{}", &thread_identifier))
                                    .spawn_scoped_critical(s, move || {
                                        tracing::trace!("Starting thread: {}", &thread_identifier);
                                        node.execute()
                                    })
                                    .unwrap();
                                poisoned_queue.retain(|message| {
                                    dispatcher.dispatch(message.clone()).is_err()
                                });
                            }
                        }
                    }
                }
            }
            // Ok(())
        })
    }

    fn inner_network_messages_forwarding_loop(
        inbound_network: InstrumentedReceiver<IncomingMessage>,
        cmd_sender: InstrumentedSender<Command>,
        net_metrics: Option<NetMetrics>,
        metrics: Option<BlockProductionMetrics>,
    ) -> anyhow::Result<()> {
        loop {
            match inbound_network.recv() {
                Ok(incoming) => {
                    let message_received_ms = incoming.message.received_at;
                    if let Some(message) = incoming.finish(&net_metrics) {
                        metrics.as_ref().inspect(|m| {
                            // duration between block received as bytes and received by node as block
                            m.report_rcv_as_bytes_to_rcv_by_node(
                                now_ms().saturating_sub(message_received_ms),
                                incoming.message.label,
                            )
                        });
                        cmd_sender.send(Command::Route(message))?
                    }
                }
                Err(err) => {
                    tracing::error!(
                        "NetworkMessageRouter: common receiver was disconnected: {err}"
                    );
                    anyhow::bail!(err)
                }
            }
        }
    }

    fn inner_external_messages_forwarding_loop(
        inbound_ext_messages: InstrumentedReceiver<FeedbackMessage>,
        feedback_receiver: InstrumentedReceiver<ExtMsgFeedbackList>,
        cmd_sender: InstrumentedSender<Command>,
    ) -> anyhow::Result<()> {
        // let queue_limit =
        let queue_size = Arc::new(AtomicUsize::new(0));
        let feedback_registry = Arc::new(Mutex::new(HashMap::new()));
        let feedback_loop_thread_join_handler = {
            let registry = Arc::clone(&feedback_registry);
            std::thread::Builder::new()
                .name("routing_service_ext_messages_feedback_loop".to_string())
                .spawn_critical(move || {
                    Self::inner_feedback_loop(feedback_receiver, registry, queue_size)
                })
                .unwrap()
        };
        loop {
            match inbound_ext_messages.recv() {
                Err(e) => {
                    tracing::error!(
                        "NetworkMessageRouter: external messages receiver was disconnected: {e}"
                    );
                    drop(feedback_loop_thread_join_handler);
                    anyhow::bail!("NetworkMessageRouter closed");
                }
                Ok(message) => {
                    tracing::debug!("NetworkMessageRouter: received external message");
                    let (message, sender) = message;
                    if let NetworkMessage::ExternalMessage((ref ext_message, _)) = message {
                        let message_hash = ext_message
                            .message
                            .hash()
                            .map_err(|e| anyhow::format_err!("{e}"))?
                            .to_hex_string();
                        let is_msg_exists =
                            { feedback_registry.lock().contains_key(&message_hash) };
                        if is_msg_exists {
                            if let Some(sender) = sender {
                                let feedback = ExtMsgFeedback {
                                    message_hash,
                                    error: Some(FeedbackError {
                                        code: FeedbackErrorCode::DuplicateMessage,
                                        message: None,
                                    }),
                                    ..Default::default()
                                };

                                let _ = sender.send(feedback); // warn about duplicate
                            }
                        } else {
                            feedback_registry.lock().insert(message_hash, sender.unwrap());
                        }
                        cmd_sender.send(Command::Route(message))?;
                    }
                }
            }
        }
    }

    fn inner_feedback_loop(
        feedback_receiver: InstrumentedReceiver<ExtMsgFeedbackList>,
        feedback_registry: Arc<Mutex<FeedbackRegistry>>,
        queue_size: Arc<AtomicUsize>,
    ) -> anyhow::Result<()> {
        loop {
            match feedback_receiver.recv() {
                Err(e) => {
                    tracing::error!(
                        "NetworkMessageRouter: feedback receiver was disconnected: {e}"
                    );
                }
                Ok(feedbacks) => {
                    tracing::debug!("NetworkMessageRouter: received feedback: {}", feedbacks);
                    for feedback in feedbacks.0 {
                        if let Some(sender) =
                            feedback_registry.lock().remove(&feedback.message_hash)
                        {
                            let _ = sender.send(feedback);
                        } else {
                            queue_size.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
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
        let _ = self.cmd_sender.send(Command::StartThread((*thread_id, parent_block.clone())));
    }

    fn handle_stop_thread(&mut self, _last_block: &BlockIdentifier, _thread_id: &ThreadIdentifier) {
        // Note: No reason to add this method since the collapsing thread should just exit.
        // ...
    }
}
