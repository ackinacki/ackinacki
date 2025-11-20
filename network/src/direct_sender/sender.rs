use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Instant;

use transport_layer::NetTransport;

use crate::config::NetworkConfig;
use crate::direct_sender::peer_info;
use crate::direct_sender::peer_sender::PeerSender;
use crate::direct_sender::PeerCommand;
use crate::direct_sender::PeerEvent;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::MessageDelivery;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::spawn_critical_task;
use crate::pub_sub::IncomingSender;
use crate::topology::NetTopology;
use crate::DeliveryPhase;
use crate::DirectReceiver;
use crate::SendMode;

const MAX_UNRESOLVED_PEERS_MESSAGE_QUEUE_SIZE: usize = 1000;

pub struct DirectSender<PeerId, Transport>
where
    PeerId: Display + Debug + Hash + Eq + Clone + Send + Sync + 'static,
{
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    network_config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
    network_config: NetworkConfig,
    transport: Transport,
    metrics: Option<NetMetrics>,
    messages_rx:
        tokio::sync::mpsc::UnboundedReceiver<(DirectReceiver<PeerId>, NetMessage, Instant)>,
    outgoing_broadcast_tx: tokio::sync::broadcast::Sender<OutgoingMessage<PeerId>>,
    incoming_reply_tx: IncomingSender<PeerId>,
    net_topology_rx: tokio::sync::watch::Receiver<NetTopology<PeerId>>,
    net_topology: NetTopology<PeerId>,
    unresolved_peers_message_queue: VecDeque<(PeerId, NetMessage, Instant)>,
    peer_senders: HashMap<PeerId, Vec<(SocketAddr, tokio::sync::mpsc::Sender<PeerCommand>)>>,
    peer_events_tx: tokio::sync::mpsc::UnboundedSender<PeerEvent<PeerId>>,
    peer_events_rx: tokio::sync::mpsc::UnboundedReceiver<PeerEvent<PeerId>>,
}

impl<PeerId, Transport> DirectSender<PeerId, Transport>
where
    PeerId: Display + Debug + Hash + Eq + Clone + Send + Sync + FromStr<Err: Display> + 'static,
    Transport: NetTransport + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
        network_config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
        transport: Transport,
        metrics: Option<NetMetrics>,
        messages_rx: tokio::sync::mpsc::UnboundedReceiver<(
            DirectReceiver<PeerId>,
            NetMessage,
            Instant,
        )>,
        outgoing_reply_tx: tokio::sync::broadcast::Sender<OutgoingMessage<PeerId>>,
        incoming_reply_tx: IncomingSender<PeerId>,
        net_topology_rx: tokio::sync::watch::Receiver<NetTopology<PeerId>>,
    ) -> Self {
        let network_config = network_config_rx.borrow().clone();
        let (peer_events_tx, peer_events_rx) =
            tokio::sync::mpsc::unbounded_channel::<PeerEvent<PeerId>>();
        let net_topology = net_topology_rx.borrow().clone();
        Self {
            shutdown_rx,
            network_config,
            network_config_rx,
            transport,
            metrics,
            messages_rx,
            outgoing_broadcast_tx: outgoing_reply_tx,
            incoming_reply_tx,
            net_topology_rx,
            net_topology,
            unresolved_peers_message_queue: VecDeque::new(),
            peer_senders: HashMap::new(),
            peer_events_tx,
            peer_events_rx,
        }
    }

    pub(crate) async fn run(&mut self) {
        tracing::info!(
            target: "monit",
            "Direct sender started with host id {}",
            self.network_config.credential.identity_prefix()
        );
        let reason = loop {
            tokio::select! {
                shutdown_changed = self.shutdown_rx.changed() => if shutdown_changed.is_err() || *self.shutdown_rx.borrow() {
                    break "shutdown signal received";
                },
                config_changed = self.network_config_rx.changed() => if config_changed.is_ok() {
                    self.network_config = self.network_config_rx.borrow().clone();
                } else {
                    break "network config channel closed";
                },
                message = self.messages_rx.recv() => match message {
                    Some((receiver, net_message, buffer_duration)) => {
                        self.dispatch_message(receiver, net_message, buffer_duration);
                    }
                    None => {
                        break "message channel closed";
                    }
                },
                net_topology_changed = self.net_topology_rx.changed() => if net_topology_changed.is_ok() {
                    let new_topolgy = self.net_topology_rx.borrow().clone();
                    self.update_net_topology(new_topolgy);
                } else {
                    break "peer resolver channel closed";
                },
                peer_event = self.peer_events_rx.recv() => match peer_event {
                    Some(event) => self.handle_peer_event(event),
                    None => {
                        break "per event channel closed";
                    }
                },
            }
        };
        tracing::info!(
            target: "monit",
            "Direct sender stopped: {reason}",
        );
    }

    fn update_net_topology(&mut self, new_topology: NetTopology<PeerId>) {
        self.net_topology = new_topology;
        let stop_start = self.get_senders_stop_start_plan();
        for (peer_id, (stop, start)) in stop_start {
            self.stop_peer_senders(&peer_id, &stop);
            self.start_peer_senders(&peer_id, &start);
            if self.peer_senders.get(&peer_id).map(|x| x.is_empty()).unwrap_or(false) {
                self.peer_senders.remove(&peer_id);
            }
        }
        self.drain_unresolved_peers_message_queue();
    }

    fn get_senders_stop_start_plan(
        &self,
    ) -> HashMap<PeerId, (HashSet<SocketAddr>, HashSet<SocketAddr>)> {
        let mut stop_start = HashMap::new();
        for (peer_id, peer_senders) in &self.peer_senders {
            let old_addrs = peer_senders.iter().map(|(addr, _)| *addr).collect::<HashSet<_>>();
            let (stop, start) = if let Some(new_peers) = self.net_topology.resolve_peer(peer_id) {
                let new_addrs = new_peers.iter().map(|x| x.addr).collect::<HashSet<_>>();
                (
                    old_addrs.difference(&new_addrs).cloned().collect(),
                    new_addrs.difference(&old_addrs).cloned().collect(),
                )
            } else {
                (old_addrs, HashSet::new())
            };
            if !stop.is_empty() || !start.is_empty() {
                stop_start.insert(peer_id.clone(), (stop, start));
            }
        }
        stop_start
    }

    fn dispatch_message(
        &mut self,
        receiver: DirectReceiver<PeerId>,
        net_message: NetMessage,
        buffer_duration: Instant,
    ) {
        tracing::debug!(
            receiver = receiver.to_string(),
            msg_id = net_message.id,
            msg_type = net_message.label,
            broadcast = false,
            "Message delivery: accepted by peer dispatcher"
        );
        match receiver {
            DirectReceiver::Peer(id) => {
                let mut broadcast_msg = net_message.clone();
                broadcast_msg.direct_receiver = Some(id.to_string());
                let _ = self.outgoing_broadcast_tx.send(OutgoingMessage {
                    delivery: MessageDelivery::Broadcast,
                    message: broadcast_msg,
                    duration_before_transfer: buffer_duration,
                });
                let peer_senders = if let Some(senders) = self.peer_senders.get(&id) {
                    senders
                } else {
                    match self.net_topology.resolve_peer(&id) {
                        Some(peers) => {
                            self.start_peer_senders(&id, &peers.iter().map(|x| x.addr).collect());
                            self.drain_unresolved_peers_message_queue();
                            self.peer_senders.get(&id).unwrap()
                        }
                        None => {
                            tracing::error!(
                                peer = id.to_string(),
                                msg_type = net_message.label,
                                broadcast = false,
                                "Message delivery: forwarding to peer sender failed, peer is unresolved"
                            );
                            if let Some(metrics) = self.metrics.as_ref() {
                                metrics.report_error("fwd_to_peer_lagged");
                            }
                            self.unresolved_peers_message_queue.push_front((
                                id,
                                net_message,
                                buffer_duration,
                            ));
                            while self.unresolved_peers_message_queue.len()
                                > MAX_UNRESOLVED_PEERS_MESSAGE_QUEUE_SIZE
                            {
                                self.unresolved_peers_message_queue.pop_back();
                            }
                            return;
                        }
                    }
                };
                for (addr, sender) in peer_senders {
                    self.try_send_to_peer(&id, *addr, sender, net_message.clone(), buffer_duration);
                }
            }
            DirectReceiver::Addr(addr) => {
                let _ = self.outgoing_broadcast_tx.send(OutgoingMessage {
                    message: net_message,
                    delivery: MessageDelivery::Addr(addr),
                    duration_before_transfer: Instant::now(),
                });
            }
        };
    }

    fn start_peer_senders(&mut self, peer_id: &PeerId, addrs: &HashSet<SocketAddr>) {
        if addrs.is_empty() {
            return;
        }
        let mut new_senders = Vec::new();
        for addr in addrs {
            new_senders.push(self.start_peer_sender(peer_id.clone(), *addr));
        }
        if let Some(senders) = self.peer_senders.get_mut(peer_id) {
            senders.extend(new_senders);
        } else {
            self.peer_senders.insert(peer_id.clone(), new_senders);
        }
    }

    fn drain_unresolved_peers_message_queue(&mut self) {
        self.unresolved_peers_message_queue
            .retain(|(peer_id, _, _)| !self.peer_senders.contains_key(peer_id));
    }

    fn stop_peer_senders(&mut self, peer_id: &PeerId, addrs: &HashSet<SocketAddr>) {
        if addrs.is_empty() {
            return;
        }
        if let Some(peer_senders) = self.peer_senders.get_mut(peer_id) {
            peer_senders.retain(|(addr, command_tx)| {
                if addrs.contains(addr) {
                    let _ = command_tx.try_send(PeerCommand::Stop);
                    false
                } else {
                    true
                }
            });
        }
    }

    fn handle_peer_event(&mut self, event: PeerEvent<PeerId>) {
        match event {
            PeerEvent::SenderStopped(id, addr) => {
                tracing::debug!(
                    target: "monit",
                    peer = peer_info(&id, addr),
                    "Peer sender stopped",
                );
                let remove = if let Some(peers) = self.peer_senders.get_mut(&id) {
                    peers.retain(|(x, _)| *x != addr);
                    peers.is_empty()
                } else {
                    false
                };
                if remove {
                    self.peer_senders.remove(&id);
                }
            }
        }
    }

    fn try_send_to_peer(
        &self,
        id: &PeerId,
        addr: SocketAddr,
        tx: &tokio::sync::mpsc::Sender<PeerCommand>,
        message: NetMessage,
        buffer_duration: Instant,
    ) {
        let label = message.label.clone();
        let is_sent = match tx.try_send(PeerCommand::SendMessage(message, buffer_duration)) {
            Ok(()) => true,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::error!(
                    peer = peer_info(id, addr),
                    msg_type = label,
                    broadcast = false,
                    "Message delivery: forwarding to peer sender failed, sender is lagged"
                );
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.report_error("fwd_to_peer_lagged");
                }
                false
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                let _ = self.peer_events_tx.send(PeerEvent::SenderStopped(id.clone(), addr));
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.report_error("fwd_to_peer_closed");
                }
                false
            }
        };
        if !is_sent {
            self.metrics.as_ref().inspect(|x| {
                x.finish_delivery_phase(
                    DeliveryPhase::OutgoingBuffer,
                    1,
                    &label,
                    SendMode::Direct,
                    buffer_duration.elapsed(),
                )
            });
        }
    }

    fn start_peer_sender(
        &mut self,
        id: PeerId,
        addr: SocketAddr,
    ) -> (SocketAddr, tokio::sync::mpsc::Sender<PeerCommand>) {
        let (command_tx, rx) = tokio::sync::mpsc::channel::<PeerCommand>(100);
        let mut sender = PeerSender::new(
            id.clone(),
            addr,
            self.shutdown_rx.clone(),
            self.transport.clone(),
            self.metrics.clone(),
            self.network_config.credential.clone(),
            self.peer_events_tx.clone(),
            self.incoming_reply_tx.clone(),
        );
        let metrics = self.metrics.clone();
        let metrics_clone = self.metrics.clone();
        spawn_critical_task(
            "Direct_peer_sender",
            async move { sender.run(rx, metrics).await },
            metrics_clone,
        );
        for (peer_id, message, buffer_duration) in &self.unresolved_peers_message_queue {
            if *peer_id == id {
                self.try_send_to_peer(
                    peer_id,
                    addr,
                    &command_tx,
                    message.clone(),
                    *buffer_duration,
                );
            }
        }
        (addr, command_tx)
    }
}
