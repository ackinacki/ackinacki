use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;
use std::time::Instant;

use transport_layer::NetTransport;

use crate::config::NetworkConfig;
use crate::direct_sender::peer_info;
use crate::direct_sender::peer_sender::PeerSender;
use crate::direct_sender::PeerEvent;
use crate::direct_sender::RESOLVE_RETRY_TIMEOUT;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::network::PeerData;
use crate::pub_sub::connection::MessageDelivery;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::spawn_critical_task;
use crate::pub_sub::IncomingSender;
use crate::DeliveryPhase;
use crate::DirectReceiver;
use crate::SendMode;

pub struct DirectSender<PeerId, Transport>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    network_config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
    network_config: NetworkConfig,
    transport: Transport,
    metrics: Option<NetMetrics>,
    messages_rx:
        tokio::sync::mpsc::UnboundedReceiver<(DirectReceiver<PeerId>, NetMessage, Instant)>,
    outgoing_reply_tx: tokio::sync::broadcast::Sender<OutgoingMessage>,
    incoming_reply_tx: IncomingSender,
    peer_resolver_rx: tokio::sync::watch::Receiver<HashMap<PeerId, Vec<PeerData>>>,
    unresolved_peers: HashMap<PeerId, Vec<(NetMessage, Instant)>>,
    resolved_peers:
        HashMap<PeerId, Vec<(SocketAddr, tokio::sync::mpsc::Sender<(NetMessage, Instant)>)>>,
    peer_events_tx: tokio::sync::mpsc::UnboundedSender<PeerEvent<PeerId>>,
    peer_events_rx: tokio::sync::mpsc::UnboundedReceiver<PeerEvent<PeerId>>,
}

impl<PeerId, Transport> DirectSender<PeerId, Transport>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
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
        outgoing_reply_tx: tokio::sync::broadcast::Sender<OutgoingMessage>,
        incoming_reply_tx: IncomingSender,
        peer_resolver_rx: tokio::sync::watch::Receiver<HashMap<PeerId, Vec<PeerData>>>,
    ) -> Self {
        let network_config = network_config_rx.borrow().clone();
        let (peer_events_tx, peer_events_rx) =
            tokio::sync::mpsc::unbounded_channel::<PeerEvent<PeerId>>();
        Self {
            shutdown_rx,
            network_config,
            network_config_rx,
            transport,
            metrics,
            messages_rx,
            outgoing_reply_tx,
            incoming_reply_tx,
            peer_resolver_rx,
            unresolved_peers: HashMap::new(),
            resolved_peers: HashMap::new(),
            peer_events_tx,
            peer_events_rx,
        }
    }

    pub(crate) async fn run(&mut self) {
        tracing::info!(
            "Direct sender started with host id {}",
            self.network_config.credential.identity_prefix()
        );
        loop {
            tokio::select! {
                sender = self.shutdown_rx.changed() => if sender.is_err() || *self.shutdown_rx.borrow() {
                    break;
                } else {
                    continue;
                },
                sender = self.network_config_rx.changed() => if sender.is_err() {
                    break;
                } else {
                    self.network_config = self.network_config_rx.borrow().clone();
                    continue;
                },
                result = self.messages_rx.recv() => match result {
                    Some((receiver, net_message, buffer_duration)) => {
                        self.dispatch_message(receiver, net_message, buffer_duration);
                    }
                    None => {
                        break;
                    }
                },
                result = self.peer_events_rx.recv() => match result {
                    Some(event) => self.handle_peer_event(event),
                    None => {
                        break;
                    }
                },
            }
        }
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
                if let Some(resolved) = self.resolved_peers.get(&id) {
                    for (addr, sender) in resolved {
                        self.try_send_to_peer(
                            &id,
                            *addr,
                            sender,
                            net_message.clone(),
                            buffer_duration,
                        );
                    }
                } else if let Some(unresolved) = self.unresolved_peers.get_mut(&id) {
                    unresolved.push((net_message, buffer_duration));
                } else {
                    self.unresolved_peers.insert(id.clone(), vec![(net_message, buffer_duration)]);
                    self.start_peer_resolver(id);
                };
            }
            DirectReceiver::Addr(addr) => {
                let _ = self.outgoing_reply_tx.send(OutgoingMessage {
                    message: net_message,
                    delivery: MessageDelivery::Addr(addr),
                    duration_before_transfer: Instant::now(),
                });
            }
        };
    }

    fn handle_peer_event(&mut self, event: PeerEvent<PeerId>) {
        match event {
            PeerEvent::AddrsResolved(id, addrs) => {
                let Some(messages) = self.unresolved_peers.remove(&id) else {
                    return;
                };
                let mut resolved = Vec::new();
                for addr in addrs {
                    let tx = self.start_peer_sender(id.clone(), addr);
                    for message in &messages {
                        self.try_send_to_peer(&id, addr, &tx, message.0.clone(), message.1)
                    }
                    resolved.push((addr, tx));
                }
                self.resolved_peers.insert(id.clone(), resolved);
            }
            PeerEvent::SenderStopped(id, addr) => {
                tracing::trace!(peer = peer_info(&id, addr), "Peer sender stopped");
                let remove = if let Some(peers) = self.resolved_peers.get_mut(&id) {
                    peers.retain(|(x, _)| *x != addr);
                    peers.is_empty()
                } else {
                    false
                };
                if remove {
                    self.resolved_peers.remove(&id);
                }
            }
        }
    }

    fn try_send_to_peer(
        &self,
        id: &PeerId,
        addr: SocketAddr,
        tx: &tokio::sync::mpsc::Sender<(NetMessage, Instant)>,
        message: NetMessage,
        buffer_duration: Instant,
    ) {
        let label = message.label.clone();
        let is_sent = match tx.try_send((message, buffer_duration)) {
            Ok(()) => true,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::error!(
                    peer = peer_info(id, addr),
                    msg_type = label,
                    broadcast = false,
                    "Message delivery: forwarding to peer sender failed, sender is lagged"
                );
                false
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                let _ = self.peer_events_tx.send(PeerEvent::SenderStopped(id.clone(), addr));
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

    fn start_peer_resolver(&self, id: PeerId) {
        spawn_critical_task(
            "Direct peer resolver",
            peer_resolver(
                id,
                self.shutdown_rx.clone(),
                self.peer_resolver_rx.clone(),
                self.peer_events_tx.clone(),
            ),
        );
    }

    fn start_peer_sender(
        &self,
        id: PeerId,
        addr: SocketAddr,
    ) -> tokio::sync::mpsc::Sender<(NetMessage, Instant)> {
        let (tx, rx) = tokio::sync::mpsc::channel::<(NetMessage, Instant)>(100);
        let mut sender = PeerSender::new(
            id,
            addr,
            self.shutdown_rx.clone(),
            self.transport.clone(),
            self.metrics.clone(),
            self.network_config.credential.clone(),
            self.peer_events_tx.clone(),
            self.incoming_reply_tx.clone(),
        );
        spawn_critical_task("Direct peer sender", async move { sender.run(rx).await });
        tx
    }
}

async fn peer_resolver<PeerId>(
    id: PeerId,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut peers_rx: tokio::sync::watch::Receiver<HashMap<PeerId, Vec<PeerData>>>,
    peer_events_tx: tokio::sync::mpsc::UnboundedSender<PeerEvent<PeerId>>,
) where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    tracing::trace!(peer = id.to_string(), "Peer resolver loop started");
    loop {
        tokio::select! {
            sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                tracing::trace!(peer = id.to_string(), "Peer resolver loop finished");
                break;
            } else {
                continue;
            },
            addrs = resolve_peer_addrs(&mut peers_rx, id.clone()) => {
                let _ = peer_events_tx.send(PeerEvent::AddrsResolved(id.clone(), addrs));
                break;
            },
        }
    }
}

async fn resolve_peer_addrs<PeerId>(
    peers_rx: &mut tokio::sync::watch::Receiver<HashMap<PeerId, Vec<PeerData>>>,
    id: PeerId,
) -> Vec<SocketAddr>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    let mut attempt = 0;
    loop {
        if let Some(peer_data) = peers_rx.borrow().get(&id) {
            return peer_data.iter().map(|x| x.peer_addr).collect();
        }
        tracing::warn!(peer_id = id.to_string(), attempt, "Failed to resolve peer addr");
        tokio::time::sleep(RESOLVE_RETRY_TIMEOUT).await;
        attempt += 1;
    }
}
