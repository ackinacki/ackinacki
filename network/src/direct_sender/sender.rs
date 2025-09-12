use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use transport_layer::NetTransport;

use crate::config::NetworkConfig;
use crate::direct_sender::peer_sender::PeerSender;
use crate::direct_sender::DirectPeer;
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
    peer_resolver_rx: tokio::sync::watch::Receiver<HashMap<PeerId, PeerData>>,
    peers_by_id: HashMap<PeerId, Arc<DirectPeer<PeerId>>>,
    peers_by_addr: HashMap<SocketAddr, Arc<DirectPeer<PeerId>>>,
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
        peer_resolver_rx: tokio::sync::watch::Receiver<HashMap<PeerId, PeerData>>,
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
            peers_by_id: HashMap::new(),
            peers_by_addr: HashMap::new(),
            peer_events_tx,
            peer_events_rx,
        }
    }

    fn remove_peer(&mut self, peer: Arc<DirectPeer<PeerId>>) {
        self.peers_by_id.retain(|_, x| !Arc::ptr_eq(&peer, x));
        self.peers_by_addr.retain(|_, x| !Arc::ptr_eq(&peer, x));
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
                let peer = if let Some(peer) = self.peers_by_id.get(&id) {
                    peer.clone()
                } else {
                    let peer = Arc::new(DirectPeer::new(Some(id.clone()), None));
                    self.peers_by_id.insert(id.clone(), peer.clone());
                    self.start_peer_resolver(peer.clone(), id);
                    peer
                };
                self.try_send_to_peer(&peer, net_message, buffer_duration);
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
            PeerEvent::AddrResolved(peer, id, addr) => {
                if let Some(other_peer) = self.peers_by_addr.get(&addr).cloned() {
                    other_peer.state.lock().id = Some(id.clone());
                    self.peers_by_id.insert(id, other_peer.clone());
                    self.move_messages(&peer, &other_peer);
                } else {
                    peer.state.lock().addr = Some(addr);
                    self.peers_by_addr.insert(addr, peer.clone());
                    self.start_peer_sender(peer, addr);
                }
            }
            PeerEvent::SenderStopped(peer) => {
                tracing::trace!(peer_id = peer.to_string(), "Peer sender stopped");
                self.remove_peer(peer);
            }
        }
    }

    fn move_messages(&mut self, from: &Arc<DirectPeer<PeerId>>, to: &Arc<DirectPeer<PeerId>>) {
        if let Some(mut rx) = from.state.lock().messages_rx.take() {
            while let Ok((message, buffer_duration)) = rx.try_recv() {
                self.try_send_to_peer(to, message, buffer_duration);
            }
        }
    }

    fn try_send_to_peer(
        &mut self,
        peer: &Arc<DirectPeer<PeerId>>,
        message: NetMessage,
        buffer_duration: Instant,
    ) {
        let label = message.label.clone();
        let is_sent = match peer.messages_tx.try_send((message, buffer_duration)) {
            Ok(()) => true,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::error!(
                    peer = peer.to_string(),
                    msg_type = label,
                    broadcast = false,
                    "Message delivery: forwarding to peer sender failed, sender is lagged"
                );
                false
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                let _ = self.peer_events_tx.send(PeerEvent::SenderStopped(peer.clone()));
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

    fn start_peer_resolver(&self, peer: Arc<DirectPeer<PeerId>>, id: PeerId) {
        spawn_critical_task(
            "Direct peer resolver",
            peer_resolver(
                peer.clone(),
                id,
                self.shutdown_rx.clone(),
                self.peer_resolver_rx.clone(),
                self.peer_events_tx.clone(),
            ),
        );
    }

    fn start_peer_sender(&self, peer: Arc<DirectPeer<PeerId>>, addr: SocketAddr) {
        let mut sender = PeerSender::new(
            peer.clone(),
            addr,
            self.shutdown_rx.clone(),
            self.transport.clone(),
            self.metrics.clone(),
            self.network_config.credential.clone(),
            self.peer_events_tx.clone(),
            self.incoming_reply_tx.clone(),
        );
        spawn_critical_task("Direct peer sender", async move { sender.run().await });
    }
}

async fn peer_resolver<PeerId>(
    peer: Arc<DirectPeer<PeerId>>,
    id: PeerId,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut peers_rx: tokio::sync::watch::Receiver<HashMap<PeerId, PeerData>>,
    peer_events_tx: tokio::sync::mpsc::UnboundedSender<PeerEvent<PeerId>>,
) where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    tracing::trace!(peer = peer.to_string(), "Peer resolver loop started");
    loop {
        tokio::select! {
            sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                tracing::trace!(peer = peer.to_string(), "Peer resolver loop finished");
                break;
            } else {
                continue;
            },
            addr = resolve_peer_addr(&mut peers_rx, id.clone()) => {
                let _ = peer_events_tx.send(PeerEvent::AddrResolved(peer, id, addr));
                break;
            },
        }
    }
}

async fn resolve_peer_addr<PeerId>(
    peers_rx: &mut tokio::sync::watch::Receiver<HashMap<PeerId, PeerData>>,
    id: PeerId,
) -> SocketAddr
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    let mut attempt = 0;
    loop {
        if let Some(peer_data) = peers_rx.borrow().get(&id) {
            return peer_data.peer_addr;
        }
        tracing::warn!(peer_id = id.to_string(), attempt, "Failed to resolve peer addr");
        tokio::time::sleep(RESOLVE_RETRY_TIMEOUT).await;
        attempt += 1;
    }
}
