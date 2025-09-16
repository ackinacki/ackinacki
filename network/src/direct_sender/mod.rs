mod peer_sender;
mod sender;

use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use sender::DirectSender;
use transport_layer::NetTransport;

use crate::config::NetworkConfig;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::network::PeerData;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::IncomingSender;

const RESOLVE_RETRY_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(PartialEq, Hash, Eq, Clone, Debug)]
pub enum DirectReceiver<PeerId>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    Peer(PeerId),
    Addr(SocketAddr),
}

impl<PeerId> From<PeerId> for DirectReceiver<PeerId>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    fn from(value: PeerId) -> Self {
        Self::Peer(value)
    }
}

impl<PeerId> Display for DirectReceiver<PeerId>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DirectReceiver::Peer(id) => f.write_str(&id.to_string()),
            DirectReceiver::Addr(addr) => f.write_str(&addr.to_string()),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_direct_sender<Transport, PeerId>(
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
    peers_rx: tokio::sync::watch::Receiver<HashMap<PeerId, PeerData>>,
) where
    Transport: NetTransport + 'static,
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    DirectSender::new(
        shutdown_rx,
        network_config_rx,
        transport,
        metrics,
        messages_rx,
        outgoing_reply_tx,
        incoming_reply_tx,
        peers_rx,
    )
    .run()
    .await;
}

struct DirectPeer<PeerId>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    state: parking_lot::Mutex<DirectPeerState<PeerId>>,
    messages_tx: tokio::sync::mpsc::Sender<(NetMessage, Instant)>,
}

struct DirectPeerState<PeerId>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    id: Option<PeerId>,
    addr: Option<SocketAddr>,
    messages_rx: Option<tokio::sync::mpsc::Receiver<(NetMessage, Instant)>>,
}

impl<PeerId> DirectPeer<PeerId>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    fn new(id: Option<PeerId>, addr: Option<SocketAddr>) -> Self {
        let (messages_tx, messages_rx) = tokio::sync::mpsc::channel(100);
        Self {
            state: parking_lot::Mutex::new(DirectPeerState {
                id,
                addr,
                messages_rx: Some(messages_rx),
            }),
            messages_tx,
        }
    }
}

impl<PeerId> Display for DirectPeer<PeerId>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        if let Some(id) = &state.id {
            f.write_str(&id.to_string())?;
            if let Some(addr) = state.addr {
                write!(f, " ({addr})")?;
            }
        } else if let Some(addr) = state.addr {
            f.write_str(&addr.to_string())?;
        }
        Ok(())
    }
}

enum PeerEvent<PeerId>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    AddrResolved(Arc<DirectPeer<PeerId>>, PeerId, SocketAddr),
    SenderStopped(Arc<DirectPeer<PeerId>>),
}
