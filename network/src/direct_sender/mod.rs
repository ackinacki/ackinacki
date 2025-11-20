mod peer_sender;
mod sender;

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Instant;

use sender::DirectSender;
use transport_layer::NetTransport;

use crate::config::NetworkConfig;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::IncomingSender;
use crate::topology::NetTopology;

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
pub async fn run_direct_sender<PeerId, Transport>(
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    network_config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
    transport: Transport,
    metrics: Option<NetMetrics>,
    messages_rx: tokio::sync::mpsc::UnboundedReceiver<(
        DirectReceiver<PeerId>,
        NetMessage,
        Instant,
    )>,
    outgoing_broadcast_tx: tokio::sync::broadcast::Sender<OutgoingMessage<PeerId>>,
    incoming_reply_tx: IncomingSender<PeerId>,
    net_topology_rx: tokio::sync::watch::Receiver<NetTopology<PeerId>>,
) where
    Transport: NetTransport + 'static,
    PeerId: Display + Debug + Hash + Eq + Clone + Send + Sync + FromStr<Err: Display> + 'static,
{
    DirectSender::new(
        shutdown_rx,
        network_config_rx,
        transport,
        metrics,
        messages_rx,
        outgoing_broadcast_tx,
        incoming_reply_tx,
        net_topology_rx,
    )
    .run()
    .await;
}

enum PeerEvent<PeerId>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    SenderStopped(PeerId, SocketAddr),
}

enum PeerCommand {
    SendMessage(NetMessage, Instant),
    Stop,
}

fn peer_info<PeerId>(id: &PeerId, addr: SocketAddr) -> String
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
{
    format!("{id} ({addr})")
}
