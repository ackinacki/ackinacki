use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;

use gossip::gossip_peer::GossipPeer;
use itertools::Itertools;

use crate::metrics::NetMetrics;
use crate::resolver::WatchGossipConfig;
use crate::topology::NetEndpoint;
use crate::topology::NetPeer;
use crate::topology::NetTopology;

pub struct TopologyWatcher<PeerId: Clone + PartialEq + Eq + Display + Debug + Hash> {
    metrics: Option<NetMetrics>,
    config: WatchGossipConfig<PeerId>,
    peers: Vec<GossipPeer<PeerId>>,
    topology_tx: tokio::sync::watch::Sender<NetTopology<PeerId>>,
}

impl<PeerId: Clone + PartialEq + Eq + Display + Debug + Hash> TopologyWatcher<PeerId> {
    pub fn new(
        metrics: Option<NetMetrics>,
        config: WatchGossipConfig<PeerId>,
        mut peers: Vec<GossipPeer<PeerId>>,
        topology_tx: tokio::sync::watch::Sender<NetTopology<PeerId>>,
    ) -> Self {
        dedup_peers(&mut peers);
        topology_tx.send_replace(NetTopology::new(&config, &peers));
        Self { metrics, config, peers, topology_tx }
    }

    pub fn refresh(
        &mut self,
        config: WatchGossipConfig<PeerId>,
        mut peers: Vec<GossipPeer<PeerId>>,
        live_nodes_len: usize,
    ) {
        dedup_peers(&mut peers);
        if config == self.config && peers == self.peers {
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.report_gossip_peers(peers.len(), live_nodes_len as u64);
            }
            return;
        }
        let topology = NetTopology::new(&config, &peers);
        tracing::trace!("Gossip updated topology {topology:?}");
        self.config = config;
        self.peers = peers;
        self.topology_tx.send_replace(topology.clone());
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.report_gossip_peers(topology.peer_resolver().len(), live_nodes_len as u64);
        }
        tracing::trace!(
            subject = endpoint_info(&self.config.endpoint),
            subscribe = subscribe_info(topology.my_subscribe_plan()),
            peers = peers_info(topology.peer_resolver()),
            "Network topology watcher updated"
        );
    }
}

fn dedup_peers<PeerId: Clone + Eq + Hash + Display>(peers: &mut Vec<GossipPeer<PeerId>>) {
    let mut seen = HashSet::new();
    peers.retain(|peer| seen.insert((peer.id.clone(), peer.advertise_addr)));
    peers.sort_by_key(|peer| (peer.id.to_string(), peer.advertise_addr));
}

fn endpoint_info<PeerId: Display>(endpoint: &NetEndpoint<PeerId>) -> String {
    match endpoint {
        NetEndpoint::Peer(peer) => format!("Peer({})", peer.id),
        NetEndpoint::Proxy(addrs) => format!("Proxy({})", addrs_to_string(addrs.iter())),
    }
}
fn subscribe_info<PeerId>(subscribe: &[NetEndpoint<PeerId>]) -> String {
    subscribe.iter().map(|x| addrs_to_string(x.subscribe_addrs().iter())).join(",")
}

fn peers_info<P>(peers: &HashMap<P, Vec<NetPeer<P>>>) -> String
where
    P: Display,
{
    peers
        .iter()
        .map(|(id, peers)| {
            format!(
                "{}: {}",
                &id.to_string().chars().take(6).collect::<String>(),
                addrs_to_string(peers.iter().map(|x| &x.addr))
            )
        })
        .join(",")
}

fn addrs_to_string<'a>(addrs: impl IntoIterator<Item = &'a SocketAddr>) -> String {
    let mut s = String::new();
    let mut count = 0;
    for addr in addrs {
        if !s.is_empty() {
            s.push(',');
        }
        s.push_str(&addr.to_string());
        count += 1;
    }
    if count > 0 {
        s.insert(0, '[');
        s.push(']');
    }
    s
}
