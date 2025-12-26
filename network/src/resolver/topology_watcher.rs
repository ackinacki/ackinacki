use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;
use std::time::Duration;
use std::time::Instant;

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
    peers_live_time: Vec<Instant>,
    topology_tx: tokio::sync::watch::Sender<NetTopology<PeerId>>,
}

impl<PeerId: Clone + PartialEq + Eq + Ord + Display + Debug + Hash> TopologyWatcher<PeerId> {
    pub fn new(
        metrics: Option<NetMetrics>,
        config: WatchGossipConfig<PeerId>,
        live_peers: Vec<GossipPeer<PeerId>>,
        topology_tx: tokio::sync::watch::Sender<NetTopology<PeerId>>,
    ) -> Self {
        let mut watcher =
            Self { metrics, config, topology_tx, peers: vec![], peers_live_time: vec![] };
        watcher.merge_with_live(live_peers);
        watcher.send_new_topology();
        watcher
    }

    pub fn refresh(
        &mut self,
        config: WatchGossipConfig<PeerId>,
        live_peers: Vec<GossipPeer<PeerId>>,
        live_nodes_len: usize,
    ) {
        let old_config = self.config.clone();
        let old_peers = self.peers.clone();

        self.config = config;
        self.drain_expired();
        self.merge_with_live(live_peers);

        if let Some(metrics) = self.metrics.as_ref() {
            metrics.report_gossip_peers(self.peers.len(), live_nodes_len as u64);
        }

        if self.config != old_config {
            tracing::trace!(
                endpoint = endpoint_info(&self.config.endpoint),
                "Gossip watcher has updated config"
            );
        }

        if self.peers != old_peers {
            let topology = self.send_new_topology();
            tracing::info!(
                target: "monit",
                subscribe = subscribe_info(topology.my_subscribe_plan()),
                peers = peers_info(topology.peer_resolver()),
                "Gossip watcher has updated topology: {topology:?}"
            );
        }
    }

    fn send_new_topology(&self) -> NetTopology<PeerId> {
        let topology = NetTopology::new(&self.config, &self.peers);
        self.topology_tx.send_replace(topology.clone());
        topology
    }

    fn drain_expired(&mut self) {
        if self.config.peer_ttl_seconds == 0 {
            self.peers.clear();
            self.peers_live_time.clear();
        } else {
            let ttl = Duration::from_secs(self.config.peer_ttl_seconds);
            for i in (0..self.peers.len()).rev() {
                if self.peers_live_time[i].elapsed() > ttl {
                    self.peers.remove(i);
                    self.peers_live_time.remove(i);
                }
            }
        }
    }

    fn merge_with_live(&mut self, live_peers: Vec<GossipPeer<PeerId>>) {
        // Merge with live peers and update live time.
        // Merged peer list is always sorted by (peer_id, peer_addr) to make peer list comparable.
        for live_peer in live_peers {
            match self.peers.binary_search_by_key(&peer_key(&live_peer), peer_key) {
                Ok(existing_index) => {
                    self.peers[existing_index] = live_peer;
                    self.peers_live_time[existing_index] = Instant::now();
                }
                Err(insertion_index) => {
                    self.peers.insert(insertion_index, live_peer);
                    self.peers_live_time.insert(insertion_index, Instant::now());
                }
            }
        }
    }
}

fn peer_key<PeerId>(peer: &GossipPeer<PeerId>) -> (&PeerId, SocketAddr) {
    (&peer.id, peer.node_protocol_addr)
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
