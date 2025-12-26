use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;

use chitchat::ChitchatRef;

use crate::metrics::NetMetrics;
use crate::resolver::gossip::collect_gossip_peers;
use crate::resolver::topology_watcher::TopologyWatcher;
use crate::topology::NetEndpoint;
use crate::topology::NetTopology;

#[derive(Clone, PartialEq)]
pub struct WatchGossipConfig<PeerId> {
    pub endpoint: NetEndpoint<PeerId>,
    pub max_nodes_with_same_id: usize,
    pub override_subscribe: Vec<NetEndpoint<PeerId>>,
    pub trusted_pubkeys: HashSet<transport_layer::VerifyingKey>,
    pub peer_ttl_seconds: u64,
}

#[allow(clippy::too_many_arguments)]
pub async fn watch_gossip<PeerId>(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut config_rx: tokio::sync::watch::Receiver<WatchGossipConfig<PeerId>>,
    chitchat: ChitchatRef,
    net_topology_tx: tokio::sync::watch::Sender<NetTopology<PeerId>>,
    metrics: Option<NetMetrics>,
) where
    PeerId:
        Clone + Display + Debug + Send + Sync + Hash + Eq + Ord + FromStr<Err: Display> + 'static,
{
    let mut live_nodes_rx = chitchat.lock().live_nodes_watcher();
    let (peers, _) = collect_gossip_peers(&chitchat, &config_rx.borrow());
    let mut watcher =
        TopologyWatcher::new(metrics, config_rx.borrow().clone(), peers, net_topology_tx);
    let stop_reason = loop {
        let (peers, live_nodes_len) = collect_gossip_peers(&chitchat, &config_rx.borrow());
        watcher.refresh(config_rx.borrow().clone(), peers, live_nodes_len);
        tokio::select! {
            shutdown_changed = shutdown_rx.changed() => if shutdown_changed.is_err() || *shutdown_rx.borrow() {
                break "shutdown signal received";
            },
            config_changed = config_rx.changed() => if config_changed.is_err() {
                break "config channel closed";
            },
            live_nodes_changed = live_nodes_rx.changed() => if live_nodes_changed.is_err() {
                break "live nodes channel closed";
            }
        }
    };
    tracing::trace!("Gossip watcher stopped: {stop_reason}");
}
