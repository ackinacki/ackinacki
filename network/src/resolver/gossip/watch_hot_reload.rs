use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;

use chitchat::ChitchatId;
use chitchat::ChitchatRef;
use chitchat::NodeState;

use crate::metrics::NetMetrics;
use crate::resolver::gossip::collect_gossip_peers;
use crate::resolver::topology_watcher::TopologyWatcher;
use crate::resolver::WatchGossipConfig;
use crate::topology::NetTopology;

#[allow(clippy::too_many_arguments)]
pub async fn watch_gossip<
    PeerId: Clone + PartialEq + Eq + Ord + Display + Debug + FromStr<Err: Display> + Hash,
>(
    metrics: Option<NetMetrics>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut config_rx: tokio::sync::watch::Receiver<WatchGossipConfig<PeerId>>,
    mut chitchat_rx: tokio::sync::watch::Receiver<Option<ChitchatRef>>,
    topology_tx: tokio::sync::watch::Sender<NetTopology<PeerId>>,
) {
    let (default_live_nodes_tx, _) =
        tokio::sync::watch::channel(BTreeMap::<ChitchatId, NodeState>::default());
    let mut live_nodes_rx = watch_live_nodes(&chitchat_rx, &default_live_nodes_tx);
    let peers = if let Some(chitchat) = &*chitchat_rx.borrow() {
        collect_gossip_peers(chitchat, &config_rx.borrow()).0
    } else {
        vec![]
    };
    let mut watcher = TopologyWatcher::new(metrics, config_rx.borrow().clone(), peers, topology_tx);

    let finish_reason = loop {
        let (peers, live_nodes_len) = if let Some(chitchat) = &*chitchat_rx.borrow() {
            collect_gossip_peers(chitchat, &config_rx.borrow())
        } else {
            (vec![], 0)
        };
        watcher.refresh(config_rx.borrow().clone(), peers, live_nodes_len);

        tokio::select! {
            shutdown_changed = shutdown_rx.changed() => if shutdown_changed.is_err() || *shutdown_rx.borrow() {
                break "shutdown signal received";
            },
            config_changed = config_rx.changed() => if config_changed.is_err() {
                break "config channel closed";
            },
            chitchat_changed = chitchat_rx.changed() => if chitchat_changed.is_ok() {
                live_nodes_rx = watch_live_nodes(&chitchat_rx, &default_live_nodes_tx);
            } else {
                break "chitchat channel closed";
            },
            live_nodes_changed = live_nodes_rx.changed() => if live_nodes_changed.is_err() {
                live_nodes_rx = watch_live_nodes(&chitchat_rx, &default_live_nodes_tx);
                tracing::debug!("Gossip watcher live nodes channel closed. Reopened.");
            }
        }
    };
    tracing::trace!("Gossip watcher finished: {finish_reason}");
}

fn watch_live_nodes(
    chitchat_rx: &tokio::sync::watch::Receiver<Option<ChitchatRef>>,
    default_tx: &tokio::sync::watch::Sender<BTreeMap<ChitchatId, NodeState>>,
) -> tokio::sync::watch::Receiver<BTreeMap<ChitchatId, NodeState>> {
    if let Some(chitchat) = &*chitchat_rx.borrow() {
        chitchat.lock().live_nodes_watcher()
    } else {
        default_tx.subscribe()
    }
}
