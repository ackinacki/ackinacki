use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;
use std::str::FromStr;

use chitchat::ChitchatRef;
use itertools::Itertools;

use crate::metrics::NetMetrics;
use crate::network::PeerData;
use crate::resolver::GossipPeer;

pub enum SubscribeStrategy<PeerId> {
    Peer(PeerId),
    Proxy(SocketAddr),
}

pub async fn watch_gossip<PeerId>(
    strategy: SubscribeStrategy<PeerId>,
    chitchat: ChitchatRef,
    subscribe_tx: Option<tokio::sync::watch::Sender<Vec<Vec<SocketAddr>>>>,
    peers_tx: Option<tokio::sync::watch::Sender<HashMap<PeerId, PeerData>>>,
    metrics: Option<NetMetrics>,
) where
    PeerId: Clone + Display + Send + Sync + Hash + Eq + FromStr<Err: Display> + 'static,
{
    let mut live_nodes_rx = chitchat.lock().live_nodes_watcher();
    let mut subscribe = Vec::new();
    let mut peers: HashMap<PeerId, PeerData> = HashMap::new();
    loop {
        let mut live_nodes_total = 0;
        let refreshed =
            refresh(&strategy, &chitchat, &mut subscribe, &mut peers, &mut live_nodes_total);
        if refreshed {
            if let Some(metrics) = metrics.as_ref() {
                metrics.report_gossip_peers(peers.len(), live_nodes_total);
            }
            tracing::trace!(
                strategy = strategy_info(&strategy),
                subscribe = subscribe_info(&subscribe),
                peers = peers_info(&peers),
                "Gossip watcher updated"
            );
            if let Some(tx) = &subscribe_tx {
                let _ = tx.send_replace(subscribe.clone());
            }
            if let Some(tx) = &peers_tx {
                let _ = tx.send_replace(peers.clone());
            }
        }
        if live_nodes_rx.changed().await.is_err() {
            break;
        }
    }
    tracing::trace!("Gossip watcher stopped");
}

fn refresh<PeerId>(
    strategy: &SubscribeStrategy<PeerId>,
    chitchat: &ChitchatRef,
    subscribe: &mut Vec<Vec<SocketAddr>>,
    peers: &mut HashMap<PeerId, PeerData>,
    live_nodes_total: &mut u64,
) -> bool
where
    PeerId: Clone + Display + FromStr<Err: Display> + Send + Sync + Hash + Eq + 'static,
{
    let chitchat = chitchat.lock();
    let mut already_subscribed = HashSet::<Vec<SocketAddr>>::from_iter(subscribe.iter().cloned());
    let mut refreshed = false;

    for chitchat_id in chitchat.live_nodes() {
        *live_nodes_total += 1;
        if let Some(peer) =
            chitchat.node_state(chitchat_id).and_then(GossipPeer::<PeerId>::try_get_from)
        {
            let subscribe_addrs = match strategy {
                SubscribeStrategy::Peer(self_id) => {
                    if peer.id != *self_id {
                        peer_subscribe_addrs(peer.advertise_addr, &peer.proxies)
                    } else {
                        vec![]
                    }
                }
                SubscribeStrategy::Proxy(self_addr) => {
                    if peer.proxies.contains(self_addr) {
                        vec![peer.advertise_addr]
                    } else {
                        peer_subscribe_addrs(peer.advertise_addr, &peer.proxies)
                    }
                }
            };
            if !subscribe_addrs.is_empty() && !already_subscribed.contains(&subscribe_addrs) {
                subscribe.push(subscribe_addrs.clone());
                already_subscribed.insert(subscribe_addrs);
                refreshed = true;
            }
            if let Some(existing) = peers.get_mut(&peer.id) {
                if existing.peer_addr != peer.advertise_addr
                    || existing.bk_api_socket != peer.bk_api_socket
                {
                    existing.peer_addr = peer.advertise_addr;
                    existing.bk_api_socket = peer.bk_api_socket;
                    refreshed = true;
                }
            } else {
                peers.insert(
                    peer.id.clone(),
                    PeerData { peer_addr: peer.advertise_addr, bk_api_socket: peer.bk_api_socket },
                );
                refreshed = true;
            }
        }
    }
    refreshed
}

fn peer_subscribe_addrs(peer_addr: SocketAddr, proxies: &[SocketAddr]) -> Vec<SocketAddr> {
    if proxies.is_empty() {
        vec![peer_addr]
    } else {
        proxies.to_vec()
    }
}

fn strategy_info<P: Display>(strategy: &SubscribeStrategy<P>) -> String {
    match strategy {
        SubscribeStrategy::Peer(id) => format!("Peer({id})"),
        SubscribeStrategy::Proxy(proxy_url) => format!("Proxy({proxy_url})"),
    }
}
fn subscribe_info(subscribe: &[Vec<SocketAddr>]) -> String {
    subscribe.iter().map(|addrs| format!("[{}]", addrs.iter().join(","))).join(",")
}

fn peers_info<P>(peers: &HashMap<P, PeerData>) -> String
where
    P: Display,
{
    peers
        .iter()
        .map(|(id, peer_data)| {
            format!("{}: {}", &id.to_string().as_str()[0..4], peer_data.peer_addr)
        })
        .join(",")
}
