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
    Proxy(Vec<SocketAddr>),
}

#[derive(Clone, PartialEq)]
pub struct WatchGossipConfig {
    pub trusted_pubkeys: HashSet<transport_layer::VerifyingKey>,
}

pub async fn watch_gossip<PeerId>(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut config_rx: tokio::sync::watch::Receiver<WatchGossipConfig>,
    strategy: SubscribeStrategy<PeerId>,
    chitchat: ChitchatRef,
    subscribe_tx: tokio::sync::watch::Sender<Vec<Vec<SocketAddr>>>,
    peers_tx: tokio::sync::watch::Sender<HashMap<PeerId, PeerData>>,
    metrics: Option<NetMetrics>,
) where
    PeerId: Clone + Display + Send + Sync + Hash + Eq + FromStr<Err: Display> + 'static,
{
    let mut live_nodes_rx = chitchat.lock().live_nodes_watcher();
    let mut config = config_rx.borrow().clone();
    let mut subscribe = HashMap::new();
    let mut peers = HashMap::new();
    let mut subscribe_to_send = Vec::new();
    let mut peers_to_send = HashMap::new();
    loop {
        let live_nodes_len = refresh(&strategy, &chitchat, &mut subscribe, &mut peers, &config);

        if let Some(metrics) = metrics.as_ref() {
            metrics.report_gossip_peers(peers.len(), live_nodes_len as u64);
        }

        let new_subscribe_to_send = subscribe.keys().cloned().collect::<Vec<_>>();
        let new_peers_to_send = peers
            .iter()
            .map(|(peer_id, (peer_data, _))| (peer_id.clone(), peer_data.clone()))
            .collect();
        let subscribe_to_send_changed = new_subscribe_to_send != subscribe_to_send;
        let peers_to_send_changed = new_peers_to_send != peers_to_send;
        if subscribe_to_send_changed || peers_to_send_changed {
            tracing::trace!(
                strategy = strategy_info(&strategy),
                subscribe = subscribe_info(&subscribe),
                peers = peers_info(&peers),
                "Gossip watcher updated"
            );
        }
        if subscribe_to_send_changed {
            subscribe_to_send = new_subscribe_to_send;
            subscribe_tx.send_replace(subscribe_to_send.clone());
        }

        if peers_to_send_changed {
            peers_to_send = new_peers_to_send;
            peers_tx.send_replace(peers_to_send.clone());
        }

        tokio::select! {
            sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                break;
            },
            sender = config_rx.changed() => if sender.is_err() {
                break;
            } else {
                config = config_rx.borrow().clone();
            },
            sender = live_nodes_rx.changed() => if sender.is_err() {
                break;
            }
        }
    }
    tracing::trace!("Gossip watcher stopped");
}

fn refresh<PeerId>(
    strategy: &SubscribeStrategy<PeerId>,
    chitchat: &ChitchatRef,
    subscribe: &mut HashMap<Vec<SocketAddr>, HashSet<transport_layer::VerifyingKey>>,
    peers: &mut HashMap<PeerId, (PeerData, HashSet<transport_layer::VerifyingKey>)>,
    config: &WatchGossipConfig,
) -> usize
where
    PeerId: Clone + Display + FromStr<Err: Display> + Send + Sync + Hash + Eq + 'static,
{
    let chitchat = chitchat.lock();
    let trusted_pubkeys =
        HashSet::<&transport_layer::VerifyingKey>::from_iter(config.trusted_pubkeys.iter());

    let mut live_nodes_len = 0;
    for chitchat_id in chitchat.live_nodes() {
        live_nodes_len += 1;
        let Some(peer) =
            chitchat.node_state(chitchat_id).and_then(GossipPeer::<PeerId>::try_get_from)
        else {
            continue;
        };

        let verify_pubkey = match (!trusted_pubkeys.is_empty(), &peer.pubkey_signature) {
            (true, Some((pubkey, _))) => Some(pubkey),
            (true, None) => continue,
            (false, _) => None,
        };

        let subscribe_addrs = match strategy {
            SubscribeStrategy::Peer(self_id) => {
                if peer.id != *self_id {
                    peer_subscribe_addrs(peer.advertise_addr, &peer.proxies)
                } else {
                    vec![]
                }
            }
            SubscribeStrategy::Proxy(self_addrs) => {
                if self_addrs.iter().any(|x| peer.proxies.contains(x)) {
                    vec![peer.advertise_addr]
                } else {
                    peer_subscribe_addrs(peer.advertise_addr, &peer.proxies)
                }
            }
        };

        if !subscribe_addrs.is_empty() && !subscribe.contains_key(&subscribe_addrs) {
            subscribe.insert(subscribe_addrs.clone(), HashSet::new());
        }
        if let Some((peer_data, _)) = peers.get_mut(&peer.id) {
            peer_data.peer_addr = peer.advertise_addr;
            peer_data.bk_api_socket = peer.bk_api_socket;
        } else {
            peers.insert(
                peer.id.clone(),
                (
                    PeerData { peer_addr: peer.advertise_addr, bk_api_socket: peer.bk_api_socket },
                    HashSet::new(),
                ),
            );
        }

        if let Some(pubkey) = verify_pubkey {
            let is_trusted = trusted_pubkeys.contains(pubkey);
            if !subscribe_addrs.is_empty() {
                verify_pubkey_in(subscribe, &subscribe_addrs, pubkey, is_trusted, |v| v);
            }
            verify_pubkey_in(peers, &peer.id, pubkey, is_trusted, |(_, x)| x);
        }
    }
    live_nodes_len
}

fn verify_pubkey_in<K, V, F>(
    map: &mut HashMap<K, V>,
    key: &K,
    pubkey: &transport_layer::VerifyingKey,
    is_trusted: bool,
    get_pubkeys: F,
) where
    K: Eq + Hash,
    F: Fn(&mut V) -> &mut HashSet<transport_layer::VerifyingKey>,
{
    let mut remove = false;
    if let Some(v) = map.get_mut(key) {
        let trusted_pubkeys = get_pubkeys(v);
        if is_trusted {
            trusted_pubkeys.insert(*pubkey);
        } else {
            trusted_pubkeys.remove(pubkey);
            remove = trusted_pubkeys.is_empty();
        }
    }
    if remove {
        map.remove(key);
    }
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
        SubscribeStrategy::Proxy(addrs) => format!("Proxy({})", addrs.iter().join(",")),
    }
}
fn subscribe_info(
    subscribe: &HashMap<Vec<SocketAddr>, HashSet<transport_layer::VerifyingKey>>,
) -> String {
    subscribe.keys().map(|addrs| format!("[{}]", addrs.iter().join(","))).join(",")
}

fn peers_info<P>(peers: &HashMap<P, (PeerData, HashSet<transport_layer::VerifyingKey>)>) -> String
where
    P: Display,
{
    peers
        .iter()
        .map(|(id, (peer_data, _))| {
            format!("{}: {}", &id.to_string().as_str()[0..4], peer_data.peer_addr)
        })
        .join(",")
}
