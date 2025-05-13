use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;
use std::sync::Arc;

use chitchat::Chitchat;
use itertools::Itertools;
use url::Url;

use crate::network::PeerData;
use crate::resolver::GossipPeer;
use crate::try_url_from_socket_addr;

pub enum SubscribeStrategy<PeerId> {
    Peer(PeerId),
    Proxy(Url),
}

pub async fn watch_gossip<PeerId>(
    strategy: SubscribeStrategy<PeerId>,
    chitchat: Arc<tokio::sync::Mutex<Chitchat>>,
    subscribe_tx: Option<tokio::sync::watch::Sender<Vec<Vec<Url>>>>,
    peers_tx: Option<tokio::sync::watch::Sender<HashMap<PeerId, PeerData>>>,
) where
    PeerId: Clone + Display + Send + Sync + Hash + Eq + FromStr<Err = anyhow::Error> + 'static,
{
    let mut live_nodes_rx = chitchat.lock().await.live_nodes_watcher();
    let mut subscribe = Vec::new();
    let mut peers: HashMap<PeerId, PeerData> = HashMap::new();
    loop {
        (subscribe, peers) = refresh(&strategy, &chitchat, subscribe, peers).await;
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
        if live_nodes_rx.changed().await.is_err() {
            break;
        }
    }
    tracing::trace!("Gossip watcher stopped");
}

async fn refresh<PeerId>(
    strategy: &SubscribeStrategy<PeerId>,
    chitchat: &Arc<tokio::sync::Mutex<Chitchat>>,
    _old_subscribe: Vec<Vec<Url>>,
    _old_peers: HashMap<PeerId, PeerData>,
) -> (Vec<Vec<Url>>, HashMap<PeerId, PeerData>)
where
    PeerId: Clone + Display + FromStr<Err = anyhow::Error> + Send + Sync + Hash + Eq + 'static,
{
    let chitchat = chitchat.lock().await;
    let mut subscribe = Vec::new();
    let mut peers = HashMap::new();
    for chitchat_id in chitchat.live_nodes() {
        if let Some(peer) =
            chitchat.node_state(chitchat_id).and_then(GossipPeer::<PeerId>::try_get_from)
        {
            if let Some(peer_url) = try_url_from_socket_addr(&peer.advertise_addr) {
                match strategy {
                    SubscribeStrategy::Peer(self_id) => {
                        if peer.id != *self_id {
                            subscribe.push(peer_subscribe_urls(&peer_url, &peer.proxies));
                        }
                    }
                    SubscribeStrategy::Proxy(self_url) => {
                        if peer.proxies.contains(self_url) {
                            subscribe.push(vec![peer_url.clone()]);
                        } else {
                            subscribe.push(peer_subscribe_urls(&peer_url, &peer.proxies));
                        }
                    }
                }
                peers.insert(
                    peer.id.clone(),
                    PeerData { peer_url, bk_api_socket: peer.bk_api_socket },
                );
            }
        }
    }
    (subscribe, peers)
}

fn peer_subscribe_urls(peer_url: &Url, proxies: &[Url]) -> Vec<Url> {
    if proxies.is_empty() {
        vec![peer_url.clone()]
    } else {
        proxies.to_vec()
    }
}

fn strategy_info<P: Display>(strategy: &SubscribeStrategy<P>) -> String {
    match strategy {
        SubscribeStrategy::Peer(id) => format!("Peer({})", id),
        SubscribeStrategy::Proxy(proxy_url) => format!("Proxy({})", proxy_url),
    }
}
fn subscribe_info(subscribe: &[Vec<Url>]) -> String {
    subscribe.iter().map(|urls| format!("[{}]", urls.iter().join(","))).join(",")
}

fn peers_info<P>(peers: &HashMap<P, PeerData>) -> String
where
    P: Display,
{
    peers
        .iter()
        .map(|(id, peer_data)| {
            format!("{}: {}", &id.to_string().as_str()[0..4], peer_data.peer_url)
        })
        .join(",")
}
