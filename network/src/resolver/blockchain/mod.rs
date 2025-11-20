mod accounts;
mod node_db;

use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;

use itertools::Itertools;
pub use node_db::NodeDb;
use tvm_block::Account;
use tvm_types::UInt256;

use crate::config::SocketAddrSet;
use crate::resolver::blockchain::accounts::collect_bk_set;
use crate::resolver::blockchain::accounts::Bk;
use crate::topology::NetEndpoint;
use crate::topology::NetPeer;

pub trait BkSetProvider {
    fn get_bk_set(&self) -> Vec<UInt256>;
}

pub trait AccountProvider {
    fn get_account(&self, id: &UInt256) -> Option<Account>;
}

pub async fn watch_blockchain<PeerId, B, A>(
    bk_set_provider: B,
    account_provider: A,
    self_peer_id: PeerId,
    subscribe_tx: tokio::sync::watch::Sender<Vec<NetEndpoint<PeerId>>>,
    peers_tx: tokio::sync::watch::Sender<HashMap<PeerId, Vec<NetPeer<PeerId>>>>,
) where
    B: BkSetProvider + Send + Sync + 'static,
    A: AccountProvider + Send + Sync + 'static,
    PeerId: Display + Clone + Eq + From<UInt256> + Hash,
{
    let mut subscribe = Vec::new();
    let mut peers = HashMap::new();
    loop {
        (subscribe, peers) =
            refresh(&self_peer_id, &bk_set_provider, &account_provider, subscribe, peers).await;
        let _ = subscribe_tx.send_replace(subscribe.clone());
        let _ = peers_tx.send_replace(peers.clone());
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        tracing::trace!("Gossip watcher updated");
    }
}

async fn refresh<PeerId, B, A>(
    self_peer_id: &PeerId,
    bk_set_provider: &B,
    account_provider: &A,
    _old_subscribe: Vec<NetEndpoint<PeerId>>,
    old_peers: HashMap<PeerId, Vec<NetPeer<PeerId>>>,
) -> (Vec<NetEndpoint<PeerId>>, HashMap<PeerId, Vec<NetPeer<PeerId>>>)
where
    B: BkSetProvider + Send + Sync + 'static,
    A: AccountProvider + Send + Sync + 'static,
    PeerId: Display + Clone + Eq + From<UInt256> + Hash,
{
    let mut bk_set = HashMap::<PeerId, _>::new();
    // let mut peers = HashMap::new();

    for id in bk_set_provider.get_bk_set() {
        let bk = match account_provider.get_account(&id) {
            Some(account) => Bk(account),
            None => {
                continue;
            }
        };
        if let Err(err) = collect_bk_set(account_provider, &bk, &mut bk_set) {
            // TODO: metrics
            tracing::error!("Failed to collect publishers: {err}");
        }
    }

    let peers = bk_set
        .iter()
        .filter_map(|(peer_id, (addr, _))| {
            addr.as_ref()
                .map(|x| (peer_id.clone(), vec![NetPeer::with_id_and_addr(peer_id.clone(), *x)]))
        })
        .collect();
    let subscribe = bk_set
        .into_iter()
        .filter_map(|(peer_id, (peer_addr, proxies))| {
            if peer_id != *self_peer_id {
                Some(peer_subscribe(peer_id, peer_addr, proxies))
            } else {
                None
            }
        })
        .collect();
    if peers != old_peers {
        let new_info = peers_info(&peers);
        let old_info = peers_info(&old_peers);
        if new_info != old_info {
            tracing::trace!("Gossip peers updated: {new_info}");
        }
    }
    (subscribe, peers)
}

fn peer_subscribe<PeerId: Display>(
    peer_id: PeerId,
    peer_addr: Option<SocketAddr>,
    proxies: SocketAddrSet,
) -> NetEndpoint<PeerId> {
    if !proxies.is_empty() {
        NetEndpoint::Proxy(proxies.clone())
    } else if let Some(addr) = peer_addr {
        NetEndpoint::Peer(NetPeer::with_id_and_addr(peer_id, addr))
    } else {
        NetEndpoint::Proxy(Default::default())
    }
}

fn peers_info<PeerId: Display>(peers: &HashMap<PeerId, Vec<NetPeer<PeerId>>>) -> String {
    peers
        .values()
        .flatten()
        .map(|peer| {
            format!("{}: {}", &peer.id.to_string().chars().take(6).collect::<String>(), peer.addr)
        })
        .join(",")
}
