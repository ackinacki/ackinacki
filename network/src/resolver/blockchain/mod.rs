mod accounts;
mod node_db;

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::hash::Hash;

use itertools::Itertools;
pub use node_db::NodeDb;
use tvm_block::Account;
use tvm_types::AccountId;
use url::Url;

use crate::resolver::blockchain::accounts::collect_bk_set;
use crate::resolver::blockchain::accounts::Bk;

pub trait BkSetProvider {
    fn get_bk_set(&self) -> Vec<AccountId>;
}

pub trait AccountProvider {
    fn get_account(&self, id: &AccountId) -> Option<Account>;
}

pub async fn watch_blockchain<PeerId, B, A>(
    bk_set_provider: B,
    account_provider: A,
    self_id: PeerId,
    subscribe_tx: tokio::sync::watch::Sender<Vec<Vec<Url>>>,
    peers_tx: tokio::sync::watch::Sender<HashMap<PeerId, Url>>,
) where
    B: BkSetProvider + Send + Sync + 'static,
    A: AccountProvider + Send + Sync + 'static,
    PeerId: Display + Clone + Hash + Eq + From<AccountId>,
{
    let mut subscribe = Vec::new();
    let mut peers = HashMap::new();
    loop {
        (subscribe, peers) =
            refresh(&self_id, &bk_set_provider, &account_provider, subscribe, peers).await;
        let _ = subscribe_tx.send_replace(subscribe.clone());
        let _ = peers_tx.send_replace(peers.clone());
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        tracing::trace!("Gossip watcher updated");
    }
}

async fn refresh<PeerId, B, A>(
    self_id: &PeerId,
    bk_set_provider: &B,
    account_provider: &A,
    _old_subscribe: Vec<Vec<Url>>,
    old_peers: HashMap<PeerId, Url>,
) -> (Vec<Vec<Url>>, HashMap<PeerId, Url>)
where
    B: BkSetProvider + Send + Sync + 'static,
    A: AccountProvider + Send + Sync + 'static,
    PeerId: Display + Clone + Hash + Eq + From<AccountId>,
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
            tracing::error!("Failed to collect publishers: {err}");
        }
    }

    let peers = bk_set
        .iter()
        .filter_map(|(id, (url, _))| url.as_ref().map(|x| (id.clone(), x.clone())))
        .collect();
    let subscribe = bk_set
        .into_iter()
        .filter_map(
            |(id, (peer_url, proxies))| {
                if id != *self_id {
                    Some(peer_subscribe(peer_url, proxies))
                } else {
                    None
                }
            },
        )
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

fn peer_subscribe(peer_url: Option<Url>, proxies: HashSet<Url>) -> Vec<Url> {
    if !proxies.is_empty() {
        proxies.into_iter().collect()
    } else if let Some(url) = peer_url {
        vec![url]
    } else {
        Vec::default()
    }
}

fn peers_info<PeerId>(nodes: &HashMap<PeerId, Url>) -> String
where
    PeerId: Display,
{
    nodes.iter().map(|(id, url)| format!("{}: {}", &id.to_string().as_str()[0..4], url)).join(",")
}
