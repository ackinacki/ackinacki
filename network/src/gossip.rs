use std::collections::HashSet;
use std::fmt::Display;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use chitchat::Chitchat;
use chitchat::NodeState;
use tokio::sync::Mutex;
use url::Url;

use crate::direct_sender::PeerResolver;
use crate::socket_addr::StringSocketAddr;
use crate::socket_addr::ToOneSocketAddr;
use crate::NetworkPeerId;

pub fn get_live_gossip_ids(chitchat: &Chitchat, filter_self: bool) -> HashSet<String> {
    let self_gossip_id = &chitchat.self_chitchat_id().node_id;
    chitchat
        .live_nodes()
        .filter_map(|id| {
            if !filter_self || id.node_id != *self_gossip_id {
                Some(id.node_id.clone())
            } else {
                None
            }
        })
        .collect()
}

pub fn get_peer_id_and_addr<PeerId: NetworkPeerId>(
    state: &NodeState,
) -> Option<(PeerId, SocketAddr)>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    let node_id = state.get("node_id")?;
    let peer_id = PeerId::from_str(node_id)
        .inspect_err(|err| tracing::warn!(node_id, "Invalid value for node_id: {err}"))
        .ok()?;
    let Some(addr) = state.get("node_advertise_addr") else {
        tracing::error!("Missing value for node_advertise_addr");
        return None;
    };
    let addr = StringSocketAddr::from(addr)
        .try_to_socket_addr()
        .inspect_err(|err| {
            tracing::warn!(addr, "Invalid gossip node_advertise_addr: {err}");
        })
        .ok()?;
    Some((peer_id, addr))
}

pub fn get_live_peers<PeerId: NetworkPeerId>(
    chitchat: &Chitchat,
    filter_self: bool,
) -> Vec<(PeerId, SocketAddr)>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    let live_gossip_ids_without_self = get_live_gossip_ids(chitchat, filter_self);
    let mut peers = Vec::new();
    for snapshot in chitchat.state_snapshot().node_state_snapshots {
        if live_gossip_ids_without_self.contains(&snapshot.chitchat_id.node_id) {
            if let Some(id_and_addr) = get_peer_id_and_addr(&snapshot.node_state) {
                peers.push(id_and_addr);
            }
        }
    }
    peers
}

#[derive(Clone)]
pub struct GossipPeerResolver<PeerId: NetworkPeerId>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    _peer_id: std::marker::PhantomData<PeerId>,
    chitchat: Arc<Mutex<Chitchat>>,
}

impl<PeerId: NetworkPeerId> GossipPeerResolver<PeerId>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    pub fn new(chitchat: Arc<Mutex<Chitchat>>) -> Self {
        Self { _peer_id: std::marker::PhantomData, chitchat }
    }
}

#[async_trait]
impl<PeerId: NetworkPeerId> PeerResolver for GossipPeerResolver<PeerId>
where
    <PeerId as FromStr>::Err: Display + Send + Sync + 'static,
    anyhow::Error: From<<PeerId as FromStr>::Err>,
{
    type PeerId = PeerId;

    async fn resolve_peer(&self, peer_id: &PeerId) -> Option<Url> {
        let chitchat = self.chitchat.lock().await;
        for snapshot in chitchat.state_snapshot().node_state_snapshots {
            if let Some((id, addr)) = get_peer_id_and_addr::<PeerId>(&snapshot.node_state) {
                if id == *peer_id {
                    return match Url::parse(&format!("https://{}", addr)) {
                        Ok(url) => Some(url),
                        Err(err) => {
                            tracing::trace!(addr = addr.to_string(), "Invalid peer addr: {err}");
                            None
                        }
                    };
                }
            }
        }
        None
    }
}
