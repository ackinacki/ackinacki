use std::fmt::Display;
use std::net::SocketAddr;
use std::str::FromStr;

use chitchat::NodeState;
use url::Url;

use crate::socket_addr::StringSocketAddr;
use crate::socket_addr::ToOneSocketAddr;

pub struct GossipPeer<PeerId> {
    pub id: PeerId,
    pub advertise_addr: SocketAddr,
    pub proxies: Vec<Url>,
    pub public_endpoint: Option<String>,
}

impl<PeerId> GossipPeer<PeerId>
where
    PeerId: FromStr<Err = anyhow::Error> + Display,
{
    const ADVERTISE_ADDR_KEY: &'static str = "node_advertise_addr";
    const ID_KEY: &'static str = "node_id";
    const PROXIES_KEY: &'static str = "node_proxies";
    const PUBLIC_ENDPOINT_KEY: &'static str = "public_endpoint";

    pub fn new(
        id: PeerId,
        advertise_addr: SocketAddr,
        proxies: Vec<Url>,
        public_endpoint: Option<String>,
    ) -> Self {
        Self { id, advertise_addr, proxies, public_endpoint }
    }

    pub fn try_get_from(node_state: &NodeState) -> Option<Self> {
        let peer_id_str = node_state.get(Self::ID_KEY)?;
        let id = PeerId::from_str(peer_id_str)
            .inspect_err(|err| {
                tracing::warn!(peer_id_str, "Invalid value for {}: {err}", Self::ID_KEY)
            })
            .ok()?;
        let Some(addr) = node_state.get(Self::ADVERTISE_ADDR_KEY) else {
            tracing::error!("Missing value for {}", Self::ADVERTISE_ADDR_KEY);
            return None;
        };
        let advertise_addr = StringSocketAddr::from(addr)
            .try_to_socket_addr()
            .inspect_err(|err| {
                tracing::warn!(addr, "Invalid gossip {}: {err}", Self::ADVERTISE_ADDR_KEY);
            })
            .ok()?;
        let public_endpoint = node_state.get(Self::PUBLIC_ENDPOINT_KEY).map(String::from);
        let proxies = node_state
            .get(Self::PROXIES_KEY)
            .map(|x| {
                serde_json::from_str(x)
                    .inspect_err(|err| {
                        tracing::warn!(x, "Invalid value for {}: {err}", Self::PROXIES_KEY);
                    })
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        Some(Self { id, advertise_addr, proxies, public_endpoint })
    }

    pub fn set_to(&self, node_state: &mut NodeState) {
        node_state.set(Self::ADVERTISE_ADDR_KEY, self.advertise_addr);
        if !self.proxies.is_empty() {
            if let Ok(proxies) = serde_json::to_string(&self.proxies) {
                node_state.set(Self::PROXIES_KEY, proxies);
            }
        }
        node_state.set(Self::ID_KEY, self.id.to_string());
        if let Some(endpoint) = &self.public_endpoint {
            node_state.set(Self::PUBLIC_ENDPOINT_KEY, endpoint);
        }
    }
}
