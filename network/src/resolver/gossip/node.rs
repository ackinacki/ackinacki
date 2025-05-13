use std::fmt;
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
    pub bm_api_socket: Option<StringSocketAddr>,
    pub bk_api_socket: Option<StringSocketAddr>,
}

impl<PeerId> GossipPeer<PeerId>
where
    PeerId: FromStr<Err = anyhow::Error> + fmt::Display,
{
    const ADVERTISE_ADDR_KEY: &'static str = "node_advertise_addr";
    const BK_API_SOCKET_KEY: &'static str = "bk_api_socket";
    const BM_API_SOCKET_KEY: &'static str = "bm_api_socket";
    const ID_KEY: &'static str = "node_id";
    const PROXIES_KEY: &'static str = "node_proxies";

    pub fn new(
        id: PeerId,
        advertise_addr: SocketAddr,
        proxies: Vec<Url>,
        bm_api_socket: Option<StringSocketAddr>,
        bk_api_socket: Option<StringSocketAddr>,
    ) -> Self {
        Self { id, advertise_addr, proxies, bm_api_socket, bk_api_socket }
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
        let bm_api_socket = node_state.get(Self::BM_API_SOCKET_KEY).map(StringSocketAddr::from);
        let bk_api_socket = node_state.get(Self::BK_API_SOCKET_KEY).map(StringSocketAddr::from);
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
        Some(Self { id, advertise_addr, proxies, bm_api_socket, bk_api_socket })
    }

    pub fn set_to(&self, node_state: &mut NodeState) {
        node_state.set(Self::ADVERTISE_ADDR_KEY, self.advertise_addr);
        if !self.proxies.is_empty() {
            if let Ok(proxies) = serde_json::to_string(&self.proxies) {
                node_state.set(Self::PROXIES_KEY, proxies);
            }
        }
        node_state.set(Self::ID_KEY, self.id.to_string());
        if let Some(endpoint) = &self.bm_api_socket {
            node_state.set(Self::BM_API_SOCKET_KEY, endpoint);
        }
        if let Some(endpoint) = &self.bk_api_socket {
            node_state.set(Self::BK_API_SOCKET_KEY, endpoint);
        }
    }
}

impl<PeerId: fmt::Display> fmt::Display for GossipPeer<PeerId> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GossipPeer {{ id: {}, advertise_addr: {}, proxies: [{}], bm_api_socket: {}, bk_api_socket: {} }}",
            self.id,
            self.advertise_addr,
            self.proxies.iter().map(|url| url.to_string()).collect::<Vec<_>>().join(", "),
            self.bm_api_socket.as_ref().map_or("None".to_string(), |s| s.to_string()),
            self.bk_api_socket.as_ref().map_or("None".to_string(), |s| s.to_string()),
        )
    }
}
