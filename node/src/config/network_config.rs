// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
use itertools::Itertools;
use network::socket_addr::StringSocketAddr;
use network::socket_addr::ToOneSocketAddr;
use serde::Deserialize;
use serde::Serialize;

use super::default::default_bind;
use super::default::default_block_manager_listen;
use super::default::default_buffer_size;
use super::default::default_gossip_listen;

// TODO: need to rework gossip arguments, now it has some advertised parameters
// that are not used (e.g. ["node_state"]["node_id"] section)
/// Network settings
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NetworkConfig {
    /// Socket to listen other nodes messages (QUIC UDP).
    /// Defaults to "127.0.0.1:8500"
    #[serde(default = "default_bind")]
    pub bind: StringSocketAddr,

    /// Public node socket address that will be advertised with gossip (QUIC
    /// UDP).
    pub node_advertise_addr: StringSocketAddr,

    /// UDP socket address to listen gossip.
    /// Defaults to "127.0.0.1:10000"
    #[serde(default = "default_gossip_listen")]
    pub gossip_listen_addr: StringSocketAddr,

    /// Gossip advertise socket address.
    /// Defaults to `bind` address
    pub gossip_advertise_addr: Option<StringSocketAddr>,

    /// Gossip seed nodes socket addresses.
    pub gossip_seeds: Vec<StringSocketAddr>,

    /// Socket to listen for lite node requests (QUIC UDP).
    #[serde(default = "default_block_manager_listen")]
    pub block_manager_listen_addr: StringSocketAddr,

    /// Static storages urls (e.g. <https://example.com/storage/>)
    #[serde(default)]
    pub static_storages: Vec<url::Url>,

    /// Socket address for SDK API
    pub api_addr: String,

    /// Network send buffer size
    /// Defaults to 1000
    #[serde(default = "default_buffer_size")]
    pub send_buffer_size: usize,

    /// Public endpoint for this node
    pub public_endpoint: Option<String>,
}

impl NetworkConfig {
    pub fn get_gossip_seeds(&self) -> Vec<String> {
        self.gossip_seeds
            .iter()
            .map(|s| {
                s.try_to_socket_addr().map_err(|e| {
                    tracing::error!(
                        "Failed to convert gossip seed {} to SocketAddr, skip it ({})",
                        s,
                        e
                    );
                    e
                })
            })
            .filter_map(|res| res.map(|socket| socket.to_string()).ok())
            .collect_vec()
    }
}
