// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.

use std::path::PathBuf;

use itertools::Itertools;
use network::socket_addr::StringSocketAddr;
use network::socket_addr::ToOneSocketAddr;
use serde::Deserialize;
use serde::Serialize;
use typed_builder::TypedBuilder;
use url::Url;

// TODO: need to rework gossip arguments, now it has some advertised parameters
// that are not used (e.g. ["node_state"]["node_id"] section)
/// Network settings
#[derive(Serialize, Deserialize, Debug, Clone, TypedBuilder)]
pub struct NetworkConfig {
    /// Socket to listen other nodes messages (QUIC UDP).
    /// Defaults to "127.0.0.1:8500"
    #[builder(default = StringSocketAddr::from("127.0.0.1:8500".to_string()))]
    pub bind: StringSocketAddr,

    /// TLS auth cert.
    ///
    /// Node uses a TLS auth cert and key file to represent itself and prove it in two scenarios:
    /// - when a node acts as a server and accepts connections from another node or proxy;
    /// - when a node acts as a client and connects to another node or proxy.
    ///
    /// Should be a path to the `*.ca.pem` file.
    #[builder(default)]
    pub my_cert: PathBuf,

    /// TLS auth key.
    ///
    /// Node uses a TLS auth cert and key file to represent itself and prove it in two scenarios:
    /// - when a node acts as a server and accepts connections from another node or proxy;
    /// - when a node acts as a client and connects to another node or proxy.
    ///
    /// Should be a path to the `*.key.pem` file.
    #[builder(default)]
    pub my_key: PathBuf,

    /// Subscribes.
    ///
    /// Node uses a subscribes to subscribe for blocks and other broadcast protocol messages.
    #[builder(default)]
    pub subscribe: Vec<Url>,

    /// Files and directories with TLS certificates (*.ca.pem), required to verify
    /// server certificate when node establish client connection to other node or proxy.
    #[builder(default)]
    pub peer_certs: Vec<PathBuf>,

    /// Public node socket address that will be advertised with gossip (QUIC
    /// UDP).
    pub node_advertise_addr: StringSocketAddr,

    /// UDP socket address to listen gossip.
    /// Defaults to "127.0.0.1:10000"
    #[builder(default = StringSocketAddr::from("127.0.0.1:10000".to_string()))]
    pub gossip_listen_addr: StringSocketAddr,

    /// Gossip advertise socket address.
    /// Defaults to `bind` address
    #[builder(default)]
    pub gossip_advertise_addr: Option<StringSocketAddr>,

    /// Gossip seed nodes socket addresses.
    #[builder(default)]
    pub gossip_seeds: Vec<StringSocketAddr>,

    /// Socket to listen for lite node requests (QUIC UDP).
    #[builder(default = StringSocketAddr::from("127.0.0.1:12000".to_string()))]
    pub block_manager_listen_addr: StringSocketAddr,

    /// Static storages urls (e.g. <https://example.com/storage/>)
    #[builder(default)]
    pub static_storages: Vec<url::Url>,

    /// Socket address for SDK API
    pub api_addr: String,

    /// Network send buffer size
    /// Defaults to 1000
    #[builder(default = 1000)]
    pub send_buffer_size: usize,

    /// Public endpoint for this node
    #[builder(default)]
    pub public_endpoint: Option<String>,

    /// Number of max tries to download shared state
    /// Defaults to 3
    #[builder(default = 30)]
    pub shared_state_max_download_tries: u8,

    /// Retry timeout for shared state download
    /// Defaults to 2000
    #[builder(default = 200)]
    pub shared_state_retry_download_timeout_millis: u64,

    /// Chitchat cluster id for gossip
    pub chitchat_cluster_id: String,
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
