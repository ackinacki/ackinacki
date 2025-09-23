// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.

use std::net::SocketAddr;
use std::path::PathBuf;

use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;
use typed_builder::TypedBuilder;

// TODO: need to rework gossip arguments, now it has some advertised parameters
// that are not used (e.g. ["node_state"]["node_id"] section)
/// Network settings
#[derive(Serialize, Deserialize, Debug, Clone, TypedBuilder)]
pub struct NetworkConfig {
    /// Socket to listen other nodes messages (QUIC UDP).
    /// Defaults to "127.0.0.1:8500"
    #[builder(default = SocketAddr::from(([127,0,0,1], 8500)))]
    #[serde(default = "default_bind")]
    pub bind: SocketAddr,

    /// TLS auth cert.
    ///
    /// Node uses a TLS auth cert and key file to represent itself and prove it in two scenarios:
    /// - when a node acts as a server and accepts connections from another node or proxy;
    /// - when a node acts as a client and connects to another node or proxy.
    ///
    /// Should be a path to the `*.ca.pem` file.
    #[builder(default)]
    #[serde(default)]
    pub my_cert: PathBuf,

    /// TLS auth key.
    ///
    /// Node uses a TLS auth cert and key file to represent itself and prove it in two scenarios:
    /// - when a node acts as a server and accepts connections from another node or proxy;
    /// - when a node acts as a client and connects to another node or proxy.
    ///
    /// Should be a path to the `*.key.pem` file.
    #[builder(default)]
    #[serde(default)]
    pub my_key: PathBuf,

    /// Optional secret key of the block keeper's owner wallet key pair.
    /// Should be represented as a 64-char hex.
    /// If specified, then owner_key_path should be omitted.
    #[builder(default)]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub my_ed_key_secret: Vec<String>,

    /// Optional path to the block keeper's owner wallet key file.
    /// Should be stored as json `{ "public": "64-char hex", "secret": "64-char hex" }`.
    /// If specified, then owner_key_secret should be omitted.
    #[builder(default)]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub my_ed_key_path: Vec<String>,

    /// Subscribes.
    ///
    /// Node uses a subscribes to subscribe for blocks and other broadcast protocol messages.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "network::serialize_subscribe",
        deserialize_with = "network::deserialize_subscribe"
    )]
    #[builder(default)]
    pub subscribe: Vec<Vec<SocketAddr>>,

    /// Proxies.
    ///
    /// Node propagates this proxy list via gossip.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "network::deserialize_publisher_addrs"
    )]
    #[builder(default)]
    pub proxies: Vec<SocketAddr>,

    /// Files and directories with TLS certificates (*.ca.pem), required to verify
    /// server certificate when node establish client connection to other node or proxy.
    #[builder(default)]
    #[serde(default)]
    pub peer_certs: Vec<PathBuf>,

    /// Files and directories with TLS certificates (*.ca.pem), required to verify
    /// server certificate when node establish client connection to other node or proxy.
    #[builder(default)]
    #[serde(default, with = "transport_layer::hex_verifying_keys")]
    pub peer_ed_pubkeys: Vec<transport_layer::VerifyingKey>,

    /// Public node socket address that will be advertised with gossip (QUIC
    /// UDP).
    pub node_advertise_addr: SocketAddr,

    /// UDP socket address to listen gossip.
    /// Defaults to "127.0.0.1:10000"
    #[builder(default = SocketAddr::from(([127,0,0,1],10000)))]
    #[serde(default = "default_gossip_listen_addr")]
    pub gossip_listen_addr: SocketAddr,

    /// Gossip advertise socket address.
    /// Defaults to `bind` address
    #[builder(default)]
    pub gossip_advertise_addr: Option<SocketAddr>,

    /// Gossip seed nodes socket addresses.
    #[builder(default)]
    pub gossip_seeds: Vec<SocketAddr>,

    /// Socket to listen for lite node requests (QUIC UDP).
    #[builder(default = SocketAddr::from(([127,0,0,1],12000)))]
    #[serde(default = "default_block_manager_listen_addr")]
    pub block_manager_listen_addr: SocketAddr,

    /// Static storages urls (e.g. <https://example.com/storage/>)
    #[builder(default)]
    #[serde(default = "Default::default")]
    pub static_storages: Vec<url::Url>,

    /// Socket address for SDK API
    pub api_addr: String,

    /// Advertise url for SDK API
    pub api_advertise_addr: url::Url,

    /// Network send buffer size
    /// Defaults to 1000
    #[builder(default = 1000)]
    #[serde(default = "default_send_buffer_size")]
    pub send_buffer_size: usize,

    /// Public address for Block Manager API of this node
    #[builder(default)]
    pub bm_api_socket: Option<SocketAddr>,

    /// Public address for Block Keeper API this node
    #[builder(default)]
    pub bk_api_socket: Option<SocketAddr>,

    /// Number of max tries to download shared state
    /// Defaults to 3
    #[builder(default = 50)]
    #[serde(default = "default_shared_state_max_download_tries")]
    pub shared_state_max_download_tries: u8,

    /// Retry timeout for shared state download
    /// Defaults to 2000
    #[builder(default = 500)]
    #[serde(default = "default_shared_state_retry_download_timeout_millis")]
    pub shared_state_retry_download_timeout_millis: u64,

    /// Chitchat cluster id for gossip
    #[serde(default = "default_chitchat_cluster_id")]
    pub chitchat_cluster_id: String,

    /// Number of max nodes in gossip with the same id
    /// Defaults to 5
    #[builder(default = 5)]
    #[serde(default = "default_max_nodes_with_same_id")]
    pub max_nodes_with_same_id: u8,
}

fn default_bind() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8500))
}

fn default_gossip_listen_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 10000))
}

fn default_block_manager_listen_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 12000))
}

fn default_send_buffer_size() -> usize {
    1000
}

fn default_shared_state_retry_download_timeout_millis() -> u64 {
    200
}

fn default_shared_state_max_download_tries() -> u8 {
    30
}

fn default_chitchat_cluster_id() -> String {
    "acki_nacki".to_string()
}

fn default_max_nodes_with_same_id() -> u8 {
    5
}

impl NetworkConfig {
    pub fn get_gossip_seeds(&self) -> Vec<String> {
        self.gossip_seeds.iter().map(|s| s.to_string()).collect_vec()
    }
}
