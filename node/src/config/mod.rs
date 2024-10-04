// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

mod default;
mod serde_config;
#[cfg(test)]
mod test;

use std::path::PathBuf;
use std::time::Duration;

use default::default_bind;
use default::default_buffer_size;
use default::default_gossip_listen;
use default::default_lite_server_listen;
use network::socket_addr::StringSocketAddr;
use serde::Deserialize;
use serde::Serialize;
pub use serde_config::load_config_from_file;
pub use serde_config::save_config_to_file;

use crate::node::NodeIdentifier;

// TODO: These settings should be moved onchain.
/// Global node config, including block producer and synchronization settings.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GlobalConfig {
    /// Number of child blocks that has to be accepted as main candidate before
    /// finalizing the block. 0 to disable, defaults to 4
    pub require_minimum_blocks_to_finalize: usize,

    /// Time in milliseconds that is required to pass after block creation
    /// before finalization. 0 to disable, defaults to 0
    pub require_minimum_time_milliseconds_to_finalize: u64,

    /// Duration of one iteration of producing cycle in milliseconds.
    /// Defaults to 330
    pub time_to_produce_block_millis: u64,

    /// Number of non-finalized blocks, after which the block producer slows
    /// down. Defaults to 32
    pub finalization_delay_to_slow_down: u64,

    /// Block producer slow down multiplier.
    /// Defaults to 4
    pub slow_down_multiplier: u64,

    /// Number of non-finalized blocks, after which the block producer stops.
    /// Defaults to 128
    pub finalization_delay_to_stop: u64,

    /// Difference between the seq no of the incoming block and the seq no of
    /// the last saved block, which causes the node synchronization process
    /// to start. Defaults to 6
    pub need_synchronization_block_diff: u64,

    /// Minimal time between publishing state.
    /// Defaults to 600 seconds
    pub min_time_between_state_publish_directives: Duration,

    /// Number of nodes in producer group.
    /// Defaults to 5
    pub producer_group_size: usize,

    /// Block gap size that causes block producer rotation.
    /// Defaults to 6
    pub producer_change_gap_size: usize,

    /// Timeout between consecutive NodeJoining messages sending.
    /// Defaults to 60 seconds
    pub node_joining_timeout: Duration,

    /// Number of signatures, required for block acceptance.
    /// Defaults to 2
    pub min_signatures_cnt_for_acceptance: usize,

    /// Block gap before sharing the state on sync.
    /// Defaults to 32
    pub sync_gap: u64,

    /// Delay in milliseconds which node waits after receiving block it can't
    /// apply before switching to sync mode.
    /// Defaults to 500
    pub sync_delay_milliseconds: u128,

    /// Save optimistic state frequency (every N'th block)
    /// Defaults to 200
    pub save_state_frequency: u64,

    /// Block keeper epoch code hash
    pub block_keeper_epoch_code_hash: String,

    /// Send special transaction gas limit
    pub gas_limit_for_special_transaction: u64,
}

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
    #[serde(default = "default_lite_server_listen")]
    pub lite_server_listen_addr: StringSocketAddr,

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

/// Node interaction settings
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeConfig {
    /// Identifier of the current node.
    pub node_id: NodeIdentifier,

    /// Path to the file with blockchain config.
    pub blockchain_config_path: PathBuf,

    /// Path to the file with BLS key pair.
    pub key_path: String,

    /// Path to the file with block keeper seed key.
    pub block_keeper_seed_path: String,

    /// Path to zerostate file.
    pub zerostate_path: PathBuf,

    /// Local directory path which will be shared to other nodes.
    pub external_state_share_local_base_dir: PathBuf,

    /// Level of block production parallelization.
    pub parallelization_level: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    /// Global config
    #[serde(default)]
    pub global: GlobalConfig,

    /// Network config
    pub network: NetworkConfig,

    /// Local config
    pub local: NodeConfig,
}
