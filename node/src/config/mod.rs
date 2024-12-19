// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
mod default;
mod network_config;
mod serde_config;
#[cfg(test)]
mod test;
mod validations;

use std::path::PathBuf;
use std::time::Duration;

pub use network_config::NetworkConfig;
use serde::Deserialize;
use serde::Serialize;
pub use serde_config::load_config_from_file;
pub use serde_config::save_config_to_file;

use crate::node::NodeIdentifier;
use crate::types::BlockSeqNo;

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
    pub finalization_delay_to_slow_down: <BlockSeqNo as std::ops::Sub>::Output,

    /// Block producer slow down multiplier.
    /// Defaults to 4
    pub slow_down_multiplier: u64,

    /// Number of non-finalized blocks, after which the block producer stops.
    /// Defaults to 128
    pub finalization_delay_to_stop: <BlockSeqNo as std::ops::Sub>::Output,

    /// Difference between the seq no of the incoming block and the seq no of
    /// the last saved block, which causes the node synchronization process
    /// to start. Defaults to 6
    pub need_synchronization_block_diff: <BlockSeqNo as std::ops::Sub>::Output,

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
    pub save_state_frequency: u32,

    /// Block keeper epoch code hash
    pub block_keeper_epoch_code_hash: String,

    /// Send special transaction gas limit
    pub gas_limit_for_special_transaction: u64,

    /// Expected maximum number of threads.
    /// Note: it can grow over this value for some time on the running network.
    pub max_threads_count: usize,

    /// Number of block gap after which block attestation become invalid
    pub attestation_validity_block_gap: <BlockSeqNo as std::ops::Sub>::Output,
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
