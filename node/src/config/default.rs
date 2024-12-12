// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::time::Duration;

use network::socket_addr::StringSocketAddr;

use crate::config::Config;
use crate::config::GlobalConfig;

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            require_minimum_blocks_to_finalize: 0,
            require_minimum_time_milliseconds_to_finalize: 0,
            time_to_produce_block_millis: 330,
            finalization_delay_to_slow_down: 6,
            slow_down_multiplier: 4,
            finalization_delay_to_stop: 6,
            need_synchronization_block_diff: 6,
            min_time_between_state_publish_directives: Duration::from_secs(600),
            producer_group_size: 5,
            producer_change_gap_size: 6,
            node_joining_timeout: Duration::from_secs(300),
            min_signatures_cnt_for_acceptance: 3,
            sync_gap: 32,
            sync_delay_milliseconds: 500,
            save_state_frequency: 200,
            block_keeper_epoch_code_hash:
                "88305d70a51fe7f281a5cd5a24136706b2f1b4ae1fa1d2fc69ff3db12deb3090".to_string(),
            gas_limit_for_special_transaction: 10_000_000,
            attestation_validity_block_gap: 5,
        }
    }
}

pub fn default_bind() -> StringSocketAddr {
    StringSocketAddr::from("127.0.0.1:8500".to_string())
}

pub fn default_gossip_listen() -> StringSocketAddr {
    StringSocketAddr::from("127.0.0.1:10000".to_string())
}

pub fn default_block_manager_listen() -> StringSocketAddr {
    StringSocketAddr::from("127.0.0.1:12000".to_string())
}

pub fn default_buffer_size() -> usize {
    1000
}

impl Default for Config {
    fn default() -> Self {
        let default_config_str = r#"{
    "network": {
        "node_advertise_addr": "0.0.0.0:8500",
        "api_addr": "127.0.0.1:8600",
        "gossip_seeds": []
    },
    "local": {
        "node_id": 0,
        "blockchain_config_path": "blockchain_config.json",
        "key_path": "block_keeper.keys.json",
        "zerostate_path": "zerostate",
        "external_state_share_local_base_dir": "/tmp",
        "parallelization_level": 20,
        "block_keeper_seed_path": "block_keeper.keys.json"
    }
}"#;
        serde_json::from_str::<Config>(default_config_str)
            .expect("Failed to construct default node config")
    }
}
