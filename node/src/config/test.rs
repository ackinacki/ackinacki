// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use network::socket_addr::StringSocketAddr;

    use crate::config::Config;
    use crate::config::NetworkConfig;

    #[test]
    fn test_network_config() -> anyhow::Result<()> {
        let config_str = r#"{
    "node_advertise_addr": "0.0.0.0:8500",
    "api_addr": "127.0.0.1:8600",
    "gossip_seeds": []
}"#;
        let config: NetworkConfig = serde_json::from_str(config_str)?;
        assert_eq!(config.bind, StringSocketAddr::from("127.0.0.1:8500".to_string()));
        assert_eq!(config.node_advertise_addr, StringSocketAddr::from("0.0.0.0:8500".to_string()));
        assert_eq!(config.api_addr, "127.0.0.1:8600".to_string());
        assert!(config.gossip_seeds.is_empty());
        assert!(config.static_storages.is_empty());
        assert_eq!(
            config.gossip_listen_addr,
            StringSocketAddr::from("127.0.0.1:10000".to_string())
        );
        assert_eq!(config.send_buffer_size, 1000);
        Ok(())
    }

    #[test]
    fn test_config_load() -> anyhow::Result<()> {
        let config_str = r#"{
    "network": {
        "node_advertise_addr": "0.0.0.0:8500",
        "api_addr": "127.0.0.1:8600",
        "gossip_seeds": []
    },
    "local": {
        "node_id": 0,
        "blockchain_config_path": "../bc_config.json",
        "key_path": "key1.json",
        "zerostate_path": "./zerostate",
        "external_state_share_local_base_dir": "/tmp",
        "parallelization_level": 20,
        "block_keeper_seed_path": "block_keeper.keys.json"
    }
}"#;
        let config: Config = serde_json::from_str(config_str)?;
        assert_eq!(config.network.bind, StringSocketAddr::from("127.0.0.1:8500".to_string()));
        assert_eq!(
            config.network.node_advertise_addr,
            StringSocketAddr::from("0.0.0.0:8500".to_string())
        );
        assert_eq!(config.network.api_addr, "127.0.0.1:8600".to_string());
        assert!(config.network.gossip_seeds.is_empty());
        assert!(config.network.static_storages.is_empty());
        assert_eq!(
            config.network.gossip_listen_addr,
            StringSocketAddr::from("127.0.0.1:10000".to_string())
        );
        assert_eq!(config.network.send_buffer_size, 1000);

        assert_eq!(config.local.node_id, 0);
        assert_eq!(config.local.blockchain_config_path, PathBuf::from("../bc_config.json"));
        assert_eq!(config.local.key_path, "key1.json");
        assert_eq!(config.local.zerostate_path, PathBuf::from("./zerostate"));
        assert_eq!(config.local.external_state_share_local_base_dir, PathBuf::from("/tmp"));

        assert_eq!(config.global.require_minimum_blocks_to_finalize, 0);
        assert_eq!(config.global.require_minimum_time_milliseconds_to_finalize, 0);
        assert_eq!(config.global.time_to_produce_block_millis, 330);
        assert_eq!(config.global.finalization_delay_to_slow_down, 6);
        assert_eq!(config.global.slow_down_multiplier, 4);
        assert_eq!(config.global.finalization_delay_to_stop, 6);
        assert_eq!(config.global.need_synchronization_block_diff, 6);
        assert_eq!(
            config.global.min_time_between_state_publish_directives,
            Duration::from_secs(600)
        );

        Ok(())
    }
}
