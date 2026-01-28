// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::time::Duration;

    use crate::config::Config;
    use crate::config::GlobalConfig;
    use crate::config::NetworkConfig;
    use crate::node::NodeIdentifier;

    #[test]
    fn test_network_config() -> anyhow::Result<()> {
        let config_str = r#"{
    "node_advertise_addr": "0.0.0.0:8500",
    "api_addr": "127.0.0.1:8600",
    "api_advertise_addr": "http://node0:8600",
    "gossip_seeds": []
}"#;
        let config: NetworkConfig = serde_json::from_str(config_str)?;
        assert_eq!(config.bind, SocketAddr::from(([127, 0, 0, 1], 8500)));
        assert_eq!(config.node_advertise_addr, SocketAddr::from(([0, 0, 0, 0], 8500)));
        assert_eq!(config.api_addr, "127.0.0.1:8600".to_string());
        assert!(config.gossip_seeds.is_empty());
        assert!(config.static_storages.is_empty());
        assert_eq!(config.gossip_listen_addr, SocketAddr::from(([127, 0, 0, 1], 10000)));
        assert_eq!(config.send_buffer_size, 1000);
        Ok(())
    }

    #[test]
    fn test_sync_gap_config() -> anyhow::Result<()> {
        let config = GlobalConfig { sync_gap: 30, ..Default::default() };
        assert_eq!(config.sync_gap, 30);
        let config = config.ensure_min_sync_gap();
        assert_eq!(config.sync_gap, 61);
        Ok(())
    }

    #[test]
    fn test_config_load() -> anyhow::Result<()> {
        let config_str = r#"{
    "network": {
        "node_advertise_addr": "0.0.0.0:8500",
        "api_addr": "127.0.0.1:8600",
        "api_advertise_addr": "https://node0:8600",
        "gossip_seeds": []
    },
    "local": {
        "node_id": "81a6bea128f5e03843362e55fd574c42a8e457dd553498cbc8ec7e14966d20a3",
        "blockchain_config_path": "../bc_config.json",
        "key_path": "key1.json",
        "zerostate_path": "./zerostate",
        "external_state_share_local_base_dir": "/tmp",
        "parallelization_level": 20,
        "split_state": false,
        "block_keeper_seed_path": "block_keeper.keys.json",
        "block_cache_size": 20,
        "state_cache_size": 10,
        "message_storage_path": "message_strage",
        "rate_limit_on_incoming_block_req": 1000,
        "ext_messages_cache_size": 10,
        "node_wallet_pubkey": "hex_string"
    }
}"#;
        let config: Config = serde_json::from_str(config_str)?;
        assert_eq!(config.network.bind, SocketAddr::from(([127, 0, 0, 1], 8500)));
        assert_eq!(config.network.node_advertise_addr, SocketAddr::from(([0, 0, 0, 0], 8500)));
        assert_eq!(config.network.api_addr, "127.0.0.1:8600".to_string());
        assert!(config.network.gossip_seeds.is_empty());
        assert!(config.network.static_storages.is_empty());
        assert_eq!(config.network.gossip_listen_addr, SocketAddr::from(([127, 0, 0, 1], 10000)));
        assert_eq!(config.network.send_buffer_size, 1000);

        assert_eq!(config.local.node_id, NodeIdentifier::some_id());
        assert_eq!(config.local.blockchain_config_path, PathBuf::from("../bc_config.json"));
        assert_eq!(config.local.key_path, "key1.json");
        assert_eq!(config.local.zerostate_path, Some(PathBuf::from("./zerostate")));
        assert_eq!(config.local.external_state_share_local_base_dir, PathBuf::from("/tmp"));

        let config = GlobalConfig::default();
        assert_eq!(config.time_to_produce_block_millis, 330);
        assert_eq!(config.need_synchronization_block_diff, 20);
        assert_eq!(config.min_time_between_state_publish_directives, Duration::from_secs(600));

        Ok(())
    }
}
