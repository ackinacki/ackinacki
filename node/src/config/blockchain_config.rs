use serde_json::Value;
use tvm_executor::BlockchainConfig;

pub static BLOCKCHAIN_CONFIG: &str = include_str!("../../blockchain.conf.json");

pub fn load_blockchain_config() -> anyhow::Result<BlockchainConfig> {
    let map = serde_json::from_str::<serde_json::Map<String, Value>>(BLOCKCHAIN_CONFIG)?;
    let config_params =
        tvm_block_json::parse_config(&map).expect("Failed to parse blockchain config params");
    BlockchainConfig::with_config(config_params)
        .map_err(|e| anyhow::format_err!("Failed to create blockchain config: {e}"))
}
