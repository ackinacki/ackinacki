use std::path::PathBuf;

use serde_json::Value;
use tvm_executor::BlockchainConfig;

pub fn load_blockchain_config(path: &PathBuf) -> anyhow::Result<BlockchainConfig> {
    let json = std::fs::read_to_string(path).unwrap_or_else(|_| {
        panic!("Failed to load blockchain config params from file: {}", path.display())
    });
    let map = serde_json::from_str::<serde_json::Map<String, Value>>(&json)?;
    let config_params = tvm_block_json::parse_config(&map).map_err(|e| {
        anyhow::format_err!("Failed to parse config params from file {:?}: {e}", path,)
    })?;
    BlockchainConfig::with_config(config_params)
        .map_err(|e| anyhow::format_err!("Failed to create blockchain config: {e}"))
}
