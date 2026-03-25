use std::sync::Arc;

use serde_json::Value;
use tvm_executor::BlockchainConfig;

use crate::types::BlockSeqNo;

#[cfg(feature = "fix_flag_16")]
pub static BLOCKCHAIN_CONFIG_OLD: &str = include_str!("../../old_blockchain.conf.json");
pub static BLOCKCHAIN_CONFIG: &str = include_str!("../../blockchain.conf.json");

#[derive(Clone)]
pub struct BlockchainConfigRead {
    #[cfg(feature = "fix_flag_16")]
    bc_config_old: Arc<BlockchainConfig>,
    bc_config_new: Arc<BlockchainConfig>,
}

impl BlockchainConfigRead {
    pub fn get(
        &self,
        _block_seq_no: &BlockSeqNo,
        #[cfg(feature = "fix_flag_16")] is_block_of_retired_version: bool,
    ) -> Arc<BlockchainConfig> {
        #[cfg(feature = "fix_flag_16")]
        if is_block_of_retired_version {
            return self.bc_config_old.clone();
        }
        self.bc_config_new.clone()
    }
}

pub fn load_blockchain_config() -> anyhow::Result<BlockchainConfigRead> {
    let map = serde_json::from_str::<serde_json::Map<String, Value>>(BLOCKCHAIN_CONFIG)?;
    let config_params =
        tvm_block_json::parse_config(&map).expect("Failed to parse blockchain config params");
    let new = BlockchainConfig::with_config(config_params)
        .map_err(|e| anyhow::format_err!("Failed to create blockchain config: {e}"))?;
    #[cfg(feature = "fix_flag_16")]
    let old = {
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(BLOCKCHAIN_CONFIG_OLD)?;
        let config_params = tvm_block_json::parse_config(&map)
            .expect("Failed to parse old blockchain config params");
        BlockchainConfig::with_config(config_params)
            .map_err(|e| anyhow::format_err!("Failed to create old blockchain config: {e}"))?
    };
    Ok(BlockchainConfigRead {
        bc_config_new: Arc::new(new),
        #[cfg(feature = "fix_flag_16")]
        bc_config_old: Arc::new(old),
    })
}
