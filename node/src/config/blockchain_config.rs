use std::sync::Arc;

use serde_json::Value;
use tvm_executor::BlockchainConfig;

use crate::types::BlockSeqNo;

pub static BLOCKCHAIN_CONFIG_OLD: &str = include_str!("../../old_blockchain.conf.json");
pub static BLOCKCHAIN_CONFIG: &str = include_str!("../../blockchain.conf.json");
pub static UPDATE_MIRRORS_BLOCK_SEQ_NO: u32 = 21_220_000;

#[derive(Clone)]
pub struct BlockchainConfigRead {
    bc_config_old: Arc<BlockchainConfig>,
    bc_config_new: Arc<BlockchainConfig>,
}

impl BlockchainConfigRead {
    pub fn get(&self, block_seq_no: &BlockSeqNo) -> Arc<BlockchainConfig> {
        if *block_seq_no <= UPDATE_MIRRORS_BLOCK_SEQ_NO.into() {
            self.bc_config_old.clone()
        } else {
            self.bc_config_new.clone()
        }
    }
}

pub fn load_blockchain_config() -> anyhow::Result<BlockchainConfigRead> {
    let map = serde_json::from_str::<serde_json::Map<String, Value>>(BLOCKCHAIN_CONFIG)?;
    let config_params =
        tvm_block_json::parse_config(&map).expect("Failed to parse blockchain config params");
    let new = BlockchainConfig::with_config(config_params)
        .map_err(|e| anyhow::format_err!("Failed to create blockchain config: {e}"))?;
    let map = serde_json::from_str::<serde_json::Map<String, Value>>(BLOCKCHAIN_CONFIG_OLD)?;
    let config_params =
        tvm_block_json::parse_config(&map).expect("Failed to parse blockchain config params");
    let old = BlockchainConfig::with_config(config_params)
        .map_err(|e| anyhow::format_err!("Failed to create blockchain config: {e}"))?;
    Ok(BlockchainConfigRead { bc_config_new: Arc::new(new), bc_config_old: Arc::new(old) })
}
