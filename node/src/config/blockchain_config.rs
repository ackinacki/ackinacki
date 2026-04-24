use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tvm_block::GetRepresentationHash;
use tvm_executor::BlockchainConfig;

pub static BLOCKCHAIN_CONFIG: &str = include_str!("../../blockchain.conf.json");

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(transparent)]
pub struct BlockchainConfigHash(pub [u8; 32]);

fn generate_config_hash(config: &str) -> BlockchainConfigHash {
    let map = serde_json::from_str::<serde_json::Map<String, Value>>(config)
        .expect("Invalid blockchain config");
    let config_params =
        tvm_block_json::parse_config(&map).expect("Failed to parse blockchain config params");
    let hash = config_params.hash().expect("Failed to get config params hash");
    BlockchainConfigHash(hash.inner())
}

impl From<[u8; 32]> for BlockchainConfigHash {
    fn from(hash: [u8; 32]) -> Self {
        BlockchainConfigHash(hash)
    }
}

impl From<String> for BlockchainConfigHash {
    fn from(hash: String) -> Self {
        let data = hex::decode(&hash).unwrap();
        BlockchainConfigHash(data.try_into().unwrap())
    }
}

impl Debug for BlockchainConfigHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0.as_slice()))
    }
}

impl Display for BlockchainConfigHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0.as_slice()))
    }
}

lazy_static::lazy_static!(
    pub static ref DEFAULT_BLOCKCAHIN_CONFIG_HASH: BlockchainConfigHash = {
        generate_config_hash(BLOCKCHAIN_CONFIG)
    };
);

#[derive(Clone)]
pub struct BlockchainConfigRead {
    bc_configs: HashMap<BlockchainConfigHash, Arc<BlockchainConfig>>,
}

impl BlockchainConfigRead {
    pub fn get(&self, config_hash: &BlockchainConfigHash) -> anyhow::Result<Arc<BlockchainConfig>> {
        self.bc_configs
            .get(config_hash)
            .cloned()
            .ok_or(anyhow::anyhow!("Blockchain config not found"))
    }
}

pub fn load_blockchain_config() -> anyhow::Result<BlockchainConfigRead> {
    let map = serde_json::from_str::<serde_json::Map<String, Value>>(BLOCKCHAIN_CONFIG)?;
    let config_params =
        tvm_block_json::parse_config(&map).expect("Failed to parse blockchain config params");
    let new = BlockchainConfig::with_config(config_params)
        .map_err(|e| anyhow::format_err!("Failed to create blockchain config: {e}"))?;

    Ok(BlockchainConfigRead {
        bc_configs: HashMap::from_iter([(DEFAULT_BLOCKCAHIN_CONFIG_HASH.clone(), Arc::new(new))]),
    })
}
