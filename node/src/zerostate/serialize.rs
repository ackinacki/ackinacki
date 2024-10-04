// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::block_keeper_system::BlockKeeperSet;
use crate::zerostate::ZeroState;

#[derive(Serialize, Deserialize)]
pub(crate) struct WrappedZeroState {
    pub(crate) shard_state_data: Vec<u8>,
    pub(crate) block_keeper_set: BlockKeeperSet,
}

impl Serialize for ZeroState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.wrap_serialize().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ZeroState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = WrappedZeroState::deserialize(deserializer)?;
        let block = ZeroState::wrap_deserialize(data);
        Ok(block)
    }
}

impl ZeroState {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path.as_ref())?;
        bincode::deserialize(&bytes).map_err(|e| {
            anyhow::format_err!("Failed to load zerostate from file {:?}: {e}", path.as_ref())
        })
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let bytes = bincode::serialize(&self)?;
        std::fs::write(path.as_ref(), bytes).map_err(|e| {
            anyhow::format_err!("Failed to save zerostate to file {:?}: {e}", path.as_ref())
        })
    }
}
