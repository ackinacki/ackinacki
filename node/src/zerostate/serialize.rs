// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;

use versioned_struct::Transitioning;

use crate::zerostate::ZeroState;

impl ZeroState {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path.as_ref())?;
        let (zs, _is_new) = ZeroState::deserialize_data_compat(&bytes).map_err(|e| {
            anyhow::format_err!("Failed to load zerostate from {:?}: {e}", path.as_ref())
        })?;
        Ok(zs)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let bytes = bincode::serialize(&self)?;
        std::fs::write(path.as_ref(), bytes).map_err(|e| {
            anyhow::format_err!("Failed to save zerostate to file {:?}: {e}", path.as_ref())
        })
    }
}
