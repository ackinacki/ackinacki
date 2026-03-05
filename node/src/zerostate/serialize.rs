// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;

use crate::zerostate::ZeroState;

impl ZeroState {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path.as_ref())?;
        // #[cfg(feature = "transitioning_node_version")]
        // let res = versioned_struct::Transitioning::deserialize_data_compat(&bytes).map_err(|e| {
        //     anyhow::format_err!("Failed to load zerostate from file {:?}: {e}", path.as_ref())
        // });
        // #[cfg(not(feature = "transitioning_node_version"))]
        let res = bincode::deserialize(&bytes).map_err(|e| {
            anyhow::format_err!("Failed to load zerostate from file {:?}: {e}", path.as_ref())
        });
        res
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let bytes = bincode::serialize(&self)?;
        std::fs::write(path.as_ref(), bytes).map_err(|e| {
            anyhow::format_err!("Failed to save zerostate to file {:?}: {e}", path.as_ref())
        })
    }
}
