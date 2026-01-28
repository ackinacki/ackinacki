// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;

use crate::zerostate::ZeroState;

impl ZeroState {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let bytes = std::fs::read(path.as_ref())?;
        #[cfg(feature = "transitioning_node_version")]
        let res = versioned_struct::Transitioning::deserialize_data_compat(&bytes).map_err(|e| {
            anyhow::format_err!("Failed to load zerostate from file {:?}: {e}", path.as_ref())
        });
        #[cfg(not(feature = "transitioning_node_version"))]
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

#[test]
#[cfg(feature = "transitioning_node_version")]
fn load_zerostate_from_main() -> anyhow::Result<()> {
    use std::path::PathBuf;

    use versioned_struct::Transitioning;

    use crate::repository::repository_impl::ThreadSnapshot;
    // println!("load_zerostate_from_main: {:?}", std::env::current_dir());
    // println!("load_zerostate_from_main: {:?}", std::env::current_exe());
    // Note: current dir is affected by tempdir which replaces node dir with temp.
    // Use current_exe path to get right absolute path to the test files
    let mut path: PathBuf = std::env::current_exe()?;
    path.pop();
    path.pop();
    path.pop();
    path.pop();
    path.push("node");
    path.push("tests");
    path.push("test_decode");
    let root = path.as_path();
    let _ = ZeroState::load_from_file(root.join("zerostate_main"))
        .map_err(|e| anyhow::format_err!("Failed to decode zs: {e}"))?;
    let bytes = std::fs::read(
        root.join("f9c0bf7cafdc2586b846169a951e9f5ed4fc53febe0e2f284634a755ef9bbce8"),
    )
    .map_err(|e| anyhow::format_err!("Failed to load snapshot: {e}"))?;
    let _: ThreadSnapshot = Transitioning::deserialize_data_compat(&bytes)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize ThreadSnapshot: {e}"))?;
    Ok(())
}
