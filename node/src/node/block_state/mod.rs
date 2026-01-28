pub mod attestation_target_checkpoints;
pub mod block_state_inner;
pub mod repository;
mod save_service;
pub mod state;
pub mod tools;
pub mod unfinalized_ancestor_blocks;
pub use save_service::start_state_save_service;

// TODO: migrate to any embedded db.
mod private {
    use std::path::PathBuf;

    use super::state::AckiNackiBlockState;
    use crate::repository::repository_impl::save_to_file;

    pub fn load_state(file_path: PathBuf) -> anyhow::Result<Option<AckiNackiBlockState>> {
        if !file_path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&file_path).map_err(|e| {
            anyhow::format_err!("Failed to read bytes from file {file_path:?}: {e}")
        })?;
        #[cfg(feature = "transitioning_node_version")]
        let mut state: AckiNackiBlockState = Transitioning::deserialize_data_compat(&bytes)
            .map_err(|e| {
                anyhow::format_err!("Failed to load block state from bytes {file_path:?}: {e}")
            })?;
        #[cfg(not(feature = "transitioning_node_version"))]
        let mut state: AckiNackiBlockState = bincode::deserialize(&bytes).map_err(|e| {
            anyhow::format_err!("Failed to load block state from bytes {file_path:?}: {e}")
        })?;
        state.file_path = file_path;
        Ok(Some(state))
    }

    pub fn save(state: &AckiNackiBlockState) -> anyhow::Result<()> {
        let file_path = state.file_path.clone();
        save_to_file(&file_path, &state, false)?;
        Ok(())
    }
}
