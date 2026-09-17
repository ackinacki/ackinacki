pub mod attestation_target_checkpoints;
pub mod block_state_inner;
pub mod repository;
mod save_service;
pub mod state;
pub mod temporary_state;
pub mod tools;
pub mod unfinalized_ancestor_blocks;
pub use save_service::start_state_save_service;

// TODO: migrate to any embedded db.
mod private {
    use std::fs;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;

    use super::state::AckiNackiBlockState;
    use crate::helper::get_temp_file_path;

    pub fn load_state(file_path: PathBuf) -> anyhow::Result<Option<AckiNackiBlockState>> {
        if !file_path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&file_path).map_err(|e| {
            anyhow::format_err!("Failed to read bytes from file {file_path:?}: {e}")
        })?;
        let mut state: AckiNackiBlockState = bincode::deserialize(&bytes).map_err(|e| {
            anyhow::format_err!("Failed to load block state from bytes {file_path:?}: {e}")
        })?;

        state.file_path = file_path;
        Ok(Some(state))
    }

    pub fn save(state: &AckiNackiBlockState) -> anyhow::Result<()> {
        let file_path = state.file_path.clone();
        let buffer = bincode::serialize(&state)?;

        let parent_dir = if let Some(path) = file_path.parent() {
            fs::create_dir_all(path)?;
            path.to_owned()
        } else {
            PathBuf::new()
        };

        let tmp_file_path = get_temp_file_path(&parent_dir);

        let mut file = File::create(&tmp_file_path)?;

        file.write_all(&buffer)?;

        if cfg!(feature = "sync_files") {
            file.sync_all()?;
        }
        drop(file);

        std::fs::rename(tmp_file_path, &file_path)?;
        Ok(())
    }
}
