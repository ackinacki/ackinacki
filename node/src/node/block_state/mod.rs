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
    use std::time::Instant;

    use super::state::AckiNackiBlockState;
    use crate::helper::get_temp_file_path;

    const BLOCK_STATE_SAVE_TARGET: &str = "block_state_save";

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
        let block_identifier = *state.block_identifier();
        let block_seq_no = *state.block_seq_no();
        let total_started_at = Instant::now();

        let serialize_started_at = Instant::now();
        let buffer = bincode::serialize(&state)?;
        let serialize_ms = serialize_started_at.elapsed().as_millis();

        let create_dir_started_at = Instant::now();
        let parent_dir = if let Some(path) = file_path.parent() {
            fs::create_dir_all(path)?;
            path.to_owned()
        } else {
            PathBuf::new()
        };
        let create_dir_ms = create_dir_started_at.elapsed().as_millis();

        let tmp_file_path = get_temp_file_path(&parent_dir);

        let create_file_started_at = Instant::now();
        let mut file = File::create(&tmp_file_path)?;
        let create_file_ms = create_file_started_at.elapsed().as_millis();

        let write_started_at = Instant::now();
        file.write_all(&buffer)?;
        let write_ms = write_started_at.elapsed().as_millis();

        let mut sync_ms = 0;
        let sync_enabled = cfg!(feature = "sync_files");
        if sync_enabled {
            let sync_started_at = Instant::now();
            file.sync_all()?;
            sync_ms = sync_started_at.elapsed().as_millis();
        }
        drop(file);

        let rename_started_at = Instant::now();
        std::fs::rename(tmp_file_path, &file_path)?;
        let rename_ms = rename_started_at.elapsed().as_millis();

        tracing::trace!(
            target: BLOCK_STATE_SAVE_TARGET,
            "Block state object save timing: seq_no={block_seq_no:?} block_id={block_identifier:?} bytes={} serialize_ms={serialize_ms} create_dir_ms={create_dir_ms} create_file_ms={create_file_ms} write_ms={write_ms} sync_enabled={sync_enabled} sync_ms={sync_ms} rename_ms={rename_ms} total_ms={} file_path={file_path:?}",
            buffer.len(),
            total_started_at.elapsed().as_millis(),
        );
        Ok(())
    }
}
