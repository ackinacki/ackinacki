pub mod dependent_ancestor_blocks;
pub mod repository;
pub mod state;
pub mod unfinalized_ancestor_blocks;

// TODO: migrate to any embedded db.
mod private {
    use std::path::PathBuf;

    use super::state::AckiNackiBlockState;
    use crate::repository::repository_impl::load_from_file;
    use crate::repository::repository_impl::save_to_file;

    pub fn load_state(file_path: PathBuf) -> anyhow::Result<Option<AckiNackiBlockState>> {
        if let Some(mut state) = load_from_file::<AckiNackiBlockState>(&file_path).map_err(|e| {
            anyhow::format_err!("Failed to load block state from file {file_path:?}: {e}")
        })? {
            state.file_path = file_path;
            Ok(Some(state))
        } else {
            Ok(None)
        }
    }

    pub fn save(state: &AckiNackiBlockState) -> anyhow::Result<()> {
        let file_path = state.file_path.clone();
        save_to_file(&file_path, state, false)
    }
}
