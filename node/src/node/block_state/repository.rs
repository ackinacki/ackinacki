use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Weak;

use parking_lot::Mutex;
use weak_table::WeakValueHashMap;

use super::state::AckiNackiBlockState;
use crate::types::BlockIdentifier;

type BlockStateInner = Mutex<AckiNackiBlockState>;
pub type BlockState = Arc<BlockStateInner>;

#[derive(Clone)]
pub struct BlockStateRepository {
    block_states_data_dir: PathBuf,
    map: Arc<Mutex<WeakValueHashMap<BlockIdentifier, Weak<BlockStateInner>>>>,
}

impl BlockStateRepository {
    pub fn new(block_states_data_dir: PathBuf) -> Self {
        Self { block_states_data_dir, map: Default::default() }
    }

    pub fn get(&self, block_identifier: &BlockIdentifier) -> anyhow::Result<BlockState> {
        let mut guarded = self.map.lock();
        let result = match guarded.get(block_identifier) {
            Some(e) => e,
            None => {
                let file_path = self.block_states_data_dir.join(format!("{:x}", block_identifier));
                let state = super::private::load_state(file_path.clone())?.unwrap_or_else(|| {
                    let mut state = AckiNackiBlockState::new(block_identifier.clone());
                    state.file_path = file_path;
                    state
                });
                let state = Arc::new(Mutex::new(state));
                guarded.insert(block_identifier.clone(), state.clone());
                state
            }
        };
        guarded.remove_expired();
        drop(guarded);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn ensure_repository_returns_same_ref_for_the_same_key() {
        let some_block_id = BlockIdentifier::default();
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_owned();
        assert!(tmp_path.exists());
        let repository = BlockStateRepository::new(tmp_path);
        // ensure initially empty
        assert!(repository.map.lock().is_empty());
        let value_a = repository.get(&some_block_id).unwrap();
        let value_b = repository.get(&some_block_id).unwrap();
        assert!(!value_a.lock().is_finalized());
        assert!(!value_b.lock().is_finalized());
        value_a.lock().set_finalized().unwrap();
        // prove they share state.
        assert!(value_b.lock().is_finalized());
    }

    #[test]
    fn ensure_repository_cleans_up_expired_entries_on_access() {
        let some_block_id = BlockIdentifier::default();
        let some_other_block_id = BlockIdentifier::from_str(
            "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFBC",
        )
        .unwrap();
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_owned();
        assert!(tmp_path.exists());
        let repository = BlockStateRepository::new(tmp_path);
        // ensure table is empty
        assert!(repository.map.lock().is_empty());
        let value_a = repository.get(&some_block_id).unwrap();
        let value_b = repository.get(&some_other_block_id).unwrap();
        // ensure table stores values
        assert!(repository.map.lock().len() == 2);
        drop(value_a);
        drop(value_b);
        let _ = repository.get(&some_block_id).unwrap();
        assert!(repository.map.lock().len() == 1);
    }

    #[test]
    fn ensure_repository_loads_saved_values() {
        let some_block_id = BlockIdentifier::default();
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_owned();
        assert!(tmp_path.exists());
        let repository = BlockStateRepository::new(tmp_path);
        let value_a = repository.get(&some_block_id).unwrap();
        assert!(!value_a.lock().is_finalized());
        value_a.lock().set_finalized().unwrap();
        assert!(value_a.lock().is_finalized());
        drop(value_a);
        // ensure durable
        let value = repository.get(&some_block_id).unwrap();
        assert!(value.lock().is_finalized());
    }
}
