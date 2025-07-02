use std::fmt::Debug;
use std::hash::Hash;
use std::hash::Hasher;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::sync::Weak;

use derive_getters::Getters;
use parking_lot::Mutex;
use weak_table::WeakValueHashMap;

use super::state::AckiNackiBlockState;
use crate::types::BlockIdentifier;

pub type BlockStateInner = Mutex<AckiNackiBlockState>;

#[derive(Clone, Getters)]
pub struct BlockState {
    block_identifier: BlockIdentifier,
    inner: Arc<BlockStateInner>,
}

#[cfg(test)]
impl BlockState {
    pub fn test() -> Self {
        BlockState {
            block_identifier: BlockIdentifier::default(),
            inner: Arc::new(Mutex::new(AckiNackiBlockState::default())),
        }
    }
}

impl Debug for BlockState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlockState({:?})", self.block_identifier)
    }
}

impl std::ops::Deref for BlockState {
    type Target = Arc<BlockStateInner>;

    fn deref(&self) -> &Arc<BlockStateInner> {
        &self.inner
    }
}

#[derive(Clone)]
pub struct BlockStateRepository {
    block_state_repo_data_dir: PathBuf,
    map: Arc<Mutex<WeakValueHashMap<BlockIdentifier, Weak<BlockStateInner>>>>,
    notifications: Arc<AtomicU32>,
}

impl PartialEq for BlockState {
    fn eq(&self, other: &Self) -> bool {
        self.block_identifier == other.block_identifier
    }
}

impl Eq for BlockState {}

impl Hash for BlockState {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.block_identifier.hash(state)
    }
}

impl BlockStateRepository {
    pub fn new(block_state_repo_data_dir: PathBuf) -> Self {
        Self {
            block_state_repo_data_dir,
            map: Default::default(),
            notifications: Default::default(),
        }
    }

    pub fn block_state_repo_data_dir(&self) -> &PathBuf {
        &self.block_state_repo_data_dir
    }

    pub fn get(&self, block_identifier: &BlockIdentifier) -> anyhow::Result<BlockState> {
        let mut guarded = self.map.lock();
        let result = match guarded.get(block_identifier) {
            Some(e) => e,
            None => {
                let file_path =
                    self.block_state_repo_data_dir.join(format!("{block_identifier:x}"));
                let state = super::private::load_state(file_path.clone())?.unwrap_or_else(|| {
                    let mut state = AckiNackiBlockState::new(block_identifier.clone());
                    state.file_path = file_path;
                    state.notifications = Arc::clone(&self.notifications);
                    state
                });
                let state = Arc::new(Mutex::new(state));
                guarded.insert(block_identifier.clone(), state.clone());
                state
            }
        };
        guarded.remove_expired();
        drop(guarded);
        Ok(BlockState { block_identifier: block_identifier.clone(), inner: result })
    }

    pub fn notifications(&self) -> &AtomicU32 {
        self.notifications.as_ref()
    }

    pub fn touch(&self) {
        let notifications = self.notifications();
        notifications.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        atomic_wait::wake_all(notifications);
    }

    pub fn clone_notifications_arc(&self) -> Arc<AtomicU32> {
        Arc::clone(&self.notifications)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::node::NodeIdentifier;

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
        let some_node_id = NodeIdentifier::some_id();
        let some_block_id = BlockIdentifier::default();
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_owned();
        assert!(tmp_path.exists());
        let repository = BlockStateRepository::new(tmp_path);
        let value_a = repository.get(&some_block_id).unwrap();
        assert!(!value_a.lock().is_finalized());
        value_a.lock().try_add_attestations_interest(some_node_id.clone()).unwrap();
        value_a.lock().set_finalized().unwrap();
        assert!(value_a.lock().is_finalized());
        drop(value_a);
        // ensure durable
        let value = repository.get(&some_block_id).unwrap();
        let value = value.lock();
        assert!(value.known_attestation_interested_parties().contains(&some_node_id));
        assert!(value.is_finalized());
    }
}
