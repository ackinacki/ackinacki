use std::fmt::Debug;
use std::hash::Hash;
use std::hash::Hasher;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Weak;

use parking_lot::RwLock;
#[cfg(test)]
use telemetry_utils::mpsc::instrumented_channel;
use telemetry_utils::mpsc::InstrumentedSender;
use weak_table::WeakValueHashMap;

use super::block_state_inner::BlockStateInner;
use super::state::AckiNackiBlockState;
#[cfg(test)]
use crate::helper::metrics::BlockProductionMetrics;
use crate::node::block_state::block_state_inner::StateSaveCommand;
#[cfg(test)]
use crate::node::block_state::start_state_save_service;
use crate::types::notification::Notification;
use crate::types::BlockIdentifier;

#[derive(Clone)]
pub struct BlockState {
    // Note: it is a duplicate of BlockStateInner block_identifier value.
    // It is there due to the historic reason. It was easier to duplicate than
    // to fix it everywhere
    block_identifier: BlockIdentifier,
    inner: Arc<BlockStateInner>,
    save_sender: Arc<InstrumentedSender<StateSaveCommand>>,
}

impl std::ops::Drop for BlockState {
    fn drop(&mut self) {
        // add drop service for block state
        if Arc::strong_count(&self.inner) == 1 {
            // dropping the last ref.
            let _ = self.save_sender.send(StateSaveCommand::Save(self.inner.clone()));
        }
    }
}

impl BlockState {
    pub fn block_identifier(&self) -> &BlockIdentifier {
        &self.block_identifier
    }
}

#[cfg(test)]
impl BlockState {
    pub fn test() -> Self {
        let block_identifier = BlockIdentifier::default();
        let (tx, _) = instrumented_channel(None::<BlockProductionMetrics>, "test");
        BlockState {
            block_identifier: block_identifier.clone(),
            inner: Arc::new(BlockStateInner {
                block_identifier: block_identifier.clone(),
                shared_access: RwLock::new(AckiNackiBlockState::default()),
            }),
            save_sender: Arc::new(tx),
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
    map: Arc<RwLock<WeakValueHashMap<BlockIdentifier, Weak<BlockStateInner>>>>,
    //    cache: Arc<Mutex<LruCache<BlockIdentifier, BlockState>>>,
    notifications: Notification,
    save_service_sender: Arc<InstrumentedSender<StateSaveCommand>>,
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
    pub fn new(
        block_state_repo_data_dir: PathBuf,
        save_service_sender: Arc<InstrumentedSender<StateSaveCommand>>,
    ) -> Self {
        Self {
            block_state_repo_data_dir,
            map: Default::default(),
            notifications: Notification::new(),
            //            cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(100_000).unwrap()))),
            save_service_sender,
        }
    }

    #[cfg(test)]
    pub fn test(block_state_repo_data_dir: PathBuf) -> Self {
        let (state_save_tx, state_save_rx) =
            instrumented_channel(None::<BlockProductionMetrics>, "test");
        let _state_save_service = std::thread::Builder::new()
            .name("State save service".to_string())
            .spawn(move || start_state_save_service(state_save_rx))
            .unwrap();
        Self {
            block_state_repo_data_dir,
            map: Default::default(),
            notifications: Notification::new(),
            // cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1).unwrap()))),
            save_service_sender: Arc::new(state_save_tx),
        }
    }

    pub fn block_state_repo_data_dir(&self) -> &PathBuf {
        &self.block_state_repo_data_dir
    }

    pub fn get(&self, block_identifier: &BlockIdentifier) -> anyhow::Result<BlockState> {
        {
            let guarded = self.map.read();
            if let Some(e) = guarded.get(block_identifier) {
                let state = BlockState {
                    block_identifier: block_identifier.clone(),
                    inner: e,
                    save_sender: self.save_service_sender.clone(),
                };
                drop(guarded);
                return Ok(state);
            }
            drop(guarded);
        }
        // Keeping this out of the write-lock.
        // Tradeoff: It takes more time for new item,
        // however it reduces overall time, since most of the time we read.
        let file_path = self.block_state_repo_data_dir.join(format!("{block_identifier:x}"));
        let mut state = super::private::load_state(file_path.clone())?.unwrap_or_else(|| {
            let mut state = AckiNackiBlockState::new(block_identifier.clone());
            state.file_path = file_path;
            state.add_subscriber(self.notifications.clone());
            state
        });
        if state.parent_block_identifier().is_none()
            && block_identifier == &BlockIdentifier::default()
        {
            state.set_parent_block_identifier(BlockIdentifier::default())?;
        }
        let inner_state = BlockStateInner {
            block_identifier: block_identifier.clone(),
            shared_access: RwLock::new(state),
        };

        {
            let mut guarded = self.map.write();
            guarded.remove_expired();
            if let Some(e) = guarded.get(block_identifier) {
                let state = BlockState {
                    block_identifier: block_identifier.clone(),
                    inner: e,
                    save_sender: self.save_service_sender.clone(),
                };
                drop(guarded);
                return Ok(state);
            }
            let state = Arc::new(inner_state);
            guarded.insert(block_identifier.clone(), state.clone());
            drop(guarded);
            let state = BlockState {
                block_identifier: block_identifier.clone(),
                inner: state,
                save_sender: self.save_service_sender.clone(),
            };
            Ok(state)
        }
    }

    pub fn notifications(&self) -> &Notification {
        &self.notifications
    }

    pub fn touch(&mut self) {
        self.notifications.touch();
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::node::NodeIdentifier;
    use crate::utilities::guarded::Guarded;
    use crate::utilities::guarded::GuardedMut;

    #[test]
    fn ensure_repository_returns_same_ref_for_the_same_key() {
        let some_block_id = BlockIdentifier::default();
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_owned();
        assert!(tmp_path.exists());
        let repository = BlockStateRepository::test(tmp_path);
        // ensure initially empty
        assert!(repository.map.read().is_empty());
        let value_a = repository.get(&some_block_id).unwrap();
        let value_b = repository.get(&some_block_id).unwrap();
        assert!(!value_a.guarded(|e| e.is_finalized()));
        assert!(!value_b.guarded(|e| e.is_finalized()));
        value_a.guarded_mut(|e| e.set_finalized()).unwrap();
        // prove they share state.
        assert!(value_b.guarded(|e| e.is_finalized()));
    }

    #[test]
    #[ignore]
    // TODO: return this test, now it fails due to additional cache usage in BlockStateRepo
    fn ensure_repository_cleans_up_expired_entries_on_access() {
        let some_block_id = BlockIdentifier::default();
        let some_other_block_id = BlockIdentifier::from_str(
            "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFBC",
        )
        .unwrap();
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_owned();
        assert!(tmp_path.exists());
        let repository = BlockStateRepository::test(tmp_path);
        // ensure table is empty
        assert!(repository.map.read().is_empty());
        let value_a = repository.get(&some_block_id).unwrap();
        let value_b = repository.get(&some_other_block_id).unwrap();
        // ensure table stores values
        assert!(repository.map.read().len() == 2);
        drop(value_a);
        drop(value_b);
        let _ = repository.get(&some_block_id).unwrap();
        assert!(repository.map.read().len() == 1);
    }

    #[test]
    fn ensure_repository_loads_saved_values() {
        let some_node_id = NodeIdentifier::some_id();
        let some_block_id = BlockIdentifier::default();
        let tmp_dir = tempfile::tempdir().unwrap();
        let tmp_path = tmp_dir.path().to_owned();
        assert!(tmp_path.exists());
        let repository = BlockStateRepository::test(tmp_path);
        let value_a = repository.get(&some_block_id).unwrap();
        assert!(!value_a.guarded(|e| e.is_finalized()));
        value_a.guarded_mut(|e| e.try_add_attestations_interest(some_node_id.clone())).unwrap();
        value_a.guarded_mut(|e| e.set_finalized()).unwrap();
        assert!(value_a.guarded_mut(|e| e.is_finalized()));
        drop(value_a);
        // ensure durable
        let value = repository.get(&some_block_id).unwrap();
        value.guarded(|e| {
            assert!(e.known_attestation_interested_parties().contains(&some_node_id));
            assert!(e.is_finalized());
        })
    }
}
