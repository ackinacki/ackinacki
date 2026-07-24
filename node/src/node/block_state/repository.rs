use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::Hash;
use std::hash::Hasher;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Weak;

use node_types::BlockIdentifier;
use node_types::ParentRef;
use node_types::TemporaryBlockId;
use node_types::ThreadIdentifier;
use parking_lot::RwLock;
#[cfg(test)]
use telemetry_utils::mpsc::instrumented_channel;
use telemetry_utils::mpsc::InstrumentedSender;
use weak_table::WeakValueHashMap;

use super::block_state_inner::BlockStateInner;
use super::state::AckiNackiBlockState;
use super::temporary_state::TemporaryBlockState;
use crate::helper::metrics::BlockProductionMetrics;
use crate::node::block_state::block_state_inner::StateSaveCommand;
#[cfg(test)]
use crate::node::block_state::start_state_save_service;
use crate::node::services::block_processor::service::MAX_ATTESTATION_TARGET_BETA;
use crate::types::notification::Notification;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::Guarded;

const LOCAL_PRODUCED_BLOCK_STATE_RETENTION_LIMIT: usize = MAX_ATTESTATION_TARGET_BETA * 4;

type LocalProducedRetentionEntry = (ThreadIdentifier, BlockSeqNo, BlockIdentifier, BlockState);

struct RingBuffer<T> {
    items: Vec<Option<T>>,
    head: usize,
    len: usize,
}

impl<T> RingBuffer<T> {
    fn with_capacity(capacity: usize) -> Self {
        let mut items = Vec::with_capacity(capacity);
        items.resize_with(capacity, || None);
        Self { items, head: 0, len: 0 }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.items.len()
    }

    fn push_back(&mut self, item: T) -> Option<T> {
        match self.capacity() {
            0 => Some(item),
            capacity if self.len == capacity => {
                let evicted = self.items[self.head].replace(item);
                self.head = (self.head + 1) % capacity;
                evicted
            }
            capacity => {
                let index = (self.head + self.len) % capacity;
                self.items[index] = Some(item);
                self.len += 1;
                None
            }
        }
    }

    fn iter(&self) -> impl Iterator<Item = &T> {
        let capacity = self.capacity();
        (0..self.len).filter_map(move |offset| {
            let index = (self.head + offset) % capacity;
            self.items[index].as_ref()
        })
    }

    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&T) -> bool,
    {
        let capacity = self.capacity();
        let mut retained = Vec::with_capacity(self.len);
        for offset in 0..self.len {
            let index = (self.head + offset) % capacity;
            if let Some(item) = self.items[index].take() {
                if f(&item) {
                    retained.push(item);
                }
            }
        }

        self.head = 0;
        self.len = retained.len();
        for (index, item) in retained.into_iter().enumerate() {
            self.items[index] = Some(item);
        }
    }
}

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
        let mut state = AckiNackiBlockState::default();
        state.attach_metrics(None);
        state.report_created();
        BlockState {
            block_identifier,
            inner: Arc::new(BlockStateInner {
                block_identifier,
                shared_access: RwLock::new(state),
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
    metrics: Option<BlockProductionMetrics>,

    // In-memory storage for temporary block states.
    // Not persisted to disk — temporary states live only while the producer is active.
    temporary_map: Arc<RwLock<VecDeque<(TemporaryBlockId, Arc<RwLock<TemporaryBlockState>>)>>>,

    // Strong refs for locally produced blocks. The main map intentionally stores
    // weak refs, so locally produced states need an explicit holder until they
    // are finalized and can be saved/dropped normally.
    local_produced_retention: Arc<RwLock<RingBuffer<LocalProducedRetentionEntry>>>,
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
    pub fn live_count(&self) -> usize {
        self.map.read().len()
    }

    pub fn new(
        block_state_repo_data_dir: PathBuf,
        save_service_sender: Arc<InstrumentedSender<StateSaveCommand>>,
        metrics: Option<BlockProductionMetrics>,
    ) -> Self {
        let repository = Self {
            block_state_repo_data_dir,
            map: Default::default(),
            notifications: Notification::new(),
            //            cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(100_000).unwrap()))),
            save_service_sender,
            metrics,
            temporary_map: Default::default(),
            local_produced_retention: Arc::new(RwLock::new(RingBuffer::with_capacity(
                LOCAL_PRODUCED_BLOCK_STATE_RETENTION_LIMIT,
            ))),
        };
        repository.report_temporary_map_size(0);
        repository
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
            metrics: None,
            temporary_map: Default::default(),
            local_produced_retention: Arc::new(RwLock::new(RingBuffer::with_capacity(
                LOCAL_PRODUCED_BLOCK_STATE_RETENTION_LIMIT,
            ))),
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
                    block_identifier: *block_identifier,
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
            let mut state = AckiNackiBlockState::new(*block_identifier);
            state.file_path = file_path;
            state
        });
        state.attach_metrics(self.metrics.clone());
        state.report_created();
        state.add_subscriber(self.notifications.clone());
        if state.parent_block_identifier().is_none()
            && block_identifier == &BlockIdentifier::default()
        {
            state.set_parent_block_identifier(BlockIdentifier::default())?;
        }
        let inner_state = BlockStateInner {
            block_identifier: *block_identifier,
            shared_access: RwLock::new(state),
        };

        {
            let mut guarded = self.map.write();
            guarded.remove_expired();
            if let Some(e) = guarded.get(block_identifier) {
                let state = BlockState {
                    block_identifier: *block_identifier,
                    inner: e,
                    save_sender: self.save_service_sender.clone(),
                };
                drop(guarded);
                return Ok(state);
            }
            let state = Arc::new(inner_state);
            guarded.insert(*block_identifier, state.clone());
            drop(guarded);
            let state = BlockState {
                block_identifier: *block_identifier,
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

    fn report_temporary_map_size(&self, size: usize) {
        if let Some(metrics) = &self.metrics {
            metrics.report_temporary_block_states_size(size as u64);
        }
    }

    pub fn retain_local_produced_until_finalized(&self, block_state: BlockState) {
        let Some(block_seq_no) = block_state.guarded(|e| *e.block_seq_no()) else {
            tracing::trace!(
                "Skip local produced block state retention: missing seq_no block_id={:?}",
                block_state.block_identifier(),
            );
            return;
        };
        let Some(thread_identifier) = block_state.guarded(|e| *e.thread_identifier()) else {
            tracing::trace!(
                "Skip local produced block state retention: missing thread_id block_id={:?}",
                block_state.block_identifier(),
            );
            return;
        };

        let block_identifier = *block_state.block_identifier();
        let mut retained = self.local_produced_retention.write();
        if retained.iter().any(|(_, _, id, _)| id == &block_identifier) {
            return;
        }

        if let Some((thread_id, seq_no, id, _)) =
            retained.push_back((thread_identifier, block_seq_no, block_identifier, block_state))
        {
            tracing::trace!(
                "Drop local produced block state from retention by capacity: thread_id={thread_id:?} seq_no={seq_no:?} block_id={id:?}"
            );
        }
        tracing::trace!(
            "Retain local produced block state until finalized: thread_id={thread_identifier:?} seq_no={block_seq_no:?} block_id={block_identifier:?} retained={}",
            retained.len(),
        );
    }

    pub fn remove_retained_local_produced_up_to(
        &self,
        thread_identifier: &ThreadIdentifier,
        last_finalized_seq_no: &BlockSeqNo,
    ) {
        let mut retained = self.local_produced_retention.write();
        let old_len = retained.len();
        retained.retain(|(thread_id, seq_no, _, _)| {
            thread_id != thread_identifier || seq_no > last_finalized_seq_no
        });
        let removed = old_len - retained.len();
        if removed > 0 {
            tracing::trace!(
                "Removed finalized local produced block states from retention: thread_id={thread_identifier:?} removed={removed} retained={} last_finalized_seq_no={last_finalized_seq_no:?}",
                retained.len(),
            );
        }
    }

    /// Create a temporary block state and register it in the in-memory map.
    /// Called by the block-producer BEFORE block production starts.
    pub fn create_temporary(
        &self,
        temp_id: TemporaryBlockId,
        parent_ref: ParentRef,
        thread_id: ThreadIdentifier,
    ) -> Arc<RwLock<TemporaryBlockState>> {
        let mut state = TemporaryBlockState::new(temp_id, parent_ref);
        state.set_thread_identifier(thread_id);
        let arc = Arc::new(RwLock::new(state));
        let mut temporary_map = self.temporary_map.write();
        temporary_map.push_back((temp_id, arc.clone()));
        while temporary_map.len() > MAX_ATTESTATION_TARGET_BETA * 2 {
            temporary_map.pop_front();
        }
        self.report_temporary_map_size(temporary_map.len());
        arc
    }

    /// Get a temporary block state by its ID.
    pub fn get_temporary(
        &self,
        temp_id: &TemporaryBlockId,
    ) -> Option<Arc<RwLock<TemporaryBlockState>>> {
        self.temporary_map
            .read()
            .iter()
            .find(|(id, _state)| id == temp_id)
            .map(|(_, state)| state)
            .cloned()
    }

    /// Remove temporary states that have been promoted to real BlockStates.
    /// Call this after the production loop has stopped — at that point no thread
    /// holds a `ParentRef::Temporary` referencing these entries.
    pub fn cleanup_temporaries(&self) {
        let mut map = self.temporary_map.write();
        map.clear();
        self.report_temporary_map_size(map.len());
    }

    #[cfg(test)]
    pub fn remove(&self, block_identifier: &BlockIdentifier) -> anyhow::Result<()> {
        let mut guarded = self.map.write();
        let _ = guarded.remove(block_identifier);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::thread::sleep;
    use std::time::Duration;

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
        sleep(Duration::from_secs(1));
        repository.remove(&some_block_id).unwrap();
        // ensure durable
        let value = repository.get(&some_block_id).unwrap();
        value.guarded(|e| {
            assert!(e.known_attestation_interested_parties().contains(&some_node_id));
            assert!(e.is_finalized());
        })
    }

    #[test]
    fn ensure_repository_saves_reloaded_values() {
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
        sleep(Duration::from_secs(1));
        repository.remove(&some_block_id).unwrap();
        let value_b = repository.get(&some_block_id).unwrap();
        value_b.guarded_mut(|e| e.set_must_be_validated_in_fallback_case()).unwrap();
        assert!(value_b.guarded(|e| e.must_be_validated_in_fallback_case() == &Some(true)));
        drop(value_b);
        sleep(Duration::from_secs(1));
        repository.remove(&some_block_id).unwrap();
        let value = repository.get(&some_block_id).unwrap();
        value.guarded(|e| {
            assert!(e.known_attestation_interested_parties().contains(&some_node_id));
            assert!(e.is_finalized());
            assert!(e.must_be_validated_in_fallback_case() == &Some(true));
        })
    }
}
