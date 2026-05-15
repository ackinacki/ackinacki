use std::collections::HashSet;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use serde::Deserialize;
use serde::Serialize;

use crate::thread_accounts::durable::kv_store::KVRecord;
use crate::thread_accounts::durable::kv_store::KVStore;

/// Update phase for a single blockchain thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UpdatePhase {
    /// Thread registered but not yet populated.
    Uninitialized,
    /// No update in progress. Both copies consistent.
    Idle,
    /// Writing Copy A. A has partial/in-progress data, B is consistent.
    WritingCopyA,
    /// Writing Copy B. A is consistent, B has partial/in-progress data.
    WritingCopyB,
    /// Thread collapsed.
    Collapsed,
}

impl UpdatePhase {
    /// Whether Copy A is consistent and safe to read.
    pub fn copy_a_ready(&self) -> bool {
        matches!(self, UpdatePhase::Idle | UpdatePhase::WritingCopyB)
    }

    /// Whether Copy B is consistent and safe to read.
    pub fn copy_b_ready(&self) -> bool {
        matches!(self, UpdatePhase::Idle | UpdatePhase::WritingCopyA)
    }
}

/// In-memory representation of a Thread Control Record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadControlState {
    pub thread_id: ThreadIdentifier,
    pub update_phase: UpdatePhase,
    pub last_block_id: Option<BlockIdentifier>,
}

impl ThreadControlState {
    pub fn new_uninitialized(thread_id: ThreadIdentifier) -> Self {
        Self { thread_id, update_phase: UpdatePhase::Uninitialized, last_block_id: None }
    }

    pub fn new_idle(thread_id: ThreadIdentifier, block_id: BlockIdentifier) -> Self {
        Self { thread_id, update_phase: UpdatePhase::Idle, last_block_id: Some(block_id) }
    }

    pub fn new_collapsed(thread_id: ThreadIdentifier) -> Self {
        Self { thread_id, update_phase: UpdatePhase::Collapsed, last_block_id: None }
    }
}

/// Serializable form of the Thread Control Record (stored in KVStore).
#[derive(Serialize, Deserialize)]
struct ThreadControlRecord {
    update_phase: u8,
    last_block_id: Option<BlockIdentifier>,
}

impl ThreadControlRecord {
    fn to_state(
        &self,
        thread_id: ThreadIdentifier,
    ) -> Result<ThreadControlState, super::error::ArchiveStateError> {
        let update_phase = match self.update_phase {
            0 => UpdatePhase::Uninitialized,
            1 => UpdatePhase::Idle,
            2 => UpdatePhase::WritingCopyA,
            3 => UpdatePhase::WritingCopyB,
            4 => UpdatePhase::Collapsed,
            _ => {
                return Err(super::error::ArchiveStateError::CorruptedControlRecord(
                    thread_id,
                    "invalid update_phase".to_string(),
                ))
            }
        };
        Ok(ThreadControlState { thread_id, update_phase, last_block_id: self.last_block_id })
    }

    fn from_state(state: &ThreadControlState) -> Self {
        Self {
            update_phase: match state.update_phase {
                UpdatePhase::Uninitialized => 0,
                UpdatePhase::Idle => 1,
                UpdatePhase::WritingCopyA => 2,
                UpdatePhase::WritingCopyB => 3,
                UpdatePhase::Collapsed => 4,
            },
            last_block_id: state.last_block_id,
        }
    }
}

/// In-memory representation of the System Record.
#[derive(Debug, Clone)]
pub struct SystemState {
    pub threads: HashSet<ThreadIdentifier>,
    pub data_epoch: u64,
}

/// Serializable form of the System Record (stored in KVStore).
#[derive(Serialize, Deserialize)]
struct SystemRecord {
    threads: HashSet<ThreadIdentifier>,
    data_epoch: u64,
}

// ---- Constants ----

/// KVStore set name for metadata (system record + thread control records).
#[cfg(test)]
pub const META_SET: &str = "meta";

/// Key for the System Record within META_SET.
const SYSTEM_KEY: &[u8] = b"__system";

/// Suffix appended to thread_id bytes to form the control record key.
const CTRL_SUFFIX: &[u8] = b":ctrl";

// ---- Key helpers ----

/// Build the KVStore key for a thread's control record.
fn thread_ctrl_key(thread_id: &ThreadIdentifier) -> Vec<u8> {
    let mut key = Vec::with_capacity(34 + CTRL_SUFFIX.len());
    key.extend_from_slice(thread_id.as_ref());
    key.extend_from_slice(CTRL_SUFFIX);
    key
}

// ---- KVStore I/O — System Record ----

/// Read the System Record. Returns None if it doesn't exist.
pub fn read_system_record(store: &KVStore, meta_set: &str) -> anyhow::Result<Option<SystemState>> {
    let results = store.get(meta_set, &[SYSTEM_KEY.to_vec()])?;
    match results.into_iter().next().flatten() {
        Some(record) => {
            let sys: SystemRecord = bincode::deserialize(&record.data)?;
            Ok(Some(SystemState { threads: sys.threads, data_epoch: sys.data_epoch }))
        }
        None => Ok(None),
    }
}

/// Write the System Record.
pub fn write_system_record(
    store: &KVStore,
    meta_set: &str,
    state: &SystemState,
) -> anyhow::Result<()> {
    let sys = SystemRecord { threads: state.threads.clone(), data_epoch: state.data_epoch };
    let record = KVRecord {
        key: SYSTEM_KEY.to_vec(),
        generation: 0,
        data_epoch: None,
        data: bincode::serialize(&sys)?,
    };
    store.put(meta_set, vec![record], false)
}

// ---- KVStore I/O — Thread Control Record ----

/// Read a single Thread Control Record. Returns None if it doesn't exist.
/// Returns `(state, kv_generation)` where `kv_generation` is the KVStore record
/// generation for use with CAS writes.
pub fn read_thread_control(
    store: &KVStore,
    meta_set: &str,
    thread_id: &ThreadIdentifier,
) -> anyhow::Result<Option<(ThreadControlState, u64)>> {
    let key = thread_ctrl_key(thread_id);
    let results = store.get(meta_set, &[key])?;
    match results.into_iter().next().flatten() {
        Some(record) => {
            let generation = record.generation;
            let ctrl: ThreadControlRecord = bincode::deserialize(&record.data)?;
            Ok(Some((ctrl.to_state(*thread_id)?, generation)))
        }
        None => Ok(None),
    }
}

/// Write a Thread Control Record.
/// `cas_generation`: if `Some`, use CAS with this generation. If `None`, write unconditionally.
pub fn write_thread_control(
    store: &KVStore,
    meta_set: &str,
    state: &ThreadControlState,
    cas_generation: Option<u64>,
) -> anyhow::Result<()> {
    let key = thread_ctrl_key(&state.thread_id);
    let ctrl = ThreadControlRecord::from_state(state);
    let record = KVRecord {
        key,
        generation: cas_generation.unwrap_or(0),
        data_epoch: None,
        data: bincode::serialize(&ctrl)?,
    };
    store.put(meta_set, vec![record], cas_generation.is_some())
}

/// Read Thread Control Records for all given thread IDs.
/// Returns a Vec in the same order as the input. None for missing records.
pub fn read_all_thread_controls(
    store: &KVStore,
    meta_set: &str,
    thread_ids: &[ThreadIdentifier],
) -> anyhow::Result<Vec<Option<(ThreadControlState, u64)>>> {
    let keys: Vec<Vec<u8>> = thread_ids.iter().map(thread_ctrl_key).collect();
    let results = store.get(meta_set, &keys)?;
    results
        .into_iter()
        .zip(thread_ids.iter())
        .map(|(opt_record, thread_id)| match opt_record {
            Some(record) => {
                let generation = record.generation;
                let ctrl: ThreadControlRecord = bincode::deserialize(&record.data)?;
                Ok(Some((ctrl.to_state(*thread_id)?, generation)))
            }
            None => Ok(None),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::thread_accounts::durable::kv_store::in_memory::InMemoryKVStore;

    fn test_store() -> KVStore {
        KVStore::InMemory(InMemoryKVStore::new())
    }

    #[test]
    fn test_system_record_roundtrip() {
        let store = test_store();
        assert!(read_system_record(&store, META_SET).unwrap().is_none());

        let state =
            SystemState { threads: HashSet::from([ThreadIdentifier::default()]), data_epoch: 0 };
        write_system_record(&store, META_SET, &state).unwrap();

        let loaded = read_system_record(&store, META_SET).unwrap().unwrap();
        assert_eq!(loaded.threads.len(), 1);
        assert_eq!(loaded.data_epoch, 0);
    }

    #[test]
    fn test_thread_control_roundtrip() {
        let store = test_store();
        let tid = ThreadIdentifier::default();
        assert!(read_thread_control(&store, META_SET, &tid).unwrap().is_none());

        let state = ThreadControlState::new_uninitialized(tid);
        write_thread_control(&store, META_SET, &state, None).unwrap();

        let (loaded, _gen) = read_thread_control(&store, META_SET, &tid).unwrap().unwrap();
        assert_eq!(loaded.update_phase, UpdatePhase::Uninitialized);
        assert!(loaded.last_block_id.is_none());
    }

    #[test]
    fn test_thread_control_idle_roundtrip() {
        let store = test_store();
        let tid = ThreadIdentifier::default();
        let block_id = BlockIdentifier::default();

        let state = ThreadControlState::new_idle(tid, block_id);
        write_thread_control(&store, META_SET, &state, None).unwrap();

        let (loaded, _gen) = read_thread_control(&store, META_SET, &tid).unwrap().unwrap();
        assert_eq!(loaded.update_phase, UpdatePhase::Idle);
        assert_eq!(loaded.last_block_id, Some(block_id));
    }

    #[test]
    fn test_read_all_thread_controls() {
        let store = test_store();
        let tid1 = ThreadIdentifier::default();
        let tid2 = ThreadIdentifier::from([1u8; 34]);

        let state1 = ThreadControlState::new_uninitialized(tid1);
        write_thread_control(&store, META_SET, &state1, None).unwrap();

        let results = read_all_thread_controls(&store, META_SET, &[tid1, tid2]).unwrap();
        assert!(results[0].is_some());
        assert!(results[1].is_none());
    }

    #[test]
    fn test_all_phases_roundtrip() {
        let store = test_store();
        let tid = ThreadIdentifier::default();

        for phase in [
            UpdatePhase::Uninitialized,
            UpdatePhase::Idle,
            UpdatePhase::WritingCopyA,
            UpdatePhase::WritingCopyB,
            UpdatePhase::Collapsed,
        ] {
            let state =
                ThreadControlState { thread_id: tid, update_phase: phase, last_block_id: None };
            write_thread_control(&store, META_SET, &state, None).unwrap();
            let (loaded, _gen) = read_thread_control(&store, META_SET, &tid).unwrap().unwrap();
            assert_eq!(loaded.update_phase, phase);
        }
    }

    #[test]
    fn test_copy_ready_helpers() {
        assert!(UpdatePhase::Idle.copy_a_ready());
        assert!(UpdatePhase::Idle.copy_b_ready());

        assert!(!UpdatePhase::WritingCopyA.copy_a_ready());
        assert!(UpdatePhase::WritingCopyA.copy_b_ready());

        assert!(UpdatePhase::WritingCopyB.copy_a_ready());
        assert!(!UpdatePhase::WritingCopyB.copy_b_ready());

        assert!(!UpdatePhase::Uninitialized.copy_a_ready());
        assert!(!UpdatePhase::Uninitialized.copy_b_ready());

        assert!(!UpdatePhase::Collapsed.copy_a_ready());
        assert!(!UpdatePhase::Collapsed.copy_b_ready());
    }
}
