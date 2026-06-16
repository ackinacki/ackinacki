use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use parking_lot::Mutex;
use parking_lot::RwLock;

use super::config::ArchiveStoreConfig;
use super::control::*;
use super::error::ArchiveStateError;
use super::update::ThreadSnapshot;
use crate::thread_accounts::archive::apply::routing_to_key;
use crate::thread_accounts::archive::update::AccumulatedUpdate;
use crate::thread_accounts::durable::kv_store::KVRecord;
use crate::thread_accounts::durable::kv_store::KVStore;
use crate::thread_accounts::durable::repository::AccountWrittenCache;
use crate::ArchiveOperation;
use crate::ThreadAccount;

/// Batch size for `thread_init_from_raw_entries`. Sized for the V2 backend's
/// native `client.batch(...)` write path — one round trip per batch — so
/// larger batches amortize the per-RPC fixed cost. The Aerospike server is
/// happy with batches in the tens of thousands.
const THREAD_INIT_BATCH_SIZE: usize = 10_000;

struct ArchiveStateStoreInner {
    /// KVStore backend (Aerospike or InMemory).
    store: KVStore,

    /// Base names for the two account-data set families. The actual set
    /// in use at any moment is `{base}_e{data_epoch}` — see
    /// `set_accounts_a()` / `set_accounts_b()`. Encoding the epoch in the
    /// set name makes a stale read after `reset()` physically impossible:
    /// the new epoch's set is empty until `thread_init_*` populates it,
    /// and the old epoch's set is no longer referenced by any read path.
    set_accounts_base_a: String,
    set_accounts_base_b: String,
    set_meta: String,

    /// In-memory mirror of the System Record's active thread set.
    thread_registry: RwLock<HashSet<ThreadIdentifier>>,

    /// In-memory mirror of the System Record's current data_epoch.
    data_epoch: RwLock<u64>,

    /// Per-thread OS mutexes for serializing concurrent apply_update calls.
    thread_write_locks: RwLock<HashMap<ThreadIdentifier, Arc<Mutex<()>>>>,

    /// In-flight AccumulatedUpdates. Read path checks these before KVStore.
    active_updates: RwLock<Vec<Arc<AccumulatedUpdate>>>,

    account_written_cache: Arc<AccountWrittenCache>,
}

/// Multi-thread archive state store.
/// Cheap to clone — all clones share the same underlying state.
#[derive(Clone)]
pub struct ArchiveStateStore {
    inner: Arc<ArchiveStateStoreInner>,
}

impl ArchiveStateStore {
    /// Connect to the store and load the current state.
    ///
    /// - If no System Record exists: creates a fresh one (empty thread set, data_epoch=0).
    /// - Reads Thread Control Records for all registered threads.
    /// - Returns `Err(NotReady)` if any thread is in `WritingCopyA` or `WritingCopyB`.
    /// - Returns `Err(CorruptedControlRecord)` if any thread is `Collapsed`.
    /// - Missing control record for a registered thread: writes a fresh `Uninitialized`
    ///   record (handles crash between reset step 1 and step 2).
    pub fn new(store: KVStore, config: ArchiveStoreConfig) -> Result<Self, ArchiveStateError> {
        // Compute prefixed set names
        let prefix = &config.node_id;
        let set_meta =
            if prefix.is_empty() { "meta".to_string() } else { format!("{prefix}_meta") };
        let set_accounts_base_a = if prefix.is_empty() {
            "accounts_A".to_string()
        } else {
            format!("{prefix}_accounts_A")
        };
        let set_accounts_base_b = if prefix.is_empty() {
            "accounts_B".to_string()
        } else {
            format!("{prefix}_accounts_B")
        };

        // Step 1: Read or create System Record
        let system = match read_system_record(&store, &set_meta).map_err(ArchiveStateError::from)? {
            Some(sys) => sys,
            None => {
                let sys = SystemState { threads: HashSet::new(), data_epoch: 0 };
                write_system_record(&store, &set_meta, &sys).map_err(ArchiveStateError::from)?;
                sys
            }
        };

        // Step 2: Read and validate Thread Control Records
        let mut thread_write_locks = HashMap::new();

        let thread_ids: Vec<ThreadIdentifier> = system.threads.iter().copied().collect();
        let controls = read_all_thread_controls(&store, &set_meta, &thread_ids)
            .map_err(ArchiveStateError::from)?;

        for (thread_id, opt_control) in thread_ids.iter().zip(controls) {
            let control = match opt_control {
                Some((c, _generation)) => c,
                None => {
                    let fresh = ThreadControlState::new_uninitialized(*thread_id);
                    write_thread_control(&store, &set_meta, &fresh, None)
                        .map_err(ArchiveStateError::from)?;
                    fresh
                }
            };

            match control.update_phase {
                UpdatePhase::Uninitialized | UpdatePhase::Idle => {}
                UpdatePhase::WritingCopyA | UpdatePhase::WritingCopyB => {
                    return Err(ArchiveStateError::NotReady {
                        thread_id: *thread_id,
                        phase: control.update_phase,
                    });
                }
                UpdatePhase::Collapsed => {
                    return Err(ArchiveStateError::CorruptedControlRecord(
                        *thread_id,
                        "collapsed thread at startup".to_string(),
                    ));
                }
            }

            thread_write_locks.insert(*thread_id, Arc::new(Mutex::new(())));
        }

        Ok(Self {
            inner: Arc::new(ArchiveStateStoreInner {
                store,
                set_accounts_base_a,
                set_accounts_base_b,
                set_meta,
                thread_registry: RwLock::new(system.threads),
                data_epoch: RwLock::new(system.data_epoch),
                thread_write_locks: RwLock::new(thread_write_locks),
                active_updates: RwLock::new(Vec::new()),
                account_written_cache: Arc::new(AccountWrittenCache::new()),
            }),
        })
    }

    /// Convenience constructor for in-memory testing.
    pub fn in_memory() -> Self {
        let store = KVStore::InMemory(
            crate::thread_accounts::durable::kv_store::in_memory::InMemoryKVStore::new(),
        );
        let config = ArchiveStoreConfig {
            aerospike_address: None,
            node_id: String::new(),
            write_parallelism: 0,
        };
        Self::new(store, config).expect("InMemory store creation cannot fail")
    }

    /// Connect to an Aerospike-backed archive store.
    pub fn connect_aerospike(address: &str) -> Result<Self, ArchiveStateError> {
        let kv_config = crate::thread_accounts::durable::kv_store::aerospike::AerospikeKVConfig {
            address: address.to_string(),
            namespace: "node".to_string(),
            num_write_threads: 0,
            metrics: None,
        };
        let kv = KVStore::Aerospike(
            crate::thread_accounts::durable::kv_store::aerospike::AerospikeKVStore::new(kv_config)
                .map_err(ArchiveStateError::Store)?,
        );
        let config = ArchiveStoreConfig {
            aerospike_address: Some(address.to_string()),
            node_id: String::new(),
            write_parallelism: 0,
        };
        Self::new(kv, config)
    }

    /// Read the Thread Control State for a specific blockchain thread.
    pub fn get_thread_control_state(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> Result<Option<ThreadControlState>, ArchiveStateError> {
        Ok(read_thread_control(&self.inner.store, &self.inner.set_meta, thread_id)
            .map_err(ArchiveStateError::from)?
            .map(|(state, _generation)| state))
    }

    /// Read the Thread Control States for all registered blockchain threads.
    pub fn get_all_thread_control_states(
        &self,
    ) -> Result<Vec<ThreadControlState>, ArchiveStateError> {
        let registry = self.inner.thread_registry.read();
        let thread_ids: Vec<ThreadIdentifier> = registry.iter().copied().collect();
        drop(registry);

        let controls =
            read_all_thread_controls(&self.inner.store, &self.inner.set_meta, &thread_ids)
                .map_err(ArchiveStateError::from)?;

        let mut result = Vec::with_capacity(thread_ids.len());
        for (tid, opt) in thread_ids.into_iter().zip(controls) {
            match opt {
                Some((state, _generation)) => result.push(state),
                None => {
                    return Err(ArchiveStateError::CorruptedControlRecord(
                        tid,
                        "get_all_thread_control_states: missing control record".to_string(),
                    ))
                }
            }
        }
        Ok(result)
    }

    /// Access the underlying KVStore.
    pub(crate) fn store(&self) -> &KVStore {
        &self.inner.store
    }

    /// Access the thread registry.
    pub(crate) fn thread_registry(&self) -> &RwLock<HashSet<ThreadIdentifier>> {
        &self.inner.thread_registry
    }

    /// Access the data epoch.
    pub(crate) fn data_epoch(&self) -> u64 {
        *self.inner.data_epoch.read()
    }

    /// Access per-thread write locks.
    pub(crate) fn thread_write_locks(&self) -> &RwLock<HashMap<ThreadIdentifier, Arc<Mutex<()>>>> {
        &self.inner.thread_write_locks
    }

    /// Access in-flight active updates.
    pub(crate) fn active_updates(&self) -> &RwLock<Vec<Arc<AccumulatedUpdate>>> {
        &self.inner.active_updates
    }

    pub(crate) fn account_written_cache(&self) -> Arc<AccountWrittenCache> {
        Arc::clone(&self.inner.account_written_cache)
    }

    /// Prefixed set name for accounts Copy A at the *current* data_epoch.
    /// Format: `{base}_e{epoch}`. Bumping `data_epoch` (via `reset()`)
    /// shifts all reads and writes to a new, empty set; the previous
    /// epoch's set is left untouched until `reset()` truncates it.
    pub(crate) fn set_accounts_a(&self) -> String {
        Self::format_set_name(&self.inner.set_accounts_base_a, self.data_epoch())
    }

    /// Prefixed set name for accounts Copy B at the *current* data_epoch.
    /// See `set_accounts_a` for naming semantics.
    pub(crate) fn set_accounts_b(&self) -> String {
        Self::format_set_name(&self.inner.set_accounts_base_b, self.data_epoch())
    }

    /// Build a set name from a base and an explicit epoch. Used for both
    /// the current-epoch accessors above and the truncate-old-epoch
    /// cleanup in `reset()`.
    fn format_set_name(base: &str, epoch: u64) -> String {
        format!("{base}_e{epoch}")
    }

    /// Prefixed set name for metadata.
    pub(crate) fn set_meta(&self) -> &str {
        &self.inner.set_meta
    }

    /// Ensure a thread exists in the archive store. If the thread is not
    /// registered, creates it as Idle with the given block_id.
    /// If already registered, does nothing (idempotent).
    pub fn ensure_thread(
        &self,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
    ) -> Result<(), ArchiveStateError> {
        {
            let registry = self.inner.thread_registry.read();
            if registry.contains(thread_id) {
                return Ok(());
            }
        }

        let kv = &self.inner.store;
        let meta = &self.inner.set_meta;

        // Check if control record exists (from a previous run)
        if read_thread_control(kv, meta, thread_id).map_err(ArchiveStateError::from)?.is_some() {
            // Control record exists but not in registry — add to registry
            let mut registry = self.inner.thread_registry.write();
            registry.insert(*thread_id);
            let system = SystemState {
                threads: registry.clone(),
                data_epoch: *self.inner.data_epoch.read(),
            };
            write_system_record(kv, meta, &system).map_err(ArchiveStateError::from)?;
            self.inner.thread_write_locks.write().insert(*thread_id, Arc::new(Mutex::new(())));
            return Ok(());
        }

        // Thread doesn't exist — create Idle control record
        let idle = ThreadControlState::new_idle(*thread_id, *block_id);
        write_thread_control(kv, meta, &idle, None).map_err(ArchiveStateError::from)?;

        let mut registry = self.inner.thread_registry.write();
        registry.insert(*thread_id);
        let system =
            SystemState { threads: registry.clone(), data_epoch: *self.inner.data_epoch.read() };
        write_system_record(kv, meta, &system).map_err(ArchiveStateError::from)?;

        self.inner.thread_write_locks.write().insert(*thread_id, Arc::new(Mutex::new(())));

        Ok(())
    }

    /// Apply an accumulated update via the A/B commit protocol.
    pub fn apply_update(
        &self,
        update: &super::update::AccumulatedUpdate,
    ) -> Result<(), ArchiveStateError> {
        super::apply::apply_update_impl(self, update)
    }

    /// Read account data for a routing.
    /// Always reads from Copy A (safe at runtime for all phases).
    pub fn read_account_operation(
        &self,
        routing: &node_types::AccountRouting,
    ) -> Result<Option<ThreadAccount>, ArchiveStateError> {
        // Check active updates first — serves in-flight data during writes
        {
            for update in self.active_updates().read().iter() {
                if let Some(operation) = update.operations.get(routing) {
                    return Ok(match operation {
                        ArchiveOperation::UpdateOrInsert(account) => Some(account.clone()),
                        ArchiveOperation::Remove => None,
                    });
                }
            }
        }

        // Not in any active update — read from Copy A. The set name is
        // suffixed with the current data_epoch, so a stale record from a
        // prior epoch is unreachable by construction (it lives in a
        // differently-named set that no read path queries).
        let kv = self.store();
        let key = routing_to_key(routing);
        let results = kv.get(&self.set_accounts_a(), &[key]).map_err(ArchiveStateError::from)?;

        match results.into_iter().next().flatten() {
            Some(record) if !record.data.is_empty() => {
                let account: ThreadAccount = bincode::deserialize(&record.data).map_err(|e| {
                    ArchiveStateError::Store(anyhow::anyhow!("Deserialize error: {e}"))
                })?;
                Ok(Some(account))
            }
            _ => Ok(None),
        }
    }

    /// Return a human-readable summary of the archive state:
    /// data_epoch, number of threads, and per-thread phase + last_block_id.
    pub fn summary(&self) -> String {
        let epoch = self.data_epoch();
        let registry = self.inner.thread_registry.read();
        let num_threads = registry.len();
        drop(registry);

        let mut lines = vec![format!("Archive: epoch={epoch}, threads={num_threads}")];

        match self.get_all_thread_control_states() {
            Ok(states) => {
                for s in &states {
                    let block = s
                        .last_block_id
                        .map(|b| b.to_hex_string())
                        .unwrap_or_else(|| "none".to_string());
                    lines.push(format!(
                        "  thread {} phase={:?} block={}",
                        s.thread_id, s.update_phase, block,
                    ));
                }
            }
            Err(e) => {
                lines.push(format!("  error reading thread states: {e}"));
            }
        }

        let active = self.inner.active_updates.read().len();
        if active > 0 {
            lines.push(format!("  active_updates={active}"));
        }

        lines.join("\n")
    }

    /// Populate both copies of a single thread from a complete snapshot.
    /// The thread must be in `Uninitialized` phase.
    pub fn thread_init(
        &self,
        thread_id: &ThreadIdentifier,
        initial_block_id: BlockIdentifier,
        snapshot: &ThreadSnapshot,
    ) -> Result<(), ArchiveStateError> {
        self.thread_init_from_entries(
            thread_id,
            initial_block_id,
            snapshot.entries.iter().map(|(routing, account)| Ok((*routing, account.clone()))),
        )
    }

    pub fn thread_init_from_entries<I>(
        &self,
        thread_id: &ThreadIdentifier,
        initial_block_id: BlockIdentifier,
        entries: I,
    ) -> Result<(), ArchiveStateError>
    where
        I: IntoIterator<Item = anyhow::Result<(node_types::AccountRouting, ThreadAccount)>>,
    {
        self.thread_init_from_raw_entries(
            thread_id,
            initial_block_id,
            entries.into_iter().map(|entry| {
                entry.and_then(|(routing, account)| {
                    let data = account.write_bytes()?;
                    Ok((routing, data))
                })
            }),
        )
    }

    pub fn thread_init_from_raw_entries<I>(
        &self,
        thread_id: &ThreadIdentifier,
        initial_block_id: BlockIdentifier,
        entries: I,
    ) -> Result<(), ArchiveStateError>
    where
        I: IntoIterator<Item = anyhow::Result<(node_types::AccountRouting, Vec<u8>)>>,
    {
        let kv = &self.inner.store;
        let meta = &self.inner.set_meta;
        let data_epoch = self.data_epoch();
        // Resolve epoch-suffixed set names once. We don't expect a
        // concurrent `reset()` here (the caller holds the snapshot pin),
        // but binding the names locally guarantees every batch in this
        // import targets the same set even if `data_epoch()` were to
        // change mid-flight.
        let set_a = self.set_accounts_a();
        let set_b = self.set_accounts_b();

        // Validate: must be Uninitialized
        let (ctrl, _generation) = read_thread_control(kv, meta, thread_id)
            .map_err(ArchiveStateError::from)?
            .ok_or_else(|| {
                ArchiveStateError::CorruptedControlRecord(
                    *thread_id,
                    "thread_init: missing control record".to_string(),
                )
            })?;

        if ctrl.update_phase != UpdatePhase::Uninitialized {
            return Err(ArchiveStateError::NotUninitialized(*thread_id));
        }

        let mut batch = Vec::with_capacity(THREAD_INIT_BATCH_SIZE);
        let mut total_records: usize = 0;
        let mut batch_index: usize = 0;
        for entry in entries {
            let (routing, data) = entry.map_err(ArchiveStateError::from)?;
            let data = bincode::serialize(&data)
                .map_err(|e| ArchiveStateError::from(anyhow::Error::from(e)))?;
            batch.push(KVRecord {
                key: super::apply::routing_to_key(&routing),
                generation: 0,
                data_epoch: Some(data_epoch as u32),
                data,
            });
            total_records += 1;

            if batch.len() >= THREAD_INIT_BATCH_SIZE {
                let max_data = batch.iter().map(|r| r.data.len()).max().unwrap_or(0);
                let total_data: usize = batch.iter().map(|r| r.data.len()).sum();
                tracing::debug!(
                    target: "monit",
                    "thread_init batch={batch_index} starting A: records={} max_data={} total_data={}",
                    batch.len(), max_data, total_data,
                );
                kv.bulk_put_no_overwrite(&set_a, batch.clone()).map_err(ArchiveStateError::from)?;
                tracing::debug!(target: "monit", "thread_init batch={batch_index} A done, starting B");
                kv.bulk_put_no_overwrite(&set_b, std::mem::take(&mut batch))
                    .map_err(ArchiveStateError::from)?;
                tracing::debug!(target: "monit", "thread_init batch={batch_index} B done, total_records={total_records}");
                batch_index += 1;
            }
        }

        if !batch.is_empty() {
            tracing::debug!(
                target: "monit",
                "thread_init final batch={batch_index} starting A: records={}", batch.len(),
            );
            kv.bulk_put_no_overwrite(&set_a, batch.clone()).map_err(ArchiveStateError::from)?;
            tracing::debug!(target: "monit", "thread_init final batch={batch_index} A done, starting B");
            kv.bulk_put_no_overwrite(&set_b, batch).map_err(ArchiveStateError::from)?;
            tracing::debug!(target: "monit", "thread_init final batch={batch_index} B done, total_records={total_records}");
        }

        tracing::debug!(target: "monit", "thread_init writing thread control: total_records={total_records}");
        // Mark thread Idle
        let idle = ThreadControlState::new_idle(*thread_id, initial_block_id);
        write_thread_control(kv, meta, &idle, None).map_err(ArchiveStateError::from)?;
        tracing::debug!(target: "monit", "thread_init done");

        Ok(())
    }

    /// Reset all threads: increment data_epoch, all threads → Uninitialized.
    /// All threads must be Idle. After reset, each thread must be populated
    /// via `thread_init` before reads or updates.
    pub fn reset(&self) -> Result<(), ArchiveStateError> {
        let kv = &self.inner.store;
        let meta = &self.inner.set_meta;

        // Read System Record
        let system =
            read_system_record(kv, meta).map_err(ArchiveStateError::from)?.ok_or_else(|| {
                ArchiveStateError::Store(anyhow::anyhow!("System record not found during reset"))
            })?;

        // Acquire all per-thread mutexes in sorted order
        let mut sorted_ids: Vec<ThreadIdentifier> = system.threads.iter().copied().collect();
        sorted_ids.sort();

        let held_arcs: Vec<Arc<Mutex<()>>> = {
            let locks_map = self.inner.thread_write_locks.read();
            sorted_ids.iter().filter_map(|tid| locks_map.get(tid).cloned()).collect()
        };
        let _guards: Vec<_> = held_arcs.iter().map(|m| m.lock()).collect();

        // Verify all threads are Idle
        for thread_id in &sorted_ids {
            let (ctrl, _generation) = read_thread_control(kv, meta, thread_id)
                .map_err(ArchiveStateError::from)?
                .ok_or_else(|| {
                    ArchiveStateError::CorruptedControlRecord(
                        *thread_id,
                        "reset: missing control record".to_string(),
                    )
                })?;

            if ctrl.update_phase != UpdatePhase::Idle {
                return Err(ArchiveStateError::NotReady {
                    thread_id: *thread_id,
                    phase: ctrl.update_phase,
                });
            }
        }

        // Capture the OLD epoch's set names before we bump. After the
        // epoch increment, `set_accounts_a()` / `set_accounts_b()` will
        // resolve to the new (empty) sets — the old ones become
        // garbage-only. Truncating them frees server-side storage but is
        // not load-bearing for correctness: no read path queries the old
        // names once `data_epoch` advances.
        let old_set_a = Self::format_set_name(&self.inner.set_accounts_base_a, system.data_epoch);
        let old_set_b = Self::format_set_name(&self.inner.set_accounts_base_b, system.data_epoch);

        // Write new System Record with incremented data_epoch
        let new_epoch = system.data_epoch + 1;
        let new_system = SystemState { threads: system.threads.clone(), data_epoch: new_epoch };
        write_system_record(kv, meta, &new_system).map_err(ArchiveStateError::from)?;

        // Mark all threads Uninitialized
        for thread_id in &sorted_ids {
            let uninit = ThreadControlState::new_uninitialized(*thread_id);
            write_thread_control(kv, meta, &uninit, None).map_err(ArchiveStateError::from)?;
        }

        // Update in-memory data_epoch — from this point on, all reads
        // and writes target the NEW empty sets. The old sets are
        // unreferenced; subsequent truncates are pure cleanup.
        *self.inner.data_epoch.write() = new_epoch;

        // Cleanup of the prior epoch's sets. Failures here do not affect
        // correctness (the new epoch's sets are isolated by name); we
        // log and continue rather than returning Err. The next reset on
        // top of the same epoch base would just truncate an empty set.
        if let Err(err) = kv.truncate_set(&old_set_a) {
            tracing::warn!(
                target: "monit",
                "archive.reset: truncate of old set_accounts_a={old_set_a} failed (non-fatal): {err}",
            );
        }
        if let Err(err) = kv.truncate_set(&old_set_b) {
            tracing::warn!(
                target: "monit",
                "archive.reset: truncate of old set_accounts_b={old_set_b} failed (non-fatal): {err}",
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use node_types::BlockIdentifier;

    use super::*;
    use crate::thread_accounts::durable::archive::apply;
    use crate::thread_accounts::durable::archive::update::AccumulatedUpdate;
    use crate::thread_accounts::durable::kv_store::in_memory::InMemoryKVStore;

    fn test_kv() -> KVStore {
        KVStore::InMemory(InMemoryKVStore::new())
    }

    fn test_config() -> ArchiveStoreConfig {
        ArchiveStoreConfig { aerospike_address: None, node_id: String::new(), write_parallelism: 0 }
    }

    #[test]
    fn test_new_empty() {
        let kv = test_kv();
        let store = ArchiveStateStore::new(kv, test_config()).unwrap();
        let states = store.get_all_thread_control_states().unwrap();
        assert!(states.is_empty());
    }

    #[test]
    fn test_new_creates_system_record() {
        let kv = test_kv();
        let _store = ArchiveStateStore::new(kv.clone(), test_config()).unwrap();
        // Second call reads existing System Record
        let store2 = ArchiveStateStore::new(kv, test_config()).unwrap();
        assert!(store2.get_all_thread_control_states().unwrap().is_empty());
    }

    #[test]
    fn test_new_with_idle_threads() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();
        let block_id = BlockIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        let ctrl = ThreadControlState::new_idle(tid, block_id);
        write_thread_control(&kv, META_SET, &ctrl, None).unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();
        let states = store.get_all_thread_control_states().unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].update_phase, UpdatePhase::Idle);
        assert_eq!(states[0].last_block_id, Some(block_id));
    }

    #[test]
    fn test_new_with_uninitialized_thread() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        let ctrl = ThreadControlState::new_uninitialized(tid);
        write_thread_control(&kv, META_SET, &ctrl, None).unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();
        let states = store.get_all_thread_control_states().unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].update_phase, UpdatePhase::Uninitialized);
    }

    #[test]
    fn test_new_rejects_writing_copy_a() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        let ctrl = ThreadControlState {
            thread_id: tid,
            update_phase: UpdatePhase::WritingCopyA,
            last_block_id: None,
        };
        write_thread_control(&kv, META_SET, &ctrl, None).unwrap();

        let result = ArchiveStateStore::new(kv, test_config());
        assert!(matches!(
            result,
            Err(ArchiveStateError::NotReady { phase: UpdatePhase::WritingCopyA, .. })
        ));
    }

    #[test]
    fn test_new_rejects_writing_copy_b() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        let ctrl = ThreadControlState {
            thread_id: tid,
            update_phase: UpdatePhase::WritingCopyB,
            last_block_id: None,
        };
        write_thread_control(&kv, META_SET, &ctrl, None).unwrap();

        let result = ArchiveStateStore::new(kv, test_config());
        assert!(matches!(
            result,
            Err(ArchiveStateError::NotReady { phase: UpdatePhase::WritingCopyB, .. })
        ));
    }

    #[test]
    fn test_new_rejects_collapsed_in_registry() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        let ctrl = ThreadControlState::new_collapsed(tid);
        write_thread_control(&kv, META_SET, &ctrl, None).unwrap();

        let result = ArchiveStateStore::new(kv, test_config());
        assert!(matches!(result, Err(ArchiveStateError::CorruptedControlRecord(_, _))));
    }

    #[test]
    fn test_new_missing_control_record_creates_uninitialized() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();

        // System record lists a thread, but no control record exists
        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();
        let states = store.get_all_thread_control_states().unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0].update_phase, UpdatePhase::Uninitialized);
    }

    #[test]
    fn test_new_multiple_threads() {
        let kv = test_kv();
        let tid1 = ThreadIdentifier::default();
        let tid2 = ThreadIdentifier::from([1u8; 34]);
        let block_id = BlockIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid1, tid2]), data_epoch: 5 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid1, block_id), None)
            .unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_uninitialized(tid2), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();
        assert_eq!(store.data_epoch(), 5);
        let states = store.get_all_thread_control_states().unwrap();
        assert_eq!(states.len(), 2);
    }

    #[test]
    fn test_in_memory_convenience() {
        let store = ArchiveStateStore::in_memory();
        assert!(store.get_all_thread_control_states().unwrap().is_empty());
    }

    #[test]
    fn test_per_thread_mutexes_created() {
        let kv = test_kv();
        let tid1 = ThreadIdentifier::default();
        let tid2 = ThreadIdentifier::from([1u8; 34]);

        let sys = SystemState { threads: HashSet::from([tid1, tid2]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(
            &kv,
            META_SET,
            &ThreadControlState::new_idle(tid1, BlockIdentifier::default()),
            None,
        )
        .unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_uninitialized(tid2), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();
        let locks = store.thread_write_locks().read();
        assert!(locks.contains_key(&tid1));
        assert!(locks.contains_key(&tid2));
    }

    // ---- thread_init tests ----

    fn make_routing(seed: u8) -> node_types::AccountRouting {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        node_types::AccountIdentifier::new(bytes).redirect()
    }

    fn make_block(seed: u8) -> BlockIdentifier {
        BlockIdentifier::new([seed; 32])
    }

    #[test]
    fn test_thread_init_basic() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();
        let block_0 = make_block(0);

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_uninitialized(tid), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();

        let mut snapshot = ThreadSnapshot::new();
        let routing = make_routing(1);
        snapshot.insert(routing, crate::ThreadAccount::default());

        store.thread_init(&tid, block_0, &snapshot).unwrap();

        let ctrl = store.get_thread_control_state(&tid).unwrap().unwrap();
        assert_eq!(ctrl.update_phase, UpdatePhase::Idle);
        assert_eq!(ctrl.last_block_id, Some(block_0));

        let result = store.read_account_operation(&routing).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_thread_init_not_uninitialized_rejected() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();
        let block_0 = make_block(0);

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid, block_0), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();

        let snapshot = ThreadSnapshot::new();
        let result = store.thread_init(&tid, block_0, &snapshot);
        assert!(matches!(result, Err(ArchiveStateError::NotUninitialized(_))));
    }

    #[test]
    fn test_thread_init_empty_snapshot() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();
        let block_0 = make_block(0);

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_uninitialized(tid), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();

        let snapshot = ThreadSnapshot::new();
        store.thread_init(&tid, block_0, &snapshot).unwrap();

        let ctrl = store.get_thread_control_state(&tid).unwrap().unwrap();
        assert_eq!(ctrl.update_phase, UpdatePhase::Idle);
        assert_eq!(ctrl.last_block_id, Some(block_0));
    }

    #[test]
    fn test_thread_init_data_in_both_copies() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();
        let block_0 = make_block(0);

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_uninitialized(tid), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();

        let mut snapshot = ThreadSnapshot::new();
        let routing = make_routing(1);
        snapshot.insert(routing, crate::ThreadAccount::default());

        store.thread_init(&tid, block_0, &snapshot).unwrap();

        let key = apply::routing_to_key(&routing);
        let a = store.store().get(apply::ACCOUNTS_SET_A, std::slice::from_ref(&key)).unwrap();
        let b = store.store().get(apply::ACCOUNTS_SET_B, &[key]).unwrap();
        assert!(a[0].is_some());
        assert!(b[0].is_some());
    }

    // ---- reset tests ----

    #[test]
    fn test_reset_basic() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();
        let block_0 = make_block(0);

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid, block_0), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();
        assert_eq!(store.data_epoch(), 0);

        store.reset().unwrap();

        assert_eq!(store.data_epoch(), 1);

        let ctrl = store.get_thread_control_state(&tid).unwrap().unwrap();
        assert_eq!(ctrl.update_phase, UpdatePhase::Uninitialized);
        assert!(ctrl.last_block_id.is_none());
    }

    #[test]
    fn test_reset_multiple_threads() {
        let kv = test_kv();
        let tid1 = ThreadIdentifier::default();
        let tid2 = ThreadIdentifier::from([1u8; 34]);
        let block_0 = make_block(0);

        let sys = SystemState { threads: HashSet::from([tid1, tid2]), data_epoch: 3 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid1, block_0), None)
            .unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid2, block_0), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();

        store.reset().unwrap();

        assert_eq!(store.data_epoch(), 4);

        for tid in [tid1, tid2] {
            let ctrl = store.get_thread_control_state(&tid).unwrap().unwrap();
            assert_eq!(ctrl.update_phase, UpdatePhase::Uninitialized);
        }

        assert_eq!(store.thread_registry().read().len(), 2);
    }

    #[test]
    fn test_reset_not_idle_rejected() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_uninitialized(tid), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();

        let result = store.reset();
        assert!(matches!(result, Err(ArchiveStateError::NotReady { .. })));
    }

    #[test]
    fn test_reset_then_init_then_update() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();
        let block_0 = make_block(0);

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid, block_0), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();

        // Reset
        store.reset().unwrap();
        assert_eq!(store.data_epoch(), 1);

        // Init with snapshot
        let mut snapshot = ThreadSnapshot::new();
        let routing = make_routing(1);
        snapshot.insert(routing, crate::ThreadAccount::default());
        store.thread_init(&tid, block_0, &snapshot).unwrap();

        // Apply update
        let block_1 = make_block(1);
        let mut update = AccumulatedUpdate::new();
        update.transition_thread(tid, block_0, block_1);
        update.insert(routing, ArchiveOperation::UpdateOrInsert(crate::ThreadAccount::default()));
        store.apply_update(&update).unwrap();

        let ctrl = store.get_thread_control_state(&tid).unwrap().unwrap();
        assert_eq!(ctrl.last_block_id, Some(block_1));
    }
}
