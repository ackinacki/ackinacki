use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use node_types::AccountRouting;
use node_types::ThreadIdentifier;
use parking_lot::Mutex;

use super::control::*;
use super::error::ArchiveStateError;
use super::store::ArchiveStateStore;
use super::update::AccumulatedUpdate;
use crate::thread_accounts::durable::kv_store::KVRecord;
use crate::thread_accounts::durable::kv_store::KVStore;
use crate::thread_accounts::ArchiveOperation;

/// Set names for account data records *at data_epoch=0*. Tests that
/// don't bump the epoch (i.e., everything except `reset()` tests) can
/// use these directly. Tests that exercise reset must use
/// `store.set_accounts_a()` / `set_accounts_b()` instead.
#[cfg(test)]
pub(crate) const ACCOUNTS_SET_A: &str = "accounts_A_e0";
#[cfg(test)]
pub(crate) const ACCOUNTS_SET_B: &str = "accounts_B_e0";

/// Serialize AccountRouting as raw 64 bytes for use as a KV key.
pub(crate) fn routing_to_key(routing: &AccountRouting) -> Vec<u8> {
    let mut key = Vec::with_capacity(64);
    key.extend_from_slice(routing.dapp_id().as_slice());
    key.extend_from_slice(routing.account_id().as_slice());
    key
}

pub(crate) fn key_to_routing(key: &[u8]) -> anyhow::Result<AccountRouting> {
    anyhow::ensure!(key.len() == 64, "Invalid AccountRouting key length: {}", key.len());
    let mut dapp = [0u8; 32];
    dapp.copy_from_slice(&key[..32]);
    let mut account = [0u8; 32];
    account.copy_from_slice(&key[32..]);
    Ok(AccountRouting::new(
        node_types::DAppIdentifier::new(dapp),
        node_types::AccountIdentifier::new(account),
    ))
}

/// Holds Arc<Mutex> clones and their locked guards together.
/// Drop releases guards first (fields drop in declaration order),
/// then releases Arc references.
struct HeldLocks {
    // Each entry: (arc_clone, guard). Guard borrows from the Mutex inside the Arc.
    // We use a raw pointer trick: the Arc keeps the Mutex alive, so the guard is valid.
    _entries: Vec<HeldLock>,
}

struct HeldLock {
    // Guard declared first so it drops before the Arc (Rust drops fields
    // in declaration order). This ensures the Mutex is still alive when
    // the guard releases the lock.
    _guard: parking_lot::MutexGuard<'static, ()>,
    _arc: Arc<Mutex<()>>,
}

impl HeldLock {
    fn new(arc: Arc<Mutex<()>>) -> Self {
        let guard = arc.lock();
        // SAFETY: The Arc keeps the Mutex alive for the lifetime of this
        // struct. _guard drops before _arc (declaration order), so the
        // Mutex is guaranteed alive when the guard releases the lock.
        let guard: parking_lot::MutexGuard<'static, ()> = unsafe { std::mem::transmute(guard) };
        Self { _guard: guard, _arc: arc }
    }
}

fn acquire_sorted_locks(store: &ArchiveStateStore, sorted_ids: &[ThreadIdentifier]) -> HeldLocks {
    // Ensure mutexes exist for all threads (including emerging)
    {
        let mut locks = store.thread_write_locks().write();
        for tid in sorted_ids {
            locks.entry(*tid).or_insert_with(|| Arc::new(Mutex::new(())));
        }
    }

    // Collect Arc clones in sorted order, then lock each
    let locks_map = store.thread_write_locks().read();
    let entries: Vec<HeldLock> = sorted_ids
        .iter()
        .map(|tid| {
            let arc = locks_map[tid].clone();
            HeldLock::new(arc)
        })
        .collect();

    HeldLocks { _entries: entries }
}

/// Collect all thread IDs involved in the update, deduplicated and sorted.
fn collect_all_thread_ids(update: &AccumulatedUpdate) -> Vec<ThreadIdentifier> {
    let mut ids: Vec<ThreadIdentifier> = update
        .thread_transitions
        .keys()
        .chain(update.emerging_threads.keys())
        .chain(update.collapsing_threads.iter())
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    ids.sort();
    ids
}

/// Control state + KVStore generation for CAS.
struct ValidatedThread {
    state: ThreadControlState,
    generation: u64,
}

pub(crate) fn apply_update_impl(
    store: &ArchiveStateStore,
    update: &AccumulatedUpdate,
) -> Result<(), ArchiveStateError> {
    let kv = store.store();
    let meta = store.set_meta();
    let data_epoch = store.data_epoch();

    tracing::trace!(target: "monit", "Applying update: {:?}", update);

    let all_thread_ids = collect_all_thread_ids(update);
    if all_thread_ids.is_empty() {
        return Ok(());
    }

    // Step 1: Acquire per-thread mutexes in sorted order
    let _locks = acquire_sorted_locks(store, &all_thread_ids);

    // Step 2: Validate all threads
    let validated = validate_all_threads(kv, meta, update)?;

    // Shortcut: no data to write and no emerging threads
    if update.operations.is_empty() && update.emerging_threads.is_empty() {
        return finalize_no_data(kv, meta, store, update);
    }

    // Step 3: Write Uninitialized control records for emerging threads
    for thread_id in update.emerging_threads.keys() {
        let uninit = ThreadControlState::new_uninitialized(*thread_id);
        write_thread_control(kv, meta, &uninit, None).map_err(ArchiveStateError::from)?;
    }

    // Step 4: Claim all threads as WritingCopyA (CAS on initial claim)
    set_all_phases(kv, meta, update, &validated, UpdatePhase::WritingCopyA, true)?;

    // Register active update for read path
    let update_arc = Arc::new(update.clone());
    store.active_updates().write().push(update_arc.clone());

    // Steps 5-8 wrapped so we can deregister on error
    let result = (|| -> Result<(), ArchiveStateError> {
        // Step 5: Write Copy A
        let (put_records, delete_keys) =
            prepare_data_records(&update.operations, data_epoch, store);
        write_copy(kv, &store.set_accounts_a(), put_records.clone(), &delete_keys)?;

        // Step 6: Promote all threads to WritingCopyB (no CAS — we own via mutex)
        set_all_phases(kv, meta, update, &validated, UpdatePhase::WritingCopyB, false)?;

        // Step 7: Write Copy B
        write_copy(kv, &store.set_accounts_b(), put_records, &delete_keys)?;

        // Step 8: Finalize all threads
        finalize_all(kv, meta, store, update)?;

        Ok(())
    })();

    // Deregister active update (always — success or error)
    store.active_updates().write().retain(|u| !Arc::ptr_eq(u, &update_arc));

    // Step 9: locks released by _locks drop
    result
}

/// Set all participating threads to a given phase (WritingCopyA or WritingCopyB).
fn set_all_phases(
    kv: &KVStore,
    meta_set: &str,
    update: &AccumulatedUpdate,
    validated: &HashMap<ThreadIdentifier, ValidatedThread>,
    phase: UpdatePhase,
    use_cas: bool,
) -> Result<(), ArchiveStateError> {
    for thread_id in update.thread_transitions.keys().chain(update.collapsing_threads.iter()) {
        // Skip threads that are also emerging — handled in the emerging loop below
        if update.emerging_threads.contains_key(thread_id) {
            continue;
        }
        let vt = &validated[thread_id];
        let state = ThreadControlState {
            thread_id: *thread_id,
            update_phase: phase,
            last_block_id: vt.state.last_block_id,
        };
        let cas_gen = if use_cas { Some(vt.generation) } else { None };
        write_thread_control(kv, meta_set, &state, cas_gen)
            .map_err(|_| ArchiveStateError::ConcurrentModification(*thread_id))?;
    }
    for thread_id in update.emerging_threads.keys() {
        let state =
            ThreadControlState { thread_id: *thread_id, update_phase: phase, last_block_id: None };
        write_thread_control(kv, meta_set, &state, None).map_err(ArchiveStateError::from)?;
    }
    Ok(())
}

/// Validate all threads in the update. Returns validated states with KVStore generations.
fn validate_all_threads(
    kv: &KVStore,
    meta_set: &str,
    update: &AccumulatedUpdate,
) -> Result<HashMap<ThreadIdentifier, ValidatedThread>, ArchiveStateError> {
    let mut validated = HashMap::new();

    for (thread_id, transition) in &update.thread_transitions {
        // If this thread is also emerging in the same update, skip —
        // its control record doesn't exist yet and will be created in Step 3.
        if update.emerging_threads.contains_key(thread_id) {
            continue;
        }

        let (ctrl, generation) = read_thread_control(kv, meta_set, thread_id)
            .map_err(ArchiveStateError::from)?
            .ok_or_else(|| {
                ArchiveStateError::CorruptedControlRecord(
                    *thread_id,
                    "validate_all_threads: missing control record from thread_transitions"
                        .to_string(),
                )
            })?;

        if ctrl.update_phase != UpdatePhase::Idle {
            return Err(ArchiveStateError::NotReady {
                thread_id: *thread_id,
                phase: ctrl.update_phase,
            });
        }

        if let Some(current_block_id) = ctrl.last_block_id {
            if current_block_id != transition.expected_block_id {
                return Err(ArchiveStateError::StaleUpdate {
                    thread_id: *thread_id,
                    expected: transition.expected_block_id,
                    actual: current_block_id,
                });
            }
        }

        validated.insert(*thread_id, ValidatedThread { state: ctrl, generation });
    }

    for thread_id in update.emerging_threads.keys() {
        let existing =
            read_thread_control(kv, meta_set, thread_id).map_err(ArchiveStateError::from)?;
        if existing.is_some() {
            return Err(ArchiveStateError::ThreadAlreadyExists(*thread_id));
        }
    }

    for thread_id in &update.collapsing_threads {
        let (ctrl, generation) = read_thread_control(kv, meta_set, thread_id)
            .map_err(ArchiveStateError::from)?
            .ok_or_else(|| {
                ArchiveStateError::CorruptedControlRecord(
                    *thread_id,
                    "validate_all_threads: missing control record from collapsing_threads"
                        .to_string(),
                )
            })?;

        if ctrl.update_phase != UpdatePhase::Idle {
            return Err(ArchiveStateError::NotReady {
                thread_id: *thread_id,
                phase: ctrl.update_phase,
            });
        }

        validated.insert(*thread_id, ValidatedThread { state: ctrl, generation });
    }

    Ok(validated)
}

/// Finalize all threads after data writes complete.
fn finalize_all(
    kv: &KVStore,
    meta_set: &str,
    store: &ArchiveStateStore,
    update: &AccumulatedUpdate,
) -> Result<(), ArchiveStateError> {
    // Finalize transitioning threads (skip those also emerging — handled below)
    for (thread_id, transition) in &update.thread_transitions {
        if update.emerging_threads.contains_key(thread_id) {
            continue;
        }
        let finalized = ThreadControlState {
            thread_id: *thread_id,
            update_phase: UpdatePhase::Idle,
            last_block_id: Some(transition.new_block_id),
        };
        write_thread_control(kv, meta_set, &finalized, None).map_err(ArchiveStateError::from)?;
    }

    // Finalize emerging threads — if also in thread_transitions, use transition's new_block_id
    for (thread_id, initial_block_id) in &update.emerging_threads {
        let final_block_id = update
            .thread_transitions
            .get(thread_id)
            .map(|t| t.new_block_id)
            .unwrap_or(*initial_block_id);
        let finalized = ThreadControlState {
            thread_id: *thread_id,
            update_phase: UpdatePhase::Idle,
            last_block_id: Some(final_block_id),
        };
        write_thread_control(kv, meta_set, &finalized, None).map_err(ArchiveStateError::from)?;
    }

    // Update System Record FIRST (crash safety for collapse)
    update_registry(kv, meta_set, store, update)?;

    for thread_id in &update.collapsing_threads {
        let collapsed = ThreadControlState::new_collapsed(*thread_id);
        write_thread_control(kv, meta_set, &collapsed, None).map_err(ArchiveStateError::from)?;
    }

    Ok(())
}

/// Shortcut finalization when there are no data operations and no emerging threads.
fn finalize_no_data(
    kv: &KVStore,
    meta_set: &str,
    store: &ArchiveStateStore,
    update: &AccumulatedUpdate,
) -> Result<(), ArchiveStateError> {
    for (thread_id, transition) in &update.thread_transitions {
        let advanced = ThreadControlState {
            thread_id: *thread_id,
            update_phase: UpdatePhase::Idle,
            last_block_id: Some(transition.new_block_id),
        };
        write_thread_control(kv, meta_set, &advanced, None).map_err(ArchiveStateError::from)?;
    }

    update_registry(kv, meta_set, store, update)?;

    for thread_id in &update.collapsing_threads {
        let collapsed = ThreadControlState::new_collapsed(*thread_id);
        write_thread_control(kv, meta_set, &collapsed, None).map_err(ArchiveStateError::from)?;
    }

    Ok(())
}

/// Update System Record and in-memory thread_registry / thread_write_locks.
fn update_registry(
    kv: &KVStore,
    meta_set: &str,
    store: &ArchiveStateStore,
    update: &AccumulatedUpdate,
) -> Result<(), ArchiveStateError> {
    if update.emerging_threads.is_empty() && update.collapsing_threads.is_empty() {
        return Ok(());
    }

    {
        let mut registry = store.thread_registry().write();
        for thread_id in update.emerging_threads.keys() {
            registry.insert(*thread_id);
        }
        for thread_id in &update.collapsing_threads {
            registry.remove(thread_id);
        }
        let system = SystemState { threads: registry.clone(), data_epoch: store.data_epoch() };
        write_system_record(kv, meta_set, &system).map_err(ArchiveStateError::from)?;
    }

    // Update in-memory write locks (remove collapsed; emerging already added)
    {
        let mut locks = store.thread_write_locks().write();
        for thread_id in &update.collapsing_threads {
            locks.remove(thread_id);
        }
    }

    Ok(())
}

/// Prepare KVRecords from the operations map.
fn prepare_data_records(
    operations: &HashMap<AccountRouting, ArchiveOperation>,
    data_epoch: u64,
    store: &ArchiveStateStore,
) -> (Vec<KVRecord>, Vec<Vec<u8>>) {
    let mut put_records = Vec::new();
    let mut delete_keys = Vec::new();
    let account_written_cache = store.account_written_cache();

    for (routing, operation) in operations {
        let key = routing_to_key(routing);
        match operation {
            ArchiveOperation::UpdateOrInsert(account) => {
                let data = bincode::serialize(account).expect("Failed to serialize account");
                if let Ok(vm_account) = account.vm_account() {
                    let hash = vm_account.hash();
                    if let Some(level) =
                        account_written_cache.insert_by_serialized_len(hash, account, data.len())
                    {
                        tracing::trace!(
                            target: "mem",
                            routing = %routing,
                            hash = %hash.to_hex_string(),
                            level = ?level,
                            serialized_size = data.len(),
                            "account written cache insert",
                        );
                    }
                }
                tracing::trace!(
                    target: "monit",
                    "prepare_data_records: routing={routing}, serialized_size={}, key_len={}",
                    data.len(),
                    key.len(),
                );
                put_records.push(KVRecord {
                    key,
                    generation: 0,
                    data_epoch: Some(data_epoch as u32),
                    data,
                });
            }
            ArchiveOperation::Remove => {
                delete_keys.push(key);
            }
        }
    }

    (put_records, delete_keys)
}

/// Write records to one copy (A or B).
fn write_copy(
    kv: &KVStore,
    set: &str,
    put_records: Vec<KVRecord>,
    delete_keys: &[Vec<u8>],
) -> Result<(), ArchiveStateError> {
    if !put_records.is_empty() {
        kv.put(set, put_records, false).map_err(ArchiveStateError::from)?;
    }
    if !delete_keys.is_empty() {
        kv.delete(set, delete_keys).map_err(ArchiveStateError::from)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use node_types::BlockIdentifier;
    use node_types::ThreadIdentifier;

    use super::*;
    use crate::thread_accounts::durable::archive::config::ArchiveStoreConfig;
    use crate::thread_accounts::durable::kv_store::in_memory::InMemoryKVStore;
    use crate::ThreadAccount;

    fn test_kv() -> KVStore {
        KVStore::InMemory(InMemoryKVStore::new())
    }

    fn test_config() -> ArchiveStoreConfig {
        ArchiveStoreConfig { aerospike_address: None, node_id: String::new(), write_parallelism: 0 }
    }

    fn setup_single_thread() -> (ArchiveStateStore, ThreadIdentifier, BlockIdentifier) {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();
        let block_id = BlockIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        let ctrl = ThreadControlState::new_idle(tid, block_id);
        write_thread_control(&kv, META_SET, &ctrl, None).unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();
        (store, tid, block_id)
    }

    fn make_routing(seed: u8) -> AccountRouting {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        node_types::AccountIdentifier::new(bytes).redirect()
    }

    fn make_block(seed: u8) -> BlockIdentifier {
        BlockIdentifier::new([seed; 32])
    }

    // ---- Single-thread tests (Phase 4) ----

    #[test]
    fn test_apply_basic() {
        let (store, tid, block_0) = setup_single_thread();
        let block_1 = make_block(1);
        let routing = make_routing(1);

        let mut update = AccumulatedUpdate::new();
        update.transition_thread(tid, block_0, block_1);
        update.insert(routing, ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));

        store.apply_update(&update).unwrap();

        let ctrl = store.get_thread_control_state(&tid).unwrap().unwrap();
        assert_eq!(ctrl.update_phase, UpdatePhase::Idle);
        assert_eq!(ctrl.last_block_id, Some(block_1));
    }

    #[test]
    fn test_apply_empty_operations() {
        let (store, tid, block_0) = setup_single_thread();
        let block_1 = make_block(1);

        let mut update = AccumulatedUpdate::new();
        update.transition_thread(tid, block_0, block_1);

        store.apply_update(&update).unwrap();

        let ctrl = store.get_thread_control_state(&tid).unwrap().unwrap();
        assert_eq!(ctrl.last_block_id, Some(block_1));
    }

    #[test]
    fn test_apply_stale_block() {
        let (store, tid, _) = setup_single_thread();
        let wrong = make_block(99);
        let block_1 = make_block(1);

        let mut update = AccumulatedUpdate::new();
        update.transition_thread(tid, wrong, block_1);

        assert!(matches!(store.apply_update(&update), Err(ArchiveStateError::StaleUpdate { .. })));
    }

    #[test]
    fn test_apply_sequential() {
        let (store, tid, block_0) = setup_single_thread();
        let block_1 = make_block(1);
        let block_2 = make_block(2);

        let mut u1 = AccumulatedUpdate::new();
        u1.transition_thread(tid, block_0, block_1);
        store.apply_update(&u1).unwrap();

        let mut u2 = AccumulatedUpdate::new();
        u2.transition_thread(tid, block_1, block_2);
        store.apply_update(&u2).unwrap();

        let ctrl = store.get_thread_control_state(&tid).unwrap().unwrap();
        assert_eq!(ctrl.last_block_id, Some(block_2));
    }

    #[test]
    fn test_apply_data_in_both_copies() {
        let (store, tid, block_0) = setup_single_thread();
        let block_1 = make_block(1);
        let routing = make_routing(1);

        let mut update = AccumulatedUpdate::new();
        update.transition_thread(tid, block_0, block_1);
        update.insert(routing, ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));
        store.apply_update(&update).unwrap();

        let kv = store.store();
        let key = routing_to_key(&routing);
        let a = kv.get(ACCOUNTS_SET_A, std::slice::from_ref(&key)).unwrap();
        let b = kv.get(ACCOUNTS_SET_B, &[key]).unwrap();
        assert!(a[0].is_some());
        assert!(b[0].is_some());
    }

    #[test]
    fn test_apply_delete_from_both_copies() {
        let (store, tid, block_0) = setup_single_thread();
        let block_1 = make_block(1);
        let block_2 = make_block(2);
        let routing = make_routing(1);

        let mut u1 = AccumulatedUpdate::new();
        u1.transition_thread(tid, block_0, block_1);
        u1.insert(routing, ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));
        store.apply_update(&u1).unwrap();

        let mut u2 = AccumulatedUpdate::new();
        u2.transition_thread(tid, block_1, block_2);
        u2.insert(routing, ArchiveOperation::Remove);
        store.apply_update(&u2).unwrap();

        let kv = store.store();
        let key = routing_to_key(&routing);
        let a = kv.get(ACCOUNTS_SET_A, std::slice::from_ref(&key)).unwrap();
        let b = kv.get(ACCOUNTS_SET_B, &[key]).unwrap();
        assert!(a[0].is_none());
        assert!(b[0].is_none());
    }

    #[test]
    fn test_apply_not_idle_rejected() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_uninitialized(tid), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();

        let mut update = AccumulatedUpdate::new();
        update.transition_thread(tid, BlockIdentifier::default(), BlockIdentifier::default());

        assert!(matches!(store.apply_update(&update), Err(ArchiveStateError::NotReady { .. })));
    }

    #[test]
    fn test_apply_data_epoch_stamped() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();
        let block_0 = BlockIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 42 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid, block_0), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();

        let block_1 = make_block(1);
        let routing = make_routing(1);
        let mut update = AccumulatedUpdate::new();
        update.transition_thread(tid, block_0, block_1);
        update.insert(routing, ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));
        store.apply_update(&update).unwrap();

        let key = routing_to_key(&routing);
        // This test runs at data_epoch=42 (not 0), so use the live
        // accessor instead of the epoch-0 ACCOUNTS_SET_A constant.
        let results = store.store().get(&store.set_accounts_a(), &[key]).unwrap();
        assert_eq!(results[0].as_ref().unwrap().data_epoch, Some(42));
    }

    // ---- Multi-thread tests (Phase 6) ----

    #[test]
    fn test_apply_multi_thread_transitions() {
        let kv = test_kv();
        let tid1 = ThreadIdentifier::default();
        let tid2 = ThreadIdentifier::from([1u8; 34]);
        let block_0 = BlockIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid1, tid2]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid1, block_0), None)
            .unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid2, block_0), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();
        let block_1 = make_block(1);

        let mut update = AccumulatedUpdate::new();
        update.transition_thread(tid1, block_0, block_1);
        update.transition_thread(tid2, block_0, block_1);
        update.insert(make_routing(1), ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));

        store.apply_update(&update).unwrap();

        let c1 = store.get_thread_control_state(&tid1).unwrap().unwrap();
        let c2 = store.get_thread_control_state(&tid2).unwrap().unwrap();
        assert_eq!(c1.last_block_id, Some(block_1));
        assert_eq!(c2.last_block_id, Some(block_1));
    }

    #[test]
    fn test_apply_emerge_thread() {
        let store = ArchiveStateStore::in_memory();
        let tid = ThreadIdentifier::default();
        let block_0 = make_block(0);

        let mut update = AccumulatedUpdate::new();
        update.emerge_thread(tid, block_0);
        update.insert(make_routing(1), ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));

        store.apply_update(&update).unwrap();

        let ctrl = store.get_thread_control_state(&tid).unwrap().unwrap();
        assert_eq!(ctrl.update_phase, UpdatePhase::Idle);
        assert_eq!(ctrl.last_block_id, Some(block_0));

        // Data in both copies
        let key = routing_to_key(&make_routing(1));
        let a = store.store().get(ACCOUNTS_SET_A, std::slice::from_ref(&key)).unwrap();
        let b = store.store().get(ACCOUNTS_SET_B, &[key]).unwrap();
        assert!(a[0].is_some());
        assert!(b[0].is_some());

        // Thread in registry
        assert!(store.thread_registry().read().contains(&tid));
    }

    #[test]
    fn test_apply_collapse_thread() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();
        let block_0 = BlockIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid, block_0), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();

        let mut update = AccumulatedUpdate::new();
        update.collapse_thread(tid);

        store.apply_update(&update).unwrap();

        let ctrl = store.get_thread_control_state(&tid).unwrap().unwrap();
        assert_eq!(ctrl.update_phase, UpdatePhase::Collapsed);

        // Removed from registry
        assert!(!store.thread_registry().read().contains(&tid));
    }

    #[test]
    fn test_apply_emerge_already_exists() {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();
        let block_0 = BlockIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid, block_0), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();

        let mut update = AccumulatedUpdate::new();
        update.emerge_thread(tid, make_block(1));

        assert!(matches!(
            store.apply_update(&update),
            Err(ArchiveStateError::ThreadAlreadyExists(_))
        ));
    }

    #[test]
    fn test_apply_emerge_and_transition_together() {
        let kv = test_kv();
        let tid_existing = ThreadIdentifier::default();
        let tid_new = ThreadIdentifier::from([1u8; 34]);
        let block_0 = BlockIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid_existing]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(
            &kv,
            META_SET,
            &ThreadControlState::new_idle(tid_existing, block_0),
            None,
        )
        .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();
        let block_1 = make_block(1);

        let mut update = AccumulatedUpdate::new();
        update.transition_thread(tid_existing, block_0, block_1);
        update.emerge_thread(tid_new, block_1);
        update.insert(make_routing(1), ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));
        update.insert(make_routing(2), ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));

        store.apply_update(&update).unwrap();

        let c1 = store.get_thread_control_state(&tid_existing).unwrap().unwrap();
        let c2 = store.get_thread_control_state(&tid_new).unwrap().unwrap();
        assert_eq!(c1.update_phase, UpdatePhase::Idle);
        assert_eq!(c2.update_phase, UpdatePhase::Idle);
        assert!(store.thread_registry().read().contains(&tid_new));
    }

    #[test]
    fn test_apply_transition_and_collapse_together() {
        let kv = test_kv();
        let tid1 = ThreadIdentifier::default();
        let tid2 = ThreadIdentifier::from([1u8; 34]);
        let block_0 = BlockIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid1, tid2]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid1, block_0), None)
            .unwrap();
        write_thread_control(&kv, META_SET, &ThreadControlState::new_idle(tid2, block_0), None)
            .unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();
        let block_1 = make_block(1);

        let mut update = AccumulatedUpdate::new();
        update.transition_thread(tid1, block_0, block_1);
        update.collapse_thread(tid2);

        store.apply_update(&update).unwrap();

        let c1 = store.get_thread_control_state(&tid1).unwrap().unwrap();
        let c2 = store.get_thread_control_state(&tid2).unwrap().unwrap();
        assert_eq!(c1.update_phase, UpdatePhase::Idle);
        assert_eq!(c1.last_block_id, Some(block_1));
        assert_eq!(c2.update_phase, UpdatePhase::Collapsed);
        assert!(store.thread_registry().read().contains(&tid1));
        assert!(!store.thread_registry().read().contains(&tid2));
    }

    #[test]
    fn test_apply_emerge_with_transition() {
        // Simulates a batched update where a thread emerges AND transitions
        // in the same AccumulatedUpdate (emerge from block N, transition from block N+1).
        let store = ArchiveStateStore::in_memory();
        let tid = ThreadIdentifier::default();
        let block_0 = make_block(0);
        let block_1 = make_block(1);

        let mut update = AccumulatedUpdate::new();
        update.emerge_thread(tid, block_0);
        update.transition_thread(tid, block_0, block_1);
        update.insert(make_routing(1), ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));

        store.apply_update(&update).unwrap();

        let ctrl = store.get_thread_control_state(&tid).unwrap().unwrap();
        assert_eq!(ctrl.update_phase, UpdatePhase::Idle);
        // Should use transition's new_block_id, not emerge's initial_block_id
        assert_eq!(ctrl.last_block_id, Some(block_1));

        // Data in both copies
        let key = routing_to_key(&make_routing(1));
        let a = store.store().get(&store.set_accounts_a(), std::slice::from_ref(&key)).unwrap();
        let b = store.store().get(&store.set_accounts_b(), &[key]).unwrap();
        assert!(a[0].is_some());
        assert!(b[0].is_some());

        // Thread in registry
        assert!(store.thread_registry().read().contains(&tid));
    }
}
