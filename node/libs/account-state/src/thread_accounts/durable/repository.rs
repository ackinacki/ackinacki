use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use critical_thread::SpawnCritical;
use lru::LruCache;
use multi_map::node::Node as MultiMapNode;
use multi_map::MultiMap;
use node_types::AccountHash;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifier;
use node_types::DAppIdentifierPath;
use node_types::ThreadAccountsHash;
use node_types::ThreadIdentifier;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use tvm_block::Serializable;
use tvm_block::ShardAccount;

use crate::thread_accounts::accumulator;
use crate::thread_accounts::accumulator::ArchiveUpdateAccumulator;
use crate::thread_accounts::accumulator::MerkleMapStore;
use crate::thread_accounts::archive::update::ThreadSnapshot;
use crate::thread_accounts::durable::archive::apply::key_to_routing;
use crate::thread_accounts::durable::archive::control::ThreadControlState;
use crate::thread_accounts::durable::archive::store::ArchiveStateStore;
use crate::thread_accounts::durable::AccountInfo;
use crate::thread_accounts::durable::DurableStateSnapshot;
use crate::thread_accounts::ArchiveOperation;
use crate::StateAccountsMetrics;
use crate::ThreadAccount;
use crate::ThreadAccountMapRepository;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AccountHashMismatchError {
    pub routing: AccountRouting,
    pub expected_hash: AccountHash,
}

impl std::fmt::Display for AccountHashMismatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Account {} exists in map (hash {}) but no matching version found in pool, accumulator, or archive",
            self.routing,
            self.expected_hash.to_hex_string(),
        )
    }
}

impl std::error::Error for AccountHashMismatchError {}

/// Independent leaf-counter for the two-level durable map. Walks the L1
/// trie node-by-node (not via `MapIter`) and, for each L1 leaf, walks the
/// L2 trie node-by-node. Returns the total number of L2 leaves across all
/// L1 leaves — i.e. the total number of `(routing, AccountInfo)` pairs
/// the snapshot serializer would emit.
///
/// This exists so that `export_durable_snapshot_to_writer` has a check
/// that's independent of the iteration machinery `map_iter` uses. If
/// `map_iter` ever undercounts due to a missing-leaf bug, the export
/// `expected_len` (built from `map_iter`) and this counter will diverge,
/// and the export will bail rather than silently emit a snapshot whose
/// map references routings whose bodies were never written.
fn count_serialized_map_leaves(map: &super::ThreadAccountMap) -> usize {
    fn count_l1_leaves<V: multi_map::MultiMapValue>(
        node: &MultiMapNode<V>,
        on_leaf: &mut impl FnMut(&V),
    ) {
        match node {
            MultiMapNode::Empty => {}
            MultiMapNode::Leaf(data) => on_leaf(&data.value),
            MultiMapNode::Branch(data) => {
                for child in data.children.iter() {
                    count_l1_leaves(child, on_leaf);
                }
            }
            MultiMapNode::Ext(data) => count_l1_leaves(&data.child, on_leaf),
        }
    }

    fn count_node_leaves<V: multi_map::MultiMapValue>(node: &MultiMapNode<V>) -> usize {
        match node {
            MultiMapNode::Empty => 0,
            MultiMapNode::Leaf(_) => 1,
            MultiMapNode::Branch(data) => data.children.iter().map(count_node_leaves).sum(),
            MultiMapNode::Ext(data) => count_node_leaves(&data.child),
        }
    }

    let mut total = 0usize;
    count_l1_leaves(&map.root, &mut |l1_value: &super::maps::L1MapValue| {
        total += count_node_leaves(&l1_value.0.root);
    });
    total
}

fn write_snapshot_account_entry<W: Write>(
    writer: &mut W,
    routing: &AccountRouting,
    account_bytes: &[u8],
) -> anyhow::Result<()> {
    writer.write_all(routing.dapp_id().as_slice())?;
    writer.write_all(routing.account_id().as_slice())?;
    writer.write_all(&(account_bytes.len() as u64).to_le_bytes())?;
    writer.write_all(account_bytes)?;
    Ok(())
}

fn read_snapshot_account_entry<R: Read>(
    reader: &mut R,
) -> anyhow::Result<(AccountRouting, Vec<u8>)> {
    let mut dapp = [0u8; 32];
    reader.read_exact(&mut dapp)?;

    let mut account = [0u8; 32];
    reader.read_exact(&mut account)?;

    let mut len = [0u8; 8];
    reader.read_exact(&mut len)?;
    let account_bytes_len = u64::from_le_bytes(len);
    let account_bytes_len = usize::try_from(account_bytes_len)
        .map_err(|_| anyhow::anyhow!("Snapshot account entry length does not fit usize"))?;

    let mut account_bytes = vec![0u8; account_bytes_len];
    reader.read_exact(&mut account_bytes)?;

    Ok((
        AccountRouting::new(DAppIdentifier::new(dapp), AccountIdentifier::new(account)),
        account_bytes,
    ))
}

fn validate_snapshot_account_bytes(
    archive_data: &[u8],
    expected: &AccountInfo,
) -> anyhow::Result<Option<Vec<u8>>> {
    let account_bytes: Vec<u8> = bincode::deserialize(archive_data)?;
    let account = ThreadAccount::read_bytes(&account_bytes)?;
    let matches = match expected {
        AccountInfo::Redirect(dapp_id) => {
            account.is_redirect() && account.get_dapp_id() == Some(*dapp_id)
        }
        AccountInfo::VmAccountHash(expected_hash) => account
            .vm_account()
            .map(|vm_account| vm_account.hash() == *expected_hash)
            .unwrap_or(false),
    };
    Ok(matches.then_some(account_bytes))
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum BlockAccountOperation {
    UpdateOrInsert(ThreadAccount),
    Remove,
    #[default]
    MoveFromTvm,
    /// Merkle update delta for an existing account.
    /// Contains the BOC-serialized tvm_block::MerkleUpdate bytes for ShardAccount cell.
    /// Applicable only to TVM accounts that already exist in the durable state.
    AccountMerkleUpdate(Vec<u8>),
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableThreadAccountsStateDiff {
    pub accounts: HashMap<AccountRouting, BlockAccountOperation>,
}

#[derive(Serialize, Deserialize)]
pub struct DurableThreadAccountsStateDiffSerDe {
    pub accounts: BTreeMap<AccountRouting, BlockAccountOperation>,
}

impl From<DurableThreadAccountsStateDiffSerDe> for DurableThreadAccountsStateDiff {
    fn from(value: DurableThreadAccountsStateDiffSerDe) -> Self {
        Self { accounts: value.accounts.into_iter().collect() }
    }
}

impl From<DurableThreadAccountsStateDiff> for DurableThreadAccountsStateDiffSerDe {
    fn from(value: DurableThreadAccountsStateDiff) -> Self {
        Self { accounts: value.accounts.into_iter().collect() }
    }
}

impl DurableThreadAccountsStateDiff {
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }
}

/// Thread-level durable state: a two-level map plus pool handle and pending diff.
///
/// Level 1: DAppIdentifier → DAppMapValue (embeds an account map directly)
/// Level 2 (embedded): AccountIdentifier → AccountInfo
///
/// States are persisted as self-contained files in `state_maps/`.
#[derive(Clone, Debug)]
pub struct DurableThreadAccountsState(Arc<DurableThreadAccountsStateInner>);

static LIVE_INNER_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

pub struct DurableThreadAccountsStateInner {
    // Merkle tree root related to this state
    map: ThreadAccountMap,
}

impl Drop for DurableThreadAccountsStateInner {
    fn drop(&mut self) {
        let count = LIVE_INNER_COUNT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
        tracing::trace!(target: "monit", "DurableState inner dropped, live={count}");
    }
}

impl std::fmt::Debug for DurableThreadAccountsStateInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableThreadAccountsState").field("map", &self.map).finish()
    }
}

impl DurableThreadAccountsState {
    pub fn new(map: MultiMap<L1MapValue>) -> Self {
        let count = LIVE_INNER_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        tracing::trace!(target: "monit", "DurableState inner created, live={count}");
        Self(Arc::new(DurableThreadAccountsStateInner { map }))
    }

    /// Number of live inner state instances (for diagnostics).
    pub fn live_inner_count() -> usize {
        LIVE_INNER_COUNT.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// State backed directly by the archive — no in-memory parents.
    pub fn with_map(map: MultiMap<L1MapValue>) -> Self {
        Self::new(map)
    }

    pub fn map(&self) -> &ThreadAccountMap {
        &self.0.map
    }
}

use crate::thread_accounts::durable::maps::L1MapValue;
use crate::thread_accounts::durable::maps::ThreadAccountMap;
use crate::thread_accounts::kv_store::aerospike::AerospikeAccountsCacheStat;

const DEFAULT_ACCUMULATED_UPDATE_BLOCK_LIMIT: usize = 2000;
// TODO: move cache capacities into account-state/archive configuration if tuning is needed.
const ACCOUNT_READ_CACHE_L1_CAPACITY: usize = 1_500;
const ACCOUNT_READ_CACHE_L2_CAPACITY: usize = 1_500;
const ACCOUNT_READ_CACHE_L1_THRESHOLD: Duration = Duration::from_millis(15);
const ACCOUNT_READ_CACHE_L2_THRESHOLD: Duration = Duration::from_millis(3);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CacheLevel {
    L1,
    L2,
}

struct AccountReadCache {
    l1: Mutex<LruCache<AccountHash, Arc<ThreadAccount>>>,
    l2: Mutex<LruCache<AccountHash, Arc<ThreadAccount>>>,
}

impl AccountReadCache {
    fn new() -> Self {
        Self {
            l1: Mutex::new(LruCache::new(
                NonZeroUsize::new(ACCOUNT_READ_CACHE_L1_CAPACITY)
                    .expect("L1 account read cache capacity must be non-zero"),
            )),
            l2: Mutex::new(LruCache::new(
                NonZeroUsize::new(ACCOUNT_READ_CACHE_L2_CAPACITY)
                    .expect("L2 account read cache capacity must be non-zero"),
            )),
        }
    }

    fn get(&self, hash: &AccountHash) -> Option<(CacheLevel, Arc<ThreadAccount>)> {
        if let Some(account) = self.l1.lock().get(hash).cloned() {
            return Some((CacheLevel::L1, account));
        }
        self.l2.lock().get(hash).cloned().map(|account| (CacheLevel::L2, account))
    }

    fn insert_by_elapsed(
        &self,
        hash: AccountHash,
        account: &ThreadAccount,
        elapsed: Duration,
    ) -> Option<CacheLevel> {
        if elapsed > ACCOUNT_READ_CACHE_L1_THRESHOLD {
            self.insert_l1(hash, Arc::new(account.clone()));
            return Some(CacheLevel::L1);
        }
        if elapsed > ACCOUNT_READ_CACHE_L2_THRESHOLD {
            self.insert_l2(hash, Arc::new(account.clone()));
            return Some(CacheLevel::L2);
        }
        None
    }

    fn insert_l1(&self, hash: AccountHash, account: Arc<ThreadAccount>) {
        self.l1.lock().put(hash, account);
        self.l2.lock().pop(&hash);
    }

    fn insert_l2(&self, hash: AccountHash, account: Arc<ThreadAccount>) {
        self.l2.lock().put(hash, account);
    }
}

#[derive(Clone)]
pub struct DurableThreadAccountsRepository {
    durable_path: PathBuf,
    map_repo: ThreadAccountMapRepository,
    archive: ArchiveStateStore,
    accumulator: Arc<ArchiveUpdateAccumulator>,
    account_read_cache: Arc<AccountReadCache>,
    update_thread: Arc<parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>>,
    metrics: Option<StateAccountsMetrics>,
}

impl DurableThreadAccountsRepository {
    const SNAPSHOT_COPY_BUFFER_CAPACITY: usize = 1024 * 1024;

    pub fn new(
        durable_path: PathBuf,
        archive: ArchiveStateStore,
        metrics: Option<StateAccountsMetrics>,
    ) -> anyhow::Result<Self> {
        // Ensure the durable directory exists (WAL and checkpoint files live here)
        fs::create_dir_all(&durable_path)?;
        let map_repo = ThreadAccountMapRepository::new();
        let accumulator = Arc::new(ArchiveUpdateAccumulator::new(
            DEFAULT_ACCUMULATED_UPDATE_BLOCK_LIMIT,
            &archive.get_all_thread_control_states()?,
            metrics.clone(),
        ));
        Ok(Self {
            durable_path,
            map_repo,
            archive,
            accumulator,
            account_read_cache: Arc::new(AccountReadCache::new()),
            update_thread: Arc::new(parking_lot::Mutex::new(None)),
            metrics,
        })
    }

    pub fn start_archive_update_service(&self) -> anyhow::Result<()> {
        let mut update_thread_guard = self.update_thread.lock();
        if update_thread_guard.is_some() {
            return Ok(());
        }
        let thread_archive = self.archive.clone();
        let thread_accumulator = self.accumulator.clone();
        let maps_store =
            MerkleMapStore::new(&self.durable_path, self.map_repo.clone(), self.metrics.clone());
        let update_thread = std::thread::Builder::new()
            .name("accounts-commit-loop".to_string())
            .spawn_critical(move || {
                let result =
                    accumulator::update_loop(thread_accumulator, thread_archive, maps_store);
                if let Err(e) = &result {
                    tracing::error!("Archive update loop failed: {e}");
                }
                result
            })
            .map_err(|e| anyhow::anyhow!("Failed to spawn commit loop: {e}"))?;
        *update_thread_guard = Some(update_thread);
        Ok(())
    }

    pub fn map_repo(&self) -> &ThreadAccountMapRepository {
        &self.map_repo
    }

    pub fn aerospike_cache_stat(&self) -> Option<AerospikeAccountsCacheStat> {
        None
    }

    pub fn state_accounts_metrics(&self) -> Option<StateAccountsMetrics> {
        self.metrics.clone()
    }

    /// Return a human-readable summary of the archive state store.
    pub fn archive_summary(&self) -> String {
        self.archive.summary()
    }

    /// Ensure a thread exists in the archive store (idempotent).
    pub fn ensure_thread(
        &self,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
        overwrite: bool,
    ) -> anyhow::Result<()> {
        self.archive.ensure_thread(thread_id, block_id).map_err(|e| anyhow::anyhow!("{e}"))?;
        self.accumulator.set_expected_block(*thread_id, *block_id, overwrite);
        Ok(())
    }

    // ---- State persistence ----

    fn accounts_map_file_path(&self, block_id: &BlockIdentifier) -> PathBuf {
        self.durable_path
            .join(format!("_optimistic_state/{}.accounts_map", block_id.to_hex_string()))
    }

    /// Save a state to disk as a self-contained file.
    /// The file contains the full two-level tree serialized with write_map.
    pub fn state_save(
        &self,
        state: &DurableThreadAccountsState,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<()> {
        // accounts.commit() is handled by the commit loop
        let path = self.accounts_map_file_path(block_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Atomic write via a temp file + rename
        let tmp = path.with_extension("accounts_map.tmp");
        {
            let f = File::create(&tmp)?;
            let mut w = BufWriter::new(f);
            self.map_repo.map_write(&state.0.map, &mut w)?;
            w.flush()?;
            w.into_inner()?.sync_all()?;
        }
        fs::rename(&tmp, &path)?;

        Ok(())
    }

    /// Load a state from the optimistic-state map file written by `state_save`.
    pub fn state_load(
        &self,
        block_id: &BlockIdentifier,
        _thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Option<DurableThreadAccountsState>> {
        let path = self.accounts_map_file_path(block_id);
        Ok(if path.exists() {
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);
            let map = self.map_repo.map_read(&mut reader)?;
            tracing::trace!(target: "monit", "Loaded durable state from optimistic map file");
            Some(DurableThreadAccountsState::with_map(map))
        } else {
            None
        })
    }

    // ---- State accessors (legacy compat wrappers) ----

    pub fn new_state() -> DurableThreadAccountsState {
        DurableThreadAccountsState::new(ThreadAccountMapRepository::new_map())
    }

    pub fn state_hash(&self, state: &DurableThreadAccountsState) -> ThreadAccountsHash {
        self.map_repo.map_hash(&state.0.map)
    }

    // ---- Map operations ----

    pub fn state_account(
        &self,
        state: &DurableThreadAccountsState,
        routing: &AccountRouting,
    ) -> anyhow::Result<Option<ThreadAccount>> {
        Ok(match self.map_repo.map_get(&state.0.map, routing) {
            Some(AccountInfo::Redirect(dapp_id)) => Some(ThreadAccount::redirect(dapp_id)),
            Some(AccountInfo::VmAccountHash(_)) => self.find_account(state, routing)?,
            None => None,
        })
    }

    #[cfg(test)]
    pub(crate) fn archive_account_for_test(
        &self,
        routing: &AccountRouting,
    ) -> anyhow::Result<Option<ThreadAccount>> {
        self.archive.read_account_operation(routing).map_err(|err| anyhow::anyhow!("{err}"))
    }

    // Find account using fallback chain:
    // - operational pool
    // - accumulator
    // - archive active updates
    // - archive store
    fn find_account(
        &self,
        state: &DurableThreadAccountsState,
        routing: &AccountRouting,
    ) -> anyhow::Result<Option<ThreadAccount>> {
        match self.map_repo.map_get(&state.0.map, routing) {
            Some(info) => self.find_account_with_info(state, routing, info),
            None => Ok(None),
        }
    }

    fn find_account_with_info(
        &self,
        state: &DurableThreadAccountsState,
        routing: &AccountRouting,
        info: AccountInfo,
    ) -> anyhow::Result<Option<ThreadAccount>> {
        match info {
            AccountInfo::Redirect(dapp_id) => {
                self.find_account(state, &routing.with_dapp_id(dapp_id))
            }
            AccountInfo::VmAccountHash(vm_hash) => {
                // 1. Unfinalized pool lookup by hash — always correct
                if let Some(account) = self.accumulator.unfinalized_pool_get(&vm_hash) {
                    return Ok(Some(account));
                }
                // 2. Accumulator lookup by routing — verify hash
                if let Some(operation) = self.accumulator.find_account_operation(routing, &vm_hash)
                {
                    match operation {
                        ArchiveOperation::UpdateOrInsert(account) => {
                            return Ok(Some(account));
                        }
                        ArchiveOperation::Remove => return Ok(None),
                    }
                }
                // 3. Content-addressed read cache by expected hash.
                if let Some((level, account)) = self.account_read_cache.get(&vm_hash) {
                    tracing::trace!(
                        target: "mem",
                        routing = %routing,
                        hash = %vm_hash.to_hex_string(),
                        level = ?level,
                        "account read cache hit",
                    );
                    return Ok(Some(Arc::unwrap_or_clone(account)));
                }
                tracing::trace!(
                    target: "mem",
                    routing = %routing,
                    hash = %vm_hash.to_hex_string(),
                    "account read cache miss",
                );
                // 3. Archive lookup by routing — verify hash
                let started = Instant::now();
                if let Some(account) = self.archive.read_account_operation(routing)? {
                    let elapsed = started.elapsed();
                    let account_hash = account.vm_account()?.hash();
                    if account_hash == vm_hash {
                        if let Some(level) =
                            self.account_read_cache.insert_by_elapsed(vm_hash, &account, elapsed)
                        {
                            tracing::trace!(
                                target: "mem",
                                routing = %routing,
                                hash = %vm_hash.to_hex_string(),
                                level = ?level,
                                elapsed_ms = elapsed.as_millis(),
                                "account read cache insert",
                            );
                        }
                        return Ok(Some(account));
                    }
                }
                // 4. Routing exists in the map but no version matches the hash
                Err(AccountHashMismatchError { routing: *routing, expected_hash: vm_hash }.into())
            }
        }
    }

    pub fn state_split(
        &self,
        state: &DurableThreadAccountsState,
        dapp_id_path: DAppIdentifierPath,
    ) -> anyhow::Result<(DurableThreadAccountsState, DurableThreadAccountsState)> {
        let (map_a, map_b) = self.map_repo.map_split(&state.0.map, dapp_id_path);
        Ok((
            DurableThreadAccountsState::with_map(map_a),
            DurableThreadAccountsState::with_map(map_b),
        ))
    }

    pub fn merge(
        &self,
        a: &DurableThreadAccountsState,
        b: &DurableThreadAccountsState,
    ) -> anyhow::Result<DurableThreadAccountsState> {
        let merged_map = self.map_repo.merge(&a.0.map, &b.0.map);
        Ok(DurableThreadAccountsState::with_map(merged_map))
    }

    pub fn state_update(
        &self,
        thread_id: &ThreadIdentifier,
        block_height: u64,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &DurableThreadAccountsState,
        accounts: &HashMap<AccountRouting, BlockAccountOperation>,
    ) -> anyhow::Result<(DurableThreadAccountsState, HashMap<AccountRouting, ArchiveOperation>)>
    {
        tracing::trace!(target: "monit", "Update accounts in durable state: prepare update: {}", accounts.len());
        let (account_updates, map_updates) =
            self.prepare_updates(old_tvm_accounts, state, accounts)?;

        self.accumulator.unfinalized_pool_insert(*thread_id, block_height, &account_updates);
        tracing::trace!(target: "monit", "Update accounts in durable maps: {}", account_updates.len());
        // Apply only this batch's map updates to the trie (state already has old entries applied)
        let new_map = self.map_repo.map_update(&state.0.map, &map_updates);
        tracing::info!(
            target: "mem",
            "state_update: accounts_changed={} map_updates={}",
            accounts.len(),
            map_updates.len(),
        );
        let new_state = DurableThreadAccountsState::with_map(new_map);
        tracing::trace!(target: "monit", "Update accounts in durable finished");
        Ok((new_state, account_updates))
    }

    pub fn finalize_thread_transition(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        block_height: u64,
        state: &DurableThreadAccountsState,
        account_operations: HashMap<AccountRouting, ArchiveOperation>,
    ) -> anyhow::Result<()> {
        let t = Instant::now();
        let operations_len = account_operations.len();
        let ops_data_bytes: usize = account_operations
            .values()
            .map(|op| match op {
                ArchiveOperation::UpdateOrInsert(acc) => acc
                    .vm_account()
                    .map(|v| v.write_bytes().map(|b| b.len()).unwrap_or(0))
                    .unwrap_or(0),
                ArchiveOperation::Remove => 0,
            })
            .sum();
        tracing::info!(
            target: "mem",
            "finalize_thread_transition T:{thread_id} B:{block_id} ops={operations_len} ops_data_bytes={ops_data_bytes}",
        );
        self.accumulator.push_transition(
            *thread_id,
            *block_id,
            block_height,
            account_operations,
            state.map(),
        );
        tracing::trace!(target: "monit", "Finalizing state for {}: queuing {} accounts for commit",
            t.elapsed().as_micros(),
            operations_len,
        );
        Ok(())
    }

    /// Declare that a new blockchain thread is emerging.
    pub fn thread_emerge(
        &self,
        thread_id: &ThreadIdentifier,
        initial_block_id: &BlockIdentifier,
        state: &DurableThreadAccountsState,
        account_operations: HashMap<AccountRouting, ArchiveOperation>,
    ) -> anyhow::Result<()> {
        self.accumulator.push_emerge(
            *thread_id,
            *initial_block_id,
            account_operations,
            state.map(),
        );
        Ok(())
    }

    /// Declare that a blockchain thread is collapsing.
    pub fn thread_collapse(&self, thread_id: &ThreadIdentifier) -> anyhow::Result<()> {
        self.accumulator.push_collapse(*thread_id);
        Ok(())
    }

    /// Populate an Uninitialized thread from a complete snapshot.
    pub fn thread_init(
        &self,
        thread_id: &ThreadIdentifier,
        initial_block_id: &BlockIdentifier,
        snapshot: &ThreadSnapshot,
    ) -> anyhow::Result<()> {
        self.archive
            .thread_init(thread_id, *initial_block_id, snapshot)
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Deprecated: producers no longer need the producer-side pause that
    /// `FlushGuard` provided; the snapshot pin (`request_snapshot_pin` /
    /// `acquire_snapshot_pin`) gates apply, not send. Callers that need
    /// to wait for in-flight batches to drain should use
    /// `wait_for_drain` instead. Removal scheduled for a follow-up PR.
    #[deprecated(note = "use `acquire_snapshot_pin` for snapshot consistency \
        or `wait_for_drain` for visibility; this is now an alias for the latter")]
    pub fn flush_accumulator(&self) -> super::FlushGuard<'_> {
        self.accumulator.flush()
    }

    /// Block until `in_flight` and the batch channel are empty. Does not
    /// block sends and does not send the current pending batch.
    pub fn wait_for_drain(&self, timeout: std::time::Duration) -> bool {
        self.accumulator.wait_for_drain(timeout)
    }

    /// Send the current pending batch, then wait for all sent and in-flight
    /// batches to be applied. Does not block sends.
    pub fn flush_pending_and_wait_for_drain(&self, timeout: std::time::Duration) -> bool {
        self.accumulator.flush_pending_and_wait_for_drain(timeout)
    }

    // ---- Snapshot pin ----

    /// Register a snapshot pin request. See `spec/SNAPSHOT.md`.
    /// Latest acceptable wins; rejected requests are dropped + logged.
    /// Non-blocking. Must be called synchronously from the
    /// finalization path, before the next `push_transition` for the
    /// anchor thread can land (mitigation #1 in the spec).
    pub fn request_snapshot_pin(&self, anchor: super::AnchorBlockRef) -> super::PinRequestGuard {
        self.accumulator.request_snapshot_pin(anchor)
    }

    pub fn cancel_snapshot_pin(
        &self,
        expected_thread: node_types::ThreadIdentifier,
        expected_block: node_types::BlockIdentifier,
    ) {
        self.accumulator.cancel_snapshot_pin(expected_thread, expected_block);
    }

    /// Wait for the snapshot pin to be acquirable, then run the
    /// reachability check across every pin entry. Returns a
    /// `PinHandle` on success; `None` on timeout, atomic-skip
    /// reachability failure, or unrecoverable resolution failure for
    /// any pin entry.
    ///
    /// Reachability resolution: for every routing in every pinned
    /// thread's map at the pinned block, the body must be findable via
    /// `unfinalized_pool_get(hash)` → `accumulator.find_account_operation
    /// (routing, hash)` → `archive.read_account_operation(routing)
    /// (verifying hash matches)`. If any routing on any pinned thread
    /// fails this resolution, the snapshot is skipped atomically.
    pub fn acquire_snapshot_pin(
        &self,
        expected_thread: node_types::ThreadIdentifier,
        expected_block: node_types::BlockIdentifier,
        timeout: std::time::Duration,
    ) -> Option<super::PinHandle> {
        let pin =
            self.accumulator.acquire_snapshot_pin(expected_thread, expected_block, timeout)?;
        // Reachability check at the repository layer (this is where
        // we have the per-block maps and the resolution chain).
        let pin_set = pin.pin_set();
        for (thread_id, block_id) in pin_set.iter() {
            // The defer-apply entry (anchor's own thread) doesn't
            // need a reachability check — the archive is at exactly
            // that block. Skip it.
            if thread_id == pin.anchor_thread_id() && block_id == pin.anchor_block_id() {
                continue;
            }
            if let Err(reason) = self.check_pin_entry_reachable(*thread_id, *block_id) {
                tracing::warn!(
                    target: "monit",
                    "acquire_snapshot_pin: reachability failed for pin entry \
                     (thread={thread_id:?}, block={block_id:?}): {reason}; skipping snapshot",
                );
                // PinHandle drop on this return transitions Pinned →
                // Releasing, which drains the deferred queue. Atomic
                // skip is preserved because the handle hasn't been
                // returned to the caller — no snapshot file is touched.
                return None;
            }
        }
        Some(pin)
    }

    /// Verify that every routing in `(thread_id, block_id)`'s map can
    /// be resolved with a hash-matching body via the pool / accumulator
    /// / archive lookup chain. Returns `Err` describing the first
    /// routing that fails; `Ok(())` on full coverage.
    fn check_pin_entry_reachable(
        &self,
        thread_id: ThreadIdentifier,
        block_id: BlockIdentifier,
    ) -> anyhow::Result<()> {
        let map_state = self.state_load(&block_id, &thread_id)?.ok_or_else(|| {
            anyhow::anyhow!("no on-disk map for thread {thread_id:?} block {block_id:?}")
        })?;
        for (routing, info) in self.map_repo.map_iter(&map_state.0.map) {
            match info {
                AccountInfo::Redirect(_) => {
                    // Redirect entries are pure map entries; no body
                    // to resolve. Continue.
                }
                AccountInfo::VmAccountHash(expected_hash) => {
                    if !self.routing_resolves_to_hash(&routing, *expected_hash) {
                        return Err(anyhow::anyhow!(
                            "routing {routing:?} hash {expected_hash} not found in pool, \
                             accumulator, or archive (or archive bytes hash mismatched)",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Same fallback chain as `find_account_with_info` for the
    /// `VmAccountHash` branch, but only checks existence + hash match.
    /// Used by reachability checking at snapshot acquire time.
    fn routing_resolves_to_hash(
        &self,
        routing: &AccountRouting,
        expected_hash: AccountHash,
    ) -> bool {
        // 1. Unfinalized operational pool by hash — content-addressed, always correct.
        if self.accumulator.unfinalized_pool_get(&expected_hash).is_some() {
            return true;
        }
        // 2. Accumulator by routing+hash.
        if self.accumulator.find_account_operation(routing, &expected_hash).is_some() {
            return true;
        }
        // 3. Archive by routing — must verify hash.
        if let Ok(Some(account)) = self.archive.read_account_operation(routing) {
            if let Ok(vm_account) = account.vm_account() {
                if vm_account.hash() == expected_hash {
                    return true;
                }
            }
        }
        false
    }

    pub fn get_archive_thread_control_states(&self) -> anyhow::Result<Vec<ThreadControlState>> {
        Ok(self.archive.get_all_thread_control_states()?)
    }

    pub fn reset_archive(&self) -> anyhow::Result<()> {
        self.archive.reset()?;
        let control_states = self.archive.get_all_thread_control_states()?;
        self.accumulator.reset(&control_states);
        Ok(())
    }

    pub fn reset_accumulator(&self) -> anyhow::Result<()> {
        let control_states = self.archive.get_all_thread_control_states()?;
        self.accumulator.reset(&control_states);
        Ok(())
    }

    pub fn shutdown(&self) {
        self.accumulator.signal_stop();
        if let Some(handle) = self.update_thread.lock().take() {
            let _ = handle.join();
        }
    }

    fn prepare_updates(
        &self,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &DurableThreadAccountsState,
        accounts: &HashMap<AccountRouting, BlockAccountOperation>,
    ) -> anyhow::Result<(
        HashMap<AccountRouting, ArchiveOperation>,
        Vec<(AccountRouting, Option<AccountInfo>)>,
    )> {
        let mut account_updates = HashMap::new();
        let mut map_updates = Vec::new();
        for (routing, state_account) in accounts {
            tracing::trace!(target: "builder", "Update account in durable state: {}", routing);
            match self.get_account_update(routing, state_account, state, old_tvm_accounts)? {
                AccountUpdate::Redirect(info) => {
                    let AccountInfo::Redirect(dapp_id) = info else {
                        anyhow::bail!("Redirect account update has non-redirect info: {info}");
                    };
                    account_updates.insert(
                        *routing,
                        ArchiveOperation::UpdateOrInsert(ThreadAccount::redirect(dapp_id)),
                    );
                    map_updates.push((*routing, Some(info)));
                }
                AccountUpdate::Update(info, account) => {
                    account_updates.insert(*routing, ArchiveOperation::UpdateOrInsert(account));
                    map_updates.push((*routing, Some(info)));
                }
                AccountUpdate::Remove => {
                    account_updates.insert(*routing, ArchiveOperation::Remove);
                    map_updates.push((*routing, None));
                }
                AccountUpdate::Skip => {}
            }
        }
        Ok((account_updates, map_updates))
    }

    fn get_account_update(
        &self,
        routing: &AccountRouting,
        state_account: &BlockAccountOperation,
        state: &DurableThreadAccountsState,
        old_tvm_accounts: &tvm_block::ShardAccounts,
    ) -> anyhow::Result<AccountUpdate> {
        Ok(match state_account {
            BlockAccountOperation::UpdateOrInsert(state_account) => {
                Self::state_account_update(routing, state_account)?
            }
            BlockAccountOperation::Remove => {
                let has_account = self.map_repo.map_get(&state.0.map, routing).is_some();
                if has_account {
                    AccountUpdate::Remove
                } else {
                    AccountUpdate::Skip
                }
            }
            BlockAccountOperation::MoveFromTvm => {
                if let Ok(Some(tvm_acc)) = old_tvm_accounts.account(&routing.account_id().into()) {
                    Self::state_account_update(routing, &tvm_acc.into())?
                } else {
                    AccountUpdate::Skip
                }
            }
            BlockAccountOperation::AccountMerkleUpdate(update_bytes) => {
                self.apply_account_merkle_update(routing, update_bytes, state, old_tvm_accounts)?
            }
        })
    }

    fn apply_account_merkle_update(
        &self,
        routing: &AccountRouting,
        update_bytes: &[u8],
        state: &DurableThreadAccountsState,
        old_tvm_accounts: &tvm_block::ShardAccounts,
    ) -> anyhow::Result<AccountUpdate> {
        use tvm_block::Deserializable;

        tracing::trace!(
            target: "monit",
            "Applying account merkle update: {}, size: {}",
            routing, update_bytes.len(),);

        // Look up the old account state from the durable state first, then TVM
        let old_shard_acc = if let Some(info) = self.map_repo.map_get(&state.0.map, routing) {
            let account = self.find_account_with_info(state, routing, info)?.ok_or_else(|| {
                anyhow::anyhow!("Missing account data for {} in durable state", routing)
            })?;
            let Some(shard_account) = account.as_tvm() else {
                anyhow::bail!("Account data for {} is not Tvm", routing)
            };
            shard_account.clone()
        } else if let Ok(Some(tvm_acc)) = old_tvm_accounts.account(&routing.account_id().into()) {
            tvm_acc
        } else {
            anyhow::bail!("Account merkle update for non-existent account: {}", routing,);
        };
        let old_shard_acc_cell = old_shard_acc
            .serialize()
            .map_err(|e| anyhow::anyhow!("Failed to serialize old shard account: {e}",))?;

        // Deserialize merkle update
        let update_cell = tvm_types::read_single_root_boc(update_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to read merkle update BOC: {e}"))?;
        let merkle_update = tvm_block::MerkleUpdate::construct_from_cell(update_cell)
            .map_err(|e| anyhow::anyhow!("Failed to construct merkle update: {e}"))?;

        // Apply merkle update to get a new cell
        let new_shard_acc_cell = merkle_update.apply_for(&old_shard_acc_cell).map_err(|e| {
            anyhow::anyhow!("Failed to apply account merkle update: {e}, account: {routing}")
        })?;

        let new_shard_acc = ShardAccount::construct_from_cell(new_shard_acc_cell)
            .map_err(|e| anyhow::anyhow!("Failed to construct account from cell: {e}"))?;
        let new_state_account = ThreadAccount::from_tvm(new_shard_acc);

        tracing::trace!(target: "monit", "Account merkle update applied, account: {}, merkle update: {} bytes",
            routing,
            update_bytes.len(),
        );

        Self::state_account_update(routing, &new_state_account)
    }

    fn state_account_update(
        routing: &AccountRouting,
        account: &ThreadAccount,
    ) -> anyhow::Result<AccountUpdate> {
        // Redirect stubs have no vm account — store only dapp id in the trie
        if account.is_redirect() {
            let Some(dapp_id) = account.get_dapp_id() else {
                anyhow::bail!("Redirect account has no dapp id")
            };
            return Ok(AccountUpdate::Redirect(AccountInfo::Redirect(dapp_id)));
        }
        if account.get_dapp_id() != Some(*routing.dapp_id()) {
            tracing::trace!(
                target: "monit",
                "Account dapp id mismatch: expected [{}], got [{:?}]",
                routing.dapp_id().to_hex_string(),
                account.get_dapp_id()
            );
        }
        Ok(AccountUpdate::Update(
            AccountInfo::VmAccountHash(account.vm_account()?.hash()),
            account.clone(),
        ))
    }

    pub(crate) fn state_apply_diff(
        &self,
        thread_id: &ThreadIdentifier,
        block_height: u64,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &DurableThreadAccountsState,
        diff: &DurableThreadAccountsStateDiff,
    ) -> anyhow::Result<(DurableThreadAccountsState, HashMap<AccountRouting, ArchiveOperation>)>
    {
        self.state_update(thread_id, block_height, old_tvm_accounts, state, &diff.accounts)
    }

    // ---- Snapshot export/import ----

    pub fn export_durable_snapshot(
        &self,
        state: &DurableThreadAccountsState,
    ) -> anyhow::Result<DurableStateSnapshot> {
        let mut snapshot = std::io::Cursor::new(Vec::new());
        self.export_durable_snapshot_to_writer(state, &mut snapshot)?;
        bincode::deserialize(snapshot.get_ref()).map_err(|e| {
            anyhow::format_err!("Failed to deserialize streamed durable snapshot: {e}")
        })
    }

    pub fn import_durable_snapshot(
        &self,
        snapshot: DurableStateSnapshot,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<DurableThreadAccountsState> {
        let bytes = bincode::serialize(&snapshot)?;
        self.import_durable_snapshot_from_reader(
            &mut std::io::Cursor::new(bytes),
            thread_id,
            block_id,
        )
    }

    pub fn export_durable_snapshot_to_writer<W: Write + Seek>(
        &self,
        state: &DurableThreadAccountsState,
        writer: &mut W,
    ) -> anyhow::Result<()> {
        let mut buffered_writer =
            BufWriter::with_capacity(Self::SNAPSHOT_COPY_BUFFER_CAPACITY, writer);

        // Length headers (maps_len, accounts_count) use explicit
        // `to_le_bytes` / `from_le_bytes` rather than bincode. Both header
        // slots are 8 bytes wide, written first as a placeholder and later
        // overwritten with the real value via seek-back. The seek-back
        // pattern is only safe if the placeholder and real-value writes
        // produce the *same* number of bytes; explicit `to_le_bytes` makes
        // that a compile-time fact (always 8 bytes for u64) and removes
        // the dependency on bincode's int-encoding config (which silently
        // changed defaults across major versions and could change again).
        // See `import_durable_snapshot_from_reader` for the matching read.
        let maps_len_pos = buffered_writer.stream_position()?;
        buffered_writer.write_all(&0u64.to_le_bytes())?;
        let maps_start_pos = buffered_writer.stream_position()?;
        self.map_repo.map_write(&state.0.map, &mut buffered_writer)?;
        let maps_end_pos = buffered_writer.stream_position()?;
        let maps_len = maps_end_pos - maps_start_pos;

        let expected: HashMap<_, _> =
            self.map_repo.map_iter(&state.0.map).map(|(routing, info)| (routing, *info)).collect();
        let expected_len = expected.len();

        let accounts_count_pos = buffered_writer.stream_position()?;
        buffered_writer.write_all(&0u64.to_le_bytes())?;
        let mut accounts_count = 0u64;
        let mut written = HashSet::new();

        // Fast path: enumerate the logical archive records and accept each
        // payload only if it validates against the snapshot map. Enumeration
        // is a source of candidate bytes; the map remains authoritative.
        self.archive.store().enumerate(
            &self.archive.set_accounts_a(),
            &mut |record| -> anyhow::Result<()> {
                let routing = key_to_routing(&record.key)?;
                let Some(info) = expected.get(&routing).copied() else {
                    return Ok(());
                };
                let Some(account_bytes) = validate_snapshot_account_bytes(&record.data, &info)
                    .map_err(|err| {
                        anyhow::anyhow!(
                            "snapshot export: failed to validate enumerated archive record \
                             routing={routing:?}: {err}",
                        )
                    })?
                else {
                    return Ok(());
                };
                if written.insert(routing) {
                    write_snapshot_account_entry(&mut buffered_writer, &routing, &account_bytes)?;
                    accounts_count += 1;
                }
                Ok(())
            },
        )?;

        // Fallback path: any map entry not satisfied by archive enumeration
        // must still be exported from an exact hash-matching source, or the
        // snapshot is skipped by returning an error.
        for (routing, info) in expected {
            if written.contains(&routing) {
                continue;
            }
            let body = self.resolve_body_for_export_with_info(&routing, info).ok_or_else(|| {
                anyhow::anyhow!(
                    "snapshot export: routing {routing:?} info {info:?} not found in \
                     archive enumeration, pool, accumulator, or archive (or bytes/hash \
                     mismatched). Snapshot must be skipped — caller should drop the pin \
                     and not produce a snapshot file.",
                )
            })?;
            let body_bytes = body.write_bytes()?;
            write_snapshot_account_entry(&mut buffered_writer, &routing, &body_bytes)?;
            written.insert(routing);
            accounts_count += 1;
        }

        // Self-checks. An inconsistent snapshot must NEVER be produced.
        // If any of these invariants fails, the node is in an undefined
        // state — its in-memory map disagrees with what would be written
        // to disk. Returning an `anyhow::Error` here would let the
        // caller log and continue; that's the wrong response, because
        // any subsequent operation (next snapshot attempt, next block
        // apply, anything that touches the same map) is also operating
        // on the same broken state. Hard-panic instead so the process
        // exits and is restarted into a known-good initial state.
        //
        // These are `assert!` (active in release), not `debug_assert!`
        // (elided in release). Production producers run in release mode;
        // a debug-only check here is the same as no check.
        //
        // Three independent invariants:
        //
        // 1. accounts_count == expected_len: we wrote exactly one body
        //    for every routing the iterator visited.
        // 2. written.len() == expected_len: we visited each routing
        //    exactly once (no duplicates, no equality bugs).
        // 3. serialized_account_count == expected_len: an independent
        //    trie walk over the same map agrees with the iterator's
        //    count, ruling out a missing-leaf bug in MapIter.
        assert_eq!(
            accounts_count as usize, expected_len,
            "snapshot export consistency violated: wrote {} bodies but the snapshot map \
             has {} entries (counted via map_iter). The snapshot would be inconsistent on \
             the consumer side. Most likely cause: `map_iter` did not visit every routing \
             that `map_write` wrote — investigate the durable map's iteration vs \
             serialization paths. Aborting to prevent publishing a broken snapshot.",
            accounts_count, expected_len,
        );
        assert_eq!(
            written.len(),
            expected_len,
            "snapshot export consistency violated: deduped `written` set has {} entries \
             but the snapshot map has {}. Possible double-write or routing-equality bug. \
             Aborting.",
            written.len(),
            expected_len,
        );
        let serialized_account_count = count_serialized_map_leaves(&state.0.map);
        assert_eq!(
            serialized_account_count, expected_len,
            "snapshot export consistency violated: independent trie walk found {} account \
             leaves in the serialized map, but `map_iter` produced {}. The map that gets \
             serialized via `map_write` and the entries `map_iter` visits are out of sync \
             — `map_iter` likely has a missing-leaf bug. Aborting.",
            serialized_account_count, expected_len,
        );

        buffered_writer.flush()?;
        buffered_writer.seek(SeekFrom::Start(maps_len_pos))?;
        buffered_writer.write_all(&maps_len.to_le_bytes())?;
        buffered_writer.seek(SeekFrom::Start(accounts_count_pos))?;
        buffered_writer.write_all(&accounts_count.to_le_bytes())?;
        buffered_writer.seek(SeekFrom::End(0))?;
        buffered_writer.flush()?;
        Ok(())
    }

    /// Same fallback chain as `find_account_with_info`'s `VmAccountHash`
    /// branch, returning the resolved account body. None if the body
    /// can't be produced — caller treats this as an unrecoverable
    /// snapshot failure (atomic skip).
    fn resolve_body_for_export(
        &self,
        routing: &AccountRouting,
        expected_hash: AccountHash,
    ) -> Option<ThreadAccount> {
        if let Some(account) = self.accumulator.unfinalized_pool_get(&expected_hash) {
            return Some(account);
        }
        if let Some(operation) = self.accumulator.find_account_operation(routing, &expected_hash) {
            if let crate::thread_accounts::ArchiveOperation::UpdateOrInsert(account) = operation {
                return Some(account);
            }
            // Remove operation in the accumulator means the routing was
            // deleted; but the map still records VmAccountHash, which
            // is inconsistent. Treat as unrecoverable.
            return None;
        }
        if let Ok(Some(account)) = self.archive.read_account_operation(routing) {
            if let Ok(vm_account) = account.vm_account() {
                if vm_account.hash() == expected_hash {
                    return Some(account);
                }
            }
        }
        None
    }

    fn resolve_body_for_export_with_info(
        &self,
        routing: &AccountRouting,
        info: AccountInfo,
    ) -> Option<ThreadAccount> {
        match info {
            AccountInfo::Redirect(dapp_id) => Some(ThreadAccount::redirect(dapp_id)),
            AccountInfo::VmAccountHash(expected_hash) => {
                self.resolve_body_for_export(routing, expected_hash)
            }
        }
    }

    pub fn import_durable_snapshot_from_reader<R: Read>(
        &self,
        reader: &mut R,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<DurableThreadAccountsState> {
        tracing::debug!("import_durable_snapshot_from_reader");

        // Wrap the input in a BufReader so per-record bincode reads don't
        // turn into many small syscalls, then stream account entries
        // straight into thread_init. The previous implementation spooled
        // the full payload to a temp file and re-read it, doubling disk
        // I/O for no benefit (~2 min for an 18 GB snapshot).
        let mut reader = BufReader::with_capacity(8 * 1024 * 1024, reader);

        // Length headers are written by the producer as explicit
        // little-endian u64 (8 bytes each, fixed-width); read them the
        // same way. Don't go through bincode here — keeping the format
        // independent of bincode's int-encoding config makes the
        // seek-back-and-overwrite pattern in the producer safe by
        // construction. See `export_durable_snapshot_to_writer` for the
        // matching write.
        let mut len_buf = [0u8; 8];
        reader.read_exact(&mut len_buf)?;
        let maps_len = u64::from_le_bytes(len_buf);
        let mut maps_reader = (&mut reader).take(maps_len);
        let map = self.map_repo.map_read(&mut maps_reader)?;
        std::io::copy(&mut maps_reader, &mut std::io::sink())?;

        reader.read_exact(&mut len_buf)?;
        let accounts_len = u64::from_le_bytes(len_buf);
        tracing::debug!(
            "import_durable_snapshot_from_reader: read map, accounts_len={accounts_len}"
        );

        let mut remaining = accounts_len;
        // Track every routing the snapshot's accounts section actually
        // wrote, so we can cross-check it against the snapshot's map.
        let mut written: HashSet<AccountRouting> = HashSet::with_capacity(accounts_len as usize);
        let raw_account_entries = std::iter::from_fn(|| {
            if remaining == 0 {
                return None;
            }
            remaining -= 1;
            match read_snapshot_account_entry(&mut reader) {
                Ok((routing, bytes)) => {
                    written.insert(routing);
                    Some(Ok((routing, bytes)))
                }
                Err(err) => Some(Err(err)),
            }
        });
        self.archive.thread_init_from_raw_entries(thread_id, *block_id, raw_account_entries)?;
        self.accumulator.set_expected_block(*thread_id, *block_id, true);

        // Defense-in-depth: do the work that block verification would
        // later do, but eagerly during import while we're still in the
        // import call frame and can bail cleanly. Walk the imported
        // map; for each entry, run the same resolution that
        // `find_account_with_info` runs during a future verify, and
        // assert it succeeds with a body whose hash matches the map's
        // expected hash. This catches three distinct producer-side bugs
        // *on the consumer*, regardless of which build the producer ran:
        //
        //   1. **Missing body**: the snapshot's map references a routing
        //      whose body was never written to the snapshot. Detected
        //      because `read_account_operation` returns None for that
        //      routing. (Also separately tracked via `written`.)
        //
        //   2. **Wrong-hash body**: a body was written but read-back
        //      produces an account whose hash doesn't match the map's
        //      expected hash. This is the BOC round-trip case — the
        //      producer's `validate_snapshot_account_bytes` succeeded
        //      pre-write, so the bytes-as-written hash to the expected
        //      value, but `ShardAccount::construct_from_bytes` on the
        //      consumer somehow produces a different cell tree. Catches
        //      it now rather than 3 minutes later inside the verifier
        //      with the cryptic "exists in map but no matching version
        //      found" panic.
        //
        //   3. **Map-iter divergence**: producer's `map_iter` undercounted
        //      vs `map_write`, so the deserialized map has more leaves
        //      than the bodies. Caught by the count check below.
        //
        // We use a *fresh* state here (no map_get fallback through pool /
        // accumulator — those are empty after `reset_accumulator`) so the
        // resolution is purely "archive must produce the expected body".
        let import_state = DurableThreadAccountsState::with_map(map.clone());
        let mut missing_bodies: Vec<AccountRouting> = Vec::new();
        let mut wrong_hashes: Vec<(AccountRouting, AccountHash, AccountHash)> = Vec::new();
        let mut total_in_map: usize = 0;
        for (routing, info) in self.map_repo.map_iter(&map) {
            total_in_map += 1;
            match info {
                AccountInfo::Redirect(_) => {
                    // Redirects don't have a hash to verify; the consumer
                    // resolves them by following to a different routing,
                    // and that target routing (if it has VmAccountHash)
                    // is checked elsewhere in this loop.
                }
                AccountInfo::VmAccountHash(expected_hash) => {
                    match self.find_account_with_info(&import_state, &routing, *info) {
                        Ok(Some(account)) => {
                            // find_account_with_info already verifies
                            // hash internally, but recompute here to
                            // surface a precise mismatch in the error.
                            match account.vm_account() {
                                Ok(vm_account) => {
                                    let actual = vm_account.hash();
                                    if actual != *expected_hash {
                                        wrong_hashes.push((routing, *expected_hash, actual));
                                    }
                                }
                                Err(_) => wrong_hashes.push((
                                    routing,
                                    *expected_hash,
                                    AccountHash::default(),
                                )),
                            }
                        }
                        Ok(None) => missing_bodies.push(routing),
                        Err(_) => missing_bodies.push(routing),
                    }
                }
            }
        }
        let serialized_count = count_serialized_map_leaves(&map);
        if !missing_bodies.is_empty()
            || !wrong_hashes.is_empty()
            || serialized_count != total_in_map
        {
            let missing_preview: Vec<String> =
                missing_bodies.iter().take(5).map(|r| r.to_string()).collect();
            let wrong_preview: Vec<String> = wrong_hashes
                .iter()
                .take(5)
                .map(|(r, exp, act)| {
                    format!("{r} expected={} actual={}", exp.to_hex_string(), act.to_hex_string())
                })
                .collect();
            anyhow::bail!(
                "Imported snapshot is internally inconsistent and would cause block-verification \
                 panics. Stats: map has {total_in_map} routings, trie-walk counted {serialized_count} \
                 leaves, accounts section wrote bodies for {} routings, {} routings have NO body, \
                 {} routings have a body whose hash does NOT match the map. \
                 First missing (up to 5): {missing_preview:?}. \
                 First wrong-hash (up to 5): {wrong_preview:?}. \
                 The producer that emitted this snapshot has a bug: either map_iter undercounts \
                 (snapshot incomplete) or write_bytes/read_bytes does not preserve account_cell \
                 repr_hash for some account shapes (BOC round-trip not stable). \
                 Aborting import; please re-fetch from a different peer or fix the producer.",
                written.len(),
                missing_bodies.len(),
                wrong_hashes.len(),
            );
        }

        Ok(DurableThreadAccountsState::with_map(map))
    }
}

#[allow(clippy::large_enum_variant)]
enum AccountUpdate {
    Skip,
    Remove,
    Redirect(AccountInfo),
    Update(AccountInfo, ThreadAccount),
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use node_types::AccountHash;
    use node_types::AccountIdentifier;
    use node_types::AccountRouting;
    use node_types::BlockIdentifier;
    use node_types::DAppIdentifier;
    use node_types::ThreadIdentifier;
    use tvm_block::MsgAddressInt;
    use tvm_block::ShardAccount;
    use tvm_types::AccountId;

    use super::read_snapshot_account_entry;
    use super::validate_snapshot_account_bytes;
    use super::write_snapshot_account_entry;
    use super::AccountReadCache;
    use super::CacheLevel;
    use crate::thread_accounts::durable::archive::update::AccumulatedUpdate;
    use crate::thread_accounts::durable::AccountInfo;
    use crate::ArchiveOperation;
    use crate::ArchiveStateStore;
    use crate::DurableThreadAccountsRepository;
    use crate::DurableThreadAccountsState;
    use crate::ThreadAccount;
    use crate::ThreadAccountsRepository;
    use crate::UpdatePhase;

    fn new_u256(name: &str, seed: usize) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(name.as_bytes());
        hasher.update(&(seed as u64).to_be_bytes());
        hasher.finalize().into()
    }

    fn new_acc(seed: usize) -> (AccountRouting, ThreadAccount) {
        let id = AccountIdentifier::new(new_u256("acc", seed));
        let routing = id.redirect();
        let tvm_acc = tvm_block::Account::with_address(
            MsgAddressInt::with_standart(None, 0, AccountId::from_raw(id.as_slice().to_vec(), 256))
                .unwrap(),
        );
        let tvm_shard_acc = ShardAccount::with_params(
            &tvm_acc,
            new_u256("trans", seed).into(),
            seed as u64,
            Some(routing.dapp_id().as_array().into()),
        )
        .unwrap();
        let acc = ThreadAccount::from(tvm_shard_acc);
        (routing, acc)
    }

    fn account_hash(account: &ThreadAccount) -> AccountHash {
        account.vm_account().unwrap().hash()
    }

    fn state_with_account_hash(
        repo: &DurableThreadAccountsRepository,
        routing: AccountRouting,
        hash: AccountHash,
    ) -> DurableThreadAccountsState {
        let map = repo.map_repo().map_update(
            &DurableThreadAccountsRepository::new_state().0.map,
            &[(routing, Some(AccountInfo::VmAccountHash(hash)))],
        );
        DurableThreadAccountsState::with_map(map)
    }

    fn setup_repo() -> (ThreadAccountsRepository, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repo = ThreadAccountsRepository::builder(PathBuf::from(dir.path()))
            .set_apply_to_durable(true)
            .build()
            .unwrap();
        repo.ensure_thread(&ThreadIdentifier::default(), &BlockIdentifier::default(), true)
            .unwrap();
        repo.start_archive_update_service().unwrap();
        (repo, dir)
    }

    fn assert_drained(repo: &ThreadAccountsRepository) {
        assert!(
            repo.flush_pending_and_wait_for_drain(Duration::from_secs(5)),
            "archive accumulator did not drain"
        );
    }

    #[test]
    fn account_read_cache_inserts_into_l1_when_elapsed_exceeds_15ms() {
        let cache = AccountReadCache::new();
        let (_, account) = new_acc(200);
        let hash = account_hash(&account);

        let level = cache.insert_by_elapsed(hash, &account, Duration::from_millis(16)).unwrap();

        assert_eq!(level, CacheLevel::L1);
        assert_eq!(
            cache.get(&hash).map(|(level, account)| (level, account.as_ref().clone())),
            Some((CacheLevel::L1, account))
        );
    }

    #[test]
    fn account_read_cache_inserts_into_l2_when_elapsed_is_between_thresholds() {
        let cache = AccountReadCache::new();
        let (_, account) = new_acc(201);
        let hash = account_hash(&account);

        let level = cache.insert_by_elapsed(hash, &account, Duration::from_millis(4)).unwrap();

        assert_eq!(level, CacheLevel::L2);
        assert_eq!(
            cache.get(&hash).map(|(level, account)| (level, account.as_ref().clone())),
            Some((CacheLevel::L2, account))
        );
    }

    #[test]
    fn account_read_cache_does_not_insert_when_elapsed_is_at_or_below_3ms() {
        let cache = AccountReadCache::new();
        let (_, account) = new_acc(202);
        let hash = account_hash(&account);

        assert!(cache.insert_by_elapsed(hash, &account, Duration::from_millis(3)).is_none());
        assert!(cache.get(&hash).is_none());
    }

    #[test]
    fn account_read_cache_checks_l1_before_l2() {
        let cache = AccountReadCache::new();
        let (_, l1_account) = new_acc(203);
        let (_, l2_account) = new_acc(204);
        let hash = AccountHash::new(new_u256("cache_hash", 1));

        cache.insert_l2(hash, Arc::new(l2_account));
        cache.insert_l1(hash, Arc::new(l1_account.clone()));

        assert_eq!(
            cache.get(&hash).map(|(level, account)| (level, account.as_ref().clone())),
            Some((CacheLevel::L1, l1_account))
        );
    }

    #[test]
    fn hash_mismatch_from_archive_is_not_cached() {
        let dir = tempfile::tempdir().unwrap();
        let archive = ArchiveStateStore::in_memory();
        let repo =
            DurableThreadAccountsRepository::new(PathBuf::from(dir.path()), archive, None).unwrap();
        let thread_id = ThreadIdentifier::default();
        let block_0 = BlockIdentifier::default();
        let block_1 = BlockIdentifier::new(new_u256("mismatch_block", 1));

        repo.ensure_thread(&thread_id, &block_0, true).unwrap();

        let (routing, expected_account) = new_acc(205);
        let (_, archived_account) = new_acc(206);
        let expected_hash = account_hash(&expected_account);
        let state = state_with_account_hash(&repo, routing, expected_hash);

        let mut update = AccumulatedUpdate::new();
        update.transition_thread(thread_id, block_0, block_1);
        update.insert(routing, ArchiveOperation::UpdateOrInsert(archived_account));
        repo.archive.apply_update(&update).unwrap();

        let err = repo
            .find_account_with_info(&state, &routing, AccountInfo::VmAccountHash(expected_hash))
            .unwrap_err();

        assert!(err.downcast_ref::<super::AccountHashMismatchError>().is_some());
        assert!(repo.account_read_cache.get(&expected_hash).is_none());
    }

    #[test]
    fn repository_lookup_returns_account_from_hash_cache_without_archive_read() {
        let dir = tempfile::tempdir().unwrap();
        let archive = ArchiveStateStore::in_memory();
        let repo =
            DurableThreadAccountsRepository::new(PathBuf::from(dir.path()), archive, None).unwrap();

        let (routing, account) = new_acc(207);
        let hash = account_hash(&account);
        let state = state_with_account_hash(&repo, routing, hash);
        repo.account_read_cache.insert_l1(hash, Arc::new(account.clone()));

        let resolved = repo
            .find_account_with_info(&state, &routing, AccountInfo::VmAccountHash(hash))
            .unwrap()
            .expect("cache hit should return account");

        assert_eq!(resolved, account);
        assert!(repo.archive.read_account_operation(&routing).unwrap().is_none());
    }

    #[test]
    fn snapshot_account_entry_writer_matches_bincode_tuple() {
        let routing = AccountRouting::new(
            DAppIdentifier::new([0x11; 32]),
            AccountIdentifier::new([0x22; 32]),
        );
        let account_bytes = vec![1u8, 2, 3, 4, 5];

        let expected = bincode::serialize(&(routing, account_bytes.clone())).unwrap();
        let mut actual = Vec::new();
        write_snapshot_account_entry(&mut actual, &routing, &account_bytes).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn snapshot_account_entry_reader_matches_bincode_tuple() {
        let routing = AccountRouting::new(
            DAppIdentifier::new([0x33; 32]),
            AccountIdentifier::new([0x44; 32]),
        );
        let account_bytes = vec![9u8, 8, 7, 6, 5];
        let encoded = bincode::serialize(&(routing, account_bytes.clone())).unwrap();

        let (decoded_routing, decoded_account_bytes) =
            read_snapshot_account_entry(&mut Cursor::new(encoded)).unwrap();

        assert_eq!(decoded_routing, routing);
        assert_eq!(decoded_account_bytes, account_bytes);
    }

    #[test]
    fn snapshot_archive_payload_validation_accepts_matching_vm_and_redirect() {
        let (routing, account) = new_acc(1);
        let account_bytes = account.write_bytes().unwrap();
        let archive_data = bincode::serialize(&account_bytes).unwrap();
        let expected_hash = account.vm_account().unwrap().hash();

        let validated = validate_snapshot_account_bytes(
            &archive_data,
            &AccountInfo::VmAccountHash(expected_hash),
        )
        .unwrap()
        .unwrap();
        assert_eq!(validated, account_bytes);

        let redirect = ThreadAccount::redirect(*routing.dapp_id());
        let redirect_bytes = redirect.write_bytes().unwrap();
        let redirect_archive_data = bincode::serialize(&redirect_bytes).unwrap();
        let validated_redirect = validate_snapshot_account_bytes(
            &redirect_archive_data,
            &AccountInfo::Redirect(*routing.dapp_id()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(validated_redirect, redirect_bytes);
    }

    #[test]
    fn snapshot_archive_payload_validation_rejects_mismatched_map_info() {
        let (_routing, account) = new_acc(2);
        let archive_data = bincode::serialize(&account.write_bytes().unwrap()).unwrap();

        let (_, other_account) = new_acc(99);
        let wrong_hash = other_account.vm_account().unwrap().hash();
        assert!(validate_snapshot_account_bytes(
            &archive_data,
            &AccountInfo::VmAccountHash(wrong_hash),
        )
        .unwrap()
        .is_none());

        let wrong_dapp = DAppIdentifier::new(new_u256("wrong_dapp", 2));
        assert!(
            validate_snapshot_account_bytes(&archive_data, &AccountInfo::Redirect(wrong_dapp),)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn export_durable_snapshot_writes_raw_account_bytes() {
        let thread_id = ThreadIdentifier::default();
        let block_id = BlockIdentifier::new(new_u256("export_block", 1));

        let (repo, _dir) = setup_repo();
        let mut builder = repo.state_builder(&thread_id, 0, &ThreadAccountsRepository::new_state());
        let (routing, account) = new_acc(3);
        let expected_bytes = account.write_bytes().unwrap();
        builder.insert_account(&routing, &account);
        let transition = builder.build(None).unwrap();
        let new_state = transition.new_state.clone();
        repo.finalize_thread_transition(
            &block_id,
            &thread_id,
            0,
            &new_state,
            transition.account_operations,
        )
        .unwrap();
        assert_drained(&repo);

        let mut snapshot = Cursor::new(Vec::new());
        repo.export_durable_snapshot_to_writer(&new_state, &mut snapshot).unwrap();
        let snapshot_bytes = snapshot.into_inner();

        let mut cursor = Cursor::new(snapshot_bytes);
        let mut len_buf = [0u8; 8];
        std::io::Read::read_exact(&mut cursor, &mut len_buf).unwrap();
        let maps_len = u64::from_le_bytes(len_buf);
        cursor.set_position(cursor.position() + maps_len);
        std::io::Read::read_exact(&mut cursor, &mut len_buf).unwrap();
        let accounts_count = u64::from_le_bytes(len_buf);
        assert_eq!(accounts_count, 1);
        let (actual_routing, actual_bytes) = read_snapshot_account_entry(&mut cursor).unwrap();
        assert_eq!(actual_routing, routing);
        assert_eq!(actual_bytes, expected_bytes);
    }

    #[test]
    fn wait_for_drain_does_not_flush_pending_batch_but_flush_pending_does() {
        let thread_id = ThreadIdentifier::default();
        let block_id = BlockIdentifier::new(new_u256("pending_block", 1));

        let (repo, _dir) = setup_repo();
        let mut builder = repo.state_builder(&thread_id, 0, &ThreadAccountsRepository::new_state());
        let (routing, account) = new_acc(17);
        builder.insert_account(&routing, &account);
        let transition = builder.build(None).unwrap();

        repo.finalize_thread_transition(
            &block_id,
            &thread_id,
            0,
            &transition.new_state,
            transition.account_operations,
        )
        .unwrap();

        assert!(
            repo.wait_for_drain(Duration::from_millis(100)),
            "wait_for_drain should succeed even while the last partial batch is still pending",
        );
        assert!(
            repo.durable_map_repo().archive_account_for_test(&routing).unwrap().is_none(),
            "wait_for_drain must not make the pending batch visible in durable storage",
        );

        assert!(
            repo.flush_pending_and_wait_for_drain(Duration::from_secs(5)),
            "flush_pending_and_wait_for_drain should ship the pending batch and drain it",
        );

        let archived = repo
            .durable_map_repo()
            .archive_account_for_test(&routing)
            .unwrap()
            .expect("flushed account must be visible in durable storage");
        assert_eq!(archived.vm_account().unwrap().hash(), account.vm_account().unwrap().hash(),);
    }

    #[test]
    fn import_durable_snapshot_from_reader_rejects_truncated_input_without_partial_archive() {
        let thread_id = ThreadIdentifier::default();
        let block_id = BlockIdentifier::new(new_u256("block", 1));

        let (repo, _dir) = setup_repo();
        let mut builder = repo.state_builder(&thread_id, 0, &ThreadAccountsRepository::new_state());
        let (routing, account) = new_acc(1);
        builder.insert_account(&routing, &account);
        let transition = builder.build(None).unwrap();
        let new_state = transition.new_state.clone();
        repo.finalize_thread_transition(
            &block_id,
            &thread_id,
            0,
            &new_state,
            transition.account_operations,
        )
        .unwrap();
        assert_drained(&repo);

        let mut snapshot = Cursor::new(Vec::new());
        repo.export_durable_snapshot_to_writer(&new_state, &mut snapshot).unwrap();
        let mut snapshot_bytes = snapshot.into_inner();
        snapshot_bytes.pop().unwrap();

        let (import_repo, _import_dir) = setup_repo();
        import_repo.reset_archive().unwrap();

        let err = import_repo
            .import_durable_snapshot_from_reader(
                &mut Cursor::new(snapshot_bytes),
                &thread_id,
                &block_id,
            )
            .unwrap_err();

        let err_msg = err.to_string();
        assert!(
            err_msg.contains("failed to fill whole buffer")
                || err_msg.contains("unexpected end of file")
        );

        let control_states = import_repo.get_archive_thread_control_states().unwrap();
        assert_eq!(control_states.len(), 1);
        assert_eq!(control_states[0].thread_id, thread_id);
        assert_eq!(control_states[0].update_phase, UpdatePhase::Uninitialized);
        assert!(import_repo
            .durable_map_repo()
            .archive_account_for_test(&routing)
            .unwrap()
            .is_none());
    }
}
