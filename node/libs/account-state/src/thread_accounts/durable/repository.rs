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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use lru::LruCache;
use multi_map::node::NodeAug as MultiMapNode;
use multi_map::MultiMapAugmentation;
use node_types::AccountHash;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifier;
use node_types::DAppIdentifierPath;
use node_types::ThreadAccountsHash;
use node_types::ThreadIdentifier;
use parking_lot::Mutex;
use rayon::prelude::*;
use serde::Deserialize;
use serde::Serialize;
use tvm_block::Serializable;
use tvm_block::ShardAccount;

use crate::thread_accounts::accumulator::ArchiveUpdateAccumulator;
use crate::thread_accounts::accumulator::MerkleMapStore;
use crate::thread_accounts::archive::update::ThreadSnapshot;
use crate::thread_accounts::durable::archive::apply::key_to_routing;
use crate::thread_accounts::durable::archive::control::ThreadControlState;
use crate::thread_accounts::durable::archive::store::ArchiveStateStore;
use crate::thread_accounts::durable::archive::store::StagedArchiveEpoch;
use crate::thread_accounts::durable::AccountCapacity;
use crate::thread_accounts::durable::AccountInfo;
use crate::thread_accounts::durable::DurableStateSnapshot;
use crate::thread_accounts::ArchiveOperation;
use crate::StateAccountsMetrics;
use crate::ThreadAccount;

const SNAPSHOT_WRITTEN_HASH_BATCH_SIZE: usize = 10_000;
const SNAPSHOT_WRITTEN_HASH_QUEUE_BATCHES: usize = 4;
const SNAPSHOT_WRITTEN_HASH_MAX_WORKERS: usize = 8;
const SNAPSHOT_EXPORT_VALIDATION_BATCH_SIZE: usize = 1_000;
const SNAPSHOT_EXPORT_VALIDATION_QUEUE_BATCHES: usize = 4;
const SNAPSHOT_EXPORT_VALIDATION_DEFAULT_WORKERS: usize = 1;
const PARALLEL_PREPARE_UPDATES_THRESHOLD: usize = 16;
// Bound old canonical bodies prefetched for Merkle apply, not all process
// memory: decoded cells, caches and the resulting new states may live longer.
const PREPARE_OLD_ACCOUNTS_BYTES: u64 = 64 * 1024 * 1024;

fn prepare_window_len(sizes: &[u64], budget: u64) -> usize {
    let mut bytes = 0u64;
    for (index, &size) in sizes.iter().enumerate() {
        if size > budget.saturating_sub(bytes) {
            // An oversized account must still make progress, alone.
            return index.max(1);
        }
        bytes += size;
    }
    sizes.len()
}

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
    fn count_l1_leaves<V: multi_map::MultiMapValue, A: MultiMapAugmentation>(
        node: &MultiMapNode<V, A>,
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

    fn count_node_leaves<V: multi_map::MultiMapValue, A: MultiMapAugmentation>(
        node: &MultiMapNode<V, A>,
    ) -> usize {
        match node {
            MultiMapNode::Empty => 0,
            MultiMapNode::Leaf(_) => 1,
            MultiMapNode::Branch(data) => data.children.iter().map(count_node_leaves).sum(),
            MultiMapNode::Ext(data) => count_node_leaves(&data.child),
        }
    }

    let mut total = 0usize;
    count_l1_leaves(&map.dapps.root, &mut |l1_value: &super::maps::L1MapValue| {
        total += count_node_leaves(&l1_value.0.root);
    });
    total
}

pub(super) fn write_snapshot_account_entry<W: Write>(
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

fn check_snapshot_cancelled<F>(should_cancel: &F) -> anyhow::Result<()>
where
    F: Fn() -> bool,
{
    if should_cancel() {
        anyhow::bail!("snapshot save cancelled");
    }
    Ok(())
}

pub(super) fn read_snapshot_account_entry<R: Read>(
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

#[cfg(test)]
fn validate_snapshot_account_bytes(
    archive_data: &[u8],
    expected: &AccountInfo,
) -> anyhow::Result<Option<Vec<u8>>> {
    validate_snapshot_export_account(archive_data, expected)
}

fn validate_snapshot_export_account(
    archive_data: &[u8],
    expected: &AccountInfo,
) -> anyhow::Result<Option<Vec<u8>>> {
    let account_bytes: Vec<u8> = bincode::deserialize(archive_data)?;
    let account = ThreadAccount::validate_tvm_bytes(Box::from(account_bytes.as_slice()))?;
    let matches = match expected {
        AccountInfo::Redirect(dapp_id) => {
            account.is_redirect() && account.get_dapp_id() == Some(*dapp_id)
        }
        AccountInfo::AccountHash(expected_hash) => account.account_hash() == *expected_hash,
    };
    if !matches {
        return Ok(None);
    }
    Ok(Some(account_bytes))
}

type SnapshotHashBatch = Vec<(AccountRouting, Vec<u8>)>;
type SnapshotHashBatchResult = anyhow::Result<Vec<SnapshotWrittenAccountInfo>>;
type SnapshotExportValidationBatch = Vec<(AccountRouting, AccountInfo, Vec<u8>)>;
type SnapshotExportValidationBatchResult = anyhow::Result<Vec<(AccountRouting, Vec<u8>)>>;

#[derive(Clone, Copy)]
struct SnapshotWrittenAccountInfo {
    routing: AccountRouting,
    hash: AccountHash,
    canonical_size: u64,
}

struct WrittenHashPipeline {
    input_tx: Option<crossbeam_channel::Sender<SnapshotHashBatch>>,
    result_rx: crossbeam_channel::Receiver<SnapshotHashBatchResult>,
    workers: Vec<JoinHandle<()>>,
    submitted_batches: usize,
}

impl WrittenHashPipeline {
    fn new() -> anyhow::Result<Self> {
        let worker_count = snapshot_written_hash_worker_count();
        let queue_batches = snapshot_written_hash_queue_batches();
        let (input_tx, input_rx) = crossbeam_channel::bounded::<SnapshotHashBatch>(queue_batches);
        let (result_tx, result_rx) = crossbeam_channel::unbounded::<SnapshotHashBatchResult>();

        let mut workers = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let input_rx = input_rx.clone();
            let result_tx = result_tx.clone();
            workers.push(
                std::thread::Builder::new()
                    .name(format!("snapshot-written-hash-{index}"))
                    .spawn(move || {
                        for batch in input_rx {
                            let result = process_snapshot_hash_batch(batch);
                            if result_tx.send(result).is_err() {
                                break;
                            }
                        }
                    })
                    .map_err(|e| anyhow::anyhow!("Failed to spawn written hash worker: {e}"))?,
            );
        }
        drop(result_tx);

        Ok(Self { input_tx: Some(input_tx), result_rx, workers, submitted_batches: 0 })
    }

    fn submit(&mut self, batch: SnapshotHashBatch) -> anyhow::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let sender = self
            .input_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Written hash pipeline is already closed"))?;
        sender.send(batch).map_err(|_| anyhow::anyhow!("Written hash pipeline workers stopped"))?;
        self.submitted_batches += 1;
        Ok(())
    }

    fn finish(mut self) -> anyhow::Result<Vec<SnapshotWrittenAccountInfo>> {
        drop(self.input_tx.take());
        let mut written = Vec::new();
        for _ in 0..self.submitted_batches {
            let result = self
                .result_rx
                .recv()
                .map_err(|_| anyhow::anyhow!("Written hash pipeline stopped before finishing"))??;
            written.extend(result);
        }
        for worker in self.workers.drain(..) {
            worker.join().map_err(|_| anyhow::anyhow!("Written hash pipeline worker panicked"))?;
        }
        Ok(written)
    }
}

impl Drop for WrittenHashPipeline {
    fn drop(&mut self) {
        drop(self.input_tx.take());
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn process_snapshot_hash_batch(batch: SnapshotHashBatch) -> SnapshotHashBatchResult {
    batch
        .into_iter()
        .map(|(routing, bytes)| {
            let account = ThreadAccount::validate_tvm_bytes(bytes.into_boxed_slice())?;
            let hash =
                if account.is_redirect() { AccountHash::default() } else { account.account_hash() };
            Ok(SnapshotWrittenAccountInfo {
                routing,
                hash,
                canonical_size: account.canonical_size(),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()
}

pub(super) fn snapshot_written_hash_worker_count() -> usize {
    std::env::var("SNAPSHOT_WRITTEN_HASH_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(NonZeroUsize::get)
                .unwrap_or(1)
                .min(SNAPSHOT_WRITTEN_HASH_MAX_WORKERS)
        })
}

pub(super) fn snapshot_written_hash_queue_batches() -> usize {
    std::env::var("SNAPSHOT_WRITTEN_HASH_QUEUE_BATCHES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(SNAPSHOT_WRITTEN_HASH_QUEUE_BATCHES)
}

struct ExportValidationPipeline {
    input_tx: Option<crossbeam_channel::Sender<SnapshotExportValidationBatch>>,
    workers: Vec<JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
}

impl ExportValidationPipeline {
    fn new(
        result_tx: crossbeam_channel::Sender<SnapshotExportValidationBatchResult>,
    ) -> anyhow::Result<Self> {
        let worker_count = snapshot_export_validation_worker_count();
        let queue_batches = snapshot_export_validation_queue_batches();
        let (input_tx, input_rx) =
            crossbeam_channel::bounded::<SnapshotExportValidationBatch>(queue_batches);
        let cancelled = Arc::new(AtomicBool::new(false));

        let mut workers = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let input_rx = input_rx.clone();
            let result_tx = result_tx.clone();
            let cancelled = cancelled.clone();
            let span = tracing::Span::current();
            workers.push(
                std::thread::Builder::new()
                    .name(format!("snapshot-export-validate-{index}"))
                    .spawn(move || {
                        let _span = span.enter();
                        let worker_started = Instant::now();
                        let mut validation_active = Duration::ZERO;
                        let mut result_send_wait = Duration::ZERO;
                        let mut batches = 0;
                        let mut accounts = 0;
                        for batch in input_rx {
                            batches += 1;
                            accounts += batch.len();
                            let validation_started = Instant::now();
                            let result =
                                process_snapshot_export_validation_batch(batch, &cancelled);
                            validation_active += validation_started.elapsed();
                            if result.is_err() {
                                cancelled.store(true, Ordering::Relaxed);
                            }
                            let send_started = Instant::now();
                            let sent = result_tx.send(result);
                            result_send_wait += send_started.elapsed();
                            if sent.is_err() {
                                break;
                            }
                            if cancelled.load(Ordering::Relaxed) {
                                break;
                            }
                        }
                        tracing::info!(target: "monit", worker = index, batches, accounts,
                            validation_active_ms = validation_active.as_millis() as u64,
                            result_send_wait_ms = result_send_wait.as_millis() as u64,
                            worker_elapsed_ms = worker_started.elapsed().as_millis() as u64,
                            "Snapshot export validation worker finished (active time includes BOC decoding, hash checks and account cleanup)");
                    })
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to spawn export validation worker: {e}")
                    })?,
            );
        }
        Ok(Self { input_tx: Some(input_tx), workers, cancelled })
    }

    fn submit(&mut self, batch: SnapshotExportValidationBatch) -> anyhow::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        if self.cancelled.load(Ordering::Relaxed) {
            anyhow::bail!("Export validation pipeline cancelled");
        }
        let sender = self
            .input_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Export validation pipeline is already closed"))?;
        sender
            .send(batch)
            .map_err(|_| anyhow::anyhow!("Export validation pipeline workers stopped"))?;
        Ok(())
    }

    fn finish(mut self) -> anyhow::Result<()> {
        drop(self.input_tx.take());
        for worker in self.workers.drain(..) {
            worker
                .join()
                .map_err(|_| anyhow::anyhow!("Export validation pipeline worker panicked"))?;
        }
        Ok(())
    }
}

impl Drop for ExportValidationPipeline {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        drop(self.input_tx.take());
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn write_validated_export_batch<W: Write>(
    batch: Vec<(AccountRouting, Vec<u8>)>,
    writer: &mut W,
    written: &mut HashSet<AccountRouting>,
    accounts_count: &mut u64,
) -> anyhow::Result<()> {
    for (routing, account_bytes) in batch {
        if written.insert(routing) {
            write_snapshot_account_entry(writer, &routing, &account_bytes)?;
            *accounts_count += 1;
        }
    }
    Ok(())
}

fn write_export_validation_results<W: Write>(
    mut writer: W,
    result_rx: crossbeam_channel::Receiver<SnapshotExportValidationBatchResult>,
) -> anyhow::Result<(W, HashSet<AccountRouting>, u64)> {
    let writer_started = Instant::now();
    let mut write_active = Duration::ZERO;
    let mut batches = 0;
    let mut written = HashSet::new();
    let mut accounts_count = 0u64;
    for result in result_rx {
        let write_started = Instant::now();
        write_validated_export_batch(result?, &mut writer, &mut written, &mut accounts_count)?;
        write_active += write_started.elapsed();
        batches += 1;
    }
    tracing::info!(target: "monit", batches, accounts_count,
        write_active_ms = write_active.as_millis() as u64,
        writer_elapsed_ms = writer_started.elapsed().as_millis() as u64,
        "Snapshot export body writer finished (active time includes encoding, writes and routing bookkeeping)");
    Ok((writer, written, accounts_count))
}

fn process_snapshot_export_validation_batch(
    batch: SnapshotExportValidationBatch,
    cancelled: &AtomicBool,
) -> SnapshotExportValidationBatchResult {
    let mut validated = Vec::with_capacity(batch.len());
    for (routing, info, archive_data) in batch {
        if cancelled.load(Ordering::Relaxed) {
            anyhow::bail!("Export validation pipeline cancelled");
        }
        let account_bytes =
            validate_snapshot_export_account(&archive_data, &info).map_err(|err| {
                anyhow::anyhow!(
                    "snapshot export: failed to validate enumerated archive record \
                 routing={routing:?}: {err}",
                )
            })?;
        if let Some(account_bytes) = account_bytes {
            validated.push((routing, account_bytes));
        }
    }
    Ok(validated)
}

fn snapshot_export_validation_worker_count() -> usize {
    std::env::var("SNAPSHOT_EXPORT_VALIDATION_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(SNAPSHOT_EXPORT_VALIDATION_DEFAULT_WORKERS)
}

fn snapshot_export_validation_queue_batches() -> usize {
    std::env::var("SNAPSHOT_EXPORT_VALIDATION_QUEUE_BATCHES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(SNAPSHOT_EXPORT_VALIDATION_QUEUE_BATCHES)
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum BlockAccountOperation {
    UpdateOrInsert(ThreadAccount),
    Remove,
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
    pub fn new(map: ThreadAccountMap) -> Self {
        let count = LIVE_INNER_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        tracing::trace!(target: "monit", "DurableState inner created, live={count}");
        Self(Arc::new(DurableThreadAccountsStateInner { map }))
    }

    /// Number of live inner state instances (for diagnostics).
    pub fn live_inner_count() -> usize {
        LIVE_INNER_COUNT.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// State backed directly by the archive — no in-memory parents.
    pub fn with_map(map: ThreadAccountMap) -> Self {
        Self::new(map)
    }

    pub fn map(&self) -> &ThreadAccountMap {
        &self.0.map
    }
}

use crate::thread_accounts::durable::maps::ThreadAccountMap;
use crate::thread_accounts::kv_store::aerospike::AerospikeAccountsCacheStat;

const DEFAULT_ACCUMULATED_UPDATE_BLOCK_LIMIT: usize = 2000;
// TODO: move cache capacities into account-state/archive configuration if tuning is needed.
const ACCOUNT_READ_CACHE_L1_CAPACITY: usize = 1_500;
const ACCOUNT_READ_CACHE_L2_CAPACITY: usize = 1_500;
const ACCOUNT_READ_CACHE_L1_THRESHOLD: Duration = Duration::from_millis(15);
const ACCOUNT_READ_CACHE_L2_THRESHOLD: Duration = Duration::from_millis(3);
const ACCOUNT_WRITTEN_CACHE_L1_THRESHOLD_BYTES: usize = 1024 * 1024;
const ACCOUNT_WRITTEN_CACHE_L2_THRESHOLD_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CacheLevel {
    L1,
    L2,
}

/// Resolve L1 completely before taking L2, without holding two guards at once.
/// Cloning the cached Arcs is cheap; account decoding stays outside these locks.
fn get_cached_accounts(
    l1: &Mutex<LruCache<AccountHash, Arc<ThreadAccount>>>,
    l2: &Mutex<LruCache<AccountHash, Arc<ThreadAccount>>>,
    hashes: &[AccountHash],
) -> Vec<Option<(CacheLevel, Arc<ThreadAccount>)>> {
    if hashes.is_empty() {
        return Vec::new();
    }
    let mut results = {
        let mut cache = l1.lock();
        hashes
            .iter()
            .map(|hash| cache.get(hash).cloned().map(|a| (CacheLevel::L1, a)))
            .collect::<Vec<_>>()
    };
    if results.iter().any(Option::is_none) {
        let mut cache = l2.lock();
        for (hash, result) in hashes.iter().zip(&mut results) {
            if result.is_none() {
                *result = cache.get(hash).cloned().map(|a| (CacheLevel::L2, a));
            }
        }
    }
    results
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

    fn get_many(&self, hashes: &[AccountHash]) -> Vec<Option<(CacheLevel, Arc<ThreadAccount>)>> {
        get_cached_accounts(&self.l1, &self.l2, hashes)
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

    fn remove_many(&self, hashes: &[AccountHash]) {
        if hashes.is_empty() {
            return;
        }
        let mut retired = Vec::with_capacity(hashes.len().saturating_mul(2));
        {
            let mut cache = self.l1.lock();
            for hash in hashes {
                if let Some(account) = cache.pop(hash) {
                    retired.push(account);
                }
            }
        }
        {
            let mut cache = self.l2.lock();
            for hash in hashes {
                if let Some(account) = cache.pop(hash) {
                    retired.push(account);
                }
            }
        }
    }

    fn insert_l1(&self, hash: AccountHash, account: Arc<ThreadAccount>) {
        let evicted = self.l1.lock().push(hash, account);
        let promoted = self.l2.lock().pop(&hash);
        drop((evicted, promoted));
    }

    fn insert_l2(&self, hash: AccountHash, account: Arc<ThreadAccount>) {
        let evicted = self.l2.lock().push(hash, account);
        drop(evicted);
    }

    #[cfg(test)]
    fn remove(&self, hash: &AccountHash) -> bool {
        let removed_l1 = self.l1.lock().pop(hash).is_some();
        let removed_l2 = self.l2.lock().pop(hash).is_some();
        removed_l1 || removed_l2
    }
}

pub(crate) struct AccountWrittenCache {
    l1: Mutex<LruCache<AccountHash, Arc<ThreadAccount>>>,
    l2: Mutex<LruCache<AccountHash, Arc<ThreadAccount>>>,
}

impl AccountWrittenCache {
    pub(crate) fn new() -> Self {
        Self {
            l1: Mutex::new(LruCache::new(
                NonZeroUsize::new(ACCOUNT_READ_CACHE_L1_CAPACITY)
                    .expect("L1 account written cache capacity must be non-zero"),
            )),
            l2: Mutex::new(LruCache::new(
                NonZeroUsize::new(ACCOUNT_READ_CACHE_L2_CAPACITY)
                    .expect("L2 account written cache capacity must be non-zero"),
            )),
        }
    }

    pub(crate) fn get(&self, hash: &AccountHash) -> Option<(CacheLevel, Arc<ThreadAccount>)> {
        if let Some(account) = self.l1.lock().get(hash).cloned() {
            return Some((CacheLevel::L1, account));
        }
        self.l2.lock().get(hash).cloned().map(|account| (CacheLevel::L2, account))
    }

    fn get_many(&self, hashes: &[AccountHash]) -> Vec<Option<(CacheLevel, Arc<ThreadAccount>)>> {
        get_cached_accounts(&self.l1, &self.l2, hashes)
    }

    pub(crate) fn insert_by_serialized_len(
        &self,
        hash: AccountHash,
        account: &ThreadAccount,
        serialized_len: usize,
    ) -> Option<CacheLevel> {
        if serialized_len > ACCOUNT_WRITTEN_CACHE_L1_THRESHOLD_BYTES {
            self.insert_l1(hash, Arc::new(account.clone()));
            return Some(CacheLevel::L1);
        }
        if serialized_len > ACCOUNT_WRITTEN_CACHE_L2_THRESHOLD_BYTES {
            self.insert_l2(hash, Arc::new(account.clone()));
            return Some(CacheLevel::L2);
        }
        None
    }

    fn insert_l1(&self, hash: AccountHash, account: Arc<ThreadAccount>) {
        let evicted = self.l1.lock().push(hash, account);
        let promoted = self.l2.lock().pop(&hash);
        drop((evicted, promoted));
    }

    fn insert_l2(&self, hash: AccountHash, account: Arc<ThreadAccount>) {
        let evicted = self.l2.lock().push(hash, account);
        drop(evicted);
    }
}

#[derive(Clone)]
pub struct DurableThreadAccountsRepository {
    durable_path: PathBuf,
    archive: ArchiveStateStore,
    accumulator: Arc<ArchiveUpdateAccumulator>,
    account_read_cache: Arc<AccountReadCache>,
    // Zerostate is an immutable replay base. Its bodies must remain available
    // after finalization drains the pool and the archive advances past genesis.
    genesis_accounts: Arc<parking_lot::RwLock<HashMap<AccountHash, ThreadAccount>>>,
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

        let accumulator = Arc::new(ArchiveUpdateAccumulator::new(
            DEFAULT_ACCUMULATED_UPDATE_BLOCK_LIMIT,
            &archive.get_all_thread_control_states()?,
            metrics.clone(),
        ));
        Ok(Self {
            durable_path,
            archive,
            accumulator,
            account_read_cache: Arc::new(AccountReadCache::new()),
            genesis_accounts: Default::default(),
            update_thread: Arc::new(parking_lot::Mutex::new(None)),
            metrics,
        })
    }

    pub fn start_archive_update_service(&self) -> anyhow::Result<()> {
        let mut update_thread_guard = self.update_thread.lock();
        if update_thread_guard.is_some() {
            return Ok(());
        }
        let maps_store = MerkleMapStore::new(&self.durable_path, self.metrics.clone());
        let update_thread = self.accumulator.start_update_loop(self.archive.clone(), maps_store)?;
        *update_thread_guard = Some(update_thread);
        Ok(())
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

    pub(crate) fn retain_genesis_accounts(
        &self,
        operations: HashMap<AccountRouting, ArchiveOperation>,
    ) {
        let mut accounts = self.genesis_accounts.write();
        for operation in operations.into_values() {
            if let ArchiveOperation::UpdateOrInsert(account) = operation {
                accounts.entry(account.account_hash()).or_insert(account);
            }
        }
    }

    /// Called during startup, before the archive update service starts.
    pub(crate) fn initialize_zerostate_archive(
        &self,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
        state: &DurableThreadAccountsState,
    ) -> anyhow::Result<()> {
        let mut snapshot = ThreadSnapshot::new();
        for (routing, _) in state.map().iter() {
            let account = self
                .state_account(state, &routing)?
                .ok_or_else(|| anyhow::anyhow!("Missing zerostate account at {routing}"))?;
            snapshot.insert(routing, account);
        }
        self.archive.bootstrap_zerostate_thread(thread_id, *block_id, &snapshot)?;
        self.accumulator.set_expected_block(*thread_id, *block_id, false);
        Ok(())
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
            state.0.map.write(&mut w)?;
            w.flush()?;
            w.into_inner()?.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        // A recovery record in another directory may be persisted next.
        // Make the map's directory entry durable before publishing that record.
        File::open(path.parent().expect("account map parent"))?.sync_all()?;

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
            let map = ThreadAccountMap::read(&mut reader)?;
            tracing::trace!(target: "monit", "Loaded durable state from optimistic map file");
            Some(DurableThreadAccountsState::with_map(map))
        } else {
            None
        })
    }

    // ---- State accessors ----

    pub fn new_state() -> DurableThreadAccountsState {
        DurableThreadAccountsState::new(ThreadAccountMap::new())
    }

    pub fn state_hash(&self, state: &DurableThreadAccountsState) -> ThreadAccountsHash {
        state.0.map.hash()
    }

    // ---- Map operations ----

    pub fn state_account(
        &self,
        state: &DurableThreadAccountsState,
        routing: &AccountRouting,
    ) -> anyhow::Result<Option<ThreadAccount>> {
        Ok(match state.0.map.get(routing) {
            Some(AccountInfo::Redirect(dapp_id)) => Some(ThreadAccount::redirect(dapp_id)),
            Some(AccountInfo::AccountHash(_)) => self.find_account(state, routing)?,
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
        match state.0.map.get(routing) {
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
            AccountInfo::AccountHash(vm_hash) => {
                if let Some(account) = self.find_account_in_memory(routing, vm_hash) {
                    return Ok(account);
                }
                // 3. Archive lookup by routing — verify hash
                let started = Instant::now();
                if let Some(account) = self.archive.read_account_operation(routing)? {
                    let elapsed = started.elapsed();
                    let account_hash = account.account_hash();
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

    /// Some(None) is an authoritative accumulator removal, not a cache miss.
    fn find_account_in_memory(
        &self,
        routing: &AccountRouting,
        vm_hash: AccountHash,
    ) -> Option<Option<ThreadAccount>> {
        // Genesis bodies remain available for old states after finalization.
        if let Some(account) = self.genesis_accounts.read().get(&vm_hash) {
            return Some(Some(account.clone()));
        }
        // 1. Unfinalized pool lookup by hash — always correct
        if let Some(account) = self.accumulator.unfinalized_pool_get(&vm_hash) {
            return Some(Some(account));
        }
        // 2. Accumulator lookup by routing — verify hash
        if let Some(operation) = self.accumulator.find_account_operation(routing, &vm_hash) {
            match operation {
                ArchiveOperation::UpdateOrInsert(account) => {
                    return Some(Some(account));
                }
                ArchiveOperation::Remove => return Some(None),
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
            return Some(Some(Arc::unwrap_or_clone(account)));
        }
        if let Some((level, account)) = self.archive.account_written_cache().get(&vm_hash) {
            tracing::trace!(
                target: "mem",
                routing = %routing,
                hash = %vm_hash.to_hex_string(),
                level = ?level,
                "account written cache hit",
            );
            return Some(Some(Arc::unwrap_or_clone(account)));
        }
        tracing::trace!(
            target: "mem",
            routing = %routing,
            hash = %vm_hash.to_hex_string(),
            "account read/write cache miss",
        );
        None
    }

    // Share redirect resolution between window planning and actual lookup: the
    // budget must count the target body, never the small redirect stub.
    fn resolve_account_info(
        state: &DurableThreadAccountsState,
        mut routing: AccountRouting,
        mut info: AccountInfo,
        mut capacity: AccountCapacity,
    ) -> anyhow::Result<Option<OldAccountRequest>> {
        let mut visited = HashSet::new();
        while let AccountInfo::Redirect(dapp) = info {
            anyhow::ensure!(visited.insert(routing), "Account redirect cycle: {routing}");
            routing = routing.with_dapp_id(dapp);
            let Some((target, target_capacity)) = state.0.map.get_with_capacity(&routing) else {
                return Ok(None);
            };
            info = target;
            capacity = target_capacity;
        }
        let AccountInfo::AccountHash(hash) = info else { return Ok(None) };
        // Unknown/zero metadata must not overfill a shared prefetch window.
        let bytes = if capacity.bytes() > 0 { capacity.bytes() } else { u64::MAX };
        Ok(Some(OldAccountRequest { routing, hash, bytes }))
    }

    fn old_account_request(
        state: &DurableThreadAccountsState,
        routing: &AccountRouting,
        operation: &BlockAccountOperation,
    ) -> anyhow::Result<Option<OldAccountRequest>> {
        if !matches!(operation, BlockAccountOperation::AccountMerkleUpdate(_)) {
            return Ok(None);
        }
        let Some((info, capacity)) = state.0.map.get_with_capacity(routing) else {
            // TVM fallback is loaded by the apply worker, not prefetched.
            return Ok(None);
        };
        let resolved = Self::resolve_account_info(state, *routing, info, capacity)?;
        anyhow::ensure!(
            resolved.is_some(),
            "Missing account data for {} in durable state",
            routing
        );
        Ok(resolved)
    }

    /// Batch counterpart of find_account_with_info. Resolve redirects and all
    /// in-memory layers first, then fetch just archive misses in one request.
    /// Results remain aligned with the input, including missing redirect targets.
    fn batch_find_account_with_info(
        &self,
        state: &DurableThreadAccountsState,
        requests: &[(AccountRouting, AccountInfo)],
        pool: Option<&rayon::ThreadPool>,
    ) -> anyhow::Result<Vec<Option<ThreadAccount>>> {
        let mut results = vec![None; requests.len()];
        let mut misses = Vec::new();
        for (index, &(routing, info)) in requests.iter().enumerate() {
            if let Some(request) =
                Self::resolve_account_info(state, routing, info, AccountCapacity::default())?
            {
                misses.push((index, request.routing, request.hash));
            }
        }
        // A layer receives only unresolved requests. A resolved removal is
        // Some(None), so it stops fallback just like a resolved body.
        let mut resolve_layer =
            |misses: &mut Vec<(usize, AccountRouting, AccountHash)>,
             hits: Vec<Option<Option<ThreadAccount>>>| {
                assert_eq!(misses.len(), hits.len());
                let mut hits = hits.into_iter();
                misses.retain(|(index, _, _)| {
                    if let Some(account) = hits.next().unwrap() {
                        results[*index] = account;
                        false
                    } else {
                        true
                    }
                });
            };
        if !misses.is_empty() {
            let hashes: Vec<_> = misses.iter().map(|(_, _, hash)| *hash).collect();
            let hits = self.accumulator.unfinalized_pool_get_many(&hashes);
            resolve_layer(&mut misses, hits.into_iter().map(|hit| hit.map(Some)).collect());
        }
        if !misses.is_empty() {
            let requests: Vec<_> =
                misses.iter().map(|(_, routing, hash)| (*routing, *hash)).collect();
            let hits = self.accumulator.find_account_operations(&requests);
            resolve_layer(
                &mut misses,
                hits.into_iter()
                    .map(|hit| {
                        hit.map(|op| match op {
                            ArchiveOperation::UpdateOrInsert(account) => Some(account),
                            ArchiveOperation::Remove => None,
                        })
                    })
                    .collect(),
            );
        }
        if !misses.is_empty() {
            let hashes: Vec<_> = misses.iter().map(|(_, _, hash)| *hash).collect();
            let hits = self.account_read_cache.get_many(&hashes);
            resolve_layer(
                &mut misses,
                hits.into_iter()
                    .map(|hit| hit.map(|(_, a)| Some(Arc::unwrap_or_clone(a))))
                    .collect(),
            );
        }
        if !misses.is_empty() {
            let hashes: Vec<_> = misses.iter().map(|(_, _, hash)| *hash).collect();
            let hits = self.archive.account_written_cache().get_many(&hashes);
            resolve_layer(
                &mut misses,
                hits.into_iter()
                    .map(|hit| hit.map(|(_, a)| Some(Arc::unwrap_or_clone(a))))
                    .collect(),
            );
        }
        if !misses.is_empty() {
            let routings: Vec<_> = misses.iter().map(|(_, routing, _)| *routing).collect();
            let accounts = self.archive.read_account_operations(&routings)?;
            // Hash verification lazily decodes TVM bodies. Keep this CPU work
            // parallel for large batches, but never issue storage I/O there.
            let resolve = |((index, routing, expected_hash), account): (
                (usize, AccountRouting, AccountHash),
                Option<ThreadAccount>,
            )|
             -> anyhow::Result<(usize, ThreadAccount)> {
                let account = account
                    .filter(|account| account.account_hash() == expected_hash)
                    .ok_or(AccountHashMismatchError { routing, expected_hash })?;
                Ok((index, account))
            };
            let resolved: anyhow::Result<Vec<_>> =
                if misses.len() < PARALLEL_PREPARE_UPDATES_THRESHOLD {
                    misses.into_iter().zip(accounts).map(resolve).collect()
                } else {
                    let work = || misses.into_par_iter().zip(accounts).map(resolve).collect();
                    match pool {
                        Some(pool) => pool.install(work),
                        None => work(),
                    }
                };
            let resolved = resolved?;
            // Batch latency cannot identify an expensive individual account.
            // These old bodies are used by the current apply window only;
            // avoid polluting the read LRUs and extending their lifetime.
            // Single-account reads retain their existing latency policy.
            for (index, account) in resolved {
                results[index] = Some(account);
            }
        }
        Ok(results)
    }

    fn invalidate_account_read_cache_for_operations<'a>(
        &self,
        operations: impl Iterator<Item = &'a ArchiveOperation>,
    ) {
        let hashes: Vec<_> = operations
            .filter_map(|op| match op {
                ArchiveOperation::UpdateOrInsert(account) => Some(account.account_hash()),
                ArchiveOperation::Remove => None,
            })
            .collect();
        self.account_read_cache.remove_many(&hashes);
    }

    pub fn state_split(
        &self,
        state: &DurableThreadAccountsState,
        dapp_id_path: DAppIdentifierPath,
    ) -> anyhow::Result<(DurableThreadAccountsState, DurableThreadAccountsState)> {
        let (map_a, map_b) = state.0.map.split(dapp_id_path);
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
        let merged_map = a.0.map.graft(&b.0.map);
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
        self.state_update_with_pool(
            thread_id,
            block_height,
            old_tvm_accounts,
            state,
            accounts,
            None,
        )
    }

    pub(crate) fn state_update_with_pool(
        &self,
        thread_id: &ThreadIdentifier,
        block_height: u64,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &DurableThreadAccountsState,
        accounts: &HashMap<AccountRouting, BlockAccountOperation>,
        pool: Option<&rayon::ThreadPool>,
    ) -> anyhow::Result<(DurableThreadAccountsState, HashMap<AccountRouting, ArchiveOperation>)>
    {
        tracing::trace!(target: "monit", "Update accounts in durable state: prepare update: {}", accounts.len());
        let (account_updates, map_updates) =
            self.prepare_updates(old_tvm_accounts, state, accounts, pool)?;
        self.invalidate_account_read_cache_for_operations(account_updates.values());

        self.accumulator.unfinalized_pool_insert(*thread_id, block_height, &account_updates);
        tracing::trace!(target: "monit", "Update accounts in durable maps: {}", account_updates.len());
        // Apply only this batch's map updates to the trie (state already has old entries applied)
        let new_map = state.0.map.update(&map_updates);
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
                ArchiveOperation::UpdateOrInsert(acc) => acc.canonical_bytes().len(),
                ArchiveOperation::Remove => 0,
            })
            .sum();
        tracing::info!(
            target: "mem",
            "finalize_thread_transition T:{thread_id} B:{block_id} ops={operations_len} ops_data_bytes={ops_data_bytes}",
        );
        self.invalidate_account_read_cache_for_operations(account_operations.values());
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
        self.invalidate_account_read_cache_for_operations(account_operations.values());
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
        self.check_snapshot_pin_reachability(pin)
    }

    pub fn acquire_snapshot_pin_cancellable<F>(
        &self,
        expected_thread: node_types::ThreadIdentifier,
        expected_block: node_types::BlockIdentifier,
        timeout: std::time::Duration,
        should_cancel: F,
    ) -> Option<super::PinHandle>
    where
        F: Fn() -> bool,
    {
        let pin = self.accumulator.acquire_snapshot_pin_cancellable(
            expected_thread,
            expected_block,
            timeout,
            should_cancel,
        )?;
        self.check_snapshot_pin_reachability(pin)
    }

    fn check_snapshot_pin_reachability(&self, pin: super::PinHandle) -> Option<super::PinHandle> {
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
        for (routing, info) in map_state.0.map.iter() {
            match info {
                AccountInfo::Redirect(_) => {
                    // Redirect entries are pure map entries; no body
                    // to resolve. Continue.
                }
                AccountInfo::AccountHash(expected_hash) => {
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
    /// `AccountHash` branch, but only checks existence + hash match.
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
            if account.account_hash() == expected_hash {
                return true;
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
        pool: Option<&rayon::ThreadPool>,
    ) -> anyhow::Result<(
        HashMap<AccountRouting, ArchiveOperation>,
        Vec<(AccountRouting, Option<(AccountInfo, AccountCapacity)>)>,
    )> {
        self.prepare_updates_with_budget(
            old_tvm_accounts,
            state,
            accounts,
            pool,
            PREPARE_OLD_ACCOUNTS_BYTES,
        )
    }

    fn prepare_updates_with_budget(
        &self,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &DurableThreadAccountsState,
        accounts: &HashMap<AccountRouting, BlockAccountOperation>,
        pool: Option<&rayon::ThreadPool>,
        byte_budget: u64,
    ) -> anyhow::Result<(
        HashMap<AccountRouting, ArchiveOperation>,
        Vec<(AccountRouting, Option<(AccountInfo, AccountCapacity)>)>,
    )> {
        let protected_routings: HashSet<AccountRouting> = accounts
            .iter()
            .filter_map(|(routing, operation)| {
                (!matches!(operation, BlockAccountOperation::Remove)).then_some(*routing)
            })
            .collect();

        let operations: Vec<_> = accounts.iter().collect();
        // Resolve each leaf once, retaining its hash, capacity and redirect
        // target for the actual fetch. Windowing never repeats trie lookups.
        let old_requests = operations
            .iter()
            .map(|(routing, operation)| Self::old_account_request(state, routing, operation))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let sizes: Vec<_> = old_requests
            .iter()
            .map(|request| request.as_ref().map_or(0, |request| request.bytes))
            .collect();
        let mut prepared_updates = Vec::with_capacity(accounts.len());
        let mut start = 0;
        while start < operations.len() {
            let count = prepare_window_len(&sizes[start..], byte_budget);
            let window = &operations[start..start + count];
            // Only this window owns prefetched bodies. Storage I/O stays out
            // of Rayon; workers apply to resolved bodies before the next read.
            let requests: Vec<_> = old_requests[start..start + count]
                .iter()
                .filter_map(|request| request.as_ref())
                .map(|request| (request.routing, AccountInfo::AccountHash(request.hash)))
                .collect();
            let resolved = self.batch_find_account_with_info(state, &requests, pool)?;
            let sources = window
                .iter()
                .zip(&old_requests[start..start + count])
                .filter_map(|((routing, _), request)| request.as_ref().map(|_| **routing));
            let mut old_accounts = HashMap::with_capacity(requests.len());
            for (routing, account) in sources.zip(resolved) {
                old_accounts.insert(
                    routing,
                    account.ok_or_else(|| {
                        anyhow::anyhow!("Missing account data for {} in durable state", routing)
                    })?,
                );
            }
            let updates = if window.len() < PARALLEL_PREPARE_UPDATES_THRESHOLD {
                self.prepare_account_updates_sequential(
                    old_tvm_accounts,
                    state,
                    window,
                    &old_accounts,
                )?
            } else {
                let work = || {
                    self.prepare_account_updates_parallel(
                        old_tvm_accounts,
                        state,
                        window,
                        &old_accounts,
                    )
                };
                match pool {
                    Some(pool) => pool.install(work)?,
                    None => work()?,
                }
            };
            // New states may share cells with old bodies. Release our old
            // handles now; do not retain the whole block's old bodies here.
            drop(old_accounts);
            prepared_updates.extend(updates);
            start += count;
        }

        self.collect_prepared_updates(prepared_updates, &protected_routings, state)
    }

    fn prepare_account_updates_sequential(
        &self,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &DurableThreadAccountsState,
        accounts: &[(&AccountRouting, &BlockAccountOperation)],
        old_accounts: &HashMap<AccountRouting, ThreadAccount>,
    ) -> anyhow::Result<Vec<PreparedAccountUpdate>> {
        let mut prepared_updates = Vec::with_capacity(accounts.len());
        for &(routing, state_account) in accounts {
            tracing::trace!(target: "builder", "Update account in durable state: {}", routing);
            prepared_updates.push(PreparedAccountUpdate {
                routing: *routing,
                update: self.get_account_update(
                    routing,
                    state_account,
                    state,
                    old_tvm_accounts,
                    old_accounts,
                )?,
            });
        }
        Ok(prepared_updates)
    }

    fn prepare_account_updates_parallel(
        &self,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &DurableThreadAccountsState,
        accounts: &[(&AccountRouting, &BlockAccountOperation)],
        old_accounts: &HashMap<AccountRouting, ThreadAccount>,
    ) -> anyhow::Result<Vec<PreparedAccountUpdate>> {
        accounts
            .par_iter()
            .map(|&(routing, state_account)| {
                tracing::trace!(target: "builder", "Update account in durable state: {}", routing);
                Ok(PreparedAccountUpdate {
                    routing: *routing,
                    update: self.get_account_update(
                        routing,
                        state_account,
                        state,
                        old_tvm_accounts,
                        old_accounts,
                    )?,
                })
            })
            .collect()
    }

    fn collect_prepared_updates(
        &self,
        prepared_updates: Vec<PreparedAccountUpdate>,
        protected_routings: &HashSet<AccountRouting>,
        state: &DurableThreadAccountsState,
    ) -> anyhow::Result<(
        HashMap<AccountRouting, ArchiveOperation>,
        Vec<(AccountRouting, Option<(AccountInfo, AccountCapacity)>)>,
    )> {
        let mut account_updates = HashMap::new();
        let mut map_updates = Vec::new();
        for PreparedAccountUpdate { routing, update } in prepared_updates {
            match update {
                AccountUpdate::Redirect(info) => {
                    let AccountInfo::Redirect(dapp_id) = info else {
                        anyhow::bail!("Redirect account update has non-redirect info: {info}");
                    };
                    let account = ThreadAccount::redirect(dapp_id);
                    let capacity = AccountCapacity::new(account.canonical_size());
                    account_updates.insert(routing, ArchiveOperation::UpdateOrInsert(account));
                    map_updates.push((routing, Some((info, capacity))));
                }
                AccountUpdate::Update(info, account) => {
                    let capacity = AccountCapacity::new(account.canonical_size());
                    account_updates.insert(routing, ArchiveOperation::UpdateOrInsert(account));
                    map_updates.push((routing, Some((info, capacity))));
                }
                AccountUpdate::Remove => {
                    account_updates.insert(routing, ArchiveOperation::Remove);
                    map_updates.push((routing, None));
                    if let Some(redirect_routing) =
                        self.derived_redirect_remove_routing(&routing, state)?
                    {
                        if !protected_routings.contains(&redirect_routing) {
                            account_updates
                                .entry(redirect_routing)
                                .or_insert(ArchiveOperation::Remove);
                            map_updates.push((redirect_routing, None));
                        }
                    }
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
        old_accounts: &HashMap<AccountRouting, ThreadAccount>,
    ) -> anyhow::Result<AccountUpdate> {
        Ok(match state_account {
            BlockAccountOperation::UpdateOrInsert(state_account) => {
                Self::state_account_update(routing, state_account)?
            }
            BlockAccountOperation::Remove => {
                let has_account = state.0.map.get(routing).is_some();
                if has_account {
                    AccountUpdate::Remove
                } else {
                    AccountUpdate::Skip
                }
            }
            BlockAccountOperation::AccountMerkleUpdate(update_bytes) => self
                .apply_account_merkle_update(
                    routing,
                    update_bytes,
                    old_accounts,
                    old_tvm_accounts,
                )?,
        })
    }

    fn apply_account_merkle_update(
        &self,
        routing: &AccountRouting,
        update_bytes: &[u8],
        old_accounts: &HashMap<AccountRouting, ThreadAccount>,
        old_tvm_accounts: &tvm_block::ShardAccounts,
    ) -> anyhow::Result<AccountUpdate> {
        use tvm_block::Deserializable;

        tracing::trace!(
            target: "monit",
            "Applying account merkle update: {}, size: {}",
            routing, update_bytes.len(),);

        // Durable bodies were resolved before parallel work. TVM remains the
        // fallback only for routings absent from the durable map.
        let old_shard_acc = if let Some(account) = old_accounts.get(routing) {
            let shard_account = account.tvm_shard_account()?;
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

    fn derived_redirect_remove_routing(
        &self,
        routing: &AccountRouting,
        state: &DurableThreadAccountsState,
    ) -> anyhow::Result<Option<AccountRouting>> {
        let redirect_routing = routing.account_id().redirect();
        if *routing == redirect_routing {
            return Ok(None);
        }

        match state.0.map.get(&redirect_routing) {
            Some(AccountInfo::Redirect(_)) => Ok(Some(redirect_routing)),
            Some(AccountInfo::AccountHash(_)) => anyhow::bail!(
                "Expected redirect stub at default routing {}, found a real account while removing {}",
                redirect_routing,
                routing
            ),
            None => Ok(None),
        }
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
        Ok(AccountUpdate::Update(AccountInfo::AccountHash(account.account_hash()), account.clone()))
    }

    pub(crate) fn apply_pool(&self) -> anyhow::Result<&'static rayon::ThreadPool> {
        super::apply_pool::get()
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
        self.state_update_with_pool(
            thread_id,
            block_height,
            old_tvm_accounts,
            state,
            &diff.accounts,
            Some(super::apply_pool::get()?),
        )
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

    pub fn export_durable_snapshot_to_writer<W: Write + Seek + Send>(
        &self,
        state: &DurableThreadAccountsState,
        writer: &mut W,
    ) -> anyhow::Result<()> {
        self.export_durable_snapshot_to_writer_checked(state, writer, &|| false)
    }

    /// Read a partition in canonical Merkle-map order without scanning storage.
    /// The caller must hold the whole-thread snapshot pin for this map's block
    /// throughout the call. The epoch guard below prevents imported/reset sets
    /// from being reclaimed; it does not replace that block-boundary pin.
    /// The pinned archive is authoritative: do not resolve bodies from the
    /// unfinalized pool or pending accumulator.
    ///
    /// Only a bounded window of batches is materialized. A callback error or
    /// cancellation stops subsequent output; the caller must discard any partial
    /// blob. No wire format is chosen here and existing snapshot export is unchanged.
    pub fn read_partition_accounts_checked<F, C>(
        &self,
        map: &super::ThreadAccountMap,
        limits: super::PartitionReadLimits,
        should_cancel: &C,
        on_account: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(AccountRouting, &ThreadAccount) -> anyhow::Result<()>,
        C: Fn() -> bool + Sync,
    {
        self.read_partition_output_checked(
            map,
            limits,
            should_cancel,
            std::convert::identity,
            on_account,
        )
    }

    pub(super) fn read_partition_canonical_checked<F, C>(
        &self,
        map: &super::ThreadAccountMap,
        limits: super::PartitionReadLimits,
        should_cancel: &C,
        mut on_account: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(AccountRouting, &[u8]) -> anyhow::Result<()>,
        C: Fn() -> bool + Sync,
    {
        self.read_partition_output_checked(
            map,
            limits,
            should_cancel,
            ThreadAccount::into_canonical_bytes,
            |routing, bytes| on_account(routing, bytes),
        )
    }

    fn read_partition_output_checked<F, C, P, O>(
        &self,
        map: &super::ThreadAccountMap,
        limits: super::PartitionReadLimits,
        should_cancel: &C,
        prepare_output: O,
        on_account: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(AccountRouting, &P) -> anyhow::Result<()>,
        C: Fn() -> bool + Sync,
        P: Send,
        O: Fn(ThreadAccount) -> P + Sync,
    {
        let epoch = self.archive.pin_current_account_sets();
        let set = self.archive.set_accounts_a_for_epoch(epoch.epoch());
        super::ordered_export::read_ordered(
            map,
            limits,
            |batch| {
                let mut accounts = vec![None; batch.len()];
                let mut stored_bodies = Vec::new();
                let mut positions = Vec::new();
                let mut keys = Vec::new();
                for (index, entry) in batch.iter().enumerate() {
                    check_snapshot_cancelled(should_cancel)?;
                    accounts[index] = match entry.info {
                        AccountInfo::Redirect(dapp) => Some(ThreadAccount::redirect(dapp)),
                        AccountInfo::AccountHash(_) => None,
                    };
                    if accounts[index].is_none() {
                        positions.push(index);
                        keys.push(super::archive::apply::routing_to_key(&entry.routing));
                    }
                }
                if !keys.is_empty() {
                    let records = self.archive.store().get(&set, &keys)?;
                    anyhow::ensure!(
                        records.len() == keys.len(),
                        "partition storage result length mismatch"
                    );
                    for ((index, key), record) in positions.into_iter().zip(keys).zip(records) {
                        check_snapshot_cancelled(should_cancel)?;
                        let record = record.ok_or_else(|| {
                            anyhow::anyhow!("partition account missing: {}", batch[index].routing)
                        })?;
                        anyhow::ensure!(record.key == key, "partition storage returned wrong key");
                        stored_bodies.push((index, record.data));
                    }
                }
                Ok((accounts, stored_bodies))
            },
            |batch, (mut accounts, stored_bodies)| {
                for (index, data) in stored_bodies {
                    check_snapshot_cancelled(should_cancel)?;
                    let canonical: Vec<u8> = bincode::deserialize(&data)?;
                    accounts[index] = Some(
                        // Archive bodies are canonicalized at ingress. The
                        // ordered reader still verifies hash and size, but
                        // need not re-encode trusted storage as a BOC.
                        ThreadAccount::validate_tvm_bytes(canonical.into_boxed_slice()).map_err(
                            |error| {
                                anyhow::anyhow!(
                                    "partition account {}: {error:#}",
                                    batch[index].routing
                                )
                            },
                        )?,
                    );
                }
                Ok(accounts
                    .into_iter()
                    .map(|account| account.expect("resolved batch account"))
                    .collect())
            },
            prepare_output,
            should_cancel,
            on_account,
        )
    }

    pub fn export_durable_snapshot_to_writer_checked<W, F>(
        &self,
        state: &DurableThreadAccountsState,
        writer: &mut W,
        should_cancel: &F,
    ) -> anyhow::Result<()>
    where
        W: Write + Seek + Send,
        F: Fn() -> bool,
    {
        let export_started = Instant::now();
        let _span = tracing::info_span!(target: "monit", "snapshot_account_export").entered();
        tracing::info!(target: "monit", canonical_bytes = state.0.map.capacity().bytes(),
            "Snapshot account export started");
        check_snapshot_cancelled(should_cancel)?;
        let serialized_account_count = count_serialized_map_leaves(&state.0.map);
        let pinned_account_sets = self.archive.pin_current_account_sets();
        let pinned_epoch_id = pinned_account_sets.epoch();
        let pinned_accounts_set = self.archive.set_accounts_a_for_epoch(pinned_epoch_id);
        let mut buffered_writer =
            BufWriter::with_capacity(Self::SNAPSHOT_COPY_BUFFER_CAPACITY, writer);
        let maps_len_pos = buffered_writer.stream_position()?;
        buffered_writer.write_all(&0u64.to_le_bytes())?;
        let maps_start_pos = buffered_writer.stream_position()?;
        state.0.map.write_checked(&mut buffered_writer, should_cancel)?;
        let maps_end_pos = buffered_writer.stream_position()?;
        let maps_len = maps_end_pos - maps_start_pos;

        check_snapshot_cancelled(should_cancel)?;
        let accounts_count_pos = buffered_writer.stream_position()?;
        buffered_writer.write_all(&0u64.to_le_bytes())?;
        let header_ms = export_started.elapsed().as_millis() as u64;
        tracing::info!(target: "monit", header_ms, "Snapshot account-map region prepared");
        let scan_started = Instant::now();
        tracing::info!(target: "monit", validation_workers = snapshot_export_validation_worker_count(),
            "Snapshot account scan started");
        let (mut buffered_writer, mut written, mut accounts_count) =
            std::thread::scope(|scope| -> anyhow::Result<_> {
                let queue_batches = snapshot_export_validation_queue_batches();
                let (result_tx, result_rx) = crossbeam_channel::bounded::<
                    SnapshotExportValidationBatchResult,
                >(queue_batches);
                let mut export_validation = ExportValidationPipeline::new(result_tx)?;
                let writer_handle = scope.spawn({
                    let span = tracing::Span::current();
                    move || {
                        let _span = span.enter();
                        write_export_validation_results(buffered_writer, result_rx)
                    }
                });
                let mut validation_batch =
                    Vec::with_capacity(SNAPSHOT_EXPORT_VALIDATION_BATCH_SIZE);

                // Fast path: enumerate the logical archive records and accept each
                // payload only if it validates against the snapshot map. Enumeration
                // is a source of candidate bytes; the map remains authoritative.
                self.archive.store().enumerate_checked(
                    &pinned_accounts_set,
                    should_cancel,
                    &mut |record| -> anyhow::Result<()> {
                        check_snapshot_cancelled(should_cancel)?;
                        let routing = key_to_routing(&record.key)?;
                        let Some(info) = state.0.map.get(&routing) else {
                            return Ok(());
                        };
                        validation_batch.push((routing, info, record.data));
                        if validation_batch.len() >= SNAPSHOT_EXPORT_VALIDATION_BATCH_SIZE {
                            let batch = std::mem::replace(
                                &mut validation_batch,
                                Vec::with_capacity(SNAPSHOT_EXPORT_VALIDATION_BATCH_SIZE),
                            );
                            export_validation.submit(batch)?;
                        }
                        Ok(())
                    },
                )?;
                if !validation_batch.is_empty() {
                    export_validation.submit(validation_batch)?;
                }
                export_validation.finish()?;
                writer_handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("Export validation writer panicked"))?
            })?;
        let scan_ms = scan_started.elapsed().as_millis() as u64;
        let fallback_started = Instant::now();
        let scanned_accounts = accounts_count;
        tracing::info!(target: "monit", scan_ms, scanned_accounts,
            "Snapshot account scan finished (includes validation and body writes)");

        // Fallback path: any map entry not satisfied by archive enumeration
        // must still be exported from an exact hash-matching source, or the
        // snapshot is skipped by returning an error.
        let mut expected_len = 0;
        for (routing, info) in state.0.map.iter() {
            expected_len += 1;
            check_snapshot_cancelled(should_cancel)?;
            if written.contains(&routing) {
                continue;
            }
            let body = self
                .resolve_body_for_export_with_info_at_epoch(&routing, *info, pinned_epoch_id)
                .ok_or_else(|| {
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
        assert_eq!(
            serialized_account_count, expected_len,
            "snapshot export consistency violated: independent trie walk found {} account \
             leaves in the serialized map, but `map_iter` produced {}. The map that gets \
             serialized via `map_write` and the entries `map_iter` visits are out of sync \
             — `map_iter` likely has a missing-leaf bug. Aborting.",
            serialized_account_count, expected_len,
        );

        check_snapshot_cancelled(should_cancel)?;
        drop(written);
        let fallback_ms = fallback_started.elapsed().as_millis() as u64;
        let trailer_started = Instant::now();
        buffered_writer.flush()?;
        buffered_writer.seek(SeekFrom::Start(maps_len_pos))?;
        buffered_writer.write_all(&maps_len.to_le_bytes())?;
        buffered_writer.seek(SeekFrom::Start(accounts_count_pos))?;
        buffered_writer.write_all(&accounts_count.to_le_bytes())?;
        buffered_writer.seek(SeekFrom::End(0))?;
        buffered_writer.flush()?;
        let trailer_ms = trailer_started.elapsed().as_millis() as u64;
        let cleanup_started = Instant::now();
        drop(buffered_writer);
        let cleanup_ms = cleanup_started.elapsed().as_millis() as u64;
        tracing::info!(
            target: "monit",
            accounts_count,
            scanned_accounts,
            header_ms,
            scan_ms,
            fallback_ms,
            trailer_ms,
            cleanup_ms,
            total_ms = export_started.elapsed().as_millis() as u64,
            "Snapshot account export stages (scan includes validation and body writes; fallback includes coverage checks; excludes compression)"
        );
        Ok(())
    }

    /// Same fallback chain as `find_account_with_info`'s `AccountHash`
    /// branch, returning the resolved account body from the pinned archive
    /// epoch. None if the body can't be produced — caller treats this as an
    /// unrecoverable snapshot failure (atomic skip).
    fn resolve_body_for_export_at_epoch(
        &self,
        routing: &AccountRouting,
        expected_hash: AccountHash,
        epoch: u64,
    ) -> Option<ThreadAccount> {
        if let Some(account) = self.genesis_accounts.read().get(&expected_hash) {
            return Some(account.clone());
        }
        if let Some(account) = self.accumulator.unfinalized_pool_get(&expected_hash) {
            return Some(account);
        }
        if let Some(operation) = self.accumulator.find_account_operation(routing, &expected_hash) {
            if let crate::thread_accounts::ArchiveOperation::UpdateOrInsert(account) = operation {
                return Some(account.clone());
            }
            return None;
        }
        if let Ok(Some(account)) = self.archive.read_account_operation_at_epoch(routing, epoch) {
            if account.account_hash() == expected_hash {
                return Some(account);
            }
        }
        None
    }

    fn resolve_body_for_export_with_info_at_epoch(
        &self,
        routing: &AccountRouting,
        info: AccountInfo,
        epoch: u64,
    ) -> Option<ThreadAccount> {
        match info {
            AccountInfo::Redirect(dapp_id) => Some(ThreadAccount::redirect(dapp_id)),
            AccountInfo::AccountHash(expected_hash) => {
                self.resolve_body_for_export_at_epoch(routing, expected_hash, epoch)
            }
        }
    }

    pub fn import_durable_snapshot_from_reader<R: Read>(
        &self,
        reader: &mut R,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<DurableThreadAccountsState> {
        self.import_durable_snapshot_from_reader_inner(reader, thread_id, block_id, None)
    }

    pub fn begin_staged_import_after_current_epoch(&self) -> StagedArchiveEpoch {
        self.archive.begin_staged_epoch_after_current()
    }

    pub fn abort_staged_import(&self, staged: StagedArchiveEpoch) {
        self.archive.truncate_epoch_best_effort(staged.epoch);
    }

    /// Append a bounded batch of already validated canonical snapshot bodies to
    /// both copies of a staged epoch. Does not publish controls or reset the
    /// accumulator. The import owner must serialize imports, verify the complete
    /// snapshot, and abort this epoch on any later error before committing it.
    pub fn append_staged_snapshot_accounts<I>(
        &self,
        staged: StagedArchiveEpoch,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
        entries: I,
    ) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = anyhow::Result<(AccountRouting, Vec<u8>)>>,
    {
        self.archive.thread_init_from_raw_entries_at_epoch(
            thread_id,
            *block_id,
            entries,
            staged.epoch,
            false,
        )?;
        Ok(())
    }

    pub fn commit_staged_import(
        &self,
        staged: StagedArchiveEpoch,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<()> {
        self.archive.commit_staged_epoch(staged, thread_id, block_id)?;
        let control_states = self.archive.get_all_thread_control_states()?;
        self.accumulator.reset(&control_states);
        Ok(())
    }

    /// Startup-only recovery; the archive must have been reopened from KV first.
    pub fn recover_staged_import(
        &self,
        staged: StagedArchiveEpoch,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<bool> {
        if !self.archive.recover_staged_epoch(staged, thread_id, block_id)? {
            return Ok(false);
        }
        self.accumulator.reset(&self.archive.get_all_thread_control_states()?);
        Ok(true)
    }

    pub fn import_durable_snapshot_from_reader_staged<R: Read>(
        &self,
        reader: &mut R,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
        staged: StagedArchiveEpoch,
    ) -> anyhow::Result<DurableThreadAccountsState> {
        self.import_durable_snapshot_from_reader_inner(reader, thread_id, block_id, Some(staged))
    }

    fn import_durable_snapshot_from_reader_inner<R: Read>(
        &self,
        reader: &mut R,
        thread_id: &ThreadIdentifier,
        block_id: &BlockIdentifier,
        staged: Option<StagedArchiveEpoch>,
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
        let map = ThreadAccountMap::read(&mut maps_reader)?;
        std::io::copy(&mut maps_reader, &mut std::io::sink())?;

        reader.read_exact(&mut len_buf)?;
        let accounts_len = u64::from_le_bytes(len_buf);
        tracing::debug!(
            "import_durable_snapshot_from_reader: read map, accounts_len={accounts_len}"
        );

        let mut remaining = accounts_len;
        // Track hashes while streaming bodies into the archive. Hash extraction is
        // done in a bounded side pipeline: Aerospike import remains on this main
        // path, but slow hash workers naturally backpressure snapshot reading.
        let mut written_pipeline = WrittenHashPipeline::new()?;
        let mut written_hash_batch =
            Vec::<(AccountRouting, Vec<u8>)>::with_capacity(SNAPSHOT_WRITTEN_HASH_BATCH_SIZE);
        let raw_account_entries = std::iter::from_fn(|| {
            if remaining == 0 {
                if !written_hash_batch.is_empty() {
                    let batch = std::mem::take(&mut written_hash_batch);
                    if let Err(err) = written_pipeline.submit(batch) {
                        return Some(Err(err));
                    }
                }
                return None;
            }
            remaining -= 1;
            match read_snapshot_account_entry(&mut reader) {
                Ok((routing, bytes)) => {
                    written_hash_batch.push((routing, bytes.clone()));
                    if written_hash_batch.len() >= SNAPSHOT_WRITTEN_HASH_BATCH_SIZE {
                        let batch = std::mem::take(&mut written_hash_batch);
                        if let Err(err) = written_pipeline.submit(batch) {
                            return Some(Err(err));
                        }
                    }
                    Some(Ok((routing, bytes)))
                }
                Err(err) => Some(Err(err)),
            }
        });
        if let Some(staged) = staged {
            self.archive.thread_init_from_raw_entries_at_epoch(
                thread_id,
                *block_id,
                raw_account_entries,
                staged.epoch,
                false,
            )?;
        } else {
            self.archive.thread_init_from_raw_entries(thread_id, *block_id, raw_account_entries)?;
            self.accumulator.set_expected_block(*thread_id, *block_id, true);
        }
        let written_infos = written_pipeline.finish()?;
        // Capacity is not committed by the Merkle hash: authenticate leaf
        // metadata against the body stream before accepting the imported map.
        for entry in &written_infos {
            anyhow::ensure!(
                map.account_capacity(&entry.routing).map(AccountCapacity::bytes)
                    == Some(entry.canonical_size),
                "Snapshot canonical account size does not match map capacity for {}",
                entry.routing,
            );
        }
        let written: HashMap<AccountRouting, AccountHash> =
            HashMap::from_iter(written_infos.into_iter().map(|entry| (entry.routing, entry.hash)));

        // Defense-in-depth: do the work that block verification would
        // later do, but eagerly during import while we're still in the
        // import call frame and can bail cleanly. Walk the imported map;
        // for each VM entry, first compare against the hash captured from
        // the snapshot body stream. If the stream did not provide a hash
        // for that routing, fall back to the old archive resolution path.
        // This keeps the compatibility/safety net without making the
        // normal path read every imported body back from durable storage.
        //
        // This catches three distinct producer-side bugs *on the consumer*,
        // regardless of which build the producer ran:
        //
        //   1. **Missing body**: the snapshot's map references a routing
        //      whose body was never written to the snapshot. Detected first
        //      because the routing is absent from `written`, then confirmed
        //      through the archive fallback.
        //
        //   2. **Wrong-hash body**: a body was written but its decoded
        //      account hash doesn't match the map's expected hash.
        //
        //   3. **Map-iter divergence**: producer's `map_iter` undercounted
        //      vs `map_write`, so the deserialized map has more leaves
        //      than the bodies. Caught by the count check below.
        //
        // The fresh state is used only for fallback misses from `written`.
        let import_state = DurableThreadAccountsState::with_map(map.clone());
        let mut missing_bodies: Vec<AccountRouting> = Vec::new();
        let mut wrong_hashes: Vec<(AccountRouting, AccountHash, AccountHash)> = Vec::new();
        let mut total_in_map: usize = 0;
        for (routing, info) in map.iter() {
            total_in_map += 1;
            match info {
                AccountInfo::Redirect(_) => {
                    // Redirects don't have a hash to verify; the consumer
                    // resolves them by following to a different routing,
                    // and that target routing (if it has AccountHash)
                    // is checked elsewhere in this loop.
                }
                AccountInfo::AccountHash(expected_hash) => match written.get(&routing) {
                    Some(actual) => {
                        if actual != expected_hash {
                            wrong_hashes.push((routing, *expected_hash, *actual));
                        }
                    }
                    None => match if let Some(staged) = staged {
                        self.archive
                            .read_account_operation_at_epoch(&routing, staged.epoch)
                            .map_err(anyhow::Error::from)
                    } else {
                        self.find_account_with_info(&import_state, &routing, *info)
                    } {
                        Ok(Some(account)) => {
                            anyhow::ensure!(
                                map.account_capacity(&routing).map(AccountCapacity::bytes)
                                    == Some(account.canonical_size()),
                                "Snapshot canonical account size does not match map capacity for {}",
                                routing,
                            );
                            let actual = account.account_hash();
                            if actual != *expected_hash {
                                wrong_hashes.push((routing, *expected_hash, actual));
                            }
                        }
                        Ok(None) => missing_bodies.push(routing),
                        Err(_) => missing_bodies.push(routing),
                    },
                },
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
                 leaves, accounts section tracked VM bodies for {} routings, {} routings have NO body, \
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

// Metadata only: body ownership is acquired later, for the current window.
struct OldAccountRequest {
    routing: AccountRouting,
    hash: AccountHash,
    bytes: u64,
}

struct PreparedAccountUpdate {
    routing: AccountRouting,
    update: AccountUpdate,
}

#[cfg(test)]
mod tests {
    include!("ordered_export_tests.rs");
    use std::collections::HashMap;
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
    use super::AccountWrittenCache;
    use super::CacheLevel;
    use crate::thread_accounts::durable::archive::update::AccumulatedUpdate;
    use crate::thread_accounts::durable::AccountInfo;
    use crate::ArchiveOperation;
    use crate::ArchiveStateStore;
    use crate::DurableThreadAccountsRepository;
    use crate::DurableThreadAccountsState;
    use crate::ThreadAccount;
    use crate::ThreadAccountMap;
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

    fn new_acc_with_dapp(seed: usize, dapp_seed: usize) -> (AccountRouting, ThreadAccount) {
        let id = AccountIdentifier::new(new_u256("acc", seed));
        let dapp_id = DAppIdentifier::new(new_u256("dapp", dapp_seed));
        let routing = id.routing(dapp_id);
        let tvm_acc = tvm_block::Account::with_address(
            MsgAddressInt::with_standart(None, 0, AccountId::from_raw(id.as_slice().to_vec(), 256))
                .unwrap(),
        );
        let tvm_shard_acc = ShardAccount::with_params(
            &tvm_acc,
            new_u256("trans", seed).into(),
            seed as u64,
            Some(dapp_id.as_array().into()),
        )
        .unwrap();
        let acc = ThreadAccount::from(tvm_shard_acc);
        (routing, acc)
    }

    fn account_hash(account: &ThreadAccount) -> AccountHash {
        account.account_hash()
    }

    fn state_with_account_hash(
        routing: AccountRouting,
        hash: AccountHash,
    ) -> DurableThreadAccountsState {
        let map = (DurableThreadAccountsRepository::new_state().0.map).update(&[(
            routing,
            Some((AccountInfo::AccountHash(hash), super::AccountCapacity::new(0))),
        )]);
        DurableThreadAccountsState::with_map(map)
    }

    #[test]
    fn batch_lookup_resolves_each_memory_level_without_storage() -> anyhow::Result<()> {
        use crate::thread_accounts::durable::kv_store::KVStore;
        let dir = tempfile::tempdir()?;
        let repo = DurableThreadAccountsRepository::new(
            dir.path().into(),
            ArchiveStateStore::in_memory(),
            None,
        )?;
        let accounts: Vec<_> = (900..905).map(new_acc).collect();
        repo.accumulator.unfinalized_pool_insert(
            ThreadIdentifier::default(),
            1,
            &HashMap::from([(
                accounts[0].0,
                ArchiveOperation::UpdateOrInsert(accounts[0].1.clone()),
            )]),
        );
        repo.account_read_cache
            .insert_l1(accounts[1].1.account_hash(), Arc::new(accounts[1].1.clone()));
        repo.account_read_cache
            .insert_l2(accounts[2].1.account_hash(), Arc::new(accounts[2].1.clone()));
        let written = repo.archive.account_written_cache();
        written.insert_l1(accounts[3].1.account_hash(), Arc::new(accounts[3].1.clone()));
        written.insert_l2(accounts[4].1.account_hash(), Arc::new(accounts[4].1.clone()));
        let requests: Vec<_> = [4, 0, 2, 1, 3, 0]
            .iter()
            .map(|&i| (accounts[i].0, AccountInfo::AccountHash(accounts[i].1.account_hash())))
            .collect();
        let KVStore::InMemory(kv) = repo.archive.store() else { unreachable!() };
        let before = kv.get_call_count();
        let results = repo.batch_find_account_with_info(
            &DurableThreadAccountsRepository::new_state(),
            &requests,
            None,
        )?;
        assert_eq!(kv.get_call_count(), before, "fully cached batch must not reach storage");
        for ((_, info), account) in requests.iter().zip(results) {
            assert_eq!(*info, AccountInfo::AccountHash(account.unwrap().account_hash()));
        }
        Ok(())
    }

    #[test]
    fn batch_invalidation_removes_both_cache_levels() {
        let cache = AccountReadCache::new();
        let accounts: Vec<_> = (910..914).map(|i| new_acc(i).1).collect();
        for account in &accounts {
            cache.insert_l1(account.account_hash(), Arc::new(account.clone()));
            cache.insert_l2(account.account_hash(), Arc::new(account.clone()));
        }
        let hashes: Vec<_> = accounts.iter().map(ThreadAccount::account_hash).collect();
        cache.remove_many(&hashes);
        assert!(cache.l1.lock().is_empty());
        assert!(cache.l2.lock().is_empty());
    }

    #[test]
    fn slow_cold_batch_does_not_fill_read_cache() -> anyhow::Result<()> {
        use crate::thread_accounts::durable::kv_store::KVRecord;
        use crate::thread_accounts::durable::kv_store::KVStore;

        let dir = tempfile::tempdir()?;
        let repo = DurableThreadAccountsRepository::new(
            dir.path().into(),
            ArchiveStateStore::in_memory(),
            None,
        )?;
        let KVStore::InMemory(kv) = repo.archive.store() else { unreachable!() };
        let accounts: Vec<_> = (920..952).map(new_acc).collect();
        let records = accounts
            .iter()
            .map(|(routing, account)| {
                Ok(KVRecord {
                    key: super::super::archive::apply::routing_to_key(routing),
                    generation: 0,
                    data_epoch: Some(0),
                    data: bincode::serialize(account)?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        // Write directly to storage, bypassing all account caches.
        kv.put(&repo.archive.set_accounts_a(), records, false)?;
        let requests: Vec<_> = accounts
            .iter()
            .map(|(routing, account)| (*routing, AccountInfo::AccountHash(account.account_hash())))
            .collect();
        let state = DurableThreadAccountsRepository::new_state();
        let before = kv.get_call_count();
        for iteration in 1..=2 {
            // The previous policy would promote the entire batch to L1.
            kv.delay_next_get(super::ACCOUNT_READ_CACHE_L1_THRESHOLD + Duration::from_millis(5));
            let results = repo.batch_find_account_with_info(&state, &requests, None)?;
            for ((_, expected), actual) in accounts.iter().zip(results) {
                assert_eq!(actual.unwrap().account_hash(), expected.account_hash());
            }
            assert!(repo.account_read_cache.l1.lock().is_empty());
            assert!(repo.account_read_cache.l2.lock().is_empty());
            assert_eq!(kv.get_call_count() - before, iteration);
        }
        Ok(())
    }

    #[test]
    fn merkle_apply_300_cold_accounts_uses_one_storage_batch() -> anyhow::Result<()> {
        check_merkle_apply_cold_accounts(None, 1..=1)
    }

    #[test]
    fn merkle_apply_oversized_accounts_use_separate_windows() -> anyhow::Result<()> {
        check_merkle_apply_cold_accounts(Some(1), 300..=300)
    }

    #[test]
    fn merkle_apply_multiple_byte_windows_preserve_all_account_hashes() -> anyhow::Result<()> {
        check_merkle_apply_cold_accounts(Some(4096), 2..=299)
    }

    fn check_merkle_apply_cold_accounts(
        byte_budget: Option<u64>,
        expected_reads: std::ops::RangeInclusive<usize>,
    ) -> anyhow::Result<()> {
        use tvm_block::Serializable;

        use crate::thread_accounts::durable::kv_store::KVRecord;
        use crate::thread_accounts::durable::kv_store::KVStore;

        let dir = tempfile::tempdir()?;
        let archive = ArchiveStateStore::in_memory();
        let repo = DurableThreadAccountsRepository::new(dir.path().into(), archive, None)?;
        let KVStore::InMemory(kv) = repo.archive.store() else { unreachable!() };
        let mut records = Vec::new();
        let mut map_updates = Vec::new();
        let mut operations = HashMap::new();
        let mut expected = HashMap::new();
        for seed in 0..300 {
            let (routing, account) = new_acc(seed);
            let cell = account.tvm_shard_account()?.serialize().unwrap();
            let update = tvm_block::MerkleUpdate::create(&cell, &cell).unwrap();
            operations.insert(
                routing,
                super::BlockAccountOperation::AccountMerkleUpdate(
                    tvm_types::write_boc(&update.serialize().unwrap()).unwrap(),
                ),
            );
            map_updates.push((
                routing,
                Some((
                    AccountInfo::AccountHash(account.account_hash()),
                    super::AccountCapacity::new(account.canonical_size()),
                )),
            ));
            records.push(KVRecord {
                key: super::super::archive::apply::routing_to_key(&routing),
                generation: 0,
                data_epoch: Some(0),
                data: bincode::serialize(&account)?,
            });
            expected.insert(routing, account.account_hash());
        }
        // Bypass write caches to exercise actual cold-storage misses.
        kv.put(&repo.archive.set_accounts_a(), records, false)?;
        let state = DurableThreadAccountsState::with_map(
            DurableThreadAccountsRepository::new_state().0.map.update(&map_updates),
        );
        let before = kv.get_call_count();
        let (new_state, writes) = if let Some(budget) = byte_budget {
            let (writes, map_updates) = repo.prepare_updates_with_budget(
                &tvm_block::ShardAccounts::default(),
                &state,
                &operations,
                None,
                budget,
            )?;
            (DurableThreadAccountsState::with_map(state.0.map.update(&map_updates)), writes)
        } else {
            repo.state_apply_diff(
                &ThreadIdentifier::default(),
                1,
                &tvm_block::ShardAccounts::default(),
                &state,
                &super::DurableThreadAccountsStateDiff { accounts: operations },
            )?
        };
        let reads = kv.get_call_count() - before;
        assert!(
            expected_reads.contains(&reads),
            "unexpected cold storage batch count: {reads}, expected {expected_reads:?}"
        );
        assert_eq!(new_state.0.map.hash(), state.0.map.hash());
        assert_eq!(writes.len(), 300);
        for (routing, hash) in expected {
            assert_eq!(new_state.0.map.get(&routing), Some(AccountInfo::AccountHash(hash)));
        }
        Ok(())
    }

    #[test]
    fn preparation_windows_follow_byte_budget_and_isolate_oversized_accounts() {
        assert_eq!(super::prepare_window_len(&[], 100), 0);
        assert_eq!(super::prepare_window_len(&[10; 300], 3000), 300);
        assert_eq!(super::prepare_window_len(&[40, 60, 1], 100), 2);
        assert_eq!(super::prepare_window_len(&[40, 101, 1], 100), 1);
        assert_eq!(super::prepare_window_len(&[101, 1], 100), 1);
        assert_eq!(super::prepare_window_len(&[u64::MAX, 1], 100), 1);
    }

    #[test]
    fn preparation_budget_uses_redirect_target_size() -> anyhow::Result<()> {
        let (routing, account) = new_acc_with_dapp(710, 711);
        let alias = routing.account_id().redirect();
        let target_size = super::PREPARE_OLD_ACCOUNTS_BYTES + 1;
        let state = DurableThreadAccountsState::with_map(ThreadAccountMap::new().update(&[
            (
                routing,
                Some((
                    AccountInfo::AccountHash(account.account_hash()),
                    super::AccountCapacity::new(target_size),
                )),
            ),
            (
                alias,
                Some((AccountInfo::Redirect(*routing.dapp_id()), super::AccountCapacity::new(1))),
            ),
        ]));
        let update = super::BlockAccountOperation::AccountMerkleUpdate(Vec::new());
        assert_eq!(
            DurableThreadAccountsRepository::old_account_request(&state, &alias, &update)?
                .unwrap()
                .bytes,
            target_size
        );
        assert_eq!(
            DurableThreadAccountsRepository::old_account_request(&state, &routing, &update)?
                .unwrap()
                .bytes,
            target_size
        );
        assert_eq!(
            DurableThreadAccountsRepository::old_account_request(
                &state,
                &routing,
                &super::BlockAccountOperation::Remove
            )?
            .map(|request| request.bytes),
            None
        );
        Ok(())
    }

    #[test]
    fn batch_lookup_preserves_cache_redirect_and_hash_checks() -> anyhow::Result<()> {
        use crate::thread_accounts::durable::kv_store::KVRecord;
        use crate::thread_accounts::durable::kv_store::KVStore;
        let dir = tempfile::tempdir()?;
        let archive = ArchiveStateStore::in_memory();
        let repo = DurableThreadAccountsRepository::new(dir.path().into(), archive, None)?;
        let KVStore::InMemory(kv) = repo.archive.store() else { unreachable!() };
        let (routing, account) = new_acc_with_dapp(710, 711);
        let (cached_routing, cached) = new_acc(712);
        let alias = routing.account_id().redirect();
        let hash = account.account_hash();
        kv.put(
            &repo.archive.set_accounts_a(),
            vec![KVRecord {
                key: super::super::archive::apply::routing_to_key(&routing),
                generation: 0,
                data_epoch: Some(0),
                data: bincode::serialize(&account)?,
            }],
            false,
        )?;
        repo.account_read_cache.insert_l1(cached.account_hash(), Arc::new(cached.clone()));
        let state = DurableThreadAccountsState::with_map(
            DurableThreadAccountsRepository::new_state().0.map.update(&[
                (routing, Some((AccountInfo::AccountHash(hash), super::AccountCapacity::new(0)))),
                (
                    alias,
                    Some((
                        AccountInfo::Redirect(*routing.dapp_id()),
                        super::AccountCapacity::new(0),
                    )),
                ),
            ]),
        );
        let before = kv.get_call_count();
        let result = repo.batch_find_account_with_info(
            &state,
            &[
                (alias, AccountInfo::Redirect(*routing.dapp_id())),
                (cached_routing, AccountInfo::AccountHash(cached.account_hash())),
                (routing, AccountInfo::AccountHash(hash)),
            ],
            None,
        )?;
        assert_eq!(kv.get_call_count() - before, 1);
        assert_eq!(
            result.iter().map(|a| a.as_ref().unwrap().account_hash()).collect::<Vec<_>>(),
            vec![hash, cached.account_hash(), hash]
        );
        // A genuinely unresolved hash must still reject the stored version.
        let unknown = AccountHash::new(new_u256("unknown", 713));
        let error = repo
            .batch_find_account_with_info(
                &state,
                &[(routing, AccountInfo::AccountHash(unknown))],
                None,
            )
            .unwrap_err();
        assert!(error.downcast_ref::<super::AccountHashMismatchError>().is_some());
        assert!(repo.account_read_cache.get(&unknown).is_none());
        Ok(())
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

    fn durable_snapshot_bytes_with_entries(
        state: &DurableThreadAccountsState,
        entries: &[(AccountRouting, Vec<u8>)],
    ) -> Vec<u8> {
        let mut snapshot = Cursor::new(Vec::new());
        std::io::Write::write_all(&mut snapshot, &0u64.to_le_bytes()).unwrap();
        let maps_start = snapshot.position();
        state.0.map.write(&mut snapshot).unwrap();
        let maps_end = snapshot.position();
        std::io::Write::write_all(&mut snapshot, &(entries.len() as u64).to_le_bytes()).unwrap();
        for (routing, account_bytes) in entries {
            write_snapshot_account_entry(&mut snapshot, routing, account_bytes).unwrap();
        }
        let end = snapshot.position();
        snapshot.set_position(0);
        std::io::Write::write_all(&mut snapshot, &(maps_end - maps_start).to_le_bytes()).unwrap();
        snapshot.set_position(end);
        snapshot.into_inner()
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
    fn account_written_cache_inserts_into_l1_when_serialized_size_exceeds_1mib() {
        let cache = AccountWrittenCache::new();
        let (_, account) = new_acc(211);
        let hash = account_hash(&account);

        let level = cache
            .insert_by_serialized_len(
                hash,
                &account,
                super::ACCOUNT_WRITTEN_CACHE_L1_THRESHOLD_BYTES + 1,
            )
            .unwrap();

        assert_eq!(level, CacheLevel::L1);
        assert_eq!(
            cache.get(&hash).map(|(level, account)| (level, account.as_ref().clone())),
            Some((CacheLevel::L1, account))
        );
    }

    #[test]
    fn account_written_cache_inserts_into_l2_when_serialized_size_exceeds_256kib() {
        let cache = AccountWrittenCache::new();
        let (_, account) = new_acc(212);
        let hash = account_hash(&account);

        let level = cache
            .insert_by_serialized_len(
                hash,
                &account,
                super::ACCOUNT_WRITTEN_CACHE_L2_THRESHOLD_BYTES + 1,
            )
            .unwrap();

        assert_eq!(level, CacheLevel::L2);
        assert_eq!(
            cache.get(&hash).map(|(level, account)| (level, account.as_ref().clone())),
            Some((CacheLevel::L2, account))
        );
    }

    #[test]
    fn account_written_cache_does_not_insert_at_or_below_256kib() {
        let cache = AccountWrittenCache::new();
        let (_, account) = new_acc(213);
        let hash = account_hash(&account);

        assert!(cache
            .insert_by_serialized_len(
                hash,
                &account,
                super::ACCOUNT_WRITTEN_CACHE_L2_THRESHOLD_BYTES
            )
            .is_none());
        assert!(cache.get(&hash).is_none());
    }

    #[test]
    fn account_written_cache_checks_l1_before_l2() {
        let cache = AccountWrittenCache::new();
        let (_, l1_account) = new_acc(214);
        let (_, l2_account) = new_acc(215);
        let hash = AccountHash::new(new_u256("written_cache_hash", 1));

        cache.insert_l2(hash, Arc::new(l2_account));
        cache.insert_l1(hash, Arc::new(l1_account.clone()));

        assert_eq!(
            cache.get(&hash).map(|(level, account)| (level, account.as_ref().clone())),
            Some((CacheLevel::L1, l1_account))
        );
    }

    #[test]
    fn account_read_cache_remove_deletes_from_l1() {
        let cache = AccountReadCache::new();
        let (_, account) = new_acc(208);
        let hash = account_hash(&account);

        cache.insert_l1(hash, Arc::new(account));

        assert!(cache.remove(&hash));
        assert!(cache.get(&hash).is_none());
    }

    #[test]
    fn account_read_cache_remove_deletes_from_l2() {
        let cache = AccountReadCache::new();
        let (_, account) = new_acc(209);
        let hash = account_hash(&account);

        cache.insert_l2(hash, Arc::new(account));

        assert!(cache.remove(&hash));
        assert!(cache.get(&hash).is_none());
    }

    #[test]
    fn account_read_cache_remove_returns_false_for_missing_hash() {
        let cache = AccountReadCache::new();
        let hash = AccountHash::new(new_u256("missing_cache_hash", 1));

        assert!(!cache.remove(&hash));
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
        let state = state_with_account_hash(routing, expected_hash);

        let mut update = AccumulatedUpdate::new();
        update.transition_thread(thread_id, block_0, block_1);
        update.insert(routing, ArchiveOperation::UpdateOrInsert(archived_account));
        repo.archive.apply_update(&update).unwrap();

        let err = repo
            .find_account_with_info(&state, &routing, AccountInfo::AccountHash(expected_hash))
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
        let state = state_with_account_hash(routing, hash);
        repo.account_read_cache.insert_l1(hash, Arc::new(account.clone()));

        let resolved = repo
            .find_account_with_info(&state, &routing, AccountInfo::AccountHash(hash))
            .unwrap()
            .expect("cache hit should return account");

        assert_eq!(resolved, account);
        assert!(repo.archive.read_account_operation(&routing).unwrap().is_none());
    }

    #[test]
    fn repository_lookup_returns_account_from_written_cache_after_read_cache_miss() {
        let dir = tempfile::tempdir().unwrap();
        let archive = ArchiveStateStore::in_memory();
        let repo =
            DurableThreadAccountsRepository::new(PathBuf::from(dir.path()), archive, None).unwrap();

        let (routing, account) = new_acc(216);
        let hash = account_hash(&account);
        let state = state_with_account_hash(routing, hash);
        repo.archive.account_written_cache().insert_l1(hash, Arc::new(account.clone()));

        let resolved = repo
            .find_account_with_info(&state, &routing, AccountInfo::AccountHash(hash))
            .unwrap()
            .expect("written cache hit should return account");

        assert_eq!(resolved, account);
        assert!(repo.archive.read_account_operation(&routing).unwrap().is_none());
    }

    #[test]
    fn repository_lookup_prefers_read_cache_over_written_cache() {
        let dir = tempfile::tempdir().unwrap();
        let archive = ArchiveStateStore::in_memory();
        let repo =
            DurableThreadAccountsRepository::new(PathBuf::from(dir.path()), archive, None).unwrap();

        let (routing, read_account) = new_acc(217);
        let (_, written_account) = new_acc(218);
        let hash = AccountHash::new(new_u256("shared_cache_key", 1));
        let state = state_with_account_hash(routing, hash);
        repo.account_read_cache.insert_l1(hash, Arc::new(read_account.clone()));
        repo.archive.account_written_cache().insert_l1(hash, Arc::new(written_account));

        let resolved = repo
            .find_account_with_info(&state, &routing, AccountInfo::AccountHash(hash))
            .unwrap()
            .expect("read cache hit should return account");

        assert_eq!(resolved, read_account);
    }

    #[test]
    fn archive_write_does_not_cache_small_written_account() {
        let archive = ArchiveStateStore::in_memory();
        let thread_id = ThreadIdentifier::default();
        let block_0 = BlockIdentifier::default();
        let block_1 = BlockIdentifier::new(new_u256("small_written_block", 1));
        archive.ensure_thread(&thread_id, &block_0).unwrap();

        let (routing, account) = new_acc(219);
        let hash = account_hash(&account);
        let mut update = AccumulatedUpdate::new();
        update.transition_thread(thread_id, block_0, block_1);
        update.insert(routing, ArchiveOperation::UpdateOrInsert(account));

        archive.apply_update(&update).unwrap();

        assert!(archive.account_written_cache().get(&hash).is_none());
    }

    #[test]
    fn state_update_invalidates_written_account_hash_from_cache() {
        let dir = tempfile::tempdir().unwrap();
        let archive = ArchiveStateStore::in_memory();
        let repo =
            DurableThreadAccountsRepository::new(PathBuf::from(dir.path()), archive, None).unwrap();

        let thread_id = ThreadIdentifier::default();
        let state = DurableThreadAccountsRepository::new_state();
        let old_tvm_accounts = tvm_block::ShardAccounts::default();
        let (routing, account) = new_acc(210);
        let hash = account_hash(&account);
        let mut accounts = HashMap::new();

        repo.account_read_cache.insert_l1(hash, Arc::new(account.clone()));
        accounts.insert(routing, super::BlockAccountOperation::UpdateOrInsert(account));

        repo.state_update(&thread_id, 0, &old_tvm_accounts, &state, &accounts).unwrap();

        assert!(repo.account_read_cache.get(&hash).is_none());
    }

    #[test]
    fn state_update_parallel_path_applies_large_batch() {
        let dir = tempfile::tempdir().unwrap();
        let archive = ArchiveStateStore::in_memory();
        let repo =
            DurableThreadAccountsRepository::new(PathBuf::from(dir.path()), archive, None).unwrap();

        let thread_id = ThreadIdentifier::default();
        let state = DurableThreadAccountsRepository::new_state();
        let old_tvm_accounts = tvm_block::ShardAccounts::default();
        let mut accounts = HashMap::new();
        let mut expected = Vec::new();
        for seed in 0..super::PARALLEL_PREPARE_UPDATES_THRESHOLD {
            let (routing, account) = new_acc(1_000 + seed);
            accounts.insert(routing, super::BlockAccountOperation::UpdateOrInsert(account.clone()));
            expected.push((routing, account));
        }

        let (new_state, account_updates) =
            repo.state_update(&thread_id, 0, &old_tvm_accounts, &state, &accounts).unwrap();

        assert_eq!(account_updates.len(), expected.len());
        for (routing, account) in expected {
            assert_eq!(repo.state_account(&new_state, &routing).unwrap(), Some(account));
        }
    }

    #[test]
    fn state_update_remove_cleans_up_redirect_entries_for_real_routing() {
        let dir = tempfile::tempdir().unwrap();
        let archive = ArchiveStateStore::in_memory();
        let repo =
            DurableThreadAccountsRepository::new(PathBuf::from(dir.path()), archive, None).unwrap();

        let thread_id = ThreadIdentifier::default();
        let old_tvm_accounts = tvm_block::ShardAccounts::default();
        let state0 = DurableThreadAccountsRepository::new_state();
        let (routing, account) = new_acc_with_dapp(300, 301);
        let redirect_routing = routing.account_id().redirect();

        let mut insert_batch = HashMap::new();
        insert_batch.insert(routing, super::BlockAccountOperation::UpdateOrInsert(account.clone()));
        insert_batch.insert(
            redirect_routing,
            super::BlockAccountOperation::UpdateOrInsert(
                account.with_redirect(*routing.dapp_id()).unwrap(),
            ),
        );

        let (state1, insert_ops) =
            repo.state_update(&thread_id, 0, &old_tvm_accounts, &state0, &insert_batch).unwrap();
        assert!(matches!(
            insert_ops.get(&redirect_routing),
            Some(ArchiveOperation::UpdateOrInsert(redirect)) if redirect.is_redirect()
        ));

        let mut remove_batch = HashMap::new();
        remove_batch.insert(routing, super::BlockAccountOperation::Remove);
        let (state2, remove_ops) =
            repo.state_update(&thread_id, 1, &old_tvm_accounts, &state1, &remove_batch).unwrap();

        assert_eq!((state2.map()).get(&routing), None);
        assert_eq!((state2.map()).get(&redirect_routing), None);
        assert!(matches!(remove_ops.get(&routing), Some(ArchiveOperation::Remove)));
        assert!(matches!(remove_ops.get(&redirect_routing), Some(ArchiveOperation::Remove)));
    }

    #[test]
    fn state_update_remove_does_not_override_redirect_upsert_in_same_batch() {
        let dir = tempfile::tempdir().unwrap();
        let archive = ArchiveStateStore::in_memory();
        let repo =
            DurableThreadAccountsRepository::new(PathBuf::from(dir.path()), archive, None).unwrap();

        let thread_id = ThreadIdentifier::default();
        let old_tvm_accounts = tvm_block::ShardAccounts::default();
        let state0 = DurableThreadAccountsRepository::new_state();
        let id = AccountIdentifier::new(new_u256("acc", 310));
        let dapp_id_1 = DAppIdentifier::new(new_u256("dapp", 311));
        let dapp_id_2 = DAppIdentifier::new(new_u256("dapp", 312));
        let routing_1 = id.routing(dapp_id_1);
        let routing_2 = id.routing(dapp_id_2);
        let redirect_routing = id.redirect();
        let (_, account_1) = new_acc_with_dapp(310, 311);
        let (_, account_2) = new_acc_with_dapp(310, 312);

        let mut insert_batch = HashMap::new();
        insert_batch
            .insert(routing_1, super::BlockAccountOperation::UpdateOrInsert(account_1.clone()));
        insert_batch.insert(
            redirect_routing,
            super::BlockAccountOperation::UpdateOrInsert(
                account_1.with_redirect(*routing_1.dapp_id()).unwrap(),
            ),
        );
        let (state1, _) =
            repo.state_update(&thread_id, 0, &old_tvm_accounts, &state0, &insert_batch).unwrap();

        let mut move_batch = HashMap::new();
        move_batch.insert(routing_1, super::BlockAccountOperation::Remove);
        move_batch
            .insert(routing_2, super::BlockAccountOperation::UpdateOrInsert(account_2.clone()));
        move_batch.insert(
            redirect_routing,
            super::BlockAccountOperation::UpdateOrInsert(
                account_2.with_redirect(*routing_2.dapp_id()).unwrap(),
            ),
        );
        let (state2, move_ops) =
            repo.state_update(&thread_id, 1, &old_tvm_accounts, &state1, &move_batch).unwrap();

        assert_eq!((state2.map()).get(&routing_1), None);
        assert!(matches!((state2.map()).get(&routing_2), Some(AccountInfo::AccountHash(_))));
        assert_eq!((state2.map()).get(&redirect_routing), Some(AccountInfo::Redirect(dapp_id_2)));
        assert!(matches!(
            move_ops.get(&redirect_routing),
            Some(ArchiveOperation::UpdateOrInsert(redirect))
                if redirect.is_redirect() && redirect.get_dapp_id() == Some(dapp_id_2)
        ));
    }

    #[test]
    fn state_update_remove_does_not_add_extra_redirect_remove_for_default_routing() {
        let dir = tempfile::tempdir().unwrap();
        let archive = ArchiveStateStore::in_memory();
        let repo =
            DurableThreadAccountsRepository::new(PathBuf::from(dir.path()), archive, None).unwrap();

        let thread_id = ThreadIdentifier::default();
        let old_tvm_accounts = tvm_block::ShardAccounts::default();
        let state0 = DurableThreadAccountsRepository::new_state();
        let (routing, account) = new_acc(320);

        let mut insert_batch = HashMap::new();
        insert_batch.insert(routing, super::BlockAccountOperation::UpdateOrInsert(account));
        let (state1, _) =
            repo.state_update(&thread_id, 0, &old_tvm_accounts, &state0, &insert_batch).unwrap();

        let mut remove_batch = HashMap::new();
        remove_batch.insert(routing, super::BlockAccountOperation::Remove);
        let (state2, remove_ops) =
            repo.state_update(&thread_id, 1, &old_tvm_accounts, &state1, &remove_batch).unwrap();

        assert_eq!((state2.map()).get(&routing), None);
        assert_eq!(remove_ops.len(), 1);
        assert!(matches!(remove_ops.get(&routing), Some(ArchiveOperation::Remove)));
    }

    #[test]
    fn state_update_capacity_tracks_canonical_bodies_and_redirects() {
        let dir = tempfile::tempdir().unwrap();
        let repo = DurableThreadAccountsRepository::new(
            dir.path().into(),
            ArchiveStateStore::in_memory(),
            None,
        )
        .unwrap();
        let (routing, account) = new_acc_with_dapp(41, 42);
        let redirect_routing = routing.account_id().redirect();
        let redirect = ThreadAccount::redirect(*routing.dapp_id());
        let old_tvm = tvm_block::ShardAccounts::default();
        let operations = HashMap::from([
            (routing, super::BlockAccountOperation::UpdateOrInsert(account.clone())),
            (redirect_routing, super::BlockAccountOperation::UpdateOrInsert(redirect.clone())),
        ]);
        let (state, _) = repo
            .state_update(
                &ThreadIdentifier::default(),
                1,
                &old_tvm,
                &DurableThreadAccountsRepository::new_state(),
                &operations,
            )
            .unwrap();
        let expected = account.canonical_size() + redirect.canonical_size();
        assert_eq!((state.map()).capacity().bytes(), expected);
        assert_eq!(
            (state.map()).dapp_capacity(routing.dapp_id()).unwrap().bytes(),
            account.canonical_size()
        );
        assert_eq!(
            (state.map()).get(&routing),
            Some(AccountInfo::AccountHash(account.account_hash()))
        );
        let mut bytes = Vec::new();
        (state.map()).write(&mut bytes).unwrap();
        let restored = ThreadAccountMap::read(&mut bytes.as_slice()).unwrap();
        assert_eq!(restored.capacity().bytes(), expected);
        let operations = HashMap::from([(routing, super::BlockAccountOperation::Remove)]);
        let (empty, _) = repo
            .state_update(&ThreadIdentifier::default(), 2, &old_tvm, &state, &operations)
            .unwrap();
        assert_eq!((empty.map()).capacity().bytes(), 0);
    }

    #[test]
    fn snapshot_hash_pipeline_rejects_malformed_tvm_body_without_panicking() {
        let routing = AccountIdentifier::default().redirect();
        let malformed = vec![0xb5, 0xee, 0x9c, 0x72];
        assert!(super::process_snapshot_hash_batch(vec![(routing, malformed)]).is_err());
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
        let expected_hash = account.account_hash();

        let validated = validate_snapshot_account_bytes(
            &archive_data,
            &AccountInfo::AccountHash(expected_hash),
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
    fn snapshot_validation_rejects_same_vm_with_different_shard_metadata() {
        let (_, account) = new_acc(41);
        let other_version = ThreadAccount::new(
            account.vm_account().unwrap(),
            account.last_trans_hash(),
            account.last_trans_lt() + 1,
            account.get_dapp_id(),
        )
        .unwrap();
        assert_eq!(
            account.vm_account().unwrap().hash(),
            other_version.vm_account().unwrap().hash()
        );
        assert_ne!(account.account_hash(), other_version.account_hash());
        let data = bincode::serialize(&account.canonical_bytes()).unwrap();
        assert!(validate_snapshot_account_bytes(
            &data,
            &AccountInfo::AccountHash(other_version.account_hash())
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn snapshot_archive_payload_validation_rejects_mismatched_map_info() {
        let (_routing, account) = new_acc(2);
        let archive_data = bincode::serialize(&account.write_bytes().unwrap()).unwrap();

        let (_, other_account) = new_acc(99);
        let wrong_hash = other_account.account_hash();
        assert!(validate_snapshot_account_bytes(
            &archive_data,
            &AccountInfo::AccountHash(wrong_hash),
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
    fn snapshot_scan_filters_archive_records_using_the_merkle_map() -> anyhow::Result<()> {
        let thread = ThreadIdentifier::default();
        let block = BlockIdentifier::new(new_u256("filtered_export_block", 1));
        let (repo, _dir) = setup_repo();
        let (routing, account) = new_acc(3);
        let mut builder = repo.state_builder(&thread, 0, &ThreadAccountsRepository::new_state());
        builder.insert_account(&routing, &account);
        // The same archive also contains accounts outside the exported map.
        for index in [4, 5] {
            let (other_routing, other_account) = new_acc(index);
            builder.insert_account(&other_routing, &other_account);
        }
        let transition = builder.build(None)?;
        repo.finalize_thread_transition(
            &block,
            &thread,
            0,
            &transition.new_state,
            transition.account_operations,
        )?;
        assert_drained(&repo);
        let map = ThreadAccountMap::new().update(&[(
            routing,
            Some((
                AccountInfo::AccountHash(account.account_hash()),
                crate::AccountCapacity::new(account.canonical_size()),
            )),
        )]);
        let state = DurableThreadAccountsState::new(map);
        {
            let mut bytes = Cursor::new(Vec::new());
            repo.durable_map_repo().export_durable_snapshot_to_writer_checked(
                &state,
                &mut bytes,
                &|| false,
            )?;
            bytes.set_position(0);
            let mut count = [0; 8];
            std::io::Read::read_exact(&mut bytes, &mut count)?;
            bytes.set_position(bytes.position() + u64::from_le_bytes(count));
            std::io::Read::read_exact(&mut bytes, &mut count)?;
            assert_eq!(u64::from_le_bytes(count), 1);
            let (actual_routing, body) = read_snapshot_account_entry(&mut bytes)?;
            assert_eq!(actual_routing, routing);
            assert_eq!(body, account.canonical_bytes());
            assert_eq!(bytes.position(), bytes.get_ref().len() as u64);
        }
        Ok(())
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
        assert_eq!(archived.account_hash(), account.account_hash(),);
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

    #[test]
    fn import_rejects_capacity_tampering_even_when_account_hash_matches() {
        let (routing, account) = new_acc(21);
        let map = (super::ThreadAccountMap::new()).update(&[(
            routing,
            Some((
                AccountInfo::AccountHash(account_hash(&account)),
                super::AccountCapacity::new(account.canonical_size() + 1),
            )),
        )]);
        let state = DurableThreadAccountsState::with_map(map);
        let snapshot = durable_snapshot_bytes_with_entries(
            &state,
            &[(routing, account.canonical_bytes().to_vec())],
        );
        let (import_repo, _import_dir) = setup_repo();
        import_repo.reset_archive().unwrap();
        let err = import_repo
            .import_durable_snapshot_from_reader(
                &mut Cursor::new(snapshot),
                &ThreadIdentifier::default(),
                &BlockIdentifier::default(),
            )
            .unwrap_err();
        assert!(err.to_string().contains("does not match map capacity"), "{err}");
    }

    #[test]
    fn import_durable_snapshot_from_reader_rejects_streamed_body_hash_mismatch() {
        let thread_id = ThreadIdentifier::default();
        let block_id = BlockIdentifier::new(new_u256("block", 2));

        let (routing, account) = new_acc(21);
        let (_, other_account) = new_acc(22);
        let map = (super::ThreadAccountMap::new()).update(&[(
            routing,
            Some((
                AccountInfo::AccountHash(account_hash(&other_account)),
                super::AccountCapacity::new(account.canonical_size()),
            )),
        )]);
        let state = DurableThreadAccountsState::with_map(map);
        let snapshot_bytes = durable_snapshot_bytes_with_entries(
            &state,
            &[(routing, account.write_bytes().unwrap())],
        );

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
            err_msg.contains("wrong-hash") || err_msg.contains("does NOT match the map"),
            "unexpected error: {err_msg}",
        );
    }
}
