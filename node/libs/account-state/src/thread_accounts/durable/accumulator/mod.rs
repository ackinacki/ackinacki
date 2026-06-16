mod map_store;
mod snapshot_pin;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

pub use map_store::MerkleMapStore;
use node_types::AccountHash;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
pub use snapshot_pin::AnchorBlockRef;
use snapshot_pin::SnapshotPinState;

/// Snapshot of the apply-gate decision the update loop needs per batch.
/// Returned by `apply_decision_anchor` so the loop doesn't hold the
/// pin_state lock across the apply.
pub(crate) struct ApplyDecision {
    pub anchor_thread: ThreadIdentifier,
    pub anchor_block: BlockIdentifier,
    /// When true, batches that advance `anchor_thread` past `anchor_block`
    /// must be deferred (i.e. state is BoundaryReached or Pinned).
    pub defer_past_anchor: bool,
}

use crate::live_metrics::set_unfinalized_pool_sizes;
use crate::live_metrics::LivePendingUpdateCounter;
use crate::thread_accounts::archive::control::ThreadControlState;
use crate::thread_accounts::archive::update::AccumulatedUpdate;
use crate::thread_accounts::durable::archive::store::ArchiveStateStore;
use crate::thread_accounts::ArchiveOperation;
use crate::StateAccountsMetrics;
use crate::ThreadAccount;
use crate::ThreadAccountMap;

fn thread_name() -> String {
    std::thread::current().name().unwrap_or("unnamed").to_string()
}

struct PendingUpdate {
    expected_thread_blocks: HashMap<ThreadIdentifier, BlockIdentifier>,
    archive_update: AccumulatedUpdate,
    merkle_map_update: HashMap<ThreadIdentifier, (BlockIdentifier, ThreadAccountMap)>,
    blocks_accumulated: usize,
    _live_counter: LivePendingUpdateCounter,
}

impl PendingUpdate {
    pub fn new(control_states: &[ThreadControlState]) -> Self {
        let mut expected_thread_blocks = HashMap::new();
        for state in control_states {
            if let Some(last_block_id) = state.last_block_id {
                expected_thread_blocks.insert(state.thread_id, last_block_id);
            }
        }
        Self {
            expected_thread_blocks,
            archive_update: AccumulatedUpdate::default(),
            merkle_map_update: HashMap::new(),
            blocks_accumulated: 0,
            _live_counter: LivePendingUpdateCounter::new(),
        }
    }
}

/// Guard returned by [`ArchiveUpdateAccumulator::flush`].
/// While alive, prevents producers from sending new batches to the update loop.
/// Producers can still accumulate into `pending`; accumulated data is sent when
/// the guard is dropped if the queue is full.
pub struct FlushGuard<'a> {
    accumulator: &'a ArchiveUpdateAccumulator,
}

impl<'a> Drop for FlushGuard<'a> {
    fn drop(&mut self) {
        // tracing::trace!(target: "lock", "[{}] FlushGuard::drop — clearing send_blocked", thread_name());
        self.accumulator.send_blocked.store(false, Ordering::Release);
    }
}

struct UnfinalizedAccountsPool {
    threads: HashMap<ThreadIdentifier, ThreadUnfinalizedAccountsPool>,
}

struct ThreadUnfinalizedAccountsPool {
    active: HeightBoundedTable,
    draining: HeightBoundedTable,
}

#[derive(Default)]
struct HeightBoundedTable {
    upper_height: u64,
    accounts: HashMap<AccountHash, ThreadAccount>,
}

impl HeightBoundedTable {
    fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }

    fn len(&self) -> usize {
        self.accounts.len()
    }

    fn insert_batch<I>(&mut self, block_height: u64, accounts: I)
    where
        I: IntoIterator<Item = (AccountHash, ThreadAccount)>,
    {
        let mut has_inserted = false;
        for (hash, account) in accounts {
            if !has_inserted {
                if self.accounts.is_empty() {
                    self.upper_height = block_height;
                } else {
                    self.upper_height = self.upper_height.max(block_height);
                }
                has_inserted = true;
            }
            self.accounts.insert(hash, account);
        }
    }

    fn clear(&mut self) {
        self.upper_height = 0;
        self.accounts.clear();
    }

    fn get(&self, hash: &AccountHash) -> Option<ThreadAccount> {
        self.accounts.get(hash).cloned()
    }
}

impl ThreadUnfinalizedAccountsPool {
    fn new() -> Self {
        Self { active: HeightBoundedTable::default(), draining: HeightBoundedTable::default() }
    }

    fn is_empty(&self) -> bool {
        self.active.is_empty() && self.draining.is_empty()
    }

    fn len(&self) -> usize {
        self.active.len() + self.draining.len()
    }

    fn active_len(&self) -> usize {
        self.active.len()
    }

    fn draining_len(&self) -> usize {
        self.draining.len()
    }

    fn insert_batch<I>(&mut self, block_height: u64, accounts: I)
    where
        I: IntoIterator<Item = (AccountHash, ThreadAccount)>,
    {
        self.active.insert_batch(block_height, accounts);
    }

    fn get(&self, hash: &AccountHash) -> Option<ThreadAccount> {
        self.active.get(hash).or_else(|| self.draining.get(hash))
    }

    fn drain_finalized(&mut self, finalized_block_height: u64) {
        if !self.draining.is_empty() && self.draining.upper_height <= finalized_block_height {
            self.draining.clear();
        }

        if self.draining.is_empty() && !self.active.is_empty() {
            self.draining = std::mem::take(&mut self.active);
        }

        if !self.draining.is_empty() && self.draining.upper_height <= finalized_block_height {
            self.draining.clear();
        }
    }
}

impl UnfinalizedAccountsPool {
    fn new() -> Self {
        Self { threads: HashMap::new() }
    }

    fn len(&self) -> usize {
        self.threads.values().map(ThreadUnfinalizedAccountsPool::len).sum()
    }

    fn active_len(&self) -> usize {
        self.threads.values().map(ThreadUnfinalizedAccountsPool::active_len).sum()
    }

    fn draining_len(&self) -> usize {
        self.threads.values().map(ThreadUnfinalizedAccountsPool::draining_len).sum()
    }

    fn report_sizes(&self) {
        set_unfinalized_pool_sizes(self.active_len() as u64, self.draining_len() as u64);
    }

    fn get(&self, hash: &AccountHash) -> Option<ThreadAccount> {
        self.threads.values().find_map(|pool| pool.get(hash))
    }

    fn insert_batch<I>(&mut self, thread_id: ThreadIdentifier, block_height: u64, accounts: I)
    where
        I: IntoIterator<Item = (AccountHash, ThreadAccount)>,
    {
        self.threads
            .entry(thread_id)
            .or_insert_with(ThreadUnfinalizedAccountsPool::new)
            .insert_batch(block_height, accounts);
        self.report_sizes();
    }

    fn drain_finalized(&mut self, thread_id: ThreadIdentifier, finalized_block_height: u64) {
        if let Some(pool) = self.threads.get_mut(&thread_id) {
            pool.drain_finalized(finalized_block_height);
            if pool.is_empty() {
                self.threads.remove(&thread_id);
            }
            self.report_sizes();
        }
    }

    fn drain_not_live(&mut self, live_threads: &HashSet<ThreadIdentifier>) {
        self.threads.retain(|thread_id, _| live_threads.contains(thread_id));
        self.report_sizes();
    }
}

pub struct ArchiveUpdateAccumulator {
    /// Current pending update being accumulated by producers.
    pending: parking_lot::Mutex<PendingUpdate>,
    accumulated_block_limit: usize,
    stop: AtomicBool,
    /// When true, `send_batch` is blocked — producers accumulate into `pending`
    /// but don't send to the update loop. Set by [`FlushGuard`].
    send_blocked: AtomicBool,
    unfinalized_pool: parking_lot::RwLock<UnfinalizedAccountsPool>,
    /// Channel to send completed batches to the update loop.
    batch_tx: crossbeam_channel::Sender<Arc<PendingUpdate>>,
    batch_rx: crossbeam_channel::Receiver<Arc<PendingUpdate>>,
    /// In-flight batches: sent to update loop but not yet processed.
    /// Kept for read access by `find_account_operation`.
    in_flight: parking_lot::RwLock<Vec<Arc<PendingUpdate>>>,
    /// Notified when an in-flight batch is consumed (for flush).
    drained: parking_lot::Condvar,
    drained_mutex: parking_lot::Mutex<()>,
    /// Snapshot pin state machine. See `spec/SNAPSHOT.md`.
    pin_state: parking_lot::Mutex<SnapshotPinState>,
    /// Signaled by the update loop when the defer-apply boundary has just
    /// been applied (the anchor thread has reached the requested anchor
    /// block in the archive). Awaited by `acquire_snapshot_pin`.
    pin_acquirable: parking_lot::Condvar,
    /// Signaled by `PinHandle::drop`. Awaited by the update loop to
    /// detect when to drain the deferred queue.
    pin_released: parking_lot::Condvar,
    /// Last block_id applied to the archive per thread. Updated only by
    /// the update loop after `apply_pending_update` succeeds.
    /// Initialized from the archive's control states at construction.
    /// Used by `request_snapshot_pin`'s stale-network shortcut: if the
    /// caller asks to pin a block that's already the archive's current
    /// state and pending has nothing further to apply for that thread,
    /// we transition Requested → BoundaryReached directly without
    /// waiting for the update loop to process a (non-existent) batch.
    applied_per_thread: parking_lot::Mutex<HashMap<ThreadIdentifier, BlockIdentifier>>,
    metrics: Option<StateAccountsMetrics>,
}

#[must_use = "dropping PinRequestGuard cancels the snapshot pin request; keep it alive until the pin is acquired or call disarm() after successful acquire"]
pub struct PinRequestGuard {
    accumulator: Arc<ArchiveUpdateAccumulator>,
    anchor_thread: ThreadIdentifier,
    anchor_block: BlockIdentifier,
    armed: bool,
}

impl PinRequestGuard {
    pub fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PinRequestGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.accumulator.cancel_snapshot_pin(self.anchor_thread, self.anchor_block);
    }
}

impl ArchiveUpdateAccumulator {
    /// Log memory stats for all internal data structures.
    /// Uses target "mem" so it can be routed to a dedicated file.
    pub fn report_mem_stats(&self, label: &str) {
        let pending = self.pending.lock();
        let pending_ops = pending.archive_update.operations.len();
        let pending_transitions = pending.archive_update.thread_transitions.len();
        let pending_emerging = pending.archive_update.emerging_threads.len();
        let pending_collapsing = pending.archive_update.collapsing_threads.len();
        let pending_expected = pending.expected_thread_blocks.len();
        let pending_maps = pending.merkle_map_update.len();
        let pending_blocks = pending.blocks_accumulated;
        drop(pending);

        let in_flight_count = self.in_flight.read().len();

        let unfinalized_pool_len = self.unfinalized_pool.read().len();

        tracing::info!(
            target: "mem",
            "[{label}] pending: ops={pending_ops} transitions={pending_transitions} \
             emerging={pending_emerging} collapsing={pending_collapsing} \
             expected_blocks={pending_expected} maps={pending_maps} blocks_accumulated={pending_blocks} | \
             in_flight={in_flight_count} | \
             unfinalized_pool={unfinalized_pool_len}",
        );
    }

    pub fn new(
        block_limit: usize,
        control_states: &[ThreadControlState],
        metrics: Option<StateAccountsMetrics>,
    ) -> Self {
        let (batch_tx, batch_rx) = crossbeam_channel::bounded(block_limit);
        let mut applied_per_thread = HashMap::new();
        for state in control_states {
            if let Some(last_block_id) = state.last_block_id {
                applied_per_thread.insert(state.thread_id, last_block_id);
            }
        }
        let accumulator = Self {
            pending: parking_lot::Mutex::new(PendingUpdate::new(control_states)),
            accumulated_block_limit: block_limit,
            stop: AtomicBool::new(false),
            send_blocked: AtomicBool::new(false),
            unfinalized_pool: parking_lot::RwLock::new(UnfinalizedAccountsPool::new()),
            batch_tx,
            batch_rx,
            in_flight: parking_lot::RwLock::new(Vec::new()),
            drained: parking_lot::Condvar::new(),
            drained_mutex: parking_lot::Mutex::new(()),
            pin_state: parking_lot::Mutex::new(SnapshotPinState::default()),
            pin_acquirable: parking_lot::Condvar::new(),
            pin_released: parking_lot::Condvar::new(),
            applied_per_thread: parking_lot::Mutex::new(applied_per_thread),
            metrics,
        };
        set_unfinalized_pool_sizes(0, 0);
        accumulator
    }

    fn report_queue_size(&self, blocks: usize) {
        if let Some(metrics) = &self.metrics {
            metrics.report_accumulator_write_queue_blocks(blocks as u64);
        }
    }

    fn report_deferred_len(&self, deferred_len: usize) {
        if let Some(metrics) = &self.metrics {
            metrics.report_accumulator_deferred_batches(deferred_len as u64);
        }
    }

    fn is_full(&self, pending: &PendingUpdate) -> bool {
        pending.blocks_accumulated >= self.accumulated_block_limit
    }

    /// Swap out the current pending, wrap in Arc, send to update loop,
    /// and register in in_flight for reads. Must be called with pending lock held.
    /// Returns the new (empty) pending.
    fn send_batch(&self, pending: &mut PendingUpdate) {
        // If a FlushGuard is active, producers just keep accumulating.
        // The guard holder will send any pending batch when dropped.
        if self.send_blocked.load(Ordering::Acquire) {
            // tracing::trace!(
            //     target: "lock",
            //     "[{}] send_batch: SKIPPED (send_blocked, blocks_accumulated={})",
            //     thread_name(),
            //     pending.blocks_accumulated,
            // );
            return;
        }
        // let blocks = pending.blocks_accumulated;
        // let ops = pending.archive_update.operations.len();
        let next = build_next_pending_update(pending);
        let batch = Arc::new(std::mem::replace(pending, next));
        self.in_flight.write().push(Arc::clone(&batch));
        self.report_queue_size(pending.blocks_accumulated);
        // let in_flight_len = self.in_flight.read().len();
        // let channel_len = self.batch_tx.len();
        // tracing::trace!(
        //     target: "lock",
        //     "[{}] send_batch: SENDING blocks={blocks} ops={ops} in_flight={in_flight_len} channel={channel_len}",
        //     thread_name(),
        // );
        // bounded channel — this blocks if update_loop is behind
        let _ = self.batch_tx.send(batch);
        // tracing::trace!(target: "lock", "[{}] send_batch: SENT", thread_name());
    }

    fn live_threads(pending: &PendingUpdate) -> HashSet<ThreadIdentifier> {
        let mut live = HashSet::new();
        live.extend(pending.expected_thread_blocks.keys().copied());
        live.extend(pending.archive_update.emerging_threads.keys().copied());
        live.extend(pending.archive_update.thread_transitions.keys().copied());
        for thread_id in &pending.archive_update.collapsing_threads {
            live.remove(thread_id);
        }
        live
    }

    /// Look up an account in the unfinalized pool by its content hash.
    pub fn unfinalized_pool_get(&self, hash: &AccountHash) -> Option<ThreadAccount> {
        self.unfinalized_pool.read().get(hash)
    }

    /// Insert accounts into the unfinalized pool from a state update.
    pub fn unfinalized_pool_insert(
        &self,
        thread_id: ThreadIdentifier,
        block_height: u64,
        accounts: &HashMap<AccountRouting, ArchiveOperation>,
    ) {
        let insert_count = accounts
            .values()
            .filter(|op| matches!(op, ArchiveOperation::UpdateOrInsert(_)))
            .count();
        let mut pool = self.unfinalized_pool.write();
        pool.insert_batch(
            thread_id,
            block_height,
            accounts.values().filter_map(|operation| {
                let ArchiveOperation::UpdateOrInsert(account) = operation else {
                    return None;
                };
                let Ok(vm_account) = account.vm_account() else {
                    return None;
                };
                Some((vm_account.hash(), account.clone()))
            }),
        );
        tracing::info!(
            target: "mem",
            "unfinalized_pool_insert: T:{thread_id} H:{block_height} added={insert_count} pool={}",
            pool.len(),
        );
    }

    /// Look up an account in the pending accumulator or in-flight batches by routing,
    /// verifying the hash matches.
    pub fn find_account_operation(
        &self,
        routing: &AccountRouting,
        expected_hash: &AccountHash,
    ) -> Option<ArchiveOperation> {
        // Check current pending first
        {
            let pending = self.pending.lock();
            if let Some(result) =
                Self::find_in_operations(&pending.archive_update.operations, routing, expected_hash)
            {
                return Some(result);
            }
        }
        // Check in-flight batches (newest first for most recent version)
        let in_flight = self.in_flight.read();
        for batch in in_flight.iter().rev() {
            if let Some(result) =
                Self::find_in_operations(&batch.archive_update.operations, routing, expected_hash)
            {
                return Some(result);
            }
        }
        None
    }

    fn find_in_operations(
        operations: &HashMap<AccountRouting, ArchiveOperation>,
        routing: &AccountRouting,
        expected_hash: &AccountHash,
    ) -> Option<ArchiveOperation> {
        match operations.get(routing) {
            Some(ArchiveOperation::UpdateOrInsert(account)) => match account.vm_account() {
                Ok(vm_account) if vm_account.hash() == *expected_hash => {
                    Some(ArchiveOperation::UpdateOrInsert(account.clone()))
                }
                _ => None,
            },
            Some(ArchiveOperation::Remove) => Some(ArchiveOperation::Remove),
            None => None,
        }
    }

    /// Clear all pending state, drop the unfinalized pool, and re-derive
    /// expected blocks from archive control records. Must be called while
    /// paused (guard held) and after the in-flight queue has been drained
    /// (the caller is responsible for `wait_for_drain`; this method
    /// asserts `in_flight` is empty).
    ///
    /// Clearing the pool is required for snapshot imports: a stale entry
    /// indexed by `AccountHash` from a prior epoch would otherwise be
    /// returned by `unfinalized_pool_get` for the new map, masking missing
    /// archive entries instead of bailing.
    pub fn reset(&self, control_states: &[ThreadControlState]) {
        let mut pending = self.pending.lock();
        *pending = PendingUpdate::new(control_states);
        self.report_queue_size(pending.blocks_accumulated);
        drop(pending);

        let mut unfinalized_pool = self.unfinalized_pool.write();
        *unfinalized_pool = UnfinalizedAccountsPool::new();
        unfinalized_pool.report_sizes();
        drop(unfinalized_pool);

        let in_flight_len = self.in_flight.read().len();
        debug_assert_eq!(
            in_flight_len, 0,
            "accumulator::reset called with {in_flight_len} in-flight batches; \
             caller must drain before reset",
        );
        if in_flight_len > 0 {
            tracing::error!(
                target: "monit",
                "accumulator::reset called with {in_flight_len} in-flight batches still pending; \
                 clearing them — caller should have drained first",
            );
            self.in_flight.write().clear();
        }
    }

    /// Set the expected block ID for a thread in the accumulator.
    pub fn set_expected_block(
        &self,
        thread_id: ThreadIdentifier,
        block_id: BlockIdentifier,
        overwrite: bool,
    ) {
        // tracing::trace!(target: "lock", "[{}] ACQUIRING pending.lock @ set_expected_block", thread_name());
        let mut pending = self.pending.lock();
        if overwrite {
            pending.expected_thread_blocks.insert(thread_id, block_id);
        } else {
            pending.expected_thread_blocks.entry(thread_id).or_insert(block_id);
        }
        // tracing::trace!(target: "lock", "[{}] RELEASED pending.lock @ set_expected_block", thread_name());
    }

    /// Push a diff into the queue. Blocks if the channel is full.
    pub fn push_transition(
        &self,
        thread_id: ThreadIdentifier,
        block_id: BlockIdentifier,
        block_height: u64,
        operations: HashMap<AccountRouting, ArchiveOperation>,
        merkle_map: &ThreadAccountMap,
    ) {
        let ops_data_bytes: usize = operations
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
            "push_transition T:{thread_id} B:{block_id} H:{block_height} ops={} ops_data_bytes={ops_data_bytes}",
            operations.len(),
        );

        // tracing::trace!(target: "lock", "[{}] ACQUIRING pending.lock @ push_transition/enqueue", thread_name());
        let mut pending = self.pending.lock();
        // tracing::trace!(target: "lock", "[{}] ACQUIRED pending.lock @ push_transition/enqueue, blocks_accumulated={}", thread_name(), pending.blocks_accumulated);

        for (routing, account) in operations {
            pending.archive_update.insert(routing, account);
        }
        let expected_block_id = if let Some(transitioning) =
            pending.expected_thread_blocks.get(&thread_id)
        {
            *transitioning
        } else if let Some(emerging) = pending.archive_update.emerging_threads.get(&thread_id) {
            *emerging
        } else {
            tracing::error!(target: "monit", "push_transition: missing expected block for thread {}", thread_id);
            BlockIdentifier::default()
        };
        pending.archive_update.transition_thread(thread_id, expected_block_id, block_id);

        pending.merkle_map_update.insert(thread_id, (block_id, merkle_map.clone()));
        pending.blocks_accumulated += 1;
        self.report_queue_size(pending.blocks_accumulated);

        let live_threads = Self::live_threads(&pending);
        {
            let mut pool = self.unfinalized_pool.write();
            pool.drain_finalized(thread_id, block_height);
            pool.drain_not_live(&live_threads);
        }

        // Snapshot pin alignment (see spec/SNAPSHOT.md "Pin granularity
        // and batch alignment"). If the just-pushed transition matches
        // the requested anchor's (T_A, A) pair, force-flush the batch
        // boundary HERE — while we still hold the pending lock — so no
        // concurrent producer can sneak a later block into the same
        // batch. The update loop will then see a clean batch ending the
        // anchor thread at the anchor block.
        let mut force_send = self.is_full(&pending);
        if !force_send {
            if let Some((anchor_tid, anchor_bid)) = self.requested_anchor() {
                if anchor_tid == thread_id && anchor_bid == block_id {
                    tracing::trace!(
                        target: "monit",
                        "push_transition: force-flushing batch boundary at anchor (T:{thread_id}, B:{block_id})",
                    );
                    force_send = true;
                }
            }
        }
        if force_send {
            self.send_batch(&mut pending);
        }
        // tracing::trace!(target: "lock", "[{}] RELEASING pending.lock (scope exit) @ push_transition/enqueue", thread_name());
    }

    /// Push an emerge event into the queue.
    pub fn push_emerge(
        &self,
        thread_id: ThreadIdentifier,
        initial_block_id: BlockIdentifier,
        operations: HashMap<AccountRouting, ArchiveOperation>,
        merkle_map: &ThreadAccountMap,
    ) {
        tracing::trace!(target: "monit", "push_emerge T:{thread_id}, B:{initial_block_id}, accounts={}", operations.len());
        // tracing::trace!(target: "lock", "[{}] ACQUIRING pending.lock @ push_emerge", thread_name());
        let mut pending = self.pending.lock();
        // tracing::trace!(target: "lock", "[{}] ACQUIRED pending.lock @ push_emerge", thread_name());

        for (routing, account) in operations {
            pending.archive_update.insert(routing, account);
        }
        pending.archive_update.emerge_thread(thread_id, initial_block_id);

        pending.merkle_map_update.insert(thread_id, (initial_block_id, merkle_map.clone()));
        pending.blocks_accumulated += 1;
        self.report_queue_size(pending.blocks_accumulated);

        if self.is_full(&pending) {
            self.send_batch(&mut pending);
        }
        // tracing::trace!(target: "lock", "[{}] RELEASING pending.lock (scope exit) @ push_emerge", thread_name());
    }

    /// Push a collapse event into the queue.
    pub fn push_collapse(&self, thread_id: ThreadIdentifier) {
        // tracing::trace!(target: "lock", "[{}] ACQUIRING pending.lock @ push_collapse", thread_name());
        let mut pending = self.pending.lock();
        // tracing::trace!(target: "lock", "[{}] ACQUIRED pending.lock @ push_collapse", thread_name());

        pending.archive_update.collapse_thread(thread_id);
        pending.merkle_map_update.remove(&thread_id);
        pending.blocks_accumulated = pending.blocks_accumulated.saturating_sub(1);
        let live_threads = Self::live_threads(&pending);
        self.unfinalized_pool.write().drain_not_live(&live_threads);
        // tracing::trace!(target: "lock", "[{}] RELEASING pending.lock (scope exit) @ push_collapse", thread_name());
    }

    /// Signal the commit loop to stop.
    pub fn signal_stop(&self) {
        // tracing::trace!(target: "lock", "[{}] signal_stop: setting stop=true", thread_name());
        self.stop.store(true, Ordering::Relaxed);
        // Send whatever is pending, then drop the sender to unblock the receiver.
        // tracing::trace!(target: "lock", "[{}] ACQUIRING pending.lock @ signal_stop", thread_name());
        let mut pending = self.pending.lock();
        // tracing::trace!(target: "lock", "[{}] ACQUIRED pending.lock @ signal_stop", thread_name());
        if !pending.archive_update.is_empty() {
            self.send_batch(&mut pending);
        }
        // tracing::trace!(target: "lock", "[{}] RELEASING pending.lock @ signal_stop", thread_name());
    }

    /// Register a snapshot pin request. Latest acceptable wins; rejected
    /// requests are dropped and logged at WARN.
    ///
    /// The cheap acceptability check verifies that the anchor thread has
    /// not yet pushed past the anchor block id (i.e. the defer-apply
    /// boundary is still alignable). The reachability check is run lazily
    /// at `acquire_snapshot_pin` time.
    ///
    /// Should be called from the finalization path either *before* the
    /// next `push_transition(T_A, ...)` for the anchor thread (so the
    /// alignment hook in `push_transition` fires immediately and ships
    /// the boundary batch in the same critical section), OR right
    /// *after* (this method's late-flush hook will then catch the
    /// already-pushed transition and ship the batch on the spot). Both
    /// orderings work; the first is preferred because it's race-free.
    /// See `spec/SNAPSHOT.md` mitigation #1.
    pub fn request_snapshot_pin(self: &Arc<Self>, anchor: AnchorBlockRef) -> PinRequestGuard {
        let anchor_thread = anchor.anchor_thread_id;
        let anchor_block = anchor.anchor_block_id;

        // Set the slot.
        {
            let mut state = self.pin_state.lock();
            match &mut *state {
                SnapshotPinState::Idle => {
                    *state = SnapshotPinState::Requested(anchor);
                }
                SnapshotPinState::Requested(existing) => {
                    // Latest wins: the anchor before boundary crossing
                    // can still be replaced by a newer one cleanly.
                    *existing = anchor;
                }
                SnapshotPinState::BoundaryReached { .. }
                | SnapshotPinState::Pinned { .. }
                | SnapshotPinState::Releasing { .. } => {
                    // The current pin's boundary has already been crossed
                    // (or a snapshot is in progress / wrapping up). Queue
                    // this request as the next one; overwrite any existing
                    // next-slot entry (latest wins).
                    self.replace_next_slot(&mut state, anchor);
                    // Late-flush below is irrelevant for the next-slot
                    // case — the active pin owns the apply gate and the
                    // queued request will be promoted on Releasing.
                    return PinRequestGuard {
                        accumulator: Arc::clone(self),
                        anchor_thread,
                        anchor_block,
                        armed: true,
                    };
                }
            }
        }

        // Late-flush: if the producer already pushed the anchor's
        // transition into `pending` *before* this request landed (i.e.
        // the caller couldn't reorder push and request), ship that
        // batch now so the update loop reaches the boundary on its
        // next iteration instead of waiting for the next push or for
        // `is_full` to fire. This is the safety-net path; the common
        // case (request before push) is handled by the alignment hook
        // inside `push_transition`.
        //
        // Note on acceptability: we don't reject "too-late" requests
        // up front. We can't compare BlockIdentifier ordinally without
        // seq_no, so any check here would be best-effort and prone to
        // false-rejecting valid requests (e.g. when pending holds a
        // PRIOR block's transition that hasn't been shipped yet). The
        // acquire-side 60 s timeout is the atomic-skip safety net for
        // genuinely-late requests — the snapshot worker will see no
        // boundary and return None.
        let mut pending = self.pending.lock();
        let already_pushed = pending
            .archive_update
            .thread_transitions
            .get(&anchor_thread)
            .map(|t| t.new_block_id == anchor_block)
            .unwrap_or(false);
        if already_pushed {
            tracing::trace!(
                target: "monit",
                "request_snapshot_pin: late-flush — anchor already in pending; force-shipping batch",
            );
            self.send_batch(&mut pending);
            return PinRequestGuard {
                accumulator: Arc::clone(self),
                anchor_thread,
                anchor_block,
                armed: true,
            };
        }

        // Stale-network shortcut: if pending has nothing further for this
        // thread (no transition past the anchor) AND the archive's last
        // applied block for this thread is exactly the anchor, the
        // archive is *already* at the anchor's state. There's nothing to
        // ship and nothing to apply. Transition Requested → BoundaryReached
        // directly so the snapshot worker can take the pin immediately.
        //
        // Without this, a stale network (no new finalize_thread_transition
        // calls coming in) would leave `Requested` state forever — the
        // alignment hook never fires and there's no batch in the channel
        // for the update loop to apply.
        let pending_at_or_before_anchor = pending
            .expected_thread_blocks
            .get(&anchor_thread)
            .map(|b| *b == anchor_block)
            .unwrap_or(false)
            && !pending.archive_update.thread_transitions.contains_key(&anchor_thread);
        drop(pending);

        if !pending_at_or_before_anchor {
            return PinRequestGuard {
                accumulator: Arc::clone(self),
                anchor_thread,
                anchor_block,
                armed: true,
            };
        }

        let archive_at_anchor = self
            .applied_per_thread
            .lock()
            .get(&anchor_thread)
            .map(|b| *b == anchor_block)
            .unwrap_or(false);

        if !archive_at_anchor {
            return PinRequestGuard {
                accumulator: Arc::clone(self),
                anchor_thread,
                anchor_block,
                armed: true,
            };
        }

        let mut state = self.pin_state.lock();
        if let SnapshotPinState::Requested(active) = &*state {
            if active.anchor_thread_id == anchor_thread && active.anchor_block_id == anchor_block {
                tracing::trace!(
                    target: "monit",
                    "request_snapshot_pin: stale-network shortcut — archive already \
                     at anchor (T:{anchor_thread}, B:{anchor_block}); transitioning \
                     Requested → BoundaryReached directly",
                );
                let active = active.clone();
                *state = SnapshotPinState::BoundaryReached { active, next: None };
                self.pin_acquirable.notify_all();
            }
        }
        PinRequestGuard { accumulator: Arc::clone(self), anchor_thread, anchor_block, armed: true }
    }

    /// Helper: write into the `next` slot. Caller must already have
    /// confirmed the state is BoundaryReached / Pinned / Releasing.
    fn replace_next_slot(
        &self,
        state: &mut parking_lot::MutexGuard<'_, SnapshotPinState>,
        anchor: AnchorBlockRef,
    ) {
        match &mut **state {
            SnapshotPinState::BoundaryReached { next, .. }
            | SnapshotPinState::Pinned { next, .. }
            | SnapshotPinState::Releasing { next } => {
                *next = Some(anchor);
            }
            _ => {
                // Defensive: Idle/Requested shouldn't reach here, but if
                // they do, fall back to a normal Requested transition.
                **state = SnapshotPinState::Requested(anchor);
            }
        }
    }

    fn cancel_snapshot_pin_locked(
        &self,
        state: &mut parking_lot::MutexGuard<'_, SnapshotPinState>,
        expected_thread: ThreadIdentifier,
        expected_block: BlockIdentifier,
    ) -> bool {
        let anchor_matches = |a: &AnchorBlockRef| {
            a.anchor_thread_id == expected_thread && a.anchor_block_id == expected_block
        };
        match std::mem::replace(&mut **state, SnapshotPinState::Idle) {
            SnapshotPinState::Requested(active) if anchor_matches(&active) => {
                **state = SnapshotPinState::Releasing { next: None };
                true
            }
            SnapshotPinState::BoundaryReached { active, next } if anchor_matches(&active) => {
                **state = SnapshotPinState::Releasing { next };
                true
            }
            other => {
                **state = other;
                false
            }
        }
    }

    pub fn cancel_snapshot_pin(
        &self,
        expected_thread: ThreadIdentifier,
        expected_block: BlockIdentifier,
    ) {
        let mut state = self.pin_state.lock();
        if self.cancel_snapshot_pin_locked(&mut state, expected_thread, expected_block) {
            self.pin_released.notify_all();
        }
    }

    /// Wait until the defer-apply pin entry for the **expected anchor**
    /// `(expected_thread, expected_block)` has been reached, then run
    /// reachability checks. Returns a `PinHandle` on success; `None` on
    /// timeout, anchor mismatch, or any unrecoverable reachability
    /// failure.
    ///
    /// Anchor verification is mandatory: a worker is snapshotting one
    /// specific block, and the pin-state machine may currently be holding
    /// a different anchor (e.g. another finalize-path snapshot just
    /// landed while this worker was waiting). Without the match check,
    /// the worker would grab the wrong pin and try to export against an
    /// optimistic state that doesn't agree with the frozen archive.
    /// On mismatch we skip atomically without touching the state — the
    /// rightful owner of that pin still gets it.
    ///
    /// While the returned handle is alive, the update loop will not
    /// advance the archive past the pinned block.
    fn acquire_snapshot_pin_impl<F>(
        self: &Arc<Self>,
        expected_thread: ThreadIdentifier,
        expected_block: BlockIdentifier,
        timeout: Duration,
        should_cancel: F,
        max_wait_slice: Option<Duration>,
    ) -> Option<PinHandle>
    where
        F: Fn() -> bool,
    {
        let deadline = Instant::now() + timeout;
        let anchor_matches = |a: &AnchorBlockRef| {
            a.anchor_thread_id == expected_thread && a.anchor_block_id == expected_block
        };

        let mut state = self.pin_state.lock();
        loop {
            if should_cancel() {
                if self.cancel_snapshot_pin_locked(&mut state, expected_thread, expected_block) {
                    self.pin_released.notify_all();
                }
                return None;
            }
            match &*state {
                SnapshotPinState::Idle => {
                    // No request was made for any anchor; specifically
                    // not ours. Atomic skip — caller will not produce a
                    // snapshot file.
                    tracing::trace!(
                        target: "monit",
                        "acquire_snapshot_pin: state is Idle; no request for \
                         anchor {expected_block:?}",
                    );
                    return None;
                }
                SnapshotPinState::Requested(active) => {
                    if !anchor_matches(active) {
                        tracing::trace!(
                            target: "monit",
                            "acquire_snapshot_pin: Requested anchor {:?} doesn't \
                             match expected {expected_block:?}; skipping",
                            active.anchor_block_id,
                        );
                        return None;
                    }
                    // Matches — fall through to wait.
                }
                SnapshotPinState::BoundaryReached { active, .. } => {
                    if !anchor_matches(active) {
                        tracing::trace!(
                            target: "monit",
                            "acquire_snapshot_pin: BoundaryReached anchor {:?} doesn't \
                             match expected {expected_block:?}; skipping",
                            active.anchor_block_id,
                        );
                        return None;
                    }
                    break;
                }
                SnapshotPinState::Pinned { active, .. } => {
                    tracing::error!(
                        target: "monit",
                        "acquire_snapshot_pin: pin already held for anchor {:?}; \
                         only one snapshot worker is supported",
                        active.anchor_block_id,
                    );
                    return None;
                }
                SnapshotPinState::Releasing { next } => {
                    // After `finish_release`, a queued `next` is promoted
                    // to `Requested(next)`. Wait only if that next anchor
                    // is ours; otherwise skip.
                    match next {
                        Some(next_anchor) if anchor_matches(next_anchor) => {
                            // Will be promoted to Requested(matching); wait.
                        }
                        _ => {
                            tracing::trace!(
                                target: "monit",
                                "acquire_snapshot_pin: Releasing with next={:?} doesn't \
                                 match expected {expected_block:?}; skipping",
                                next.as_ref().map(|a| a.anchor_block_id),
                            );
                            return None;
                        }
                    }
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                if self.cancel_snapshot_pin_locked(&mut state, expected_thread, expected_block) {
                    self.pin_released.notify_all();
                }
                return None;
            }
            let wait_for = max_wait_slice.map(|slice| remaining.min(slice)).unwrap_or(remaining);
            let res = self.pin_acquirable.wait_for(&mut state, wait_for);
            if res.timed_out() && wait_for == remaining {
                if self.cancel_snapshot_pin_locked(&mut state, expected_thread, expected_block) {
                    self.pin_released.notify_all();
                }
                return None;
            }
        }

        // BoundaryReached observed and matches the expected anchor. Pull
        // the active anchor out under the lock so we have a stable value
        // for the reachability check below.
        let active = match &*state {
            SnapshotPinState::BoundaryReached { active, .. } => active.clone(),
            _ => unreachable!("loop above only breaks on BoundaryReached"),
        };
        // Drop the lock for the (potentially expensive) reachability check.
        drop(state);

        if !self.check_reachability(&active) {
            tracing::warn!(
                target: "monit",
                "acquire_snapshot_pin: reachability check failed for anchor {:?}; skipping snapshot",
                active.anchor_block_id,
            );
            // Atomic-skip: transition to Releasing so the loop drains
            // any deferred batches it accumulated. Only do this if our
            // anchor is still the active one — guard against a race
            // where the state changed under us.
            let mut state = self.pin_state.lock();
            let next = match std::mem::replace(&mut *state, SnapshotPinState::Idle) {
                SnapshotPinState::BoundaryReached { active, next } if anchor_matches(&active) => {
                    next
                }
                other => {
                    *state = other;
                    return None;
                }
            };
            *state = SnapshotPinState::Releasing { next };
            self.pin_released.notify_all();
            return None;
        }

        // Transition BoundaryReached → Pinned, re-verifying anchor under
        // the lock.
        let mut state = self.pin_state.lock();
        let (active, next) = match std::mem::replace(&mut *state, SnapshotPinState::Idle) {
            SnapshotPinState::BoundaryReached { active, next } if anchor_matches(&active) => {
                (active, next)
            }
            other => {
                // State changed under us (or anchor no longer ours).
                *state = other;
                return None;
            }
        };
        *state = SnapshotPinState::Pinned { active: active.clone(), next };
        Some(PinHandle { accumulator: Arc::clone(self), anchor: active })
    }

    pub fn acquire_snapshot_pin(
        self: &Arc<Self>,
        expected_thread: ThreadIdentifier,
        expected_block: BlockIdentifier,
        timeout: Duration,
    ) -> Option<PinHandle> {
        self.acquire_snapshot_pin_impl(expected_thread, expected_block, timeout, || false, None)
    }

    pub fn acquire_snapshot_pin_cancellable<F>(
        self: &Arc<Self>,
        expected_thread: ThreadIdentifier,
        expected_block: BlockIdentifier,
        timeout: Duration,
        should_cancel: F,
    ) -> Option<PinHandle>
    where
        F: Fn() -> bool,
    {
        self.acquire_snapshot_pin_impl(
            expected_thread,
            expected_block,
            timeout,
            should_cancel,
            Some(Duration::from_secs(1)),
        )
    }

    /// Called by `PinHandle::drop`. Transitions Pinned → Releasing and
    /// notifies the update loop to drain its deferred queue.
    pub(crate) fn release_pin(&self) {
        let mut state = self.pin_state.lock();
        match std::mem::replace(&mut *state, SnapshotPinState::Idle) {
            SnapshotPinState::Pinned { next, .. } => {
                *state = SnapshotPinState::Releasing { next };
            }
            other => {
                // Drop without prior Pinned state — should not happen.
                tracing::warn!(
                    target: "monit",
                    "release_pin: called while not in Pinned state",
                );
                *state = other;
            }
        }
        self.pin_released.notify_all();
    }

    /// Called by the update loop after draining all deferred batches.
    /// Transitions Releasing → Idle, or → Requested(next) if a fresh
    /// request landed during the snapshot. Always promotes the next
    /// anchor unconditionally; if it's no longer alignable, the next
    /// `acquire_snapshot_pin` call will time out and skip the snapshot
    /// atomically (per the no-up-front-rejection policy in
    /// `request_snapshot_pin`).
    pub(crate) fn finish_release(&self) {
        let mut state = self.pin_state.lock();
        if let SnapshotPinState::Releasing { next: Some(next_anchor) } =
            std::mem::replace(&mut *state, SnapshotPinState::Idle)
        {
            *state = SnapshotPinState::Requested(next_anchor);
            self.pin_acquirable.notify_all();
        }
    }

    /// Called by the update loop when it has just applied the boundary
    /// batch for the currently-Requested anchor. Transitions
    /// Requested → BoundaryReached and signals `pin_acquirable`.
    pub(crate) fn mark_boundary_reached(&self) {
        let mut state = self.pin_state.lock();
        match std::mem::replace(&mut *state, SnapshotPinState::Idle) {
            SnapshotPinState::Requested(active) => {
                *state = SnapshotPinState::BoundaryReached { active, next: None };
                self.pin_acquirable.notify_all();
            }
            other => {
                // Caller must have observed Requested via `defer_decision`
                // and we're racing with another transition. Restore and
                // skip the signal.
                *state = other;
            }
        }
    }

    /// Read-only accessor for the active anchor when it should affect
    /// the apply step. Used by the update loop to decide whether to
    /// defer a batch and whether the just-applied batch was the boundary.
    pub(crate) fn apply_decision_anchor(&self) -> Option<ApplyDecision> {
        let state = self.pin_state.lock();
        match &*state {
            SnapshotPinState::Idle => None,
            SnapshotPinState::Requested(a) => Some(ApplyDecision {
                anchor_thread: a.anchor_thread_id,
                anchor_block: a.anchor_block_id,
                defer_past_anchor: false, // boundary not yet crossed; apply normally
            }),
            SnapshotPinState::BoundaryReached { active, .. }
            | SnapshotPinState::Pinned { active, .. } => Some(ApplyDecision {
                anchor_thread: active.anchor_thread_id,
                anchor_block: active.anchor_block_id,
                defer_past_anchor: true,
            }),
            SnapshotPinState::Releasing { .. } => None,
        }
    }

    /// True iff the slot is currently in `Releasing`. Used by the update
    /// loop to drain its deferred queue and call `finish_release`.
    pub(crate) fn is_releasing(&self) -> bool {
        matches!(*self.pin_state.lock(), SnapshotPinState::Releasing { .. })
    }

    /// Read-only accessor for the requested-but-not-yet-pinned anchor (if
    /// any). Used by `push_transition` to decide whether to force-flush
    /// the batch boundary at the anchor. Includes the next-slot anchor
    /// when a snapshot is currently in flight (the next snapshot still
    /// needs alignment).
    pub(crate) fn requested_anchor(&self) -> Option<(ThreadIdentifier, BlockIdentifier)> {
        let state = self.pin_state.lock();
        match &*state {
            SnapshotPinState::Requested(a) => Some((a.anchor_thread_id, a.anchor_block_id)),
            SnapshotPinState::BoundaryReached { next: Some(n), .. }
            | SnapshotPinState::Pinned { next: Some(n), .. }
            | SnapshotPinState::Releasing { next: Some(n) } => {
                Some((n.anchor_thread_id, n.anchor_block_id))
            }
            _ => None,
        }
    }

    /// Reachability check for every pin entry. Returns true iff every
    /// routing in every pinned thread's map at the pinned block id has
    /// its expected hash recoverable from pool/accumulator/archive.
    ///
    /// **Currently a stub returning `true`.** The actual check needs
    /// access to per-thread `accounts_map` files and the composite
    /// repository's lookup chain — both held outside the accumulator.
    /// The repository-level wrapper around `acquire_snapshot_pin` is
    /// where reachability is actually validated; once that wrapper
    /// lands, the stub here can be removed and the check moved up. For
    /// now this means `acquire_snapshot_pin` succeeds whenever the
    /// defer-apply boundary is reached, even if ref bytes have been
    /// evicted — to be fixed in the next commit that wires the
    /// snapshot service.
    fn check_reachability(&self, _anchor: &AnchorBlockRef) -> bool {
        true
    }

    /// Block until `in_flight` and `batch_tx` are empty. Does NOT block
    /// new sends and does NOT send the current pending batch. Returns true
    /// if the drain completed within `timeout`, false on timeout.
    pub fn wait_for_drain(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut guard = self.drained_mutex.lock();
        while !self.in_flight.read().is_empty() || !self.batch_tx.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            if self.drained.wait_for(&mut guard, remaining).timed_out() {
                return self.in_flight.read().is_empty() && self.batch_tx.is_empty();
            }
        }
        true
    }

    /// Send the current pending batch, then wait until all sent and
    /// in-flight batches are applied. Does NOT block new sends.
    pub fn flush_pending_and_wait_for_drain(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        {
            let mut pending = self.pending.lock();
            if !pending.archive_update.is_empty() {
                self.send_batch(&mut pending);
            }
        }
        self.wait_for_drain(deadline.saturating_duration_since(Instant::now()))
    }

    /// Block new batches from being sent, drain all pending and in-flight batches,
    /// then return a guard. While the guard is alive, no new batches will reach
    /// the update loop — producers can still accumulate into `pending` but
    /// `send_batch` is a no-op until the guard is dropped.
    pub fn flush(&self) -> FlushGuard<'_> {
        // tracing::trace!(target: "lock", "[{}] flush: setting send_blocked=true", thread_name());
        // Block future sends BEFORE draining so nothing sneaks in between.
        self.send_blocked.store(true, Ordering::Release);

        // Send the current pending batch if non-empty. We temporarily unblock
        // to send exactly this one batch.
        // tracing::trace!(target: "lock", "[{}] ACQUIRING pending.lock @ flush/send", thread_name());
        {
            let mut pending = self.pending.lock();
            // tracing::trace!(target: "lock", "[{}] ACQUIRED pending.lock @ flush/send, blocks_accumulated={}", thread_name(), pending.blocks_accumulated);
            if !pending.archive_update.is_empty() {
                self.send_blocked.store(false, Ordering::Release);
                self.send_batch(&mut pending);
                self.send_blocked.store(true, Ordering::Release);
            }
            // tracing::trace!(target: "lock", "[{}] RELEASING pending.lock @ flush/send", thread_name());
        }

        // Wait until in-flight and channel are empty.
        {
            // tracing::trace!(target: "lock", "[{}] flush: waiting for drain (in_flight={} channel={})", thread_name(), self.in_flight.read().len(), self.batch_tx.len());
            let mut lock = self.drained_mutex.lock();
            while !self.in_flight.read().is_empty() || !self.batch_tx.is_empty() {
                // tracing::trace!(target: "lock", "[{}] flush: WAITING drained (in_flight={} channel={})", thread_name(), self.in_flight.read().len(), self.batch_tx.len());
                self.drained.wait(&mut lock);
                // tracing::trace!(target: "lock", "[{}] flush: WOKE drained (in_flight={} channel={})", thread_name(), self.in_flight.read().len(), self.batch_tx.len());
            }
        }

        // tracing::trace!(target: "lock", "[{}] flush: drained, returning FlushGuard", thread_name());
        FlushGuard { accumulator: self }
    }

    /// Called by update_loop after processing a batch — removes it from in_flight.
    fn complete_batch(&self, batch: &Arc<PendingUpdate>) {
        // let in_flight_before = self.in_flight.read().len();
        self.in_flight.write().retain(|b| !Arc::ptr_eq(b, batch));
        // let in_flight_after = self.in_flight.read().len();
        // tracing::trace!(
        //     target: "lock",
        //     "[{}] complete_batch: in_flight {} -> {}, channel={}, NOTIFY drained",
        //     thread_name(),
        //     in_flight_before,
        //     in_flight_after,
        //     self.batch_tx.len(),
        // );
        self.drained.notify_all();
    }
}

fn build_next_pending_update(prev: &PendingUpdate) -> PendingUpdate {
    let collapsed = &prev.archive_update.collapsing_threads;
    let mut next = PendingUpdate::new(&[]);
    for (thread_id, block_id) in &prev.expected_thread_blocks {
        if !collapsed.contains(thread_id) {
            next.expected_thread_blocks.insert(*thread_id, *block_id);
        }
    }
    for (thread_id, initial_block_id) in &prev.archive_update.emerging_threads {
        if !collapsed.contains(thread_id) {
            next.expected_thread_blocks.insert(*thread_id, *initial_block_id);
        }
    }
    for (thread_id, transition) in &prev.archive_update.thread_transitions {
        if !collapsed.contains(thread_id) {
            next.expected_thread_blocks.insert(*thread_id, transition.new_block_id);
        }
    }
    next
}

fn apply_pending_update(
    pending: &PendingUpdate,
    archive: &ArchiveStateStore,
    merkle_map_store: &mut MerkleMapStore,
) -> anyhow::Result<()> {
    let merkle_map_update = pending
        .merkle_map_update
        .iter()
        .map(|(thread_id, (block_id, map))| ((*block_id, *thread_id), map.clone()))
        .collect::<HashMap<_, _>>();
    let update_keys: HashSet<_> = merkle_map_update.keys().copied().collect();

    let maps_on_disk = merkle_map_store.list();

    merkle_map_store.append(merkle_map_update)?;

    archive.apply_update(&pending.archive_update)?;

    let mut needed: HashSet<_> = archive
        .get_all_thread_control_states()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|state| state.last_block_id.map(|block_id| (block_id, state.thread_id)))
        .collect();
    needed.extend(update_keys);

    let stale: HashSet<_> = maps_on_disk.difference(&needed).copied().collect();
    if !stale.is_empty() {
        tracing::trace!(target: "monit", "Removing {} stale map files", stale.len());
        merkle_map_store.remove(stale);
    }
    Ok(())
}

/// RAII handle for an active snapshot pin. While alive, holds the pin in
/// the `Pinned` state. Dropping it transitions to `Releasing`, which
/// signals the update loop to drain its deferred queue.
///
/// The handle owns an `Arc<ArchiveUpdateAccumulator>` so it can be sent
/// across threads — captured at finalize time and handed to the snapshot
/// worker. The handle exposes `pin_set()` and `anchor_block_id()` for the
/// snapshot service to drive the per-thread sub-snapshot writes.
pub struct PinHandle {
    accumulator: Arc<ArchiveUpdateAccumulator>,
    anchor: AnchorBlockRef,
}

impl PinHandle {
    pub fn anchor_block_id(&self) -> &BlockIdentifier {
        &self.anchor.anchor_block_id
    }

    pub fn anchor_thread_id(&self) -> &ThreadIdentifier {
        &self.anchor.anchor_thread_id
    }

    pub fn pin_set(&self) -> HashMap<ThreadIdentifier, BlockIdentifier> {
        self.anchor.pin_set()
    }

    pub fn anchor(&self) -> &AnchorBlockRef {
        &self.anchor
    }
}

impl Drop for PinHandle {
    fn drop(&mut self) {
        self.accumulator.release_pin();
    }
}

/// Returns true iff the batch contains a transition for `thread` whose
/// `new_block_id` differs from `block`. False if the batch doesn't touch
/// `thread`.
fn batch_advances_thread_past(
    batch: &PendingUpdate,
    thread: ThreadIdentifier,
    block: BlockIdentifier,
) -> bool {
    batch
        .archive_update
        .thread_transitions
        .get(&thread)
        .map(|t| t.new_block_id != block)
        .unwrap_or(false)
}

/// Returns true iff the batch's transition for `thread` ends at exactly
/// `block`. The boundary batch produced by the producer-side alignment.
fn batch_ends_thread_at(
    batch: &PendingUpdate,
    thread: ThreadIdentifier,
    block: BlockIdentifier,
) -> bool {
    batch
        .archive_update
        .thread_transitions
        .get(&thread)
        .map(|t| t.new_block_id == block)
        .unwrap_or(false)
}

/// Update loop — runs in a background thread.
pub fn update_loop(
    accumulator: Arc<ArchiveUpdateAccumulator>,
    archive: ArchiveStateStore,
    mut maps_store: MerkleMapStore,
) -> anyhow::Result<()> {
    maps_store.init()?;

    tracing::trace!(
        target: "monit",
        "[{}] update_loop: started with blocks: {:?}",
        thread_name(),
        accumulator.pending.lock().expected_thread_blocks,
    );

    // Batches deferred while a snapshot pin is active. Drained in FIFO
    // order when the pin transitions to Releasing.
    let mut deferred: std::collections::VecDeque<Arc<PendingUpdate>> =
        std::collections::VecDeque::new();
    accumulator.report_deferred_len(deferred.len());

    loop {
        // Phase 1: drain deferred queue if a pin was just released.
        // Drain runs to completion before we resume normal apply.
        if accumulator.is_releasing() {
            // tracing::trace!(
            //     target: "lock",
            //     "[{}] update_loop: draining {} deferred batches @ Releasing",
            //     thread_name(),
            //     deferred.len(),
            // );
            while let Some(batch) = deferred.pop_front() {
                accumulator.report_deferred_len(deferred.len());
                if !batch.archive_update.is_empty() {
                    apply_pending_update(&batch, &archive, &mut maps_store)?;
                    let mut applied = accumulator.applied_per_thread.lock();
                    for (thread_id, transition) in &batch.archive_update.thread_transitions {
                        applied.insert(*thread_id, transition.new_block_id);
                    }
                    for (thread_id, initial_block_id) in &batch.archive_update.emerging_threads {
                        applied.insert(*thread_id, *initial_block_id);
                    }
                    for thread_id in &batch.archive_update.collapsing_threads {
                        applied.remove(thread_id);
                    }
                }
                accumulator.complete_batch(&batch);
            }
            accumulator.finish_release();
            accumulator.report_deferred_len(deferred.len());
            // After finish_release the slot may now be Requested(next).
            // Loop continues; the next iteration will pick it up.
        }

        // Receive next batch, with timeout to check stop flag
        // tracing::trace!(target: "lock", "[{}] update_loop: WAITING batch_rx (channel={})", thread_name(), accumulator.batch_rx.len());
        let batch = match accumulator.batch_rx.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(batch) => {
                // tracing::trace!(target: "lock", "[{}] update_loop: RECEIVED batch (channel={})", thread_name(), accumulator.batch_rx.len());
                batch
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if accumulator.stop.load(Ordering::Relaxed) {
                    // tracing::trace!(target: "lock", "[{}] update_loop: stop signaled, breaking", thread_name());
                    break;
                }
                continue;
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                // tracing::trace!(target: "lock", "[{}] update_loop: channel disconnected, breaking", thread_name());
                break;
            }
        };

        if batch.archive_update.is_empty() {
            // tracing::trace!(target: "lock", "[{}] update_loop: empty batch, completing without apply", thread_name());
            accumulator.complete_batch(&batch);
            continue;
        }

        // Phase 2: snapshot-pin apply gate.
        let decision = accumulator.apply_decision_anchor();
        let mut signal_boundary = false;
        if let Some(d) = &decision {
            if d.defer_past_anchor
                && batch_advances_thread_past(&batch, d.anchor_thread, d.anchor_block)
            {
                tracing::trace!(
                    target: "monit",
                    "update_loop: deferring batch (advances anchor thread past pinned block)",
                );
                deferred.push_back(batch);
                accumulator.report_deferred_len(deferred.len());
                continue;
            }
            // Either we don't defer past-anchor yet (Requested), or this
            // batch doesn't advance past. Apply normally. If this batch
            // ends the anchor thread at exactly `anchor_block` AND the
            // state is Requested, this is the boundary batch — signal
            // pin_acquirable after applying.
            if !d.defer_past_anchor && batch_ends_thread_at(&batch, d.anchor_thread, d.anchor_block)
            {
                signal_boundary = true;
            }
        }

        tracing::info!(
            target: "mem",
            "update_loop: took batch with {} ops, {} maps, {} blocks",
            batch.archive_update.operations.len(),
            batch.merkle_map_update.len(),
            batch.blocks_accumulated,
        );
        accumulator.report_mem_stats("before_apply");

        // tracing::trace!(target: "lock", "[{}] update_loop: APPLYING batch", thread_name());
        let result = apply_pending_update(&batch, &archive, &mut maps_store);
        // tracing::trace!(target: "lock", "[{}] update_loop: APPLIED batch, result={:?}", thread_name(), result.is_ok());
        if result.is_err() {
            tracing::error!("update_loop error: {:?}", result);
        } else {
            // Track the latest applied block per thread so the
            // stale-network shortcut in `request_snapshot_pin` can
            // verify that the archive is at the requested anchor.
            let mut applied = accumulator.applied_per_thread.lock();
            for (thread_id, transition) in &batch.archive_update.thread_transitions {
                applied.insert(*thread_id, transition.new_block_id);
            }
            for (thread_id, initial_block_id) in &batch.archive_update.emerging_threads {
                applied.insert(*thread_id, *initial_block_id);
            }
            for thread_id in &batch.archive_update.collapsing_threads {
                applied.remove(thread_id);
            }
        }

        accumulator.complete_batch(&batch);
        accumulator.report_mem_stats("after_apply");
        if signal_boundary {
            accumulator.mark_boundary_reached();
        }
        result?;
    }

    // Final flush: process remaining batches in channel
    // tracing::trace!(target: "lock", "[{}] update_loop: final flush (channel={})", thread_name(), accumulator.batch_rx.len());
    while let Ok(batch) = accumulator.batch_rx.try_recv() {
        if !batch.archive_update.is_empty() {
            apply_pending_update(&batch, &archive, &mut maps_store)?;
        }
        accumulator.complete_batch(&batch);
    }

    // Process any unsent pending data
    {
        // tracing::trace!(target: "lock", "[{}] ACQUIRING pending.lock @ update_loop/final_unsent", thread_name());
        let mut pending = accumulator.pending.lock();
        // tracing::trace!(target: "lock", "[{}] ACQUIRED pending.lock @ update_loop/final_unsent, blocks_accumulated={}", thread_name(), pending.blocks_accumulated);
        let remaining = std::mem::replace(&mut *pending, PendingUpdate::new(&[]));
        if !remaining.archive_update.is_empty() {
            apply_pending_update(&remaining, &archive, &mut maps_store)?;
        }
        // tracing::trace!(target: "lock", "[{}] RELEASING pending.lock @ update_loop/final_unsent", thread_name());
    }

    tracing::trace!(target: "monit", "[{}] update_loop: exiting", thread_name());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::thread as std_thread;
    use std::time::Duration;

    use super::*;

    fn hash(seed: u8) -> AccountHash {
        AccountHash::new([seed; 32])
    }

    fn thread(seed: u8) -> ThreadIdentifier {
        ThreadIdentifier::new(&BlockIdentifier::new([seed; 32]), seed as u16)
    }

    fn insert(
        pool: &mut UnfinalizedAccountsPool,
        hash: AccountHash,
        thread_id: ThreadIdentifier,
        block_height: u64,
    ) {
        pool.insert_batch(thread_id, block_height, [(hash, ThreadAccount::default())]);
    }

    fn anchor(seed: u8) -> AnchorBlockRef {
        AnchorBlockRef {
            anchor_block_id: BlockIdentifier::new([seed; 32]),
            anchor_thread_id: thread(seed),
            cross_thread_refs: HashMap::new(),
        }
    }

    #[test]
    fn unfinalized_pool_drains_finalized_height_inclusive() {
        let thread_id = thread(1);
        let other_thread = thread(2);
        let mut pool = UnfinalizedAccountsPool::new();

        insert(&mut pool, hash(1), thread_id, 1);
        insert(&mut pool, hash(2), thread_id, 2);
        insert(&mut pool, hash(3), thread_id, 3);
        insert(&mut pool, hash(4), other_thread, 1);

        pool.drain_finalized(thread_id, 2);

        assert!(pool.get(&hash(1)).is_some());
        assert!(pool.get(&hash(2)).is_some());
        assert!(pool.get(&hash(3)).is_some());
        assert!(pool.get(&hash(4)).is_some());

        pool.drain_finalized(thread_id, 3);

        assert!(pool.get(&hash(1)).is_none());
        assert!(pool.get(&hash(2)).is_none());
        assert!(pool.get(&hash(3)).is_none());
        assert!(pool.get(&hash(4)).is_some());
    }

    #[test]
    fn unfinalized_pool_drains_non_live_threads() {
        let live_thread = thread(1);
        let collapsed_thread = thread(2);
        let mut pool = UnfinalizedAccountsPool::new();

        insert(&mut pool, hash(1), live_thread, 1);
        insert(&mut pool, hash(2), collapsed_thread, 1);

        let live_threads = HashSet::from([live_thread]);
        pool.drain_not_live(&live_threads);

        assert!(pool.get(&hash(1)).is_some());
        assert!(pool.get(&hash(2)).is_none());
    }

    #[test]
    fn unfinalized_pool_reinsert_updates_active_upper_height() {
        let thread_id = thread(1);
        let mut pool = UnfinalizedAccountsPool::new();
        let hash = hash(7);

        insert(&mut pool, hash, thread_id, 1);
        insert(&mut pool, hash, thread_id, 5);

        let thread_pool = pool.threads.get(&thread_id).unwrap();
        assert_eq!(thread_pool.active.upper_height, 5);
        assert_eq!(thread_pool.active.len(), 1);
        assert!(thread_pool.draining.is_empty());
    }

    #[test]
    fn unfinalized_pool_moves_active_to_draining_when_draining_is_empty() {
        let thread_id = thread(1);
        let mut pool = UnfinalizedAccountsPool::new();

        insert(&mut pool, hash(1), thread_id, 1);
        insert(&mut pool, hash(2), thread_id, 3);
        pool.drain_finalized(thread_id, 2);

        let thread_pool = pool.threads.get(&thread_id).unwrap();
        assert!(thread_pool.active.is_empty());
        assert_eq!(thread_pool.draining.upper_height, 3);
        assert_eq!(thread_pool.draining.len(), 2);
    }

    #[test]
    fn unfinalized_pool_finalized_height_can_clear_draining_and_active() {
        let thread_id = thread(1);
        let mut pool = UnfinalizedAccountsPool::new();

        insert(&mut pool, hash(1), thread_id, 1);
        insert(&mut pool, hash(2), thread_id, 3);
        pool.drain_finalized(thread_id, 2);

        insert(&mut pool, hash(3), thread_id, 4);
        insert(&mut pool, hash(4), thread_id, 7);
        pool.drain_finalized(thread_id, 10);

        assert!(!pool.threads.contains_key(&thread_id));
    }

    #[test]
    fn pin_request_guard_drop_cancels_requested_pin() {
        let accumulator = Arc::new(ArchiveUpdateAccumulator::new(1, &[], None));
        let anchor = anchor(9);

        let guard = accumulator.request_snapshot_pin(anchor.clone());
        drop(guard);

        assert!(matches!(
            *accumulator.pin_state.lock(),
            SnapshotPinState::Releasing { next: None }
        ));
        accumulator.finish_release();
        assert!(matches!(*accumulator.pin_state.lock(), SnapshotPinState::Idle));
    }

    #[test]
    fn cancel_snapshot_pin_preserves_non_matching_request() {
        let accumulator = Arc::new(ArchiveUpdateAccumulator::new(1, &[], None));
        let active = anchor(1);
        let other = anchor(2);

        let _guard = accumulator.request_snapshot_pin(active.clone());
        accumulator.cancel_snapshot_pin(other.anchor_thread_id, other.anchor_block_id);

        let state = accumulator.pin_state.lock();
        match &*state {
            SnapshotPinState::Requested(requested) => {
                assert_eq!(requested.anchor_thread_id, active.anchor_thread_id);
                assert_eq!(requested.anchor_block_id, active.anchor_block_id);
            }
            _ => panic!("unexpected state after non-matching cancel"),
        };
    }

    #[test]
    fn acquire_timeout_cancels_boundary_reached_pin() {
        let accumulator = Arc::new(ArchiveUpdateAccumulator::new(1, &[], None));
        let anchor = anchor(7);
        let guard = accumulator.request_snapshot_pin(anchor.clone());
        std::mem::forget(guard);

        let worker_accumulator = Arc::clone(&accumulator);
        let worker_anchor = anchor.clone();
        let updater = std_thread::Builder::new()
            .name("test".to_string())
            .spawn(move || {
                std_thread::sleep(Duration::from_millis(10));
                let mut state = worker_accumulator.pin_state.lock();
                *state = SnapshotPinState::BoundaryReached { active: worker_anchor, next: None };
            })
            .expect("failed to spawn worker thread");

        let pin = accumulator.acquire_snapshot_pin(
            anchor.anchor_thread_id,
            anchor.anchor_block_id,
            Duration::from_millis(50),
        );

        updater.join().unwrap();
        assert!(pin.is_none());
        assert!(matches!(
            *accumulator.pin_state.lock(),
            SnapshotPinState::Releasing { next: None }
        ));
        accumulator.finish_release();
        assert!(matches!(*accumulator.pin_state.lock(), SnapshotPinState::Idle));
    }

    #[test]
    fn cancellable_acquire_does_not_cancel_on_poll_slice_timeout() {
        let accumulator = Arc::new(ArchiveUpdateAccumulator::new(1, &[], None));
        let anchor = anchor(8);
        let guard = accumulator.request_snapshot_pin(anchor.clone());
        std::mem::forget(guard);

        let worker_accumulator = Arc::clone(&accumulator);
        let worker_anchor = anchor.clone();
        let worker = std_thread::Builder::new()
            .name("test".to_string())
            .spawn(move || {
                worker_accumulator.acquire_snapshot_pin_cancellable(
                    worker_anchor.anchor_thread_id,
                    worker_anchor.anchor_block_id,
                    Duration::from_millis(1300),
                    || false,
                )
            })
            .expect("failed to spawn worker thread");

        std_thread::sleep(Duration::from_millis(1050));
        assert!(matches!(*accumulator.pin_state.lock(), SnapshotPinState::Requested(_)));

        assert!(worker.join().unwrap().is_none());
        assert!(matches!(
            *accumulator.pin_state.lock(),
            SnapshotPinState::Releasing { next: None }
        ));
    }
}
