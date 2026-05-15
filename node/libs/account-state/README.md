# Specification: `archive-state-store` (multi-thread revision)

**Library for double-buffered, parallel-write node archive account state in Aerospike,
with support for multiple concurrent blockchain threads.**

Supersedes the single-thread `archive-state-store` spec. Differences from that spec are
called out explicitly. The baseline `dual-copy-state-store` remains unchanged.

---

## 1. Purpose

`archive-state-store` maintains two physical copies of a node archive's account state in
Aerospike. In the multi-thread world, a node archive tracks multiple concurrent
blockchain threads. Each blockchain thread has its own independent lifecycle (it can
emerge and collapse) and its own update history tracked via a per-thread Control Record.

The library guarantees:

* At least one copy is always `Ready` for any given thread, enabling lock-free reads at
  any point including during active updates.
* An `AccumulatedUpdate` can span multiple blockchain threads atomically from the
  library's perspective — all affected threads are validated and claimed before any data
  is written.
* Concurrent `apply_update` calls on **disjoint** sets of blockchain threads proceed
  without interference.
* The library never moves account data regardless of threads emerging or collapsing.
  Which thread a routing is associated with at any given time is entirely the client's
  concern; the library only stores and retrieves values.
* `ArchiveStateStore` is cheap to clone and safe to share across OS threads.

---

## 2. Terminology

| Term                      | Meaning                                                                                                                                                                                                                                                       |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Blockchain Thread**     | A logical execution thread in the node's consensus layer. Multiple blockchain threads can be active simultaneously. Not to be confused with OS threads.                                                                                                       |
| **Copy**                  | One complete snapshot of all account routing → value pairs across all active blockchain threads. Two copies exist: `CopyA` and `CopyB`.                                                                                                                       |
| **Ready**                 | A per-thread copy status: fully consistent and safe to read for that thread's routings.                                                                                                                                                                       |
| **Dirty**                 | A per-thread copy status: currently being rewritten; must not be used as a canonical read source for that thread's routings.                                                                                                                                  |
| **Thread Control Record** | A single Aerospike record per blockchain thread that atomically encodes that thread's `Ready`/`Dirty` copy status, update phase, and the block identifier of the last applied block.                                                                          |
| **System Record**         | A single Aerospike record per namespace storing the registry of active blockchain threads and the current global `data_epoch`.                                                                                                                                |
| **data_epoch**            | A single global `u64` stored in the System Record. Incremented atomically by `reset()` for all threads at once. Written as a bin on every account data record so the janitor can identify and delete all records belonging to the previous epoch in one pass. |
| **ThreadSnapshot**        | An in-memory map of account routing → value for a single blockchain thread. Used exclusively by `init_thread` to populate an `Uninitialized` thread from scratch after a `reset`.                                                                             |
| **BlockIdentifier**       | An opaque identifier for a specific block in the blockchain thread's history. Stored in the Thread Control Record as the "last applied block" marker.                                                                                                         |
| **ThreadTransition**      | A pair `(expected_block_id, new_block_id)` carried by `AccumulatedUpdate` for each blockchain thread being updated. Used to validate state and advance the thread's block identifier after the update.                                                        |
| **AccumulatedUpdate**     | An in-memory structure describing a set of changes arising from one or more blocks. Carries account-level operations, per-thread transitions, threads emerging in this update, and threads collapsing in this update.                                         |
| **Update Phase**          | An enum tracked per-thread in the Thread Control Record.                                                                                                                                                                                                      |

---

## 3. Aerospike Data Layout

### 3.1 Namespacing

```
namespace  : node_id       (uniquely identifies this node archive)
set        : "accounts"    (fixed)
```

### 3.2 System Record

**Key:** `{node_id}/accounts/__system`

One record per namespace. Created on the first `ArchiveStateStore::new` call.

| Bin name     | Type   | Description                                                                                                                                                                                                                                                        |
|--------------|--------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `threads`    | `Blob` | Bincode-serialized `HashSet<T>` of currently active thread IDs.                                                                                                                                                                                                    |
| `data_epoch` | `u64`  | Global epoch counter. Starts at `0`. Incremented only by `reset()`. All account data records written in the current epoch carry this value in their `epoch` bin; the janitor uses it to identify and delete all stale records from the previous epoch in one pass. |

> **Design note:** `reset()` is always a global operation — it resets all threads
> simultaneously and increments `data_epoch` by 1. Per-thread reset is intentionally
> not supported because a node archive cannot re-synchronize a single blockchain thread
> in isolation. If one thread needs re-synchronization the entire state is stale:
> threads share a common block history and cannot make progress independently of each
> other. A single `data_epoch` increment therefore correctly invalidates every account
> record across all threads in one write.

Update ordering:
* **On emergence**: Thread Control Record written first (as `Uninitialized`), then System Record updated to add the thread ID. A crash between the two leaves an orphaned Control Record which startup ignores because it is absent from the System Record.
* **On normal collapse**: System Record updated first (thread ID removed), then Control Record marked `Collapsed`. A crash between the two leaves a stale Control Record which startup ignores.
* **On `reset`**: System Record updated first (`data_epoch` incremented, thread set preserved), then each Control Record updated to `Uninitialized`. A crash between the two is handled by startup (see §8.1).

### 3.3 Thread Control Records

**Key:** `{node_id}/accounts/{thread_id}:ctrl`

One record per blockchain thread. All bins are written together in a single atomic
`put`; they are never updated individually.

| Bin name        | Type   | Description                                                                                                                                                                                                                     |
|-----------------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `copy_a_status` | `u8`   | `0` = Ready, `1` = Dirty                                                                                                                                                                                                        |
| `copy_b_status` | `u8`   | `0` = Ready, `1` = Dirty                                                                                                                                                                                                        |
| `update_phase`  | `u8`   | See §5.1                                                                                                                                                                                                                        |
| `last_block_id` | `Blob` | Bincode-serialized `BlockIdentifier` of the last block applied to this thread. Set to the `BlockIdentifier` supplied in `emerging_threads` on emergence, or to `initial_block_id` in `init_thread`. Empty when `Uninitialized`. |
| `update_seq`    | `u64`  | Monotonically increasing counter per thread. Incremented once per completed update cycle. Used as CAS generation check alongside `last_block_id`.                                                                               |

### 3.4 Data Records

Account data records have no thread reference in their keys. They live in a flat global
key space shared across all blockchain threads. This is intentional: accounts are not
owned by or scoped to a thread at the storage level. When a thread collapses, its
accounts remain in place untouched. When another thread is given access to those
accounts by the client, it reads them directly without any migration.

**Key pattern:**
```
{node_id}/accounts:A:{encoded_routing}
{node_id}/accounts:B:{encoded_routing}
```

| Bin name   | Type   | Description                                                                                                                                                                                                               |
|------------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `value`    | `Blob` | Bincode-serialized value `V`                                                                                                                                                                                              |
| `epoch`    | `u64`  | The `data_epoch` value at the time this record was written. Written on every `put`; used by the janitor after `reset()` to identify and delete all records from the previous epoch. Not used by the read or update paths. |

The `epoch` bin is the only cleanup marker on account records. A single scan filtering
`epoch < current_data_epoch` is sufficient to find every stale record across all threads.

---

## 4. Types and Traits

### 4.1 `UpdateOperation<V>`

Unchanged from single-thread spec.

```rust
pub enum UpdateOperation<V> {
    Delete,
    Replace(V),
}
```

### 4.2 `ThreadTransition`

*New in this revision.*

Carried by `AccumulatedUpdate` for each existing blockchain thread that this update
touches.

```rust
/// Describes the expected and resulting state for one blockchain thread in an update.
pub struct ThreadTransition {
    /// The `last_block_id` this thread's Control Record must currently hold.
    /// Used to validate that the update is being applied to the correct historical
    /// state. If the actual value differs, `apply_update` returns
    /// `Err(ArchiveStateError::StaleUpdate { thread_id, expected, actual })`.
    pub expected_block_id: BlockIdentifier,

    /// The `last_block_id` to write into this thread's Control Record after the
    /// update completes successfully.
    pub new_block_id: BlockIdentifier,
}
```

### 4.3 `ThreadSnapshot<R, V>`

Used exclusively by `init_thread`. Contains only values — `Delete` has no meaning for a
thread being initialized from scratch.

```rust
pub struct ThreadSnapshot<R, V>
where
    R: Eq + Hash + Clone + Serialize + DeserializeOwned + Send + Sync,
    V: Clone + Serialize + DeserializeOwned + Send + Sync,
{
    entries: HashMap<R, V>,
}

impl<R, V> ThreadSnapshot<R, V> {
    pub fn new() -> Self;
    pub fn insert(&mut self, routing: R, value: V) -> &mut Self;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
}
```

### 4.4 `AccumulatedUpdate<R, V, T>`

*Significantly changed from single-thread spec. Now carries four distinct maps.*

```rust
pub struct AccumulatedUpdate<R, V, T>
where
    R: Eq + Hash + Clone + Serialize + DeserializeOwned + Send + Sync,
    V: Clone + Serialize + DeserializeOwned + Send + Sync,
    T: Eq + Hash + Clone + Serialize + DeserializeOwned + Send + Sync,
{
    /// Account-level operations. These apply across all threads — the client knows
    /// which routings belong to which thread and declares that via `thread_transitions`
    /// or `emerging_threads`.
    operations: HashMap<R, UpdateOperation<V>>,

    /// For each existing blockchain thread touched by this update: the expected
    /// current block_id and the new block_id to write after the update.
    ///
    /// Every routing in `operations` must belong to a thread that has an entry
    /// in exactly one of `thread_transitions`, `emerging_threads`, or
    /// `collapsing_threads`. The library enforces this at apply time.
    thread_transitions: HashMap<T, ThreadTransition>,

    /// Blockchain threads that emerge as part of this update.
    /// Maps thread_id → initial BlockIdentifier, written as `last_block_id` into
    /// the new thread's Control Record.
    ///
    /// Account routings for the emerging thread arrive via `operations` as normal
    /// `Replace` entries. Each thread_id here must not already have an active Control
    /// Record; if it does, `apply_update` returns `Err(ThreadAlreadyExists)`.
    emerging_threads: HashMap<T, BlockIdentifier>,

    /// Blockchain threads that collapse as part of this update. Their Thread Control
    /// Record will be marked `Collapsed` and left in place — no cleanup is needed.
    /// The thread ID is removed from the System Record, so startup will never
    /// load the stale Control Record again.
    ///
    /// Account data records belonging to these threads are NOT deleted. They remain
    /// in place and become accessible to whichever thread the client assigns them to
    /// in a future update. The library does not track which accounts belong to which
    /// thread; that is entirely the client's responsibility.
    ///
    /// Each thread_id here must have an active Control Record in `Idle` phase.
    collapsing_threads: HashSet<T>,
}

impl<R, V, T> AccumulatedUpdate<R, V, T> {
    pub fn new() -> Self;

    /// Record a Delete for `routing`.
    pub fn delete(&mut self, routing: R) -> &mut Self;

    /// Record a Replace for `routing`.
    pub fn replace(&mut self, routing: R, value: V) -> &mut Self;

    /// Declare that `thread_id` is being updated from `expected` to `new` block.
    pub fn transition_thread(
        &mut self,
        thread_id: T,
        expected_block_id: BlockIdentifier,
        new_block_id: BlockIdentifier,
    ) -> &mut Self;

    /// Declare that `thread_id` is emerging with the given initial block identifier.
    /// Account data for this thread is supplied via `replace()` calls as normal.
    pub fn emerge_thread(
        &mut self,
        thread_id: T,
        initial_block_id: BlockIdentifier,
    ) -> &mut Self;

    /// Declare that `thread_id` is collapsing.
    pub fn collapse_thread(&mut self, thread_id: T) -> &mut Self;

    /// Return the pending operation for `routing`, or `None` if none is recorded.
    /// Used by the read path during `WritingCopyA` (see §7).
    pub fn get(&self, routing: &R) -> Option<&UpdateOperation<V>>;

    pub fn is_empty(&self) -> bool;
}
```

### 4.5 `ControlState`

*Differs from single-thread spec: now carries `last_block_id`; `UpdatePhase` gains a
`Collapsed` variant.*

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadControlState {
    pub thread_id: ThreadId,
    pub copy_a: CopyStatus,
    pub copy_b: CopyStatus,
    pub update_phase: UpdatePhase,
    pub last_block_id: Option<BlockIdentifier>,
    pub update_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyStatus {
    Ready,
    Dirty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdatePhase {
    /// Thread has been registered (Control Record exists) but not yet populated.
    /// Occurs when a thread emerges mid-update. Both copies are Dirty.
    /// `last_block_id` is empty. The thread becomes `Idle` at the end of the
    /// `apply_update` that contains it in `emerging_threads`.
    /// Control Record invariant: A=Dirty, B=Dirty.
    Uninitialized,

    /// No update in progress for this thread. Both copies are Ready.
    /// Control Record invariant: A=Ready, B=Ready.
    Idle,

    /// Phase 1 of the double-write protocol: Copy A is being written.
    /// Control Record invariant: A=Dirty, B=Ready.
    WritingCopyA,

    /// Phase 2 of the double-write protocol: Copy B is being written.
    /// Control Record invariant: A=Ready, B=Dirty.
    WritingCopyB,

    /// Thread has been collapsed. Its Control Record remains in Aerospike but is
    /// unreachable: the thread ID was removed from the System Record, so no future
    /// operation will reference this key. No cleanup is performed.
    /// Control Record invariant: A=Dirty, B=Dirty.
    Collapsed,
}
```

### 4.6 `ArchiveStateStore<R, V, T>`

*Significantly changed: now Arc-wrapped, Clone, and multi-thread aware.*

```rust
/// Cheap to clone; all clones share the same underlying state.
/// Safe to use from multiple OS threads concurrently.
#[derive(Clone)]
pub struct ArchiveStateStore<R, V, T> {
    inner: Arc<ArchiveStateStoreInner<R, V, T>>,
}

impl<R, V, T> ArchiveStateStore<R, V, T>
where
    R: Eq + Hash + Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    T: Eq + Hash + Clone + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Connect to Aerospike and open the current active state.
    ///
    /// Reads the System Record to obtain the thread registry, then reads the Thread
    /// Control Record for each known thread. Any thread in `WritingCopyA` or
    /// `WritingCopyB` causes `new` to return `Err(ArchiveStateError::NotReady)`.
    pub fn new(config: ArchiveStoreConfig) -> anyhow::Result<Self>;

    /// Read the value for `routing`, given the blockchain thread it belongs to.
    ///
    /// Returns `Err(Uninitialized)` if the thread is in `Uninitialized` phase.
    /// Returns `Err(Collapsed)` if the thread is in `Collapsed` phase.
    /// Concurrent reads from multiple OS threads are safe with no locking.
    /// See §7 for the full read protocol.
    pub fn read(&self, routing: &R, thread_id: &T) -> anyhow::Result<Option<V>>;

    /// Apply an accumulated update via the double-write protocol (§6).
    ///
    /// May be called concurrently from multiple OS threads. Calls that touch
    /// disjoint sets of blockchain threads proceed concurrently. Calls that share
    /// any blockchain thread are serialized via per-thread OS mutexes held
    /// internally (see §9).
    ///
    /// Returns `Err(StaleUpdate)` if any thread's `last_block_id` does not match
    /// the `expected_block_id` in the corresponding `ThreadTransition`.
    pub fn apply_update(
        &self,
        update: &AccumulatedUpdate<R, V, T>,
    ) -> anyhow::Result<()>;

    /// Read the Thread Control State for a specific blockchain thread.
    pub fn get_thread_control_state(
        &self,
        thread_id: &T,
    ) -> anyhow::Result<ThreadControlState>;

    /// Read the Thread Control States for all known blockchain threads.
    pub fn get_all_thread_control_states(
        &self,
    ) -> anyhow::Result<Vec<ThreadControlState>>;

    /// Populate both copies of a single thread from a complete snapshot.
    ///
    /// The thread must be in `Uninitialized` phase (i.e. it was just emerged or the
    /// store was just reset). Returns `Err(NotUninitialized)` otherwise.
    ///
    /// `initial_block_id` is written as `last_block_id` in the Thread Control Record.
    /// Account data from the snapshot is written to both copies in parallel.
    /// See §6.2 for the full protocol.
    pub fn init_thread(
        &self,
        thread_id: &T,
        initial_block_id: BlockIdentifier,
        snapshot: &ThreadSnapshot<R, V>,
    ) -> anyhow::Result<()>;

    /// Reset all threads simultaneously: increment the global `data_epoch` in the
    /// System Record, mark all threads `Uninitialized`, and enqueue all account data
    /// records from the previous epoch for background deletion via the janitor.
    ///
    /// All threads must be in `Idle` phase. Returns `Err(NotReady)` if any thread is
    /// mid-update. Returns immediately; janitor cleanup is asynchronous.
    ///
    /// > **Note:** Per-thread reset is intentionally not supported. A single blockchain
    /// > thread cannot be reset in isolation because threads are not independent — they
    /// > share a common block history and their states are coupled. Resetting one thread
    /// > while others continue would produce an inconsistent global state. Reset is
    /// > always a global operation. See §8.4.
    ///
    /// After `reset`, each thread is `Uninitialized` and must be populated via
    /// `init_thread` before reads or updates.
    pub fn reset(&self) -> anyhow::Result<()>;
}
```

Note: `T` has an additional `Ord` bound (absent in `R` and `V`). This is required to
sort blockchain thread IDs into a deterministic acquisition order for OS mutexes,
preventing deadlock when an `apply_update` spans multiple threads (see §9).

### 4.7 `ArchiveStoreConfig`

```rust
pub struct ArchiveStoreConfig {
    pub aerospike_address: String,
    /// Aerospike namespace — uniquely identifies this node archive.
    pub node_id: String,
    /// Number of parallel write threads (Rayon pool size). 0 = use all available cores.
    pub write_parallelism: usize,
    pub write_retry_count: u32,
    pub write_retry_delay_ms: u64,
    /// If true, `apply_update` returns `Err(Busy)` on mutex contention.
    /// If false, it waits for the blocking update to complete.
    pub fail_if_busy: bool,
    pub janitor: Arc<StateJanitor>,
}
```

---

## 5. State Machine

### 5.1 Phase Encoding (Thread Control Record `update_phase` bin)

| Value   | Variant         | Copy A   | Copy B   | Meaning                                                      |
|---------|-----------------|----------|----------|--------------------------------------------------------------|
| `0`     | `Uninitialized` | Dirty    | Dirty    | Thread registered, not yet written                           |
| `1`     | `Idle`          | Ready    | Ready    | Normal, safe to read and update                              |
| `2`     | `WritingCopyA`  | Dirty    | Ready    | First pass of double-write in progress                       |
| `3`     | `WritingCopyB`  | Ready    | Dirty    | Second pass in progress                                      |
| `4`     | `Collapsed`     | Dirty    | Dirty    | Thread is gone; Control Record left in place but unreachable |

### 5.2 Valid Per-Thread Transitions

```
Uninitialized ──(init_thread)──────────► Idle
Uninitialized ──(emerge in apply_update)─► Idle
Idle          ──(apply_update begins)───► WritingCopyA ──► WritingCopyB ──► Idle
Idle          ──(collapse in update)────► Collapsed  (terminal; Control Record left in place)
```

`reset()` atomically increments the global `data_epoch` and transitions **all** active
threads to `Uninitialized` simultaneously. Per-thread reset is not supported; see the
note on `reset()` in §4.6.

---

## 6. Operation Protocols

### 6.1 `apply_update` Protocol (multi-thread)

`apply_update` handles four categories in a single pass:

* **Existing thread updates** (`thread_transitions` + `operations`): double-write
  protocol, identical to the single-thread spec but applied per-thread.
* **Emerging threads** (`emerging_threads`): new threads written to both copies
  concurrently, similar to `init_state` in the single-thread spec.
* **Collapsing threads** (`collapsing_threads`): threads marked `Collapsed`. Their
  Control Records are left in place; the System Record is updated to remove the thread
  ID so no future startup or operation will reference them.

**Step 0 — Validate update consistency (in-memory, no I/O)**

Check that every routing in `operations` belongs to a thread that is declared in exactly
one of `thread_transitions`, `emerging_threads`, or `collapsing_threads`. Return
`Err(InconsistentUpdate)` if any routing is unaccounted for.

**Step 1 — Acquire per-thread OS mutexes**

Collect all thread IDs involved: `thread_transitions.keys()`,
`emerging_threads.keys()`, `collapsing_threads`. Sort by `T: Ord`. Acquire the OS mutex
for each thread in sorted order (creates the mutex lazily if the thread is emerging).
This deterministic ordering prevents deadlock between concurrent `apply_update` calls.

**Step 2 — Read and validate all Thread Control Records**

For each thread in `thread_transitions`: read its Thread Control Record and verify:
* `update_phase == Idle` — return `Err(NotReady)` otherwise.
* `last_block_id == transition.expected_block_id` — return `Err(StaleUpdate)` otherwise.

For each thread in `emerging_threads`: verify no Control Record exists — return
`Err(ThreadAlreadyExists)` otherwise.

For each thread in `collapsing_threads`: verify Control Record exists with `Idle` phase
— return `Err(NotReady)` otherwise.

**Step 3 — Write Uninitialized Control Records for emerging threads**

For each thread in `emerging_threads`, write a fresh Control Record:
```
copy_a_status = Dirty
copy_b_status = Dirty
update_phase  = Uninitialized
last_block_id = <empty>
update_seq    = 0
```

**Step 4 — Claim all existing and collapsing threads as WritingCopyA**

Because Aerospike does not support multi-record transactions, Control Records are marked
`Dirty` one at a time in the same deterministic sorted order established in Step 1
(`T: Ord`). For each thread in `thread_transitions` and `collapsing_threads`, CAS its
Control Record to:
```
copy_a_status = Dirty
copy_b_status = Ready
update_phase  = WritingCopyA
last_block_id = <unchanged>
update_seq    = <unchanged>
```

If any CAS fails (stale `update_seq` or concurrent modification from outside this
process), all Control Records successfully claimed so far are rolled back to `Idle` one
by one in reverse sorted order, and `apply_update` returns `Err(ConcurrentModification)`.
No data records are written if Step 4 fails.

> **Note:** The sorted sequential claim order means two concurrent `apply_update` calls
> sharing at least one thread will contend deterministically on the first shared thread
> in sorted order. One will succeed its CAS; the other will fail and roll back cleanly.
> Calls with fully disjoint thread sets never interact at this step.

> After Step 4: **Copy B is the canonical read copy** for all affected threads.

**Step 5 — Write Copy A in parallel**

Dispatch all tasks into the Rayon pool simultaneously:

* For each `Delete` in `operations`: issue Aerospike `delete` for the Copy A key.
* For each `Replace` in `operations`: issue Aerospike `put` for the Copy A key.
* For each thread in `emerging_threads`: issue `put` for every entry's Copy A key.

All tasks are independent. Pool is saturated immediately, no prior reads required.

**Step 6 — Promote all threads: WritingCopyA → WritingCopyB**

For each thread in `thread_transitions`, `emerging_threads`, and `collapsing_threads`,
CAS its Control Record to:
```
copy_a_status = Ready
copy_b_status = Dirty
update_phase  = WritingCopyB
last_block_id = <unchanged>
update_seq    = <unchanged>
```

> After Step 6: **Copy A is the canonical read copy** for all affected threads.

**Step 7 — Write Copy B in parallel**

Identical to Step 5, targeting Copy B keys.

**Step 8 — Finalize all threads**

For each thread in `thread_transitions` and `emerging_threads`, CAS its Control Record
to:
```
copy_a_status = Ready
copy_b_status = Ready
update_phase  = Idle
last_block_id = transition.new_block_id  (or emerging_threads[thread_id] for emerging)
update_seq    = previous + 1
```

For each thread in `collapsing_threads`, CAS its Control Record to:
```
copy_a_status = Dirty
copy_b_status = Dirty
update_phase  = Collapsed
last_block_id = <unchanged>
update_seq    = <unchanged>
```
Then atomically remove those thread IDs from the System Record's `threads` set.
The Control Record is left in Aerospike. It is now unreachable: no startup will load it
(absent from System Record), no operation will write to it, and the janitor does not
touch it. No account data records are affected.

**Step 9 — Release all OS mutexes.**

---

### 6.2 `init_thread` Protocol

```
Precondition: update_phase == Uninitialized  (set by thread emergence or reset)
```

**Step 1 — Guard check**

Read the Thread Control Record. If `update_phase != Uninitialized`, return
`Err(NotUninitialized)`.

**Step 2 — Write both copies in parallel**

Dispatch all entries from `ThreadSnapshot` as `put` tasks into the Rayon pool, targeting
Copy A keys and Copy B keys simultaneously. Each `put` writes both the `value` bin and
the `epoch` bin (current `data_epoch` read from the System Record at the start of
`init_thread`). All tasks are independent; the pool is saturated immediately.

**Step 3 — Mark thread Ready**

CAS the Thread Control Record to:
```
copy_a_status = Ready
copy_b_status = Ready
update_phase  = Idle
last_block_id = initial_block_id  (caller-supplied)
update_seq    = 0
```

The thread is now ready for reads and updates.

---

## 7. Read Protocol

`read(routing, thread_id)` executes the following decision tree:

```
1. Look up the in-memory active update for `thread_id`, if any.

2. Match on update.get(routing):
   ├─ Some(Delete)     → return None.            (in-memory, no Aerospike I/O)
   └─ Some(Replace(v)) → return Some(v).         (in-memory, no Aerospike I/O)

3. If no active update for this routing, read the Thread Control Record for `thread_id`.

4. Match on update_phase:
   ├─ Uninitialized → return Err(Uninitialized).
   ├─ Collapsed     → return Err(Collapsed).
   ├─ Idle          → read from Copy A.
   ├─ WritingCopyA  → read from Copy B  (B is stable for this thread).
   └─ WritingCopyB  → read from Copy A  (A is fully updated for this thread).
```

**Why `thread_id` is an explicit parameter:** account routings are globally unique
across threads, but the read path needs to check the correct Thread Control Record to
determine which copy is currently stable. The caller always knows which blockchain thread
a routing belongs to; requiring it explicitly avoids a hidden routing→thread lookup
inside the library.

### Concurrency of reads

* Reads never acquire any OS mutex.
* The active update map inside `ArchiveStateStoreInner` is a
  `RwLock<HashMap<T, Arc<AccumulatedUpdate>>>`. Reads take a shared lock to look up the
  update (non-blocking when no structural change is in progress).
* Thread Control Record reads are direct Aerospike `get` calls; no library-level lock.

---

## 8. Initialization and Lifecycle

### 8.1 `ArchiveStateStore::new` Startup

1. Read the System Record. If absent: write a fresh System Record with an empty thread
   set and `data_epoch = 0`, then return a store with no active threads.
2. Deserialize `threads` (`HashSet<T>`) and `data_epoch` from the System Record.
3. For each `thread_id` in the set, read the Thread Control Record at
   `{node_id}/accounts/{thread_id}:ctrl`.
   * If absent: `reset` step 1 succeeded but step 2 crashed — write a fresh
     `Uninitialized` Control Record for this thread and continue.
   * `Uninitialized` or `Idle`: accepted, loaded into in-memory thread registry.
   * `Collapsed`: stale Control Record left from a previous collapse. Since the thread
     ID was removed from the System Record before this state was written, this should
     never appear during normal startup. Treat as corrupted and return
     `Err(CorruptedControlRecord { thread_id })`.
   * `WritingCopyA` or `WritingCopyB`: return `Err(NotReady { thread_id, phase })`.
   * Unrecognized phase value: return `Err(CorruptedControlRecord { thread_id })`.

### 8.2 Thread Emergence and Collapse

Both are handled entirely within `AccumulatedUpdate` (see §6.1). There are no separate
`emerge_thread` / `collapse_thread` API methods. This keeps thread lifecycle atomic with
the block that causes it and ensures `last_block_id` is set correctly.

**Account data is never touched by thread lifecycle operations.** Emergence writes new
account data for the accounts that enter the system with that thread. Collapse writes
nothing to account data at all — accounts remain in Aerospike exactly as they are. The
client is responsible for routing future reads and writes for those accounts to whichever
active thread takes ownership of them. The library enforces only that account sets are
non-overlapping: at most one active thread controls any given routing at a time, and
this is the client's invariant to maintain.

**Collapsed thread Control Records are left in place and require no cleanup.** Because
the thread ID is removed from the System Record before the Control Record is marked
`Collapsed`, no future startup or operation will ever reference it. It is unreachable
and harmless.

### 8.3 Background Janitor

### 8.3 Background Janitor

The janitor has a single responsibility: after `reset()`, delete all account data
records whose `epoch` bin is less than the current `data_epoch`. Collapsed thread
Control Records require no cleanup — they are unreachable and left in place.

The janitor performs a full scan of the `accounts` set with an expression filter
(`epoch < current_data_epoch`), deleting every matching record (both A and B copies).
This scan runs once per `reset()` call and is expected to be infrequent.

```rust
pub struct StateJanitor { /* opaque */ }

impl StateJanitor {
    pub fn new(config: JanitorConfig) -> anyhow::Result<Arc<Self>>;

    /// Enqueue a full scan of the `accounts` set under `node_id`, deleting all
    /// records (A and B copies) whose `epoch` bin is less than `current_data_epoch`.
    ///
    /// Called once per `reset()`.
    pub fn enqueue_drop_stale_epoch(
        &self,
        node_id: String,
        current_data_epoch: u64,
    );

    /// Block until the queue is empty.
    pub fn flush(&self) -> anyhow::Result<()>;
}

pub struct JanitorConfig {
    pub aerospike_address: String,
    pub delete_parallelism: usize,
    pub poll_interval_ms: u64,
}
```

### 8.4 `reset()` — Global Reset

> **Design note:** Per-thread reset is intentionally not supported. A blockchain thread
> cannot be reset in isolation because threads are not independent — they share a common
> block history and their account states are coupled. If one thread needs
> re-synchronization, the entire state is stale and must be rebuilt from scratch. A
> single `data_epoch` increment invalidates all account records across all threads at
> once, which is exactly the correct scope.

Protocol:

1. Read the current System Record to obtain `threads` and the current `data_epoch`.
2. Acquire all per-thread OS mutexes in sorted order (same rule as `apply_update`).
3. Verify all threads are in `Idle` phase. Return `Err(NotReady)` if any are not.
4. Write a new System Record with `data_epoch` incremented by 1 (thread set unchanged).
5. For each thread, write its Control Record to:
   ```
   update_phase  = Uninitialized
   copy_a_status = Dirty
   copy_b_status = Dirty
   last_block_id = <empty>
   update_seq    = 0
   ```
6. Call `janitor.enqueue_drop_stale_epoch(node_id, new_data_epoch)`.
7. Release all OS mutexes.

After `reset()`, each thread is `Uninitialized` and must be populated via `init_thread`
before any reads or updates.

### 8.5 Crash Recovery — Not Implemented

Same as single-thread spec: if the process crashes during `WritingCopyA` or
`WritingCopyB` for any thread, `new()` will return `Err(NotReady)` for that thread.
The intended recovery path is `reset()` followed by `apply_update` with all threads in
`emerging_threads`. A future version may introduce per-thread recovery.

---

## 9. Concurrency Model

`ArchiveStateStore<R, V, T>` is `Clone + Send + Sync`. All clones share a single
`Arc<ArchiveStateStoreInner>`. The inner struct contains:

```rust
struct ArchiveStateStoreInner<R, V, T> {
    /// Aerospike client — already Arc-wrapped and Clone internally.
    client: SplitValueStore<AerospikeStore>,

    /// In-memory mirror of the System Record's active thread set.
    /// Updated under write lock on every structural change (emerge, collapse, reset).
    thread_registry: RwLock<HashSet<T>>,

    /// In-memory mirror of the System Record's current data_epoch.
    /// Read when writing account records (to stamp the epoch bin).
    /// Updated under write lock by reset().
    data_epoch: RwLock<u64>,

    /// One OS mutex per blockchain thread. Used to serialize concurrent
    /// apply_update calls that share a thread. Created lazily on first use.
    thread_write_locks: RwLock<HashMap<T, Arc<Mutex<()>>>>,

    /// The active AccumulatedUpdate per blockchain thread, set during apply_update
    /// Steps 4–8 and cleared on completion. Used by the read path.
    active_updates: RwLock<HashMap<T, Arc<AccumulatedUpdate<R, V, T>>>>,

    /// Rayon pool for parallel Aerospike writes.
    write_pool: Arc<rayon::ThreadPool>,

    config: ArchiveStoreConfig,
}
```

**Mutex acquisition order** (prevents deadlock between concurrent `apply_update` calls):

The thread IDs in `thread_transitions ∪ emerging_threads ∪ collapsing_threads` are
sorted using `T: Ord` and the OS mutexes are acquired in that order. Any two concurrent
`apply_update` calls that share at least one thread will contend on the first shared
mutex in sorted order, serializing naturally. Calls with fully disjoint thread sets
never contend.

**Active update visibility** (read path during `WritingCopyA`):

`apply_update` writes to `active_updates` (under the `RwLock` write lock, briefly) at
the end of Step 4, before releasing the lock to begin Rayon writes. Reads take the
`RwLock` shared lock for a lookup, which is non-blocking unless an update is
simultaneously registering or deregistering. The `Arc<AccumulatedUpdate>` means the
reader holds a reference to an immutable snapshot independent of any ongoing writes.

---

## 10. Crate Structure

```
archive-state-store/
├── Cargo.toml
├── src/
│   ├── lib.rs               — public re-exports
│   ├── config.rs            — ArchiveStoreConfig, JanitorConfig
│   ├── control.rs           — ThreadControlState, UpdatePhase, CopyStatus;
│   │                           Thread Control Record and System Record I/O
│   ├── update.rs            — UpdateOperation, ThreadTransition, AccumulatedUpdate
│   ├── store.rs             — ArchiveStateStore, ArchiveStateStoreInner,
│   │                           CopyStore (private)
│   ├── apply.rs             — apply_update multi-thread protocol (§6.1)
│   ├── read.rs              — read protocol (§7)
│   ├── janitor.rs           — StateJanitor background deletion task
│   └── error.rs             — ArchiveStateError enum
└── tests/
    ├── apply_single_thread.rs  — double-write on one blockchain thread
    ├── apply_multi_thread.rs   — concurrent apply_update on disjoint threads
    ├── emerge_collapse.rs      — thread lifecycle inside apply_update
    ├── read_during_update.rs   — read consistency under each phase per thread
    ├── init_guard.rs           — new() rejection on WritingCopyA/WritingCopyB
    ├── reset.rs                — global state reset
    └── concurrency.rs          — OS-thread concurrent read/write safety
```

---

## 11. Example Usage

```rust
// First startup — no System Record exists yet.
let store = ArchiveStateStore::<AccountRouting, Account, ThreadId>::new(config)?;
// No threads active; must emerge them via apply_update.

// Emerge two blockchain threads as part of the first block.
// Account data for each thread arrives via normal replace() calls.
let mut update = AccumulatedUpdate::new();
update.emerge_thread(thread_0, genesis_block_id);
update.emerge_thread(thread_1, genesis_block_id);
update.replace(routing_a, account_a);
update.replace(routing_b, account_b);
update.replace(routing_c, account_c);
store.apply_update(&update)?;

// Normal block processing — update routings across two threads.
let mut update = AccumulatedUpdate::new();
update.transition_thread(thread_0, block_0, block_1);
update.transition_thread(thread_1, block_0, block_1);
update.replace(routing_a, updated_account_a);
update.delete(routing_c);
store.apply_update(&update)?;

// Read from any OS thread at any time.
let account = store.read(&routing_a, &thread_0)?;

// Thread 1 collapses. Its Control Record is left in place but unreachable.
// Its accounts remain in Aerospike; another thread takes ownership on the client side.
let mut update = AccumulatedUpdate::new();
update.transition_thread(thread_0, block_1, block_2);
update.collapse_thread(thread_1);
store.apply_update(&update)?;

// Full global reset (rare).
let store = store.reset()?;
```

---

## 12. Out of Scope

* Crash recovery for in-flight updates (see §8.5).
* Account routing enumeration / iteration.
* Transactions spanning multiple `node_id` namespaces.
* Routing→thread index maintained by the library (caller always provides `thread_id`).
* TTL / expiry management of data records.
* Schema migration between versions of `V`.




---

T0 B0 (M0):
   Accounts: M1
   Threads: T0 + T1
   Merkle Trees: M2(M1 - T1) + M3(T1)

---

Merkle Trees:

archive (initial state):

T0 -> B0
T1 -> B1
T3 -> B0

merkle trees (initial state):

T0+B0 -> M0
T3+B0 -> M3
T1+B1 -> M1

accumulated update:

T0 -> B10
T2 -> B9

merkle trees (updated):

T0+B0  -> M0
T3+B0  -> M3
T1+B1  -> M1

T0+B10 -> M4
T2+B9  -> M5

archive (final state):

T0 -> B10
T1 -> B1
T3 -> B0
T2 -> B9

merkle trees save:

new:
T0+B10 -> M4
T2+B9  -> M5

delete:
T0+B0  -> M0


-- node
1. accumulated account update (ready)
2. prepare new merkle trees (ready)
3. save new merkle trees on disk
4. save accumulated updates to aerospike
5. remove old merkle trees from disk (ignore failures)



Sync Snapshot

export:

1. pause archive writing
2. export
3. resume writing

import:

1. reset



















---

# Structure


## Operational Pool

- accounts lru cache (hash → account)

## Accumulator

- accumulated update (routing → account)
- merkle maps update (thread → block + markle root)

## Archive

- active updates (routing → account)
- aerospike (routing → account)


# Operations


## Create a state from prev and diff

1. add accounts to the operational pool
2. build new merkle root

## Finalize state

1. wait for accumulator is not full
2. merge with accumulator 

## Update loop

1. wait for accumulator is not empty
2. take accumulated update
3. add to active updates
4. write new merkle maps to disk
5. apply to aerospike
6. remove old merkle maps from disk
7. remove from active updates

## Find an account in state

1. find in operational pool:
   - find hash in the state's merkle map
   - find account in the operational pool
2. find in accumulator
3. find in active updates
4. find in aerospike


# Questions

## Split

### Optimistic State 0 on block B0

- T1: thread
- M0: merkle root (from prev state)
- TT0: threads table
- C0: cropped = (T1 + TT0)

### Input Block B1

- B1: block
- D1: account operations in block B1
- T2 add thread

### Optimistic State 1 on block B1

- T1: thread
- M1: merkle root = M0 + D1
- TT1: threads table
- C1: cropped = (T1 + TT0)

### Archive Operations

2. push thread transition (T1, B1, D, M1)
3. push thread emerge (T2, B1, M1)


### Input Block B3(T1) parent B1

- M1: prev state
- D3: account operations in B3

1. check if B1 has new thread T2
3. archive transition (T1, B3, D3, M3)

### Optimistic State 3 on block B3

- T1: thread
- M3: M3 = (M1 - T2 accounts) + D3
- TT1: threads table
- C3: cropped = (T1 + TT1)


### Input Block B4(T2) parent B1

- M1: prev state
- D4: account operations in B4

1. check if B1 has new thread T2
3. archive transition (T2, B4, D4, M4)

### Optimistic State 4 on block B4

- T2: thread
- M4 = (T2 accounts from M1) + D4
- TT1: threads table
- C4: cropped = (T2 + TT1)

