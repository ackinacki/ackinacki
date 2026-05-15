//! Snapshot pin state machine for `ArchiveUpdateAccumulator`.
//!
//! See `spec/SNAPSHOT.md` for the full design. The pin gates the update
//! loop's `archive.apply_update(...)` call when a database-wide snapshot
//! is being assembled. Producers and the snapshot worker interact via the
//! accumulator's `request_snapshot_pin` / `acquire_snapshot_pin` methods.
//!
//! Pin set semantics for an anchor block `A`:
//!   pin_set(A) = { (T_A, A.id) } ∪ { (T_i, A.refs[T_i]) | i ≠ A }
//!
//! The anchor entry is **defer-apply** — the update loop must not advance
//! the anchor thread past `A` while the pin is held. Cross-thread ref
//! entries are **reachability** — bytes for the ref block must be
//! resolvable via pool/accumulator/archive at acquire time.

use std::collections::HashMap;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;

/// Description of a snapshot anchor block, supplied by the requester.
///
/// The requester (typically `mark_block_as_finalized`) is responsible for
/// translating the anchor block's `common_section().refs` (a flat list of
/// block ids) into the per-thread map required here. The accumulator does
/// not have access to the cross-thread ref data service.
#[derive(Clone, Debug)]
pub struct AnchorBlockRef {
    pub anchor_block_id: BlockIdentifier,
    pub anchor_thread_id: ThreadIdentifier,
    /// Cross-thread refs: `T_i → A.refs[T_i]` for every other thread.
    /// Must NOT include `anchor_thread_id`.
    pub cross_thread_refs: HashMap<ThreadIdentifier, BlockIdentifier>,
}

impl AnchorBlockRef {
    /// Expand to the full pin set keyed by thread. The anchor thread's
    /// entry maps to the anchor block id; other threads map to the ref
    /// block id supplied by the caller.
    pub fn pin_set(&self) -> HashMap<ThreadIdentifier, BlockIdentifier> {
        let mut set = HashMap::with_capacity(self.cross_thread_refs.len() + 1);
        set.insert(self.anchor_thread_id, self.anchor_block_id);
        for (tid, bid) in &self.cross_thread_refs {
            // Defensive: if a caller supplies a self-ref, the anchor
            // entry above wins.
            set.entry(*tid).or_insert(*bid);
        }
        set
    }
}

/// Internal state of the pin slot.
///
/// Transitions:
///
/// - `request_snapshot_pin` :
///   - Idle → Requested(A)
///   - Requested(A) → Requested(B)         (latest-wins, same slot, alignable)
///   - BoundaryReached / Pinned / Releasing → write into `next` slot
///
/// - update_loop apply step :
///   - Requested(A) + boundary batch applied → BoundaryReached(A)
///     (signal `pin_acquirable`)
///
/// - `acquire_snapshot_pin` :
///   - BoundaryReached(A) + reachability OK → Pinned(active=A, next=None)
///   - BoundaryReached(A) + reachability fail / timeout → Releasing(next=None)
///   - Requested(A) + timeout (boundary never reached) → Releasing(next=None)
///
/// - `PinHandle::drop` :
///   - Pinned → Releasing
///
/// - update_loop drain step :
///   - Releasing(next=None) → Idle
///   - Releasing(next=Some(B)) → Requested(B)            (if still alignable)
///     OR Idle                    (otherwise, dropped + WARN)
#[derive(Default)]
pub(crate) enum SnapshotPinState {
    #[default]
    Idle,
    /// A pin has been requested. The producer side aligns the batch
    /// boundary on the anchor's `(T_A, A)` entry. The update loop
    /// applies batches normally until it crosses the boundary, then
    /// transitions to `BoundaryReached`.
    Requested(AnchorBlockRef),
    /// The boundary batch has been applied to the archive. The
    /// `acquire_snapshot_pin` caller (snapshot worker) is expected to
    /// take this state and transition it to `Pinned`. Until that
    /// happens, the update loop defers any batch that would advance
    /// the anchor thread past `A`. `next` may carry a fresh request
    /// that arrived while this pin was waiting to be acquired.
    BoundaryReached { active: AnchorBlockRef, next: Option<AnchorBlockRef> },
    /// A pin is actively held by the snapshot worker. The update loop
    /// defers any batch that would advance the anchor thread past `A`.
    /// `next` may carry a fresh request that arrived during this pin.
    Pinned { active: AnchorBlockRef, next: Option<AnchorBlockRef> },
    /// The pin handle has been dropped (or the request was aborted).
    /// The update loop is draining deferred batches. Once drained, the
    /// slot transitions to Idle (or, if `next` is set, to Requested
    /// with that anchor).
    Releasing { next: Option<AnchorBlockRef> },
}
