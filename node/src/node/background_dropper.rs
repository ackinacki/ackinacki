use std::sync::mpsc;
use std::sync::LazyLock;
use std::time::Instant;

use super::unprocessed_blocks_collection::CandidateEntry;
use super::unprocessed_blocks_collection::CandidateMap;

// A heuristic, not a byte budget: small batches are cheaper to release locally than to send to a
// worker. Keep this shared by vectors and snapshots so their handoff policy stays consistent.
pub(super) const HEAVY_DROP_MIN_ENTRIES: usize = 128;

// During catch-up, an applier can remove thousands of old blocks while a finalizer's queue snapshot
// still holds their last references. Dropping that snapshot then makes the finalizer destroy the
// entire object graph. If those objects were swapped out, even freeing them requires paging memory
// back in and can take minutes. Meanwhile application can keep producing optimistic states which
// cannot be retired without finalization, amplifying the memory pressure.
//
// Hand off both removed entries and queue snapshots so whichever caller owns the last references
// does not perform bulk destruction inline. Small batches still drop locally. A single long-lived
// worker avoids creating threads for each batch and concurrent cleanup workers competing for swap
// I/O. This does not make drops faster or eliminate paging; it moves that work off the processing
// threads while capacity allows.
// Keep only two waiting tasks: an unbounded queue could retain more memory and worsen the problem.
pub(crate) static BACKGROUND_DROPPER: LazyLock<BackgroundDropper> =
    LazyLock::new(BackgroundDropper::new);

/// Owns references handed off by processing threads. Add other object families here when their
/// destruction also needs to leave the caller's thread; keep each task batched, not per-object.
/// Actual destruction happens here only if the task holds the last reference to an object.
enum BackgroundDrop {
    /// Keep states together with bodies so neither is destroyed on the processing thread.
    Blocks(Vec<CandidateEntry>),
    /// Transfer the existing map without rebuilding it into a vector.
    BlockSnapshot(CandidateMap),
}

impl BackgroundDrop {
    fn details(&self) -> (&'static str, usize) {
        match self {
            Self::Blocks(blocks) => ("blocks", blocks.len()),
            Self::BlockSnapshot(blocks) => ("block_snapshot", blocks.len()),
        }
    }
}

/// Process-wide, long-lived destruction worker. This isolates expensive drops, not memory or I/O:
/// swapping can still affect other threads. The bounded channel intentionally stalls senders when
/// destruction cannot keep up, rather than allowing an unlimited backlog of retained objects.
pub(crate) struct BackgroundDropper {
    sender: mpsc::SyncSender<BackgroundDrop>,
}

impl BackgroundDropper {
    fn new() -> Self {
        let (sender, receiver) = mpsc::sync_channel::<BackgroundDrop>(2);
        std::thread::Builder::new()
            .name("background-dropper".into())
            .spawn(move || {
                while let Ok(task) = receiver.recv() {
                    let (kind, count) = task.details();
                    let started = Instant::now();
                    drop(task);
                    tracing::info!(target: "monit", kind, items = count,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "background-dropper: released references");
                }
            })
            .expect("Failed to start background-dropper");
        Self { sender }
    }

    /// Release small batches locally; transfer heavy batches as one task. Call after releasing
    /// collection and state locks: destruction can be expensive, and enqueueing can wait for space.
    pub(crate) fn defer_heavy_drop_blocks(&self, blocks: Vec<CandidateEntry>) {
        if blocks.len() >= HEAVY_DROP_MIN_ENTRIES {
            self.defer(BackgroundDrop::Blocks(blocks));
        }
    }

    /// Apply the same heavy-drop threshold without rebuilding the snapshot's map. Like
    /// `defer_heavy_drop_blocks`, call only after releasing collection and state locks.
    pub(crate) fn defer_heavy_drop_block_snapshot(&self, blocks: CandidateMap) {
        if blocks.len() >= HEAVY_DROP_MIN_ENTRIES {
            self.defer(BackgroundDrop::BlockSnapshot(blocks));
        }
    }

    fn defer(&self, task: BackgroundDrop) {
        let (kind, count) = task.details();
        let started = Instant::now();
        if let Err(mpsc::SendError(task)) = self.sender.send(task) {
            // A disconnected worker cannot accept ownership. Release on the caller as a last
            // resort; never leak the task or panic from a snapshot's Drop implementation.
            tracing::error!(target: "monit", kind,
                "background-dropper disconnected; releasing references on caller");
            drop(task);
        }
        let elapsed_ms = started.elapsed().as_millis();
        if elapsed_ms >= 50 {
            tracing::warn!(target: "monit", kind, items = count, elapsed_ms = elapsed_ms as u64,
                "background-dropper: waited to enqueue references");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::node::unprocessed_blocks_collection::tests::make_block_state;
    use crate::types::BlockIndex;

    fn make_entries(count: usize) -> Vec<CandidateEntry> {
        (1..=count).map(|seq| make_block_state(seq as u32, seq as u64)).collect()
    }

    #[test]
    fn blocks_are_transferred_in_one_task_without_synchronous_destruction() {
        let blocks = make_entries(HEAVY_DROP_MIN_ENTRIES);
        let weak_blocks: Vec<_> = blocks.iter().map(|(_, block)| Arc::downgrade(block)).collect();
        // Leave the receiver paused to inspect ownership before the worker destroys the task.
        let (sender, receiver) = mpsc::sync_channel(1);
        let dropper = BackgroundDropper { sender };
        dropper.defer_heavy_drop_blocks(blocks);
        assert!(weak_blocks.iter().all(|block| block.strong_count() == 1));
        let task = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            matches!(&task, BackgroundDrop::Blocks(entries) if entries.len() == HEAVY_DROP_MIN_ENTRIES)
        );
        assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
        drop(task);
        assert!(weak_blocks.iter().all(|block| block.upgrade().is_none()));
    }

    #[test]
    fn applies_backpressure_without_dropping_queued_batches() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let dropper = BackgroundDropper { sender };
        let first = make_entries(HEAVY_DROP_MIN_ENTRIES);
        let second = make_entries(HEAVY_DROP_MIN_ENTRIES);
        let weak = Arc::downgrade(&second[0].1);
        dropper.defer_heavy_drop_blocks(first);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("test-background-dropper-backpressure".into())
            .spawn(move || {
                entered_tx.send(()).unwrap();
                dropper.defer_heavy_drop_blocks(second);
                finished_tx.send(()).unwrap();
            })
            .unwrap();
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(finished_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert!(weak.upgrade().is_some());
        drop(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        finished_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(receiver.recv_timeout(Duration::from_secs(5)).unwrap());
        worker.join().unwrap();
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn small_batches_drop_locally_even_when_the_queue_is_full() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let dropper = BackgroundDropper { sender };
        dropper.defer_heavy_drop_blocks(make_entries(HEAVY_DROP_MIN_ENTRIES));
        for count in [0, 1, HEAVY_DROP_MIN_ENTRIES - 1] {
            let blocks = make_entries(count);
            let weak: Vec<_> = blocks.iter().map(|(_, block)| Arc::downgrade(block)).collect();
            dropper.defer_heavy_drop_blocks(blocks);
            assert!(weak.iter().all(|block| block.upgrade().is_none()));
        }
        // Only the original large task was enqueued; small batches neither wait nor consume slots.
        drop(receiver.try_recv().unwrap());
        assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }

    #[test]
    fn snapshot_threshold_matches_the_vector_threshold() {
        for count in [0, 1, HEAVY_DROP_MIN_ENTRIES - 1, HEAVY_DROP_MIN_ENTRIES] {
            let blocks = make_entries(count);
            let weak: Vec<_> = blocks.iter().map(|(_, block)| Arc::downgrade(block)).collect();
            let map = blocks
                .into_iter()
                .map(|entry| (BlockIndex::from(entry.1.as_ref()), entry))
                .collect();
            let (sender, receiver) = mpsc::sync_channel(1);
            let dropper = BackgroundDropper { sender };
            dropper.defer_heavy_drop_block_snapshot(map);
            if count < HEAVY_DROP_MIN_ENTRIES {
                assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
                assert!(weak.iter().all(|block| block.upgrade().is_none()));
            } else {
                assert!(weak.iter().all(|block| block.strong_count() == 1));
                let task = receiver.try_recv().unwrap();
                assert!(
                    matches!(&task, BackgroundDrop::BlockSnapshot(entries) if entries.len() == count)
                );
                drop(task);
                assert!(weak.iter().all(|block| block.upgrade().is_none()));
            }
        }
    }
}
