//! Reproduce the previous producer/verifier's indexed traversal in cross-model tests.
//! The override is scoped to the calling thread: parallel tests cannot affect it,
//! and the guard restores the previous mode even if block construction panics.
use std::cell::Cell;
use std::sync::Arc;

use account_inbox::iter::iterator::MessagesRangeIterator;
use account_inbox::storage::DurableStorageRead;

use super::super::legacy;
use super::super::ThreadMessageQueueState;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;

thread_local! {
    static INDEXED: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn with_indexed_walk<R>(indexed: bool, run: impl FnOnce() -> R) -> R {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            INDEXED.set(self.0);
        }
    }
    let _restore = Restore(INDEXED.replace(indexed));
    run()
}

pub(super) fn iterator<'a, T: DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>>>(
    queue: &ThreadMessageQueueState,
    db: &'a T,
) -> Option<std::vec::IntoIter<MessagesRangeIterator<'a, MessageIdentifier, Arc<WrappedMessage>, T>>>
{
    if !INDEXED.get() {
        return None;
    }
    // The legacy representation has sorted order_set vectors and numeric cursors.
    // Run its original modulo/index loops, independently of CyclicIter. Message
    // bodies and FIFO ranges are unchanged, so only account scheduling is replaced.
    let queue = legacy::Queue::from_current(queue);
    let mut inboxes = Vec::new();
    for dapp_offset in 0..queue.order_set.len() {
        let id = queue.order_set[(queue.cursor + dapp_offset) % queue.order_set.len()];
        let dapp = &queue.messages[&id];
        for account_offset in 0..dapp.order_set.len() {
            let routing = dapp.order_set[(dapp.cursor + account_offset) % dapp.order_set.len()];
            inboxes.push(MessagesRangeIterator::new(db, dapp.messages[&routing].clone()));
        }
    }
    Some(inboxes.into_iter())
}
