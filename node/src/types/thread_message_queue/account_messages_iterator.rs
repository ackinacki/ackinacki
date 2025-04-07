use std::sync::Arc;

use account_inbox::iter::iterable::DurableStorageIterable;
use account_inbox::iter::iterator::MessagesRangeIterator;
use account_inbox::storage::DurableStorageRead;

use super::ThreadMessageQueueState;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;

pub trait AccountMessagesIterator {
    fn iter<'a, T>(
        &self,
        db_storage: &'a T,
    ) -> impl Iterator<Item = MessagesRangeIterator<'a, MessageIdentifier, Arc<WrappedMessage>, T>>
    where
        T: DurableStorageIterable<MessageIdentifier, Arc<WrappedMessage>>
            + DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>>;
}

impl AccountMessagesIterator for ThreadMessageQueueState {
    fn iter<'a, T>(
        &self,
        db_storage: &'a T,
    ) -> impl Iterator<Item = MessagesRangeIterator<'a, MessageIdentifier, Arc<WrappedMessage>, T>>
    where
        T: DurableStorageIterable<MessageIdentifier, Arc<WrappedMessage>>
            + DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>>,
    {
        ThreadMessageQueueIterator::<'a, T> { state: self.clone(), offset: 0, db: db_storage }
    }
}

pub struct ThreadMessageQueueIterator<'a, Storage>
where
    Storage: DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>>,
{
    state: ThreadMessageQueueState,
    offset: usize,
    db: &'a Storage,
}

impl<'a, Storage> Iterator for ThreadMessageQueueIterator<'a, Storage>
where
    Storage: DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>>,
{
    type Item = MessagesRangeIterator<'a, MessageIdentifier, Arc<WrappedMessage>, Storage>;

    fn next(&mut self) -> Option<Self::Item> {
        // tracing::trace!("get_next_int_message: next(): state={:?}", self.state.messages);
        if self.offset >= self.state.order_set.len() {
            return None;
        }
        let next_index = (self.state.cursor + self.offset) % self.state.order_set.len();
        let next_account = self.state.order_set.get_index(next_index)?;
        let range = self.state.messages.get(next_account)?.clone();
        self.offset += 1;
        Some(MessagesRangeIterator::<'a, MessageIdentifier, Arc<WrappedMessage>, Storage>::new(
            self.db, range,
        ))
    }
}
