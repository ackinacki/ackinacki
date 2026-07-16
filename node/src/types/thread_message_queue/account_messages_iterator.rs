use std::sync::Arc;

use account_inbox::iter::iterable::DurableStorageIterable;
use account_inbox::iter::iterator::MessagesRangeIterator;
use account_inbox::storage::DurableStorageRead;

use super::ThreadMessageQueueState;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;

pub trait AccountMessagesIterator {
    fn iter<'a, T>(
        &'a self,
        db_storage: &'a T,
    ) -> impl Iterator<Item = MessagesRangeIterator<'a, MessageIdentifier, Arc<WrappedMessage>, T>> + 'a
    where
        T: DurableStorageIterable<MessageIdentifier, Arc<WrappedMessage>>
            + DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>>
            + 'a;
}

impl AccountMessagesIterator for ThreadMessageQueueState {
    fn iter<'a, T>(
        &'a self,
        db_storage: &'a T,
    ) -> impl Iterator<Item = MessagesRangeIterator<'a, MessageIdentifier, Arc<WrappedMessage>, T>> + 'a
    where
        T: DurableStorageIterable<MessageIdentifier, Arc<WrappedMessage>>
            + DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>>
            + 'a,
    {
        ThreadMessageQueueIterator::<'a, T> {
            state: self,
            dapp_offset: 0,
            account_offset: 0,
            db: db_storage,
        }
    }
}

pub struct ThreadMessageQueueIterator<'a, Storage>
where
    Storage: DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>>,
{
    state: &'a ThreadMessageQueueState,
    dapp_offset: usize,
    account_offset: usize,
    db: &'a Storage,
}

impl<'a, Storage> Iterator for ThreadMessageQueueIterator<'a, Storage>
where
    Storage: DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>>,
{
    type Item = MessagesRangeIterator<'a, MessageIdentifier, Arc<WrappedMessage>, Storage>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.dapp_offset < self.state.order_set.len() {
            let dapp_index = (self.state.cursor + self.dapp_offset) % self.state.order_set.len();
            let dapp_id = self.state.order_set.get_index(dapp_index)?;
            let dapp_queue = self.state.messages.get(dapp_id)?;

            if self.account_offset >= dapp_queue.order_set.len() {
                self.dapp_offset += 1;
                self.account_offset = 0;
                continue;
            }

            let account_index =
                (dapp_queue.cursor + self.account_offset) % dapp_queue.order_set.len();
            self.account_offset += 1;
            let routing = dapp_queue.order_set.get_index(account_index)?;
            let range = dapp_queue.messages.get(routing)?.as_ref().clone();
            return Some(MessagesRangeIterator::<
                'a,
                MessageIdentifier,
                Arc<WrappedMessage>,
                Storage,
            >::new(self.db, range));
        }
        None
    }
}
