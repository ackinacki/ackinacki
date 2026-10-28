use std::sync::Arc;

use account_inbox::iter::iterable::DurableStorageIterable;
use account_inbox::iter::iterator::MessagesRangeIterator;
use account_inbox::storage::DurableStorageRead;

use super::seek::CyclicIter;
use super::DAppMessageQueueState;
use super::InboxCapacity;
use super::ThreadMessageQueueState;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::types::AccountInbox;

#[cfg(test)]
#[path = "legacy_iterator_test_support.rs"]
pub(crate) mod legacy_test_support;

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
            dapps: self.messages.cyclic(self.cursor),
            accounts: None,
            db: db_storage,
            #[cfg(test)]
            legacy: legacy_test_support::iterator(self, db_storage),
        }
    }
}

pub struct ThreadMessageQueueIterator<'a, Storage>
where
    Storage: DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>>,
{
    dapps: CyclicIter<'a, Arc<DAppMessageQueueState>, InboxCapacity>,
    accounts: Option<CyclicIter<'a, Arc<AccountInbox>, InboxCapacity>>,
    db: &'a Storage,
    // Test-only substitution runs the former index-based traversal through the
    // real producer/verifier. Release builds have no override or extra state.
    #[cfg(test)]
    legacy: Option<
        std::vec::IntoIter<
            MessagesRangeIterator<'a, MessageIdentifier, Arc<WrappedMessage>, Storage>,
        >,
    >,
}

impl<'a, Storage> Iterator for ThreadMessageQueueIterator<'a, Storage>
where
    Storage: DurableStorageRead<MessageIdentifier, Arc<WrappedMessage>>,
{
    type Item = MessagesRangeIterator<'a, MessageIdentifier, Arc<WrappedMessage>, Storage>;

    fn next(&mut self) -> Option<Self::Item> {
        #[cfg(test)]
        if let Some(legacy) = &mut self.legacy {
            return legacy.next();
        }
        loop {
            if let Some((_, inbox)) = self.accounts.as_mut().and_then(Iterator::next) {
                return Some(MessagesRangeIterator::new(self.db, inbox.messages.clone()));
            }
            let (_, dapp) = self.dapps.next()?;
            self.accounts = Some(dapp.messages.cyclic(dapp.cursor));
        }
    }
}
