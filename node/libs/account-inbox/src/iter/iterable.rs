use std::iter::Iterator;

use crate::iter::iterator::IteratorError;
use crate::iter::iterator::MessagesRangeIterator;
use crate::range::MessagesRange;
use crate::storage::DurableStorageRead;

pub trait DurableStorageIterable<MessageKey, Message> {
    type IterError;
    fn iter(
        &self,
        range: MessagesRange<MessageKey, Message>,
    ) -> impl Iterator<Item = Result<(Message, MessageKey), Self::IterError>>;
}

impl<MessageKey, Message, Storage> DurableStorageIterable<MessageKey, Message> for Storage
where
    Storage: DurableStorageRead<MessageKey, Message>,
    MessageKey: Clone + PartialEq + From<Message>,
    Message: Clone,
{
    type IterError = IteratorError<Storage::LoadError, MessageKey>;

    fn iter<'a>(
        &'a self,
        range: MessagesRange<MessageKey, Message>,
    ) -> impl Iterator<Item = Result<(Message, MessageKey), Self::IterError>> {
        MessagesRangeIterator::<'a, MessageKey, Message, Storage>::new(self, range)
    }
}
