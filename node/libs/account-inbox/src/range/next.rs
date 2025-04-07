use crate::iter::iterator::IteratorError;
use crate::iter::iterator::MessagesRangeIterator;
use crate::range::MessagesRange;
use crate::storage::DurableStorageRead;

pub enum BuildNextRangeError<Message, StorageErr, IteratorError> {
    StorageError(StorageErr),
    InvalidRangeOrConsumedMessage { next_consumed: Message, next_in_range: Message },
    RangeIteratorError(IteratorError),
    OutOfRange(Message),
}

pub trait BuildNextRange<MessageKey, Message> {
    type Error;
    fn build_next_range(
        &self,
        range: MessagesRange<MessageKey, Message>,
        appended_messages: &[(MessageKey, Message)],
        consumed_messages: &[Message],
    ) -> Result<MessagesRange<MessageKey, Message>, Self::Error>;
}

impl<MessageKey, Message, Storage> BuildNextRange<MessageKey, Message> for Storage
where
    Storage: DurableStorageRead<MessageKey, Message>,
    MessageKey: PartialEq + Clone + From<Message>,
    Message: PartialEq + Clone,
{
    type Error = BuildNextRangeError<
        Message,
        Storage::LoadError,
        IteratorError<Storage::LoadError, MessageKey>,
    >;

    fn build_next_range(
        &self,
        mut range: MessagesRange<MessageKey, Message>,
        appended_messages: &[(MessageKey, Message)],
        consumed_messages: &[Message],
    ) -> Result<MessagesRange<MessageKey, Message>, Self::Error> {
        range.tail_sequence.extend(appended_messages.iter().cloned());
        let mut iterator = MessagesRangeIterator::new(self, range);
        for consumed in consumed_messages.iter() {
            let actual = iterator
                .next()
                .ok_or(BuildNextRangeError::OutOfRange(consumed.clone()))?
                .map_err(|e| BuildNextRangeError::RangeIteratorError(e))?
                .0;
            if &actual != consumed {
                return Err(BuildNextRangeError::InvalidRangeOrConsumedMessage {
                    next_consumed: consumed.clone(),
                    next_in_range: actual,
                });
            }
        }
        let remaining = iterator.remaining().clone();

        // TODO: consume messages first
        // TODO: append blocks as candidates
        // TODO:
        //   In a loop
        //   - try extend last finalized blocks range
        //   - try to make a candidate block into a finalized block (check if this block is in the finalized blocks storage)
        Ok(remaining)
    }
}
