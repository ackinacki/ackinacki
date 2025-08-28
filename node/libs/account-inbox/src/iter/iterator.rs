use std::fmt::Debug;
use std::iter::Iterator;
use std::ops::RangeInclusive;

use crate::range::MessagesRange;
use crate::storage::DurableStorageRead;

//# pub mod iterable;

#[derive(Debug)]
pub enum IteratorError<StorageErr, MessageKey> {
    LoadMessageError(StorageErr),
    LoadNextRefError(StorageErr),
    BrokenRange { missing_end: MessageKey, actual_last: MessageKey },
}

pub struct MessagesRangeIterator<'a, MessageKey, Message, Storage>
where
    Storage: DurableStorageRead<MessageKey, Message>,
{
    // pub struct MessagesRange<MessageKey, Message> {
    //     compacted_history: Option<RangeInclusive<MessageKey>>,
    //     pub(crate) tail_sequence: VecDeque<(MessageKey, Message)>,
    // }
    remaining: MessagesRange<MessageKey, Message>,
    db: &'a Storage,
}

impl<'a, MessageKey, Message, Storage> MessagesRangeIterator<'a, MessageKey, Message, Storage>
where
    Storage: DurableStorageRead<MessageKey, Message>,
    Message: Clone,
    MessageKey: Clone + PartialEq + From<Message>,
{
    pub fn new(db: &'a Storage, range: MessagesRange<MessageKey, Message>) -> Self {
        Self { remaining: range, db }
    }

    pub fn remaining(&self) -> &MessagesRange<MessageKey, Message> {
        &self.remaining
    }

    pub fn next_range(
        &mut self,
        limit: usize,
    ) -> Result<Vec<Message>, IteratorError<Storage::LoadError, MessageKey>> {
        let mut messages = if let Some(db_stored) = self.remaining.compacted_history() {
            let (msg_ref, end) = db_stored.clone().into_inner();
            let result = match consume::range_from_storage(self.db, msg_ref.clone(), end, limit) {
                Ok((messages, remains)) => {
                    self.remaining.set_compacted_history(remains);
                    Ok(messages)
                }
                Err(e) => Err(e),
            };
            result?
        } else {
            vec![]
        };
        let remaining_limit =
            std::cmp::min(limit.saturating_sub(messages.len()), self.remaining.tail_sequence.len());
        let tail_messages = self.remaining.tail_sequence.split_off(remaining_limit);
        messages.extend(
            self.remaining.tail_sequence.clone().into_iter().map(|(_, m)| m).collect::<Vec<_>>(),
        );
        self.remaining.tail_sequence = tail_messages;
        Ok(messages)
    }
}

impl<MessageKey, Message, Storage> Iterator
    for MessagesRangeIterator<'_, MessageKey, Message, Storage>
where
    Storage: DurableStorageRead<MessageKey, Message>,
    Message: Clone,
    MessageKey: Clone + std::cmp::PartialEq,
{
    type Item =
        std::result::Result<(Message, MessageKey), IteratorError<Storage::LoadError, MessageKey>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(db_stored) = self.remaining.compacted_history() {
            let (msg_ref, end) = db_stored.clone().into_inner();
            let result = match consume::from_storage(self.db, msg_ref.clone(), end) {
                Ok((message, remains)) => {
                    self.remaining.set_compacted_history(remains);
                    Ok((message, msg_ref))
                }
                Err(e) => Err(e),
            };
            return Some(result);
        }
        let (key, message) = self.remaining.tail_sequence.pop_front()?;
        Some(Ok((message, key)))
    }
}

impl<MessageKey, Message, Storage> MessagesRangeIterator<'_, MessageKey, Message, Storage>
where
    Storage: DurableStorageRead<MessageKey, Message>,
    Message: Clone,
    MessageKey: Clone + std::cmp::PartialEq,
{
    pub fn remaining_messages_from_db(
        &self,
    ) -> Result<Vec<Message>, <Storage as DurableStorageRead<MessageKey, Message>>::LoadError> {
        if let Some(db_stored) = self.remaining.compacted_history() {
            let (msg_ref, _end) = db_stored.clone().into_inner();
            self.db.remaining_messages(&msg_ref, usize::MAX)
        } else {
            Ok(vec![])
        }
    }
}

mod consume {
    use super::*;

    pub fn from_storage<MessageKey, Message, Storage>(
        db: &Storage,
        msg_ref: MessageKey,
        end: MessageKey,
    ) -> Result<
        (Message, Option<RangeInclusive<MessageKey>>),
        IteratorError<Storage::LoadError, MessageKey>,
    >
    where
        Storage: DurableStorageRead<MessageKey, Message>,
        MessageKey: std::cmp::PartialEq,
    {
        let message = db.load_message(&msg_ref).map_err(|e| IteratorError::LoadMessageError(e))?;
        if msg_ref == end {
            return Ok((message, None));
        }
        let Some(next_message_key) =
            db.next(&msg_ref).map_err(|e| IteratorError::LoadNextRefError(e))?
        else {
            return Err(IteratorError::BrokenRange { missing_end: end, actual_last: msg_ref });
        };
        Ok((message, Some(RangeInclusive::new(next_message_key, end))))
    }

    pub fn range_from_storage<MessageKey, Message, Storage>(
        db: &Storage,
        msg_ref: MessageKey,
        end: MessageKey,
        limit: usize,
    ) -> Result<
        (Vec<Message>, Option<RangeInclusive<MessageKey>>),
        IteratorError<Storage::LoadError, MessageKey>,
    >
    where
        Storage: DurableStorageRead<MessageKey, Message>,
        MessageKey: std::cmp::PartialEq + From<Message> + Clone,
        Message: Clone,
    {
        let mut messages = db
            .remaining_messages(&msg_ref, limit)
            .map_err(|e| IteratorError::LoadMessageError(e))?;

        let message_refs: Vec<_> = messages.iter().cloned().map(MessageKey::from).collect();

        if let Some(pos) = message_refs.iter().position(|k| k == &end) {
            if pos == message_refs.len() - 1 {
                // println!("Position OK");
                return Ok((messages, None));
            } else {
                // println!("Position in between");
                messages.truncate(pos + 1);
                let next_key = message_refs[pos + 1].clone();
                return Ok((messages, Some(RangeInclusive::new(next_key, end))));
            }
        }
        // The end has not been found yet
        match message_refs.last().cloned() {
            Some(last_key) => match db.next(&last_key).map_err(IteratorError::LoadNextRefError)? {
                Some(next_key) => {
                    // println!("Position not reached");
                    Ok((messages, Some(RangeInclusive::new(next_key, end))))
                }
                None => Err(IteratorError::BrokenRange { missing_end: end, actual_last: last_key }),
            },
            None => {
                // println!("Position none");
                Ok((vec![], None))
            }
        }
    }
}
