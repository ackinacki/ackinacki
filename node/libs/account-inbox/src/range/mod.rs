use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::ops::RangeInclusive;

use derive_getters::Getters;
use derive_setters::Setters;
use serde::Deserialize;
use serde::Serialize;

pub mod next;

#[derive(Clone, Getters, Setters, Serialize, Deserialize)]
#[setters(borrow_self, prefix = "set_")]
pub struct MessagesRange<MessageKey, Message> {
    compacted_history: Option<RangeInclusive<MessageKey>>,
    pub(crate) tail_sequence: VecDeque<(MessageKey, Message)>,
}

// TODO: for debug, remove this code
impl<MessageKey, Message> Debug for MessagesRange<MessageKey, Message>
where
    MessageKey: Debug,
    Message: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MessagesRange: {:?} {:?}", self.compacted_history, self.tail_sequence)
    }
}

impl<MessageKey, Message> MessagesRange<MessageKey, Message>
where
    MessageKey: PartialEq + Eq + Hash + Clone,
{
    pub fn empty() -> Self {
        Self { compacted_history: None, tail_sequence: VecDeque::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.compacted_history.is_none() && self.tail_sequence.is_empty()
    }

    pub fn add_messages(&mut self, messages: Vec<(MessageKey, Message)>) {
        self.tail_sequence.extend(messages);
    }

    pub fn length(&self) -> usize {
        self.tail_sequence.len()
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {}
}
