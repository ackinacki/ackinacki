use chrono::DateTime;
use chrono::Utc;
use derive_getters::Getters;
use serde::Deserialize;
use serde::Serialize;

use super::progress::Progress;
use crate::message::WrappedMessage;
use crate::utilities::guarded::AllowGuardedMut;

#[derive(Serialize, Deserialize)]
pub struct Stamp {
    index: u32,
    timestamp: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Getters)]
pub struct ExternalMessagesQueue {
    messages: Vec<(Stamp, WrappedMessage)>,
    last_index: u32,
}

impl AllowGuardedMut for ExternalMessagesQueue {}

impl ExternalMessagesQueue {
    pub fn empty() -> Self {
        Self { messages: vec![], last_index: 0 }
    }

    pub fn erase_till(&mut self, progress_inclusive: &Progress) {
        self.messages
            .retain(|e| e.0.index > *progress_inclusive.last_processed_external_message_index());
    }

    pub fn push_external_messages(&mut self, messages: &[WrappedMessage]) {
        let mut cursor = self.last_index;
        let timestamp = Utc::now();
        for message in messages {
            cursor += 1;
            self.messages.push((Stamp { index: cursor, timestamp }, message.clone()));
        }
        self.last_index = cursor;
    }

    pub fn clone_tail(&self, progress_exclusive: &Progress) -> Vec<WrappedMessage> {
        tracing::trace!(
            "last_index: {}",
            progress_exclusive.last_processed_external_message_index(),
        );

        let mut res = vec![];
        for message in self.messages.iter() {
            if message.0.index > *progress_exclusive.last_processed_external_message_index() {
                res.push(message.1.clone())
            }
        }
        tracing::trace!("get_remaining_ext_messages result: {}", res.len());
        res
    }
}
