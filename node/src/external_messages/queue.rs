// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;

use chrono::Utc;
use derive_getters::Getters;
use serde::Deserialize;
use serde::Serialize;
use tvm_block::Message;

use crate::external_messages::stamp::Stamp;
use crate::message::WrappedMessage;
use crate::utilities::guarded::AllowGuardedMut;

#[derive(Serialize, Deserialize, Getters)]
pub struct ExternalMessagesQueue {
    messages: BTreeMap<Stamp, WrappedMessage>,
    last_index: u32,
}

impl std::fmt::Debug for ExternalMessagesQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExternalMessagesQueue(last_index={})", self.last_index)
    }
}

impl AllowGuardedMut for ExternalMessagesQueue {}

impl ExternalMessagesQueue {
    pub fn empty() -> Self {
        Self { messages: BTreeMap::new(), last_index: 0 }
    }

    pub fn erase_processed(&mut self, processed: &Vec<Stamp>) {
        for stamp in processed {
            self.messages.remove(stamp);
        }
    }

    pub fn push_external_messages(&mut self, messages: &[WrappedMessage]) {
        let mut cursor = self.last_index;
        let timestamp = Utc::now();
        for message in messages {
            cursor += 1;
            self.messages.insert(Stamp { index: cursor, timestamp }, message.clone());
        }
        self.last_index = cursor;
    }

    pub fn unprocessed_messages(&self) -> Vec<(Stamp, Message)> {
        self.messages.iter().map(|(stamp, msg)| (stamp.clone(), msg.message.clone())).collect()
    }
}
