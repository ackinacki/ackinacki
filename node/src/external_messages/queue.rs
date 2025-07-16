// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;

use chrono::DateTime;
use chrono::Utc;
use derive_getters::Getters;
use tvm_block::Message;
use tvm_types::AccountId;

use crate::external_messages::stamp::Stamp;
use crate::message::WrappedMessage;

#[derive(Getters, Debug)]
pub struct ExternalMessagesQueue {
    messages: BTreeMap<Stamp, (AccountId, WrappedMessage)>,
    last_index: u64,
}

impl ExternalMessagesQueue {
    pub fn empty() -> Self {
        Self { messages: BTreeMap::new(), last_index: 0 }
    }

    pub fn erase_processed(&mut self, processed: &[Stamp]) {
        let to_remove: BTreeSet<_> = processed.iter().cloned().collect();
        self.messages.retain(|stamp, _| !to_remove.contains(stamp));
    }

    pub fn push_external_messages(
        &mut self,
        messages: &[WrappedMessage],
        timestamp: DateTime<Utc>,
    ) {
        let mut cursor = self.last_index;
        for message in messages.iter() {
            cursor += 1;
            let stamp = Stamp { index: cursor, timestamp };
            self.messages
                .insert(stamp, (message.message.int_dst_account_id().unwrap(), message.clone()));
        }
        self.last_index = cursor;
    }

    pub fn unprocessed_messages(&self) -> HashMap<AccountId, VecDeque<(Stamp, Message)>> {
        let mut grouped_by_acc: HashMap<AccountId, VecDeque<(Stamp, Message)>> = HashMap::new();

        for (stamp, (acc_id, msg)) in &self.messages {
            grouped_by_acc
                .entry(acc_id.clone())
                .or_default()
                .push_back((stamp.clone(), msg.message.clone()));
        }

        grouped_by_acc
    }
}
