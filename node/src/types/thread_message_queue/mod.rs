use serde::Deserialize;
use serde::Serialize;

use crate::types::thread_message_queue::diff::from_maps::ThreadMessageQueueStateBuilder;
use crate::types::thread_message_queue::diff::from_maps::ThreadMessageQueueStateDiff;
use crate::types::AccountAddress;
use crate::types::AccountInbox;
pub mod account_messages_iterator;
mod diff;
mod order_set;

#[derive(Clone, Serialize, Deserialize)]
pub struct ThreadMessageQueueState {
    // TODO: fix this:
    // Note: dirty solution
    pub messages: std::collections::BTreeMap<AccountAddress, AccountInbox>,
    order_set: order_set::OrderSet,
    cursor: usize,
}

impl ThreadMessageQueueState {
    pub fn empty() -> Self {
        Self { messages: Default::default(), order_set: order_set::OrderSet::new(), cursor: 0 }
    }

    pub fn account_inbox<'a>(
        &'a self,
        account_address: &'a AccountAddress,
    ) -> Option<&'a AccountInbox> {
        self.messages.get(account_address)
    }

    pub fn build_next() -> ThreadMessageQueueStateBuilder {
        ThreadMessageQueueStateDiff::builder()
    }

    pub fn length(&self) -> usize {
        self.messages.values().map(|inbox| inbox.length()).sum()
    }

    pub fn destinations(&self) -> &order_set::OrderSet {
        &self.order_set
    }
}
