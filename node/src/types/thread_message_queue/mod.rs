use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use node_types::AccountRouting;
use node_types::DAppIdentifier;
use serde::Deserialize;
use serde::Serialize;

use crate::types::thread_message_queue::diff::from_maps::ThreadMessageQueueStateBuilder;
use crate::types::thread_message_queue::diff::from_maps::ThreadMessageQueueStateDiff;
use crate::types::AccountInbox;

pub mod account_messages_iterator;
mod diff;
mod order_set;

#[derive(Clone, Serialize, Deserialize)]
pub struct ThreadMessageQueueState {
    pub messages: BTreeMap<DAppIdentifier, Arc<DAppMessageQueueState>>,
    pub(crate) order_set: order_set::OrderSetDappIdentifier,
    pub(crate) cursor: usize,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DAppMessageQueueState {
    pub messages: BTreeMap<AccountRouting, Arc<AccountInbox>>,
    order_set: order_set::OrderSet,
    cursor: usize,
}

impl Debug for ThreadMessageQueueState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadMessageQueueState")
            .field("messages", &self.messages)
            .field("order_set", &self.order_set)
            .field("cursor", &self.cursor)
            .finish()
    }
}

impl Debug for DAppMessageQueueState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DAppMessageQueueState")
            .field("messages", &self.messages)
            .field("order_set", &self.order_set)
            .field("cursor", &self.cursor)
            .finish()
    }
}

impl DAppMessageQueueState {
    pub fn empty() -> Self {
        Self { messages: BTreeMap::new(), order_set: order_set::OrderSet::new(), cursor: 0 }
    }

    pub fn length(&self) -> usize {
        self.messages.values().map(|inbox| inbox.length()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn account_inbox(&self, routing: &AccountRouting) -> Option<&AccountInbox> {
        self.messages.get(routing).map(Arc::as_ref)
    }

    pub fn insert_inbox(&mut self, routing: AccountRouting, inbox: AccountInbox) {
        let prev = self.messages.insert(routing, Arc::new(inbox));
        #[cfg(feature = "fail-fast")]
        assert!(prev.is_none(), "dirty state detected");
        if !self.order_set.contains(&routing) {
            self.order_set.insert(routing);
        }
        self.normalize_cursor();
    }

    pub fn inbox_mut_or_insert_empty(&mut self, routing: AccountRouting) -> &mut AccountInbox {
        if !self.order_set.contains(&routing) {
            self.order_set.insert(routing);
        }
        Arc::make_mut(
            self.messages.entry(routing).or_insert_with(|| Arc::new(AccountInbox::empty())),
        )
    }

    pub fn remove_account(&mut self, routing: &AccountRouting) {
        self.messages.remove(routing);
        self.order_set.remove(routing);
        self.normalize_cursor();
    }

    pub fn normalize_cursor(&mut self) {
        self.cursor =
            if !self.order_set.is_empty() { self.cursor % self.order_set.len() } else { 0 };
    }

    pub fn destinations(&self) -> &order_set::OrderSet {
        &self.order_set
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn advance_cursor(&mut self, consumed_accounts: usize) {
        self.cursor = if !self.order_set.is_empty() {
            (self.cursor + consumed_accounts) % self.order_set.len()
        } else {
            0
        };
    }
}

impl ThreadMessageQueueState {
    pub fn empty() -> Self {
        Self {
            messages: BTreeMap::new(),
            order_set: order_set::OrderSetDappIdentifier::new(),
            cursor: 0,
        }
    }

    pub fn account_inbox_by_routing(&self, routing: &AccountRouting) -> Option<&AccountInbox> {
        self.messages
            .get(routing.dapp_id())
            .and_then(|dapp_queue| dapp_queue.account_inbox(routing))
    }

    pub fn build_next() -> ThreadMessageQueueStateBuilder {
        ThreadMessageQueueStateDiff::builder()
    }

    pub fn length(&self) -> usize {
        self.messages.values().map(|dapp_queue| dapp_queue.length()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn destinations(&self) -> indexset::BTreeSet<AccountRouting> {
        self.messages.values().flat_map(|dapp_queue| dapp_queue.destinations().to_set()).collect()
    }

    pub fn remove_accounts(&mut self, accounts: impl IntoIterator<Item = AccountRouting>) {
        for routing in accounts {
            self.remove_routing(&routing);
        }
        self.normalize_cursor();
    }

    pub fn dapp_queue_mut_or_insert_empty(
        &mut self,
        dapp_id: DAppIdentifier,
    ) -> &mut DAppMessageQueueState {
        if !self.order_set.contains(&dapp_id) {
            self.order_set.insert(dapp_id);
        }
        Arc::make_mut(
            self.messages
                .entry(dapp_id)
                .or_insert_with(|| Arc::new(DAppMessageQueueState::empty())),
        )
    }

    pub fn remove_routing(&mut self, routing: &AccountRouting) {
        let dapp_id = *routing.dapp_id();
        let should_remove_dapp = if let Some(dapp_queue) = self.messages.get_mut(&dapp_id) {
            let dapp_queue = Arc::make_mut(dapp_queue);
            dapp_queue.remove_account(routing);
            dapp_queue.is_empty()
        } else {
            false
        };
        if should_remove_dapp {
            self.messages.remove(&dapp_id);
            self.order_set.remove(&dapp_id);
        }
        self.normalize_cursor();
    }

    pub fn normalize_cursor(&mut self) {
        self.cursor =
            if !self.order_set.is_empty() { self.cursor % self.order_set.len() } else { 0 };
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn advance_cursor(&mut self, consumed_dapps: usize) {
        self.cursor = if !self.order_set.is_empty() {
            (self.cursor + consumed_dapps) % self.order_set.len()
        } else {
            0
        };
    }
}
