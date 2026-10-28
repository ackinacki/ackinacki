use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use multi_map::MapHash;
use multi_map::MapKey;
use multi_map::MapKeyPath;
use multi_map::MultiMapValue;
use node_types::AccountRouting;
use node_types::Blake3Hashable;
use node_types::DAppIdentifier;
use serde::Deserialize;
use serde::Serialize;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
pub use crate::types::thread_message_queue::diff::from_maps::ThreadMessageQueueStateDiff;
use crate::types::AccountInbox;

pub mod account_messages_iterator;
mod capacity;
pub use capacity::InboxCapacity;
mod diff;
pub(crate) mod legacy;
pub mod maps;
#[cfg(test)]
mod merkle_tests;
pub mod partition;
pub mod partition_codec;
mod seek;

#[derive(Clone, Serialize, Deserialize)]
pub struct ThreadMessageQueueState {
    pub messages: maps::QueueMap<DAppIdentifier, Arc<DAppMessageQueueState>>,
    pub(crate) cursor: Option<MapKey>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct DAppMessageQueueState {
    pub messages: maps::QueueMap<AccountRouting, Arc<AccountInbox>>,
    cursor: Option<MapKey>,
}

impl Debug for ThreadMessageQueueState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThreadMessageQueueState")
            .field("messages", &self.messages)
            .field("cursor", &self.cursor)
            .finish()
    }
}

impl Debug for DAppMessageQueueState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DAppMessageQueueState")
            .field("messages", &self.messages)
            .field("cursor", &self.cursor)
            .finish()
    }
}

impl DAppMessageQueueState {
    pub fn empty() -> Self {
        Self { messages: maps::QueueMap::new(), cursor: None }
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
        self.normalize_cursor();
    }

    pub fn normalize_cursor(&mut self) {
        self.cursor = self.messages.advance(self.cursor, 0);
    }

    pub fn destinations(&self) -> impl Iterator<Item = AccountRouting> + '_ {
        self.messages.keys()
    }

    pub fn cursor(&self) -> Option<MapKey> {
        self.cursor
    }

    pub fn advance_cursor(&mut self, consumed_accounts: usize) {
        // At the block boundary rotate by the number of accounts with consumption,
        // not by the last yielded/consumed account. Missing anchors seek forward.
        self.cursor = self.messages.advance(self.cursor, consumed_accounts);
    }
}

impl ThreadMessageQueueState {
    pub fn empty() -> Self {
        Self { messages: maps::QueueMap::new(), cursor: None }
    }

    pub fn account_inbox_by_routing(&self, routing: &AccountRouting) -> Option<&AccountInbox> {
        self.messages
            .get(routing.dapp_id())
            .and_then(|dapp_queue| dapp_queue.account_inbox(routing))
    }

    // Execution-only working copy. Its commitment is intentionally not updated;
    // the complete block diff is applied to the base queue in postprocessing.
    pub(crate) fn append_messages(
        &mut self,
        additions: &HashMap<AccountRouting, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    ) {
        let mut changes = HashMap::new();
        for (routing, messages) in additions {
            if messages.is_empty() {
                continue;
            }
            let mut inbox =
                self.account_inbox_by_routing(routing).cloned().unwrap_or_else(AccountInbox::empty);
            inbox.add_messages(messages.clone()).expect("inbox canonical size overflow");
            changes.insert(*routing, Some(Arc::new(inbox)));
        }
        self.update_inboxes(changes, &HashMap::new());
    }

    pub fn length(&self) -> usize {
        self.messages.values().map(|dapp_queue| dapp_queue.length()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    pub fn destinations(&self) -> indexset::BTreeSet<AccountRouting> {
        self.messages.values().flat_map(|dapp_queue| dapp_queue.destinations()).collect()
    }

    /// Commit each touched DApp once: single-key L2 updates, then one L1 batch.
    pub(crate) fn update_inboxes(
        &mut self,
        changes: HashMap<AccountRouting, Option<Arc<AccountInbox>>>,
        consumed_accounts: &HashMap<DAppIdentifier, usize>,
    ) {
        let mut by_dapp: HashMap<DAppIdentifier, Vec<_>> = HashMap::new();
        for (routing, inbox) in changes {
            by_dapp
                .entry(*routing.dapp_id())
                .or_default()
                .push((routing, inbox.filter(|inbox| !inbox.is_empty())));
        }
        let mut updates = Vec::with_capacity(by_dapp.len());
        for (id, changes) in by_dapp {
            let mut dapp = self
                .messages
                .get(&id)
                .cloned()
                .unwrap_or_else(|| Arc::new(DAppMessageQueueState::empty()));
            let queue = Arc::make_mut(&mut dapp);
            for (routing, inbox) in changes {
                queue.messages.update_single(routing, inbox);
            }
            queue.advance_cursor(consumed_accounts.get(&id).copied().unwrap_or_default());
            let value = if queue.is_empty() { None } else { Some(dapp) };
            updates.push((id, value));
        }
        self.messages.update_batch(updates);
        self.advance_cursor(consumed_accounts.len());
    }

    #[cfg(test)]
    pub(crate) fn set_inbox(&mut self, routing: AccountRouting, inbox: AccountInbox) {
        self.update_inboxes(HashMap::from([(routing, Some(Arc::new(inbox)))]), &HashMap::new());
    }

    pub fn remove_accounts(&mut self, accounts: impl IntoIterator<Item = AccountRouting>) {
        self.update_inboxes(
            accounts.into_iter().map(|routing| (routing, None)).collect(),
            &HashMap::new(),
        );
    }

    pub fn remove_routing(&mut self, routing: &AccountRouting) {
        self.remove_accounts([*routing]);
    }

    pub fn root_hash(&self) -> MapHash {
        self.messages.root_hash()
    }

    pub fn split_dapps(&self, prefix: MapKeyPath) -> (Self, Self) {
        let (rest, part) = self.messages.split(prefix);
        let mut rest = Self { messages: rest, cursor: self.cursor };
        rest.normalize_cursor();
        (rest, Self { messages: part, cursor: None })
    }

    pub fn graft_dapps(&self, part: &Self, prefix: MapKeyPath) -> anyhow::Result<Self> {
        let mut result =
            Self { messages: self.messages.graft(&part.messages, prefix)?, cursor: self.cursor };
        result.normalize_cursor();
        Ok(result)
    }

    pub fn split_accounts(&self, id: DAppIdentifier, prefix: MapKeyPath) -> (Self, Self) {
        assert!(prefix.len <= 256);
        let Some(dapp) = self.messages.get(&id) else {
            return (self.clone(), Self::empty());
        };
        let (rest, part) = dapp.messages.split(prefix);
        let mut without = self.clone();
        let mut rest = DAppMessageQueueState { messages: rest, cursor: dapp.cursor };
        rest.normalize_cursor();
        without.messages.update_batch(vec![(id, (!rest.is_empty()).then(|| Arc::new(rest)))]);
        without.normalize_cursor();
        let mut branch = Self::empty();
        if !part.is_empty() {
            branch
                .messages
                .insert(id, Arc::new(DAppMessageQueueState { messages: part, cursor: None }));
        }
        (without, branch)
    }

    pub fn graft_accounts(
        &self,
        part: &Self,
        id: DAppIdentifier,
        prefix: MapKeyPath,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(part.messages.keys().all(|key| key == id), "Account graft crosses DApps");
        let empty = DAppMessageQueueState::empty();
        let dest = self.messages.get(&id).map(Arc::as_ref).unwrap_or(&empty);
        let src = part.messages.get(&id).map(Arc::as_ref).unwrap_or(&empty);
        let messages = dest.messages.graft(&src.messages, prefix)?;
        let mut dapp = DAppMessageQueueState { messages, cursor: dest.cursor };
        dapp.normalize_cursor();
        let mut result = self.clone();
        result.messages.update_batch(vec![(id, (!dapp.is_empty()).then(|| Arc::new(dapp)))]);
        result.normalize_cursor();
        Ok(result)
    }

    pub fn normalize_cursor(&mut self) {
        self.cursor = self.messages.advance(self.cursor, 0);
    }

    pub fn cursor(&self) -> Option<MapKey> {
        self.cursor
    }

    pub fn advance_cursor(&mut self, consumed_dapps: usize) {
        // Same rotation rule at L1, independently for each priority queue.
        self.cursor = self.messages.advance(self.cursor, consumed_dapps);
    }
}

impl Blake3Hashable for DAppMessageQueueState {
    fn update_hasher(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&self.messages.root_hash().0);
    }
}

impl maps::QueueMapValue for DAppMessageQueueState {
    fn capacity(&self) -> InboxCapacity {
        self.messages.capacity()
    }
}

impl maps::QueueMapValue for AccountInbox {
    fn capacity(&self) -> InboxCapacity {
        InboxCapacity::new(self.canonical_size())
    }
}

impl MultiMapValue for DAppMessageQueueState {
    fn write_value<W: std::io::Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        Ok(bincode::serialize_into(writer, self)?)
    }

    fn read_value<R: std::io::Read>(reader: &mut R) -> anyhow::Result<Self> {
        Ok(bincode::deserialize_from(reader)?)
    }
}
