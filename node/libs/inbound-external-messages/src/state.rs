// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeSet as StdBTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::hash::Hash;

use derive_getters::Getters;
use indexset::BTreeSet;

use crate::Stamp;

pub trait InboundMessage: Clone + Send {
    type Destination: Copy + Eq + Hash + Send;
    type DApp: Copy + Eq + Ord + Hash + Send;
    type Account: Copy + Eq + Ord + Hash + Send;

    fn destination(&self) -> Self::Destination;
    fn dapp_id(destination: &Self::Destination) -> Self::DApp;
    fn account_id(destination: &Self::Destination) -> Self::Account;
}

#[derive(Clone, Copy, Debug)]
pub struct MessagesLimits {
    pub total: usize,
    pub per_dapp: usize,
    pub per_account: usize,
}

#[derive(Clone)]
pub struct MessagesSelectionCursor<M: InboundMessage> {
    dapp_offset: usize,
    account_offsets: HashMap<M::DApp, usize>,
}

impl<M: InboundMessage> Default for MessagesSelectionCursor<M> {
    fn default() -> Self {
        Self { dapp_offset: 0, account_offsets: HashMap::new() }
    }
}

#[derive(Clone, Getters)]
pub struct AccountMessages<M: InboundMessage> {
    messages: VecDeque<(Stamp, M)>,
}

impl<M: InboundMessage> Default for AccountMessages<M> {
    fn default() -> Self {
        Self { messages: VecDeque::new() }
    }
}

impl<M: InboundMessage> AccountMessages<M> {
    pub fn len(&self) -> usize {
        self.messages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }

    fn push_back(&mut self, stamp: Stamp, message: M) {
        self.messages.push_back((stamp, message));
    }

    fn insert_ordered(&mut self, stamp: Stamp, message: M) {
        let index = self
            .messages
            .iter()
            .position(|(existing_stamp, _)| existing_stamp > &stamp)
            .unwrap_or(self.messages.len());
        self.messages.insert(index, (stamp, message));
    }

    fn remove_stamps(&mut self, stamps: &StdBTreeSet<Stamp>) -> Vec<(Stamp, M)> {
        let mut removed = Vec::new();
        let mut retained = VecDeque::new();

        while let Some((stamp, message)) = self.messages.pop_front() {
            if stamps.contains(&stamp) {
                removed.push((stamp, message));
            } else {
                retained.push_back((stamp, message));
            }
        }

        self.messages = retained;
        removed
    }

    fn contains_stamp(&self, stamp: &Stamp) -> bool {
        self.messages.iter().any(|(existing, _)| existing == stamp)
    }
}

#[derive(Clone, Getters)]
pub struct DappMessages<M: InboundMessage> {
    messages: HashMap<M::Account, AccountMessages<M>>,
    count: usize,
    order_set: BTreeSet<M::Account>,
    cursor: usize,
}

impl<M: InboundMessage> Default for DappMessages<M> {
    fn default() -> Self {
        Self { messages: HashMap::new(), count: 0, order_set: BTreeSet::new(), cursor: 0 }
    }
}

impl<M: InboundMessage> DappMessages<M> {
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn push_back(&mut self, account_id: M::Account, stamp: Stamp, message: M) {
        if !self.order_set.contains(&account_id) {
            self.order_set.insert(account_id);
        }
        self.messages.entry(account_id).or_default().push_back(stamp, message);
        self.count += 1;
        self.normalize_cursor();
    }

    fn insert_ordered(&mut self, account_id: M::Account, stamp: Stamp, message: M) {
        if !self.order_set.contains(&account_id) {
            self.order_set.insert(account_id);
        }
        self.messages.entry(account_id).or_default().insert_ordered(stamp, message);
        self.count += 1;
        self.normalize_cursor();
    }

    fn remove_stamps(
        &mut self,
        stamps: &StdBTreeSet<Stamp>,
    ) -> (Vec<(Stamp, M)>, HashSet<M::Account>) {
        let old_order: Vec<_> = self.order_set.iter().copied().collect();
        let old_cursor = self.cursor;
        let mut removed = Vec::new();
        let mut empty_accounts = Vec::new();
        let mut consumed_accounts = HashSet::new();

        for (account_id, account_queue) in self.messages.iter_mut() {
            let mut account_removed = account_queue.remove_stamps(stamps);
            if !account_removed.is_empty() {
                consumed_accounts.insert(*account_id);
            }
            self.count -= account_removed.len();
            removed.append(&mut account_removed);
            if account_queue.is_empty() {
                empty_accounts.push(*account_id);
            }
        }

        for account_id in empty_accounts {
            self.messages.remove(&account_id);
            self.order_set.remove(&account_id);
        }
        self.cursor = cursor_after_consumed_groups(
            &old_order,
            old_cursor,
            &self.order_set,
            &consumed_accounts,
        );
        (removed, consumed_accounts)
    }

    fn contains_stamp(&self, stamp: &Stamp) -> bool {
        self.messages.values().any(|account_queue| account_queue.contains_stamp(stamp))
    }

    fn normalize_cursor(&mut self) {
        self.cursor =
            if !self.order_set.is_empty() { self.cursor % self.order_set.len() } else { 0 };
    }
}

#[derive(Clone, Getters)]
pub struct Messages<M: InboundMessage> {
    messages: HashMap<M::DApp, DappMessages<M>>,
    total_count: usize,
    limits: MessagesLimits,
    order_set: BTreeSet<M::DApp>,
    cursor: usize,
    last_index: u64,
}

impl<M: InboundMessage> Messages<M> {
    pub fn empty(limits: MessagesLimits) -> Self {
        Self {
            messages: HashMap::new(),
            total_count: 0,
            limits,
            order_set: BTreeSet::new(),
            cursor: 0,
            last_index: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.total_count
    }

    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    pub fn dapp_queue_sizes(&self) -> Vec<(M::DApp, usize)> {
        let mut sizes: Vec<_> =
            self.messages.iter().map(|(dapp_id, queue)| (*dapp_id, queue.len())).collect();
        sizes.sort_by_key(|(dapp_id, _)| *dapp_id);
        sizes
    }

    pub fn push_external_messages(
        &mut self,
        ext_messages: &[M],
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Vec<M> {
        let mut unused = Vec::new();

        for ext_message in ext_messages {
            let dst = ext_message.destination();
            let dapp_id = M::dapp_id(&dst);
            let account_id = M::account_id(&dst);

            let dapp_count = self.messages.get(&dapp_id).map(DappMessages::len).unwrap_or_default();
            let account_count = self
                .messages
                .get(&dapp_id)
                .and_then(|dapp| dapp.messages.get(&account_id))
                .map(AccountMessages::len)
                .unwrap_or_default();

            if self.total_count >= self.limits.total
                || dapp_count >= self.limits.per_dapp
                || account_count >= self.limits.per_account
            {
                unused.push(ext_message.clone());
                continue;
            }

            self.last_index += 1;
            let stamp = Stamp { index: self.last_index, timestamp };
            self.insert_with_stamp(dapp_id, account_id, stamp, ext_message.clone());
        }

        unused
    }

    pub fn erase_processed(&mut self, processed: &[Stamp]) -> Vec<(Stamp, M)> {
        let to_remove: StdBTreeSet<_> = processed.iter().cloned().collect();
        let old_order: Vec<_> = self.order_set.iter().copied().collect();
        let old_cursor = self.cursor;
        let mut removed = Vec::new();
        let mut empty_dapps = Vec::new();
        let mut consumed_dapps = HashSet::new();

        for (dapp_id, dapp_queue) in self.messages.iter_mut() {
            let (mut dapp_removed, _consumed_accounts) = dapp_queue.remove_stamps(&to_remove);

            if !dapp_removed.is_empty() {
                consumed_dapps.insert(*dapp_id);
            }
            self.total_count -= dapp_removed.len();
            removed.append(&mut dapp_removed);

            if dapp_queue.is_empty() {
                empty_dapps.push(*dapp_id);
            }
        }

        for dapp_id in empty_dapps {
            self.messages.remove(&dapp_id);
            self.order_set.remove(&dapp_id);
        }
        self.cursor =
            cursor_after_consumed_groups(&old_order, old_cursor, &self.order_set, &consumed_dapps);

        removed
    }

    pub fn next_message(
        &self,
        ignore_list: &HashSet<M::Destination>,
        requested_stamps: &StdBTreeSet<Stamp>,
        selection_cursor: &mut MessagesSelectionCursor<M>,
    ) -> Option<(Stamp, M)> {
        if self.order_set.is_empty() {
            selection_cursor.dapp_offset = 0;
            return None;
        }

        let dapp_start_offset = selection_cursor.dapp_offset % self.order_set.len();
        for dapp_offset in 0..self.order_set.len() {
            let dapp_offset = (dapp_start_offset + dapp_offset) % self.order_set.len();
            let dapp_index = (self.cursor + dapp_offset) % self.order_set.len();
            let Some(dapp_id) = self.order_set.get_index(dapp_index) else {
                continue;
            };
            let Some(dapp_queue) = self.messages.get(dapp_id) else {
                continue;
            };
            if dapp_queue.order_set.is_empty() {
                continue;
            }

            let account_start_offset =
                selection_cursor.account_offsets.get(dapp_id).copied().unwrap_or_default()
                    % dapp_queue.order_set.len();
            for account_offset in 0..dapp_queue.order_set.len() {
                let account_offset =
                    (account_start_offset + account_offset) % dapp_queue.order_set.len();
                let account_index =
                    (dapp_queue.cursor + account_offset) % dapp_queue.order_set.len();
                let Some(account_id) = dapp_queue.order_set.get_index(account_index) else {
                    continue;
                };
                let Some(account_queue) = dapp_queue.messages.get(account_id) else {
                    continue;
                };

                for (stamp, message) in &account_queue.messages {
                    if requested_stamps.contains(stamp) {
                        continue;
                    }
                    if ignore_list.contains(&message.destination()) {
                        break;
                    }
                    selection_cursor.dapp_offset = (dapp_offset + 1) % self.order_set.len();
                    selection_cursor
                        .account_offsets
                        .insert(*dapp_id, (account_offset + 1) % dapp_queue.order_set.len());
                    return Some((stamp.clone(), message.clone()));
                }
            }
        }

        None
    }

    pub fn restore_processed(&mut self, processed: &[(Stamp, M)]) {
        for (stamp, message) in processed {
            if self.contains_stamp(stamp) {
                continue;
            }

            let dst = message.destination();
            self.insert_with_existing_stamp(
                M::dapp_id(&dst),
                M::account_id(&dst),
                stamp.clone(),
                message.clone(),
            );
            self.last_index = self.last_index.max(stamp.index);
        }
    }

    pub fn drain_all(&mut self) -> Vec<(Stamp, M)> {
        let mut drained = Vec::with_capacity(self.total_count);
        for dapp_queue in self.messages.values_mut() {
            for account_queue in dapp_queue.messages.values_mut() {
                drained.extend(account_queue.messages.drain(..));
            }
        }

        self.messages.clear();
        self.order_set.clear();
        self.total_count = 0;
        self.cursor = 0;
        drained
    }

    fn insert_with_stamp(
        &mut self,
        dapp_id: M::DApp,
        account_id: M::Account,
        stamp: Stamp,
        message: M,
    ) {
        if !self.order_set.contains(&dapp_id) {
            self.order_set.insert(dapp_id);
        }
        self.messages.entry(dapp_id).or_default().push_back(account_id, stamp, message);
        self.total_count += 1;
        self.normalize_cursor();
    }

    fn insert_with_existing_stamp(
        &mut self,
        dapp_id: M::DApp,
        account_id: M::Account,
        stamp: Stamp,
        message: M,
    ) {
        if !self.order_set.contains(&dapp_id) {
            self.order_set.insert(dapp_id);
        }
        self.messages.entry(dapp_id).or_default().insert_ordered(account_id, stamp, message);
        self.total_count += 1;
        self.normalize_cursor();
    }

    fn contains_stamp(&self, stamp: &Stamp) -> bool {
        self.messages.values().any(|dapp_queue| dapp_queue.contains_stamp(stamp))
    }

    fn normalize_cursor(&mut self) {
        self.cursor =
            if !self.order_set.is_empty() { self.cursor % self.order_set.len() } else { 0 };
    }
}

fn cursor_after_consumed_groups<T>(
    old_order: &[T],
    old_cursor: usize,
    new_order: &BTreeSet<T>,
    consumed: &HashSet<T>,
) -> usize
where
    T: Copy + Eq + Ord + Hash,
{
    if new_order.is_empty() || old_order.is_empty() {
        return 0;
    }

    for offset in 0..old_order.len() {
        let old_index = (old_cursor + offset) % old_order.len();
        let item = old_order[old_index];
        if consumed.contains(&item) {
            continue;
        }
        if let Some(new_index) = new_order.iter().position(|candidate| candidate == &item) {
            return new_index;
        }
    }

    0
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet as StdBTreeSet;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::collections::VecDeque;

    use chrono::Utc;

    use super::*;
    use crate::grouped_scheduler;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct Destination {
        dapp_id: u8,
        account_id: u8,
    }

    #[derive(Clone, Debug)]
    struct TestMessage {
        dst: Destination,
    }

    impl InboundMessage for TestMessage {
        type Account = u8;
        type DApp = u8;
        type Destination = Destination;

        fn destination(&self) -> Self::Destination {
            self.dst
        }

        fn dapp_id(destination: &Self::Destination) -> Self::DApp {
            destination.dapp_id
        }

        fn account_id(destination: &Self::Destination) -> Self::Account {
            destination.account_id
        }
    }

    fn message(dapp_id: u8, account_id: u8) -> TestMessage {
        TestMessage { dst: Destination { dapp_id, account_id } }
    }

    fn limits(value: usize) -> MessagesLimits {
        MessagesLimits { total: value, per_dapp: value, per_account: value }
    }

    fn stamp(index: u64) -> Stamp {
        Stamp { index, timestamp: Utc::now() }
    }

    #[test]
    fn push_updates_total_dapp_and_account_counts() {
        let mut state = Messages::empty(limits(10));

        state.push_external_messages(&[message(1, 1), message(1, 2)], Utc::now());

        assert_eq!(state.len(), 2);
        assert_eq!(state.messages.get(&1).unwrap().len(), 2);
        assert_eq!(state.messages.get(&1).unwrap().messages.get(&1).unwrap().len(), 1);
    }

    #[test]
    fn limits_reject_messages_at_each_level() {
        let mut state = Messages::empty(MessagesLimits { total: 3, per_dapp: 2, per_account: 1 });

        let rejected = state.push_external_messages(
            &[
                message(1, 1),
                message(1, 1),
                message(1, 2),
                message(1, 3),
                message(2, 1),
                message(3, 1),
            ],
            Utc::now(),
        );

        assert_eq!(state.len(), 3);
        assert_eq!(rejected.len(), 3);
        assert_eq!(state.messages.get(&1).unwrap().len(), 2);
    }

    #[test]
    fn erase_processed_removes_empty_groups_and_updates_counts() {
        let mut state = Messages::empty(limits(10));
        state.push_external_messages(&[message(1, 1), message(1, 2)], Utc::now());

        let stamp = state
            .messages
            .get(&1)
            .unwrap()
            .messages
            .get(&1)
            .unwrap()
            .messages
            .front()
            .unwrap()
            .0
            .clone();

        let removed = state.erase_processed(&[stamp]);

        assert_eq!(removed.len(), 1);
        assert_eq!(state.len(), 1);
        assert!(!state.messages.get(&1).unwrap().messages.contains_key(&1));
    }

    #[test]
    fn restore_processed_is_idempotent() {
        let mut state = Messages::empty(limits(10));
        let msg = message(1, 1);
        let stamp = stamp(1);

        state.restore_processed(&[(stamp.clone(), msg.clone())]);
        state.restore_processed(&[(stamp, msg)]);

        assert_eq!(state.len(), 1);
        assert_eq!(state.messages.get(&1).unwrap().len(), 1);
    }

    #[test]
    fn restore_processed_keeps_stamp_order_with_newer_messages() {
        let mut state = Messages::empty(limits(10));
        let old_msg = message(1, 1);
        let old_stamp = stamp(1);

        state.restore_processed(&[(old_stamp.clone(), old_msg.clone())]);
        let removed = state.erase_processed(std::slice::from_ref(&old_stamp));
        state.push_external_messages(&[message(1, 1)], Utc::now());
        state.restore_processed(&removed);

        let mut selection_cursor = MessagesSelectionCursor::default();
        let (selected_stamp, selected_msg) = state
            .next_message(&HashSet::new(), &StdBTreeSet::new(), &mut selection_cursor)
            .unwrap();

        assert_eq!(selected_stamp, old_stamp);
        assert_eq!(selected_msg.destination(), old_msg.destination());
    }

    #[test]
    fn erase_processed_advances_dapp_cursor_by_consumed_dapps() {
        let mut state = Messages::empty(limits(10));
        state.push_external_messages(
            &[message(1, 1), message(1, 2), message(2, 1), message(3, 1)],
            Utc::now(),
        );

        let dapp_one_stamps: Vec<_> = state
            .messages
            .get(&1)
            .unwrap()
            .messages
            .values()
            .flat_map(|account_queue| account_queue.messages.iter().map(|(stamp, _)| stamp.clone()))
            .collect();
        state.erase_processed(&dapp_one_stamps);

        let mut selection_cursor = MessagesSelectionCursor::default();
        let (_selected_stamp, selected_msg) = state
            .next_message(&HashSet::new(), &StdBTreeSet::new(), &mut selection_cursor)
            .unwrap();

        assert_eq!(selected_msg.destination().dapp_id, 2);
    }

    #[test]
    fn erase_processed_does_not_advance_dapp_cursor_for_non_prefix_consumed_dapp() {
        let mut state = Messages::empty(limits(10));
        state.push_external_messages(&[message(1, 1), message(2, 1), message(3, 1)], Utc::now());

        let third_dapp_stamp = state
            .messages
            .get(&3)
            .unwrap()
            .messages
            .get(&1)
            .unwrap()
            .messages
            .front()
            .unwrap()
            .0
            .clone();
        state.erase_processed(&[third_dapp_stamp]);

        let mut selection_cursor = MessagesSelectionCursor::default();
        let (_selected_stamp, selected_msg) = state
            .next_message(&HashSet::new(), &StdBTreeSet::new(), &mut selection_cursor)
            .unwrap();

        assert_eq!(selected_msg.destination().dapp_id, 1);
    }

    #[test]
    fn erase_processed_does_not_skip_account_after_removed_account() {
        let mut state = Messages::empty(limits(10));
        state.push_external_messages(&[message(1, 1), message(1, 2), message(1, 3)], Utc::now());

        let first_account_stamp = state
            .messages
            .get(&1)
            .unwrap()
            .messages
            .get(&1)
            .unwrap()
            .messages
            .front()
            .unwrap()
            .0
            .clone();
        state.erase_processed(&[first_account_stamp]);

        let mut selection_cursor = MessagesSelectionCursor::default();
        let (_selected_stamp, selected_msg) = state
            .next_message(&HashSet::new(), &StdBTreeSet::new(), &mut selection_cursor)
            .unwrap();

        assert_eq!(selected_msg.destination().account_id, 2);
    }

    #[test]
    fn erase_processed_does_not_skip_non_prefix_consumed_account() {
        let mut state = Messages::empty(limits(10));
        state.push_external_messages(&[message(1, 1), message(1, 2), message(1, 3)], Utc::now());

        let stamps: Vec<_> = [1, 3]
            .into_iter()
            .map(|account_id| {
                state
                    .messages
                    .get(&1)
                    .unwrap()
                    .messages
                    .get(&account_id)
                    .unwrap()
                    .messages
                    .front()
                    .unwrap()
                    .0
                    .clone()
            })
            .collect();
        state.erase_processed(&stamps);

        let mut selection_cursor = MessagesSelectionCursor::default();
        let (_selected_stamp, selected_msg) = state
            .next_message(&HashSet::new(), &StdBTreeSet::new(), &mut selection_cursor)
            .unwrap();

        assert_eq!(selected_msg.destination().account_id, 2);
    }

    #[test]
    fn next_message_keeps_same_account_in_different_dapps_separate() {
        let mut state = Messages::empty(limits(10));
        state.push_external_messages(&[message(1, 1), message(2, 1)], Utc::now());

        let ignore_list = HashSet::new();
        let mut requested_stamps = StdBTreeSet::new();
        let mut selection_cursor = MessagesSelectionCursor::default();
        let (first_stamp, first_message) =
            state.next_message(&ignore_list, &requested_stamps, &mut selection_cursor).unwrap();
        requested_stamps.insert(first_stamp);
        let (_second_stamp, second_message) =
            state.next_message(&ignore_list, &requested_stamps, &mut selection_cursor).unwrap();

        assert_eq!(first_message.destination().account_id, 1);
        assert_eq!(second_message.destination().account_id, 1);
        assert_ne!(first_message.destination().dapp_id, second_message.destination().dapp_id);
    }

    #[test]
    fn next_message_round_robins_between_dapps_during_selection() {
        let mut state = Messages::empty(limits(10));
        state.push_external_messages(&[message(1, 1), message(1, 2), message(2, 1)], Utc::now());

        let ignore_list = HashSet::new();
        let mut requested_stamps = StdBTreeSet::new();
        let mut selection_cursor = MessagesSelectionCursor::default();
        let mut selected_dapps = Vec::new();

        for _ in 0..3 {
            let (stamp, message) =
                state.next_message(&ignore_list, &requested_stamps, &mut selection_cursor).unwrap();
            requested_stamps.insert(stamp);
            selected_dapps.push(message.destination().dapp_id);
        }

        assert_eq!(selected_dapps, vec![1, 2, 1]);
    }

    #[test]
    fn source_from_grouped_replays_messages_by_stamp_order() {
        let mut grouped: HashMap<Destination, VecDeque<(Stamp, TestMessage)>> = HashMap::new();
        grouped
            .entry(Destination { dapp_id: 1, account_id: 1 })
            .or_default()
            .push_back((stamp(2), message(1, 1)));
        grouped
            .entry(Destination { dapp_id: 2, account_id: 1 })
            .or_default()
            .push_back((stamp(1), message(2, 1)));

        let source = grouped_scheduler(grouped);
        let mut cursor = MessagesSelectionCursor::default();
        let (stamp, message) =
            source.next_message(&HashSet::new(), &StdBTreeSet::new(), &mut cursor).unwrap();

        assert_eq!(stamp.index, 1);
        assert_eq!(message.destination().dapp_id, 2);
    }
}
