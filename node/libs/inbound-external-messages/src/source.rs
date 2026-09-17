// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeSet as StdBTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::InboundMessage;
use crate::Messages;
use crate::MessagesSelectionCursor;
use crate::Stamp;

pub trait SchedulableMessage: InboundMessage {}

impl<M: InboundMessage> SchedulableMessage for M {}

pub trait MessageScheduler<M: SchedulableMessage>: Send {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn next_message(
        &self,
        ignore_list: &HashSet<M::Destination>,
        requested_stamps: &StdBTreeSet<Stamp>,
        selection_cursor: &mut MessagesSelectionCursor<M>,
    ) -> Option<(Stamp, M)>;

    fn next_low_priority_message(
        &self,
        ignore_list: &HashSet<M::Destination>,
        requested_stamps: &StdBTreeSet<Stamp>,
        selection_cursor: &mut MessagesSelectionCursor<M>,
    ) -> Option<(Stamp, M)>;

    fn restore_processed(&mut self, processed: &[(Stamp, M)]);
}

pub struct OrderedMessages<M: SchedulableMessage> {
    messages: Vec<(Stamp, M)>,
}

impl<M: SchedulableMessage> OrderedMessages<M> {
    pub fn new(mut messages: Vec<(Stamp, M)>) -> Self {
        messages.sort_by(|(left_stamp, _), (right_stamp, _)| left_stamp.cmp(right_stamp));
        Self { messages }
    }

    pub fn from_grouped(grouped: HashMap<M::Destination, VecDeque<(Stamp, M)>>) -> Self {
        Self::new(grouped.into_values().flatten().collect())
    }
}

impl<M: SchedulableMessage> MessageScheduler<M> for Arc<Mutex<Messages<M>>> {
    fn len(&self) -> usize {
        self.lock().len()
    }

    fn next_message(
        &self,
        ignore_list: &HashSet<M::Destination>,
        requested_stamps: &StdBTreeSet<Stamp>,
        selection_cursor: &mut MessagesSelectionCursor<M>,
    ) -> Option<(Stamp, M)> {
        self.lock().next_message(ignore_list, requested_stamps, selection_cursor)
    }

    fn next_low_priority_message(
        &self,
        ignore_list: &HashSet<M::Destination>,
        requested_stamps: &StdBTreeSet<Stamp>,
        selection_cursor: &mut MessagesSelectionCursor<M>,
    ) -> Option<(Stamp, M)> {
        self.lock().next_low_priority_message(ignore_list, requested_stamps, selection_cursor)
    }

    fn restore_processed(&mut self, processed: &[(Stamp, M)]) {
        self.lock().restore_processed(processed);
    }
}

impl<M: SchedulableMessage> MessageScheduler<M> for Messages<M> {
    fn len(&self) -> usize {
        self.len()
    }

    fn next_message(
        &self,
        ignore_list: &HashSet<M::Destination>,
        requested_stamps: &StdBTreeSet<Stamp>,
        selection_cursor: &mut MessagesSelectionCursor<M>,
    ) -> Option<(Stamp, M)> {
        self.next_message(ignore_list, requested_stamps, selection_cursor)
    }

    fn next_low_priority_message(
        &self,
        ignore_list: &HashSet<M::Destination>,
        requested_stamps: &StdBTreeSet<Stamp>,
        selection_cursor: &mut MessagesSelectionCursor<M>,
    ) -> Option<(Stamp, M)> {
        self.next_low_priority_message(ignore_list, requested_stamps, selection_cursor)
    }

    fn restore_processed(&mut self, processed: &[(Stamp, M)]) {
        self.restore_processed(processed);
    }
}

impl<M: SchedulableMessage> MessageScheduler<M> for OrderedMessages<M> {
    fn len(&self) -> usize {
        self.messages.len()
    }

    fn next_message(
        &self,
        ignore_list: &HashSet<M::Destination>,
        requested_stamps: &StdBTreeSet<Stamp>,
        _selection_cursor: &mut MessagesSelectionCursor<M>,
    ) -> Option<(Stamp, M)> {
        self.messages
            .iter()
            .find(|(stamp, message)| {
                !requested_stamps.contains(stamp) && !ignore_list.contains(&message.destination())
            })
            .cloned()
    }

    fn next_low_priority_message(
        &self,
        ignore_list: &HashSet<M::Destination>,
        requested_stamps: &StdBTreeSet<Stamp>,
        _selection_cursor: &mut MessagesSelectionCursor<M>,
    ) -> Option<(Stamp, M)> {
        self.messages
            .iter()
            .find(|(stamp, message)| {
                message.is_low_priority()
                    && !requested_stamps.contains(stamp)
                    && !ignore_list.contains(&message.destination())
            })
            .cloned()
    }

    fn restore_processed(&mut self, processed: &[(Stamp, M)]) {
        for (stamp, message) in processed {
            if self.messages.iter().any(|(existing_stamp, _)| existing_stamp == stamp) {
                continue;
            }
            self.messages.push((stamp.clone(), message.clone()));
        }
        self.messages.sort_by(|(left_stamp, _), (right_stamp, _)| left_stamp.cmp(right_stamp));
    }
}

pub fn shared_scheduler<M: SchedulableMessage + 'static>(
    queue: Arc<Mutex<Messages<M>>>,
) -> Box<dyn MessageScheduler<M>> {
    Box::new(queue)
}

pub fn owned_scheduler<M: SchedulableMessage + 'static>(
    messages: Messages<M>,
) -> Box<dyn MessageScheduler<M>> {
    Box::new(messages)
}

pub fn grouped_scheduler<M: SchedulableMessage + 'static>(
    grouped: HashMap<M::Destination, VecDeque<(Stamp, M)>>,
) -> Box<dyn MessageScheduler<M>> {
    Box::new(OrderedMessages::from_grouped(grouped))
}
