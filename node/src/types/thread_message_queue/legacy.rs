//! Numeric traversal oracle used only by iterator regression tests.
use std::collections::BTreeMap;

use account_inbox::range::MessagesRange;

use super::*;

pub(crate) type LegacyInbox = MessagesRange<MessageIdentifier, Arc<WrappedMessage>>;

pub(crate) struct Queue {
    pub messages: BTreeMap<DAppIdentifier, Dapp>,
    pub order_set: Vec<DAppIdentifier>,
    pub cursor: usize,
}

pub(crate) struct Dapp {
    pub messages: BTreeMap<AccountRouting, LegacyInbox>,
    pub order_set: Vec<AccountRouting>,
    pub cursor: usize,
}

impl Queue {
    pub(crate) fn from_current(source: &ThreadMessageQueueState) -> Self {
        let messages: BTreeMap<_, _> = source
            .messages
            .iter()
            .map(|(id, dapp)| {
                let messages: BTreeMap<_, _> = dapp
                    .messages
                    .iter()
                    .map(|(routing, inbox)| (routing, inbox.messages.clone()))
                    .collect();
                let order_set: Vec<_> = messages.keys().copied().collect();
                let cursor = dapp
                    .cursor()
                    .and_then(|cursor| {
                        order_set
                            .iter()
                            .position(|route| MapKey(*route.account_id().as_array()) >= cursor)
                    })
                    .unwrap_or(0);
                (id, Dapp { messages, order_set, cursor })
            })
            .collect();
        let order_set: Vec<_> = messages.keys().copied().collect();
        let cursor = source
            .cursor()
            .and_then(|cursor| order_set.iter().position(|id| MapKey(*id.as_array()) >= cursor))
            .unwrap_or(0);
        Self { messages, order_set, cursor }
    }
}
