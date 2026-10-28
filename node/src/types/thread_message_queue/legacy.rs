//! SNAP2V1 queue wire layout, confined to snapshot compatibility.
use std::collections::BTreeMap;

use account_inbox::range::MessagesRange;
use anyhow::ensure;

use super::*;

pub(crate) type LegacyInbox = MessagesRange<MessageIdentifier, Arc<WrappedMessage>>;

pub(crate) fn inbox_from_legacy(
    messages: LegacyInbox,
    history: &[Arc<WrappedMessage>],
) -> anyhow::Result<AccountInbox> {
    match messages.compacted_history() {
        Some(range) => ensure!(
            history.first().map(|m| m.identifier()) == Some(range.start().clone())
                && history.last().map(|m| m.identifier()) == Some(range.end().clone()),
            "Legacy inbox history endpoints mismatch"
        ),
        None => ensure!(history.is_empty(), "Unexpected legacy history bodies"),
    }
    let mut inbox = AccountInbox::empty();
    for body in history {
        ensure!(
            body.tvm_message().map_err(anyhow::Error::msg)?.int_header().is_some(),
            "Non-internal legacy inbox message"
        );
        inbox.commitment.add(&body.identifier());
        inbox.add_capacity(body.canonical_size())?;
    }
    for (key, body) in messages.tail_sequence() {
        ensure!(
            body.tvm_message().map_err(anyhow::Error::msg)?.int_header().is_some(),
            "Non-internal legacy inbox message"
        );
        ensure!(*key == body.identifier(), "Legacy inbox message key mismatch");
        inbox.commitment.add(key);
        inbox.add_capacity(body.canonical_size())?;
    }
    inbox.messages = messages;
    Ok(inbox)
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Queue {
    pub messages: BTreeMap<DAppIdentifier, Dapp>,
    pub order_set: Vec<DAppIdentifier>,
    pub cursor: usize,
}

#[derive(Serialize, Deserialize)]
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

    pub(crate) fn into_current(
        self,
        groups: &mut HashMap<MessageIdentifier, &[Arc<WrappedMessage>]>,
    ) -> anyhow::Result<ThreadMessageQueueState> {
        ensure!(self.order_set.iter().eq(self.messages.keys()), "Legacy DApp index mismatch");
        ensure!(self.cursor < self.order_set.len().max(1), "Invalid legacy DApp cursor");
        let mut queue = ThreadMessageQueueState::empty();
        queue.cursor = self.order_set.get(self.cursor).map(|id| MapKey(*id.as_array()));
        for (dapp_id, source) in self.messages {
            ensure!(
                source.order_set.iter().eq(source.messages.keys()),
                "Legacy account index mismatch"
            );
            ensure!(source.cursor < source.order_set.len().max(1), "Invalid legacy account cursor");
            let mut dapp = DAppMessageQueueState::empty();
            dapp.cursor = source
                .order_set
                .get(source.cursor)
                .map(|route| MapKey(*route.account_id().as_array()));
            for (routing, messages) in source.messages {
                ensure!(routing.dapp_id() == &dapp_id, "Legacy inbox DApp mismatch");
                let history = if let Some(range) = messages.compacted_history() {
                    let history = groups.remove(range.start()).ok_or_else(|| {
                        anyhow::anyhow!("Missing legacy inbox history: {routing}")
                    })?;
                    ensure!(
                        history.last().map(|m| m.identifier()) == Some(range.end().clone()),
                        "Legacy inbox history endpoint mismatch"
                    );
                    history
                } else {
                    &[]
                };
                let inbox = inbox_from_legacy(messages, history)?;
                if !inbox.is_empty() {
                    dapp.messages.insert(routing, Arc::new(inbox));
                }
            }
            dapp.normalize_cursor();
            if !dapp.is_empty() {
                queue.messages.insert(dapp_id, Arc::new(dapp));
            }
        }
        queue.normalize_cursor();
        Ok(queue)
    }
}
