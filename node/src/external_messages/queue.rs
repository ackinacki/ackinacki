// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::VecDeque;

use chrono::DateTime;
use chrono::Utc;
use derive_getters::Getters;
use http_server::NotQueuedExtMessage;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;
use tvm_block::GetRepresentationHash;
use tvm_block::Message;
use tvm_types::UInt256;

use crate::external_messages::stamp::Stamp;

#[derive(Clone, Debug)]
enum Status {
    Pending,  // Just queued and not processed yet external messages
    Included, // External messages already included in block in undergoing validation
}

#[derive(Default, Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ExtMessageDst {
    pub account_id: AccountIdentifier,
    pub dapp_id: Option<DAppIdentifier>,
}

impl ExtMessageDst {
    pub fn new(account_id: AccountIdentifier, dapp_id: Option<DAppIdentifier>) -> Self {
        Self { account_id, dapp_id }
    }

    pub fn from_message(
        message: &Message,
        dapp_id: Option<DAppIdentifier>,
    ) -> anyhow::Result<Self> {
        Ok(Self::new(
            message
                .int_dst_account_id()
                .map(AccountIdentifier::from)
                .ok_or_else(|| anyhow::anyhow!("Message doesn't have destination account ID"))?,
            dapp_id,
        ))
    }
}

#[derive(Clone, Debug)]
pub struct QueuedExtMessage {
    _status: Status, // Not used yet.
    tvm_message: Message,
    hash: UInt256,
    dst: ExtMessageDst,
}

impl QueuedExtMessage {
    fn try_new(
        status: Status,
        dapp_id: Option<DAppIdentifier>,
        message: Message,
    ) -> anyhow::Result<Self> {
        let hash = message
            .hash()
            .map_err(|err| anyhow::anyhow!("Failed to calculate message hash: {err}"))?;
        let dst = ExtMessageDst::from_message(&message, dapp_id)?;
        Ok(Self { _status: status, tvm_message: message, hash, dst })
    }

    pub fn try_from_incoming(ext_message: NotQueuedExtMessage) -> anyhow::Result<Self> {
        Self::try_new(Status::Pending, ext_message.dst_dapp_id(), ext_message.into_tvm_message())
    }

    pub fn try_from_block(tvm_message: Message) -> anyhow::Result<Self> {
        Self::try_new(Status::Included, None, tvm_message)
    }

    pub fn hash(&self) -> &UInt256 {
        &self.hash
    }

    pub fn tvm_message(&self) -> &Message {
        &self.tvm_message
    }

    pub fn into_tvm_message(self) -> Message {
        self.tvm_message
    }

    pub fn dst(&self) -> &ExtMessageDst {
        &self.dst
    }
}

#[derive(Getters, Debug)]
pub struct ExternalMessagesQueue {
    messages: BTreeMap<Stamp, QueuedExtMessage>,
    last_index: u64,
}

impl ExternalMessagesQueue {
    pub(super) fn empty() -> Self {
        Self { messages: BTreeMap::new(), last_index: 0 }
    }

    pub(super) fn erase_processed(&mut self, processed: &[Stamp]) {
        let to_remove: BTreeSet<_> = processed.iter().cloned().collect();
        self.messages.retain(|stamp, _| !to_remove.contains(stamp));
    }

    pub(super) fn push_external_messages(
        &mut self,
        ext_messages: &[QueuedExtMessage],
        timestamp: DateTime<Utc>,
    ) {
        let mut cursor = self.last_index;
        for ext_message in ext_messages.iter() {
            cursor += 1;
            let stamp = Stamp { index: cursor, timestamp };
            self.messages.insert(stamp, ext_message.clone());
        }
        self.last_index = cursor;
    }

    pub(super) fn unprocessed_messages(
        &self,
    ) -> HashMap<ExtMessageDst, VecDeque<(Stamp, QueuedExtMessage)>> {
        let mut grouped_by_acc: HashMap<ExtMessageDst, VecDeque<(Stamp, QueuedExtMessage)>> =
            HashMap::new();

        for (stamp, message) in &self.messages {
            grouped_by_acc
                .entry(*message.dst())
                .or_default()
                .push_back((stamp.clone(), message.clone()));
        }

        grouped_by_acc
    }
}
