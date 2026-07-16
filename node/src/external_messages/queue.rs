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

#[derive(Default, Clone, Copy, Debug)]
pub struct ExtMessageDst {
    pub account_id: AccountIdentifier,
    pub dapp_id: Option<DAppIdentifier>,
}

// Note: this fix is necessary for current state impl, (ACC_ID, None) and (ACC_ID, Some(DAPP)) are
// the same destinations and can't be processed in parallel but in state v2 DAPP will be mandatory
// and this impl should be removed.
//
// `Hash` must stay consistent with `PartialEq` (equal values must hash equally), so it ignores
// `dapp_id` as well. Otherwise HashMap/HashSet keyed by `ExtMessageDst` would split equal
// destinations across buckets and treat them as distinct.
impl PartialEq for ExtMessageDst {
    fn eq(&self, other: &Self) -> bool {
        self.account_id == other.account_id
    }
}

impl Eq for ExtMessageDst {}

impl std::hash::Hash for ExtMessageDst {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.account_id, state);
    }
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
        Self::try_new(Status::Pending, Some(ext_message.dapp_id()), ext_message.into_tvm_message())
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

    #[cfg(test)]
    pub(crate) fn new_for_test(dst: ExtMessageDst) -> Self {
        Self {
            _status: Status::Pending,
            tvm_message: Message::default(),
            hash: UInt256::default(),
            dst,
        }
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

    pub(super) fn erase_processed(
        &mut self,
        processed: &[Stamp],
    ) -> Vec<(Stamp, QueuedExtMessage)> {
        let to_remove: BTreeSet<_> = processed.iter().cloned().collect();
        let removed = self
            .messages
            .iter()
            .filter(|(stamp, _)| to_remove.contains(*stamp))
            .map(|(stamp, message)| (stamp.clone(), message.clone()))
            .collect();
        self.messages.retain(|stamp, _| !to_remove.contains(stamp));
        removed
    }

    pub(super) fn restore_processed(&mut self, processed: &[(Stamp, QueuedExtMessage)]) {
        for (stamp, message) in processed {
            self.messages.entry(stamp.clone()).or_insert_with(|| message.clone());
        }
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

    pub(super) fn drain_all(&mut self) -> BTreeMap<Stamp, QueuedExtMessage> {
        std::mem::take(&mut self.messages)
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

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::collections::HashMap;
    use std::hash::Hash;
    use std::hash::Hasher;
    use std::str::FromStr;

    use node_types::AccountIdentifier;
    use node_types::DAppIdentifier;

    use super::ExtMessageDst;
    use super::ExternalMessagesQueue;
    use super::QueuedExtMessage;
    use crate::external_messages::Stamp;

    fn hash_of(dst: &ExtMessageDst) -> u64 {
        let mut hasher = DefaultHasher::new();
        dst.hash(&mut hasher);
        hasher.finish()
    }

    // (ACC, None), (ACC, Some(D1)) and (ACC, Some(D2)) are the same destination, so they must be
    // equal AND hash equally. A derived `Hash` would include `dapp_id` and break this contract,
    // splitting equal destinations across HashMap buckets.
    #[test]
    fn eq_and_hash_ignore_dapp_id() {
        let account = AccountIdentifier::from_str(&"ab".repeat(32)).unwrap();
        let dapp1 = DAppIdentifier::from_str(&"11".repeat(32)).unwrap();
        let dapp2 = DAppIdentifier::from_str(&"22".repeat(32)).unwrap();

        let none = ExtMessageDst::new(account, None);
        let some1 = ExtMessageDst::new(account, Some(dapp1));
        let some2 = ExtMessageDst::new(account, Some(dapp2));

        assert_eq!(none, some1);
        assert_eq!(some1, some2);

        assert_eq!(hash_of(&none), hash_of(&some1));
        assert_eq!(hash_of(&some1), hash_of(&some2));
    }

    #[test]
    fn hashmap_groups_same_account_across_dapps() {
        let account = AccountIdentifier::from_str(&"cd".repeat(32)).unwrap();
        let dapp = DAppIdentifier::from_str(&"33".repeat(32)).unwrap();

        let mut map: HashMap<ExtMessageDst, u32> = HashMap::new();
        *map.entry(ExtMessageDst::new(account, Some(dapp))).or_default() += 1;
        *map.entry(ExtMessageDst::new(account, None)).or_default() += 1;

        assert_eq!(map.len(), 1);
        assert_eq!(map.values().next(), Some(&2));
    }

    #[test]
    fn erase_processed_can_be_restored_for_production_restart() {
        let account = AccountIdentifier::from_str(&"ef".repeat(32)).unwrap();
        let dst = ExtMessageDst::new(account, None);
        let msg = QueuedExtMessage::new_for_test(dst);
        let timestamp = chrono::Utc::now();
        let stamp = Stamp { index: 1, timestamp };

        let mut queue = ExternalMessagesQueue::empty();
        queue.messages.insert(stamp.clone(), msg);

        let removed = queue.erase_processed(std::slice::from_ref(&stamp));
        assert!(queue.messages.is_empty());
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].0, stamp);

        queue.restore_processed(&removed);
        assert_eq!(queue.messages.len(), 1);
        assert!(queue.messages.contains_key(&stamp));
    }
}
