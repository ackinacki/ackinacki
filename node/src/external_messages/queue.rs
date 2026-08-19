// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use http_server::NotQueuedExtMessage;
use inbound_external_messages::InboundMessage;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;
use tvm_block::GetRepresentationHash;
use tvm_block::Message;
use tvm_types::UInt256;

#[derive(Clone, Debug)]
enum Status {
    Pending,  // Just queued and not processed yet external messages
    Included, // External messages already included in block in undergoing validation
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq, Hash)]
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

impl InboundMessage for QueuedExtMessage {
    type Account = AccountIdentifier;
    type DApp = DAppIdentifier;
    type Destination = ExtMessageDst;

    fn destination(&self) -> Self::Destination {
        *self.dst()
    }

    fn dapp_id(destination: &Self::Destination) -> Self::DApp {
        destination.dapp_id.unwrap_or_else(|| destination.account_id.redirect_dapp_id())
    }

    fn account_id(destination: &Self::Destination) -> Self::Account {
        destination.account_id
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

    fn hash_of(dst: &ExtMessageDst) -> u64 {
        let mut hasher = DefaultHasher::new();
        dst.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn eq_and_hash_include_dapp_id() {
        let account = AccountIdentifier::from_str(&"ab".repeat(32)).unwrap();
        let dapp1 = DAppIdentifier::from_str(&"11".repeat(32)).unwrap();
        let dapp2 = DAppIdentifier::from_str(&"22".repeat(32)).unwrap();

        let none = ExtMessageDst::new(account, None);
        let some1 = ExtMessageDst::new(account, Some(dapp1));
        let some2 = ExtMessageDst::new(account, Some(dapp2));

        assert_ne!(none, some1);
        assert_ne!(some1, some2);

        assert_ne!(hash_of(&none), hash_of(&some1));
        assert_ne!(hash_of(&some1), hash_of(&some2));
    }

    #[test]
    fn hashmap_keeps_same_account_in_different_dapps_separate() {
        let account = AccountIdentifier::from_str(&"cd".repeat(32)).unwrap();
        let dapp = DAppIdentifier::from_str(&"33".repeat(32)).unwrap();

        let mut map: HashMap<ExtMessageDst, u32> = HashMap::new();
        *map.entry(ExtMessageDst::new(account, Some(dapp))).or_default() += 1;
        *map.entry(ExtMessageDst::new(account, None)).or_default() += 1;

        assert_eq!(map.len(), 2);
        assert_eq!(map.values().sum::<u32>(), 2);
    }
}
