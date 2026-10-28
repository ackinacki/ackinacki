use std::ops::Deref;
use std::sync::Arc;

use account_inbox::iter::iterator::MessagesRangeIterator;
use account_inbox::range::MessagesRange;
use serde::Deserialize;
use serde::Serialize;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::storage::MessageDurableStorage;

pub type MessageChainHash = [u8; 32];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxCommitment {
    pub added: MessageChainHash,
    pub consumed: MessageChainHash,
}

impl Default for InboxCommitment {
    fn default() -> Self {
        let seed = *blake3::hash(b"acki-nacki-inbox-message-chain-v1").as_bytes();
        Self { added: seed, consumed: seed }
    }
}

impl InboxCommitment {
    fn step(previous: MessageChainHash, message: &MessageIdentifier) -> MessageChainHash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"acki-nacki-inbox-message-step-v1");
        hasher.update(&previous);
        hasher.update(message.inner().hash.as_slice());
        *hasher.finalize().as_bytes()
    }

    pub fn add(&mut self, message: &MessageIdentifier) {
        self.added = Self::step(self.added, message);
    }

    pub fn consume(&mut self, message: &MessageIdentifier) {
        self.consumed = Self::step(self.consumed, message);
    }

    pub fn verify(
        &self,
        messages: impl IntoIterator<Item = anyhow::Result<MessageIdentifier>>,
    ) -> anyhow::Result<()> {
        let mut current = self.consumed;
        let mut empty = true;
        for id in messages {
            empty = false;
            current = Self::step(current, &id?);
        }
        anyhow::ensure!(current == self.added, "Inbox contents do not match the commitment");
        if empty {
            anyhow::ensure!(*self == Self::default(), "Empty inbox commitment is not reset");
        } else {
            anyhow::ensure!(self.added != self.consumed, "Nonempty inbox has an empty commitment");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountInbox {
    pub messages: MessagesRange<MessageIdentifier, Arc<WrappedMessage>>,
    // Valid only at a completed block boundary. Mutating messages during
    // preprocessing/execution does not update or reset these chains.
    pub commitment: InboxCommitment,
    // Derived canonical body bytes, independent of in-memory/DB body placement.
    canonical_size: u64,
}

impl node_types::Blake3Hashable for AccountInbox {
    fn update_hasher(&self, hasher: &mut blake3::Hasher) {
        hasher.update(&self.commitment.added);
        hasher.update(&self.commitment.consumed);
    }
}

impl multi_map::MultiMapValue for AccountInbox {
    fn write_value<W: std::io::Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        bincode::serialize_into(writer, self).map_err(Into::into)
    }

    fn read_value<R: std::io::Read>(reader: &mut R) -> anyhow::Result<Self> {
        bincode::deserialize_from(reader).map_err(Into::into)
    }
}

impl AccountInbox {
    pub fn empty() -> Self {
        Self {
            messages: MessagesRange::empty(),
            commitment: InboxCommitment::default(),
            canonical_size: 0,
        }
    }

    pub fn canonical_size(&self) -> u64 {
        self.canonical_size
    }

    pub(crate) fn add_capacity(&mut self, bytes: u64) -> anyhow::Result<()> {
        self.canonical_size = self
            .canonical_size
            .checked_add(bytes)
            .ok_or_else(|| anyhow::anyhow!("Inbox canonical size overflow"))?;
        Ok(())
    }

    pub(crate) fn remove_capacity(&mut self, bytes: u64) -> anyhow::Result<()> {
        self.canonical_size = self
            .canonical_size
            .checked_sub(bytes)
            .ok_or_else(|| anyhow::anyhow!("Inbox canonical size underflow"))?;
        Ok(())
    }

    pub fn add_messages(
        &mut self,
        messages: Vec<(MessageIdentifier, Arc<WrappedMessage>)>,
    ) -> anyhow::Result<()> {
        let bytes = messages.iter().try_fold(0u64, |sum, (_, message)| {
            sum.checked_add(message.canonical_size())
                .ok_or_else(|| anyhow::anyhow!("Inbox canonical size overflow"))
        })?;
        self.add_capacity(bytes)?;
        self.messages.add_messages(messages);
        Ok(())
    }

    // Legacy/bootstrap input has no chain history: start a new anchored chain.
    pub fn rebase(&mut self, db: &MessageDurableStorage) -> anyhow::Result<()> {
        let mut commitment = InboxCommitment::default();
        let mut canonical_size = 0u64;
        for message in MessagesRangeIterator::new(db, self.messages.clone()) {
            let (body, id) =
                message.map_err(|error| anyhow::anyhow!("Reading inbox: {error:?}"))?;
            commitment.add(&id);
            canonical_size = canonical_size
                .checked_add(body.canonical_size())
                .ok_or_else(|| anyhow::anyhow!("Inbox canonical size overflow"))?;
        }
        self.commitment = commitment;
        self.canonical_size = canonical_size;
        Ok(())
    }
}

impl Deref for AccountInbox {
    type Target = MessagesRange<MessageIdentifier, Arc<WrappedMessage>>;

    fn deref(&self) -> &Self::Target {
        &self.messages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(lt: u64) -> MessageIdentifier {
        let header = tvm_block::InternalMessageHeader { created_lt: lt, ..Default::default() };
        WrappedMessage::from_tvm(&tvm_block::Message::with_int_header(header)).identifier()
    }

    #[test]
    fn rolling_chains_prove_the_visible_fifo_without_block_history() {
        let [a, b, c] = [id(1), id(2), id(3)];
        let mut commitment = InboxCommitment::default();
        for message in [&a, &b, &c] {
            commitment.add(message);
        }
        commitment.consume(&a);
        commitment.verify([Ok(b.clone()), Ok(c.clone())]).unwrap();
        for messages in
            [vec![c.clone(), b.clone()], vec![b.clone()], vec![b.clone(), b.clone(), c.clone()]]
        {
            assert!(commitment.verify(messages.into_iter().map(Ok)).is_err());
        }
        assert!(commitment.verify([Ok(id(4)), Ok(c.clone())]).is_err());
        let mut corrupted = commitment;
        corrupted.added[0] ^= 1;
        assert!(corrupted.verify([Ok(b.clone()), Ok(c.clone())]).is_err());
        corrupted = commitment;
        corrupted.consumed[0] ^= 1;
        assert!(corrupted.verify([Ok(b), Ok(c)]).is_err());
    }

    #[test]
    fn operation_interleaving_does_not_change_the_two_ordered_chains() {
        let [a, b] = [id(1), id(2)];
        let mut first = InboxCommitment::default();
        first.add(&a);
        first.consume(&a); // Transiently empty within the block: no reset.
        first.add(&b);
        let mut second = InboxCommitment::default();
        second.add(&a);
        second.add(&b);
        second.consume(&a);
        assert_eq!(first, second);
        first.verify([Ok(b)]).unwrap();
        assert_ne!(first.consumed, InboxCommitment::default().consumed);
    }

    #[test]
    fn serde_preserves_the_exact_chain_anchor_not_a_rebased_content_hash() {
        let mut inbox = AccountInbox::empty();
        let header = tvm_block::InternalMessageHeader { created_lt: 2, ..Default::default() };
        let message =
            Arc::new(WrappedMessage::from_tvm(&tvm_block::Message::with_int_header(header)));
        inbox.commitment.add(&id(1));
        inbox.commitment.add(&message.identifier());
        inbox.commitment.consume(&id(1));
        inbox.add_messages(vec![(message.identifier(), message.clone())]).unwrap();
        let bytes = bincode::serialize(&inbox).unwrap();
        let restored: AccountInbox = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored.commitment, inbox.commitment);
        restored.commitment.verify([Ok(message.identifier())]).unwrap();
        let mut rebased = restored;
        rebased.rebase(&MessageDurableStorage::mem()).unwrap();
        assert_ne!(rebased.commitment, inbox.commitment);
        assert_eq!(rebased.canonical_size(), message.canonical_size());
    }

    #[test]
    fn capacity_arithmetic_is_checked_and_does_not_change_on_error() {
        let mut inbox = AccountInbox::empty();
        assert!(inbox.remove_capacity(1).is_err());
        assert_eq!(inbox.canonical_size(), 0);
        inbox.add_capacity(u64::MAX).unwrap();
        assert!(inbox.add_capacity(1).is_err());
        assert_eq!(inbox.canonical_size(), u64::MAX);
        inbox.remove_capacity(u64::MAX).unwrap();
        assert_eq!(inbox.canonical_size(), 0);
    }
}
