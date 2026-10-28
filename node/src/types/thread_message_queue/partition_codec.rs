//! MSGSPV1 logical inbox records. No Merkle nodes, cursors or storage pointers.
use std::collections::VecDeque;
use std::io::Read;
use std::io::Write;
use std::sync::Arc;

use multi_map::MapHash;
use multi_map::MapKeyPath;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use serde::Deserialize;
use serde::Serialize;

use super::partition::inbox_partition_root;
use super::partition::InboxPartition;
use super::DAppMessageQueueState;
use super::ThreadMessageQueueState;
use crate::message::WrappedMessage;
use crate::types::account_inbox::InboxCommitment;
use crate::types::AccountInbox;

pub const MESSAGE_PARTITION_MAGIC: &[u8; 7] = b"MSGSPV1";

/// Same enum order and bincode key framing as the message-archive branch.
/// Bincode encodes this enum's discriminant as u32, NOT its one-byte blob-ID tag.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageQueueKind {
    Normal,
    HighPriority,
}

/// Local allocation/work budgets, not consensus partition-capacity parameters.
#[derive(Clone, Copy, Debug)]
pub struct MessagePartitionImportLimits {
    pub max_inboxes: u64,
    pub max_messages_per_inbox: u64,
    pub max_messages: u64,
}

/// Stream a complete inline partition. The caller discards partial output on
/// errors. No message DB is consulted, and archive-only bodies are rejected.
pub fn write_message_partition_checked<W, C>(
    partition: &InboxPartition,
    kind: MessageQueueKind,
    writer: &mut W,
    should_cancel: &C,
) -> anyhow::Result<()>
where
    W: Write + ?Sized,
    C: Fn() -> bool,
{
    let mut count = 0u64;
    let mut previous_dapp = None;
    for (dapp, map) in &partition.dapps {
        anyhow::ensure!(
            previous_dapp.is_none_or(|previous| previous < *dapp),
            "message partition DApps must be strictly ordered"
        );
        anyhow::ensure!(
            !map.root.is_empty() && map.root_path == MapKeyPath::default(),
            "invalid message partition DApp map"
        );
        for (_, inbox) in map.iter() {
            check_cancel(should_cancel)?;
            anyhow::ensure!(
                inbox.compacted_history().is_none(),
                "message partition requires inline bodies; archive history is unsupported"
            );
            anyhow::ensure!(!inbox.is_empty(), "empty inbox in message partition");
            count = count.checked_add(1).ok_or_else(|| anyhow::anyhow!("inbox count overflow"))?;
        }
        previous_dapp = Some(*dapp);
    }
    anyhow::ensure!(count > 0, "empty message partition");
    anyhow::ensure!(
        inbox_partition_root(&partition.dapps) == partition.root_hash,
        "message partition root mismatch"
    );
    check_cancel(should_cancel)?;
    writer.write_all(MESSAGE_PARTITION_MAGIC)?;
    writer.write_all(&count.to_le_bytes())?;
    let mut total = 0u64;
    for (dapp, map) in &partition.dapps {
        for (key, inbox) in map.iter() {
            check_cancel(should_cancel)?;
            let routing = AccountIdentifier::new(key.0).routing(*dapp);
            // A tuple is byte-compatible with InboxKey { kind, routing }.
            bincode::serialize_into(&mut *writer, &(kind, routing))?;
            bincode::serialize_into(&mut *writer, &(inbox.tail_sequence().len() as u64))?;
            writer.write_all(&64u64.to_le_bytes())?;
            writer.write_all(&inbox.commitment.added)?;
            writer.write_all(&inbox.commitment.consumed)?;
            let mut bytes = 0u64;
            inbox.commitment.verify(inbox.tail_sequence().iter().map(|(id, message)| {
                check_cancel(should_cancel)?;
                let actual = internal_identifier(message)?;
                anyhow::ensure!(*id == actual, "message partition message key mismatch");
                bytes = bytes
                    .checked_add(message.canonical_size())
                    .ok_or_else(|| anyhow::anyhow!("inbox capacity overflow"))?;
                writer.write_all(&message.canonical_size().to_le_bytes())?;
                writer.write_all(message.canonical_bytes())?;
                Ok(actual)
            }))?;
            anyhow::ensure!(bytes == inbox.canonical_size(), "inbox canonical size mismatch");
            total = total
                .checked_add(bytes)
                .ok_or_else(|| anyhow::anyhow!("partition capacity overflow"))?;
        }
    }
    anyhow::ensure!(total == partition.capacity, "message partition capacity mismatch");
    Ok(())
}

/// Decode a complete, EOF-delimited uncompressed blob. No imported state is
/// returned until all inbox chains and the manifest partition root are verified.
/// Both local cursor levels start at the first canonical key, not an exporter cursor.
pub fn read_message_partition_checked<R, C>(
    reader: &mut R,
    kind: MessageQueueKind,
    expected_root: MapHash,
    limits: MessagePartitionImportLimits,
    should_cancel: &C,
) -> anyhow::Result<ThreadMessageQueueState>
where
    R: Read + ?Sized,
    C: Fn() -> bool,
{
    check_cancel(should_cancel)?;
    let mut magic = [0; 7];
    reader.read_exact(&mut magic)?;
    anyhow::ensure!(&magic == MESSAGE_PARTITION_MAGIC, "invalid message partition marker");
    let count = read_u64(reader)?;
    anyhow::ensure!(
        count > 0 && count <= limits.max_inboxes,
        "message partition inbox count budget exceeded"
    );
    let mut result = ThreadMessageQueueState::empty();
    let mut dapp = DAppMessageQueueState::empty();
    let mut previous: Option<AccountRouting> = None;
    let mut total_messages = 0u64;
    let mut total_bytes = 0u64;
    for _ in 0..count {
        check_cancel(should_cancel)?;
        let (actual_kind, routing): (MessageQueueKind, AccountRouting) =
            bincode::deserialize_from(&mut *reader)?;
        anyhow::ensure!(actual_kind == kind, "message partition queue kind mismatch");
        anyhow::ensure!(
            previous.is_none_or(|previous| previous < routing),
            "message partition inboxes must be strictly ordered and unique"
        );
        let message_count = read_u64(reader)?;
        anyhow::ensure!(
            message_count > 0 && message_count <= limits.max_messages_per_inbox,
            "message partition per-inbox count budget exceeded"
        );
        total_messages = total_messages
            .checked_add(message_count)
            .ok_or_else(|| anyhow::anyhow!("message count overflow"))?;
        anyhow::ensure!(
            total_messages <= limits.max_messages,
            "message partition count budget exceeded"
        );
        anyhow::ensure!(read_u64(reader)? == 64, "invalid inbox commitment length");
        let mut commitment = InboxCommitment::default();
        reader.read_exact(&mut commitment.added)?;
        reader.read_exact(&mut commitment.consumed)?;
        let mut inbox = AccountInbox::empty();
        inbox.commitment = commitment;
        let start_bytes = total_bytes;
        let mut tail = VecDeque::new();
        for _ in 0..message_count {
            check_cancel(should_cancel)?;
            let length = read_u64(reader)?;
            total_bytes = total_bytes
                .checked_add(length)
                .ok_or_else(|| anyhow::anyhow!("message capacity overflow"))?;
            // No untrusted count/length is used as a preallocation hint.
            let mut bytes = Vec::new();
            (&mut *reader).take(length).read_to_end(&mut bytes)?;
            anyhow::ensure!(bytes.len() as u64 == length, "truncated message body");
            let message =
                Arc::new(WrappedMessage::validate_canonical_bytes(bytes.into_boxed_slice())?);
            let id = internal_identifier(&message)?;
            tail.push_back((id, message));
        }
        inbox.add_capacity(total_bytes - start_bytes)?;
        inbox.messages.set_tail_sequence(tail);
        commitment.verify(inbox.tail_sequence().iter().map(|(id, _)| Ok(id.clone())))?;
        if let Some(previous) = previous {
            if previous.dapp_id() != routing.dapp_id() {
                result.messages.insert(*previous.dapp_id(), Arc::new(dapp));
                dapp = DAppMessageQueueState::empty();
            }
        }
        dapp.insert_inbox(routing, inbox);
        previous = Some(routing);
    }
    result.messages.insert(*previous.expect("nonempty partition").dapp_id(), Arc::new(dapp));
    result.normalize_cursor();
    let mut trailing = [0];
    anyhow::ensure!(reader.read(&mut trailing)? == 0, "trailing message partition bytes");
    check_cancel(should_cancel)?;
    anyhow::ensure!(result.root_hash() == expected_root, "message partition root mismatch");
    Ok(result)
}

fn internal_identifier(
    message: &WrappedMessage,
) -> anyhow::Result<crate::message::identifier::MessageIdentifier> {
    let tvm = message.tvm_message().map_err(|error| anyhow::anyhow!("{error}"))?;
    anyhow::ensure!(
        tvm.int_header().is_some() && tvm.int_dst_account_id().is_some(),
        "message partition contains a non-internal message"
    );
    Ok(message.identifier())
}

fn read_u64<R: Read + ?Sized>(reader: &mut R) -> anyhow::Result<u64> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn check_cancel<C: Fn() -> bool>(should_cancel: &C) -> anyhow::Result<()> {
    anyhow::ensure!(!should_cancel(), "message partition cancelled");
    Ok(())
}

#[cfg(test)]
#[path = "partition_codec_tests.rs"]
mod tests;
