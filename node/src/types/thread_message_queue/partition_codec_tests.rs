use multi_map::MapKey;
use multi_map::MultiMapAug;
use node_types::DAppIdentifier;

use super::*;
use crate::node::services::sync::snapshot_compression::write_partition_blob;

fn message(lt: u64) -> Arc<WrappedMessage> {
    let header = tvm_block::InternalMessageHeader { created_lt: lt, ..Default::default() };
    Arc::new(WrappedMessage::from_tvm(&tvm_block::Message::with_int_header(header)))
}

fn inbox(lt: u64) -> AccountInbox {
    let mut inbox = AccountInbox::empty();
    let consumed = message(lt);
    inbox.commitment.add(&consumed.identifier());
    inbox.commitment.consume(&consumed.identifier());
    for body in [message(lt + 1), message(lt + 2)] {
        inbox.commitment.add(&body.identifier());
        inbox.add_messages(vec![(body.identifier(), body)]).unwrap();
    }
    inbox
}

fn fixture() -> ThreadMessageQueueState {
    let mut state = ThreadMessageQueueState::empty();
    for (dapp, account) in [(1, 0), (1, 1), (2, 0), (2, 1)] {
        state.set_inbox(
            AccountIdentifier::new([account; 32]).routing(DAppIdentifier::new([dapp; 32])),
            inbox(100 * dapp as u64 + 10 * account as u64),
        );
    }
    state
}

fn limits() -> MessagePartitionImportLimits {
    MessagePartitionImportLimits { max_inboxes: 10, max_messages_per_inbox: 10, max_messages: 100 }
}

fn encode(part: &InboxPartition, kind: MessageQueueKind) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    write_message_partition_checked(part, kind, &mut bytes, &|| false)?;
    Ok(bytes)
}

fn decode(
    mut bytes: &[u8],
    kind: MessageQueueKind,
    root: MapHash,
) -> anyhow::Result<ThreadMessageQueueState> {
    read_message_partition_checked(&mut bytes, kind, root, limits(), &|| false)
}

#[test]
fn message_partition_codec_roundtrip_preserves_both_kinds_and_reconstructs_original_maps(
) -> anyhow::Result<()> {
    let source = fixture();
    let parts = source.partition_by_capacity(source.capacity().bytes() / 3).unwrap();
    assert!(parts.len() > 1);
    for kind in [MessageQueueKind::Normal, MessageQueueKind::HighPriority] {
        let mut rebuilt = ThreadMessageQueueState::empty();
        let mut l2 = std::collections::BTreeMap::<_, Vec<_>>::new();
        for part in &parts {
            let raw = encode(part, kind)?;
            let compressed = write_partition_blob(Vec::new(), |writer| {
                write_message_partition_checked(part, kind, writer, &|| false)
            })?;
            assert_eq!(&compressed[..4], b"ZSTD");
            let end = snapshot_storage::chunks::verify_file(
                &mut std::io::Cursor::new(&compressed),
                &|| false,
            )?
            .offset as usize;
            assert_eq!(zstd::decode_all(&compressed[4..end])?, raw);
            let restored = decode(&raw, kind, part.root_hash)?;
            assert_eq!(restored.capacity().bytes(), part.capacity);
            let mut permutation = Vec::new();
            for (dapp, entries) in restored.messages.iter() {
                l2.entry(dapp).or_default().push(entries.messages.map.clone());
                for (routing, inbox) in entries.messages.iter() {
                    let expected = source.account_inbox_by_routing(&routing).unwrap();
                    assert_eq!(inbox.commitment, expected.commitment);
                    assert_ne!(inbox.commitment.consumed, InboxCommitment::default().consumed);
                    assert_eq!(bincode::serialize(inbox)?, bincode::serialize(expected)?);
                    assert!(rebuilt.account_inbox_by_routing(&routing).is_none());
                    rebuilt.set_inbox(routing, inbox.as_ref().clone());
                    permutation.push((routing, inbox.as_ref().clone()));
                }
            }
            let mut reversed = ThreadMessageQueueState::empty();
            for (routing, inbox) in permutation.into_iter().rev() {
                reversed.set_inbox(routing, inbox);
            }
            assert_eq!(reversed.root_hash(), restored.root_hash());
        }
        for (dapp, maps) in l2 {
            let merged = MultiMapAug::merge_disjoint_batch(&maps).unwrap();
            assert_eq!(
                MapHash(merged.root.hash()),
                source.messages.get(&dapp).unwrap().messages.root_hash()
            );
        }
        assert_eq!(rebuilt.root_hash(), source.root_hash());
        assert_eq!(rebuilt.capacity(), source.capacity());
    }
    Ok(())
}

#[test]
fn message_partition_codec_uses_archive_compatible_framing_and_ignores_cursors(
) -> anyhow::Result<()> {
    #[derive(Serialize)]
    struct InboxKey {
        kind: MessageQueueKind,
        routing: AccountRouting,
    }
    let mut state = fixture();
    let kind = MessageQueueKind::HighPriority;
    let part = state.partition_by_capacity(u64::MAX).unwrap().remove(0);
    let raw = encode(&part, kind)?;
    let mut reference = MESSAGE_PARTITION_MAGIC.to_vec();
    reference.extend_from_slice(&4u64.to_le_bytes());
    for (dapp, map) in &part.dapps {
        for (key, inbox) in map.iter() {
            let routing = AccountIdentifier::new(key.0).routing(*dapp);
            bincode::serialize_into(&mut reference, &InboxKey { kind, routing })?;
            bincode::serialize_into(&mut reference, &(inbox.tail_sequence().len() as u64))?;
            let commitment: Vec<_> =
                inbox.commitment.added.into_iter().chain(inbox.commitment.consumed).collect();
            bincode::serialize_into(&mut reference, &commitment)?;
            for (_, body) in inbox.tail_sequence() {
                bincode::serialize_into(&mut reference, body)?;
            }
        }
    }
    assert_eq!(reference, raw);
    state.advance_cursor(1);
    let dapp = DAppIdentifier::new([1; 32]);
    let mut changed = state.messages.get(&dapp).unwrap().as_ref().clone();
    changed.advance_cursor(1);
    state.messages.insert(dapp, Arc::new(changed));
    let updated = state.partition_by_capacity(u64::MAX).unwrap().remove(0);
    assert_eq!(encode(&updated, kind)?, raw);
    let restored = decode(&raw, kind, part.root_hash)?;
    assert_eq!(restored.cursor(), Some(MapKey([1; 32])));
    assert_eq!(restored.messages.get(&dapp).unwrap().cursor(), Some(MapKey([0; 32])));
    Ok(())
}

#[test]
fn message_partition_codec_rejects_corruption_truncation_mixed_kinds_and_empty_inboxes() {
    let part = fixture().partition_by_capacity(u64::MAX).unwrap().remove(0);
    let raw = encode(&part, MessageQueueKind::Normal).unwrap();
    let read = |bytes: &[u8]| decode(bytes, MessageQueueKind::Normal, part.root_hash);
    assert!(read(&raw).is_ok());
    for end in 0..raw.len() {
        assert!(read(&raw[..end]).is_err(), "truncation at {end}");
    }
    for offset in [
        0,
        7,
        15,
        15 + 68 + 8,
        15 + 68 + 8 + 8,
        15 + 68 + 8 + 8 + 32,
        15 + 68 + 8 + 8 + 64 + 8,
        raw.len() - 1,
    ] {
        let mut corrupt = raw.clone();
        corrupt[offset] ^= 0x80;
        assert!(read(&corrupt).is_err(), "corruption at {offset}");
    }
    let mut trailing = raw.clone();
    trailing.push(0);
    assert!(read(&trailing).is_err());
    assert!(decode(&raw, MessageQueueKind::HighPriority, part.root_hash).is_err());
    assert!(decode(&raw, MessageQueueKind::Normal, MapHash([42; 32])).is_err());
    // First inbox's message count is zero, despite a syntactically valid header.
    let mut zero = raw.clone();
    zero[15 + 68..15 + 68 + 8].fill(0);
    assert!(read(&zero).is_err());
    // Set the second inbox's kind to high, leaving the first normal.
    let first = part.dapps[0].1.iter().next().unwrap().1;
    let first_len = 68
        + 8
        + 8
        + 64
        + first
            .tail_sequence()
            .iter()
            .map(|(_, body)| 8 + body.canonical_size() as usize)
            .sum::<usize>();
    let mut mixed = raw.clone();
    mixed[15 + first_len..15 + first_len + 4].copy_from_slice(&1u32.to_le_bytes());
    assert!(read(&mixed).is_err());
    // Duplicate the first key into the second record.
    let mut duplicate = raw.clone();
    duplicate[15 + first_len..15 + first_len + 68].copy_from_slice(&raw[15..15 + 68]);
    assert!(read(&duplicate).is_err());
    // Preserve both bodies but reverse their FIFO order.
    let body_start = 15 + 68 + 8 + 8 + 64;
    let record_len = 8 + first.tail_sequence()[0].1.canonical_size() as usize;
    assert_eq!(record_len, 8 + first.tail_sequence()[1].1.canonical_size() as usize);
    let mut reordered = raw.clone();
    reordered[body_start..body_start + record_len]
        .copy_from_slice(&raw[body_start + record_len..body_start + 2 * record_len]);
    reordered[body_start + record_len..body_start + 2 * record_len]
        .copy_from_slice(&raw[body_start..body_start + record_len]);
    assert!(read(&reordered).is_err());
}

#[test]
fn message_partition_codec_enforces_import_budgets_and_cancellation() {
    let part = fixture().partition_by_capacity(u64::MAX).unwrap().remove(0);
    let raw = encode(&part, MessageQueueKind::Normal).unwrap();
    for limits in [
        MessagePartitionImportLimits { max_inboxes: 1, ..limits() },
        MessagePartitionImportLimits { max_messages_per_inbox: 1, ..limits() },
        MessagePartitionImportLimits { max_messages: 1, ..limits() },
    ] {
        assert!(read_message_partition_checked(
            &mut raw.as_slice(),
            MessageQueueKind::Normal,
            part.root_hash,
            limits,
            &|| false
        )
        .is_err());
    }
    assert!(read_message_partition_checked(
        &mut raw.as_slice(),
        MessageQueueKind::Normal,
        part.root_hash,
        limits(),
        &|| true
    )
    .is_err());
    assert!(write_message_partition_checked(
        &part,
        MessageQueueKind::Normal,
        &mut Vec::new(),
        &|| true
    )
    .is_err());
    let mut sink = std::io::Cursor::new(&mut [][..]);
    assert!(write_message_partition_checked(&part, MessageQueueKind::Normal, &mut sink, &|| false)
        .is_err());
    let mut empty = MESSAGE_PARTITION_MAGIC.to_vec();
    empty.extend_from_slice(&0u64.to_le_bytes());
    assert!(decode(&empty, MessageQueueKind::Normal, part.root_hash).is_err());
}

#[test]
fn message_partition_codec_rejects_unavailable_or_inconsistent_export_state() {
    let state = fixture();
    let route = AccountIdentifier::new([0; 32]).routing(DAppIdentifier::new([1; 32]));
    for mode in 0..5 {
        let mut changed = state.clone();
        let mut inbox = state.account_inbox_by_routing(&route).unwrap().clone();
        match mode {
            0 => {
                let id = inbox.tail_sequence()[0].0.clone();
                inbox.messages.set_compacted_history(Some(id.clone()..=id));
            }
            1 => {
                inbox.commitment.added[0] ^= 1;
            }
            2 => {
                inbox.add_capacity(1).unwrap();
            }
            3 => {
                let mut tail = inbox.tail_sequence().clone();
                tail[0].0 = message(999).identifier();
                inbox.messages.set_tail_sequence(tail);
            }
            _ => {
                inbox.messages.set_tail_sequence(Default::default());
            }
        }
        // Bypass queue filtering to retain an intentionally invalid empty leaf.
        let mut dapp = changed.messages.get(route.dapp_id()).unwrap().as_ref().clone();
        dapp.messages.insert(route, Arc::new(inbox));
        changed.messages.insert(*route.dapp_id(), Arc::new(dapp));
        let part = changed.partition_by_capacity(u64::MAX).unwrap().remove(0);
        assert!(encode(&part, MessageQueueKind::Normal).is_err(), "mode {mode}");
    }
    let mut part = state.partition_by_capacity(u64::MAX).unwrap().remove(0);
    part.root_hash.0[0] ^= 1;
    assert!(encode(&part, MessageQueueKind::Normal).is_err());
    part.root_hash.0[0] ^= 1;
    part.capacity += 1;
    assert!(encode(&part, MessageQueueKind::Normal).is_err());
}

#[test]
fn message_partition_codec_golden_vectors_for_both_queue_kinds() -> anyhow::Result<()> {
    let part = fixture().partition_by_capacity(u64::MAX).unwrap().remove(0);
    let mut vectors = Vec::new();
    for kind in [MessageQueueKind::Normal, MessageQueueKind::HighPriority] {
        let raw = encode(&part, kind)?;
        let compressed = write_partition_blob(Vec::new(), |writer| {
            writer.write_all(&raw)?;
            Ok(())
        })?;
        let end =
            snapshot_storage::chunks::verify_file(&mut std::io::Cursor::new(&compressed), &|| {
                false
            })?
            .offset as usize;
        vectors.push((blake3::hash(&raw).to_hex().to_string(), hex::encode(&compressed[..end])));
    }
    assert_eq!(
        vectors,
        vec![
            (
                "92c525837261e51050d328eb534d3e225a0d4b50d921c6e31df3a3d61ecd6809".to_string(),
                concat!(
                    "5a53544428b52ffd0058c50b0074134d53475350563104000100020040006502",
                    "ccec09ac5ec3c5011c9ca38c7b4ab9c14aba440bcdf79dc6c24e7d2a3fbf181a",
                    "320cf0d3995f0b73df40e807e10f34f16869c5f439e1e6f4008d1600e2933e00",
                    "b5ee9c720101010100330000610194000000000898b90d15267443149a18eff6",
                    "6d470aa8ee57a46ae006bc15b39d7dcf4db8d672c211eaded943240f332425dd",
                    "d6c6bf7d2ec34ad8c8ffccad90434a2302d61bb315bcc002009e9c42657e521c",
                    "7e6dde0e344bfe6e6d0a025ca0dd7762ba1b98c8948a8a0f222dd53f5d253433",
                    "7918a2af3a144a1c1727801cb44caba1ec45d05c8c92b7c92703240328a3d9d8",
                    "d90802b3039e0ab50a45b3cfcae14da8f1b3cb530b4c24633622b5cf064878fb",
                    "031c05ad2f71a522af4aadc0623b6bc5bf214884a6dc6e70de28ecb83f034c03",
                    "5000000000081a0001500408a41b223c0b22c4004038008a0081740031d0305c",
                    "13509c00940105520024721c8e2c0220dc26413270001ac80b601c301a181a68",
                    "04b4bb8123",
                )
                .to_string(),
            ),
            (
                "8e0051577b36c95832c7c599f930b99baf927d514abf07b8c2b7160639be38cd".to_string(),
                concat!(
                    "5a53544428b52ffd0058e50b00b4134d53475350563104000100000001000200",
                    "40006502ccec09ac5ec3c5011c9ca38c7b4ab9c14aba440bcdf79dc6c24e7d2a",
                    "3fbf181a320cf0d3995f0b73df40e807e10f34f16869c5f439e1e6f4008d1600",
                    "e2933e00b5ee9c720101010100330000610194000000000898b90d1526744314",
                    "9a18eff66d470aa8ee57a46ae006bc15b39d7dcf4db8d672c211eaded943240f",
                    "332425ddd6c6bf7d2ec34ad8c8ffccad90434a2302d61bb315bcc002009e9c42",
                    "657e521c7e6dde0e344bfe6e6d0a025ca0dd7762ba1b98c8948a8a0f222dd53f",
                    "5d2534337918a2af3a144a1c1727801cb44caba1ec45d05c8c92b7c927032403",
                    "28a3d9d8d90802b3039e0ab50a45b3cfcae14da8f1b3cb530b4c24633622b5cf",
                    "064878fb031c05ad2f71a522af4aadc0623b6bc5bf214884a6dc6e70de28ecb8",
                    "3f034c035000000000081a0001500408a41b223c0b22c4004038008a00817400",
                    "31d0305c13509c00940105520024721c8e2c0220dc26413270001ac80b601c30",
                    "1a181a6804687a0047",
                )
                .to_string(),
            ),
        ]
    );
    Ok(())
}
