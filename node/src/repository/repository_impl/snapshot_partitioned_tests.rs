use account_state::AccountCapacity;
use account_state::AccountInfo;
use account_state::DurableThreadAccountsState;
use account_state::ThreadAccountMap;
use node_types::AccountIdentifier;
use node_types::AccountRouting;

use super::*;
use crate::node::associated_types::AttestationTargetType;
use crate::node::block_state::attestation_target_checkpoints::AttestationTargetCheckpoint;
use crate::node::services::sync::snapshot_compression::write_partition_blob;
use crate::types::thread_message_queue::ThreadMessageQueueState;

fn fixture() -> (ThreadSnapshot, OptimisticStateImpl, ThreadSnapshotManifest) {
    let snapshot = super::super::tests::make_test_snapshot();
    let (mut state, _) = super::super::inbox_snapshot_tests::fixture();
    state.block_id = snapshot.finalized_block.data().identifier();
    state.block_seq_no = snapshot.finalized_block.data().seq_no();
    state.thread_id = *snapshot.finalized_block.data().common_section().thread_id();
    let manifest = ThreadSnapshotManifest {
        account_partition_roots: vec![[1; 32], [2; 32]],
        normal_message_partition_roots: vec![state.messages.root_hash().0],
        high_priority_message_partition_roots: vec![state.high_priority_messages.root_hash().0],
    };
    (snapshot, state, manifest)
}

fn encode(
    snapshot: &ThreadSnapshot,
    state: &OptimisticStateImpl,
    manifest: &ThreadSnapshotManifest,
) -> anyhow::Result<Vec<u8>> {
    let mut raw = Vec::new();
    write_primary_snapshot_checked(snapshot, state, manifest, &mut raw, &|| false)?;
    Ok(raw)
}

fn decode(raw: &[u8]) -> anyhow::Result<PrimaryThreadSnapshot> {
    read_primary_snapshot_checked(&mut &raw[..], 100, &|| false)
}

#[test]
fn primary_root_limit_is_per_list_and_cannot_be_relaxed_by_the_caller() -> anyhow::Result<()> {
    let (snapshot, state, _) = fixture();
    let manifest = ThreadSnapshotManifest {
        account_partition_roots: vec![[1; 32]; MAX_SNAPSHOT_ROOTS_PER_LIST as usize],
        normal_message_partition_roots: vec![[2; 32]; MAX_SNAPSHOT_ROOTS_PER_LIST as usize],
        high_priority_message_partition_roots: vec![[3; 32]; MAX_SNAPSHOT_ROOTS_PER_LIST as usize],
    };
    let raw = encode(&snapshot, &state, &manifest)?;
    let parsed = read_primary_snapshot_checked(&mut raw.as_slice(), u64::MAX, &|| false)?;
    assert_eq!(parsed.manifest, manifest);
    for kind in 0..3 {
        let mut invalid = manifest.clone();
        match kind {
            0 => &mut invalid.account_partition_roots,
            1 => &mut invalid.normal_message_partition_roots,
            _ => &mut invalid.high_priority_message_partition_roots,
        }
        .push([4; 32]);
        let mut output = Vec::new();
        assert!(write_primary_snapshot_checked(&snapshot, &state, &invalid, &mut output, &|| {
            false
        })
        .is_err());
        assert!(output.is_empty());
        // Bypass the checked writer to model an untrusted network primary.
        let mut raw = MAGIC.to_vec();
        bincode::serialize_into(&mut raw, &invalid)?;
        bincode::serialize_into(&mut raw, &parsed.metadata)?;
        let error =
            read_primary_snapshot_checked(&mut raw.as_slice(), u64::MAX, &|| false).err().unwrap();
        assert!(error.to_string().contains("root budget exceeded"));
    }
    Ok(())
}

#[test]
fn primary_snapshot_roundtrip_retains_metadata_without_partition_collections() -> anyhow::Result<()>
{
    let (mut snapshot, mut state, manifest) = fixture();
    let raw = encode(&snapshot, &state, &manifest)?;
    assert!(raw.starts_with(b"SNAP2V2"));
    let primary = decode(&raw)?;
    assert_eq!(primary.manifest, manifest);
    assert_eq!(
        super::super::snapshot_manifest::snapshot_manifest_hash(&primary.manifest),
        super::super::snapshot_manifest::snapshot_manifest_hash(&manifest),
    );
    assert_eq!(primary.metadata.state.block_id, state.block_id);
    assert_eq!(primary.metadata.state.block_seq_no, state.block_seq_no);
    assert_eq!(primary.metadata.state.thread_id, state.thread_id);
    assert_eq!(
        primary.metadata.finalized_block.data().identifier(),
        snapshot.finalized_block.data().identifier()
    );
    assert_eq!(primary.metadata.bk_set, snapshot.bk_set);
    assert_eq!(primary.metadata.descendant_bk_set, snapshot.descendant_bk_set);
    assert_eq!(primary.metadata.finalized_block_stats, snapshot.finalized_block_stats);
    assert_eq!(primary.metadata.producer_selector, snapshot.producer_selector);
    assert!(primary.metadata.state.shard_state.0.durable.map().is_empty());
    // No old optimistic bytes, durable bodies, queues or compacted-message
    // section reach this payload, regardless of their size or contents.
    snapshot.optimistic_state = vec![42; 100_000];
    snapshot.durable_state_snapshot = Some(vec![43; 100_000]);
    snapshot.db_messages = vec![vec![Arc::new(WrappedMessage::from_tvm(
        &tvm_block::Message::with_int_header(Default::default()),
    ))]];
    state.messages = ThreadMessageQueueState::empty();
    state.high_priority_messages = ThreadMessageQueueState::empty();
    let map = ThreadAccountMap::new().update(&[(
        AccountRouting::default(),
        Some((
            AccountInfo::Redirect(node_types::DAppIdentifier::new([17; 32])),
            AccountCapacity::new(86),
        )),
    )]);
    state.shard_state.0 =
        state.shard_state.0.with_durable_state(DurableThreadAccountsState::with_map(map));
    assert_eq!(encode(&snapshot, &state, &manifest)?, raw);
    // Every retained metadata field also survives exact canonical reencoding.
    let mut reencoded = MAGIC.to_vec();
    bincode::serialize_into(&mut reencoded, &primary.manifest)?;
    bincode::serialize_into(&mut reencoded, &primary.metadata)?;
    assert_eq!(reencoded, raw);
    Ok(())
}

fn checkpoint() -> AttestationTargetCheckpoint {
    AttestationTargetCheckpoint::builder()
        .current_distance(1)
        .deadline(10)
        .required_attestation_count(2)
        .attestation_target_type(AttestationTargetType::Primary)
        .build()
}

#[test]
fn primary_snapshot_sorts_all_unordered_metadata_maps() -> anyhow::Result<()> {
    let (mut snapshot, mut state, manifest) = fixture();
    let mut expected = None;
    for reverse in [false, true, false] {
        let order: Vec<_> = if reverse { (1..=16u8).rev().collect() } else { (1..=16u8).collect() };
        let mut bk = BlockKeeperSet::new();
        let mut primary = HashMap::new();
        let mut fallback = HashMap::new();
        let mut refs = HashMap::new();
        let mut accounts = HashMap::new();
        let mut messages = HashMap::new();
        for i in order {
            // Deliberately do not derive the map key from keeper.signer_index.
            let keeper = BlockKeeperData {
                owner_address: AccountIdentifier::new([i; 32]),
                ..Default::default()
            };
            bk.insert(i as u16, keeper);
            let id = BlockIdentifier::new([i; 32]);
            primary.insert(id, checkpoint());
            fallback.insert(id, vec![checkpoint()]);
            let thread = ThreadIdentifier::new(&id, i as u16);
            refs.insert(thread, crate::multithreading::cross_thread_messaging::thread_references_state::ReferencedBlock {
                block_thread_identifier: thread, block_identifier: id, block_seq_no: BlockSeqNo::from(i as u32).into(),
            });
            let route = AccountIdentifier::new([i; 32]).redirect();
            accounts.insert(route, (None, None));
            messages.insert(route, vec![]);
        }
        state.thread_refs_state = ThreadReferencesState::builder().all_thread_refs(refs).build();
        snapshot.bk_set = bk.clone();
        snapshot.descendant_bk_set = bk.clone();
        snapshot.future_bk_set = bk.clone();
        snapshot.descendant_future_bk_set = bk.clone();
        snapshot.ancestor_blocks_finalization_checkpoints =
            AncestorBlocksFinalizationCheckpoints::builder()
                .primary(primary)
                .fallback(fallback)
                .build();
        snapshot.parent_ancestor_blocks_finalization_checkpoints =
            snapshot.ancestor_blocks_finalization_checkpoints.clone();
        snapshot.ancestor_blocks_data = vec![AncestorBlockData {
            block_identifier: state.block_id,
            block_seq_no: state.block_seq_no,
            thread_identifier: state.thread_id,
            cross_thread_ref_data: CrossThreadRefData::builder()
                .block_identifier(state.block_id)
                .block_seq_no(state.block_seq_no)
                .block_thread_identifier(state.thread_id)
                .outbound_accounts(accounts)
                .outbound_messages(messages)
                .threads_table(state.threads_table.clone())
                .parent_block_identifier(BlockIdentifier::default())
                .block_refs(vec![])
                .build(),
            bk_set: bk.clone(),
            future_bk_set: bk,
            envelope_hash: AckiNackiEnvelopeHash([0; 32]),
            parent_block_identifier: BlockIdentifier::default(),
            block_version_state: BlockProtocolVersionState::none(),
        }];
        let raw = encode(&snapshot, &state, &manifest)?;
        assert_eq!(decode(&raw)?.metadata.bk_set, snapshot.bk_set);
        if reverse {
            let sorted = bincode::serialize(&SortedBk(&snapshot.bk_set))?;
            let offset = raw.windows(sorted.len()).position(|bytes| bytes == sorted).unwrap();
            let entry_len = (sorted.len() - 8) / 16;
            assert_eq!(8 + 16 * entry_len, sorted.len());
            let first = offset + 8;
            let mut unsorted = raw.clone();
            unsorted[first..first + entry_len]
                .copy_from_slice(&raw[first + entry_len..first + 2 * entry_len]);
            unsorted[first + entry_len..first + 2 * entry_len]
                .copy_from_slice(&raw[first..first + entry_len]);
            assert!(decode(&unsorted).err().unwrap().to_string().contains("non-canonical"));
        }
        let compressed = write_partition_blob(Vec::new(), |writer| {
            write_primary_snapshot_checked(&snapshot, &state, &manifest, writer, &|| false)
        })?;
        if let Some((expected_raw, expected_compressed)) = &expected {
            assert_eq!(&raw, expected_raw);
            assert_eq!(&compressed, expected_compressed);
        }
        expected = Some((raw, compressed));
    }
    Ok(())
}

#[test]
fn primary_snapshot_rejects_wrong_boundaries_budgets_truncation_and_trailing_data(
) -> anyhow::Result<()> {
    let (snapshot, mut state, manifest) = fixture();
    let raw = encode(&snapshot, &state, &manifest)?;
    assert!(read_primary_snapshot_checked(&mut raw.as_slice(), 1, &|| false).is_err());
    assert!(read_primary_snapshot_checked(&mut raw.as_slice(), 100, &|| true).is_err());
    let mut trailing = raw.clone();
    trailing.push(0);
    assert!(decode(&trailing).is_err());
    for end in (0..raw.len()).step_by(71) {
        assert!(decode(&raw[..end]).is_err(), "end={end}");
    }
    let mut corrupt = raw.clone();
    corrupt[0] ^= 1;
    assert!(decode(&corrupt).is_err());
    for marker in [b"SNAP2V1", b"SNAP2V3", b"SNAP2V4"] {
        let mut obsolete = raw.clone();
        obsolete[..MAGIC.len()].copy_from_slice(marker);
        assert!(decode(&obsolete).is_err());
    }
    let mut huge = raw.clone();
    huge[7..15].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(decode(&huge).is_err());
    state.block_id = BlockIdentifier::default();
    assert!(encode(&snapshot, &state, &manifest).is_err());
    let mut sink = Vec::new();
    assert!(
        write_primary_snapshot_checked(&snapshot, &state, &manifest, &mut sink, &|| true).is_err()
    );
    assert!(sink.is_empty());
    let mut sink = std::io::Cursor::new(&mut [][..]);
    let (_, valid_state, _) = fixture();
    assert!(write_primary_snapshot_checked(&snapshot, &valid_state, &manifest, &mut sink, &|| {
        false
    })
    .is_err());
    assert!(
        super::super::read_thread_snapshot_from_payload(&raw, SnapshotCompression::Plain).is_err()
    );
    Ok(())
}

#[test]
fn primary_snapshot_nested_decoders_reject_truncated_blocks_and_message_ids_without_panics(
) -> anyhow::Result<()> {
    let (snapshot, _, _) = fixture();
    let serialized = bincode::serialize(snapshot.finalized_block.data())?;
    let block_bytes: Vec<u8> = bincode::deserialize(&serialized)?;
    for end in (0..block_bytes.len()).step_by(17) {
        let corrupt = bincode::serialize(&block_bytes[..end])?;
        assert!(bincode::deserialize::<AckiNackiBlock>(&corrupt).is_err(), "end={end}");
    }
    let mut corrupt = block_bytes;
    corrupt[..8].copy_from_slice(&u64::MAX.to_be_bytes());
    assert!(bincode::deserialize::<AckiNackiBlock>(&bincode::serialize(&corrupt)?).is_err());
    let invalid_id = bincode::serialize(&vec![0u8; 4])?;
    assert!(bincode::deserialize::<MessageIdentifier>(&invalid_id).is_err());
    Ok(())
}

#[test]
fn primary_snapshot_fixed_plain_and_compressed_vectors() -> anyhow::Result<()> {
    let (snapshot, state, manifest) = fixture();
    let raw = encode(&snapshot, &state, &manifest)?;
    let compressed = write_partition_blob(Vec::new(), |writer| {
        write_primary_snapshot_checked(&snapshot, &state, &manifest, writer, &|| false)
    })?;
    let end =
        snapshot_storage::chunks::verify_file(&mut std::io::Cursor::new(&compressed), &|| false)?
            .offset as usize;
    assert_eq!(zstd::decode_all(&compressed[4..end])?, raw);
    assert_eq!(
        (blake3::hash(&raw).to_hex().to_string(), hex::encode(&compressed[..end])),
        (
            "1b9de04f1be82b301cb3717be2fa26a6cb16b1f7404c75cd8850110685b9b402".to_string(),
            concat!(
                "5a53544428b52ffd0058751d00242e534e4150325632020001020100e6a63f88",
                "5e59d293ebad7b0232c2c0d8733e45723ac0cf22da98cf4f0adda93a429eec0f",
                "9ebb463cbbc8228498a0b742964b0b9e37b189e55b59cb34335d256f20006b06",
                "f669a2075e93c616465a142d5f2380d91987c5aa60c52ed1d6620eba09e76000",
                "00b5ee9c7201010401005500035b9023afe20020030201002300080001400011",
                "001059000101004e00009801222201222220c0051a28e8a8a98ecc06b51c0a95",
                "2f021e02a1f56944a25df953e074b887ddad2202c85e2f99de8d6416ea167b27",
                "9207b108126184924a57042816e4abef95fc5925333bc652c1d3a48e1ac2a300",
                "204126b052ded4c2e9d7ecf8fc3b6d5f2a8ff5129d6004e5ca49b572cbe37ab8",
                "622d28b5933e7afde0d1a357d06cf2fd52ca45feac6a9a5d512b48088d863e3f",
                "79a165085c631397b19af2a8b48cc52684b30e4203fb7f197f1814428d9789dd",
                "d4b05b8f303b57c52d0fdc5f4ab01c60d325f321030181200009000800400038",
                "3161366265613132386635653033383433333632653535666435373463343261",
                "3865343537646435353334393863626338656337653134393636643230613300",
                "08002207000158020a0100014c00041011ef55aa0805030103894a33f6fd4007",
                "02020001020a8a0496a296d224f285c67bee93c30f8a309157f0daa35dc5b87e",
                "410b78630a09cfc700000000040400000209b8e48dfb04070600050000080003",
                "002001869bc7a987698320d0000954d15b1336aa82728aae6c48b4adbc4d7b26",
                "a5dba75cd2e2e56209e7d4c16aa60100600695ad325dfc7e1191fbc9f186f58e",
                "ff42a634029731b18380ff89bf42c464a42cb8ca55b200f051f57f1e1893c687",
                "5910ea7912ef7a227c01298a7c7a96b1851b23021741c71938f39638b1d368aa",
                "a621452426b5d8199773a2cb5b2743a5da010474657374010201000102012000",
                "c009090920010000001b93c89281acaffd6b2c31fdd488968f10b6cf5ec97161",
                "8793de88b077a7cd6e00004e6f6e6500c065a871553532c1042929a358c30160",
                "831426491b12a070585ca082288a0021254002222088086302a7041248552005",
                "26c72601909ae26612acbfe520e3d562ac911f698295ae1e0e54a5546b5a2ca2",
                "696d3110d8b8c8d0f80583735308c31805d3d0804f09f6e314d08a4e01b45e28",
                "e1dd6d4cfdfb86864e627c408149e33f2fabd242cba064a74c31bbca1204920d",
                "23e02e4321b87a8973a47957d52909142b4f043e8fc9cbf13ee074239e06bf88",
                "46661512905dcbb5169c0e5ee6a21222471a9d8455e44d42380067",
            )
            .to_string(),
        ),
    );
    Ok(())
}
