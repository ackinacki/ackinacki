use std::sync::Arc;

use account_state::AccountCapacity;
use account_state::AccountInfo;
use node_types::AccountHash;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;

use super::*;
use crate::message::WrappedMessage;
use crate::types::AccountInbox;

fn manifest() -> ThreadSnapshotManifest {
    ThreadSnapshotManifest {
        account_partition_roots: vec![[1; 32], [2; 32]],
        normal_message_partition_roots: vec![[3; 32]],
        high_priority_message_partition_roots: vec![[3; 32], [4; 32]],
    }
}

#[test]
fn manifest_hash_uses_logical_lists_with_big_endian_boundaries() {
    let manifest = manifest();
    let mut input = b"ACKI_NACKI_THREAD_SNAPSHOT_MANIFEST_V1".to_vec();
    input.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 2]);
    input.extend_from_slice(&[1; 32]);
    input.extend_from_slice(&[2; 32]);
    input.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]);
    input.extend_from_slice(&[3; 32]);
    input.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 2]);
    input.extend_from_slice(&[3; 32]);
    input.extend_from_slice(&[4; 32]);
    assert_eq!(snapshot_manifest_hash(&manifest), *blake3::hash(&input).as_bytes());
    assert_ne!(
        snapshot_manifest_hash(&manifest),
        *blake3::hash(&bincode::serialize(&manifest).unwrap()).as_bytes()
    );
    // Exercise a count above 255 as well as the short protocol vector.
    let large = ThreadSnapshotManifest {
        account_partition_roots: vec![[7; 32]; 256],
        ..Default::default()
    };
    let mut input = MANIFEST_HASH_DOMAIN.to_vec();
    input.extend_from_slice(&256u64.to_be_bytes());
    for _ in 0..256 {
        input.extend_from_slice(&[7; 32]);
    }
    input.extend_from_slice(&[0; 16]); // Empty normal and high lists.
    assert_eq!(snapshot_manifest_hash(&large), *blake3::hash(&input).as_bytes());
}

#[test]
fn manifest_hash_distinguishes_type_boundaries_order_and_membership() {
    let root = [11; 32];
    let accounts =
        ThreadSnapshotManifest { account_partition_roots: vec![root], ..Default::default() };
    let normal =
        ThreadSnapshotManifest { normal_message_partition_roots: vec![root], ..Default::default() };
    let high = ThreadSnapshotManifest {
        high_priority_message_partition_roots: vec![root],
        ..Default::default()
    };
    assert_ne!(snapshot_manifest_hash(&accounts), snapshot_manifest_hash(&normal));
    assert_ne!(snapshot_manifest_hash(&normal), snapshot_manifest_hash(&high));
    assert_ne!(snapshot_manifest_hash(&accounts), snapshot_manifest_hash(&high));
    let original = manifest();
    let mut reversed = original.clone();
    reversed.account_partition_roots.reverse();
    assert_ne!(snapshot_manifest_hash(&original), snapshot_manifest_hash(&reversed));
    let mut duplicated = original.clone();
    duplicated.normal_message_partition_roots.push([3; 32]);
    assert_ne!(snapshot_manifest_hash(&original), snapshot_manifest_hash(&duplicated));
    let mut changed = original.clone();
    changed.high_priority_message_partition_roots[1][0] ^= 1;
    assert_ne!(snapshot_manifest_hash(&original), snapshot_manifest_hash(&changed));
    // The same root in both message lists is legitimate, not a hash collision.
    let both = ThreadSnapshotManifest {
        normal_message_partition_roots: vec![root],
        high_priority_message_partition_roots: vec![root],
        ..Default::default()
    };
    let joined = ThreadSnapshotManifest {
        normal_message_partition_roots: vec![root, root],
        ..Default::default()
    };
    assert_ne!(snapshot_manifest_hash(&both), snapshot_manifest_hash(&joined));
}

#[test]
fn partition_blob_ids_bind_marker_and_one_byte_priority_not_manifest_or_block() {
    let root = [23; 32];
    let account = account_partition_blob_id(&root);
    let normal = message_partition_blob_id(MessageQueueKind::Normal, &root);
    let high = message_partition_blob_id(MessageQueueKind::HighPriority, &root);
    assert_ne!(account, normal);
    assert_ne!(normal, high);
    assert_ne!(account, high);
    let mut input = b"ACCTPV1".to_vec();
    input.extend_from_slice(&root);
    assert_eq!(account, *blake3::hash(&input).as_bytes());
    for (kind, tag) in [(MessageQueueKind::Normal, 0), (MessageQueueKind::HighPriority, 1)] {
        let mut input = b"MSGSPV1".to_vec();
        input.push(tag);
        input.extend_from_slice(&root);
        assert_eq!(message_partition_blob_id(kind, &root), *blake3::hash(&input).as_bytes());
        let mut wrong = b"MSGSPV1".to_vec();
        wrong.extend_from_slice(&bincode::serialize(&kind).unwrap());
        wrong.extend_from_slice(&root);
        assert_ne!(message_partition_blob_id(kind, &root), *blake3::hash(&wrong).as_bytes());
        assert_ne!(
            message_partition_blob_id(kind, &root),
            message_partition_blob_id(kind, &[24; 32])
        );
    }
    let mut old =
        ThreadSnapshotManifest { account_partition_roots: vec![root], ..Default::default() };
    let old_hash = snapshot_manifest_hash(&old);
    old.normal_message_partition_roots.push([42; 32]);
    assert_ne!(old_hash, snapshot_manifest_hash(&old));
    assert_eq!(account_partition_blob_id(&old.account_partition_roots[0]), account);
}

fn metadata_maps(reverse: bool) -> (ThreadAccountMap, ThreadMessageQueueState) {
    let mut accounts = ThreadAccountMap::new();
    let mut queue = ThreadMessageQueueState::empty();
    let order: Vec<_> = if reverse { (0..24u8).rev().collect() } else { (0..24u8).collect() };
    for i in order {
        let routing = AccountIdentifier::new([i; 32]).routing(DAppIdentifier::new([i / 8; 32]));
        accounts = accounts.update(&[(
            routing,
            Some((
                AccountInfo::AccountHash(AccountHash::new([i; 32])),
                AccountCapacity::new(100 + i as u64),
            )),
        )]);
        let header =
            tvm_block::InternalMessageHeader { created_lt: i as u64, ..Default::default() };
        let valid = WrappedMessage::from_tvm(&tvm_block::Message::with_int_header(header));
        let mut inbox = AccountInbox::empty();
        inbox.commitment.add(&valid.identifier());
        // Intentionally undecodable resident body: planning must never ask for
        // its TVM view. Only its already stored size and commitment are relevant.
        let body = Arc::new(WrappedMessage::from_canonical_bytes(
            vec![0xb5, 0xee, 0x9c, 0x72].into_boxed_slice(),
        ));
        inbox.add_messages(vec![(valid.identifier(), body)]).unwrap();
        queue.set_inbox(routing, inbox);
    }
    (accounts, queue)
}

#[test]
fn manifest_builds_from_cached_maps_without_reading_bodies_and_ignores_update_history(
) -> anyhow::Result<()> {
    let (accounts, queue) = metadata_maps(false);
    let (rebuilt_accounts, rebuilt_queue) = metadata_maps(true);
    let expected = build_snapshot_manifest(&accounts, &queue, &queue, 16, 100)?;
    let rebuilt =
        build_snapshot_manifest(&rebuilt_accounts, &rebuilt_queue, &rebuilt_queue, 16, 100)?;
    assert_eq!(expected, rebuilt);
    assert_eq!(snapshot_manifest_hash(&expected), snapshot_manifest_hash(&rebuilt));
    assert!(expected.account_partition_roots.len() > 1);
    assert!(expected.normal_message_partition_roots.len() > 1);
    assert_eq!(
        expected.normal_message_partition_roots,
        expected.high_priority_message_partition_roots
    );
    assert_eq!(
        expected.account_partition_roots,
        accounts
            .partition_by_capacity(16)
            .unwrap()
            .iter()
            .map(|part| *part.map.hash().as_array())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        expected.normal_message_partition_roots,
        queue
            .partition_by_capacity(16)
            .unwrap()
            .iter()
            .map(|part| part.root_hash.0)
            .collect::<Vec<_>>()
    );
    assert!(build_snapshot_manifest(&accounts, &queue, &queue, 0, 100).is_err());
    assert!(build_snapshot_manifest(&accounts, &queue, &queue, 16, 1).is_err());
    // Check each message list's limit independently, not only the account list.
    assert!(build_snapshot_manifest(
        &ThreadAccountMap::new(),
        &queue,
        &ThreadMessageQueueState::empty(),
        16,
        1
    )
    .is_err());
    assert!(build_snapshot_manifest(
        &ThreadAccountMap::new(),
        &ThreadMessageQueueState::empty(),
        &queue,
        16,
        1
    )
    .is_err());
    let empty = build_snapshot_manifest(
        &ThreadAccountMap::new(),
        &ThreadMessageQueueState::empty(),
        &ThreadMessageQueueState::empty(),
        16,
        0,
    )?;
    assert_eq!(empty, ThreadSnapshotManifest::default());
    Ok(())
}

#[test]
fn manifest_and_blob_id_fixed_vectors() {
    let actual = [
        snapshot_manifest_hash(&ThreadSnapshotManifest::default()),
        snapshot_manifest_hash(&manifest()),
        account_partition_blob_id(&[0; 32]),
        message_partition_blob_id(MessageQueueKind::Normal, &[0; 32]),
        message_partition_blob_id(MessageQueueKind::HighPriority, &[0; 32]),
    ]
    .map(hex::encode);
    assert_eq!(
        actual,
        [
            "150cbb441a4e53ca1c883e6c43979c8b0d96689459dcffd1b52bdceb31fff048",
            "6de0318d8796933601e05d145018d2dffeeeb046bebda967eab528547d10942b",
            "2478d4a336946db0bae258915bfc49cd2068bc94b5368a02eed5b24654b4fed2",
            "6cce20946b316b4a5412af00412c1a99b558a38974a16563885ce7bd5b419207",
            "a14c3f507a528ec65132f1c48f3e362afc912a17de143b0cbb7ec4bc92cae364",
        ]
    );
}

#[test]
fn protocol_root_limit_caps_even_unlimited_local_budget() -> anyhow::Result<()> {
    check_root_limit(MAX_SNAPSHOT_ROOTS_PER_LIST as usize, u64::MAX)?;
    assert!(check_root_limit(MAX_SNAPSHOT_ROOTS_PER_LIST as usize + 1, u64::MAX).is_err());
    assert!(check_root_limit(101, 100).is_err());
    for kind in 0..3 {
        let mut manifest = ThreadSnapshotManifest::default();
        let roots = match kind {
            0 => &mut manifest.account_partition_roots,
            1 => &mut manifest.normal_message_partition_roots,
            _ => &mut manifest.high_priority_message_partition_roots,
        };
        roots.resize(MAX_SNAPSHOT_ROOTS_PER_LIST as usize + 1, [0; 32]);
        assert!(manifest.validate_root_limits(u64::MAX).is_err());
    }
    Ok(())
}
