use std::collections::BTreeMap;
use std::collections::HashMap;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use rand::seq::SliceRandom;
use rand::Rng;
use rand::SeedableRng;
use tvm_block::Account;
use tvm_block::Serializable;
use tvm_block::ShardAccount;

use super::super::AccountPartition;
use super::super::ArchiveStateStore;
use super::*;
fn fixture() -> Vec<(AccountRouting, ThreadAccount)> {
    (0..24u8)
        .map(|i| {
            let dapp = DAppIdentifier::new([i / 8; 32]);
            let routing = AccountRouting::new(dapp, AccountIdentifier::new([i; 32]));
            let account = if i % 7 == 0 {
                ThreadAccount::redirect(DAppIdentifier::new([99; 32]))
            } else {
                ThreadAccount::from_tvm(
                    ShardAccount::with_params(
                        &Account::default(),
                        [i; 32].into(),
                        i as u64,
                        Some(dapp.as_array().into()),
                    )
                    .unwrap(),
                )
            };
            (routing, account)
        })
        .collect()
}

fn map(accounts: &[(AccountRouting, ThreadAccount)]) -> ThreadAccountMap {
    accounts.iter().fold(ThreadAccountMap::new(), |map, (routing, account)| {
        let info = if account.is_redirect() {
            AccountInfo::Redirect(account.get_dapp_id().unwrap())
        } else {
            AccountInfo::AccountHash(account.account_hash())
        };
        map.update(&[(*routing, Some((info, AccountCapacity::new(account.canonical_size()))))])
    })
}

fn payload(accounts: &[(AccountRouting, ThreadAccount)]) -> Vec<u8> {
    let mut bytes = ACCOUNT_PARTITION_MAGIC.to_vec();
    for (routing, account) in accounts {
        write_snapshot_account_entry(&mut bytes, routing, account.canonical_bytes()).unwrap();
    }
    bytes
}

#[test]
fn account_partition_parallel_batches_preserve_root_order_and_cancellation() -> anyhow::Result<()> {
    use std::cell::Cell;

    use super::super::account_validation::VALIDATION_BATCH_RECORDS;

    let template = fixture()[1].1.clone();
    let count = VALIDATION_BATCH_RECORDS * 3 + 1;
    let accounts: Vec<_> = (0..count)
        .map(|i| {
            let mut key = [0; 32];
            key[24..].copy_from_slice(&(i as u64).to_be_bytes());
            (
                AccountRouting::new(DAppIdentifier::new([0; 32]), AccountIdentifier::new(key)),
                template.clone(),
            )
        })
        .collect();
    let expected = map(&accounts);
    let bytes = payload(&accounts);
    let mut seen = 0;
    let restored = read_account_partition_checked(
        &mut bytes.as_slice(),
        MapKeyPath::default(),
        expected.hash(),
        &|| false,
        |routing, account| {
            assert_eq!(routing, accounts[seen].0);
            assert_eq!(account.canonical_bytes(), template.canonical_bytes());
            seen += 1;
            Ok(())
        },
    )?;
    assert_eq!(seen, count);
    assert_eq!(restored.hash(), expected.hash());
    let cancelled = Cell::new(false);
    seen = 0;
    let error = read_account_partition_checked(
        &mut bytes.as_slice(),
        MapKeyPath::default(),
        expected.hash(),
        &|| cancelled.get(),
        |_, _| {
            seen += 1;
            if seen == VALIDATION_BATCH_RECORDS + 1 {
                cancelled.set(true);
            }
            Ok(())
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(seen, VALIDATION_BATCH_RECORDS + 1);
    Ok(())
}

#[test]
fn account_partition_import_normalizes_bodies_sizes_and_transfers_owned_bytes() -> anyhow::Result<()>
{
    let accounts = fixture();
    let source = map(&accounts);
    let mut wire = ACCOUNT_PARTITION_MAGIC.to_vec();
    let mut wire_size = 0u64;
    for (routing, account) in &accounts {
        let decoded = if account.is_redirect() {
            ShardAccount::with_redirect(
                [5; 32].into(),
                123,
                Some(account.get_dapp_id().unwrap().into()),
            )?
        } else {
            account.tvm_shard_account()?.clone()
        };
        let mut body = Vec::new();
        tvm_types::BocWriter::with_root(&decoded.serialize()?)?
            .write_ex(&mut body, true, true, None, None)?;
        wire_size += body.len() as u64;
        write_snapshot_account_entry(&mut wire, routing, &body)?;
    }
    assert!(wire_size > source.capacity().bytes());
    let mut seen = 0;
    let restored = read_account_partition_checked(
        &mut wire.as_slice(),
        MapKeyPath::default(),
        source.hash(),
        &|| false,
        |routing, account| {
            assert_eq!(routing, accounts[seen].0);
            assert_eq!(account.canonical_bytes(), accounts[seen].1.canonical_bytes());
            let allocation = account.canonical_bytes().as_ptr();
            let owned = account.into_canonical_bytes().into_vec();
            assert_eq!(owned.as_ptr(), allocation);
            seen += 1;
            Ok(())
        },
    )?;
    assert_eq!(seen, accounts.len());
    assert_eq!(restored.hash(), source.hash());
    assert_eq!(restored.capacity(), source.capacity());
    Ok(())
}

#[test]
fn account_partition_codec_roundtrip_reconstructs_and_merges_sparse_maps() -> anyhow::Result<()> {
    let accounts = fixture();
    let source = map(&accounts);
    let archive = ArchiveStateStore::in_memory();
    let thread = ThreadIdentifier::default();
    let block = BlockIdentifier::default();
    archive.ensure_thread(&thread, &block)?;
    archive.reset()?;
    archive.thread_init_from_entries(&thread, block, accounts.iter().cloned().map(Ok))?;
    let dir = tempfile::tempdir()?;
    let repo = DurableThreadAccountsRepository::new(dir.path().into(), archive, None)?;
    let by_key: HashMap<_, _> = accounts.iter().cloned().collect();
    let parts = source.partition_by_capacity(source.capacity().bytes() / 4).unwrap();
    assert!(parts.len() > 1);
    let mut restored = Vec::new();
    for part in parts {
        let mut output = Vec::new();
        write_account_partition_checked(
            &repo,
            &part.map,
            &mut output,
            PartitionReadLimits {
                accounts_per_batch: 2,
                in_flight_batches: 3,
                ..Default::default()
            },
            &|| false,
        )?;
        let expected: Vec<_> =
            part.map.iter().map(|(key, _)| (key, by_key[&key].clone())).collect();
        assert_eq!(output, payload(&expected));
        let mut second = Vec::new();
        write_account_partition_checked(
            &repo,
            &part.map,
            &mut second,
            Default::default(),
            &|| false,
        )?;
        assert_eq!(output, second);
        let mut imported = Vec::new();
        let decoded = read_account_partition_checked(
            &mut output.as_slice(),
            part.map.dapps.root_path,
            part.map.hash(),
            &|| false,
            |routing, account| {
                imported.push((routing, account.clone()));
                Ok(())
            },
        )?;
        assert_eq!(imported, expected);
        assert_eq!(decoded.hash(), part.map.hash());
        assert_eq!(decoded.capacity(), part.map.capacity());
        // Reconstructing from a different insertion history still yields the same root.
        imported.reverse();
        assert_eq!(map(&imported).hash(), decoded.hash());
        restored.push(AccountPartition { capacity: decoded.capacity().bytes(), map: decoded });
    }
    assert_eq!(ThreadAccountMap::merge_partitions(&restored).unwrap().hash(), source.hash());
    Ok(())
}

#[test]
fn account_partition_roundtrip_after_deleting_a_shared_prefix_neighbor() -> anyhow::Result<()> {
    let accounts = fixture();
    let fresh = map(&accounts);
    let original = accounts[1].0;
    let mut neighbor = *original.account_id().as_array();
    neighbor[31] ^= 1;
    let neighbor = AccountRouting::new(*original.dapp_id(), AccountIdentifier::new(neighbor));
    assert!(fresh.get(&neighbor).is_none());
    let source = fresh
        .update(&[(
            neighbor,
            Some((AccountInfo::AccountHash(accounts[1].1.account_hash()), AccountCapacity::new(1))),
        )])
        .update(&[(neighbor, None)]);
    let encoded = payload(&accounts);
    let decoded = read_account_partition_checked(
        &mut encoded.as_slice(),
        MapKeyPath::default(),
        source.hash(),
        &|| false,
        |_, _| Ok(()),
    )?;
    assert_eq!(decoded.hash(), fresh.hash());
    for target in [1, 1024, 4096, u64::MAX] {
        let parts = source.partition_by_capacity(target).unwrap();
        for part in &parts {
            let entries: Vec<_> =
                accounts.iter().filter(|(key, _)| part.map.get(key).is_some()).cloned().collect();
            let bytes = payload(&entries);
            let restored = read_account_partition_checked(
                &mut bytes.as_slice(),
                MapKeyPath::default(),
                part.map.hash(),
                &|| false,
                |_, _| Ok(()),
            )?;
            assert_eq!(restored.hash(), part.map.hash());
        }
        assert_eq!(ThreadAccountMap::merge_partitions(&parts).unwrap().hash(), fresh.hash());
    }
    Ok(())
}

fn long_key(dapp: u8, account: u8) -> AccountRouting {
    let mut dapp_bytes = [0xe8; 32];
    dapp_bytes[31] = dapp;
    let mut account_bytes = [0xf8; 32];
    account_bytes[31] = account;
    AccountRouting::new(DAppIdentifier::new(dapp_bytes), AccountIdentifier::new(account_bytes))
}

fn account_version(key: AccountRouting, revision: u64) -> ThreadAccount {
    if revision.is_multiple_of(11) {
        ThreadAccount::redirect(DAppIdentifier::new([99; 32]))
    } else {
        ThreadAccount::from_tvm(
            ShardAccount::with_params(
                &Account::default(),
                (*blake3::hash(&revision.to_le_bytes()).as_bytes()).into(),
                revision,
                Some(key.dapp_id().as_array().into()),
            )
            .unwrap(),
        )
    }
}

fn map_bytes(map: &ThreadAccountMap) -> Vec<u8> {
    let mut bytes = Vec::new();
    map.write(&mut bytes).unwrap();
    bytes
}

#[test]
fn account_partitions_after_stateful_updates_export_import_and_reexport_identically(
) -> anyhow::Result<()> {
    let mut saw_split_inside_dapp = false;
    let mut saw_partition_cross_dapps = false;
    for seed in 0..12 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(0xc0de_0000 + seed);
        let mut accounts = BTreeMap::new();
        for (dapp, count) in [1, 33, 2, 17, 1].into_iter().enumerate() {
            for account in 0..count {
                let key = long_key(dapp as u8, account);
                accounts.insert(key, account_version(key, u64::from(account) + 100 * dapp as u64));
            }
        }
        let initial: Vec<_> =
            accounts.iter().map(|(key, account)| (*key, account.clone())).collect();
        let mut source = map(&initial);
        let mut retained = std::collections::VecDeque::new();
        for step in 0..80 {
            retained.push_back((source.clone(), map_bytes(&source)));
            if retained.len() > 10 {
                retained.pop_front();
            }
            if step % 15 == 0 {
                // Collapse a complete L2 map and its L1 leaf while snapshots
                // of the old state remain alive; later operations refill it.
                let dapp = *long_key(rng.gen_range(0..5), 0).dapp_id();
                let keys: Vec<_> =
                    accounts.keys().filter(|key| *key.dapp_id() == dapp).copied().collect();
                source = source.update(&keys.iter().map(|key| (*key, None)).collect::<Vec<_>>());
                for key in keys {
                    accounts.remove(&key);
                }
                assert!(source.dapp_capacity(&dapp).is_none());
            } else {
                let key = long_key(rng.gen_range(0..5), rng.gen_range(0..37));
                if rng.gen_bool(0.3) {
                    accounts.remove(&key);
                    source = source.update(&[(key, None)]);
                } else {
                    let account = account_version(key, 1000 + step);
                    let info = if account.is_redirect() {
                        AccountInfo::Redirect(account.get_dapp_id().unwrap())
                    } else {
                        AccountInfo::AccountHash(account.account_hash())
                    };
                    source = source.update(&[(
                        key,
                        Some((info, AccountCapacity::new(account.canonical_size()))),
                    )]);
                    accounts.insert(key, account);
                }
            }
            let expected: Vec<_> =
                accounts.iter().map(|(key, account)| (*key, account.clone())).collect();
            assert_eq!(map_bytes(&source), map_bytes(&map(&expected)), "seed={seed}, step={step}");
            for (old, encoded) in &retained {
                assert_eq!(&map_bytes(old), encoded);
            }
        }
        let accounts: Vec<_> = accounts.into_iter().collect();
        assert!(!accounts.is_empty());
        let thread = ThreadIdentifier::default();
        let block = BlockIdentifier::default();
        let archive = ArchiveStateStore::in_memory();
        archive.ensure_thread(&thread, &block)?;
        archive.reset()?;
        archive.thread_init_from_entries(&thread, block, accounts.iter().cloned().map(Ok))?;
        let dir = tempfile::tempdir()?;
        let repo = DurableThreadAccountsRepository::new(dir.path().into(), archive, None)?;
        for target in [1, source.capacity().bytes() / 7 + 1] {
            let mut parts = source.partition_by_capacity(target).unwrap();
            saw_split_inside_dapp |= parts.windows(2).any(|pair| {
                pair[0].map.iter().last().unwrap().0.dapp_id()
                    == pair[1].map.iter().next().unwrap().0.dapp_id()
            });
            saw_partition_cross_dapps |=
                parts.iter().any(|part| part.map.dapps.iter().take(2).count() == 2);
            parts.shuffle(&mut rng);
            let imported_archive = ArchiveStateStore::in_memory();
            let staged = imported_archive.begin_staged_epoch_after_current();
            let mut restored = Vec::new();
            let mut encoded_parts = Vec::new();
            for part in parts {
                let mut encoded = Vec::new();
                write_account_partition_checked(
                    &repo,
                    &part.map,
                    &mut encoded,
                    PartitionReadLimits {
                        accounts_per_batch: 3,
                        in_flight_batches: 2,
                        ..Default::default()
                    },
                    &|| false,
                )?;
                let decoded = read_account_partition_checked(
                    &mut encoded.as_slice(),
                    source.dapps.root_path,
                    part.map.hash(),
                    &|| false,
                    |key, account| {
                        imported_archive.thread_init_from_raw_entries_at_epoch(
                            &thread,
                            block,
                            [Ok((key, account.write_bytes()?))],
                            staged.epoch,
                            false,
                        )?;
                        Ok(())
                    },
                )?;
                assert_eq!(map_bytes(&decoded), map_bytes(&part.map));
                encoded_parts.push((decoded.clone(), encoded));
                restored
                    .push(AccountPartition { capacity: decoded.capacity().bytes(), map: decoded });
            }
            let merged = ThreadAccountMap::merge_partitions(&restored).unwrap();
            assert_eq!(map_bytes(&merged), map_bytes(&source));
            for (key, _) in &accounts {
                assert!(imported_archive.read_account_operation(key)?.is_none());
            }
            imported_archive.commit_staged_epoch(staged, &thread, &block)?;
            for (key, account) in &accounts {
                assert_eq!(imported_archive.read_account_operation(key)?.unwrap(), *account);
            }
            let imported_dir = tempfile::tempdir()?;
            let imported_repo = DurableThreadAccountsRepository::new(
                imported_dir.path().into(),
                imported_archive,
                None,
            )?;
            for (part, expected) in encoded_parts {
                let mut reexported = Vec::new();
                write_account_partition_checked(
                    &imported_repo,
                    &part,
                    &mut reexported,
                    PartitionReadLimits {
                        accounts_per_batch: 7,
                        in_flight_batches: 1,
                        ..Default::default()
                    },
                    &|| false,
                )?;
                assert_eq!(reexported, expected, "byte-identical reexport, seed={seed}");
            }
        }
    }
    assert!(saw_split_inside_dapp);
    assert!(saw_partition_cross_dapps);
    Ok(())
}

#[test]
fn account_partition_codec_rejects_truncation_corruption_duplicates_and_order() {
    let accounts = fixture();
    let expected = map(&accounts).hash();
    let encoded = payload(&accounts);
    let decode = |mut bytes: &[u8]| {
        read_account_partition_checked(
            &mut bytes,
            MapKeyPath::default(),
            expected,
            &|| false,
            |_, _| Ok(()),
        )
    };
    assert!(decode(&encoded).is_ok());
    for end in 0..encoded.len() {
        assert!(decode(&encoded[..end]).is_err(), "end={end}");
    }
    let mut corrupt = encoded.clone();
    corrupt[0] ^= 1;
    assert!(decode(&corrupt).is_err());
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(decode(&trailing).is_err());
    let mut duplicate = accounts.clone();
    duplicate.insert(1, accounts[0].clone());
    assert!(decode(&payload(&duplicate)).is_err());
    let mut reversed = accounts.clone();
    reversed.reverse();
    assert!(decode(&payload(&reversed)).is_err());
    let mut wrong_body = accounts.clone();
    wrong_body[1].1 = accounts[2].1.clone();
    assert!(decode(&payload(&wrong_body)).is_err());
}

#[test]
fn account_partition_codec_enforces_cancellation_and_staged_sink_errors() {
    let accounts = fixture();
    let bytes = payload(&accounts);
    assert!(read_account_partition_checked(
        &mut bytes.as_slice(),
        MapKeyPath::default(),
        map(&accounts).hash(),
        &|| true,
        |_, _| panic!("must not emit")
    )
    .is_err());
    let error = read_account_partition_checked(
        &mut bytes.as_slice(),
        MapKeyPath::default(),
        map(&accounts).hash(),
        &|| false,
        |_, _| anyhow::bail!("staged sink failed"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("staged sink failed"));
    let empty = payload(&[]);
    assert!(read_account_partition_checked(
        &mut empty.as_slice(),
        MapKeyPath::default(),
        ThreadAccountMap::new().hash(),
        &|| false,
        |_, _| Ok(())
    )
    .is_err());
}

#[test]
fn account_partition_codec_preserves_thread_scope_and_rejects_foreign_keys() {
    let accounts = fixture();
    let scope = MapKeyPath { prefix: multi_map::MapKey([0; 32]), len: 8 };
    let source = map(&accounts);
    let (_, cropped) = source.dapps.split(scope);
    let scoped = ThreadAccountMap { dapps: cropped };
    let selected: Vec<_> =
        accounts.iter().filter(|(routing, _)| scoped.get(routing).is_some()).cloned().collect();
    assert!(!selected.is_empty());
    assert!(selected.len() < accounts.len());
    let encoded = payload(&selected);
    let restored = read_account_partition_checked(
        &mut encoded.as_slice(),
        scope,
        scoped.hash(),
        &|| false,
        |_, _| Ok(()),
    )
    .unwrap();
    assert_eq!(restored.dapps.root_path, scope);
    assert_eq!(restored.hash(), scoped.hash());
    assert!(read_account_partition_checked(
        &mut payload(&accounts).as_slice(),
        scope,
        scoped.hash(),
        &|| false,
        |_, _| Ok(()),
    )
    .is_err());
}

#[test]
fn account_partition_codec_has_a_fixed_byte_vector() {
    let account = ThreadAccount::redirect(DAppIdentifier::new([99; 32]));
    let routing =
        AccountRouting::new(DAppIdentifier::new([1; 32]), AccountIdentifier::new([2; 32]));
    let encoded = payload(&[(routing, account)]);
    assert_eq!(
        hex::encode(encoded),
        concat!(
            "4143435450563101010101010101010101010101010101010101010101010101",
            "0101010101010102020202020202020202020202020202020202020202020202",
            "020202020202025600000000000000b5ee9c7201010101004b00009100000000",
            "0000000000000000000000000000000000000000000000000000000000000000",
            "0000000058d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8",
            "d8d8d8d8e0",
        )
    );
}
