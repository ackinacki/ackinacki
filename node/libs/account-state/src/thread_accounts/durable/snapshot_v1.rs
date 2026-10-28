//! Compatibility adapter for the SNAP2V1 durable segment (unaugmented, u8 paths).
#[cfg(test)]
use std::collections::HashMap;
use std::io::Read;
#[cfg(test)]
use std::io::Write;
use std::time::Instant;

use anyhow::ensure;
#[cfg(test)]
use multi_map::MapKey;
#[cfg(test)]
use multi_map::MultiMapAug;
#[cfg(test)]
use multi_map::MultiMapValue;
#[cfg(test)]
use node_types::AccountIdentifier;
use node_types::AccountRouting;
#[cfg(test)]
use node_types::DAppIdentifier;

use super::account_validation::validate_account;
use super::account_validation::with_validated_accounts;
#[cfg(test)]
use super::account_validation::VALIDATION_BATCH_RECORDS;
#[cfg(test)]
use super::maps::L1MapValue;
#[cfg(test)]
use super::AccountCapacity;
#[cfg(test)]
use super::AccountInfo;
use super::ThreadAccountMap;
#[cfg(test)]
use crate::ThreadAccount;

#[cfg(test)]
#[path = "snapshot_v1_hash_audit.rs"]
mod hash_audit;

/// Reference encoder for legacy fixtures: supplied hashes, no conversion or augmentation.
#[cfg(test)]
pub(super) fn write_account_map<W: Write, F: Fn() -> bool>(
    map: &ThreadAccountMap,
    writer: &mut W,
    cancelled: &F,
) -> anyhow::Result<()> {
    fn tree<V: MultiMapValue, W: Write, F: Fn() -> bool>(
        map: &MultiMapAug<V, AccountCapacity>,
        writer: &mut W,
        cancelled: &F,
        value: &impl Fn(&V, &mut W) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(map.root_path.len == 0, "Legacy export requires a full map");
        writer.write_all(&[0; 33])?;
        node(&map.root, writer, cancelled, value)
    }
    fn node<V: MultiMapValue, W: Write, F: Fn() -> bool>(
        current: &multi_map::node::NodeAug<V, AccountCapacity>,
        writer: &mut W,
        cancelled: &F,
        value: &impl Fn(&V, &mut W) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        use multi_map::node::NodeAug;
        anyhow::ensure!(!cancelled(), "snapshot save cancelled");
        match current {
            NodeAug::Empty => writer.write_all(&[0])?,
            NodeAug::Leaf(leaf) => {
                writer.write_all(&[1])?;
                writer.write_all(&leaf.hash)?;
                value(&leaf.value, writer)?;
            }
            NodeAug::Branch(branch) => {
                writer.write_all(&[2])?;
                writer.write_all(&branch.hash)?;
                writer.write_all(&branch.bitmap.to_le_bytes())?;
                for child in &branch.children {
                    node(child, writer, cancelled, value)?;
                }
            }
            NodeAug::Ext(ext) => {
                writer.write_all(&[3])?;
                writer.write_all(&ext.hash)?;
                writer.write_all(&[ext.nibble_count])?;
                writer.write_all(&ext.nibbles[..usize::from(ext.nibble_count)])?;
                node(&ext.child, writer, cancelled, value)?;
            }
        }
        Ok(())
    }
    tree(&map.dapps, writer, cancelled, &|dapp, writer| {
        tree(&dapp.0, writer, cancelled, &|info, writer| info.write_value(writer))
    })
}

fn read_array<const N: usize>(reader: &mut impl Read) -> anyhow::Result<[u8; N]> {
    let mut bytes = [0; N];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

#[cfg(test)]
fn read_map<R: Read>(
    reader: &mut R,
    leaf: &mut impl FnMut(&mut R, MapKey) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let len = read_array::<1>(reader)?[0];
    let prefix = read_array::<32>(reader)?;
    ensure!(len == 0 && prefix == [0; 32], "Legacy snapshot map must cover the full key space");
    read_node(reader, 0, MapKey([0; 32]), leaf)?;
    Ok(())
}

// Retained only for legacy-format fixtures and forensic experiments, not runtime import.
// Return whether this node is nonempty, for branch bitmap / extension checks.
#[cfg(test)]
fn read_node<R: Read>(
    reader: &mut R,
    depth: usize,
    mut key: MapKey,
    leaf: &mut impl FnMut(&mut R, MapKey) -> anyhow::Result<()>,
) -> anyhow::Result<bool> {
    let tag = read_array::<1>(reader)?[0];
    if tag == 0 {
        return Ok(false);
    }
    let _stored_hash = read_array::<32>(reader)?;
    match tag {
        1 => {
            ensure!(depth == 64, "Legacy leaf does not have a complete key");
            leaf(reader, key)?;
        }
        2 => {
            ensure!(depth < 64, "Legacy branch exceeds key depth");
            let bitmap = u16::from_le_bytes(read_array(reader)?);
            ensure!(bitmap.count_ones() >= 2, "Noncanonical legacy branch");
            for index in 0..16 {
                let mut child_key = key;
                set_nibble(&mut child_key, depth, index as u8);
                let nonempty = read_node(reader, depth + 1, child_key, leaf)?;
                ensure!(nonempty == (bitmap & (1 << index) != 0), "Legacy branch bitmap mismatch");
            }
        }
        3 => {
            let count = usize::from(read_array::<1>(reader)?[0]);
            ensure!(count > 0 && depth + count <= 64, "Invalid legacy extension length");
            let mut nibbles = [0; 64];
            reader.read_exact(&mut nibbles[..count])?;
            ensure!(nibbles[..count].iter().all(|n| *n < 16), "Invalid legacy nibble");
            for (offset, nibble) in nibbles[..count].iter().enumerate() {
                set_nibble(&mut key, depth + offset, *nibble);
            }
            // The legacy writer can preserve adjacent extensions.
            ensure!(read_node(reader, depth + count, key, leaf)?, "Invalid legacy extension child");
        }
        _ => anyhow::bail!("Invalid legacy map node tag: {tag}"),
    }
    Ok(true)
}

#[cfg(test)]
fn set_nibble(key: &mut MapKey, depth: usize, nibble: u8) {
    let byte = &mut key.0[depth / 2];
    if depth.is_multiple_of(2) {
        *byte = (*byte & 0x0f) | (nibble << 4);
    } else {
        *byte = (*byte & 0xf0) | nibble;
    }
}

#[cfg(test)]
fn read_legacy_account_infos(
    reader: &mut impl Read,
) -> anyhow::Result<HashMap<AccountRouting, AccountInfo>> {
    let maps_len = u64::from_le_bytes(read_array(reader)?);
    let mut maps_reader = (&mut *reader).take(maps_len);
    let mut expected = HashMap::new();
    read_map(&mut maps_reader, &mut |reader, dapp| {
        read_map(reader, &mut |reader, account| {
            let routing =
                AccountRouting::new(DAppIdentifier::new(dapp.0), AccountIdentifier::new(account.0));
            let info = AccountInfo::read_value(reader)?;
            ensure!(expected.insert(routing, info).is_none(), "Duplicate legacy routing");
            Ok(())
        })
    })?;
    ensure!(maps_reader.limit() == 0, "Trailing bytes in legacy account map");
    Ok(expected)
}

/// Validate bodies in bounded parallel batches before yielding them to the sink,
/// incrementally inserting their metadata into the current map. The sink must consume the
/// iterator completely. Batches are yielded in completion order; writes remain provisional until
/// validation completes. The obsolete serialized map is skipped, not checked against bodies.
pub(super) fn read_snapshot_v1_durable<R: Read>(
    reader: &mut R,
    consume: impl FnOnce(
        &mut dyn Iterator<Item = anyhow::Result<(AccountRouting, Vec<u8>)>>,
    ) -> anyhow::Result<()>,
) -> anyhow::Result<ThreadAccountMap> {
    let started = Instant::now();
    // SNAP2V1's stored tree is not authenticated by the requested block. Checking bodies against
    // it only detects internal disagreement, not a coherent substitution by a snapshot supplier.
    // Use bodies as the sole source of leaves; keep only framing checks for the obsolete segment.
    // Streaming to a sink avoids allocating either the old tree or an expected-routing HashMap.
    let maps_len = u64::from_le_bytes(read_array(reader)?);
    ensure!(
        std::io::copy(&mut (&mut *reader).take(maps_len), &mut std::io::sink())? == maps_len,
        "Truncated legacy account map segment"
    );
    let count = u64::from_le_bytes(read_array(reader)?);

    let workers = super::repository::snapshot_written_hash_worker_count();
    let window = workers.saturating_add(super::repository::snapshot_written_hash_queue_batches());
    tracing::info!(accounts = count, workers, window, "Legacy snapshot account validation started");
    // Avoid retaining all leaf updates and the scratch allocations of a final bulk rebuild.
    // This same map also detects duplicates across and within independently completed batches.
    let mut map = ThreadAccountMap::new();
    let mut remaining = count;
    let mut delivered = 0u64;
    let mut raw = std::iter::from_fn(|| {
        if remaining == 0 {
            return None;
        }
        remaining -= 1;
        Some(super::repository::read_snapshot_account_entry(reader))
    });
    with_validated_accounts(&mut raw, false, validate_account, |validated| {
        let mut entries = std::iter::from_fn(|| {
            validated.next().map(|account| {
                let (routing, bytes, info, capacity) = account?;
                ensure!(map.get(&routing).is_none(), "Duplicate legacy account body: {routing}");
                map = map.update_single(&routing, Some((info, capacity)));
                delivered += 1;
                if delivered.is_multiple_of(10_000) || delivered == count {
                    tracing::info!(
                        imported = delivered,
                        total = count,
                        elapsed_secs = started.elapsed().as_secs(),
                        "Legacy snapshot accounts verified"
                    );
                }
                Ok((routing, bytes.into_vec()))
            })
        });
        consume(&mut entries)
    })?;
    ensure!(delivered == count, "Legacy account sink did not consume all bodies");
    ensure!(reader.read(&mut [0])? == 0, "Trailing bytes in legacy durable segment");
    tracing::info!(
        accounts = count,
        incremental = true,
        elapsed_secs = started.elapsed().as_secs(),
        "Legacy snapshot account map rebuilt"
    );
    Ok(map)
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use multi_map::node::Node;
    use multi_map::MapKeyPath;
    use multi_map::MultiMap;
    use node_types::AccountHash;
    use node_types::Blake3Hashable;

    use super::*;

    #[derive(Clone)]
    struct DappMap(MultiMap<AccountInfo>);

    impl Blake3Hashable for DappMap {
        fn update_hasher(&self, hasher: &mut blake3::Hasher) {
            hasher.update(&self.0.root.hash());
        }
    }

    impl MultiMapValue for DappMap {
        fn write_value<W: Write>(&self, _writer: &mut W) -> anyhow::Result<()> {
            unreachable!("Test fixtures use the independent legacy encoder")
        }

        fn read_value<R: Read>(_reader: &mut R) -> anyhow::Result<Self> {
            unreachable!("Legacy import visits leaves without building a tree")
        }
    }

    fn read_account_leaves(bytes: &[u8]) -> anyhow::Result<Vec<(MapKey, AccountInfo)>> {
        let mut leaves = Vec::new();
        read_map(&mut &*bytes, &mut |reader, key| {
            leaves.push((key, AccountInfo::read_value(reader)?));
            Ok(())
        })?;
        Ok(leaves)
    }

    // Independent SNAP2V1 encoding: one-byte root-path length, no augmentation,
    // stored hashes, and all 16 branch slots (including empty ones).
    fn write_old_map<V: MultiMapValue>(
        map: &MultiMap<V>,
        writer: &mut Vec<u8>,
        value: &impl Fn(&V, &mut Vec<u8>),
    ) {
        fn node<V: MultiMapValue>(
            current: &Node<V>,
            writer: &mut Vec<u8>,
            value: &impl Fn(&V, &mut Vec<u8>),
        ) {
            match current {
                Node::Empty => writer.push(0),
                Node::Leaf(leaf) => {
                    writer.push(1);
                    writer.extend(leaf.hash);
                    value(&leaf.value, writer);
                }
                Node::Branch(branch) => {
                    writer.push(2);
                    writer.extend(branch.hash);
                    writer.extend(branch.bitmap.to_le_bytes());
                    for child in &branch.children {
                        node(child, writer, value);
                    }
                }
                Node::Ext(ext) => {
                    writer.push(3);
                    writer.extend(ext.hash);
                    writer.push(ext.nibble_count);
                    writer.extend(&ext.nibbles[..usize::from(ext.nibble_count)]);
                    node(&ext.child, writer, value);
                }
            }
        }
        writer.push(0);
        writer.extend([0; 32]);
        node(&map.root, writer, value);
    }

    fn fixture() -> (Vec<u8>, Vec<(AccountRouting, ThreadAccount)>) {
        let dapp = DAppIdentifier::new([7; 32]);
        let route = AccountIdentifier::new([42; 32]).routing(dapp);
        let tvm = tvm_block::Account::with_address(
            tvm_block::MsgAddressInt::with_standart(
                None,
                0,
                tvm_types::AccountId::from_raw(route.account_id().as_slice().to_vec(), 256),
            )
            .unwrap(),
        );
        let account = ThreadAccount::from(
            tvm_block::ShardAccount::with_params(
                &tvm,
                [8; 32].into(),
                19,
                Some(dapp.as_array().into()),
            )
            .unwrap(),
        );
        let redirect = ThreadAccount::redirect(DAppIdentifier::new([9; 32]));
        let routes =
            vec![(route, account), (AccountIdentifier::new([43; 32]).routing(dapp), redirect)];
        (encode_accounts(&routes), routes)
    }

    fn encode_accounts(routes: &[(AccountRouting, ThreadAccount)]) -> Vec<u8> {
        let dapp = routes[0].0.dapp_id();
        let mut l2 = MultiMap::new();
        for (route, account) in routes {
            let info = if account.is_redirect() {
                AccountInfo::Redirect(account.get_dapp_id().unwrap())
            } else {
                AccountInfo::AccountHash(AccountHash::from(
                    account.tvm_shard_account().unwrap().account_cell().unwrap().repr_hash(),
                ))
            };
            l2 = l2.update_single(&MapKey(*route.account_id().as_array()), Some(info));
        }
        let l1 = (MultiMap::<DappMap>::new())
            .update_single(&MapKey(*dapp.as_array()), Some(DappMap(l2)))
            // Historical snapshots may retain DApps with no account leaves.
            .update_single(&MapKey([8; 32]), Some(DappMap(MultiMap::new())));
        let mut maps = Vec::new();
        write_old_map(&l1, &mut maps, &|dapp, w| {
            write_old_map(&dapp.0, w, &|info, w| info.write_value(w).unwrap())
        });
        let mut bytes = Vec::new();
        bytes.extend((maps.len() as u64).to_le_bytes());
        bytes.extend(maps);
        bytes.extend((routes.len() as u64).to_le_bytes());
        for (route, account) in routes {
            super::super::repository::write_snapshot_account_entry(
                &mut bytes,
                route,
                account.canonical_bytes(),
            )
            .unwrap();
        }
        bytes
    }

    #[test]
    fn parallel_legacy_validation_preserves_contents_and_root_across_batches() -> anyhow::Result<()>
    {
        let (_, fixture_accounts) = fixture();
        let dapp = *fixture_accounts[0].0.dapp_id();
        // More than one validation window; alternate normal accounts and redirects.
        let accounts: Vec<_> = (0..VALIDATION_BATCH_RECORDS * 17 + 1)
            .map(|index| {
                let mut key = [0; 32];
                key[..8].copy_from_slice(&(index as u64).to_le_bytes());
                (AccountIdentifier::new(key).routing(dapp), fixture_accounts[index % 2].1.clone())
            })
            .rev()
            .collect();
        let bytes = encode_accounts(&accounts);
        let mut unseen: HashMap<_, _> = accounts.iter().map(|(key, value)| (*key, value)).collect();
        let map = read_snapshot_v1_durable(&mut bytes.as_slice(), |entries| {
            for entry in entries {
                let (routing, body) = entry?;
                let account = unseen.remove(&routing).expect("Unexpected or duplicate output");
                assert_eq!(body, account.canonical_bytes());
            }
            Ok(())
        })?;
        assert!(unseen.is_empty());
        let expected = ThreadAccountMap::new().update(
            &accounts
                .iter()
                .map(|(routing, account)| {
                    let info = if account.is_redirect() {
                        AccountInfo::Redirect(account.get_dapp_id().unwrap())
                    } else {
                        AccountInfo::AccountHash(account.account_hash())
                    };
                    (*routing, Some((info, AccountCapacity::new(account.canonical_size()))))
                })
                .collect::<Vec<_>>(),
        );
        assert_eq!(map.hash(), expected.hash());
        assert_eq!(map.capacity(), expected.capacity());
        let mut corrupt = bytes;
        let map_len = u64::from_le_bytes(corrupt[..8].try_into().unwrap()) as usize;
        // Damage the BOC magic, not an otherwise valid account's content: bodies no longer need
        // to match the redundant legacy-map hashes.
        corrupt[8 + map_len + 8 + 64 + 8] ^= 0xff;
        assert!(read_snapshot_v1_durable(&mut corrupt.as_slice(), |entries| {
            for entry in entries {
                entry?;
            }
            Ok(())
        })
        .is_err());
        Ok(())
    }

    #[test]
    fn legacy_sink_must_drain_prefetched_validation_results() {
        let (bytes, _) = fixture();
        assert!(read_snapshot_v1_durable(&mut bytes.as_slice(), |entries| {
            entries.next().unwrap()?;
            Ok(())
        })
        .is_err());
    }

    /// Reproduce the legacy export's sequential map rebuild on real mainnet
    /// routings/hashes, without importing bodies or accessing a database.
    #[test]
    #[ignore = "requires a real SNAP2V1 file and zstd CLI"]
    fn real_legacy_snapshot_map_rebuild_timing() -> anyhow::Result<()> {
        use std::fs::File;
        use std::io::BufReader;
        use std::io::Cursor;
        use std::process::Command;
        use std::process::Stdio;

        let path = std::env::var("LOCAL_SNAPSHOT_PATH")
            .unwrap_or_else(|_| "/Volumes/x5/_/latest-main-snapshot".into());
        let started = Instant::now();
        let mut file = File::open(&path)?;
        ensure!(read_array::<4>(&mut file)? == *b"ZSTD", "Expected compressed snapshot");
        let mut child = Command::new("zstd")
            .arg("-dc")
            .stdin(Stdio::from(file))
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let mut reader = BufReader::with_capacity(8 * 1024 * 1024, child.stdout.take().unwrap());
        let metadata = (|| -> anyhow::Result<_> {
            ensure!(read_array::<7>(&mut reader)? == *b"SNAP2V1", "Expected legacy snapshot");
            let prefix_len = u64::from_le_bytes(read_array(&mut reader)?);
            ensure!(
                std::io::copy(&mut (&mut reader).take(prefix_len), &mut std::io::sink())?
                    == prefix_len,
                "Truncated snapshot prefix"
            );
            let map_len = u64::from_le_bytes(read_array(&mut reader)?);
            // Legacy full-map header: path length + 32-byte path, node tag,
            // then its stored root hash. Replay these bytes to the normal parser.
            ensure!(map_len >= 66, "Expected nonempty legacy map");
            let header = read_array::<66>(&mut reader)?;
            ensure!(header[0] == 0 && header[33] != 0, "Expected full nonempty map");
            let root: [u8; 32] = header[34..66].try_into()?;
            let replay = [map_len.to_le_bytes().as_slice(), &header].concat();
            let entries = read_legacy_account_infos(
                &mut Cursor::new(replay).chain((&mut reader).take(map_len - 66)),
            )?;
            let accounts = u64::from_le_bytes(read_array(&mut reader)?);
            ensure!(accounts == entries.len() as u64, "Account count mismatch");
            Ok((entries, root, map_len))
        })();
        // The account bodies are deliberately not consumed or validated.
        drop(reader);
        let _ = child.kill();
        child.wait()?;
        let (metadata, expected_root, map_bytes) = metadata?;
        let read_s = started.elapsed().as_secs_f64();
        let entries_started = Instant::now();
        let entries: Vec<_> = metadata
            .into_iter()
            .map(|(routing, info)| (routing, Some((info, AccountCapacity::default()))))
            .collect();
        let entries_s = entries_started.elapsed().as_secs_f64();
        eprintln!("MAINNET_MAP metadata_read_s={read_s:.3} entries_s={entries_s:.3} accounts={} serialized_map_bytes={map_bytes} bodies_read=false", entries.len());
        let build_started = Instant::now();
        let map = ThreadAccountMap::new().update(&entries);
        let build_s = build_started.elapsed().as_secs_f64();
        let root_matches = map.hash().as_slice() == expected_root;
        eprintln!("MAINNET_MAP build_s={build_s:.3} root={:?} stored_root={expected_root:02x?} root_matches={root_matches}", map.hash());
        let verify_started = Instant::now();
        ensure!(map.iter().count() == entries.len(), "Rebuilt map coverage mismatch");
        for (routing, entry) in &entries {
            ensure!(
                map.get(routing) == entry.map(|(info, _)| info),
                "Rebuilt leaf differs: {routing}"
            );
        }
        let verify_s = verify_started.elapsed().as_secs_f64();
        let write_started = Instant::now();
        write_account_map(&map, &mut std::io::sink(), &|| false)?;
        let write_s = write_started.elapsed().as_secs_f64();
        eprintln!("MAINNET_MAP accounts={} dapps={} build_s={build_s:.3} verify_s={verify_s:.3} write_to_sink_s={write_s:.3} leaves_match=true root_matches={root_matches} capacity=zero_as_in_legacy_export", entries.len(), map.dapps.iter().count());
        // Account-leaf equality does not require the historical root: legacy
        // snapshots may retain empty L2 maps as L1 leaves and adjacent Ext
        // nodes. The independent hash audit checks those structural differences.
        Ok(())
    }

    #[test]
    #[ignore = "requires a real SNAP2V1 file, zstd CLI and an isolated Aerospike"]
    fn real_legacy_snapshot_subset_import_roundtrip() -> anyhow::Result<()> {
        use std::fs::File;
        use std::io::BufReader;
        use std::io::Cursor;
        use std::process::Command;
        use std::process::Stdio;

        use node_types::BlockIdentifier;
        use node_types::ThreadIdentifier;

        use crate::ThreadAccountsRepository;

        let path = std::env::var("LOCAL_SNAPSHOT_PATH")
            .unwrap_or_else(|_| "/Volumes/x5/_/latest-main-snapshot".into());
        let address = std::env::var("LOCAL_SNAPSHOT_AEROSPIKE_ADDRESS")?;
        let namespace = std::env::var("LOCAL_SNAPSHOT_AEROSPIKE_NAMESPACE")
            .unwrap_or_else(|_| "snapshot_import".into());
        let limit: usize = std::env::var("LOCAL_SNAPSHOT_ACCOUNT_LIMIT")
            .unwrap_or_else(|_| "100000".into())
            .parse()?;
        ensure!(limit > 0, "Account limit must be positive");
        let started = Instant::now();
        let mut file = File::open(path)?;
        ensure!(read_array::<4>(&mut file)? == *b"ZSTD", "Expected compressed snapshot");
        let mut child = Command::new("zstd")
            .arg("-dc")
            .stdin(Stdio::from(file))
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let mut reader = BufReader::with_capacity(8 * 1024 * 1024, child.stdout.take().unwrap());
        let subset = (|| -> anyhow::Result<_> {
            ensure!(read_array::<7>(&mut reader)? == *b"SNAP2V1", "Expected legacy snapshot");
            let prefix_len = u64::from_le_bytes(read_array(&mut reader)?);
            let skipped = std::io::copy(&mut (&mut reader).take(prefix_len), &mut std::io::sink())?;
            ensure!(skipped == prefix_len, "Truncated snapshot prefix");
            let mut expected = read_legacy_account_infos(&mut reader)?;
            let count = u64::from_le_bytes(read_array(&mut reader)?);
            ensure!(
                count == expected.len() as u64 && count >= limit as u64,
                "Invalid source count or subset limit"
            );
            let mut groups: HashMap<MapKey, Vec<_>> = HashMap::new();
            let mut bodies = Vec::with_capacity(limit);
            for _ in 0..limit {
                let (routing, body) =
                    super::super::repository::read_snapshot_account_entry(&mut reader)?;
                let info = expected
                    .remove(&routing)
                    .ok_or_else(|| anyhow::anyhow!("Unexpected or duplicate source body"))?;
                groups
                    .entry(MapKey(*routing.dapp_id().as_array()))
                    .or_default()
                    .push((MapKey(*routing.account_id().as_array()), Some(info)));
                bodies.push((routing, body));
            }
            drop(expected);
            let l1 = MultiMap::new().update(
                &groups
                    .into_iter()
                    .map(|(key, updates)| (key, Some(DappMap(MultiMap::new().update(&updates)))))
                    .collect::<Vec<_>>(),
            );
            let mut maps = Vec::new();
            write_old_map(&l1, &mut maps, &|dapp, writer| {
                write_old_map(&dapp.0, writer, &|info, writer| info.write_value(writer).unwrap())
            });
            let mut bytes = Vec::new();
            bytes.extend((maps.len() as u64).to_le_bytes());
            bytes.extend(maps);
            bytes.extend((limit as u64).to_le_bytes());
            for (routing, body) in &bodies {
                super::super::repository::write_snapshot_account_entry(&mut bytes, routing, body)?;
            }
            Ok((bytes, bodies))
        })();
        drop(reader);
        let _ = child.kill();
        let _ = child.wait();
        let (bytes, bodies) = subset?;
        if let Some(path) = std::env::var_os("LOCAL_SNAPSHOT_SUBSET_OUTPUT") {
            // Test fixture only: never overwrite an existing artifact.
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)?
                .write_all(&bytes)?;
        }
        eprintln!(
            "SUBSET accounts={} durable_bytes={} prepare_s={:.3}",
            limit,
            bytes.len(),
            started.elapsed().as_secs_f64()
        );

        let dir = tempfile::tempdir()?;
        let unique = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos();
        let prefix = format!("snapshot_subset_{unique}");
        let repo = ThreadAccountsRepository::builder(dir.path().join("source"))
            .set_accounts_aerospike_store(&address, &namespace, &prefix)
            .build()?;
        let thread = ThreadIdentifier::default();
        let block = BlockIdentifier::new([1; 32]); // A diagnostic subset, not a full network state.
        let import_started = Instant::now();
        let staged = repo.begin_staged_import_after_current_epoch();
        let result = repo.import_snapshot_v1_from_reader_staged(
            &mut bytes.as_slice(),
            &thread,
            &block,
            staged,
        );
        let state = match result {
            Ok(state) => state,
            Err(err) => {
                repo.abort_staged_import(staged);
                return Err(err);
            }
        };
        repo.commit_staged_import(staged, &thread, &block)?;
        let import_s = import_started.elapsed().as_secs_f64();
        assert_eq!(state.map().iter().count(), limit);
        assert_eq!(
            state.map().capacity().bytes(),
            bodies.iter().map(|(_, b)| b.len() as u64).sum::<u64>()
        );
        for (routing, body) in bodies.iter().take(100) {
            let stored = repo.durable_map_repo().archive_account_for_test(routing)?.unwrap();
            assert_eq!(stored.canonical_bytes(), body);
        }
        let mut map_bytes = Vec::new();
        state.map().write(&mut map_bytes)?;
        let restored = ThreadAccountMap::read(&mut map_bytes.as_slice())?;
        assert_eq!(restored.hash(), state.map().hash());
        assert_eq!(restored.capacity(), state.map().capacity());

        let export_started = Instant::now();
        let mut exported = Cursor::new(Vec::new());
        repo.durable_map_repo().export_durable_snapshot_to_writer(&state, &mut exported)?;
        let export_s = export_started.elapsed().as_secs_f64();
        let target = ThreadAccountsRepository::builder(dir.path().join("target"))
            .set_accounts_aerospike_store(&address, &namespace, format!("{prefix}_rt"))
            .build()?;
        let staged = target.begin_staged_import_after_current_epoch();
        exported.set_position(0);
        let restored = match target.import_durable_snapshot_from_reader_staged(
            &mut exported,
            &thread,
            &block,
            staged,
        ) {
            Ok(state) => state,
            Err(err) => {
                target.abort_staged_import(staged);
                return Err(err);
            }
        };
        target.commit_staged_import(staged, &thread, &block)?;
        assert_eq!(restored.map().hash(), state.map().hash());
        assert_eq!(restored.map().capacity(), state.map().capacity());
        assert_eq!(restored.map().iter().count(), limit);
        eprintln!("VERIFIED accounts={limit} import_s={import_s:.3} export_s={export_s:.3} total_s={:.3} prefix={prefix} root={:?}",
            started.elapsed().as_secs_f64(), state.map().hash());
        Ok(())
    }

    #[test]
    fn legacy_writer_matches_independent_baseline_encoding() -> anyhow::Result<()> {
        let (expected, routes) = fixture();
        let updates: Vec<_> = routes
            .iter()
            .map(|(route, account)| {
                let info = if account.is_redirect() {
                    AccountInfo::Redirect(account.get_dapp_id().unwrap())
                } else {
                    AccountInfo::AccountHash(AccountHash::from(
                        account.tvm_shard_account().unwrap().account_cell().unwrap().repr_hash(),
                    ))
                };
                (*route, Some((info, AccountCapacity::default())))
            })
            .collect();
        let mut map = ThreadAccountMap::new().update(&updates);
        map.dapps = map.dapps.update_single_aug(
            &MapKey([8; 32]),
            Some((L1MapValue(MultiMapAug::new()), AccountCapacity::default())),
        );
        let mut bytes = Vec::new();
        write_account_map(&map, &mut bytes, &|| false)?;
        let len = u64::from_le_bytes(expected[..8].try_into().unwrap()) as usize;
        assert!(
            bytes == expected[8..8 + len],
            "Legacy node layout or hashes differ: actual={} expected={} first_diff={:?}",
            bytes.len(),
            len,
            bytes.iter().zip(&expected[8..8 + len]).position(|(a, b)| a != b)
        );
        assert!(write_account_map(&map, &mut Vec::new(), &|| true).is_err());
        Ok(())
    }

    #[test]
    fn legacy_reader_preserves_keys_across_adjacent_extensions() -> anyhow::Result<()> {
        let value = AccountInfo::AccountHash(AccountHash::from(tvm_types::UInt256::from([5; 32])));
        let source = MultiMap {
            root: Node::new_ext(&[1; 32], Node::new_ext(&[2; 32], Node::new_leaf(value))),
            root_path: MapKeyPath::default(),
        };
        let mut bytes = Vec::new();
        write_old_map(&source, &mut bytes, &|info, w| info.write_value(w).unwrap());
        let expected: Vec<_> = source.iter().map(|(key, value)| (key, *value)).collect();
        assert_eq!(read_account_leaves(&bytes)?, expected);

        // Stored hashes are consumed but do not affect leaf extraction.
        let inner_hash_offset = 33 + 1 + 32 + 1 + 32 + 1;
        bytes[inner_hash_offset] ^= 1;
        assert_eq!(read_account_leaves(&bytes)?, expected);

        for (offset, value) in [(33, 4), (33 + 33, 65), (33 + 34, 16)] {
            let mut malformed = bytes.clone();
            malformed[offset] = value;
            assert!(read_account_leaves(&malformed).is_err());
        }

        let empty_child = MultiMap::<AccountInfo> {
            root: Node::new_ext(&[1], Node::Empty),
            root_path: MapKeyPath::default(),
        };
        let mut bytes = Vec::new();
        write_old_map(&empty_child, &mut bytes, &|info, w| info.write_value(w).unwrap());
        assert!(read_account_leaves(&bytes).is_err());
        Ok(())
    }

    #[test]
    fn legacy_reader_still_rejects_inconsistent_bitmaps_and_short_keys() -> anyhow::Result<()> {
        let value = AccountInfo::Redirect(DAppIdentifier::new([7; 32]));
        let source = MultiMap::new()
            .update_single(&MapKey([0x11; 32]), Some(value))
            .update_single(&MapKey([0x22; 32]), Some(value));
        let mut bytes = Vec::new();
        write_old_map(&source, &mut bytes, &|info, w| info.write_value(w).unwrap());
        assert_eq!(read_account_leaves(&bytes)?.len(), 2);
        assert_eq!(bytes[33], 2); // Root branch.
        bytes[33 + 1 + 32] |= 1; // Claim a third child that is actually empty.
        assert!(read_account_leaves(&bytes).is_err());

        let short_key = MultiMap { root: Node::new_leaf(value), root_path: MapKeyPath::default() };
        let mut bytes = Vec::new();
        write_old_map(&short_key, &mut bytes, &|info, w| info.write_value(w).unwrap());
        assert!(read_account_leaves(&bytes).is_err());
        Ok(())
    }

    #[test]
    fn legacy_accounts_and_redirects_are_verified_and_rebuilt() -> anyhow::Result<()> {
        let (bytes, accounts) = fixture();
        let mut bytes = bytes;
        // The old tree is an opaque length-delimited segment. Even unusable tree bytes must not
        // affect body-derived account hashes, redirect targets, capacities or the canonical root.
        let map_len = u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize;
        bytes[8..8 + map_len].fill(0xff);
        let mut seen = 0;
        let map = read_snapshot_v1_durable(&mut bytes.as_slice(), |entries| {
            for (index, entry) in entries.enumerate() {
                let (routing, body) = entry?;
                assert_eq!(routing, accounts[index].0);
                assert_eq!(body, accounts[index].1.canonical_bytes());
                seen += 1;
            }
            Ok(())
        })?;
        assert_eq!(seen, accounts.len());
        for (routing, account) in accounts {
            let expected = if account.is_redirect() {
                AccountInfo::Redirect(account.get_dapp_id().unwrap())
            } else {
                AccountInfo::AccountHash(account.account_hash())
            };
            assert_eq!(map.get(&routing), Some(expected));
            assert_eq!(
                map.account_capacity(&routing),
                Some(AccountCapacity::new(account.canonical_size()))
            );
        }
        Ok(())
    }

    #[test]
    fn legacy_import_stores_normalized_bodies_and_capacities() -> anyhow::Result<()> {
        use tvm_block::Serializable;

        let (original, accounts) = fixture();
        let map_len = u64::from_le_bytes(original[..8].try_into().unwrap()) as usize;
        let mut wire = original[..8 + map_len + 8].to_vec();
        let mut wire_size = 0;
        for (routing, account) in &accounts {
            let decoded = if account.is_redirect() {
                tvm_block::ShardAccount::with_redirect(
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
            super::super::repository::write_snapshot_account_entry(&mut wire, routing, &body)?;
        }
        let mut seen = 0;
        let map = read_snapshot_v1_durable(&mut wire.as_slice(), |entries| {
            for entry in entries {
                let (routing, body) = entry?;
                let expected = &accounts.iter().find(|(key, _)| *key == routing).unwrap().1;
                assert_eq!(body, expected.canonical_bytes());
                seen += 1;
            }
            Ok(())
        })?;
        assert_eq!(seen, accounts.len());
        let canonical_size: u64 =
            accounts.iter().map(|(_, account)| account.canonical_size()).sum();
        assert!(wire_size > canonical_size);
        assert_eq!(map.capacity().bytes(), canonical_size);
        for (routing, account) in accounts {
            assert_eq!(
                map.account_capacity(&routing),
                Some(AccountCapacity::new(account.canonical_size()))
            );
        }
        Ok(())
    }

    #[test]
    fn legacy_reader_accepts_empty_body_stream_with_opaque_old_map() -> anyhow::Result<()> {
        let mut bytes = Vec::new();
        bytes.extend(3u64.to_le_bytes());
        bytes.extend([0xff; 3]);
        bytes.extend(0u64.to_le_bytes());
        let map = read_snapshot_v1_durable(&mut bytes.as_slice(), |entries| {
            assert!(entries.next().is_none());
            Ok(())
        })?;
        assert!(map.is_empty());
        assert_eq!(map.capacity(), AccountCapacity::default());
        Ok(())
    }

    #[test]
    fn legacy_reader_uses_bodies_instead_of_obsolete_map_values() -> anyhow::Result<()> {
        let (bytes, accounts) = fixture();
        let map_len = u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize;
        let body_offset = 8 + map_len + 8;
        let mut changed = bytes[..body_offset].to_vec();
        // A valid different body supersedes the obsolete map leaf. This explicitly documents
        // the loss of internal map/body consistency checking, not of block authentication.
        let different = ThreadAccount::default();
        super::super::repository::write_snapshot_account_entry(
            &mut changed,
            &accounts[0].0,
            different.canonical_bytes(),
        )?;
        super::super::repository::write_snapshot_account_entry(
            &mut changed,
            &accounts[1].0,
            accounts[1].1.canonical_bytes(),
        )?;
        let map = read_snapshot_v1_durable(&mut changed.as_slice(), |entries| {
            for entry in entries {
                entry?;
            }
            Ok(())
        })?;
        assert_eq!(
            map.get(&accounts[0].0),
            Some(AccountInfo::AccountHash(different.account_hash()))
        );
        assert_eq!(
            map.account_capacity(&accounts[0].0),
            Some(AccountCapacity::new(different.canonical_size()))
        );
        Ok(())
    }

    #[test]
    fn legacy_reader_rejects_bad_bodies_duplicates_counts_and_framing() -> anyhow::Result<()> {
        let (bytes, accounts) = fixture();
        let map_len = u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize;
        let count_offset = 8 + map_len;
        let body_offset = count_offset + 8;
        let mut cases = Vec::new();
        let mut malformed = bytes.clone();
        malformed[body_offset + 64 + 8] ^= 0xff;
        cases.push(malformed);
        let mut duplicate = bytes[..body_offset].to_vec();
        for _ in 0..2 {
            super::super::repository::write_snapshot_account_entry(
                &mut duplicate,
                &accounts[0].0,
                accounts[0].1.canonical_bytes(),
            )?;
        }
        cases.push(duplicate);
        cases.push(bytes[..bytes.len() - 1].to_vec());
        let mut trailing = bytes.clone();
        trailing.push(0);
        cases.push(trailing);
        for count in [1u64, 3] {
            let mut wrong_count = bytes.clone();
            wrong_count[count_offset..body_offset].copy_from_slice(&count.to_le_bytes());
            cases.push(wrong_count);
        }
        cases.push(bytes[..8 + map_len - 1].to_vec());
        for bytes in cases {
            assert!(read_snapshot_v1_durable(&mut bytes.as_slice(), |entries| {
                for entry in entries {
                    entry?;
                }
                Ok(())
            })
            .is_err());
        }
        Ok(())
    }

    #[test]
    fn legacy_reader_rejects_duplicate_routings_across_validation_batches() -> anyhow::Result<()> {
        let (_, accounts) = fixture();
        // More than one batch, with the same routing in separate batches. Completion order must
        // not hide the duplicate, and no separate seen-routing HashSet is needed.
        let mut routes: Vec<_> = (0..=VALIDATION_BATCH_RECORDS)
            .map(|index| {
                let mut key = [0; 32];
                key[..8].copy_from_slice(&(index as u64).to_le_bytes());
                (
                    AccountIdentifier::new(key).routing(*accounts[0].0.dapp_id()),
                    accounts[0].1.clone(),
                )
            })
            .collect();
        let duplicate = routes[0].0;
        routes.last_mut().unwrap().0 = duplicate;
        let bytes = encode_accounts(&routes);
        let error = read_snapshot_v1_durable(&mut bytes.as_slice(), |entries| {
            for entry in entries {
                entry?;
            }
            Ok(())
        })
        .unwrap_err();
        assert!(error.to_string().contains("Duplicate legacy account body"), "{error}");
        Ok(())
    }

    #[test]
    fn legacy_bodies_are_staged_before_commit_and_late_failure_can_be_aborted() -> anyhow::Result<()>
    {
        use node_types::BlockIdentifier;
        use node_types::ThreadIdentifier;

        use crate::ArchiveStateStore;
        use crate::ThreadAccountsRepository;

        let (bytes, accounts) = fixture();
        let dir = tempfile::tempdir()?;
        let archive = ArchiveStateStore::in_memory();
        let repo =
            ThreadAccountsRepository::new(dir.path().join("maps"), archive.clone(), true, None)?;
        let thread = ThreadIdentifier::default();
        let block = BlockIdentifier::new([1; 32]);
        repo.ensure_thread(&thread, &BlockIdentifier::default(), true)?;
        let initial_epoch = archive.data_epoch();
        let staged = repo.begin_staged_import_after_current_epoch();
        let state = repo.import_snapshot_v1_from_reader_staged(
            &mut bytes.as_slice(),
            &thread,
            &block,
            staged,
        )?;
        assert_eq!(archive.data_epoch(), initial_epoch);
        for (routing, account) in &accounts {
            assert!(archive.read_account_operation_at_epoch(routing, initial_epoch)?.is_none());
            let body = archive.read_account_operation_at_epoch(routing, staged.epoch)?.unwrap();
            assert_eq!(body.canonical_bytes(), account.canonical_bytes());
            assert_eq!(
                (state.map()).account_capacity(routing),
                Some(AccountCapacity::new(account.canonical_size()))
            );
        }
        repo.commit_staged_import(staged, &thread, &block)?;
        let active_epoch = archive.data_epoch();
        assert_eq!(active_epoch, staged.epoch);
        let controls = repo.get_archive_thread_control_states()?;

        let staged = repo.begin_staged_import_after_current_epoch();
        let mut corrupt = bytes;
        corrupt.push(0); // All bodies have been written before this is detected.
        assert!(repo
            .import_snapshot_v1_from_reader_staged(
                &mut corrupt.as_slice(),
                &thread,
                &BlockIdentifier::new([2; 32]),
                staged
            )
            .is_err());
        assert!(archive.read_account_operation_at_epoch(&accounts[0].0, staged.epoch)?.is_some());
        assert_eq!(archive.data_epoch(), active_epoch);
        assert_eq!(repo.get_archive_thread_control_states()?, controls);
        repo.abort_staged_import(staged);
        assert!(archive.read_account_operation_at_epoch(&accounts[0].0, staged.epoch)?.is_none());
        assert_eq!(
            archive
                .read_account_operation_at_epoch(&accounts[0].0, active_epoch)?
                .unwrap()
                .canonical_bytes(),
            accounts[0].1.canonical_bytes()
        );
        Ok(())
    }
}
