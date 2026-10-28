use std::cell::Cell;
use std::io::BufWriter;
use std::io::Cursor;

use node_types::AccountHash;

use super::*;

fn fixture(count: u32, dapps: u32) -> (ThreadAccountMap, Vec<(AccountRouting, AccountInfo)>) {
    let mut source = ThreadAccountMap::new();
    let mut legacy = Vec::with_capacity(count as usize);
    for i in 0..count {
        let mut dapp = [0; 32];
        dapp[..4].copy_from_slice(&(i % dapps).to_be_bytes());
        let mut account = [0; 32];
        account[..4].copy_from_slice(&(i / dapps).to_be_bytes());
        account[31] = (i % 256) as u8;
        let routing =
            AccountRouting::new(DAppIdentifier::new(dapp), AccountIdentifier::new(account));
        let hash = *blake3::hash(&i.to_be_bytes()).as_bytes();
        let info = if i.is_multiple_of(7) {
            AccountInfo::Redirect(DAppIdentifier::new(hash))
        } else {
            AccountInfo::AccountHash(AccountHash::new(hash))
        };
        let source_info = match info {
            AccountInfo::Redirect(_) => info,
            AccountInfo::AccountHash(hash) => AccountInfo::AccountHash(AccountHash::new(
                *blake3::hash(hash.as_slice()).as_bytes(),
            )),
        };
        source = source.update_single(&routing, Some((source_info, AccountCapacity::new(1234))));
        legacy.push((routing, info));
    }
    legacy.sort_unstable_by_key(|(routing, _)| *routing);
    (source, legacy)
}

#[test]
fn bounded_writer_matches_rebuilt_legacy_tree_at_both_levels() -> anyhow::Result<()> {
    // Includes empty/singleton maps, one large DApp and many singleton DApps. Tiny buffers
    // force discarded probes after leaves, splitting inside L2 and then crossing into L1.
    for (count, dapps) in [(0, 1), (1, 1), (700, 1), (700, 3), (700, 700)] {
        let (source, entries) = fixture(count, dapps);
        let updates: Vec<_> = entries
            .iter()
            .map(|(routing, info)| (*routing, Some((*info, AccountCapacity::default()))))
            .collect();
        let rebuilt = ThreadAccountMap::new().update(&updates);
        if count > 1 {
            assert_ne!(source.hash(), rebuilt.hash());
        }
        let mut expected = Vec::new();
        super::super::snapshot_v1::write_account_map(&rebuilt, &mut expected, &|| false)?;
        let (layout_bytes, layout_accounts) = account_map_layout(&source, &|| false)?;
        assert_eq!(layout_bytes, expected.len() as u64);
        assert_eq!(layout_accounts, entries.len());
        let source_hash = source.hash();
        let source_capacity = source.capacity();
        for limit in [1, 2, 7, 32, HASH_BUFFER_NODES] {
            // Production rewrites a reserved map region before already-written account bodies.
            // Verify absolute seek positions, final cursor, and that surrounding bytes survive.
            let mut bytes = vec![0xab; 19];
            bytes.resize(19 + expected.len(), 0xdd);
            bytes.extend_from_slice(b"account bodies stay intact");
            let mut output = Cursor::new(bytes);
            output.set_position(19);
            let stats;
            {
                let mut writer = BufWriter::with_capacity(127, &mut output);
                stats = write_with_buffer(&source, &entries, &mut writer, &|| false, limit)?;
                assert_eq!(writer.stream_position()?, 19 + expected.len() as u64);
            }
            assert_eq!(&output.get_ref()[..19], &[0xab; 19]);
            assert_eq!(&output.get_ref()[19..19 + expected.len()], expected);
            assert_eq!(&output.get_ref()[19 + expected.len()..], b"account bodies stay intact");
            assert!(stats.hash_buffer_peak_bytes <= limit * 32);
            if count == 700 && limit == 7 {
                assert!(stats.patched_nodes > 0);
            }
        }
        assert_eq!(source.hash(), source_hash);
        assert_eq!(source.capacity(), source_capacity);
    }
    Ok(())
}

#[test]
fn bounded_writer_rejects_missing_unused_and_misordered_metadata() {
    let (source, entries) = fixture(300, 3);
    for limit in [1, 7, HASH_BUFFER_NODES] {
        let mut missing = entries.clone();
        missing.remove(150);
        let mut extra = entries.clone();
        extra.push(*entries.last().unwrap());
        let mut duplicate = entries.clone();
        duplicate[150] = duplicate[149];
        let mut swapped = entries.clone();
        swapped.swap(149, 150);
        for bad in [&missing, &extra, &duplicate, &swapped] {
            assert!(write_with_buffer(
                &source,
                bad,
                &mut Cursor::new(Vec::new()),
                &|| false,
                limit
            )
            .is_err());
        }
    }
}

#[test]
fn bounded_writer_rejects_scoped_maps_and_empty_dapps() {
    let (mut source, entries) = fixture(1, 1);
    source.dapps.root_path.len = 1;
    assert!(account_map_layout(&source, &|| false).is_err());
    assert!(write_account_map(&source, &entries, &mut Cursor::new(Vec::new()), &|| false).is_err());
    source.dapps.root_path.len = 0;
    let dapp = MapKey(*entries[0].0.dapp_id().as_array());
    let mut value = source.dapps.get(&dapp).unwrap();
    value.0.root_path.len = 1;
    let scoped = ThreadAccountMap {
        dapps: source.dapps.update_single_aug(&dapp, Some((value, source.capacity()))),
    };
    assert!(account_map_layout(&scoped, &|| false).is_err());
    assert!(write_account_map(&scoped, &entries, &mut Cursor::new(Vec::new()), &|| false).is_err());
    let empty = ThreadAccountMap {
        dapps: source.dapps.update_single_aug(
            &dapp,
            Some((L1MapValue(multi_map::MultiMapAug::new()), AccountCapacity::default())),
        ),
    };
    assert!(account_map_layout(&empty, &|| false).is_err());
    assert!(write_account_map(&empty, &[], &mut Cursor::new(Vec::new()), &|| false).is_err());
}

#[test]
fn layout_count_is_cancellable_without_writing_or_hashing() {
    let (source, _) = fixture(300, 3);
    let checks = Cell::new(0);
    let error = account_map_layout(&source, &|| {
        checks.set(checks.get() + 1);
        checks.get() == 100
    })
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(checks.get(), 100);
}

#[test]
fn bounded_writer_cancels_during_probes_and_serialization() -> anyhow::Result<()> {
    let (source, entries) = fixture(300, 3);
    for limit in [1, 7, HASH_BUFFER_NODES] {
        let checks = Cell::new(0);
        write_with_buffer(
            &source,
            &entries,
            &mut Cursor::new(Vec::new()),
            &|| {
                checks.set(checks.get() + 1);
                false
            },
            limit,
        )?;
        let total = checks.get();
        for at in [1, total / 3, total / 2, total - 1] {
            checks.set(0);
            let error = write_with_buffer(
                &source,
                &entries,
                &mut Cursor::new(Vec::new()),
                &|| {
                    checks.set(checks.get() + 1);
                    checks.get() >= at
                },
                limit,
            )
            .err()
            .unwrap();
            assert!(error.to_string().contains("cancelled"));
        }
    }
    Ok(())
}

struct FailingWriter {
    inner: Cursor<Vec<u8>>,
    remaining: usize,
    fail_seek: bool,
}

impl Write for FailingWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Err(std::io::Error::other("injected write failure"));
        }
        let count = bytes.len().min(self.remaining);
        self.remaining -= count;
        self.inner.write(&bytes[..count])
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Seek for FailingWriter {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        if self.fail_seek && matches!(position, SeekFrom::Start(_)) {
            return Err(std::io::Error::other("injected seek failure"));
        }
        self.inner.seek(position)
    }
}

#[test]
fn bounded_writer_propagates_partial_write_and_patch_failures() {
    let (source, entries) = fixture(300, 3);
    for (remaining, fail_seek) in [(0, false), (500, false), (usize::MAX, true)] {
        let mut output = FailingWriter { inner: Cursor::new(Vec::new()), remaining, fail_seek };
        assert!(write_with_buffer(&source, &entries, &mut output, &|| false, 7).is_err());
    }
}
