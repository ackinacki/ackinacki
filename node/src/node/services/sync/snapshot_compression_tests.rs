use std::io::Read;
use std::io::Write;

use account_state::read_account_partition_checked;
use account_state::write_account_partition_checked;
use account_state::AccountCapacity;
use account_state::AccountInfo;
use account_state::ArchiveStateStore;
use account_state::DurableThreadAccountsRepository;
use account_state::PartitionReadLimits;
use account_state::ThreadAccount;
use account_state::ThreadAccountMap;
use multi_map::MapKeyPath;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifier;
use node_types::ThreadIdentifier;

use super::*;

fn account_vector() -> Vec<u8> {
    // Same protocol vector as the raw ACCTPV1 account codec test. Keep the
    // input fixed independently of whichever account writer is under test.
    hex::decode(concat!(
        "4143435450563101010101010101010101010101010101010101010101010101",
        "0101010101010102020202020202020202020202020202020202020202020202",
        "020202020202025600000000000000b5ee9c7201010101004b00009100000000",
        "0000000000000000000000000000000000000000000000000000000000000000",
        "0000000058d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8d8",
        "d8d8d8d8e0",
    ))
    .unwrap()
}

fn encode(bytes: &[u8], chunk: usize, flush: bool) -> anyhow::Result<Vec<u8>> {
    write_partition_blob(Vec::new(), |writer| {
        for part in bytes.chunks(chunk) {
            writer.write_all(part)?;
            if flush {
                writer.flush()?;
            }
        }
        Ok(())
    })
}

fn decode(blob: &[u8]) -> anyhow::Result<Vec<u8>> {
    assert_eq!(&blob[..4], COMPRESSED_SNAPSHOT_MAGIC);
    let input = snapshot_storage::chunks::PayloadReader::new(blob)?;
    let mut decoder = zstd::Decoder::with_buffer(input)?.single_frame();
    let mut raw = Vec::new();
    decoder.read_to_end(&mut raw)?;
    decoder.finish().verify_trailer()?;
    Ok(raw)
}

#[test]
fn partition_compression_account_golden_vector() -> anyhow::Result<()> {
    let raw = account_vector();
    let blob = encode(&raw, raw.len(), false)?;
    assert_eq!(decode(&blob)?, raw);
    assert_eq!(
        hex::encode(
            &blob[..snapshot_storage::chunks::verify_file(
                &mut std::io::Cursor::new(&blob),
                &|| false
            )?
            .offset as usize]
        ),
        concat!(
            "5a53544428b52ffd0058450100e04143435450563101025600b5ee9c720101010100",
            "4b0000910058d8e005100022a6280c1bd6c401",
        )
    );
    Ok(())
}

#[test]
fn partition_compression_ignores_input_chunks_and_flushes() -> anyhow::Result<()> {
    let mut raw = account_vector();
    // Cross zstd block and window boundaries, exercising more than a tiny frame.
    raw.extend((0..3 * 1024 * 1024).map(|i| ((i * 31 + i / 257) % 251) as u8));
    let expected = encode(&raw, raw.len(), false)?;
    assert_eq!(decode(&expected)?, raw);
    for chunk in [1, 7, 4096, 128 * 1024, 1024 * 1024] {
        assert_eq!(encode(&raw, chunk, true)?, expected, "chunk size {chunk}");
    }
    assert_eq!(
        blake3::hash(
            &expected[..snapshot_storage::chunks::verify_file(
                &mut std::io::Cursor::new(&expected),
                &|| false,
            )?
            .offset as usize]
        )
        .to_hex()
        .as_str(),
        "b8ea7a2f4aec9764d169c674784343a1f0e4849242a17564d9457f99342b4b63",
    );
    Ok(())
}

#[test]
fn partition_compression_account_batches_have_identical_bytes() -> anyhow::Result<()> {
    let archive = ArchiveStateStore::in_memory();
    let thread = ThreadIdentifier::default();
    let block = BlockIdentifier::default();
    archive.ensure_thread(&thread, &block)?;
    archive.reset()?;
    let accounts: Vec<_> = (0..24u8)
        .map(|i| {
            let routing = AccountRouting::new(
                DAppIdentifier::new([i / 8; 32]),
                AccountIdentifier::new([i; 32]),
            );
            let shard = tvm_block::ShardAccount::with_params(
                &tvm_block::Account::default(),
                [i; 32].into(),
                i as u64,
                Some(routing.dapp_id().as_array().into()),
            )
            .unwrap();
            (routing, ThreadAccount::from_tvm(shard))
        })
        .collect();
    archive.thread_init_from_entries(&thread, block, accounts.iter().cloned().map(Ok))?;
    let dir = tempfile::tempdir()?;
    let repo = DurableThreadAccountsRepository::new(dir.path().into(), archive, None)?;
    let updates: Vec<_> = accounts
        .iter()
        .map(|(key, body)| {
            (
                *key,
                Some((
                    AccountInfo::AccountHash(body.account_hash()),
                    AccountCapacity::new(body.canonical_size()),
                )),
            )
        })
        .collect();
    let map = ThreadAccountMap::new().update(&updates);
    let mut expected = None;
    for batch in [1, 3, 256] {
        let limits = PartitionReadLimits {
            accounts_per_batch: batch,
            canonical_bytes_per_batch: if batch == 1 { 1 } else { 8 * 1024 * 1024 },
            in_flight_batches: if batch == 3 { 4 } else { 1 },
            ..Default::default()
        };
        let blob = write_partition_blob(Vec::new(), |mut writer| {
            write_account_partition_checked(&repo, &map, &mut writer, limits, &|| false)
        })?;
        if let Some(expected) = &expected {
            assert_eq!(&blob, expected);
        }
        let raw = decode(&blob)?;
        let restored = read_account_partition_checked(
            &mut raw.as_slice(),
            MapKeyPath::default(),
            map.hash(),
            &|| false,
            |_, _| Ok(()),
        )?;
        assert_eq!(restored.capacity(), map.capacity());
        expected = Some(blob);
    }
    Ok(())
}

#[test]
fn partition_compression_propagates_errors_and_flushes_only_finished_frames() {
    #[derive(Debug, Default)]
    struct Sink {
        bytes: Vec<u8>,
        flushes: usize,
        fail_flush: bool,
    }
    impl Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes.write(bytes)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            // This must already be a complete frame when the sink is flushed.
            decode(&self.bytes).unwrap();
            self.flushes += 1;
            if self.fail_flush {
                return Err(std::io::Error::other("sink flush failed"));
            }
            Ok(())
        }
    }
    let mut sink = Sink::default();
    write_partition_blob(&mut sink, |writer| {
        writer.write_all(&account_vector())?;
        writer.flush()?;
        Ok(())
    })
    .unwrap();
    assert_eq!(sink.flushes, 1);
    let mut failed = Sink { fail_flush: true, ..Default::default() };
    assert!(write_partition_blob(&mut failed, |writer| {
        writer.write_all(&account_vector())?;
        Ok(())
    })
    .unwrap_err()
    .to_string()
    .contains("sink flush failed"));
    let mut partial = Sink::default();
    let result = write_partition_blob(&mut partial, |writer| {
        writer.write_all(&account_vector())?;
        writer.flush()?;
        anyhow::bail!("payload failed")
    });
    assert!(result.err().unwrap().to_string().contains("payload failed"));
    assert_eq!(partial.flushes, 0);
    let mut unavailable = std::io::Cursor::new(&mut [][..]);
    assert!(write_partition_blob(&mut unavailable, |_| panic!("sink must fail first")).is_err());

    // The envelope fits but the final frame does not. Propagate finish errors
    // too, rather than reporting a successfully encoded truncated blob.
    let mut storage = [0; 8];
    let mut limited = std::io::Cursor::new(storage.as_mut_slice());
    let mut invoked = false;
    assert!(write_partition_blob(&mut limited, |writer| {
        invoked = true;
        writer.write_all(&account_vector())?;
        Ok(())
    })
    .is_err());
    assert!(invoked);
}
