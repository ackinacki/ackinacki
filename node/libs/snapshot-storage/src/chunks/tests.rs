use std::io::Cursor;
use std::io::Read;
use std::io::Write;

use super::*;

fn encode(bytes: &[u8], step: usize) -> anyhow::Result<(Vec<u8>, ChunkMapMetadata)> {
    let mut writer = ChunkWriter::new(Vec::new())?;
    for part in bytes.chunks(step) {
        writer.write_all(part)?;
    }
    writer.finish()
}

#[test]
fn chunks_are_deterministic_and_cover_partial_and_exact_boundaries() -> anyhow::Result<()> {
    for size in [1, CHUNK_BYTES, CHUNK_BYTES + 1, 2 * CHUNK_BYTES] {
        let bytes: Vec<_> = (0..size).map(|i| (i % 251) as u8).collect();
        let (encoded, metadata) = encode(&bytes, bytes.len())?;
        assert_eq!(encode(&bytes, 777)?.0, encoded);
        assert_eq!(metadata.offset, 4 + bytes.len() as u64);
        assert_eq!(&encoded[4..metadata.offset as usize], bytes);
        assert_eq!(verify_file(&mut Cursor::new(&encoded), &|| false)?, metadata);
        assert_eq!(read_footer(&mut Cursor::new(&encoded))?, metadata);
        assert_eq!(verify_map(&mut Cursor::new(&encoded))?, metadata);
        let mut streaming = PayloadReader::new(Cursor::new(&encoded))?;
        let mut payload = vec![0; bytes.len()];
        streaming.read_exact(&mut payload)?;
        assert_eq!(payload, bytes);
        assert_eq!(streaming.verify_trailer()?, metadata);
    }
    Ok(())
}

#[test]
fn preverified_reader_counts_payload_without_hashing_it_again() -> anyhow::Result<()> {
    for size in [1, CHUNK_BYTES, CHUNK_BYTES + 17] {
        let payload = vec![7; size];
        let (encoded, metadata) = encode(&payload, 777)?;
        let mut reader = PayloadReader::new_preverified(Cursor::new(&encoded))?;
        let mut restored = vec![0; size];
        // Exercise BufRead::consume as used by the zstd decoder.
        reader.read_exact(&mut restored[..1])?;
        let mut offset = 1;
        while offset < size {
            let count = reader.fill_buf()?.len().min(size - offset);
            restored[offset..offset + count].copy_from_slice(&reader.fill_buf()?[..count]);
            reader.consume(count);
            offset += count;
        }
        assert_eq!(restored, payload);
        assert_eq!(reader.hashes.bytes, size as u64);
        assert!(reader.hashes.completed.is_empty());
        assert_eq!(reader.hashes.current.finalize(), blake3::hash(&[]));
        assert_eq!(reader.verify_trailer()?, metadata);
    }
    Ok(())
}

#[test]
fn preverified_reader_still_rejects_wrong_frame_boundary_and_trailer() -> anyhow::Result<()> {
    let (encoded, metadata) = encode(b"payload", 7)?;
    for consumed in [6, 8] {
        let mut reader = PayloadReader::new_preverified(Cursor::new(&encoded))?;
        reader.read_exact(&mut vec![0; consumed])?;
        assert!(reader.verify_trailer().is_err());
    }
    for index in [metadata.offset as usize, encoded.len() - 1] {
        let mut bad = encoded.clone();
        bad[index] ^= 1;
        let mut reader = PayloadReader::new_preverified(Cursor::new(&bad))?;
        reader.read_exact(&mut [0; 7])?;
        assert!(reader.verify_trailer().is_err());
    }
    // Payload integrity belongs to the caller in this mode; the default reader
    // must still detect damage when no verified transport handoff was supplied.
    let mut changed = encoded;
    changed[4] ^= 1;
    let mut verified = PayloadReader::new_preverified(Cursor::new(&changed))?;
    verified.read_exact(&mut [0; 7])?;
    verified.verify_trailer()?;
    let mut unverified = PayloadReader::new(Cursor::new(changed))?;
    unverified.read_exact(&mut [0; 7])?;
    assert!(unverified.verify_trailer().is_err());
    Ok(())
}

#[test]
fn corrupt_payload_map_footer_and_trailing_data_are_rejected() -> anyhow::Result<()> {
    let (encoded, metadata) = encode(&vec![7; CHUNK_BYTES + 17], 333)?;
    for index in [
        0,
        4,
        CHUNK_BYTES + 4,
        metadata.offset as usize,
        metadata.offset as usize + 12,
        encoded.len() - FOOTER_BYTES,
        encoded.len() - FOOTER_BYTES + 8,
        encoded.len() - FOOTER_BYTES + 16,
        encoded.len() - 1,
    ] {
        let mut bad = encoded.clone();
        bad[index] ^= 1;
        assert!(verify_file(&mut Cursor::new(bad), &|| false).is_err(), "offset {index}");
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(verify_file(&mut Cursor::new(trailing), &|| false).is_err());
    assert!(verify_file(&mut Cursor::new(&encoded[..encoded.len() - 1]), &|| false).is_err());
    assert!(verify_file(&mut Cursor::new(encoded), &|| true).is_err());
    Ok(())
}

#[test]
fn serving_metadata_reads_only_magic_and_footer() -> anyhow::Result<()> {
    struct FooterOnly(std::io::Cursor<Vec<u8>>);
    impl Read for FooterOnly {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            let offset = self.0.position() as usize;
            assert!(offset < MAGIC.len() || offset >= self.0.get_ref().len() - FOOTER_BYTES);
            self.0.read(bytes)
        }
    }
    impl std::io::Seek for FooterOnly {
        fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
            self.0.seek(position)
        }
    }
    let (encoded, map) = encode(&vec![19; CHUNK_BYTES + 17], 777)?;
    assert_eq!(read_footer(&mut FooterOnly(Cursor::new(encoded)))?, map);
    Ok(())
}

#[test]
fn map_recognition_does_not_read_the_payload() -> anyhow::Result<()> {
    struct MapOnly {
        inner: Cursor<Vec<u8>>,
        offset: u64,
    }
    impl Read for MapOnly {
        fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
            let position = self.inner.position();
            assert!(position < MAGIC.len() as u64 || position >= self.offset);
            self.inner.read(bytes)
        }
    }
    impl std::io::Seek for MapOnly {
        fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
            self.inner.seek(position)
        }
    }
    let (mut encoded, map) = encode(&vec![19; CHUNK_BYTES + 17], 777)?;
    let mut reader = MapOnly { inner: Cursor::new(encoded.clone()), offset: map.offset };
    assert_eq!(verify_map(&mut reader)?, map);
    encoded[4] ^= 1;
    assert_eq!(verify_map(&mut Cursor::new(&encoded))?, map);
    assert!(verify_file(&mut Cursor::new(&encoded), &|| false).is_err());
    encoded[map.offset as usize + 12] ^= 1;
    assert!(verify_map(&mut Cursor::new(&encoded)).is_err());
    Ok(())
}

#[test]
fn zstd_read_ahead_does_not_hash_the_map_as_payload() -> anyhow::Result<()> {
    let raw = vec![0x31; CHUNK_BYTES * 3];
    let compressed = zstd::encode_all(raw.as_slice(), 3)?;
    let (bytes, metadata) = encode(&compressed, 100)?;
    let input = PayloadReader::new(Cursor::new(bytes))?;
    let mut decoder = zstd::Decoder::with_buffer(input)?.single_frame();
    let mut restored = Vec::new();
    decoder.read_to_end(&mut restored)?;
    assert_eq!(restored, raw);
    assert_eq!(decoder.finish().verify_trailer()?, metadata);
    Ok(())
}

#[test]
#[ignore = "local file-read/hash timing experiment"]
fn chunk_verification_file_timing() -> anyhow::Result<()> {
    use std::io::Seek;
    use std::time::Instant;
    let mut file = tempfile::tempfile()?;
    let mut writer = ChunkWriter::new(&mut file)?;
    let block = vec![0x71; CHUNK_BYTES];
    for _ in 0..192 {
        writer.write_all(&block)?;
    }
    writer.finish()?;
    for round in 0..3 {
        file.rewind()?;
        let start = Instant::now();
        std::io::copy(&mut file, &mut std::io::sink())?;
        let read = start.elapsed();
        let start = Instant::now();
        verify_file(&mut file, &|| false)?;
        eprintln!("192 MiB round {round}: read={read:?}, read+verify={:?}", start.elapsed());
    }
    Ok(())
}
