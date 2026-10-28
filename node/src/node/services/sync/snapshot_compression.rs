/// Magic bytes prepended to zstd-compressed snapshots.
pub const COMPRESSED_SNAPSHOT_MAGIC: &[u8; 4] = snapshot_storage::chunks::MAGIC;

/// Zstd compression level used when saving snapshots.
/// Level 3 is the default — fast enough not to be a bottleneck,
/// good enough to shrink bincode payloads significantly.
pub const ZSTD_COMPRESSION_LEVEL: i32 = 3;

/// Bound both the canonical encoder window and consumer decoder allocation.
pub(crate) const PARTITION_WINDOW_LOG: u32 = 21;

/// Encode one partitioned-snapshot blob using the ACCTPV1/MSGSPV1/SNAP2V2
/// compression profile. Legacy snapshot writers deliberately do not use this.
///
/// The payload callback cannot change encoder parameters or finish a frame.
/// Its flush calls are suppressed: input buffering must not affect blob bytes.
/// The sink is flushed only after the single frame is finished. On any error,
/// the caller must discard the partial blob; this function does not publish it.
pub fn write_partition_blob<W, F>(writer: W, write_payload: F) -> anyhow::Result<W>
where
    W: std::io::Write,
    F: FnOnce(&mut dyn std::io::Write) -> anyhow::Result<()>,
{
    // Cargo.lock fixes the Rust adapters (zstd 0.13.3 / zstd-safe 7.3.0).
    // Also reject an incompatible system library selected by a custom build.
    anyhow::ensure!(
        zstd::zstd_safe::version_number() == 10507,
        "partitioned snapshots require zstd 1.5.7"
    );
    let mut writer = snapshot_storage::chunks::ChunkWriter::new(writer)?;
    let mut encoder = zstd::Encoder::new(&mut writer, ZSTD_COMPRESSION_LEVEL)?;
    encoder.set_parameter(zstd::zstd_safe::CParameter::NbWorkers(0))?;
    encoder.include_dictid(false)?;
    encoder.include_checksum(false)?;
    encoder.include_contentsize(false)?;
    encoder.set_pledged_src_size(None)?;
    encoder.window_log(PARTITION_WINDOW_LOG)?;
    encoder.set_target_cblock_size(None)?;
    // No dictionary is loaded into this fresh encoder.
    write_payload(&mut WithoutFlush(&mut encoder))?;
    encoder.finish()?;
    let (writer, _) = writer.finish()?;
    Ok(writer)
}

struct WithoutFlush<'a, W>(&'a mut W);

impl<W: std::io::Write> std::io::Write for WithoutFlush<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
#[ignore]
fn test_compress_snapshot() -> anyhow::Result<()> {
    let raw_bytes = std::fs::read(
        "/home/user/GOSH/debug_acki-nacki/mainnet/7d193127bdc56d4fb7ceb53bfcf442b4e5dbc8349c0bf91ae104238e27d44f98",
    )?;

    // Compress with zstd and prepend magic header
    let compressed = zstd::encode_all(raw_bytes.as_slice(), ZSTD_COMPRESSION_LEVEL)?;
    let mut bytes = Vec::with_capacity(COMPRESSED_SNAPSHOT_MAGIC.len() + compressed.len());
    bytes.extend_from_slice(COMPRESSED_SNAPSHOT_MAGIC);
    bytes.extend_from_slice(&compressed);

    println!("Compressed snapshot size (raw={} compressed={})", raw_bytes.len(), compressed.len());

    Ok(())
}

#[cfg(test)]
#[path = "snapshot_compression_tests.rs"]
mod tests;
