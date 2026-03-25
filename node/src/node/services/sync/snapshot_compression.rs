/// Magic bytes prepended to zstd-compressed snapshots.
pub const COMPRESSED_SNAPSHOT_MAGIC: &[u8; 4] = b"ZSTD";

/// Zstd compression level used when saving snapshots.
/// Level 3 is the default — fast enough not to be a bottleneck,
/// good enough to shrink bincode payloads significantly.
pub const ZSTD_COMPRESSION_LEVEL: i32 = 3;

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
