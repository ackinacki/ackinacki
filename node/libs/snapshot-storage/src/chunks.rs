//! Chunk integrity envelope shared by partitioned snapshot publication and import.
//!
//! # Scope and purpose
//!
//! Every partitioned snapshot blob uses this envelope: primary metadata, account
//! partitions, normal-inbox partitions and high-priority-inbox partitions. Chunks
//! describe the **compressed bytes**, not accounts, messages or uncompressed
//! records. They provide transport-integrity metadata and stable resume points
//! without changing the logical codecs or buffering a whole blob in memory.
//! Legacy monolithic snapshots (SNAP2V1) retain their original format and have
//! no chunk map/footer. The existing outer `ZSTD` magic is deliberately unchanged;
//! no additional envelope version or per-chunk marker is introduced.
//!
//! # File layout
//!
//! ```text
//! ZSTD | one continuous zstd frame | chunk map | 48-byte footer
//!        <--- compressed payload -->
//!
//! chunk map:
//!   chunk_size:    u32       (1 MiB)
//!   payload_bytes: u64       (compressed frame length, excluding ZSTD)
//!   chunk_hashes:  [u8; 32][] (BLAKE3 of each consecutive payload chunk)
//!
//! footer:
//!   map_offset: u64          (absolute file offset, including ZSTD)
//!   map_length: u64          (map bytes only, excluding footer)
//!   map_digest: [u8; 32]
//! ```
//!
//! All integers are little-endian. Each chunk is exactly 1 MiB except possibly
//! the last. The map contains at most 65,536 hashes, bounding map allocation and
//! the compressed payload to 64 GiB; callers may impose stricter blob limits.
//! Chunks are NOT separate zstd frames and have no interleaved headers. Thus the
//! compressor receives its original continuous input and chunking does not alter
//! the compressed frame. Publication hashes output incrementally, then appends
//! the map and footer: no seek, payload rewrite or second scan on fresh publication.
//!
//! # ETag and HTTP metadata
//!
//! `map_digest = BLAKE3(ZSTD || map || map_offset_le || map_length_le)`.
//! The strong ETag is this digest encoded as quoted lowercase hex. The digest
//! commits to the map, which commits to payload bytes and their order; it is not
//! the direct hash of the whole file and does not include its own footer field.
//! HTTP reads the footer to obtain the ETag and the
//! `snapshot-chunk-map-offset` / `snapshot-chunk-map-length` headers without
//! reading the map or payload. No per-blob metadata sidecar is required.
//!
//! # Verification levels and resumption
//!
//! These operations deliberately offer different guarantees:
//!
//! - `read_footer` reads magic/footer and checks file/map bounds. It is suitable
//!   for serving locally published immutable files, NOT validating remote input.
//! - `verify_map` additionally checks bounded map structure and its digest, but
//!   does NOT read the payload or establish that its bytes match the hashes.
//! - `verify_file` checks every compressed chunk plus the complete map/footer.
//!   The downloader uses it to recheck existing cache files. Fresh transfers
//!   verify each chunk before writing/advancing their saved prefix, avoiding a
//!   separate full-file verification pass before staged semantic import.
//! - `PayloadReader` hashes bytes consumed by the zstd decoder (not read-ahead
//!   bytes) and checks the trailer after the single frame ends. This covers
//!   streaming/local decoding and primary import as well.
//!   Its `new_preverified` mode skips payload hashing for immutable files already
//!   checked by the downloader, but still checks frame boundaries and the trailer.
//!
//! The partition downloader retains only complete payload chunks after a failed
//! transfer. Byte offsets are `4 + n * chunk_size`, never offsets in decoded data
//! or in the map/footer. Resume requires known map bounds and a strong unchanged
//! ETag, sent as `If-Range`. Missing bounds/validator force a restart; a response
//! with a different ETag is rejected without mixing it into the saved prefix.
//! Start performs HEAD, fetches/verifies the bounded map/footer with a range GET,
//! then streams a payload range. Resume already knows the map and requests only
//! the remaining payload, checking the response ETag before accepting bytes.
//! Incomplete/unverified chunk tails are never reused. The coordinator keeps
//! separate resume files per ETag and records suppliers of verified chunks.
//! Different providers may offer different valid wire representations; changing
//! a provider's own ETag or violating a map/chunk bans it for that snapshot.
//! Runtime's pre-format-dispatch primary/legacy download still uses its existing
//! restart-on-retry transport; the partitioned coordinator can also cold-download
//! a primary using the same chunk-verified worker as other partitions.
//!
//! Chunk checks detect corruption, but are NOT consensus authentication: a source
//! can generate a self-consistent envelope around arbitrary data. Existing
//! primary proofs, expected Merkle roots, logical-codec validation and staged
//! publication remain necessary; this envelope does not replace any of them.
//!
//! # Local garbage collection
//!
//! Snapshot records retain primary/partition membership and protect files needed
//! by retained snapshots or active publications. GC also recognizes orphaned
//! regular 64-hex files through `verify_map`, without reading their payloads.
//! Recognized unreferenced blobs and their discovery headers can be reclaimed;
//! legacy files, symlinks and malformed unknown envelopes are not adopted by
//! this scan. Snapshot records remain until their owned files are durably swept.
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use serde::Deserialize;
use serde::Serialize;

pub const MAGIC: &[u8; 4] = b"ZSTD";
pub const CHUNK_BYTES: usize = 1024 * 1024;
pub const MAX_CHUNKS: usize = 65_536;
pub const FOOTER_BYTES: usize = 48;
const MAP_HEADER_BYTES: usize = 12;
const MAX_MAP_BYTES: usize = MAP_HEADER_BYTES + 32 * MAX_CHUNKS;
pub const MAP_OFFSET_HEADER: &str = "snapshot-chunk-map-offset";
pub const MAP_LENGTH_HEADER: &str = "snapshot-chunk-map-length";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkMapMetadata {
    pub offset: u64,
    pub length: u64,
    pub digest: [u8; 32],
}

impl ChunkMapMetadata {
    pub fn etag(&self) -> String {
        format!("\"{}\"", hex::encode(self.digest))
    }
}

#[derive(Default)]
struct Hashes {
    bytes: u64,
    current: blake3::Hasher,
    completed: Vec<[u8; 32]>,
}

impl Hashes {
    fn update(&mut self, mut bytes: &[u8]) -> std::io::Result<()> {
        while !bytes.is_empty() {
            if self.bytes >= (CHUNK_BYTES * MAX_CHUNKS) as u64 {
                return Err(std::io::Error::other("snapshot chunk count limit exceeded"));
            }
            let count = bytes.len().min(CHUNK_BYTES - (self.bytes % CHUNK_BYTES as u64) as usize);
            self.current.update(&bytes[..count]);
            self.bytes += count as u64;
            bytes = &bytes[count..];
            if self.bytes.is_multiple_of(CHUNK_BYTES as u64) {
                self.completed.push(*self.current.finalize().as_bytes());
                self.current = blake3::Hasher::new();
            }
        }
        Ok(())
    }

    fn finish(mut self) -> (u64, Vec<[u8; 32]>) {
        if !self.bytes.is_multiple_of(CHUNK_BYTES as u64) {
            self.completed.push(*self.current.finalize().as_bytes());
        }
        (self.bytes, self.completed)
    }
}

fn metadata(map: &[u8], offset: u64) -> ChunkMapMetadata {
    let mut hash = blake3::Hasher::new();
    hash.update(MAGIC);
    hash.update(map);
    hash.update(&offset.to_le_bytes());
    hash.update(&(map.len() as u64).to_le_bytes());
    ChunkMapMetadata { offset, length: map.len() as u64, digest: *hash.finalize().as_bytes() }
}

pub struct ChunkWriter<W> {
    inner: W,
    hashes: Hashes,
}

impl<W: Write> ChunkWriter<W> {
    pub fn new(mut inner: W) -> std::io::Result<Self> {
        inner.write_all(MAGIC)?;
        Ok(Self { inner, hashes: Hashes::default() })
    }

    pub fn finish(mut self) -> anyhow::Result<(W, ChunkMapMetadata)> {
        let (bytes, hashes) = self.hashes.finish();
        anyhow::ensure!(bytes > 0, "empty snapshot compressed payload");
        let mut map = Vec::with_capacity(MAP_HEADER_BYTES + hashes.len() * 32);
        map.extend_from_slice(&(CHUNK_BYTES as u32).to_le_bytes());
        map.extend_from_slice(&bytes.to_le_bytes());
        for hash in hashes {
            map.extend_from_slice(&hash);
        }
        let metadata = metadata(&map, MAGIC.len() as u64 + bytes);
        self.inner.write_all(&map)?;
        self.inner.write_all(&metadata.offset.to_le_bytes())?;
        self.inner.write_all(&metadata.length.to_le_bytes())?;
        self.inner.write_all(&metadata.digest)?;
        self.inner.flush()?;
        Ok((self.inner, metadata))
    }
}

impl<W: Write> Write for ChunkWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let remaining = (CHUNK_BYTES * MAX_CHUNKS) as u64 - self.hashes.bytes;
        if bytes.len() as u64 > remaining {
            return Err(std::io::Error::other("snapshot chunk count limit exceeded"));
        }
        let count = self.inner.write(bytes)?;
        self.hashes.update(&bytes[..count])?;
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn check_trailer(trailer: &[u8], hashes: Hashes) -> anyhow::Result<ChunkMapMetadata> {
    let (bytes, hashes) = hashes.finish();
    check_trailer_contents(trailer, bytes, Some(hashes))
}

fn check_trailer_contents(
    trailer: &[u8],
    bytes: u64,
    hashes: Option<Vec<[u8; 32]>>,
) -> anyhow::Result<ChunkMapMetadata> {
    anyhow::ensure!(trailer.len() >= MAP_HEADER_BYTES + FOOTER_BYTES, "truncated chunk map");
    let footer = trailer.len() - FOOTER_BYTES;
    let offset = u64::from_le_bytes(trailer[footer..footer + 8].try_into()?);
    let length = u64::from_le_bytes(trailer[footer + 8..footer + 16].try_into()?);
    anyhow::ensure!(length == footer as u64, "chunk map length mismatch");
    let map = &trailer[..footer];
    anyhow::ensure!(
        u32::from_le_bytes(map[..4].try_into()?) as usize == CHUNK_BYTES,
        "invalid chunk size"
    );
    let payload_bytes = u64::from_le_bytes(map[4..12].try_into()?);
    anyhow::ensure!(bytes > 0 && bytes == payload_bytes, "chunk payload size mismatch");
    anyhow::ensure!(offset == MAGIC.len() as u64 + bytes, "chunk map offset mismatch");
    anyhow::ensure!(
        map.len() == MAP_HEADER_BYTES + bytes.div_ceil(CHUNK_BYTES as u64) as usize * 32,
        "chunk count mismatch"
    );
    if let Some(hashes) = hashes {
        for (index, (actual, expected)) in
            hashes.iter().zip(map[12..].as_chunks::<32>().0).enumerate()
        {
            anyhow::ensure!(actual.as_slice() == expected, "snapshot chunk {index} hash mismatch");
        }
    }
    let metadata = metadata(map, offset);
    anyhow::ensure!(trailer[footer + 16..] == metadata.digest, "chunk map digest mismatch");
    Ok(metadata)
}

/// Hash only bytes actually consumed by the zstd decoder, not read-ahead bytes.
pub struct PayloadReader<R> {
    inner: BufReader<R>,
    hashes: Hashes,
    verify: bool,
    preverified: bool,
}

impl<R: Read> PayloadReader<R> {
    pub fn new(mut inner: R) -> anyhow::Result<Self> {
        let mut magic = [0; 4];
        inner.read_exact(&mut magic)?;
        anyhow::ensure!(&magic == MAGIC, "invalid compressed snapshot envelope");
        Ok(Self::after_magic(inner))
    }

    pub fn after_magic(inner: R) -> Self {
        Self {
            inner: BufReader::new(inner),
            hashes: Hashes::default(),
            verify: true,
            preverified: false,
        }
    }

    /// Only for immutable files whose complete chunks and map were verified by
    /// the downloader (including cache hits). Count consumed bytes instead of
    /// hashing them again; retain envelope limits and exact frame/trailer checks.
    pub fn new_preverified(inner: R) -> anyhow::Result<Self> {
        let mut reader = Self::new(inner)?;
        reader.verify = false;
        reader.preverified = true;
        Ok(reader)
    }

    /// Legacy dispatch may discover a pre-partition codec after reading a marker.
    /// Do not impose the new envelope's transport limits on that old stream.
    pub fn disable_verification(&mut self) {
        self.verify = false;
        self.preverified = false;
    }

    pub fn verify_trailer(mut self) -> anyhow::Result<ChunkMapMetadata> {
        let mut trailer = Vec::new();
        self.inner
            .by_ref()
            .take((MAX_MAP_BYTES + FOOTER_BYTES + 1) as u64)
            .read_to_end(&mut trailer)?;
        anyhow::ensure!(trailer.len() <= MAX_MAP_BYTES + FOOTER_BYTES, "chunk map budget exceeded");
        if self.preverified {
            check_trailer_contents(&trailer, self.hashes.bytes, None)
        } else {
            check_trailer(&trailer, self.hashes)
        }
    }
}

impl<R: Read> Read for PayloadReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        let count = self.inner.read(bytes)?;
        if self.verify {
            self.hashes.update(&bytes[..count])?;
        } else if self.preverified {
            self.hashes.bytes += count as u64;
            if self.hashes.bytes > (CHUNK_BYTES * MAX_CHUNKS) as u64 {
                return Err(std::io::Error::other("snapshot chunk count limit exceeded"));
            }
        }
        Ok(count)
    }
}

impl<R: Read> BufRead for PayloadReader<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        if !self.verify && !self.preverified {
            return self.inner.fill_buf();
        }
        // Leave limit enforcement in fallible Read/fill_buf rather than consume.
        let remaining = (CHUNK_BYTES * MAX_CHUNKS) as u64 - self.hashes.bytes;
        let bytes = self.inner.fill_buf()?;
        if remaining == 0 && !bytes.is_empty() {
            return Err(std::io::Error::other("snapshot chunk count limit exceeded"));
        }
        Ok(&bytes[..bytes.len().min(remaining as usize)])
    }

    fn consume(&mut self, count: usize) {
        // fill_buf limits consumption to the validated capacity above.
        if self.verify {
            self.hashes.update(&self.inner.buffer()[..count]).expect("bounded chunk consumption");
        } else if self.preverified {
            self.hashes.bytes += count as u64;
        }
        self.inner.consume(count);
    }
}

/// Read serving metadata without reading/hashing the payload or chunk map.
/// Only use this on locally published immutable files, not for import validation.
pub fn read_footer<R: Read + Seek>(input: &mut R) -> anyhow::Result<ChunkMapMetadata> {
    input.rewind()?;
    let mut magic = [0; 4];
    input.read_exact(&mut magic)?;
    anyhow::ensure!(&magic == MAGIC, "invalid compressed snapshot envelope");
    let size = input.seek(SeekFrom::End(0))?;
    anyhow::ensure!(size >= (4 + MAP_HEADER_BYTES + FOOTER_BYTES) as u64, "truncated chunked blob");
    input.seek(SeekFrom::End(-(FOOTER_BYTES as i64)))?;
    let mut footer = [0; FOOTER_BYTES];
    input.read_exact(&mut footer)?;
    let offset = u64::from_le_bytes(footer[..8].try_into()?);
    let length = u64::from_le_bytes(footer[8..16].try_into()?);
    anyhow::ensure!(
        offset > 4
            && (MAP_HEADER_BYTES as u64..=MAX_MAP_BYTES as u64).contains(&length)
            && offset.checked_add(length).and_then(|n| n.checked_add(FOOTER_BYTES as u64))
                == Some(size),
        "invalid chunk map bounds"
    );
    input.rewind()?;
    Ok(ChunkMapMetadata { offset, length, digest: footer[16..].try_into()? })
}

/// Recognize the chunk envelope by validating map structure and its footer digest.
/// This does not read/verify the payload and is not sufficient for import.
pub fn verify_map<R: Read + Seek>(input: &mut R) -> anyhow::Result<ChunkMapMetadata> {
    let footer = read_footer(input)?;
    input.seek(SeekFrom::Start(footer.offset))?;
    let mut map = vec![0; footer.length as usize];
    input.read_exact(&mut map)?;
    anyhow::ensure!(
        u32::from_le_bytes(map[..4].try_into()?) as usize == CHUNK_BYTES,
        "invalid chunk size"
    );
    let bytes = u64::from_le_bytes(map[4..12].try_into()?);
    anyhow::ensure!(
        bytes > 0
            && bytes <= (CHUNK_BYTES * MAX_CHUNKS) as u64
            && bytes + MAGIC.len() as u64 == footer.offset
            && map.len() == MAP_HEADER_BYTES + bytes.div_ceil(CHUNK_BYTES as u64) as usize * 32,
        "invalid chunk map structure"
    );
    anyhow::ensure!(metadata(&map, footer.offset) == footer, "chunk map digest mismatch");
    input.rewind()?;
    Ok(footer)
}

/// Verify a downloaded file before any import side effects. Buffering is bounded.
pub fn verify_file<R: Read + Seek>(
    input: &mut R,
    should_cancel: &impl Fn() -> bool,
) -> anyhow::Result<ChunkMapMetadata> {
    let size = input.seek(SeekFrom::End(0))?;
    anyhow::ensure!(
        size >= (MAGIC.len() + MAP_HEADER_BYTES + FOOTER_BYTES) as u64,
        "truncated chunked blob"
    );
    input.seek(SeekFrom::End(-(FOOTER_BYTES as i64)))?;
    let mut footer = [0; FOOTER_BYTES];
    input.read_exact(&mut footer)?;
    let offset = u64::from_le_bytes(footer[..8].try_into()?);
    let length = u64::from_le_bytes(footer[8..16].try_into()?);
    anyhow::ensure!(
        length <= MAX_MAP_BYTES as u64 && length >= MAP_HEADER_BYTES as u64,
        "chunk map budget exceeded"
    );
    anyhow::ensure!(
        offset >= MAGIC.len() as u64
            && offset.checked_add(length).and_then(|n| n.checked_add(FOOTER_BYTES as u64))
                == Some(size),
        "invalid chunk map bounds"
    );
    input.rewind()?;
    let mut magic = [0; 4];
    input.read_exact(&mut magic)?;
    anyhow::ensure!(&magic == MAGIC, "invalid compressed snapshot envelope");
    let mut hashes = Hashes::default();
    let mut remaining = offset - MAGIC.len() as u64;
    let mut buffer = vec![0; 64 * 1024];
    while remaining != 0 {
        anyhow::ensure!(!should_cancel(), "snapshot chunk verification cancelled");
        let count = remaining.min(buffer.len() as u64) as usize;
        input.read_exact(&mut buffer[..count])?;
        hashes.update(&buffer[..count])?;
        remaining -= count as u64;
    }
    let mut trailer = vec![0; length as usize + FOOTER_BYTES];
    input.read_exact(&mut trailer)?;
    let result = check_trailer(&trailer, hashes)?;
    input.rewind()?;
    Ok(result)
}

#[cfg(test)]
mod tests;
