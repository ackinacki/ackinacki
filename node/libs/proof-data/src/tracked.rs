// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! `tracked_ext_out_message_hashes` blob: `groups: u32 BE`, then per group
//! `dapp_id: [u8; 32] || account_id: [u8; 32] || count: u32 BE || count * [u8; 32]`.

use crate::FormatError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedGroup {
    pub dapp_id: [u8; 32],
    pub account_id: [u8; 32],
    pub hashes: Vec<[u8; 32]>,
}

/// Decodes the blob. An empty blob means "no groups".
pub fn decode_tracked_hashes(blob: &[u8]) -> Result<Vec<TrackedGroup>, FormatError> {
    if blob.is_empty() {
        return Ok(vec![]);
    }
    let mut reader = Reader(blob);
    let group_count = reader.u32()?;
    let mut groups = Vec::new();
    for _ in 0..group_count {
        let dapp_id = reader.hash()?;
        let account_id = reader.hash()?;
        let hash_count = reader.u32()?;
        let mut hashes = Vec::new();
        for _ in 0..hash_count {
            hashes.push(reader.hash()?);
        }
        groups.push(TrackedGroup { dapp_id, account_id, hashes });
    }
    if !reader.0.is_empty() {
        return Err(FormatError::TrailingBytes);
    }
    Ok(groups)
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], FormatError> {
        if self.0.len() < N {
            return Err(FormatError::Truncated);
        }
        let (head, rest) = self.0.split_at(N);
        self.0 = rest;
        let mut out = [0u8; N];
        out.copy_from_slice(head);
        Ok(out)
    }

    fn u32(&mut self) -> Result<u32, FormatError> {
        self.take::<4>().map(u32::from_be_bytes)
    }

    fn hash(&mut self) -> Result<[u8; 32], FormatError> {
        self.take::<32>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Same layout as `encode_tracked_ext_out_message_hashes` in node/src/database/serialize_block.rs.
    fn encode(groups: &[TrackedGroup]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(groups.len() as u32).to_be_bytes());
        for group in groups {
            buf.extend_from_slice(&group.dapp_id);
            buf.extend_from_slice(&group.account_id);
            buf.extend_from_slice(&(group.hashes.len() as u32).to_be_bytes());
            for hash in &group.hashes {
                buf.extend_from_slice(hash);
            }
        }
        buf
    }

    fn groups() -> Vec<TrackedGroup> {
        vec![
            TrackedGroup {
                dapp_id: [0; 32],
                account_id: [0x1a; 32],
                hashes: vec![[1; 32], [2; 32]],
            },
            TrackedGroup { dapp_id: [4; 32], account_id: [0x10; 32], hashes: vec![[3; 32]] },
        ]
    }

    #[test]
    fn round_trip_two_groups() {
        assert_eq!(decode_tracked_hashes(&encode(&groups())), Ok(groups()));
        assert_eq!(decode_tracked_hashes(&encode(&[])), Ok(vec![]));
        assert_eq!(decode_tracked_hashes(&[]), Ok(vec![]));
    }

    #[test]
    fn trailing_byte_is_an_error() {
        let mut blob = encode(&groups());
        blob.push(0);
        assert_eq!(decode_tracked_hashes(&blob), Err(FormatError::TrailingBytes));
    }

    #[test]
    fn short_blob_is_truncated() {
        let mut blob = encode(&groups());
        blob.pop();
        assert_eq!(decode_tracked_hashes(&blob), Err(FormatError::Truncated));
        assert_eq!(decode_tracked_hashes(&[0, 0, 1]), Err(FormatError::Truncated));
        // A huge declared count must not allocate, only fail.
        assert_eq!(decode_tracked_hashes(&[0xff, 0xff, 0xff, 0xff]), Err(FormatError::Truncated));
    }
}
