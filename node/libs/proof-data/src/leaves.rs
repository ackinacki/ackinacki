// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! `block_merkle_leaves` blob: 16 leaves of 32 bytes, 512 bytes in total.

use crate::merkle::MerkleHash;
use crate::merkle::BLOCK_MERKLE_LEAF_COUNT;
use crate::FormatError;

pub fn parse_block_merkle_leaves(
    blob: &[u8],
) -> Result<[MerkleHash; BLOCK_MERKLE_LEAF_COUNT], FormatError> {
    let expected = BLOCK_MERKLE_LEAF_COUNT * 32;
    if blob.len() != expected {
        return Err(FormatError::BadLength {
            field: "block_merkle_leaves",
            expected,
            got: blob.len(),
        });
    }
    let mut leaves = [[0u8; 32]; BLOCK_MERKLE_LEAF_COUNT];
    let (chunks, _) = blob.as_chunks::<32>();
    leaves.copy_from_slice(chunks);
    Ok(leaves)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_512_bytes() {
        let blob: Vec<u8> = (0..512).map(|i| (i / 32) as u8).collect();
        let leaves = parse_block_merkle_leaves(&blob).unwrap();
        for (i, leaf) in leaves.iter().enumerate() {
            assert_eq!(leaf, &[i as u8; 32]);
        }
    }

    #[test]
    fn rejects_other_lengths() {
        assert_eq!(
            parse_block_merkle_leaves(&[0; 511]),
            Err(FormatError::BadLength { field: "block_merkle_leaves", expected: 512, got: 511 })
        );
    }
}
