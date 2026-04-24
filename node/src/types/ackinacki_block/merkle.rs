// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use sha2::Digest;
use sha2::Sha256;

pub type MerkleHash = [u8; 32];

/// Build the root of a Merkle tree from a set of leaf hashes.
/// If the number of nodes is odd, the last one is duplicated.
pub fn merkle_root(leaf_hashes: &[MerkleHash]) -> MerkleHash {
    if leaf_hashes.is_empty() {
        return [0u8; 32];
    }
    if leaf_hashes.len() == 1 {
        return leaf_hashes[0];
    }

    let mut current_level: Vec<MerkleHash> = leaf_hashes.to_vec();

    while current_level.len() > 1 {
        let mut next_level = Vec::with_capacity(current_level.len().div_ceil(2));
        for chunk in current_level.chunks(2) {
            let mut hasher = Sha256::new();
            hasher.update(chunk[0]);
            if chunk.len() == 2 {
                hasher.update(chunk[1]);
            } else {
                hasher.update(chunk[0]); // duplicate last
            }
            next_level.push(hasher.finalize().into());
        }
        current_level = next_level;
    }

    current_level[0]
}

/// Compute the hash of a single leaf from arbitrary data.
pub fn leaf_hash(data: &[u8]) -> MerkleHash {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_merkle_root() {
        let root = merkle_root(&[]);
        assert_eq!(root, [0u8; 32]);
    }

    #[test]
    fn test_single_leaf() {
        let leaf = leaf_hash(b"hello");
        let root = merkle_root(&[leaf]);
        assert_eq!(root, leaf);
    }

    #[test]
    fn test_two_leaves() {
        let a = leaf_hash(b"a");
        let b = leaf_hash(b"b");
        let root = merkle_root(&[a, b]);

        // Manually compute expected root
        let mut hasher = Sha256::new();
        hasher.update(a);
        hasher.update(b);
        let expected: MerkleHash = hasher.finalize().into();
        assert_eq!(root, expected);
    }

    #[test]
    fn test_four_leaves() {
        let leaves: Vec<MerkleHash> = (0..4u8).map(|i| leaf_hash(&[i])).collect();
        let root = merkle_root(&leaves);

        // Level 1: hash(leaf0, leaf1), hash(leaf2, leaf3)
        let mut h01 = Sha256::new();
        h01.update(leaves[0]);
        h01.update(leaves[1]);
        let n01: MerkleHash = h01.finalize().into();

        let mut h23 = Sha256::new();
        h23.update(leaves[2]);
        h23.update(leaves[3]);
        let n23: MerkleHash = h23.finalize().into();

        // Level 2: hash(n01, n23)
        let mut h_root = Sha256::new();
        h_root.update(n01);
        h_root.update(n23);
        let expected: MerkleHash = h_root.finalize().into();

        assert_eq!(root, expected);
    }

    #[test]
    fn test_three_leaves_odd_duplication() {
        let leaves: Vec<MerkleHash> = (0..3u8).map(|i| leaf_hash(&[i])).collect();
        let root = merkle_root(&leaves);

        // Level 1: hash(leaf0, leaf1), hash(leaf2, leaf2)
        let mut h01 = Sha256::new();
        h01.update(leaves[0]);
        h01.update(leaves[1]);
        let n01: MerkleHash = h01.finalize().into();

        let mut h22 = Sha256::new();
        h22.update(leaves[2]);
        h22.update(leaves[2]); // duplicated
        let n22: MerkleHash = h22.finalize().into();

        let mut h_root = Sha256::new();
        h_root.update(n01);
        h_root.update(n22);
        let expected: MerkleHash = h_root.finalize().into();

        assert_eq!(root, expected);
    }

    #[test]
    fn test_deterministic() {
        let leaves: Vec<MerkleHash> = (0..4u8).map(|i| leaf_hash(&[i])).collect();
        let root1 = merkle_root(&leaves);
        let root2 = merkle_root(&leaves);
        assert_eq!(root1, root2);
    }
}
