use blake3::Hasher;

use crate::Blake3Hashable;

/// H(0x00) — empty leaf (currently unused; empty is NODE_ID_NONE)
pub fn hash_leaf_empty() -> [u8; 32] {
    let mut h = Hasher::new();
    h.update(&[0x00]);
    *h.finalize().as_bytes()
}

/// H(0x01 || value) — leaf depends only on value
#[inline(always)]
pub fn hash_leaf_value<V: Blake3Hashable>(value: &V) -> [u8; 32] {
    value.hash(0x01)
}

/// Sparse branch hash:
/// H(0x02 || bitmap_le(u16) || repeated (nibble(u8) || child_hash([u8;32])) in nibble order)
pub fn hash_branch_sparse(bitmap: u16, nibs: &[u8], child_hashes: &[[u8; 32]]) -> [u8; 32] {
    debug_assert_eq!(nibs.len(), child_hashes.len());

    let mut h = Hasher::new();
    h.update(&[0x02]);
    h.update(&bitmap.to_le_bytes());

    for i in 0..nibs.len() {
        h.update(&[nibs[i]]);
        h.update(&child_hashes[i]);
    }

    *h.finalize().as_bytes()
}

/// Empty branch (bitmap=0)
pub fn hash_branch_empty() -> [u8; 32] {
    hash_branch_sparse(0, &[], &[])
}

/// Extension node hash:
/// H(0x03 || len(u8) || packed_nibbles || child_hash)
///
/// Packed_nibbles contains 2 nibbles per byte (high then low), length = ceil(len/2).
pub fn hash_ext(len: u8, packed_nibbles: &[u8], child_hash: &[u8; 32]) -> [u8; 32] {
    debug_assert_eq!(packed_nibbles.len(), (len as usize).div_ceil(2));

    let mut h = Hasher::new();
    h.update(&[0x03]);
    h.update(&[len]);
    h.update(packed_nibbles);
    h.update(child_hash);

    *h.finalize().as_bytes()
}
