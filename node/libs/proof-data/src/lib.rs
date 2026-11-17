// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! Formats of the proof data that the node writes into blocks and that the
//! history proofs store, block-manager and gql-server read back: history
//! proof layer roots, tracked ext-out message hashes, block Merkle leaves and
//! the heights a layer tree is built from. No hashing beyond SHA-256.

use std::fmt;

pub mod layer_tree;
pub mod layers;
pub mod leaves;
pub mod merkle;
pub mod tracked;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    /// The blob ends before a declared item.
    Truncated,
    /// The blob has bytes after the last declared item.
    TrailingBytes,
    /// More layers than any block can carry.
    TooManyLayers(u8),
    /// Layers are not exactly 1, 2, ..., k (a gap or a duplicate).
    NonContiguousLayers,
    /// The layer set does not match the block height.
    LayerSetMismatch { height: u64, expected: u8, found: Vec<u8> },
    /// A fixed-size field has a wrong length.
    BadLength { field: &'static str, expected: usize, got: usize },
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FormatError::Truncated => write!(f, "blob is truncated"),
            FormatError::TrailingBytes => write!(f, "blob has trailing bytes"),
            FormatError::TooManyLayers(n) => write!(f, "too many history proof layers: {n}"),
            FormatError::NonContiguousLayers => {
                write!(f, "history proof layers are not 1..k without gaps or duplicates")
            }
            FormatError::LayerSetMismatch { height, expected, found } => write!(
                f,
                "height {height} needs history proof layers 1..={expected}, found {found:?}"
            ),
            FormatError::BadLength { field, expected, got } => {
                write!(f, "{field} must be {expected} bytes, got {got}")
            }
        }
    }
}

impl std::error::Error for FormatError {}
