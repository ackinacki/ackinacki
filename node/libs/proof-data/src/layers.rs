// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! `history_proofs` blob: `n: u8 || n * (layer: u8, root: [u8; 32])`.

use crate::FormatError;

/// Number of blocks (or lower-layer roots) per layer window.
pub const WINDOW: u64 = 128;
/// Upper bound on the number of layers in one blob.
pub const MAX_LAYERS: u8 = 10;

pub type LayerRoots = Vec<(u8, [u8; 32])>;

/// Number of layers a block at `height` carries: the largest `n` with
/// `height % 128^n == 0`; 0 for height 0.
pub fn required_layers(height: u64) -> u8 {
    if height == 0 {
        return 0;
    }
    // 128 = 2^7: the number of layers is the count of trailing zero bits divided by 7.
    (height.trailing_zeros() / 7) as u8
}

/// Parses a blob as written by the node. `[0x00]` and an empty blob mean
/// "no layers". Layers are returned in blob order.
pub fn parse_history_proofs(blob: &[u8]) -> Result<LayerRoots, FormatError> {
    let Some((&n, rest)) = blob.split_first() else {
        return Ok(vec![]);
    };
    if n > MAX_LAYERS {
        return Err(FormatError::TooManyLayers(n));
    }
    let expected_len = n as usize * 33;
    if rest.len() < expected_len {
        return Err(FormatError::Truncated);
    }
    if rest.len() > expected_len {
        return Err(FormatError::TrailingBytes);
    }
    let (chunks, _) = rest.as_chunks::<33>();
    Ok(chunks
        .iter()
        .map(|chunk| {
            let mut root = [0u8; 32];
            root.copy_from_slice(&chunk[1..]);
            (chunk[0], root)
        })
        .collect())
}

/// Parses the blob and checks that it holds exactly layers `1..=required_layers(height)`.
/// `None` is the same as an empty blob. Returns the roots sorted by layer.
pub fn check_history_proofs_for_height(
    height: u64,
    blob: Option<&[u8]>,
) -> Result<LayerRoots, FormatError> {
    let mut roots = parse_history_proofs(blob.unwrap_or_default())?;
    roots.sort_by_key(|(layer, _)| *layer);
    if roots.iter().enumerate().any(|(i, (layer, _))| *layer as usize != i + 1) {
        return Err(FormatError::NonContiguousLayers);
    }
    let expected = required_layers(height);
    if roots.len() != expected as usize {
        return Err(FormatError::LayerSetMismatch {
            height,
            expected,
            found: roots.iter().map(|(layer, _)| *layer).collect(),
        });
    }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(layers: &[u8]) -> Vec<u8> {
        let mut buf = vec![layers.len() as u8];
        for layer in layers {
            buf.push(*layer);
            buf.extend_from_slice(&[*layer; 32]);
        }
        buf
    }

    #[test]
    fn required_layers_by_height() {
        assert_eq!(required_layers(0), 0);
        assert_eq!(required_layers(127), 0);
        assert_eq!(required_layers(128), 1);
        assert_eq!(required_layers(16_384), 2);
        assert_eq!(required_layers(2_097_152), 3);
        assert_eq!(required_layers(16_512), 1);
        assert_eq!(required_layers(1 << 63), 9);
    }

    #[test]
    fn zero_layer_blob_is_no_layers() {
        assert_eq!(parse_history_proofs(&[0x00]), Ok(vec![]));
        assert_eq!(parse_history_proofs(&[]), Ok(vec![]));
        assert_eq!(check_history_proofs_for_height(129, Some(&[0x00])), Ok(vec![]));
        assert_eq!(check_history_proofs_for_height(129, None), Ok(vec![]));
    }

    #[test]
    fn parses_and_sorts_layers() {
        assert_eq!(parse_history_proofs(&blob(&[2, 1])), Ok(vec![(2, [2; 32]), (1, [1; 32])]));
        assert_eq!(
            check_history_proofs_for_height(16_384, Some(&blob(&[2, 1]))),
            Ok(vec![(1, [1; 32]), (2, [2; 32])])
        );
    }

    #[test]
    fn check_rejects_wrong_layer_sets() {
        assert_eq!(
            check_history_proofs_for_height(16_384, Some(&blob(&[1]))),
            Err(FormatError::LayerSetMismatch { height: 16_384, expected: 2, found: vec![1] })
        );
        assert_eq!(
            check_history_proofs_for_height(129, Some(&blob(&[1]))),
            Err(FormatError::LayerSetMismatch { height: 129, expected: 0, found: vec![1] })
        );
        assert_eq!(
            check_history_proofs_for_height(2_097_152, Some(&blob(&[1, 3]))),
            Err(FormatError::NonContiguousLayers)
        );
        assert_eq!(
            check_history_proofs_for_height(256, Some(&blob(&[1, 1]))),
            Err(FormatError::NonContiguousLayers)
        );
        assert_eq!(
            check_history_proofs_for_height(128, None),
            Err(FormatError::LayerSetMismatch { height: 128, expected: 1, found: vec![] })
        );
        let mut truncated = blob(&[1]);
        truncated.pop();
        assert_eq!(parse_history_proofs(&truncated), Err(FormatError::Truncated));
        let mut trailing = blob(&[1]);
        trailing.push(0);
        assert_eq!(parse_history_proofs(&trailing), Err(FormatError::TrailingBytes));
        assert_eq!(parse_history_proofs(&[11]), Err(FormatError::TooManyLayers(11)));
    }
}
