// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! Heights a layer-`n` tree at boundary height `T` is built from.
//!
//! The tree has 130 leaves: leaf 0 is the last L(n+1) root strictly below `T`
//! (zero when there is none), leaf 1 is the Ln root at `T - 128^n` (zero when
//! `T == 128^n`), then 128 data leaves.

use std::fmt;

use crate::layers::MAX_LAYERS;
use crate::layers::WINDOW;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerTreeSpec {
    pub height: u64,
    pub layer: u8,
    /// Height whose L(n+1) root is leaf 0; `None` for a zero leaf.
    pub leaf0: Option<u64>,
    /// Height whose Ln root is leaf 1; `None` for a zero leaf.
    pub leaf1: Option<u64>,
    /// Heights of the 128 data leaves: block heights for n = 1, L(n-1) boundary heights for n >= 2.
    pub data: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayerTreeArgError {
    LayerOutOfRange(u8),
    HeightZero,
    NotABoundary { height: u64, layer: u8 },
}

impl fmt::Display for LayerTreeArgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LayerTreeArgError::LayerOutOfRange(layer) => {
                write!(f, "layer {layer} is out of range 1..=10")
            }
            LayerTreeArgError::HeightZero => write!(f, "height must be positive"),
            LayerTreeArgError::NotABoundary { height, layer } => {
                write!(f, "height {height} is not a layer {layer} boundary")
            }
        }
    }
}

impl std::error::Error for LayerTreeArgError {}

impl LayerTreeSpec {
    pub fn new(height: u64, layer: u8) -> Result<LayerTreeSpec, LayerTreeArgError> {
        if !(1..=MAX_LAYERS).contains(&layer) {
            return Err(LayerTreeArgError::LayerOutOfRange(layer));
        }
        if height == 0 {
            return Err(LayerTreeArgError::HeightZero);
        }
        let not_a_boundary = LayerTreeArgError::NotABoundary { height, layer };
        let span = WINDOW.checked_pow(layer as u32).ok_or(not_a_boundary.clone())?;
        if !height.is_multiple_of(span) {
            return Err(not_a_boundary);
        }
        let leaf0 = match WINDOW.checked_pow(layer as u32 + 1) {
            Some(upper) if height > upper => Some((height - 1) / upper * upper),
            _ => None,
        };
        let leaf1 = (height != span).then(|| height - span);
        // span / 128 = 128^(n-1): one block for n = 1, one L(n-1) window for n >= 2.
        let step = span / WINDOW;
        let data = if layer == 1 {
            (height - WINDOW..height).collect()
        } else {
            (0..WINDOW).rev().map(|i| height - i * step).collect()
        };
        Ok(LayerTreeSpec { height, layer, leaf0, leaf1, data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_n1_first_l2_boundary() {
        let spec = LayerTreeSpec::new(16_384, 1).unwrap();
        assert_eq!(spec.leaf0, None);
        assert_eq!(spec.leaf1, Some(16_256));
        assert_eq!(spec.data, (16_256..16_384).collect::<Vec<u64>>());
    }

    #[test]
    fn spec_n2_at_l3_boundary() {
        let spec = LayerTreeSpec::new(2_097_152, 2).unwrap();
        assert_eq!(spec.leaf0, None);
        assert_eq!(spec.leaf1, Some(2_080_768));
        assert_eq!(spec.data.first(), Some(&2_080_896));
        assert_eq!(spec.data.last(), Some(&2_097_152));
        assert_eq!(spec.data.len(), 128);
        assert!(spec.data.windows(2).all(|w| w[1] - w[0] == 128));
    }

    #[test]
    fn spec_first_window() {
        let spec = LayerTreeSpec::new(128, 1).unwrap();
        assert_eq!(spec.leaf0, None);
        assert_eq!(spec.leaf1, None);
        assert_eq!(spec.data[0], 0);
        assert_eq!(spec.data.len(), 128);
    }

    #[test]
    fn spec_leaf0_present() {
        assert_eq!(LayerTreeSpec::new(32_768, 1).unwrap().leaf0, Some(16_384));
        assert_eq!(LayerTreeSpec::new(16_512, 1).unwrap().leaf0, Some(16_384));
        assert_eq!(LayerTreeSpec::new(4_194_304, 2).unwrap().leaf0, Some(2_097_152));
    }

    #[test]
    fn spec_n9_no_overflow() {
        let spec = LayerTreeSpec::new(128u64.pow(9), 9).unwrap();
        assert_eq!(spec.leaf0, None);
        assert_eq!(spec.leaf1, None);
        assert_eq!(spec.data.len(), 128);
        assert_eq!(spec.data.last(), Some(&128u64.pow(9)));
    }

    #[test]
    fn spec_n10_is_argument_error() {
        let height = u64::MAX - (u64::MAX % 128);
        assert!(matches!(
            LayerTreeSpec::new(height, 10),
            Err(LayerTreeArgError::NotABoundary { .. })
        ));
    }

    #[test]
    fn spec_rejects() {
        assert_eq!(LayerTreeSpec::new(0, 1), Err(LayerTreeArgError::HeightZero));
        assert_eq!(
            LayerTreeSpec::new(130, 1),
            Err(LayerTreeArgError::NotABoundary { height: 130, layer: 1 })
        );
        assert_eq!(LayerTreeSpec::new(128, 0), Err(LayerTreeArgError::LayerOutOfRange(0)));
        assert_eq!(LayerTreeSpec::new(128, 11), Err(LayerTreeArgError::LayerOutOfRange(11)));
    }
}
