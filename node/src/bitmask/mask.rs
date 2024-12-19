// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::ops::BitAnd;

use serde::Deserialize;
use serde::Serialize;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder, Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct Bitmask<TBitsSource> {
    mask_bits: TBitsSource,
    meaningful_mask_bits: TBitsSource,
}

impl<TBitsSource> Bitmask<TBitsSource>
where
    // Note: Change to std::num::Zero once it's released
    TBitsSource: Clone + PartialEq<TBitsSource> + std::default::Default,
    for<'a> &'a TBitsSource: std::ops::BitAnd<Output = TBitsSource>,
    for<'a> <&'a TBitsSource as BitAnd>::Output: PartialEq<<&'a TBitsSource as BitAnd>::Output>,
{
    pub fn is_match(&self, account_address: &TBitsSource) -> bool {
        let mask = &self.mask_bits & &self.meaningful_mask_bits;
        let meaningful_value = account_address & &self.meaningful_mask_bits;
        meaningful_value == mask
    }

    pub fn mask_bits(&self) -> &TBitsSource {
        &self.mask_bits
    }

    pub fn meaningful_mask_bits(&self) -> &TBitsSource {
        &self.meaningful_mask_bits
    }
}

#[cfg(test)]
#[allow(clippy::unusual_byte_groupings)]
mod tests {
    use std::fmt;

    use super::*;

    // This is an inefficient implementation therefore
    // can not be used outside of tests.
    impl<TBitsSource> fmt::Display for Bitmask<TBitsSource>
    where
        TBitsSource: fmt::Display + fmt::Binary,
        TBitsSource: Clone
            + std::ops::BitAnd<Output = TBitsSource>
            + PartialEq<TBitsSource>
            + std::default::Default
            + std::ops::BitXor<Output = TBitsSource>
            + std::ops::Not<Output = TBitsSource>
            + std::ops::BitOr<Output = TBitsSource>
            + std::ops::Shr<usize, Output = TBitsSource>,
        <TBitsSource as BitAnd>::Output: std::ops::BitXor,
        <<TBitsSource as std::ops::BitAnd>::Output as std::ops::BitXor>::Output:
            PartialEq<TBitsSource>,
        for<'a> &'a TBitsSource: std::ops::BitAnd<Output = TBitsSource>,
        for<'a> <&'a TBitsSource as BitAnd>::Output: PartialEq<<&'a TBitsSource as BitAnd>::Output>,
    {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let bits_count: usize = std::mem::size_of::<TBitsSource>() * 8;
            let mut mask: String = String::default();
            let zero = TBitsSource::default();
            let mut bit = !((!zero.clone()) >> 1);
            for _ in 0..bits_count {
                if (self.meaningful_mask_bits.clone() & bit.clone()) != zero {
                    mask += if (self.mask_bits.clone() & bit.clone()) != zero { "1" } else { "0" };
                } else {
                    mask += "*";
                }
                bit = bit >> 1;
            }
            write!(f, "<mask:{}>", mask)
        }
    }

    #[test]
    fn default_mask_matches_any() {
        let mask = Bitmask::<u64>::default();
        assert!(mask.is_match(&0u64));
        assert!(mask.is_match(&!0u64));
        assert!(mask.is_match(&123u64));
    }

    #[test]
    fn full_match_returns_true() {
        let some_account_address = 10012u16;
        let mask = Bitmask::<u16>::builder()
            .meaningful_mask_bits(!0u16)
            .mask_bits(some_account_address)
            .build();
        assert!(mask.is_match(&some_account_address));
    }

    #[test]
    fn single_non_matching_bit_fails_match() {
        let some_account_address = 0b01010101_u8;
        let single_bit_difference_account_address = 0b01000101_u8;
        let mask = Bitmask::<u8>::builder()
            .meaningful_mask_bits(!0u8)
            .mask_bits(some_account_address)
            .build();
        assert!(
            !mask.is_match(&single_bit_difference_account_address),
            "Mask {mask} matched wrong address: {single_bit_difference_account_address:#b}"
        );
    }

    #[test]
    fn meaningless_non_matching_bits_are_ignored_in_matches() {
        let some_account_address = 0b01_01_0101_u8;
        let meaningful_mask_bits = 0b11_00_1111_u8;
        let two_meaningles_bit_difference_account_address = 0b01_10_0101_u8;
        let mask = Bitmask::<u8>::builder()
            .meaningful_mask_bits(meaningful_mask_bits)
            .mask_bits(some_account_address)
            .build();
        assert!(
            mask.is_match(&two_meaningles_bit_difference_account_address),
            "Mask {mask} failed to match an address: {two_meaningles_bit_difference_account_address:08b}."
        );
    }
}
