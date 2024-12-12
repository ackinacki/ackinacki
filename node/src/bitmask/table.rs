// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use anyhow::ensure;
use serde::Deserialize;
use serde::Serialize;

use super::mask::Bitmask;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BitmasksTable<TBitsSource, TTarget> {
    masks: Vec<(Bitmask<TBitsSource>, TTarget)>,
}

impl<TBitsSource, TTarget> Default for BitmasksTable<TBitsSource, TTarget>
where
    TTarget: Default + Clone + std::fmt::Debug + PartialEq,
    TBitsSource: Clone
        + std::ops::BitAnd<Output = TBitsSource>
        + PartialEq<TBitsSource>
        + std::default::Default
        + std::fmt::Debug,
    <TBitsSource as std::ops::BitAnd>::Output: PartialEq<<TBitsSource as std::ops::BitAnd>::Output>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<TBitsSource, TTarget> BitmasksTable<TBitsSource, TTarget>
where
    TTarget: Default + Clone + std::fmt::Debug + PartialEq,
    TBitsSource: Clone
        + std::ops::BitAnd<Output = TBitsSource>
        + PartialEq<TBitsSource>
        + std::default::Default
        + std::fmt::Debug,
    <TBitsSource as std::ops::BitAnd>::Output: PartialEq<<TBitsSource as std::ops::BitAnd>::Output>,
{
    pub fn rows(&self) -> impl Iterator<Item = &'_ (Bitmask<TBitsSource>, TTarget)> {
        self.masks.iter()
    }

    /// Scans the table to find first matching target
    pub fn find_match(&self, bits: TBitsSource) -> TTarget {
        for (mask, target) in self.rows() {
            if mask.is_match(bits.clone()) {
                return target.clone();
            }
        }
        panic!(
            "This should never happen. Somehow the bitmask table did not have a defult rule that matches all or the default rule did not match the value. Mask: {:?}, value: {:?}",
            &self,
            bits
        );
    }

    pub fn is_match(&self, bits: TBitsSource, target: TTarget) -> bool {
        self.find_match(bits) == target
    }

    // This method inserts a new rule into the table and points it to the thread specified.
    pub fn insert_above(
        &mut self,
        row_index: usize,
        mask: Bitmask<TBitsSource>,
        thread: TTarget,
    ) -> anyhow::Result<()> {
        ensure!(
            row_index != self.masks.len(),
            "It is not allowed to insert below the default row."
        );
        ensure!(row_index < self.masks.len(), "row_index is outside of the table size.");
        self.masks.insert(row_index, (mask, thread));
        Ok(())
    }

    pub fn remove(&mut self, row_index: usize) -> anyhow::Result<(Bitmask<TBitsSource>, TTarget)> {
        ensure!(
            (row_index + 1usize) != self.masks.len(),
            "It is not allowed to remove the default row"
        );
        ensure!((row_index + 1usize) < self.masks.len(), "row_index is out of bounds.");
        Ok(self.masks.remove(row_index))
    }

    pub fn new() -> Self {
        Self { masks: vec![(Bitmask::<TBitsSource>::default(), TTarget::default())] }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thread_split_mask_adding_is_very_specific() {
        // Note: this test mostly exists to visually check how readable api is.
        let mut threads_table = BitmasksTable::<u16, u8>::new();
        threads_table
            .insert_above(
                0,
                Bitmask::<u16>::builder()
                    .meaningful_mask_bits(!0u16)
                    .mask_bits(0b0100_0000_0000_0000_u16)
                    .build(),
                1,
            )
            .unwrap();
        let mut threads: Vec<u8> = vec![];
        for (mask, thread_id) in threads_table.rows() {
            let _ = mask.is_match(1u16);
            threads.push(*thread_id);
        }
        assert_eq!(vec![1u8, 0u8], threads);
    }

    #[test]
    fn new_table_already_has_a_default_rule() {
        let threads_table = BitmasksTable::<u16, u8>::new();
        let rows: Vec<(Bitmask<u16>, u8)> = threads_table.rows().cloned().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, Bitmask::<u16>::default());
        assert_eq!(rows[0].1, 0u8);
    }

    #[test]
    fn it_does_not_allow_to_remove_default_rule() {
        let mut threads_table = BitmasksTable::<u16, u8>::new();
        threads_table.remove(0).expect_err("Must not allow to remove the default rule");
    }

    #[test]
    fn it_does_not_allow_to_remove_default_rule_even_other_rules_exist() {
        let mut threads_table = BitmasksTable::<u16, u8>::new();
        threads_table
            .insert_above(
                0,
                Bitmask::<u16>::builder()
                    .meaningful_mask_bits(!0u16)
                    .mask_bits(0b0100_0000_0000_0000_u16)
                    .build(),
                1,
            )
            .unwrap();
        threads_table.remove(1).expect_err("Must not allow to remove the default rule");
    }

    #[test]
    fn it_allows_to_remove_any_nondefault_rule() {
        let mut threads_table = BitmasksTable::<u16, u8>::new();
        threads_table
            .insert_above(
                0,
                Bitmask::<u16>::builder()
                    .meaningful_mask_bits(!0u16)
                    .mask_bits(0b0100_0000_0000_0000_u16)
                    .build(),
                1,
            )
            .unwrap();
        let _ = threads_table.remove(0).ok();
        let rows: Vec<(Bitmask<u16>, u8)> = threads_table.rows().cloned().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, Bitmask::<u16>::default());
        assert_eq!(rows[0].1, 0u8);
    }

    #[test]
    fn it_finds_the_top_matching_rule_first() {
        let mut threads_table = BitmasksTable::<u8, u8>::new();
        const IRRELEVANT_TOPMOST_RULE_TARGET: u8 = 3;
        const FIRST_MATCHING_RULE_TARGET: u8 = 1;
        const SECOND_MATCHING_RULE_TARGET: u8 = 2;
        // Note: inserting to the top in the reverse order
        threads_table
            .insert_above(
                0,
                Bitmask::<u8>::builder()
                    .meaningful_mask_bits(0b1100_0000u8)
                    .mask_bits(0b1000_0000u8)
                    .build(),
                SECOND_MATCHING_RULE_TARGET,
            )
            .unwrap();
        threads_table
            .insert_above(
                0,
                Bitmask::<u8>::builder()
                    .meaningful_mask_bits(0b1100_0000u8)
                    .mask_bits(0b1000_0000u8)
                    .build(),
                FIRST_MATCHING_RULE_TARGET,
            )
            .unwrap();
        threads_table
            .insert_above(
                0,
                Bitmask::<u8>::builder()
                    .meaningful_mask_bits(0b1100_0000u8)
                    .mask_bits(0b1100_0000u8)
                    .build(),
                IRRELEVANT_TOPMOST_RULE_TARGET,
            )
            .unwrap();
        // -- end of setup --
        assert_eq!(FIRST_MATCHING_RULE_TARGET, threads_table.find_match(0b1011_0001u8));
    }
}
