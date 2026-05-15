use node_types::AccountRouting;

use crate::bitmask::mask::Bitmask;

impl thread_load_balance::AsPath for Bitmask<AccountRouting> {
    type Unit = bool;

    fn len(&self) -> usize {
        // Find first meaningless bit.
        let mut result = 0usize;
        let bytes = self.meaningful_mask_bits().dapp_id().as_slice();
        for value in bytes {
            if *value != u8::MAX {
                result += value.leading_ones() as usize;
                break;
            }
            result += 8;
        }
        result
    }

    fn to_vec(&self) -> Vec<Self::Unit> {
        let n: usize = self.len();
        let mut result = Vec::<Self::Unit>::with_capacity(n);
        for i in 0..n {
            result.push(self.index(i));
        }
        result
    }

    fn index(&self, i: usize) -> Self::Unit {
        let is_bit_set = self.meaningful_mask_bits().dapp_id().get_bit(i);
        let bit_value = self.mask_bits().dapp_id().get_bit(i);
        assert!(is_bit_set);
        bit_value
    }
}

impl std::convert::From<&[bool]> for Bitmask<AccountRouting> {
    fn from(value: &[bool]) -> Self {
        assert!(value.len() <= 255);
        let mut mask = Bitmask::<AccountRouting>::default();
        for (i, bit_value) in value.iter().enumerate() {
            if *bit_value {
                mask.mask_bits_mut().set_bit(i);
            }
            mask.meaningful_mask_bits_mut().set_bit(i);
        }
        mask
    }
}

#[cfg(test)]
mod tests {
    use thread_load_balance::AsPath;

    use super::*;

    #[test]
    fn ensure_mask_len_is_correct() {
        let bool_mask = vec![true, true, true, true, false, false, true, true, false, false];
        let mask = Bitmask::<AccountRouting>::from(bool_mask.as_slice());
        assert_eq!(mask.len(), 10);
    }

    #[test]
    fn ensure_mask_converted_into_vec_and_restored_from_it_is_the_same() {
        let bool_mask = vec![true, true, true, true, false, false, true, true, false, false];
        let mask = Bitmask::<AccountRouting>::from(bool_mask.as_slice());
        let converted_mask = mask.to_vec();
        assert!(converted_mask == bool_mask);
    }

    #[test]
    fn ensure_mask_index_access_works() {
        let bool_mask = vec![true, true, true, true, false, false, true, true, false, false];
        let mask = Bitmask::<AccountRouting>::from(bool_mask.as_slice());
        for (i, flag) in bool_mask.iter().enumerate() {
            assert!(*flag == mask.index(i));
        }
    }

    #[test]
    #[should_panic]
    fn ensure_accessing_non_existing_index_panics() {
        let bool_mask = vec![true, true, true, true, false, false, true, true, false, false];
        let mask = Bitmask::<AccountRouting>::from(bool_mask.as_slice());
        mask.index(bool_mask.len());
    }

    #[test]
    fn ensure_empty_path_is_the_default_mask() {
        let default_mask = Bitmask::<AccountRouting>::default();
        let empty_path: Vec<bool> = vec![];
        let empty_path_mask = Bitmask::<AccountRouting>::from(empty_path.as_slice());
        assert!(default_mask == empty_path_mask);
    }
}
