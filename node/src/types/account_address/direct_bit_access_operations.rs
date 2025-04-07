use super::AccountAddress;

// Note: this interface can panic with "index out of bounds"
pub trait DirectBitAccess {
    fn get_bit_value(&self, index: usize) -> bool;
    fn set_bit_value(&mut self, index: usize, value: bool);
}

impl DirectBitAccess for AccountAddress {
    fn get_bit_value(&self, index: usize) -> bool {
        self.0.get_bit_opt(index).expect("index out of bounds")
    }

    // TODO: value assumed to be always true, can be removed from arg
    fn set_bit_value(&mut self, index: usize, value: bool) {
        assert!(value, "set_bit_value: value must be true");
        if index >= self.0.remaining_bits() {
            panic!("index out of bounds");
        } else {
            let mut buffer: [u8; 32] = self
                .0
                .get_bytestring(0)
                .try_into()
                .expect("Account address must be an uint256 value");
            let hi = index / 8usize;
            let lo = 7 - index % 8usize;
            buffer[hi] |= 1 << lo;
            self.0 = tvm_types::AccountId::from(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_must_set_bit_values() {
        let mut addr = AccountAddress::default();
        addr.set_bit_value(101, true);
        assert!(!addr.get_bit_value(0));
        assert!(!addr.get_bit_value(255));
        assert!(!addr.get_bit_value(100));
        assert!(addr.get_bit_value(101));
    }

    #[test]
    #[should_panic]
    fn it_must_panic_when_set_out_of_bounds() {
        let mut addr = AccountAddress::default();
        addr.set_bit_value(256, true);
    }

    #[test]
    #[should_panic]
    fn it_must_panic_when_read_out_of_bounds() {
        let addr = AccountAddress::default();
        let _ = addr.get_bit_value(256);
    }

    #[test]
    fn test_set_bit_value() {
        let mut address = AccountAddress::default();
        address.set_bit_value(255, true);
        assert!(address.get_bit_value(255));
        address.set_bit_value(254, true);
        assert!(address.get_bit_value(254));
        assert!(address.get_bit_value(255));
    }
}
