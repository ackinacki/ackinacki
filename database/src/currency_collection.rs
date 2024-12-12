// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
use tvm_block::CurrencyCollection;
use tvm_block::Deserializable;
use tvm_block::VarUInteger32;
use tvm_types::HashmapType;

pub(crate) struct SignedCurrencyCollection {
    pub grams: num_bigint::BigInt,
    pub other: std::collections::HashMap<u32, num_bigint::BigInt>,
}

impl SignedCurrencyCollection {
    pub fn new() -> Self {
        SignedCurrencyCollection { grams: 0.into(), other: std::collections::HashMap::new() }
    }

    pub fn from_cc(cc: &CurrencyCollection) -> tvm_types::Result<Self> {
        let mut other = std::collections::HashMap::new();
        cc.other_as_hashmap().iterate_slices(
            |ref mut key, ref mut value| -> tvm_types::Result<bool> {
                let key = key.get_next_u32()?;
                let value = VarUInteger32::construct_from(value)?;
                other.insert(key, value.value().clone());
                Ok(true)
            },
        )?;

        Ok(SignedCurrencyCollection { grams: cc.grams.as_u128().into(), other })
    }

    pub fn add(&mut self, other: &Self) {
        self.grams += &other.grams;
        for (key, value) in self.other.iter_mut() {
            if let Some(other_value) = other.other.get(key) {
                *value += other_value;
            }
        }
        for (key, value) in other.other.iter() {
            if !self.other.contains_key(key) {
                self.other.insert(*key, value.clone());
            }
        }
    }

    pub fn sub(&mut self, other: &Self) {
        self.grams -= &other.grams;
        for (key, value) in self.other.iter_mut() {
            if let Some(other_value) = other.other.get(key) {
                *value -= other_value;
            }
        }
        for (key, value) in other.other.iter() {
            if !self.other.contains_key(key) {
                self.other.insert(*key, -value.clone());
            }
        }
    }
}
