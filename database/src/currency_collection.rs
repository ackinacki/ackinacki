// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use num_bigint::BigInt;
use tvm_block::CurrencyCollection;
use tvm_block::Deserializable;
use tvm_block::VarUInteger32;
use tvm_types::HashmapType;

pub(crate) struct SignedCurrencyCollection {
    pub grams: BigInt,
    pub other: HashMap<u32, BigInt>,
}

impl SignedCurrencyCollection {
    pub fn new() -> Self {
        SignedCurrencyCollection { grams: BigInt::default(), other: HashMap::new() }
    }

    pub fn from_cc(cc: &CurrencyCollection) -> tvm_types::Result<Self> {
        let mut other = HashMap::new();

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

        for (&key, other_val) in &other.other {
            self.other
                .entry(key)
                .and_modify(|v| *v += other_val)
                .or_insert_with(|| other_val.clone());
        }
    }

    pub fn sub(&mut self, other: &Self) {
        self.grams -= &other.grams;

        for (&key, other_val) in &other.other {
            self.other
                .entry(key)
                .and_modify(|v| *v -= other_val)
                .or_insert_with(|| -other_val.clone());
        }
    }
}
