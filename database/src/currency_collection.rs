// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;

use num_bigint::BigInt;
use num_bigint::Sign;
use serde::Deserialize;
use serde::Serialize;
use tvm_block::CurrencyCollection;
use tvm_block::Deserializable;
use tvm_block::VarUInteger32;
use tvm_types::HashmapType;

pub(crate) struct SignedCurrencyCollection {
    pub grams: BigInt,
    pub other: HashMap<u32, BigInt>,
}

/// A single extra currency amount of a signed collection.
///
/// `ExtraCurrencyCollection` — what `messages.value_other` stores — holds
/// `VarUInteger32` amounts and so cannot express an outgoing, i.e. negative,
/// amount. Transaction deltas go both ways, hence this separate representation
/// with the amount kept as a signed decimal string: the same radix `gql-server`
/// parses `OtherCurrency.value` from.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct SignedCurrency {
    pub currency: u32,
    pub value: String,
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

    /// Extra currencies as a JSON array ordered by currency id, ready to be
    /// stored in a `*_other` column of the `transactions` table.
    ///
    /// Amounts that netted out to zero are dropped, so a transaction that only
    /// forwarded the extra currencies it received stores `None` instead of a
    /// row of zeroes.
    pub fn other_to_json(&self) -> Option<String> {
        let mut currencies = self
            .other
            .iter()
            .filter(|(_, value)| value.sign() != Sign::NoSign)
            .map(|(&currency, value)| SignedCurrency { currency, value: value.to_string() })
            .collect::<Vec<_>>();

        if currencies.is_empty() {
            return None;
        }

        currencies.sort_unstable_by_key(|currency| currency.currency);
        serde_json::to_string(&currencies).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collection(amounts: &[(u32, i64)]) -> SignedCurrencyCollection {
        let mut collection = SignedCurrencyCollection::new();
        collection.other = amounts.iter().map(|&(key, value)| (key, BigInt::from(value))).collect();
        collection
    }

    #[test]
    fn other_to_json_keeps_the_sign_and_orders_by_currency() {
        let json = collection(&[(7, -300), (1, 100)]).other_to_json();
        assert_eq!(
            json.as_deref(),
            Some(r#"[{"currency":1,"value":"100"},{"currency":7,"value":"-300"}]"#)
        );

        let decoded: Vec<SignedCurrency> = serde_json::from_str(&json.unwrap()).unwrap();
        assert_eq!(decoded[1], SignedCurrency { currency: 7, value: "-300".to_string() });
    }

    #[test]
    fn other_to_json_drops_zero_amounts() {
        assert_eq!(collection(&[(1, 0)]).other_to_json(), None);
        assert_eq!(collection(&[]).other_to_json(), None);
        assert_eq!(
            collection(&[(1, 0), (2, 5)]).other_to_json().as_deref(),
            Some(r#"[{"currency":2,"value":"5"}]"#)
        );
    }

    #[test]
    fn add_and_sub_net_out_the_forwarded_amount() {
        let mut delta = SignedCurrencyCollection::new();
        delta.add(&collection(&[(1, 100), (2, 7)]));
        delta.sub(&collection(&[(1, 100)]));

        assert_eq!(delta.other_to_json().as_deref(), Some(r#"[{"currency":2,"value":"7"}]"#));
    }
}
