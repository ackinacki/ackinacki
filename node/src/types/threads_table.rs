use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::types::account_address::direct_bit_access_operations::DirectBitAccess;
use crate::types::AccountAddress;
use crate::types::DAppIdentifier;
use crate::types::ThreadIdentifier;

#[derive(Clone, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AccountRouting(pub DAppIdentifier, pub AccountAddress);

impl Debug for AccountRouting {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}:{:?}", self.0, self.1)
    }
}
impl From<(Option<DAppIdentifier>, AccountAddress)> for AccountRouting {
    fn from((dapp, acc): (Option<DAppIdentifier>, AccountAddress)) -> Self {
        let dapp = dapp.unwrap_or(DAppIdentifier(acc.clone()));
        Self(dapp, acc)
    }
}

pub type ThreadsTable = crate::bitmask::table::BitmasksTable<AccountRouting, ThreadIdentifier>;

impl ThreadsTable {
    pub fn merge(&mut self, _another_table: &ThreadsTable) -> anyhow::Result<()> {
        // todo!()
        Ok(())
    }

    pub fn list_threads(&self) -> impl Iterator<Item = &'_ ThreadIdentifier> {
        self.rows().map(|(_, thread)| thread).collect::<HashSet<_>>().into_iter()
    }
}

impl std::ops::BitAnd for &'_ AccountRouting {
    type Output = AccountRouting;

    fn bitand(self, rhs: Self) -> Self::Output {
        AccountRouting(&self.0 & &rhs.0, &self.1 & &rhs.1)
    }
}

impl DirectBitAccess for AccountRouting {
    fn get_bit_value(&self, index: usize) -> bool {
        if index < 256 {
            self.0 .0.get_bit_value(index)
        } else {
            self.1.get_bit_value(index - 256)
        }
    }

    fn set_bit_value(&mut self, index: usize, value: bool) {
        if index < 256 {
            self.0 .0.set_bit_value(index, value);
        } else {
            self.1.set_bit_value(index - 256, value);
        }
    }
}

impl std::convert::From<AccountRouting> for [[bool; 256]; 2] {
    fn from(val: AccountRouting) -> Self {
        let mut result = [[false; 256]; 2];
        let parts: [Vec<u8>; 2] = [val.0 .0 .0.get_bytestring(0), val.1 .0.get_bytestring(0)];
        for outer in 0..parts.len() {
            for inner in 0..parts[outer].len() {
                for shift in 0..8 {
                    let bits = 1 << shift;
                    result[outer][inner * 8 + (7 - shift)] = ((parts[outer][inner]) & bits) != 0;
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmask::mask::Bitmask;
    use crate::types::BlockIdentifier;

    #[test]
    fn threads_table_is_serializable() {
        let mut threads_table = ThreadsTable::new();
        let any_bitmask = Bitmask::<AccountRouting>::builder()
            .meaningful_mask_bits(AccountRouting::default())
            .mask_bits(AccountRouting::default())
            .build();
        threads_table
            .insert_above(0, any_bitmask, ThreadIdentifier::new(&BlockIdentifier::default(), 1))
            .unwrap();
        let serialized_threads_table = serde_json::to_string(&threads_table).unwrap();

        let restored_table: ThreadsTable = serde_json::from_str(&serialized_threads_table).unwrap();

        assert_eq!(threads_table, restored_table);
    }
}
