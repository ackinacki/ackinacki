use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::types::AccountAddress;
use crate::types::DAppIdentifier;
use crate::types::ThreadIdentifier;

#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AccountRouting(pub DAppIdentifier, pub AccountAddress);

impl Debug for AccountRouting {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}:{:?}", self.0, self.1)
    }
}

pub type ThreadsTable = crate::bitmask::table::BitmasksTable<AccountRouting, ThreadIdentifier>;

impl ThreadsTable {
    pub fn merge(&mut self, _another_table: &ThreadsTable) -> anyhow::Result<()> {
        // todo!()
        Ok(())
    }

    pub fn list_threads(&self) -> impl Iterator<Item = &'_ ThreadIdentifier> {
        self.rows().map(|(_, thread)| thread)
    }
}

impl std::ops::BitAnd for AccountRouting {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self(self.0 & rhs.0, self.1 & rhs.1)
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
