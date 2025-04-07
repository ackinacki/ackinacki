use std::fmt::Debug;
use std::fmt::{self};
use std::ops::Deref;
use std::sync::Arc;

use crate::types::AccountAddress;

#[derive(Clone)]
pub struct OrderSet(Arc<indexset::BTreeSet<AccountAddress>>);

impl Default for OrderSet {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderSet {
    pub fn new() -> Self {
        Self(Arc::new(indexset::BTreeSet::new()))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get_index(&self, index: usize) -> Option<&AccountAddress> {
        self.0.get_index(index)
    }

    pub fn contains(&self, account_address: &AccountAddress) -> bool {
        self.0.contains(account_address)
    }

    pub fn insert(&mut self, account_address: AccountAddress) {
        Arc::make_mut(&mut self.0).insert(account_address);
    }

    pub fn remove(&mut self, account_address: &AccountAddress) {
        Arc::make_mut(&mut self.0).remove(account_address);
    }

    pub fn to_set(&self) -> indexset::BTreeSet<AccountAddress> {
        self.0.deref().clone()
    }
}

impl serde::Serialize for OrderSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let data: Vec<_> = self.0.iter().cloned().collect();
        data.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for OrderSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = Vec::<AccountAddress>::deserialize(deserializer)?;
        Ok(OrderSet(Arc::new(indexset::BTreeSet::from_iter(data))))
    }
}

impl Debug for OrderSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("OrderSet")
            .field(&self.0) // Теперь выводится содержимое
            .finish()
    }
}
