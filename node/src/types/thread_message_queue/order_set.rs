use std::fmt;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use node_types::AccountRouting;
use node_types::DAppIdentifier;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

pub type OrderSet = OrderedSet<AccountRouting>;
pub type OrderSetDappIdentifier = OrderedSet<DAppIdentifier>;

#[derive(Clone)]
pub struct OrderedSet<T>(Arc<indexset::BTreeSet<T>>)
where
    T: Ord + Clone;

impl<T> Default for OrderedSet<T>
where
    T: Ord + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> OrderedSet<T>
where
    T: Ord + Clone,
{
    pub fn new() -> Self {
        Self(Arc::new(indexset::BTreeSet::new()))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get_index(&self, index: usize) -> Option<&T> {
        self.0.get_index(index)
    }

    pub fn contains(&self, item: &T) -> bool {
        self.0.contains(item)
    }

    pub fn insert(&mut self, item: T) {
        Arc::make_mut(&mut self.0).insert(item);
    }

    pub fn remove(&mut self, item: &T) {
        Arc::make_mut(&mut self.0).remove(item);
    }

    pub fn to_set(&self) -> indexset::BTreeSet<T> {
        self.0.deref().clone()
    }
}

impl<T> Serialize for OrderedSet<T>
where
    T: Ord + Clone + Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let data: Vec<_> = self.0.iter().cloned().collect();
        data.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for OrderedSet<T>
where
    T: Ord + Clone + DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = Vec::<T>::deserialize(deserializer)?;
        Ok(Self(Arc::new(indexset::BTreeSet::from_iter(data))))
    }
}

impl<T> Debug for OrderedSet<T>
where
    T: Ord + Clone + Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("OrderedSet").field(&self.0).finish()
    }
}
