pub mod durable;
pub mod hashmap;
pub mod trie;

use node_types::Blake3Hashable;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
pub use trie::smt::TrieMapRepository;
pub use trie::smt::TrieMapSnapshot;

extern crate alloc;
extern crate core;

pub trait MapValue:
    Blake3Hashable + Copy + Clone + Send + Sync + Serialize + DeserializeOwned
{
}

impl<T> MapValue for T where
    T: Blake3Hashable + Copy + Clone + Send + Sync + Serialize + DeserializeOwned
{
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MapHash(pub [u8; 32]);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MapKey(pub [u8; 32]);

/// Prefix view: `len` is number of HIGH BITS fixed in `prefix`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MapKeyPath {
    pub prefix: MapKey,
    pub len: u8,
}

impl Default for MapKeyPath {
    fn default() -> Self {
        Self { prefix: MapKey([0; 32]), len: 0 }
    }
}

pub trait MapRepository: Clone + Send + Sync {
    type MapRef: Clone + Copy + Send + Sync;
    type Value: MapValue;

    fn new_map() -> Self::MapRef;

    fn map_hash(&self, map: &Self::MapRef) -> MapHash;
    fn map_key_path(&self, map: &Self::MapRef) -> MapKeyPath;

    fn map_get(&self, map: &Self::MapRef, key: &MapKey) -> Option<Self::Value>;

    fn map_batch_get(&self, map: &Self::MapRef, keys: &[MapKey]) -> Vec<Option<Self::Value>> {
        keys.iter().map(|k| self.map_get(map, k)).collect()
    }

    fn map_update(
        &self,
        map: &Self::MapRef,
        updates: &[(MapKey, Option<Self::Value>)],
    ) -> Self::MapRef;

    fn map_split(&self, map: &Self::MapRef, key_path: MapKeyPath) -> (Self::MapRef, Self::MapRef);

    fn merge(&self, a: &Self::MapRef, b: &Self::MapRef) -> Self::MapRef;
}
