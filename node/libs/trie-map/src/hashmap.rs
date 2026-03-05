use std::collections::HashMap;
use std::sync::Arc;

use crate::MapHash;
use crate::MapKey;
use crate::MapKeyPath;
use crate::MapRepository;
use crate::MapValue;

#[derive(Clone, Default)]
pub struct TrieHashMap<V: MapValue> {
    maps: Arc<parking_lot::RwLock<Vec<HashMap<MapKey, V>>>>,
}

#[derive(Clone, Copy)]
pub struct TrieHashMapRef {
    index: usize,
    key_path: MapKeyPath,
    hash: MapHash,
}

impl<V: MapValue> TrieHashMap<V> {
    pub fn new() -> Self {
        Self { maps: Arc::new(parking_lot::RwLock::new(vec![HashMap::new()])) }
    }
}

impl<V: MapValue> MapRepository for TrieHashMap<V> {
    type MapRef = TrieHashMapRef;
    type Value = V;

    fn new_map() -> Self::MapRef {
        Self::MapRef { index: 0, key_path: MapKeyPath::default(), hash: MapHash([0; 32]) }
    }

    fn map_hash(&self, map: &Self::MapRef) -> MapHash {
        map.hash
    }

    fn map_key_path(&self, map: &Self::MapRef) -> MapKeyPath {
        map.key_path
    }

    fn map_get(&self, map: &Self::MapRef, key: &MapKey) -> Option<Self::Value> {
        self.maps.read()[map.index].get(key).copied()
    }

    fn map_update(
        &self,
        map: &Self::MapRef,
        updates: &[(MapKey, Option<Self::Value>)],
    ) -> Self::MapRef {
        let mut maps = self.maps.write();
        let mut map = maps[map.index].clone();
        for (key, val) in updates.iter() {
            if let Some(val) = val {
                map.insert(*key, *val);
            } else {
                map.remove(key);
            }
        }
        maps.push(map);
        Self::MapRef {
            index: maps.len() - 1,
            key_path: MapKeyPath::default(),
            hash: MapHash([0; 32]),
        }
    }

    fn map_split(
        &self,
        _map: &Self::MapRef,
        _key_path: MapKeyPath,
    ) -> (Self::MapRef, Self::MapRef) {
        todo!()
    }

    fn merge(&self, _a: &Self::MapRef, _b: &Self::MapRef) -> Self::MapRef {
        todo!()
    }
}
