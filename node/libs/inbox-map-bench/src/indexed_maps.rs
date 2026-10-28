use std::fmt::Debug;

use multi_map::MapHash;
use multi_map::MapKey;
use multi_map::MapKeyPath;
use multi_map::MultiMapAug;
use multi_map::MultiMapValue;
use node_types::AccountRouting;
use node_types::DAppIdentifier;
use serde::ser::SerializeSeq;
use serde::Deserialize;
use serde::Serialize;

use super::order_set::OrderedSet;

pub trait QueueMapKey: Copy + Ord {
    fn merkle_key(&self) -> MapKey;

    fn same_scope(&self, _other: &Self) -> bool {
        true
    }
}

impl QueueMapKey for DAppIdentifier {
    fn merkle_key(&self) -> MapKey {
        MapKey(*self.as_array())
    }
}

impl QueueMapKey for AccountRouting {
    fn merkle_key(&self) -> MapKey {
        MapKey(*self.account_id().as_array())
    }

    fn same_scope(&self, other: &Self) -> bool {
        self.dapp_id() == other.dapp_id()
    }
}

/// Persistent Merkle storage plus a non-committed index for round-robin scheduling.
/// L2 stores account IDs as Merkle keys; the routing index belongs to one DApp.
#[derive(Clone, Debug)]
pub struct QueueMap<K: QueueMapKey, V: MultiMapValue> {
    map: MultiMapAug<V, ()>,
    order: OrderedSet<K>,
}

impl<K: QueueMapKey, V: MultiMapValue> QueueMap<K, V> {
    pub fn new() -> Self {
        Self { map: MultiMapAug::new(), order: OrderedSet::new() }
    }

    pub fn root_hash(&self) -> MapHash {
        MapHash(self.map.root.hash())
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        if !self.order.contains(key) {
            return None;
        }
        multi_map::ops::get_leaf(&self.map.root, &self.map.root_path, &key.merkle_key())
            .map(|leaf| &leaf.value)
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.order.iter()
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.map.iter().map(|(_, value)| value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.keys().zip(self.values())
    }

    pub(crate) fn get_index(&self, index: usize) -> Option<&K> {
        self.order.get_index(index)
    }

    pub(crate) fn update_batch(&mut self, updates: Vec<(K, Option<V>)>) {
        for (key, value) in &updates {
            self.update_order(key, value.is_some());
        }
        let updates: Vec<_> =
            updates.into_iter().map(|(key, value)| (key.merkle_key(), value)).collect();

        self.map = match updates.as_slice() {
            [] => return,
            [(key, value)] => self.map.update_single(key, value.clone()),
            _ => self.map.update(&updates),
        };
    }

    pub(crate) fn update_single(&mut self, key: K, value: Option<V>) {
        self.update_order(&key, value.is_some());
        self.map = self.map.update_single(&key.merkle_key(), value);
    }

    fn update_order(&mut self, key: &K, present: bool) {
        if present {
            assert!(self.keys().next().is_none_or(|first| first.same_scope(key)));
            if !self.order.contains(key) {
                self.order.insert(*key);
            }
        } else if self.order.contains(key) {
            self.order.remove(key);
        }
    }

    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
        let previous = self.get(&key).cloned();
        self.update_single(key, Some(value));
        previous
    }

    /// Split a prefix, keeping both results rooted at the full key space.
    pub(crate) fn split(&self, prefix: MapKeyPath) -> (Self, Self) {
        assert!(prefix.len <= 256);

        let (without, subtree) = self.map.split(prefix);
        let mut rest = Self { map: without, order: OrderedSet::new() };
        let mut part = Self { map: (MultiMapAug::new()).graft(&subtree), order: OrderedSet::new() };
        for key in self.keys() {
            if multi_map::ops::prefix_bits_match(&prefix.prefix.0, &key.merkle_key().0, prefix.len)
            {
                part.order.insert(*key);
            } else {
                rest.order.insert(*key);
            }
        }
        (rest, part)
    }

    pub(crate) fn graft(&self, part: &Self, prefix: MapKeyPath) -> anyhow::Result<Self> {
        anyhow::ensure!(prefix.len <= 256, "Invalid queue prefix");
        let in_scope = |key: &K| {
            multi_map::ops::prefix_bits_match(&prefix.prefix.0, &key.merkle_key().0, prefix.len)
        };
        anyhow::ensure!(!self.keys().any(in_scope), "Queue graft scope is occupied");
        anyhow::ensure!(part.keys().all(in_scope), "Queue graft contains out-of-scope keys");
        anyhow::ensure!(
            self.keys().next().zip(part.keys().next()).is_none_or(|(a, b)| a.same_scope(b)),
            "Queue graft crosses DApps"
        );

        let (_, subtree) = part.map.split(prefix);
        let mut result = self.clone();
        result.map = self.map.graft(&subtree);
        for key in part.keys() {
            result.order.insert(*key);
        }
        Ok(result)
    }
}

// Serialize logical entries, not cached Merkle hashes. Import rebuilds all hashes.
impl<K: QueueMapKey + Serialize, V: MultiMapValue + Serialize> Serialize for QueueMap<K, V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.len()))?;
        for entry in self.iter() {
            sequence.serialize_element(&entry)?;
        }
        sequence.end()
    }
}

impl<'de, K, V> Deserialize<'de> for QueueMap<K, V>
where
    K: QueueMapKey + Deserialize<'de>,
    V: MultiMapValue + Deserialize<'de>,
{
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut entries = Vec::<(K, V)>::deserialize(deserializer)?;
        entries.sort_by_key(|(key, _)| key.merkle_key());
        for pair in entries.windows(2) {
            if pair[0].0.merkle_key() == pair[1].0.merkle_key() || !pair[0].0.same_scope(&pair[1].0)
            {
                return Err(serde::de::Error::custom("Duplicate or mixed-scope queue keys"));
            }
        }
        let mut result = Self::new();
        for (key, value) in entries {
            result.update_single(key, Some(value));
        }
        Ok(result)
    }
}
