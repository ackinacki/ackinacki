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

use super::seek::CyclicIter;
use super::InboxCapacity;

pub trait QueueMapValue: MultiMapValue {
    fn capacity(&self) -> InboxCapacity;
}

impl<V: QueueMapValue> QueueMapValue for std::sync::Arc<V> {
    fn capacity(&self) -> InboxCapacity {
        self.as_ref().capacity()
    }
}

pub trait QueueMapKey: Copy + Ord {
    fn merkle_key(&self) -> MapKey;
    fn from_merkle_key(key: MapKey, scope: Self) -> Self;

    fn same_scope(&self, _other: &Self) -> bool {
        true
    }
}

impl QueueMapKey for DAppIdentifier {
    fn from_merkle_key(key: MapKey, _scope: Self) -> Self {
        Self::new(key.0)
    }

    fn merkle_key(&self) -> MapKey {
        MapKey(*self.as_array())
    }
}

impl QueueMapKey for AccountRouting {
    fn from_merkle_key(key: MapKey, scope: Self) -> Self {
        node_types::AccountIdentifier::new(key.0).routing(*scope.dapp_id())
    }

    fn merkle_key(&self) -> MapKey {
        MapKey(*self.account_id().as_array())
    }

    fn same_scope(&self, other: &Self) -> bool {
        self.dapp_id() == other.dapp_id()
    }
}

/// Persistent Merkle storage; no duplicated key index. L2 keys are account IDs;
/// one scope anchor reconstructs their full routing. Length and scope are derived.
#[derive(Clone, Debug)]
pub struct QueueMap<K: QueueMapKey, V: QueueMapValue> {
    pub(super) map: MultiMapAug<V, InboxCapacity>,
    scope: Option<K>,
    len: usize,
}

impl<K: QueueMapKey, V: QueueMapValue> Default for QueueMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: QueueMapKey, V: QueueMapValue> QueueMap<K, V> {
    pub fn new() -> Self {
        Self { map: MultiMapAug::new(), scope: None, len: 0 }
    }

    pub fn root_hash(&self) -> MapHash {
        MapHash(self.map.root.hash())
    }

    pub(super) fn merge_disjoint(maps: &[&Self]) -> Result<Self, multi_map::DisjointMergeError> {
        let mut scope = None;
        let mut len = 0usize;
        for map in maps {
            if let Some(key) = map.scope {
                if scope.is_some_and(|scope: K| !scope.same_scope(&key)) {
                    return Err(multi_map::DisjointMergeError::IncompatibleRootPath);
                }
                scope = Some(key);
            }
            len = len.checked_add(map.len).ok_or(multi_map::DisjointMergeError::MalformedTree)?;
        }
        let roots: Vec<_> = maps.iter().map(|map| map.map.clone()).collect();
        let map = MultiMapAug::merge_disjoint_batch(&roots)?;
        Ok(Self { map, scope, len })
    }

    pub fn capacity(&self) -> InboxCapacity {
        self.map.root.augmentation()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        if self.scope.is_none_or(|scope| !scope.same_scope(key)) {
            return None;
        }
        multi_map::ops::get_leaf(&self.map.root, &self.map.root_path, &key.merkle_key())
            .map(|leaf| &leaf.value)
    }

    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.iter().map(|(key, _)| key)
    }

    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.map.iter().map(|(_, value)| value)
    }

    pub fn iter(&self) -> impl Iterator<Item = (K, &V)> {
        self.map.iter().map(|(key, value)| {
            (K::from_merkle_key(key, self.scope.expect("nonempty queue scope")), value)
        })
    }

    pub(crate) fn cyclic(&self, cursor: Option<MapKey>) -> CyclicIter<'_, V, InboxCapacity> {
        CyclicIter::new(&self.map, cursor)
    }

    pub(crate) fn advance(&self, cursor: Option<MapKey>, count: usize) -> Option<MapKey> {
        if self.is_empty() {
            None
        } else {
            self.cyclic(cursor).nth(count % self.len()).map(|(key, _)| key)
        }
    }

    pub(crate) fn update_batch(&mut self, updates: Vec<(K, Option<V>)>) {
        // Sequential metadata updates also handle duplicate keys in a batch.
        // Queue callers submit distinct keys; keep single-key semantics for duplicates.
        let mut seen = std::collections::BTreeSet::new();
        if updates.iter().any(|(key, _)| !seen.insert(key.merkle_key())) {
            for (key, value) in updates {
                self.update_single(key, value);
            }
            return;
        }
        let updates: Vec<_> = updates
            .into_iter()
            .filter(|(key, value)| {
                value.is_some() || self.scope.is_some_and(|scope| scope.same_scope(key))
            })
            .collect();
        for (key, value) in &updates {
            self.update_metadata(key, value.is_some());
        }
        let updates: Vec<_> = updates
            .into_iter()
            .map(|(key, value)| {
                (
                    key.merkle_key(),
                    value.map(|value| {
                        let capacity = value.capacity();
                        (value, capacity)
                    }),
                )
            })
            .collect();

        self.map = match updates.as_slice() {
            [] => return,
            [(key, value)] => self.map.update_single_aug(key, value.clone()),
            _ => self.map.update_aug(&updates),
        };
        if self.is_empty() {
            self.scope = None;
        }
    }

    pub(crate) fn update_single(&mut self, key: K, value: Option<V>) {
        if value.is_none() && self.scope.is_none_or(|scope| !scope.same_scope(&key)) {
            return;
        }
        self.update_metadata(&key, value.is_some());
        self.map = self.map.update_single_aug(
            &key.merkle_key(),
            value.map(|value| {
                let capacity = value.capacity();
                (value, capacity)
            }),
        );
        if self.is_empty() {
            self.scope = None;
        }
    }

    fn update_metadata(&mut self, key: &K, present: bool) {
        let existed = self.get(key).is_some();
        if present {
            assert!(self.scope.is_none_or(|scope| scope.same_scope(key)));
            self.scope = Some(*key);
            if !existed {
                self.len += 1;
            }
        } else if existed {
            self.len -= 1;
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
        let part_map = (MultiMapAug::new()).graft(&subtree);
        let part_len = part_map.iter().count();
        let rest_len = self.len - part_len;
        let rest = Self { map: without, scope: self.scope.filter(|_| rest_len > 0), len: rest_len };
        let part =
            Self { map: part_map, scope: self.scope.filter(|_| part_len > 0), len: part_len };
        (rest, part)
    }

    pub(crate) fn graft(&self, part: &Self, prefix: MapKeyPath) -> anyhow::Result<Self> {
        anyhow::ensure!(prefix.len <= 256, "Invalid queue prefix");
        let in_scope = |key: K| {
            multi_map::ops::prefix_bits_match(&prefix.prefix.0, &key.merkle_key().0, prefix.len)
        };
        anyhow::ensure!(!self.keys().any(in_scope), "Queue graft scope is occupied");
        anyhow::ensure!(part.keys().all(in_scope), "Queue graft contains out-of-scope keys");
        anyhow::ensure!(
            self.scope.zip(part.scope).is_none_or(|(a, b)| a.same_scope(&b)),
            "Queue graft crosses DApps"
        );

        let (_, subtree) = part.map.split(prefix);
        let mut result = self.clone();
        result.map = self.map.graft(&subtree);
        result.len += part.len;
        result.scope = self.scope.or(part.scope);
        Ok(result)
    }
}

// Serialize logical entries, not cached Merkle hashes. Import rebuilds all hashes.
impl<K: QueueMapKey + Serialize, V: QueueMapValue + Serialize> Serialize for QueueMap<K, V> {
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
    V: QueueMapValue + Deserialize<'de>,
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
