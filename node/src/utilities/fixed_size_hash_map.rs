// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

// Note:
// We should be able to clean up finalized blocks from a storage after
// all its descendants blocks have finalized.
// However this solution is much faster to implement and it's safe as
// long as we have a good capacity
pub struct FixedSizeHashMap<K, V> {
    capacity: usize,
    blocks: HashMap<K, V>,
    eviction_order: Vec<K>,
    eviction_order_cursor: usize,
}

impl<K, V> FixedSizeHashMap<K, V>
where
    K: Clone + std::hash::Hash + std::cmp::Eq,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            blocks: HashMap::with_capacity(capacity),
            eviction_order: Vec::with_capacity(capacity),
            eviction_order_cursor: 0,
        }
    }

    pub fn contains_key(&self, id: &K) -> bool {
        self.blocks.contains_key(id)
    }

    pub fn insert(&mut self, id: K, value: V) {
        if self.blocks.contains_key(&id) {
            return;
        }
        if self.capacity > self.eviction_order.len() {
            self.blocks.insert(id.clone(), value);
            self.eviction_order.push(id);
        } else {
            let evicted =
                std::mem::replace(&mut self.eviction_order[self.eviction_order_cursor], id.clone());
            self.blocks.remove(&evicted);
            self.blocks.insert(id, value);
            self.eviction_order_cursor += 1;
            if self.eviction_order_cursor >= self.capacity {
                self.eviction_order_cursor = 0;
            }
        }
    }

    pub fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.blocks.get(k)
    }
}

impl<K, V> std::fmt::Debug for FixedSizeHashMap<K, V>
where
    K: Clone + std::hash::Hash + std::cmp::Eq,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FixedSizeHashMap({}/{})", self.eviction_order.len(), self.capacity)
    }
}
