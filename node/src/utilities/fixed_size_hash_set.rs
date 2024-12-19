// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;

// Note:
// We should be able to clean up finalized blocks from a storage after
// all its descendants blocks have finalized.
// However this solution is much faster to implement and it's safe as
// long as we have a good capacity
pub struct FixedSizeHashSet<T> {
    capacity: usize,
    blocks: HashSet<T>,
    eviction_order: Vec<T>,
    eviction_order_cursor: usize,
}

impl<T> FixedSizeHashSet<T>
where
    T: Clone + std::hash::Hash + std::cmp::Eq,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            blocks: HashSet::with_capacity(capacity),
            eviction_order: Vec::with_capacity(capacity),
            eviction_order_cursor: 0,
        }
    }

    pub fn contains(&self, id: &T) -> bool {
        self.blocks.contains(id)
    }

    pub fn insert(&mut self, id: T) {
        if self.blocks.contains(&id) {
            return;
        }
        if self.capacity > self.eviction_order.len() {
            self.blocks.insert(id.clone());
            self.eviction_order.push(id);
        } else {
            let evicted =
                std::mem::replace(&mut self.eviction_order[self.eviction_order_cursor], id.clone());
            self.blocks.remove(&evicted);
            self.blocks.insert(id);
            self.eviction_order_cursor += 1;
            if self.eviction_order_cursor >= self.capacity {
                self.eviction_order_cursor = 0;
            }
        }
    }
}

impl<T> std::fmt::Debug for FixedSizeHashSet<T>
where
    T: Clone + std::hash::Hash + std::cmp::Eq,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FixedSizeHashSet({}/{})", self.eviction_order.len(), self.capacity)
    }
}
