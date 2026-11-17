use std::sync::Arc;

use multi_map::node::BranchDataAug;
use multi_map::node::ExtDataAug;
use multi_map::node::NodeAug;
use multi_map::ops::hash_branch_sparse;
use multi_map::ops::hash_ext;
use multi_map::ops::nibble_at;
use multi_map::MapKey;
use multi_map::MapKeyPath;
use multi_map::MultiMapAug;
use multi_map::MultiMapAugmentation;
use multi_map::MultiMapValue;
use node_types::AccountRouting;

use super::maps::L1MapValue;
use super::AccountCapacity;
use super::AccountInfo;
use super::ThreadAccountMap;

/// Shared metadata budget for legacy and partitioned snapshot imports. Bodies
/// stream to staged storage independently; only routes and leaf metadata wait
/// here. Sorted partition windows bypass sorting, legacy completion order need
/// not be sorted. The same owned trie continues across successive windows.
pub(super) const SNAPSHOT_MAP_WINDOW_RECORDS: usize = 500_000;

/// Build from arbitrary record order with a fixed metadata window. Already
/// sorted windows skip sorting. Each window updates the exclusively owned tree
/// in place; globally overlapping key ranges are allowed.
///
/// DApp leaves can overlap across windows, but their nested account sets must
/// remain disjoint. A repeated routing is an error even if its value is equal.
/// The reusable input buffer retains at most window_capacity metadata records;
/// bodies can go straight to staged storage. No separate window tree is built.
/// Sortedness is local to each window. An insertion error poisons the builder:
/// partial in-place changes cannot be published and the caller must abort staging.
pub struct MapBuilder {
    scope: MapKeyPath,
    window_capacity: usize,
    window: Vec<(AccountRouting, AccountInfo, AccountCapacity)>,
    sorted: bool,
    map: ThreadAccountMap,
    failed: bool,
}

impl MapBuilder {
    pub fn new(scope: MapKeyPath, window_capacity: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(scope.len <= 256, "invalid account map scope");
        anyhow::ensure!(window_capacity > 0, "account map window must be positive");
        let mut window = Vec::new();
        window.try_reserve_exact(window_capacity)?;
        Ok(Self {
            scope,
            window_capacity,
            window,
            sorted: true,
            map: ThreadAccountMap { dapps: MultiMapAug { root_path: scope, ..MultiMapAug::new() } },
            failed: false,
        })
    }

    pub fn push(
        &mut self,
        route: AccountRouting,
        info: AccountInfo,
        capacity: AccountCapacity,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(!self.failed, "account map builder failed");
        anyhow::ensure!(
            multi_map::ops::prefix_bits_match(
                &self.scope.prefix.0,
                route.dapp_id().as_array(),
                self.scope.len
            ),
            "account routing outside account map scope"
        );
        // A failed full-window flush must not allow subsequent pushes to grow
        // the buffer without bound. Retry/fail before accepting another record.
        if self.window.len() == self.window_capacity {
            self.flush()?;
        }
        if let Some(previous) = self.window.last() {
            self.sorted &= previous.0 < route;
        }
        self.window.push((route, info, capacity));
        if self.window.len() == self.window_capacity {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.failed, "account map builder failed");
        if self.window.is_empty() {
            return Ok(());
        }
        // Set before mutations, including potentially panicking augmentation
        // arithmetic. Even after a caught unwind finish must not expose a tree
        // whose ancestor hashes have not yet been refreshed.
        self.failed = true;
        let result = self.flush_in_place();
        if result.is_ok() {
            self.failed = false;
        }
        result
    }

    fn flush_in_place(&mut self) -> anyhow::Result<()> {
        if !self.sorted {
            self.window.sort_unstable_by_key(|entry| entry.0);
        }
        anyhow::ensure!(
            self.window.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "duplicate account routing in window"
        );
        insert_sorted(
            &mut self.map.dapps.root,
            usize::from(self.scope.len) / 4,
            &self.window,
            |entry| MapKey(*entry.0.dapp_id().as_array()),
            &mut |previous: Option<L1MapValue>, entries| {
                let mut accounts = previous.map(|value| value.0).unwrap_or_default();
                insert_sorted(
                    &mut accounts.root,
                    0,
                    entries,
                    |entry| MapKey(*entry.0.account_id().as_array()),
                    &mut |previous, entries| {
                        anyhow::ensure!(
                            previous.is_none(),
                            "duplicate account routing across windows"
                        );
                        anyhow::ensure!(entries.len() == 1, "duplicate account routing in window");
                        Ok((entries[0].1, entries[0].2))
                    },
                )?;
                let capacity = accounts.root.augmentation();
                Ok((L1MapValue::new(accounts), capacity))
            },
        )?;
        self.window.clear();
        self.sorted = true;
        Ok(())
    }

    pub fn finish(mut self) -> anyhow::Result<ThreadAccountMap> {
        self.flush()?;
        Ok(self.map)
    }
}

/// Insert one sorted window into an exclusively owned account trie.
///
/// Existing branches/extensions are changed in place and rehashed once after all
/// records below them have been handled. New empty ranges are built directly in
/// their final shape. There is no second window tree and no subsequent union.
/// The window builder never exposes/clones its map before finish, so every Arc
/// traversed here must be unique. Errors poison that builder: an interrupted
/// update may leave provisional hashes and must never be published.
fn insert_sorted<T, V, K, F>(
    node: &mut NodeAug<V, AccountCapacity>,
    depth: usize,
    entries: &[T],
    key: K,
    leaf: &mut F,
) -> anyhow::Result<()>
where
    V: MultiMapValue,
    K: Fn(&T) -> MapKey + Copy,
    F: FnMut(Option<V>, &[T]) -> anyhow::Result<(V, AccountCapacity)>,
{
    if entries.is_empty() {
        return Ok(());
    }
    if depth == 64 {
        let previous = match std::mem::take(node) {
            NodeAug::Empty => None,
            NodeAug::Leaf(data) => Some(
                Arc::try_unwrap(data)
                    .map_err(|_| anyhow::anyhow!("shared leaf in owned account builder"))?
                    .value,
            ),
            _ => anyhow::bail!("invalid leaf depth in owned account builder"),
        };
        let (value, capacity) = leaf(previous, entries)?;
        *node = NodeAug::new_leaf_aug(value, capacity);
        return Ok(());
    }
    match node {
        NodeAug::Empty => {
            let first = key(&entries[0]);
            let last = key(entries.last().unwrap());
            let mut common = if first == last { 64 } else { depth };
            while common < 64 && nibble_at(&first.0, common) == nibble_at(&last.0, common) {
                common += 1;
            }
            if common > depth {
                let mut child = NodeAug::Empty;
                insert_sorted(&mut child, common, entries, key, leaf)?;
                let mut path = [0; 64];
                for (i, at) in (depth..common).enumerate() {
                    path[i] = nibble_at(&first.0, at);
                }
                *node = NodeAug::new_ext(&path[..common - depth], child);
                return Ok(());
            }
            *node = NodeAug::new_branch_unhashed(Default::default());
            insert_sorted(node, depth, entries, key, leaf)
        }
        NodeAug::Branch(data) => {
            let data = Arc::get_mut(data)
                .ok_or_else(|| anyhow::anyhow!("shared branch in owned account builder"))?;
            for group in
                entries.chunk_by(|a, b| nibble_at(&key(a).0, depth) == nibble_at(&key(b).0, depth))
            {
                let nibble = nibble_at(&key(&group[0]).0, depth) as usize;
                insert_sorted(&mut data.children[nibble], depth + 1, group, key, leaf)?;
            }
            refresh_branch(data);
            Ok(())
        }
        NodeAug::Ext(data) => {
            let data = Arc::get_mut(data)
                .ok_or_else(|| anyhow::anyhow!("shared extension in owned account builder"))?;
            let count = usize::from(data.nibble_count);
            let first = key(&entries[0]);
            let last = key(entries.last().unwrap());
            let mut common = 0;
            while common < count
                && data.nibbles[common] == nibble_at(&first.0, depth + common)
                && data.nibbles[common] == nibble_at(&last.0, depth + common)
            {
                common += 1;
            }
            if common == count {
                insert_sorted(&mut data.child, depth + count, entries, key, leaf)?;
                refresh_ext(data);
                return Ok(());
            }
            let old_nibble = data.nibbles[common] as usize;
            if common == 0 {
                // Reuse the old extension allocation for its remaining suffix.
                // Its first nibble becomes the new branch's existing child.
                let old_child = if count == 1 {
                    let child = std::mem::take(&mut data.child);
                    *node = NodeAug::Empty;
                    child
                } else {
                    data.nibbles.copy_within(1..count, 0);
                    data.nibble_count -= 1;
                    refresh_ext(data);
                    std::mem::take(node)
                };
                let mut children: [NodeAug<V, AccountCapacity>; 16] = Default::default();
                children[old_nibble] = old_child;
                *node = NodeAug::new_branch_unhashed(children);
                insert_sorted(node, depth, entries, key, leaf)
            } else {
                // Keep the original allocation as the common prefix. Splitting
                // creates only the branch/suffix required in the final tree.
                let child = std::mem::take(&mut data.child);
                let tail = &data.nibbles[common + 1..count];
                let old_child = if tail.is_empty() { child } else { NodeAug::new_ext(tail, child) };
                let mut children: [NodeAug<V, AccountCapacity>; 16] = Default::default();
                children[old_nibble] = old_child;
                data.nibble_count = common as u8;
                data.child = NodeAug::new_branch_unhashed(children);
                insert_sorted(&mut data.child, depth + common, entries, key, leaf)?;
                refresh_ext(data);
                Ok(())
            }
        }
        NodeAug::Leaf(_) => anyhow::bail!("unexpected shallow leaf in owned account builder"),
    }
}

fn refresh_branch<V: MultiMapValue>(data: &mut BranchDataAug<V, AccountCapacity>) {
    let mut nibbles = [0; 16];
    let mut hashes = [[0; 32]; 16];
    let mut count = 0;
    data.bitmap = 0;
    data.augmentation = AccountCapacity::identity();
    for (nibble, child) in data.children.iter().enumerate() {
        if !child.is_empty() {
            data.bitmap |= 1 << nibble;
            nibbles[count] = nibble as u8;
            hashes[count] = child.hash();
            count += 1;
            data.augmentation = AccountCapacity::combine(&data.augmentation, &child.augmentation());
        }
    }
    debug_assert!(count >= 2, "insert-only updates cannot collapse a branch");
    data.hash = hash_branch_sparse(data.bitmap, &nibbles[..count], &hashes[..count]);
}

fn refresh_ext<V: MultiMapValue>(data: &mut ExtDataAug<V, AccountCapacity>) {
    let count = usize::from(data.nibble_count);
    let mut packed = [0; 32];
    for (i, nibble) in data.nibbles[..count].iter().enumerate() {
        packed[i / 2] |= nibble << if i.is_multiple_of(2) { 4 } else { 0 };
    }
    data.hash = hash_ext(data.nibble_count, &packed[..count.div_ceil(2)], &data.child.hash());
    data.augmentation = data.child.augmentation();
}

#[cfg(test)]
#[path = "map_builder_tests.rs"]
mod tests;
