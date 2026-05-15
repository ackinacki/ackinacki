use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::Write;

use multi_map::MapKey;
use multi_map::MapKeyPath;
use multi_map::MultiMap;
use multi_map::MultiMapRepository;
use multi_map::MultiMapValue;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::Blake3Hashable;
use node_types::DAppIdentifier;
use node_types::DAppIdentifierPath;
use node_types::ThreadAccountsHash;

use crate::thread_accounts::durable::AccountInfo;

/// A level-1 trie value that embeds an entire L2 map (MultiMapRef<AccountInfo>).
#[derive(Clone)]
pub struct L1MapValue(pub MultiMap<AccountInfo>);

impl L1MapValue {
    pub fn new(map: MultiMap<AccountInfo>) -> Self {
        Self(map)
    }

    pub fn empty() -> Self {
        Self(MultiMapRepository::<AccountInfo>::new_map())
    }
}

impl Blake3Hashable for L1MapValue {
    fn update_hasher(&self, hasher: &mut blake3::Hasher) {
        // The hash of this value IS the root hash of the embedded account map.
        // This matches DAppAccountMapHash's Blake3Hashable (32 bytes of root hash),
        // ensuring hash-compatible level-1 tries.
        hasher.update(&self.0.root.hash());
    }
}

impl MultiMapValue for L1MapValue {
    fn write_value<W: Write>(&self, w: &mut W) -> anyhow::Result<()> {
        MultiMapRepository::<AccountInfo>::new().write_map(&self.0, w)
    }

    fn read_value<R: std::io::Read>(r: &mut R) -> anyhow::Result<Self> {
        let map = MultiMapRepository::<AccountInfo>::new().read_map(r)?;
        Ok(L1MapValue(map))
    }

    fn write_value_no_hash<W: Write>(&self, w: &mut W) -> anyhow::Result<()> {
        MultiMapRepository::<AccountInfo>::new().write_map_no_hash(&self.0, w)
    }

    fn read_value_no_hash<R: std::io::Read>(r: &mut R) -> anyhow::Result<Self> {
        let map = MultiMapRepository::<AccountInfo>::new().read_map_no_hash(r)?;
        Ok(L1MapValue(map))
    }
}

pub type ThreadAccountMap = MultiMap<L1MapValue>;

#[derive(Clone)]
pub struct ThreadAccountMapRepository {
    l1: MultiMapRepository<L1MapValue>,
    l2: MultiMapRepository<AccountInfo>,
}

impl Default for ThreadAccountMapRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadAccountMapRepository {
    pub fn new() -> Self {
        Self { l1: MultiMapRepository::new(), l2: MultiMapRepository::new() }
    }

    pub fn new_map() -> ThreadAccountMap {
        MultiMapRepository::<L1MapValue>::new_map()
    }

    pub fn map_hash(&self, map: &ThreadAccountMap) -> ThreadAccountsHash {
        ThreadAccountsHash::new(self.l1.map_hash(map).0)
    }

    pub fn map_get(&self, map: &ThreadAccountMap, routing: &AccountRouting) -> Option<AccountInfo> {
        self.l1.map_get(map, &MapKey(*routing.dapp_id().as_array())).and_then(|dapp_value| {
            self.l2.map_get(&dapp_value.0, &MapKey(*routing.account_id().as_array()))
        })
    }

    pub fn map_split(
        &self,
        map: &ThreadAccountMap,
        dapp_id_path: DAppIdentifierPath,
    ) -> (ThreadAccountMap, ThreadAccountMap) {
        let DAppIdentifierPath { prefix, len } = dapp_id_path;
        let (a, b) = self.l1.map_split(map, MapKeyPath { prefix: MapKey(*prefix.as_array()), len });
        (a, b)
    }

    pub fn merge(&self, a: &ThreadAccountMap, b: &ThreadAccountMap) -> ThreadAccountMap {
        self.l1.merge(a, b)
    }

    /// Merge multiple thread maps into `a` in a single pass.
    /// Much faster than calling `merge` repeatedly.
    pub fn merge_batch(
        &self,
        a: &ThreadAccountMap,
        subtrees: &[ThreadAccountMap],
    ) -> ThreadAccountMap {
        let sub_maps: Vec<_> = subtrees.to_vec();
        self.l1.merge_batch(a, &sub_maps)
    }

    pub fn map_update(
        &self,
        map: &ThreadAccountMap,
        accounts: &[(AccountRouting, Option<AccountInfo>)],
    ) -> ThreadAccountMap {
        let mut l1_update = HashMap::new();
        for (routing, account_info) in accounts {
            let l2_update = match l1_update.entry(*routing.dapp_id()) {
                Entry::Occupied(existing) => existing.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert((self.ensure_l2_map(map, routing.dapp_id()), Vec::new()))
                }
            };
            l2_update.1.push((*routing.account_id(), *account_info));
        }

        // Apply L2 updates per dApp, then apply each to L1 individually.
        // Sequential single-key updates benefit from CPU cache locality:
        // consecutive walks through the same trie keep upper nodes hot in L1/L2 cache.
        let mut l1_map = map.clone();
        for (dapp_id, (mut l2_map, accounts)) in l1_update {
            for (id, info) in accounts {
                l2_map = self.l2.map_update_single(&l2_map, &MapKey(*id.as_array()), info);
            }
            l1_map = self.l1.map_update_single(
                &l1_map,
                &MapKey(*dapp_id.as_array()),
                Some(L1MapValue::new(l2_map)),
            );
        }
        l1_map
    }

    fn ensure_l2_map(
        &self,
        map: &ThreadAccountMap,
        dapp_id: &DAppIdentifier,
    ) -> MultiMap<AccountInfo> {
        match self.l1.map_get(map, &MapKey(*dapp_id.as_array())) {
            Some(l2_map) => l2_map.0,
            None => MultiMapRepository::<AccountInfo>::new_map(),
        }
    }

    // ---- Serialization ----

    /// Serialize a state map to a writer (includes hashes).
    pub fn map_write<W: Write>(
        &self,
        map: &ThreadAccountMap,
        writer: &mut W,
    ) -> anyhow::Result<()> {
        self.l1.write_map(map, writer)
    }

    /// Deserialize a state map from a reader (hashes stored in stream).
    pub fn map_read<R: std::io::Read>(&self, reader: &mut R) -> anyhow::Result<ThreadAccountMap> {
        self.l1.read_map(reader)
    }

    /// Serialize a state map without hashes (compact, smaller).
    /// Must be read back with `map_read_no_hash`.
    pub fn map_write_no_hash<W: Write>(
        &self,
        map: &ThreadAccountMap,
        writer: &mut W,
    ) -> anyhow::Result<()> {
        self.l1.write_map_no_hash(map, writer)
    }

    /// Deserialize a compact state map, recomputing all hashes from values/children.
    pub fn map_read_no_hash<R: std::io::Read>(
        &self,
        reader: &mut R,
    ) -> anyhow::Result<ThreadAccountMap> {
        self.l1.read_map_no_hash(reader)
    }

    // ---- Iteration ----

    /// Returns a lazy iterator over all (AccountRouting, &AccountInfo) entries
    /// in the two-level map. Iterates L1 dApps, and for each dApp iterates
    /// its L2 account entries.
    pub fn map_iter<'a>(&'a self, map: &'a ThreadAccountMap) -> DurableThreadAccountsIter<'a> {
        let l1_iter = self.l1.iter(map);
        DurableThreadAccountsIter {
            account_maps: &self.l2,
            l1_iter,
            current_dapp_id: DAppIdentifier::default(),
            current_l2_iter: None,
        }
    }
}

/// Lazy iterator over (AccountRouting, &AccountInfo) in a two-level durable thread accounts map.
pub struct DurableThreadAccountsIter<'a> {
    account_maps: &'a MultiMapRepository<AccountInfo>,
    l1_iter: multi_map::MapIter<'a, L1MapValue>,
    current_dapp_id: DAppIdentifier,
    current_l2_iter: Option<multi_map::MapIter<'a, AccountInfo>>,
}

impl<'a> Iterator for DurableThreadAccountsIter<'a> {
    type Item = (AccountRouting, &'a AccountInfo);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get the next account from the current L2 iterator
            if let Some(ref mut l2) = self.current_l2_iter {
                if let Some((account_key, info)) = l2.next() {
                    let routing = AccountRouting::new(
                        self.current_dapp_id,
                        AccountIdentifier::new(account_key.0),
                    );
                    return Some((routing, info));
                }
                // L2 exhausted, fall through to advance L1
                self.current_l2_iter = None;
            }

            // Advance L1 to next dApp
            let (dapp_key, l2_map) = self.l1_iter.next()?;
            self.current_dapp_id = DAppIdentifier::new(dapp_key.0);
            self.current_l2_iter = Some(self.account_maps.iter(&l2_map.0));
        }
    }
}
