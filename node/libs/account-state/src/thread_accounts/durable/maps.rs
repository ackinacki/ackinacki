use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::Read;
use std::io::Write;

use multi_map::MapKey;
use multi_map::MapKeyPath;
use multi_map::MultiMapAug;
use multi_map::MultiMapAugmentation;
use multi_map::MultiMapValue;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::Blake3Hashable;
use node_types::DAppIdentifier;
use node_types::DAppIdentifierPath;
use node_types::ThreadAccountsHash;

use crate::thread_accounts::durable::AccountInfo;

const ACCOUNT_MAP_FORMAT_MAGIC: &[u8; 8] = b"ACMAPV3\0";
const ACCOUNT_MAP_HASHED_ENCODING: u8 = 1;
const ACCOUNT_MAP_COMPACT_ENCODING: u8 = 2;

/// Canonical byte capacity derived from the accounts stored below a map node.
///
/// Capacity is persisted with the map but deliberately excluded from its Merkle
/// hash. It is fully derivable from leaf metadata and exists only to make
/// snapshot partition planning independent of the number of accounts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AccountCapacity {
    bytes: u64,
}

impl AccountCapacity {
    pub fn new(bytes: u64) -> Self {
        Self { bytes }
    }

    pub fn bytes(self) -> u64 {
        self.bytes
    }
}

impl MultiMapAugmentation for AccountCapacity {
    fn identity() -> Self {
        Self::default()
    }

    fn combine(left: &Self, right: &Self) -> Self {
        Self::new(left.bytes.checked_add(right.bytes).expect("account capacity overflow"))
    }

    fn replace_child(
        current: &Self,
        old_child: Option<&Self>,
        new_child: Option<&Self>,
    ) -> Option<Self> {
        let without_old = current
            .bytes
            .checked_sub(old_child.map_or(0, |child| child.bytes))
            .expect("account capacity underflow");
        Some(Self::new(
            without_old
                .checked_add(new_child.map_or(0, |child| child.bytes))
                .expect("account capacity overflow"),
        ))
    }

    fn write<W: Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        Ok(writer.write_all(&self.bytes.to_le_bytes())?)
    }

    fn read<R: Read>(reader: &mut R) -> anyhow::Result<Self> {
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        Ok(Self::new(u64::from_le_bytes(bytes)))
    }
}
/// A level-1 trie value that embeds an entire L2 map (MultiMapRef<AccountInfo>).
#[derive(Clone)]
pub struct L1MapValue(pub MultiMapAug<AccountInfo, AccountCapacity>);

impl L1MapValue {
    pub fn new(map: MultiMapAug<AccountInfo, AccountCapacity>) -> Self {
        Self(map)
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
        self.0.write(w)
    }

    fn read_value<R: std::io::Read>(r: &mut R) -> anyhow::Result<Self> {
        let map = MultiMapAug::<AccountInfo, AccountCapacity>::read(r)?;
        Ok(L1MapValue(map))
    }

    fn write_value_no_hash<W: Write>(&self, w: &mut W) -> anyhow::Result<()> {
        self.0.write_no_hash(w)
    }

    fn read_value_no_hash<R: std::io::Read>(r: &mut R) -> anyhow::Result<Self> {
        let map = MultiMapAug::<AccountInfo, AccountCapacity>::read_no_hash(r)?;
        Ok(L1MapValue(map))
    }
}

/// Persistent two-level account map keyed by complete account routing.
/// The owned DApp tree stays internal; updates return a new map.
#[derive(Clone, Debug, Default)]
#[repr(transparent)]
pub struct ThreadAccountMap {
    pub(super) dapps: MultiMapAug<L1MapValue, AccountCapacity>,
}

impl ThreadAccountMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn hash(&self) -> ThreadAccountsHash {
        ThreadAccountsHash::new(self.dapps.hash().0)
    }

    pub fn is_empty(&self) -> bool {
        self.dapps.root.is_empty()
    }

    /// Serialize the full two-level account map to a writer. The L1 entries
    /// (and their nested L2 maps via `L1MapValue::write_value`) are written
    /// in one pass. Used to ship a tombstone's sub-trie to the successor on
    /// thread collapse — see spec 5.4.3.
    pub fn write<W: std::io::Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        Self::write_format_header(writer, ACCOUNT_MAP_HASHED_ENCODING)?;
        self.dapps.write(writer)
    }

    /// Inverse of [`Self::write`].
    pub fn read<R: std::io::Read>(reader: &mut R) -> anyhow::Result<Self> {
        Self::read_format_header(reader, ACCOUNT_MAP_HASHED_ENCODING)?;
        let map = MultiMapAug::<L1MapValue, AccountCapacity>::read(reader)?;
        Self::validate_l1_capacities(&map)?;
        Ok(Self { dapps: map })
    }

    pub fn get(&self, routing: &AccountRouting) -> Option<AccountInfo> {
        self.dapps
            .get(&MapKey(*routing.dapp_id().as_array()))
            .and_then(|dapp_value| dapp_value.0.get(&MapKey(*routing.account_id().as_array())))
    }

    pub fn account_capacity(&self, routing: &AccountRouting) -> Option<AccountCapacity> {
        let dapp = self.dapps.get(&MapKey(*routing.dapp_id().as_array()))?;
        dapp.0.get_aug(&MapKey(*routing.account_id().as_array())).map(|(_, capacity)| capacity)
    }

    pub fn dapp_capacity(&self, dapp_id: &DAppIdentifier) -> Option<AccountCapacity> {
        self.dapps
            .get(&MapKey(*dapp_id.as_array()))
            .map(|dapp_value| dapp_value.0.root.augmentation())
    }

    pub fn capacity(&self) -> AccountCapacity {
        self.dapps.root.augmentation()
    }

    pub fn split(&self, dapp_id_path: DAppIdentifierPath) -> (Self, Self) {
        let DAppIdentifierPath { prefix, len } = dapp_id_path;
        let (a, b) = self
            .dapps
            .split(MapKeyPath { prefix: MapKey(*prefix.as_array()), len: u16::from(len) });
        (Self { dapps: a }, Self { dapps: b })
    }

    /// Graft `b` into this map at `b`'s prefix path. NOT a union; see
    /// [`multi_map::ops::graft`] for the full contract — when both inputs are
    /// full L1 roots, this REPLACES this map with `b`.
    pub fn graft(&self, b: &Self) -> Self {
        Self { dapps: self.dapps.graft(&b.dapps) }
    }

    /// Graft multiple thread maps into this map in a single pass.
    /// Same graft contract as [`Self::graft`].
    pub fn graft_batch(&self, subtrees: &[Self]) -> Self {
        let sub_maps: Vec<_> = subtrees.iter().map(|map| map.dapps.clone()).collect();
        Self { dapps: self.dapps.graft_batch(&sub_maps) }
    }

    /// Update one full routing without allocating the grouping HashMap/Vec used by batch updates.
    /// Preserve canonical L1 shape by removing a DApp when its last account is removed.
    pub fn update_single(
        &self,
        routing: &AccountRouting,
        account_info: Option<(AccountInfo, AccountCapacity)>,
    ) -> Self {
        let key = MapKey(*routing.dapp_id().as_array());
        let existing = self.dapps.get(&key);
        let had_dapp = existing.is_some();
        let l2 = existing
            .map(|value| value.0)
            .unwrap_or_default()
            .update_single_aug(&MapKey(*routing.account_id().as_array()), account_info);
        let value = (!l2.root.is_empty()).then(|| {
            let capacity = l2.root.augmentation();
            (L1MapValue::new(l2), capacity)
        });
        if had_dapp && value.is_none() {
            tracing::debug!(target: "account_state_dapp_cleanup",
                dapp_id = %routing.dapp_id().to_hex_string(),
                "removed empty dapp from durable account state");
        }
        Self { dapps: self.dapps.update_single_aug(&key, value) }
    }

    pub fn update(
        &self,
        accounts: &[(AccountRouting, Option<(AccountInfo, AccountCapacity)>)],
    ) -> Self {
        let mut l1_update = HashMap::new();
        for (routing, account_info) in accounts {
            let l2_update = match l1_update.entry(*routing.dapp_id()) {
                Entry::Occupied(existing) => existing.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert((self.ensure_l2_map(routing.dapp_id()), Vec::new()))
                }
            };
            l2_update.1.push((*routing.account_id(), *account_info));
        }

        // Apply L2 updates per dApp, then apply each to L1 individually.
        // Sequential single-key updates benefit from CPU cache locality:
        // consecutive walks through the same trie keep upper nodes hot in L1/L2 cache.
        let mut l1_map = self.dapps.clone();
        for (dapp_id, (mut l2_map, accounts)) in l1_update {
            for (id, info) in accounts {
                l2_map = l2_map.update_single_aug(&MapKey(*id.as_array()), info);
            }
            let dapp_key = MapKey(*dapp_id.as_array());
            let had_dapp = l1_map.get(&dapp_key).is_some();
            let dapp_update = (!l2_map.root.is_empty()).then(|| {
                let capacity = l2_map.root.augmentation();
                (L1MapValue::new(l2_map), capacity)
            });
            if had_dapp && dapp_update.is_none() {
                tracing::debug!(
                    target: "account_state_dapp_cleanup",
                    dapp_id = %dapp_id.to_hex_string(),
                    "removed empty dapp from durable account state"
                );
            }
            l1_map = l1_map.update_single_aug(&dapp_key, dapp_update);
        }
        Self { dapps: l1_map }
    }

    fn ensure_l2_map(&self, dapp_id: &DAppIdentifier) -> MultiMapAug<AccountInfo, AccountCapacity> {
        match self.dapps.get(&MapKey(*dapp_id.as_array())) {
            Some(l2_map) => l2_map.0,
            None => MultiMapAug::<AccountInfo, AccountCapacity>::new(),
        }
    }

    // ---- Serialization ----

    pub fn write_checked<W, F>(&self, writer: &mut W, should_cancel: &F) -> anyhow::Result<()>
    where
        W: Write,
        F: Fn() -> bool,
    {
        Self::write_format_header(writer, ACCOUNT_MAP_HASHED_ENCODING)?;
        self.dapps.write_checked(writer, should_cancel)
    }

    /// Serialize a state map without hashes (compact, smaller).
    /// Must be read back with [`Self::read_no_hash`].
    pub fn write_no_hash<W: Write>(&self, writer: &mut W) -> anyhow::Result<()> {
        Self::write_format_header(writer, ACCOUNT_MAP_COMPACT_ENCODING)?;
        self.dapps.write_no_hash(writer)
    }

    /// Deserialize a compact state map, recomputing all hashes from values/children.
    pub fn read_no_hash<R: std::io::Read>(reader: &mut R) -> anyhow::Result<Self> {
        Self::read_format_header(reader, ACCOUNT_MAP_COMPACT_ENCODING)?;
        let map = MultiMapAug::<L1MapValue, AccountCapacity>::read_no_hash(reader)?;
        Self::validate_l1_capacities(&map)?;
        Ok(Self { dapps: map })
    }

    fn validate_l1_capacities(
        map: &MultiMapAug<L1MapValue, AccountCapacity>,
    ) -> anyhow::Result<()> {
        fn visit(
            node: &multi_map::node::NodeAug<L1MapValue, AccountCapacity>,
        ) -> anyhow::Result<()> {
            match node {
                multi_map::node::NodeAug::Empty => Ok(()),
                multi_map::node::NodeAug::Leaf(leaf) => {
                    let embedded = leaf.value.0.root.augmentation();
                    if leaf.augmentation != embedded {
                        anyhow::bail!("L1 account capacity does not match embedded L2 map capacity")
                    }
                    Ok(())
                }
                multi_map::node::NodeAug::Branch(branch) => {
                    for child in &branch.children {
                        visit(child)?;
                    }
                    Ok(())
                }
                multi_map::node::NodeAug::Ext(ext) => visit(&ext.child),
            }
        }

        visit(&map.root)
    }

    fn write_format_header<W: Write>(writer: &mut W, encoding: u8) -> anyhow::Result<()> {
        writer.write_all(ACCOUNT_MAP_FORMAT_MAGIC)?;
        writer.write_all(&[encoding])?;
        Ok(())
    }

    fn read_format_header<R: Read>(reader: &mut R, expected_encoding: u8) -> anyhow::Result<()> {
        let mut magic = [0; ACCOUNT_MAP_FORMAT_MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if &magic != ACCOUNT_MAP_FORMAT_MAGIC {
            anyhow::bail!("unsupported account map format")
        }

        let mut encoding = [0];
        reader.read_exact(&mut encoding)?;
        if encoding[0] != expected_encoding {
            anyhow::bail!(
                "account map encoding mismatch: expected {}, got {}",
                expected_encoding,
                encoding[0]
            )
        }
        Ok(())
    }

    // ---- Iteration ----

    /// Returns a lazy iterator over all (AccountRouting, &AccountInfo) entries
    /// in the two-level map. Iterates L1 dApps, and for each dApp iterates
    /// its L2 account entries.
    pub fn iter(&self) -> DurableThreadAccountsIter<'_> {
        let l1_iter = self.dapps.iter();
        DurableThreadAccountsIter {
            l1_iter,
            current_dapp_id: DAppIdentifier::default(),
            current_l2_iter: None,
        }
    }

    /// Visit account leaves and their stored capacities together, in full routing order.
    /// The capacity is from L2, not the aggregate capacity of the enclosing DApp.
    pub fn iter_with_augmentation(
        &self,
    ) -> impl Iterator<Item = (AccountRouting, &AccountInfo, &AccountCapacity)> {
        let mut iter = self.iter();
        std::iter::from_fn(move || iter.next_with_augmentation())
    }
}

/// Lazy iterator over (AccountRouting, &AccountInfo) in a two-level durable thread accounts map.
pub struct DurableThreadAccountsIter<'a> {
    l1_iter: multi_map::MapIterAug<'a, L1MapValue, AccountCapacity>,
    current_dapp_id: DAppIdentifier,
    current_l2_iter: Option<multi_map::MapIterAug<'a, AccountInfo, AccountCapacity>>,
}

impl<'a> Iterator for DurableThreadAccountsIter<'a> {
    type Item = (AccountRouting, &'a AccountInfo);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_with_augmentation().map(|(routing, info, _)| (routing, info))
    }
}

impl<'a> DurableThreadAccountsIter<'a> {
    fn next_with_augmentation(
        &mut self,
    ) -> Option<(AccountRouting, &'a AccountInfo, &'a AccountCapacity)> {
        loop {
            // Try to get the next account from the current L2 iterator
            if let Some(ref mut l2) = self.current_l2_iter {
                if let Some((account_key, info, capacity)) = l2.next_with_augmentation() {
                    let routing = AccountRouting::new(
                        self.current_dapp_id,
                        AccountIdentifier::new(account_key.0),
                    );
                    return Some((routing, info, capacity));
                }
                // L2 exhausted, fall through to advance L1
                self.current_l2_iter = None;
            }

            // Advance L1 to next dApp. The L1 trie key IS the dapp_id
            // under the MSB-first convention, so no transformation is
            // needed here.
            let (dapp_key, l2_map) = self.l1_iter.next()?;
            self.current_dapp_id = DAppIdentifier::new(dapp_key.0);
            self.current_l2_iter = Some(l2_map.0.iter());
        }
    }
}

#[cfg(test)]
mod tests {
    use node_types::AccountHash;

    use super::*;

    #[test]
    fn account_map_wrapper_preserves_storage_hash_and_wire_layout() -> anyhow::Result<()> {
        assert_eq!(
            std::mem::size_of::<ThreadAccountMap>(),
            std::mem::size_of::<MultiMapAug<L1MapValue, AccountCapacity>>()
        );
        let map = ThreadAccountMap::new()
            .update(&[(routing(1, 2), Some((account_info(3), AccountCapacity::new(100))))]);
        assert_eq!(map.hash().as_array(), &map.dapps.root.hash());
        let mut raw = Vec::new();
        map.dapps.write(&mut raw)?;
        let mut encoded = Vec::new();
        map.write(&mut encoded)?;
        assert_eq!(&encoded[..8], ACCOUNT_MAP_FORMAT_MAGIC);
        assert_eq!(encoded[8], ACCOUNT_MAP_HASHED_ENCODING);
        assert_eq!(&encoded[9..], raw);
        let restored = ThreadAccountMap::read(&mut encoded.as_slice())?;
        assert_eq!(restored.hash(), map.hash());
        assert_eq!(restored.capacity(), map.capacity());
        assert_eq!(restored.iter().collect::<Vec<_>>(), map.iter().collect::<Vec<_>>());
        Ok(())
    }

    fn dapp(seed: u8) -> DAppIdentifier {
        DAppIdentifier::new([seed; 32])
    }

    fn account(seed: u8) -> AccountIdentifier {
        AccountIdentifier::new([seed; 32])
    }

    fn routing(dapp_seed: u8, account_seed: u8) -> AccountRouting {
        AccountRouting::new(dapp(dapp_seed), account(account_seed))
    }

    fn account_info(seed: u8) -> AccountInfo {
        AccountInfo::AccountHash(AccountHash::new([seed; 32]))
    }

    fn stored_account(seed: u8) -> (AccountInfo, AccountCapacity) {
        (account_info(seed), AccountCapacity::new(u64::from(seed)))
    }

    #[test]
    fn augmented_iteration_returns_account_not_dapp_capacity() -> anyhow::Result<()> {
        fn check(map: &ThreadAccountMap) {
            let entries: Vec<_> = map.iter_with_augmentation().collect();
            assert_eq!(
                entries.iter().map(|(routing, info, _)| (*routing, *info)).collect::<Vec<_>>(),
                map.iter().collect::<Vec<_>>()
            );
            assert!(entries.windows(2).all(|pair| pair[0].0 < pair[1].0));
            for (routing, info, capacity) in &entries {
                assert_eq!(map.get(routing), Some(**info));
                assert_eq!(map.account_capacity(routing), Some(**capacity));
            }
            assert_eq!(
                entries.iter().map(|(_, _, capacity)| capacity.bytes()).sum::<u64>(),
                map.capacity().bytes()
            );
        }

        let map = ThreadAccountMap::new().update(&[
            (routing(1, 1), Some((account_info(10), AccountCapacity::new(100)))),
            (routing(1, 2), Some((AccountInfo::Redirect(dapp(9)), AccountCapacity::new(20)))),
            (routing(1, 3), Some((account_info(30), AccountCapacity::new(0)))),
            (routing(2, 1), Some((account_info(40), AccountCapacity::new(400)))),
            (routing(7, 1), Some((account_info(50), AccountCapacity::new(500)))),
        ]);
        check(&ThreadAccountMap::new());
        check(&map);
        let modified = map
            .update_single(&routing(1, 1), Some((account_info(11), AccountCapacity::new(999))))
            .update_single(&routing(2, 1), None);
        check(&modified);
        let (without, subtree) = modified.split(DAppIdentifierPath { prefix: dapp(1), len: 8 });
        check(&without);
        check(&subtree);
        let mut encoded = Vec::new();
        modified.write(&mut encoded)?;
        let restored = ThreadAccountMap::read(&mut encoded.as_slice())?;
        check(&restored);
        assert_eq!(
            modified.iter_with_augmentation().collect::<Vec<_>>(),
            restored.iter_with_augmentation().collect::<Vec<_>>()
        );
        assert_eq!(modified.hash(), restored.hash());
        Ok(())
    }

    #[test]
    fn map_update_removes_dapp_key_after_last_account_is_removed() {
        let empty = ThreadAccountMap::new();
        let routing = routing(1, 2);

        let with_account = empty.update(&[(routing, Some(stored_account(3)))]);
        assert!(with_account.dapps.get(&MapKey(*routing.dapp_id().as_array())).is_some());

        let without_account = with_account.update(&[(routing, None)]);

        assert_eq!(without_account.get(&routing), None);
        assert!(without_account.dapps.get(&MapKey(*routing.dapp_id().as_array())).is_none());
    }

    #[test]
    fn map_update_keeps_dapp_key_when_other_accounts_remain() {
        let empty = ThreadAccountMap::new();
        let removed = routing(1, 2);
        let retained = routing(1, 3);

        let with_accounts = empty
            .update(&[(removed, Some(stored_account(4))), (retained, Some(stored_account(5)))]);
        let updated = with_accounts.update(&[(removed, None)]);

        assert_eq!(updated.get(&removed), None);
        assert_eq!(updated.get(&retained), Some(account_info(5)));
        assert!(updated.dapps.get(&MapKey(*retained.dapp_id().as_array())).is_some());
    }

    #[test]
    fn augmented_roots_match_unaugmented_account_maps() {
        let entries = [
            (routing(1, 1), account_info(1)),
            (routing(1, 2), account_info(2)),
            (routing(2, 1), AccountInfo::Redirect(dapp(3))),
        ];
        let map = (ThreadAccountMap::new())
            .update(&entries.map(|(key, info)| (key, Some((info, AccountCapacity::new(123))))));
        let mut baseline = multi_map::MultiMap::<L1MapValue>::new();
        for dapp_id in [dapp(1), dapp(2)] {
            let updates: Vec<_> = entries
                .iter()
                .filter(|(key, _)| *key.dapp_id() == dapp_id)
                .map(|(key, info)| (MapKey(*key.account_id().as_array()), Some(*info)))
                .collect();
            let l2 = multi_map::MultiMap::<AccountInfo>::new().update(&updates);
            let value = map.dapps.get(&MapKey(*dapp_id.as_array())).unwrap();
            assert_eq!(value.0.root.hash(), l2.root.hash());
            baseline = baseline.update_single(&MapKey(*dapp_id.as_array()), Some(value));
        }
        assert_eq!(map.dapps.root.hash(), baseline.root.hash());

        let (without, subtree) = map.split(DAppIdentifierPath { prefix: dapp(1), len: 8 });
        let restored = without.graft(&subtree);
        assert_eq!(restored.hash(), map.hash());
        assert_eq!(restored.capacity(), map.capacity());
    }

    #[test]
    fn single_key_updates_match_batch_roots_capacities_and_empty_dapp_cleanup() {
        let a = routing(1, 1);
        let b = routing(1, 2);
        let c = routing(2, 1);
        let operations = [
            (a, Some((account_info(1), AccountCapacity::new(10)))),
            (b, Some((AccountInfo::Redirect(dapp(3)), AccountCapacity::new(20)))),
            (c, Some((account_info(2), AccountCapacity::new(30)))),
            (a, Some((account_info(3), AccountCapacity::new(40)))),
            (b, None),
            (a, None),
            (a, None),
            (c, None),
        ];
        let mut single = ThreadAccountMap::new();
        let mut batch = ThreadAccountMap::new();
        for (routing, info) in operations {
            let previous = single.clone();
            let old_hash = previous.hash();
            let old_capacity = previous.capacity();
            single = single.update_single(&routing, info);
            batch = batch.update(&[(routing, info)]);
            assert_eq!(single.hash(), batch.hash());
            assert_eq!(single.capacity(), batch.capacity());
            assert_eq!(single.get(&routing), batch.get(&routing));
            assert_eq!(previous.hash(), old_hash);
            assert_eq!(previous.capacity(), old_capacity);
            assert!(single.dapps.iter().all(|(_, value)| !value.0.root.is_empty()));
        }
        assert!(single.is_empty());
        assert_eq!(single.capacity(), AccountCapacity::default());
    }

    #[test]
    fn capacity_is_derived_without_changing_the_merkle_hash() {
        let empty = ThreadAccountMap::new();
        let routing = routing(1, 2);
        let info = account_info(3);

        let small = empty.update(&[(routing, Some((info, AccountCapacity::new(100))))]);
        let large = empty.update(&[(routing, Some((info, AccountCapacity::new(10_000))))]);

        assert_eq!(small.hash(), large.hash());
        assert_eq!(
            small
                .dapps
                .get(&MapKey(*routing.dapp_id().as_array()))
                .unwrap()
                .0
                .root
                .augmentation()
                .bytes(),
            100,
        );
        assert_eq!(
            large
                .dapps
                .get(&MapKey(*routing.dapp_id().as_array()))
                .unwrap()
                .0
                .root
                .augmentation()
                .bytes(),
            10_000,
        );
    }

    #[test]
    fn capacity_survives_map_serialization() -> anyhow::Result<()> {
        let routing = routing(1, 2);
        let map = (ThreadAccountMap::new())
            .update(&[(routing, Some((account_info(3), AccountCapacity::new(7_654))))]);

        let mut bytes = Vec::new();
        map.write(&mut bytes)?;
        let restored = ThreadAccountMap::read(&mut bytes.as_slice())?;
        let restored_l2 = restored.dapps.get(&MapKey(*routing.dapp_id().as_array())).unwrap();

        assert_eq!(restored.hash(), map.hash());
        assert_eq!(restored_l2.0.root.augmentation().bytes(), 7_654);
        assert_eq!(restored.capacity().bytes(), 7_654);
        Ok(())
    }

    #[test]
    fn account_map_format_is_versioned_and_encoding_specific() -> anyhow::Result<()> {
        let map = (ThreadAccountMap::new())
            .update(&[(routing(1, 2), Some((account_info(3), AccountCapacity::new(100))))]);

        let mut hashed = Vec::new();
        map.write(&mut hashed)?;
        assert_eq!(&hashed[..ACCOUNT_MAP_FORMAT_MAGIC.len()], ACCOUNT_MAP_FORMAT_MAGIC);
        assert_eq!(hashed[ACCOUNT_MAP_FORMAT_MAGIC.len()], ACCOUNT_MAP_HASHED_ENCODING);
        assert!(ThreadAccountMap::read_no_hash(&mut hashed.as_slice()).is_err());

        let mut compact = Vec::new();
        map.write_no_hash(&mut compact)?;
        assert_eq!(&compact[..ACCOUNT_MAP_FORMAT_MAGIC.len()], ACCOUNT_MAP_FORMAT_MAGIC);
        assert_eq!(compact[ACCOUNT_MAP_FORMAT_MAGIC.len()], ACCOUNT_MAP_COMPACT_ENCODING);
        assert!(ThreadAccountMap::read(&mut compact.as_slice()).is_err());
        let restored = ThreadAccountMap::read_no_hash(&mut compact.as_slice())?;
        assert_eq!(restored.hash(), map.hash());
        assert_eq!(restored.capacity(), map.capacity());

        let mut previous = hashed.clone();
        previous[..8].copy_from_slice(b"ACMAPV2\0");
        assert!(ThreadAccountMap::read(&mut previous.as_slice()).is_err());
        let mut previous = compact.clone();
        previous[..8].copy_from_slice(b"ACMAPV2\0");
        assert!(ThreadAccountMap::read_no_hash(&mut previous.as_slice()).is_err());

        let mut legacy_unversioned = Vec::new();
        map.dapps.write(&mut legacy_unversioned)?;
        assert!(ThreadAccountMap::read(&mut legacy_unversioned.as_slice()).is_err());
        Ok(())
    }

    #[test]
    fn l1_capacity_sums_embedded_dapp_maps() {
        let map = (ThreadAccountMap::new()).update(&[
            (routing(1, 1), Some((account_info(1), AccountCapacity::new(100)))),
            (routing(1, 2), Some((account_info(2), AccountCapacity::new(200)))),
            (routing(2, 1), Some((account_info(3), AccountCapacity::new(400)))),
        ]);

        assert_eq!(map.dapp_capacity(&dapp(1)).map(AccountCapacity::bytes), Some(300));
        assert_eq!(map.dapp_capacity(&dapp(2)).map(AccountCapacity::bytes), Some(400));
        assert_eq!(map.capacity().bytes(), 700);
    }

    #[test]
    fn deserialization_rejects_l1_capacity_that_disagrees_with_l2() {
        let routing = routing(1, 2);
        let valid = (ThreadAccountMap::new())
            .update(&[(routing, Some((account_info(3), AccountCapacity::new(100))))]);
        let dapp_key = MapKey(*routing.dapp_id().as_array());
        let l2 = valid.dapps.get(&dapp_key).unwrap();
        let invalid = ThreadAccountMap {
            dapps: MultiMapAug::new()
                .update_single_aug(&dapp_key, Some((l2, AccountCapacity::new(99)))),
        };
        let mut bytes = Vec::new();
        invalid.write(&mut bytes).unwrap();

        assert!(ThreadAccountMap::read(&mut bytes.as_slice()).is_err());
    }

    #[test]
    #[should_panic(expected = "account capacity overflow")]
    fn capacity_overflow_is_an_invariant_violation() {
        AccountCapacity::combine(&AccountCapacity::new(u64::MAX), &AccountCapacity::new(1));
    }

    #[test]
    fn capacity_uses_one_u64_in_memory_and_on_disk() {
        assert_eq!(std::mem::size_of::<AccountCapacity>(), 8);
        let capacity = AccountCapacity::new(u64::MAX);
        let mut bytes = Vec::new();
        capacity.write(&mut bytes).unwrap();
        assert_eq!(bytes, u64::MAX.to_le_bytes());
        assert_eq!(AccountCapacity::read(&mut bytes.as_slice()).unwrap(), capacity);
        assert!(AccountCapacity::read(&mut &bytes[..7]).is_err());
    }

    #[test]
    fn two_level_map_is_independent_of_update_history() -> anyhow::Result<()> {
        let a = routing(1, 1);
        let b = routing(1, 2);
        let c = routing(2, 1);
        let final_a = (account_info(10), AccountCapacity::new(1_000));
        let final_b = (account_info(11), AccountCapacity::new(2_000));
        let final_c = (account_info(12), AccountCapacity::new(3_000));

        let direct = (ThreadAccountMap::new()).update(&[
            (c, Some(final_c)),
            (a, Some(final_a)),
            (b, Some(final_b)),
        ]);

        let mut history = ThreadAccountMap::new();
        history = history.update(&[
            (a, Some((account_info(20), AccountCapacity::new(20)))),
            (b, Some((account_info(21), AccountCapacity::new(21)))),
            (c, Some((account_info(22), AccountCapacity::new(22)))),
        ]);
        history = history.update(&[(b, None), (a, Some(final_a))]);
        history = history.update(&[(c, Some(final_c)), (b, Some(final_b))]);

        let mut direct_bytes = Vec::new();
        let mut history_bytes = Vec::new();
        direct.write(&mut direct_bytes)?;
        history.write(&mut history_bytes)?;

        assert_eq!(history.hash(), direct.hash());
        assert_eq!(history.capacity(), direct.capacity());
        assert_eq!(history_bytes, direct_bytes);
        Ok(())
    }
}
