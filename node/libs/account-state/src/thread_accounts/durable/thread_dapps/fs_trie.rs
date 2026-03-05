use std::path::PathBuf;

use node_types::BlockIdentifier;
use node_types::DAppIdentifier;
use node_types::DAppIdentifierPath;
use node_types::ThreadAccountsHash;
use trie_map::durable::DurableMapRef;
use trie_map::durable::DurableMapRepository;
use trie_map::durable::DurableMapStat;
use trie_map::MapKey;
use trie_map::MapKeyPath;
use trie_map::TrieMapSnapshot;

use crate::thread_accounts::durable::thread_dapps::ThreadDAppMapRepository;
use crate::DAppAccountMapHash;

#[derive(Clone)]
pub struct FsTrieThreadDAppMapRepository {
    inner: DurableMapRepository<DAppAccountMapHash, BlockIdentifier, ()>,
}

impl FsTrieThreadDAppMapRepository {
    pub fn new(root_path: PathBuf) -> anyhow::Result<Self> {
        Ok(Self { inner: DurableMapRepository::load(root_path)? })
    }
}

impl ThreadDAppMapRepository for FsTrieThreadDAppMapRepository {
    type MapRef = DurableMapRef;

    fn get_stat(&self) -> DurableMapStat {
        self.inner.get_stat()
    }

    fn commit(&self) -> anyhow::Result<()> {
        self.inner.commit()
    }

    fn new_map() -> Self::MapRef {
        DurableMapRepository::<BlockIdentifier, DAppAccountMapHash, ()>::new_map()
    }

    fn index_get(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<Self::MapRef>> {
        Ok(self.inner.index_get(block_id).map(|(map, _)| map))
    }

    fn index_set(&self, block_id: &BlockIdentifier, map: &Self::MapRef) -> anyhow::Result<()> {
        self.inner.index_set(block_id, map, ());
        Ok(())
    }

    fn map_hash(&self, map: &Self::MapRef) -> ThreadAccountsHash {
        ThreadAccountsHash::new(self.inner.map_hash(map).0)
    }

    fn map_get(
        &self,
        map: &Self::MapRef,
        dapp_id: &DAppIdentifier,
    ) -> anyhow::Result<Option<DAppAccountMapHash>> {
        Ok(self
            .inner
            .map_get(map, &MapKey(*dapp_id.as_array()))?
            .map(|v| DAppAccountMapHash::new(*v.as_array())))
    }

    fn map_update(
        &self,
        map: &Self::MapRef,
        updates: &[(DAppIdentifier, Option<DAppAccountMapHash>)],
    ) -> anyhow::Result<Self::MapRef> {
        let updates = updates.iter().map(|(k, v)| (MapKey(*k.as_array()), *v)).collect::<Vec<_>>();
        self.inner.map_update(map, &updates)
    }

    fn map_split(
        &self,
        map: &Self::MapRef,
        dapp_id_path: DAppIdentifierPath,
    ) -> anyhow::Result<(Self::MapRef, Self::MapRef)> {
        let DAppIdentifierPath { prefix, len } = dapp_id_path;
        let (a, b) =
            self.inner.map_split(map, MapKeyPath { prefix: MapKey(*prefix.as_array()), len })?;
        Ok((a, b))
    }

    fn merge(&self, a: &Self::MapRef, b: &Self::MapRef) -> anyhow::Result<Self::MapRef> {
        self.inner.merge(a, b)
    }

    fn export_snapshot(&self, map: &Self::MapRef) -> TrieMapSnapshot<DAppAccountMapHash> {
        self.inner.export_snapshot(map)
    }

    fn import_snapshot(&self, snapshot: TrieMapSnapshot<DAppAccountMapHash>) -> Self::MapRef {
        self.inner.import_snapshot(snapshot)
    }

    fn collect_values(&self, map: &Self::MapRef) -> Vec<(DAppIdentifier, DAppAccountMapHash)> {
        self.inner
            .collect_values(map)
            .into_iter()
            .map(|(key, val)| (DAppIdentifier::new(key.0), val))
            .collect()
    }
}
