use std::path::PathBuf;

use node_types::AccountIdentifier;
use trie_map::durable::DurableMapRef;
use trie_map::durable::DurableMapRepository;
use trie_map::MapKey;
use trie_map::TrieMapSnapshot;

use crate::thread_accounts::durable::dapp_accounts::AccountInfo;
use crate::thread_accounts::durable::dapp_accounts::DAppAccountMapRepository;
use crate::thread_accounts::DurableMapStat;
use crate::DAppAccountMapHash;

#[derive(Clone)]
pub struct FsTrieDAppAccountMapRepository {
    inner: DurableMapRepository<AccountInfo, DAppAccountMapHash, ()>,
}

impl FsTrieDAppAccountMapRepository {
    pub fn new(root_path: PathBuf) -> anyhow::Result<Self> {
        Ok(Self { inner: DurableMapRepository::load(root_path)? })
    }
}

impl DAppAccountMapRepository for FsTrieDAppAccountMapRepository {
    type MapRef = DurableMapRef;

    fn get_stat(&self) -> DurableMapStat {
        self.inner.get_stat()
    }

    fn commit(&self) -> anyhow::Result<()> {
        self.inner.commit()
    }

    fn new_map() -> Self::MapRef {
        DurableMapRepository::<AccountInfo, DAppAccountMapHash, ()>::new_map()
    }

    fn index_get(&self, map_hash: &DAppAccountMapHash) -> anyhow::Result<Option<Self::MapRef>> {
        Ok(self.inner.index_get(map_hash).map(|(map, _)| map))
    }

    fn index_set(&self, map_hash: &DAppAccountMapHash, map: &Self::MapRef) -> anyhow::Result<()> {
        self.inner.index_set(map_hash, map, ());
        Ok(())
    }

    fn map_hash(&self, map: &Self::MapRef) -> DAppAccountMapHash {
        DAppAccountMapHash::new(self.inner.map_hash(map).0)
    }

    fn map_get(
        &self,
        map: &Self::MapRef,
        account_id: &AccountIdentifier,
    ) -> anyhow::Result<Option<AccountInfo>> {
        self.inner.map_get(map, &MapKey(*account_id.as_array()))
    }

    fn map_update(
        &self,
        map: &Self::MapRef,
        accounts: Vec<(AccountIdentifier, Option<AccountInfo>)>,
    ) -> anyhow::Result<Self::MapRef> {
        let trie_patch = accounts
            .into_iter()
            .map(|(id, info)| (MapKey(*id.as_array()), info))
            .collect::<Vec<_>>();
        self.inner.map_update(map, &trie_patch)
    }

    fn export_snapshot(&self, map: &Self::MapRef) -> TrieMapSnapshot<AccountInfo> {
        self.inner.export_snapshot(map)
    }

    fn import_snapshot(&self, snapshot: TrieMapSnapshot<AccountInfo>) -> Self::MapRef {
        self.inner.import_snapshot(snapshot)
    }

    fn collect_values(&self, map: &Self::MapRef) -> Vec<(AccountIdentifier, AccountInfo)> {
        self.inner
            .collect_values(map)
            .into_iter()
            .map(|(key, val)| (AccountIdentifier::new(key.0), val))
            .collect()
    }
}
