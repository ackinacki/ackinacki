mod fs_trie;

pub use fs_trie::FsTrieThreadDAppMapRepository;
use node_types::BlockIdentifier;
use node_types::DAppIdentifier;
use node_types::DAppIdentifierPath;
use node_types::ThreadAccountsHash;
use trie_map::TrieMapSnapshot;

use crate::thread_accounts::DurableMapStat;
use crate::DAppAccountMapHash;

pub trait ThreadDAppMapRepository {
    type MapRef: Clone + Send + Sync;

    fn get_stat(&self) -> DurableMapStat;

    fn commit(&self) -> anyhow::Result<()>;

    fn new_map() -> Self::MapRef;

    fn index_get(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<Self::MapRef>>;
    fn index_set(&self, block_id: &BlockIdentifier, map: &Self::MapRef) -> anyhow::Result<()>;

    fn map_hash(&self, map: &Self::MapRef) -> ThreadAccountsHash;

    fn map_get(
        &self,
        map: &Self::MapRef,
        dapp_id: &DAppIdentifier,
    ) -> anyhow::Result<Option<DAppAccountMapHash>>;

    fn map_update(
        &self,
        map_ref: &Self::MapRef,
        updates: &[(DAppIdentifier, Option<DAppAccountMapHash>)],
    ) -> anyhow::Result<Self::MapRef>;

    fn map_split(
        &self,
        map_ref: &Self::MapRef,
        dapp_id_path: DAppIdentifierPath,
    ) -> anyhow::Result<(Self::MapRef, Self::MapRef)>;

    fn merge(&self, a: &Self::MapRef, b: &Self::MapRef) -> anyhow::Result<Self::MapRef>;

    fn export_snapshot(&self, map: &Self::MapRef) -> TrieMapSnapshot<DAppAccountMapHash>;

    fn import_snapshot(&self, snapshot: TrieMapSnapshot<DAppAccountMapHash>) -> Self::MapRef;

    fn collect_values(&self, map: &Self::MapRef) -> Vec<(DAppIdentifier, DAppAccountMapHash)>;
}
