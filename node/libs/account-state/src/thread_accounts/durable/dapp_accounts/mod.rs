pub(crate) mod fs_trie;

use node_types::AccountHash;
use node_types::AccountIdentifier;
use node_types::Blake3Hashable;
use node_types::DAppIdentifier;
use node_types::TransactionHash;
use serde::Deserialize;
use serde::Serialize;
use trie_map::TrieMapSnapshot;

use crate::thread_accounts::DurableMapStat;
use crate::DAppAccountMapHash;

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct AccountInfo {
    pub last_trans_hash: TransactionHash,
    pub last_trans_lt: u64,
    pub account_hash: AccountHash,
    /// When set, this entry is a redirect stub pointing to the given dApp.
    /// No real account data is stored in the accounts table.
    pub redirect_dapp_id: Option<DAppIdentifier>,
}

impl Blake3Hashable for AccountInfo {
    fn update_hasher(&self, hasher: &mut blake3::Hasher) {
        hasher.update(self.last_trans_hash.as_slice());
        hasher.update(&self.last_trans_lt.to_be_bytes());
        hasher.update(self.account_hash.as_slice());
        if let Some(dapp_id) = &self.redirect_dapp_id {
            hasher.update(dapp_id.as_slice());
        }
    }
}

pub trait DAppAccountMapRepository: Clone {
    type MapRef: Clone + Send + Sync;

    fn get_stat(&self) -> DurableMapStat;

    fn commit(&self) -> anyhow::Result<()>;

    fn new_map() -> Self::MapRef;

    fn index_get(&self, map_hash: &DAppAccountMapHash) -> anyhow::Result<Option<Self::MapRef>>;

    fn index_set(&self, map_hash: &DAppAccountMapHash, map: &Self::MapRef) -> anyhow::Result<()>;

    fn map_hash(&self, map: &Self::MapRef) -> DAppAccountMapHash;

    fn map_get(
        &self,
        map: &Self::MapRef,
        account_id: &AccountIdentifier,
    ) -> anyhow::Result<Option<AccountInfo>>;

    fn map_update(
        &self,
        map: &Self::MapRef,
        accounts: Vec<(AccountIdentifier, Option<AccountInfo>)>,
    ) -> anyhow::Result<Self::MapRef>;

    fn export_snapshot(&self, map: &Self::MapRef) -> TrieMapSnapshot<AccountInfo>;

    fn import_snapshot(&self, snapshot: TrieMapSnapshot<AccountInfo>) -> Self::MapRef;

    fn collect_values(&self, map: &Self::MapRef) -> Vec<(AccountIdentifier, AccountInfo)>;
}
