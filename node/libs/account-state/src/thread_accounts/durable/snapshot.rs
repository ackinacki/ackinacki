use node_types::AccountHash;
use serde::Deserialize;
use serde::Serialize;
use trie_map::TrieMapSnapshot;

use crate::thread_accounts::durable::dapp_accounts::AccountInfo;
use crate::DAppAccountMapHash;

#[derive(Clone, Serialize, Deserialize)]
pub struct CompositeDurableStateSnapshot {
    pub thread_dapps: TrieMapSnapshot<DAppAccountMapHash>,
    pub dapp_accounts: Vec<(DAppAccountMapHash, TrieMapSnapshot<AccountInfo>)>,
    pub accounts: Vec<(AccountHash, Vec<u8>)>,
}
