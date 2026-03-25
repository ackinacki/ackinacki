use node_types::AccountHash;
use node_types::Blake3Hashable;
use node_types::DAppIdentifier;
use node_types::TransactionHash;
use serde::Deserialize;
use serde::Serialize;

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
