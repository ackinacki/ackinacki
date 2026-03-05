use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use account_state::ThreadAccount;
use account_state::ThreadAccountsRepository;
use anyhow::ensure;
use node_types::AccountIdentifier;
use node_types::ThreadIdentifier;
use node_types::TransactionHash;

use crate::helper::get_temp_file_path;

pub type NodeThreadAccounts = account_state::FsCompositeThreadAccounts;
pub type NodeThreadAccountsRepository =
    <NodeThreadAccounts as account_state::ThreadAccounts>::Repository;
pub type NodeThreadAccountsRef = <NodeThreadAccounts as account_state::ThreadAccounts>::Ref;
pub type NodeThreadAccountsDiff = <NodeThreadAccounts as account_state::ThreadAccounts>::Diff;
pub type NodeThreadAccountsBuilder = <NodeThreadAccounts as account_state::ThreadAccounts>::Builder;

#[derive(Debug, Clone)]
pub struct AccountsRepository {
    data_dir: PathBuf,
    unload_after: Option<u32>,
    store_after: u32,
    deleted_accounts: Arc<Mutex<HashMap<ThreadIdentifier, BTreeMap<u64, Vec<AccountIdentifier>>>>>,
}

impl AccountsRepository {
    pub fn new(data_dir: PathBuf, unload_after: Option<u32>, store_after: u32) -> Self {
        Self {
            data_dir: data_dir.join("accounts"),
            unload_after,
            store_after,
            deleted_accounts: Default::default(),
        }
    }

    fn account_path(
        &self,
        account_id: &AccountIdentifier,
        last_trans_hash: &TransactionHash,
        last_trans_lt: u64,
    ) -> PathBuf {
        self.data_dir
            .join(account_id.to_hex_string())
            .join(format!("{last_trans_lt}_{last_trans_hash:x}"))
    }

    pub fn load_account(
        &self,
        account_id: &AccountIdentifier,
        last_trans_hash: TransactionHash,
        last_trans_lt: u64,
    ) -> anyhow::Result<ThreadAccount> {
        ensure!(self.unload_after.is_some(), "Tried to load account while unload is disabled");
        let path = self.account_path(account_id, &last_trans_hash, last_trans_lt);
        let data = std::fs::read(&path).map_err(|err| {
            anyhow::format_err!("Failed to read account {}: {err}", path.display())
        })?;
        ThreadAccount::read_bytes(&data)
    }

    pub fn store_account(
        &self,
        account_id: &AccountIdentifier,
        last_trans_hash: TransactionHash,
        last_trans_lt: u64,
        account: &ThreadAccount,
    ) -> anyhow::Result<()> {
        assert!(self.unload_after.is_some(), "Tried to store account while unload is disabled");
        let path = self.account_path(account_id, &last_trans_hash, last_trans_lt);
        let parent_dir = if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                anyhow::format_err!("Failed to create directory {}: {err}", parent.display())
            })?;
            parent.to_owned()
        } else {
            PathBuf::new()
        };
        let tmp_file_path = get_temp_file_path(&parent_dir);
        let data = account.write_bytes()?;
        let mut file = std::fs::File::create(&tmp_file_path).map_err(|err| {
            anyhow::format_err!("Failed to write account {}: {err}", tmp_file_path.display())
        })?;
        file.write_all(&data).map_err(|err| {
            anyhow::format_err!("Failed to write account {}: {err}", tmp_file_path.display())
        })?;

        if cfg!(feature = "sync_files") {
            file.sync_all()?;
        }
        drop(file);
        std::fs::rename(tmp_file_path, &path)?;
        tracing::trace!("File saved: {:?}", path);
        Ok(())
    }

    pub fn clear_old_accounts(
        &self,
        thread_id: &ThreadIdentifier,
        relevant_state: &NodeThreadAccountsRef,
        cut_lt: u64,
        thread_accounts_repository: &NodeThreadAccountsRepository,
    ) {
        thread_accounts_repository
            .state_iterate_all_accounts(relevant_state, |account_id, account| {
                let path = self.data_dir.join(account_id.to_hex_string());
                if let Ok(states) = std::fs::read_dir(path) {
                    for state in states.flatten() {
                        if let Some(state_name) = state.file_name().to_str() {
                            if let Some(Ok(state_lt)) =
                                state_name.split('_').next().map(|name| name.parse::<u64>())
                            {
                                if state_lt < account.last_trans_lt() {
                                    tracing::trace!(
                                        "Remove old account state: {}",
                                        state.path().display()
                                    );
                                    if let Err(err) = std::fs::remove_file(state.path()) {
                                        tracing::warn!(
                                            "Failed to remove old account state {}: {err}",
                                            state.path().display()
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(true)
            })
            .unwrap();

        let to_delete =
            if let Some(deleted) = self.deleted_accounts.lock().unwrap().get_mut(thread_id) {
                let remaining = deleted.split_off(&cut_lt);
                std::mem::replace(deleted, remaining)
            } else {
                BTreeMap::new()
            };
        for account_id in to_delete.values().flatten() {
            let mut remove_dir = true;
            let path = self.data_dir.join(account_id.to_hex_string());
            if let Ok(states) = std::fs::read_dir(&path) {
                for state in states.flatten() {
                    if let Some(state_name) = state.file_name().to_str() {
                        if let Some(Ok(state_lt)) =
                            state_name.split('_').next().map(|name| name.parse::<u64>())
                        {
                            if state_lt >= cut_lt {
                                remove_dir = false;
                            }
                        }
                    }
                }
                if remove_dir {
                    tracing::trace!("Remove account directory: {}", path.display());
                    std::fs::remove_dir(&path).ok();
                }
            }
        }
    }

    pub fn accounts_deleted(
        &self,
        thread_id: &ThreadIdentifier,
        accounts: Vec<AccountIdentifier>,
        lt: u64,
    ) {
        let mut deleted = self.deleted_accounts.lock().unwrap();
        let thread_deleted = deleted.entry(*thread_id).or_default();
        thread_deleted.insert(lt, accounts);
    }

    pub fn get_unload_after(&self) -> Option<u32> {
        self.unload_after
    }

    pub fn get_store_after(&self) -> u32 {
        self.store_after
    }
}
