use std::path::PathBuf;

use node_types::AccountHash;

use crate::thread_accounts::accounts::aerospike::AerospikeAccountsConfig;
use crate::thread_accounts::accounts::aerospike::AerospikeAccountsStore;
use crate::thread_accounts::accounts::fs::FsAccountsStore;
use crate::thread_accounts::accounts::fs_opt::FsOptAccountsStore;
use crate::ThreadAccount;

pub mod aerospike;
#[cfg(test)]
mod aerospike_tests;
pub mod fs;
pub mod fs_opt;

#[derive(Debug, Default)]
pub struct AerospikeAccountsCacheStat {
    pub cache_len: usize,
    pub pending_len: usize,
}

pub trait AccountsRepository: Clone {
    fn get(&self, hash: &AccountHash) -> anyhow::Result<Option<ThreadAccount>>;
    fn iter(
        &self,
        hashes: impl IntoIterator<Item = AccountHash> + Send,
    ) -> impl Iterator<Item = anyhow::Result<ThreadAccount>> {
        self.iter_rx(hashes).into_iter()
    }
    fn iter_rx(
        &self,
        hashes: impl IntoIterator<Item = AccountHash> + Send,
    ) -> std::sync::mpsc::Receiver<anyhow::Result<ThreadAccount>>;
    fn update(&self, accounts: &[(AccountHash, Option<ThreadAccount>)]) -> anyhow::Result<()>;
    fn commit(&self) -> anyhow::Result<()>;
    fn aerospike_cache_stat(&self) -> Option<AerospikeAccountsCacheStat> {
        None
    }
}

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum AccountsStore {
    Aerospike(AerospikeAccountsStore),
    Fs(FsAccountsStore),
    FsOpt(FsOptAccountsStore),
}

impl AccountsStore {
    pub fn aerospike(config: AerospikeAccountsConfig) -> anyhow::Result<Self> {
        Ok(Self::Aerospike(AerospikeAccountsStore::new(config)?))
    }

    pub fn fs(root_path: PathBuf) -> anyhow::Result<Self> {
        Ok(Self::Fs(FsAccountsStore::new(root_path)?))
    }

    pub fn fs_opt(root_path: PathBuf) -> anyhow::Result<Self> {
        Ok(Self::FsOpt(FsOptAccountsStore::new(root_path)?))
    }
}

impl AccountsRepository for AccountsStore {
    fn get(&self, hash: &AccountHash) -> anyhow::Result<Option<ThreadAccount>> {
        match self {
            AccountsStore::Aerospike(repo) => repo.get(hash),
            AccountsStore::Fs(repo) => repo.get(hash),
            AccountsStore::FsOpt(repo) => repo.get(hash),
        }
    }

    fn iter(
        &self,
        hashes: impl IntoIterator<Item = AccountHash> + Send,
    ) -> impl Iterator<Item = anyhow::Result<ThreadAccount>> {
        match self {
            AccountsStore::Aerospike(repo) => repo.iter_rx(hashes),
            AccountsStore::Fs(repo) => repo.iter_rx(hashes),
            AccountsStore::FsOpt(repo) => repo.iter_rx(hashes),
        }
        .into_iter()
    }

    fn iter_rx(
        &self,
        hashes: impl IntoIterator<Item = AccountHash> + Send,
    ) -> std::sync::mpsc::Receiver<anyhow::Result<ThreadAccount>> {
        match self {
            AccountsStore::Aerospike(repo) => repo.iter_rx(hashes),
            AccountsStore::Fs(repo) => repo.iter_rx(hashes),
            AccountsStore::FsOpt(repo) => repo.iter_rx(hashes),
        }
    }

    fn update(&self, accounts: &[(AccountHash, Option<ThreadAccount>)]) -> anyhow::Result<()> {
        match self {
            AccountsStore::Aerospike(repo) => repo.update(accounts),
            AccountsStore::Fs(repo) => repo.update(accounts),
            AccountsStore::FsOpt(repo) => repo.update(accounts),
        }
    }

    fn commit(&self) -> anyhow::Result<()> {
        match self {
            AccountsStore::Aerospike(repo) => repo.commit(),
            AccountsStore::Fs(repo) => repo.commit(),
            AccountsStore::FsOpt(repo) => repo.commit(),
        }
    }

    fn aerospike_cache_stat(&self) -> Option<AerospikeAccountsCacheStat> {
        match self {
            AccountsStore::Aerospike(repo) => Some(AerospikeAccountsCacheStat {
                cache_len: repo.cache_len(),
                pending_len: repo.pending_len(),
            }),
            _ => None,
        }
    }
}
