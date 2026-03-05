use std::path::PathBuf;

use trie_map::durable::DurableMapRef;

use crate::fs_utils::fs_repo;
use crate::thread_accounts::accounts::aerospike::AerospikeAccountsConfig;
use crate::thread_accounts::accounts::aerospike::DEFAULT_NUM_WRITE_THREADS;
use crate::thread_accounts::accounts::AccountsStore;
use crate::thread_accounts::composite::DEFAULT_APPLY_TO_DURABLE;
use crate::thread_accounts::durable::dapp_accounts::fs_trie::FsTrieDAppAccountMapRepository;
use crate::thread_accounts::durable::thread_dapps::FsTrieThreadDAppMapRepository;
use crate::thread_accounts::ThreadAccounts;
use crate::CompositeThreadAccountRepository;
use crate::CompositeThreadAccountsBuilder;
use crate::CompositeThreadAccountsDiff;
use crate::CompositeThreadAccountsRef;

type FsCompositeThreadAccountsRef = CompositeThreadAccountsRef<DurableMapRef>;
type FsCompositeThreadAccountsBuilder = CompositeThreadAccountsBuilder<
    FsTrieThreadDAppMapRepository,
    FsTrieDAppAccountMapRepository,
    AccountsStore,
>;
type FsCompositeThreadAccountsDiff = CompositeThreadAccountsDiff;
type FsCompositeThreadAccountsRepository = CompositeThreadAccountRepository<
    FsTrieThreadDAppMapRepository,
    FsTrieDAppAccountMapRepository,
    AccountsStore,
>;

pub struct FsCompositeThreadAccountsRepositoryBuilder {
    root_path: PathBuf,
    apply_to_durable: bool,
    aerospike: Option<AerospikeAccountsConfig>,
}

impl FsCompositeThreadAccountsRepositoryBuilder {
    pub fn build(
        self,
    ) -> anyhow::Result<<FsCompositeThreadAccounts as ThreadAccounts>::Repository> {
        let durable_path = self.root_path.join("durable");
        let accounts = if let Some(config) = self.aerospike {
            AccountsStore::aerospike(config)?
        } else {
            fs_repo(AccountsStore::fs_opt, durable_path.join("accounts"))?
        };
        Ok(CompositeThreadAccountRepository::new(
            fs_repo(FsTrieThreadDAppMapRepository::new, durable_path.join("thread_dapps"))?,
            fs_repo(FsTrieDAppAccountMapRepository::new, durable_path.join("dapp_accounts"))?,
            accounts,
            self.apply_to_durable,
        ))
    }

    pub fn set_apply_to_durable(mut self, apply_to_durable: bool) -> Self {
        self.apply_to_durable = apply_to_durable;
        self
    }

    pub fn set_accounts_aerospike_store(
        mut self,
        address: impl AsRef<str>,
        namespace: impl AsRef<str>,
        set: impl AsRef<str>,
    ) -> Self {
        self.aerospike = Some(AerospikeAccountsConfig {
            address: address.as_ref().to_string(),
            namespace: namespace.as_ref().to_string(),
            set: set.as_ref().to_string(),
            num_write_threads: DEFAULT_NUM_WRITE_THREADS,
        });
        self
    }

    pub fn set_accounts_aerospike_parallel_batch_mode(mut self, num_threads: usize) -> Self {
        if let Some(config) = &mut self.aerospike {
            config.num_write_threads = num_threads;
        }
        self
    }

    pub fn set_accounts_aerospike_sequential_batch_mode(mut self) -> Self {
        if let Some(config) = &mut self.aerospike {
            config.num_write_threads = 0;
        }
        self
    }
}

pub struct FsCompositeThreadAccounts;

impl FsCompositeThreadAccounts {
    pub fn new_repository(root_path: PathBuf) -> FsCompositeThreadAccountsRepositoryBuilder {
        FsCompositeThreadAccountsRepositoryBuilder {
            root_path,
            apply_to_durable: DEFAULT_APPLY_TO_DURABLE,
            aerospike: None,
        }
    }

    pub fn new_repository_with_apply_to_durable(
        root_path: PathBuf,
        apply_to_durable: bool,
    ) -> FsCompositeThreadAccountsRepositoryBuilder {
        FsCompositeThreadAccountsRepositoryBuilder { root_path, apply_to_durable, aerospike: None }
    }
}

impl ThreadAccounts for FsCompositeThreadAccounts {
    type Builder = FsCompositeThreadAccountsBuilder;
    type Diff = FsCompositeThreadAccountsDiff;
    type Ref = FsCompositeThreadAccountsRef;
    type Repository = FsCompositeThreadAccountsRepository;
}
