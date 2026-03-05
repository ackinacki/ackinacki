use std::sync::Arc;

use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifierPath;
use node_types::ThreadAccountsHash;
use tvm_block::Deserializable;
use tvm_block::Serializable;
use tvm_block::ShardStateUnsplit;
use tvm_types::Cell;
use tvm_types::UsageTree;

use crate::thread_accounts::composite::builder::CompositeThreadAccountsBuilder;
use crate::thread_accounts::composite::repository_inner::CompositeThreadAccountsRepositoryInner;
use crate::thread_accounts::durable::snapshot::CompositeDurableStateSnapshot;
use crate::thread_accounts::durable::DurableThreadAccountsDiff;
use crate::thread_accounts::tvm::TvmThreadState;
use crate::thread_accounts::tvm::TvmThreadStateDiff;
use crate::thread_accounts::AccountsRepository;
use crate::thread_accounts::DAppAccountMapRepository;
use crate::thread_accounts::DurableRepoStat;
use crate::thread_accounts::ThreadAccountsRepository;
use crate::thread_accounts::ThreadAccountsSplit;
use crate::thread_accounts::ThreadDAppMapRepository;
use crate::DurableThreadAccountsRepository;
use crate::ThreadStateAccount;

pub struct CompositeThreadAccountsDiff {
    pub durable: DurableThreadAccountsDiff,
    pub tvm: TvmThreadStateDiff,
}

#[derive(Clone, Debug)]
pub struct CompositeThreadAccountsRef<DurableRef> {
    pub durable: DurableRef,
    pub tvm: TvmThreadState,
}

impl<DurableRef: Clone> CompositeThreadAccountsRef<DurableRef> {
    pub fn with_tvm_usage_tree(&self) -> anyhow::Result<(Self, UsageTree)> {
        let usage_tree = UsageTree::with_params(self.tvm_cell()?, true);
        let shard_state = ShardStateUnsplit::construct_from_cell(usage_tree.root_cell())
            .map_err(|err| anyhow::format_err!("Failed to deserialize shard state: {}", err))?;
        let state = Self {
            durable: self.durable.clone(),
            tvm: TvmThreadState::with_shard_state(shard_state)?,
        };
        Ok((state, usage_tree))
    }

    pub fn tvm_cell(&self) -> anyhow::Result<Cell> {
        self.tvm
            .shard_state
            .serialize()
            .map_err(|err| anyhow::anyhow!("Failed to serialize shard state: {err}"))
    }
}

// Composite thread accounts state
// Includes two parts:
// - legacy TVM shard state;
// - new durable state v2.
//
#[derive(Clone)]
pub struct CompositeThreadAccountRepository<ThreadDAppsRepo, DAppAccountsRepo, AccountsRepo>(
    pub(crate)  Arc<
        CompositeThreadAccountsRepositoryInner<ThreadDAppsRepo, DAppAccountsRepo, AccountsRepo>,
    >,
)
where
    ThreadDAppsRepo: ThreadDAppMapRepository,
    DAppAccountsRepo: DAppAccountMapRepository,
    AccountsRepo: AccountsRepository;

impl<ThreadDAppsRepo, DAppAccountsRepo, AccountsRepo>
    CompositeThreadAccountRepository<ThreadDAppsRepo, DAppAccountsRepo, AccountsRepo>
where
    ThreadDAppsRepo: ThreadDAppMapRepository,
    DAppAccountsRepo: DAppAccountMapRepository,
    AccountsRepo: AccountsRepository,
{
    pub fn new(
        thread_dapps: ThreadDAppsRepo,
        dapp_accounts: DAppAccountsRepo,
        accounts: AccountsRepo,
        apply_to_durable: bool,
    ) -> Self {
        Self(Arc::new(CompositeThreadAccountsRepositoryInner {
            durable: DurableThreadAccountsRepository::new(thread_dapps, dapp_accounts, accounts),
            apply_to_durable,
        }))
    }

    pub fn get_durable_stat(&self) -> DurableRepoStat {
        self.0.durable.get_stat()
    }

    pub fn aerospike_cache_stat(
        &self,
    ) -> Option<crate::thread_accounts::durable::AerospikeAccountsCacheStat> {
        self.0.durable.aerospike_cache_stat()
    }

    pub fn state_with_tvm_cell_and_empty_durable_state(
        cell: Cell,
    ) -> anyhow::Result<CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>> {
        let shard_state = ShardStateUnsplit::construct_from_cell(cell)
            .map_err(|err| anyhow::format_err!("Failed to deserialize shard state: {}", err))?;
        Ok(CompositeThreadAccountsRef::<ThreadDAppsRepo::MapRef> {
            durable: ThreadDAppsRepo::new_map(),
            tvm: TvmThreadState::with_shard_state(shard_state)?,
        })
    }

    pub fn state_with_tvm_cell(
        &self,
        block_id: &BlockIdentifier,
        cell: Cell,
    ) -> anyhow::Result<CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>> {
        let shard_state = ShardStateUnsplit::construct_from_cell(cell)
            .map_err(|err| anyhow::format_err!("Failed to deserialize shard state: {}", err))?;
        self.state_with_tvm_state(block_id, shard_state)
    }

    pub fn state_with_tvm_state(
        &self,
        block_id: &BlockIdentifier,
        shard_state: ShardStateUnsplit,
    ) -> anyhow::Result<CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>> {
        let durable =
            self.0.durable.get_state(block_id)?.unwrap_or_else(|| ThreadDAppsRepo::new_map());
        Ok(CompositeThreadAccountsRef::<ThreadDAppsRepo::MapRef> {
            durable,
            tvm: TvmThreadState::with_shard_state(shard_state)?,
        })
    }

    pub fn state_to_tvm_cell(
        state: &CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>,
    ) -> anyhow::Result<Cell> {
        state
            .tvm
            .shard_state
            .serialize()
            .map_err(|err| anyhow::anyhow!("Failed to serialize shard state: {err}"))
    }

    pub fn export_durable_snapshot(
        &self,
        state: &CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>,
    ) -> anyhow::Result<CompositeDurableStateSnapshot> {
        self.0.durable.export_durable_snapshot(&state.durable)
    }

    pub fn import_durable_snapshot(
        &self,
        snapshot: CompositeDurableStateSnapshot,
    ) -> anyhow::Result<ThreadDAppsRepo::MapRef> {
        self.0.durable.import_durable_snapshot(snapshot)
    }
}

impl<ThreadDAppsRepo, DAppAccountsRepo, AccountsRepo> ThreadAccountsRepository
    for CompositeThreadAccountRepository<ThreadDAppsRepo, DAppAccountsRepo, AccountsRepo>
where
    ThreadDAppsRepo: ThreadDAppMapRepository,
    DAppAccountsRepo: DAppAccountMapRepository,
    AccountsRepo: AccountsRepository,
{
    type StateBuilder =
        CompositeThreadAccountsBuilder<ThreadDAppsRepo, DAppAccountsRepo, AccountsRepo>;
    type StateDiff = CompositeThreadAccountsDiff;
    type StateRef = CompositeThreadAccountsRef<ThreadDAppsRepo::MapRef>;

    fn new_state() -> Self::StateRef {
        CompositeThreadAccountsRef {
            durable: ThreadDAppsRepo::new_map(),
            tvm: TvmThreadState::with_shard_state(ShardStateUnsplit::default()).unwrap(),
        }
    }

    fn state_builder(&self, original: &Self::StateRef) -> Self::StateBuilder {
        Self::StateBuilder::new(self.0.clone(), original.clone())
    }

    fn state_split(
        &self,
        state: &Self::StateRef,
        dapp_id_path: DAppIdentifierPath,
    ) -> anyhow::Result<ThreadAccountsSplit<Self::StateRef>> {
        self.0.state_split(state, dapp_id_path)
    }

    fn merge(&self, a: &Self::StateRef, b: &Self::StateRef) -> anyhow::Result<Self::StateRef> {
        self.0.merge(a, b)
    }

    fn state_apply_diff(
        &self,
        prev_state: &Self::StateRef,
        diff: Self::StateDiff,
    ) -> anyhow::Result<Self::StateRef> {
        self.0.state_apply_diff(prev_state, diff)
    }

    fn get_state(
        &self,
        block_id: &BlockIdentifier,
        tvm: ShardStateUnsplit,
    ) -> anyhow::Result<Self::StateRef> {
        self.0.get_state(block_id, tvm)
    }

    fn set_state(&self, block_id: &BlockIdentifier, state: &Self::StateRef) -> anyhow::Result<()> {
        self.0.set_state(block_id, state)
    }

    fn commit(&self) -> anyhow::Result<()> {
        self.0.commit()
    }

    fn state_hash(&self, state: &Self::StateRef) -> ThreadAccountsHash {
        state.tvm.hash()
    }

    fn state_seq_no(&self, state: &Self::StateRef) -> u32 {
        state.tvm.seq_no()
    }

    fn state_global_id(&self, state: &Self::StateRef) -> i32 {
        state.tvm.global_id()
    }

    fn state_account(
        &self,
        state: &Self::StateRef,
        routing: &AccountRouting,
    ) -> anyhow::Result<Option<ThreadStateAccount>> {
        self.0.state_account(state, routing)
    }

    fn state_iterate_all_accounts(
        &self,
        state: &Self::StateRef,
        it: impl FnMut(&AccountRouting, ThreadStateAccount) -> anyhow::Result<bool>,
    ) -> anyhow::Result<()> {
        self.0.state_iterate_all_accounts(state, it)
    }
}
