mod composite;
mod durable;
mod tvm;

pub use composite::fs::FsCompositeThreadAccounts;
pub use composite::CompositeThreadAccountRepository;
pub use composite::CompositeThreadAccountsBuilder;
pub use composite::CompositeThreadAccountsDiff;
pub use composite::CompositeThreadAccountsRef;
pub use durable::*;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifierPath;
use node_types::ThreadAccountsHash;
use tvm_block::ShardStateUnsplit;

use crate::ThreadStateAccount;

pub struct ThreadAccountsSplit<StateRef> {
    pub without_branch: StateRef,
    pub branch: StateRef,
}

pub struct ThreadAccountsTransition<StateRef, StateDiff> {
    pub new_state: StateRef,
    pub diff: StateDiff,
}

pub trait ThreadAccounts {
    type Ref;
    type Builder: ThreadAccountsBuilder<StateRef = Self::Ref, StateDiff = Self::Diff>;
    type Diff;
    type Repository: ThreadAccountsRepository<StateRef = Self::Ref, StateDiff = Self::Diff>;
}

pub trait ThreadAccountsBuilder {
    type StateRef;
    type StateDiff;

    fn set_seq_no(&mut self, seq_no: u32);

    fn account(&self, routing: &AccountRouting) -> anyhow::Result<Option<ThreadStateAccount>>;

    fn insert_account(&mut self, routing: &AccountRouting, account: &ThreadStateAccount);

    fn remove_account(&mut self, routing: &AccountRouting);

    fn move_from_tvm(&mut self, limit: usize) -> anyhow::Result<()>;

    fn build(
        self,
        tvm_usage_tree: Option<&tvm_types::UsageTree>,
    ) -> anyhow::Result<ThreadAccountsTransition<Self::StateRef, Self::StateDiff>>;
}

pub trait ThreadAccountsRepository {
    type StateRef;
    type StateBuilder: ThreadAccountsBuilder<StateRef = Self::StateRef, StateDiff = Self::StateDiff>;
    type StateDiff;

    fn new_state() -> Self::StateRef;

    fn state_builder(&self, original: &Self::StateRef) -> Self::StateBuilder;

    fn state_split(
        &self,
        state: &Self::StateRef,
        dapp_id_path: DAppIdentifierPath,
    ) -> anyhow::Result<ThreadAccountsSplit<Self::StateRef>>;

    fn merge(&self, a: &Self::StateRef, b: &Self::StateRef) -> anyhow::Result<Self::StateRef>;

    fn state_apply_diff(
        &self,
        prev_state: &Self::StateRef,
        diff: Self::StateDiff,
    ) -> anyhow::Result<Self::StateRef>;

    fn get_state(
        &self,
        block_id: &BlockIdentifier,
        tvm: ShardStateUnsplit,
    ) -> anyhow::Result<Self::StateRef>;

    fn set_state(&self, block_id: &BlockIdentifier, state: &Self::StateRef) -> anyhow::Result<()>;

    fn commit(&self) -> anyhow::Result<()>;

    fn state_hash(&self, state: &Self::StateRef) -> ThreadAccountsHash;

    fn state_seq_no(&self, state: &Self::StateRef) -> u32;

    fn state_global_id(&self, state: &Self::StateRef) -> i32;

    fn state_account(
        &self,
        state: &Self::StateRef,
        routing: &AccountRouting,
    ) -> anyhow::Result<Option<ThreadStateAccount>>;

    fn state_iterate_all_accounts(
        &self,
        state: &Self::StateRef,
        it: impl FnMut(&AccountRouting, ThreadStateAccount) -> anyhow::Result<bool>,
    ) -> anyhow::Result<()>;
}
