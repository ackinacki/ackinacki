use std::fmt::Debug;
use std::fmt::Formatter;

use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::ThreadAccountsHash;
use tvm_block::Deserializable;
use tvm_block::GetRepresentationHash;
use tvm_block::MerkleUpdate;
use tvm_block::Serializable;
use tvm_block::ShardAccounts;
use tvm_block::ShardStateUnsplit;

use crate::BlockAccountOperation;
use crate::ThreadAccount;

#[derive(Clone, Debug)]
pub struct TvmThreadAccountsStateDiff {
    pub update: MerkleUpdate,
}

impl From<MerkleUpdate> for TvmThreadAccountsStateDiff {
    fn from(update: MerkleUpdate) -> Self {
        Self { update }
    }
}

#[derive(Clone)]
pub struct TvmThreadAccountsState {
    pub shard_state: ShardStateUnsplit,
    pub shard_accounts: ShardAccounts,
}

impl Debug for TvmThreadAccountsState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TvmThreadState {}", self.shard_state.hash().unwrap_or_default())
    }
}

impl Default for TvmThreadAccountsState {
    fn default() -> Self {
        let shard_state = ShardStateUnsplit::default();
        let shard_accounts = shard_state.read_accounts().unwrap();
        Self { shard_state, shard_accounts }
    }
}

impl TvmThreadAccountsState {
    pub fn with_shard_state(shard_state: ShardStateUnsplit) -> anyhow::Result<Self> {
        let shard_accounts = shard_state
            .read_accounts()
            .map_err(|err| anyhow::anyhow!("Failed to read shard accounts: {}", err))?;
        Ok(Self { shard_state, shard_accounts })
    }

    pub fn hash(&self) -> ThreadAccountsHash {
        self.shard_state.hash().unwrap_or_default().into()
    }

    pub fn global_id(&self) -> i32 {
        self.shard_state.global_id()
    }

    pub fn seq_no(&self) -> u32 {
        self.shard_state.seq_no()
    }

    pub fn account(
        &self,
        account_address: &AccountRouting,
    ) -> anyhow::Result<Option<ThreadAccount>> {
        self.shard_accounts
            .account(&account_address.account_id().into())
            .map_err(|err| anyhow::anyhow!("Failed to get TVM account: {}", err))
            .map(|x| x.map(From::from))
    }

    pub fn iterate_accounts(
        &self,
        mut it: impl FnMut(&AccountIdentifier, ThreadAccount) -> anyhow::Result<bool>,
    ) -> anyhow::Result<()> {
        self.shard_accounts
            .iterate_accounts(|address, account| {
                it(&AccountIdentifier::from(address), account.into())
                    .map_err(|err| tvm_types::error!("{}", err))
            })
            .map_err(|err| anyhow::anyhow!("{}", err))?;
        Ok(())
    }

    pub fn apply_diff(&self, diff: &TvmThreadAccountsStateDiff) -> anyhow::Result<Self> {
        let old_state_cell = self
            .shard_state
            .serialize()
            .map_err(|err| anyhow::anyhow!("Failed to serialize shard state: {}", err))?;
        let new_state_cell = diff
            .update
            .apply_for(&old_state_cell)
            .map_err(|err| anyhow::anyhow!("Failed to apply merkle update: {err}"))?;
        let new_state = ShardStateUnsplit::construct_from_cell(new_state_cell)
            .map_err(|err| anyhow::anyhow!("Failed to construct shard state from cell: {err}"))?;
        Self::with_shard_state(new_state)
    }
}

pub(crate) fn patch_account(
    address: AccountIdentifier,
    account: BlockAccountOperation,
    accounts: &mut ShardAccounts,
) -> anyhow::Result<()> {
    match account {
        BlockAccountOperation::UpdateOrInsert(account) => {
            accounts
                .insert(&address.into(), &account.try_into()?)
                .map_err(|err| anyhow::anyhow!("Failed to insert TVM account: {}", err))?;
        }
        BlockAccountOperation::Remove | BlockAccountOperation::MoveFromTvm => {
            accounts
                .remove(&address.into())
                .map_err(|err| anyhow::anyhow!("Failed to remove TVM account: {}", err))?;
        }
        BlockAccountOperation::AccountMerkleUpdate(update_bytes) => {
            let old_shard_acc = accounts
                .account(&address.into())
                .map_err(|e| anyhow::anyhow!("Failed to read TVM account for merkle update: {e}"))?
                .ok_or_else(|| {
                    anyhow::anyhow!("Account merkle update for non-existent TVM account")
                })?;

            let old_bytes = old_shard_acc
                .write_to_bytes()
                .map_err(|e| anyhow::anyhow!("Failed to serialize old account: {e}"))?;
            let old_cell = tvm_types::read_single_root_boc(&old_bytes)
                .map_err(|e| anyhow::anyhow!("Failed to read old account BOC: {e}"))?;

            let update_cell = tvm_types::read_single_root_boc(&update_bytes)
                .map_err(|e| anyhow::anyhow!("Failed to read merkle update BOC: {e}"))?;
            let merkle_update = MerkleUpdate::construct_from_cell(update_cell)
                .map_err(|e| anyhow::anyhow!("Failed to construct merkle update: {e}"))?;

            let new_cell = merkle_update
                .apply_for(&old_cell)
                .map_err(|e| anyhow::anyhow!("Failed to apply merkle update: {e}"))?;

            let new_shard_acc = tvm_block::ShardAccount::construct_from_cell(new_cell)
                .map_err(|e| anyhow::anyhow!("Failed to construct updated account: {e}"))?;

            accounts
                .insert(&address.into(), &new_shard_acc)
                .map_err(|err| anyhow::anyhow!("Failed to insert updated TVM account: {}", err))?;
        }
    }
    Ok(())
}
