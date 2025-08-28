// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use parking_lot::Mutex;
use tvm_types::UInt256;

use crate::repository::optimistic_state::OptimisticState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::ThreadIdentifier;

pub fn get_account_from_shard_state(
    repository: Arc<Mutex<RepositoryImpl>>,
    account_address: &str,
) -> anyhow::Result<(tvm_block::Account, Option<UInt256>)> {
    tracing::trace!("get_account_from_shard_state: address={account_address}");

    let repo = repository.lock();

    let state = repo
        .last_finalized_optimistic_state(&ThreadIdentifier::default())
        .ok_or_else(|| anyhow::anyhow!("Shard state not found"))?;

    let acc_id = tvm_types::AccountId::from_string(account_address)
        .map_err(|_| anyhow::anyhow!("Invalid account address"))?
        .into();

    let thread_id =
        state.get_thread_for_account(&acc_id).map_err(|_| anyhow::anyhow!("Account not found"))?;

    let shard_state = repo
        .last_finalized_optimistic_state(&thread_id)
        .ok_or_else(|| anyhow::anyhow!("Shard state for thread_id {thread_id} not found"))?
        .get_shard_state()
        .clone();

    let accounts = shard_state
        .read_accounts()
        .map_err(|e| anyhow::anyhow!("Can't read accounts from shard state: {e}"))?;

    let mut acc = accounts
        .account(&acc_id.clone().into())
        .map_err(|e| anyhow::anyhow!("Can't find account in shard state: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("Can't find account in shard state"))?;

    if acc.is_external() {
        // TODO:
        // verify with @vasily. It does seem like a bug
        // I guess it must be account_thread_state
        let root = match state.cached_accounts.get(&acc_id) {
            Some((_, acc_root)) => acc_root.clone(),
            None => repo.accounts_repository().load_account(
                &acc_id,
                acc.last_trans_hash(),
                acc.last_trans_lt(),
            )?,
        };
        if root.repr_hash() != acc.account_cell().repr_hash() {
            anyhow::bail!("External account cell hash mismatch");
        }
        acc.set_account_cell(root);
    }
    let dapp_id = acc.get_dapp_id().cloned();
    let account =
        acc.read_account().and_then(|acc| acc.as_struct()).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok((account, dapp_id))
}
