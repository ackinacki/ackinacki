// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use parking_lot::Mutex;
use tvm_types::UInt256;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::AccountAddress;
use crate::types::AccountRouting;
use crate::types::DAppIdentifier;
use crate::types::ThreadIdentifier;

pub fn get_account_from_shard_state(
    repository: Arc<Mutex<RepositoryImpl>>,
    account_address: &str,
) -> anyhow::Result<(tvm_block::Account, Option<UInt256>, u64)> {
    tracing::trace!("get_account_from_shard_state: address={account_address}");

    let repo = repository.lock();

    let acc_id: AccountAddress = tvm_types::AccountId::from_string(account_address)
        .map_err(|_| anyhow::anyhow!("Invalid account address"))?
        .into();

    let default_state = repo
        .last_finalized_optimistic_state(&ThreadIdentifier::default())
        .ok_or_else(|| anyhow::anyhow!("Shard state for default thread not found"))?;

    let default_routing = AccountRouting(DAppIdentifier(acc_id.clone()), acc_id.clone());
    tracing::trace!("get_account_from_shard_state: default_routing={default_routing:?}");
    let potential_thread = default_state.threads_table.find_match(&default_routing);
    tracing::trace!("get_account_from_shard_state: potential_thread={potential_thread:?}");
    let no_dapp_state = repo
        .last_finalized_optimistic_state(&potential_thread)
        .ok_or_else(|| anyhow::anyhow!("Failed to load no dapp state"))?;
    tracing::trace!("Loaded no_dapp_state");
    let mut final_state = no_dapp_state;
    let no_dapp_state = final_state.get_shard_state();
    let accounts = no_dapp_state
        .read_accounts()
        .map_err(|e| anyhow::anyhow!("Can't read accounts from shard state: {e}"))?;

    let mut acc = accounts
        .account(&acc_id.clone().into())
        .map_err(|e| anyhow::anyhow!("Can't find account in shard state: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("Can't find account in shard state"))?;

    let state_timestamp = repo
        .get_block_from_repo_or_archive(&default_state.block_id, &default_state.thread_id)?
        .data()
        .time()?;
    tracing::trace!("Loaded acc from no_dapp_state (timestamp: {state_timestamp})");
    if acc.is_redirect() {
        tracing::trace!("Account is redirect");
        let actual_dapp_id = acc
            .get_dapp_id()
            .ok_or(anyhow::format_err!("DApp ID must be set for redirect account"))?;
        let actual_routing = AccountRouting(actual_dapp_id.clone().into(), acc_id.clone());
        tracing::trace!("get_account_from_shard_state: actual_routing={actual_routing:?}");
        let actual_thread = default_state.threads_table.find_match(&actual_routing);
        tracing::trace!("get_account_from_shard_state: actual_thread={actual_thread:?}");
        let actual_state = repo
            .last_finalized_optimistic_state(&actual_thread)
            .ok_or_else(|| anyhow::anyhow!("Failed to load no dapp state"))?;
        final_state = actual_state;
        let actual_state = final_state.get_shard_state();
        let accounts = actual_state
            .read_accounts()
            .map_err(|e| anyhow::anyhow!("Can't read accounts from shard state: {e}"))?;

        acc = accounts
            .account(&acc_id.clone().into())
            .map_err(|e| anyhow::anyhow!("Can't find account in shard state: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("Can't find account in shard state"))?;
    }
    tracing::trace!("Loaded acc");

    if acc.is_external() {
        tracing::trace!("Account is external");
        // TODO:
        // verify with @vasily. It does seem like a bug
        // I guess it must be account_thread_state
        let root = match final_state.cached_accounts.get(&acc_id) {
            Some((_, acc_root)) => acc_root.clone(),
            None => repo.accounts_repository().load_account(
                &acc_id,
                acc.last_trans_hash(),
                acc.last_trans_lt(),
            )?,
        };
        if root.repr_hash()
            != acc
                .account_cell()
                .map_err(|e| anyhow::format_err!("Failed to load account cell: {e}"))?
                .repr_hash()
        {
            anyhow::bail!("External account cell hash mismatch");
        }
        acc.set_account_cell(root)
            .map_err(|e| anyhow::format_err!("Failed to set account cell: {e}"))?;
    }
    let dapp_id = acc.get_dapp_id().cloned();
    tracing::trace!("get_account_from_shard_state: dapp_id={dapp_id:?}");
    let account =
        acc.read_account().and_then(|acc| acc.as_struct()).map_err(|e| anyhow::anyhow!("{e}"))?;
    tracing::trace!("return acc");
    tracing::trace!(
        "account source state data: {:?} {:?}",
        final_state.get_block_seq_no(),
        final_state.get_block_id()
    );
    Ok((account, dapp_id, state_timestamp))
}
