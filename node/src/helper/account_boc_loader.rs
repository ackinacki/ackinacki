// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::str::FromStr;
use std::sync::Arc;

use account_state::ThreadAccount;
use account_state::ThreadAccountsRepository;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::DAppIdentifier;
use node_types::ThreadIdentifier;
use parking_lot::Mutex;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;

fn parse_account_routing(account_address: &str) -> anyhow::Result<AccountRouting> {
    if let Ok(routing) = AccountRouting::from_str(account_address) {
        return Ok(routing);
    }
    Ok(AccountIdentifier::from_str(account_address)?.dapp_originator())
}

pub fn get_account_from_shard_state(
    repository: Arc<Mutex<RepositoryImpl>>,
    account_address: &str,
) -> anyhow::Result<(ThreadAccount, Option<DAppIdentifier>, u64)> {
    tracing::trace!("get_account_from_shard_state: address={account_address}");

    let repo = repository.lock();

    let initial_routing = parse_account_routing(account_address)?;
    let acc_id = *initial_routing.account_id();

    let default_state = repo
        .last_finalized_optimistic_state(&ThreadIdentifier::default())
        .ok_or_else(|| anyhow::anyhow!("Shard state for default thread not found"))?;

    tracing::trace!("get_account_from_shard_state: default_routing={initial_routing:?}");
    let potential_thread = default_state.threads_table.find_match(&initial_routing);
    tracing::trace!("get_account_from_shard_state: potential_thread={potential_thread:?}");
    let no_dapp_state = repo
        .last_finalized_optimistic_state(&potential_thread)
        .ok_or_else(|| anyhow::anyhow!("Failed to load no dapp state"))?;
    tracing::trace!("Loaded no_dapp_state");
    let mut final_state = no_dapp_state;
    let no_dapp_state = final_state.get_shard_state();

    let mut acc = repo
        .thread_accounts_repository()
        .state_account(&no_dapp_state, &initial_routing)?
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
        let actual_routing = acc_id.routing_with(actual_dapp_id);
        tracing::trace!("get_account_from_shard_state: actual_routing={actual_routing:?}");
        let actual_thread = default_state.threads_table.find_match(&actual_routing);
        tracing::trace!("get_account_from_shard_state: actual_thread={actual_thread:?}");
        let actual_state = repo
            .last_finalized_optimistic_state(&actual_thread)
            .ok_or_else(|| anyhow::anyhow!("Failed to load no dapp state"))?;
        final_state = actual_state;
        let actual_state = final_state.get_shard_state();

        acc = repo
            .thread_accounts_repository()
            .state_account(&actual_state, &actual_routing)?
            .ok_or_else(|| anyhow::anyhow!("Can't find account in shard state"))?;
    }
    tracing::trace!("Loaded acc");

    if acc.is_unloaded() {
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
        if root.hash() != acc.account()?.hash() {
            anyhow::bail!("External account cell hash mismatch");
        }
        acc.set_account(root)?;
    }
    let dapp_id = acc.get_dapp_id();
    tracing::trace!("get_account_from_shard_state: dapp_id={dapp_id:?}");
    let account = acc.account()?;
    tracing::trace!("return acc");
    tracing::trace!(
        "account source state data: {:?} {:?}",
        final_state.get_block_seq_no(),
        final_state.get_block_id()
    );
    Ok((account, dapp_id, state_timestamp))
}

#[cfg(test)]
mod tests {
    use super::parse_account_routing;

    #[test]
    fn parse_account_identifier_success() {
        let address = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let parsed = parse_account_routing(address);
        assert!(parsed.is_ok(), "expected valid 32-byte address to parse");
    }

    #[test]
    fn parse_account_identifier_rejects_invalid_address_format() {
        let err = parse_account_routing("not-an-address").unwrap_err();
        assert!(err.to_string().contains("Invalid AccountIdentifier"));
    }

    #[test]
    fn parse_account_identifier_rejects_non_32_byte_account_id() {
        let err = parse_account_routing("1").unwrap_err();
        assert!(err.to_string().contains("64 chars expected"));
    }
}
