use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::DAppIdentifier;
use node_types::ThreadIdentifier;
use tvm_block::MerkleUpdate;
use tvm_block::Serializable;

use crate::thread_accounts::composite::repository::ThreadAccountsState;
use crate::thread_accounts::composite::repository_inner::ThreadAccountsRepositoryInner;
use crate::thread_accounts::durable::DurableThreadAccountsStateDiff;
use crate::thread_accounts::repository::DurableThreadAccountsState;
use crate::thread_accounts::tvm::TvmThreadAccountsState;
use crate::thread_accounts::tvm::TvmThreadAccountsStateDiff;
use crate::thread_accounts::ThreadAccountsStateTransition;
use crate::BlockAccountOperation;
use crate::ThreadAccount;
use crate::ThreadAccountsStateDiff;

/// If merkle_update_size * MERKLE_UPDATE_SIZE_FACTOR < full_state_size,
/// use merkle update; otherwise use full state replacement.
/// A factor of 2.0 means the merkle update must be less than half the
/// full state size to be preferred, accounting for computational overhead.
#[allow(unused)]
const MERKLE_UPDATE_SIZE_FACTOR: f64 = 2.0;

/// Minimum account size (in bytes) to consider merkle update optimization.
/// For small accounts the overhead of creating/applying merkle updates is not worth it.
#[allow(unused)]
const MERKLE_UPDATE_MIN_ACCOUNT_SIZE: usize = 256;

#[derive(Clone)]
pub struct ThreadAccountsStateBuilder {
    repository: Arc<ThreadAccountsRepositoryInner>,
    thread_id: ThreadIdentifier,
    block_height: u64,
    original: ThreadAccountsState,
    seq_no: Option<u32>,
    changed_accounts: HashMap<AccountRouting, BlockAccountOperation>,
    known_dapp_ids: HashMap<AccountIdentifier, DAppIdentifier>,
    apply_to_durable: bool,
    /// Pre-computed redirect stubs to be inserted directly into the durable diff,
    /// bypassing the re-routing logic in `build()`.
    explicit_redirects: HashMap<AccountRouting, BlockAccountOperation>,
}

impl ThreadAccountsStateBuilder {
    pub(crate) fn new(
        repository: Arc<ThreadAccountsRepositoryInner>,
        thread_id: ThreadIdentifier,
        block_height: u64,
        original: ThreadAccountsState,
    ) -> Self {
        let apply_to_durable = repository.apply_to_durable;
        Self {
            repository,
            thread_id,
            block_height,
            original,
            seq_no: None,
            changed_accounts: HashMap::new(),
            known_dapp_ids: HashMap::new(),
            apply_to_durable,
            explicit_redirects: HashMap::new(),
        }
    }

    pub fn set_apply_to_durable(&mut self, apply_to_durable: bool) {
        self.apply_to_durable = apply_to_durable;
    }

    pub fn replace_with_redirect(&mut self, routing: &AccountRouting) -> anyhow::Result<()> {
        if let Some(account) = self.account(routing)? {
            self.changed_accounts
                .insert(*routing, BlockAccountOperation::UpdateOrInsert(account.with_redirect()?));
        }
        Ok(())
    }

    /// Insert a redirect stub at an exact routing, bypassing the re-routing logic
    /// in `build()`. Only effective when `apply_to_durable` is true; ignored otherwise.
    pub fn add_explicit_redirect(
        &mut self,
        routing: AccountRouting,
        redirect_account: ThreadAccount,
    ) {
        self.explicit_redirects
            .insert(routing, BlockAccountOperation::UpdateOrInsert(redirect_account));
    }

    pub fn set_seq_no(&mut self, seq_no: u32) {
        self.seq_no = Some(seq_no);
    }

    fn changed_account_operation(&self, routing: &AccountRouting) -> Option<Option<ThreadAccount>> {
        if let Some(operation) = self.changed_accounts.get(routing) {
            match operation {
                BlockAccountOperation::UpdateOrInsert(changed) => Some(Some(changed.clone())),
                BlockAccountOperation::Remove => Some(None),
                BlockAccountOperation::MoveFromTvm => None,
                BlockAccountOperation::AccountMerkleUpdate(_) => None,
            }
        } else {
            None
        }
    }

    pub fn account(&self, routing: &AccountRouting) -> anyhow::Result<Option<ThreadAccount>> {
        if let Some(operation) = self.changed_account_operation(routing) {
            return Ok(operation);
        }
        if let Some(known_dapp_id) = self.known_dapp_ids.get(routing.account_id()) {
            if let Some(operation) =
                self.changed_account_operation(&routing.with_dapp_id(*known_dapp_id))
            {
                return Ok(operation);
            }
        }
        self.repository.state_account(&self.original, routing)
    }

    // Try to resolve dapp id for redirected routings:
    // 1. If the routing is definitely not redirected, use the routing as-is.
    // 2. If we can get actual dapp id from a provided account, use the routing with the dapp id.
    // 3. If we already know the actual dapp id for account id, use the routing with the dapp id.
    // 4. Otherwise, use maybe redirect routing
    fn resolve_redirect(
        &mut self,
        routing: &AccountRouting,
        account: Option<&ThreadAccount>,
    ) -> AccountRouting {
        if !routing.is_maybe_redirect() {
            // Dapp id is not redirected, use the routing as-is and save known dapp id for account id
            self.known_dapp_ids.insert(*routing.account_id(), *routing.dapp_id());
            *routing
        } else if let Some(dapp_id) = account.and_then(|acc| acc.get_dapp_id()) {
            // Dapp id is maybe redirect, and an account has actual dapp id,
            // use routing with actual dapp id, and save it as known
            self.known_dapp_ids.insert(*routing.account_id(), dapp_id);
            routing.with_dapp_id(dapp_id)
        } else if let Some(known_dapp_id) = self.known_dapp_ids.get(routing.account_id()) {
            // Dapp id is maybe redirect, and we already know the actual dapp id for account id,
            // use routing with known dapp id
            routing.with_dapp_id(*known_dapp_id)
        } else {
            // Dapp id is maybe redirect, and we don't know the actual dapp id for account id,
            // use maybe redirect routing
            *routing
        }
    }

    pub fn insert_account(&mut self, routing: &AccountRouting, account: &ThreadAccount) {
        let routing = self.resolve_redirect(routing, Some(account));
        self.changed_accounts
            .insert(routing, BlockAccountOperation::UpdateOrInsert(account.clone()));
    }

    pub fn remove_account(&mut self, routing: &AccountRouting) {
        let routing = self.resolve_redirect(routing, None);
        self.changed_accounts.insert(routing, BlockAccountOperation::Remove);
    }

    pub fn move_from_tvm(&mut self, limit: usize) -> anyhow::Result<()> {
        if !self.apply_to_durable {
            return Ok(());
        }
        let mut accounts_included_in_changes: HashSet<AccountIdentifier> = HashSet::new();
        let mut moved_count = 0usize;
        for (routing, update) in &self.changed_accounts {
            accounts_included_in_changes.insert(*routing.account_id());
            if matches!(update, BlockAccountOperation::MoveFromTvm) {
                moved_count += 1;
            }
        }
        let initial_moved_count = moved_count;
        if moved_count >= limit {
            return Ok(());
        }
        self.original
            .tvm
            .shard_accounts
            .iterate_accounts(|tvm_id, tvm_acc| {
                let account_id = AccountIdentifier::from(tvm_id);
                Ok(if accounts_included_in_changes.contains(&account_id) {
                    true
                } else {
                    let routing =
                        account_id.routing_or_redirect(tvm_acc.get_dapp_id().map(From::from));
                    self.changed_accounts.insert(routing, BlockAccountOperation::MoveFromTvm);
                    moved_count += 1;
                    moved_count < limit
                })
            })
            .map_err(|err| anyhow::anyhow!("Failed to iterate TVM accounts: {}", err))?;
        let newly_moved = moved_count.saturating_sub(initial_moved_count);
        if let Some(metrics) = self.repository.durable.state_accounts_metrics() {
            metrics.report_moved_from_tvm(newly_moved, &self.thread_id);
        }
        tracing::trace!(target: "monit", "Moved from TVM: {} accounts", moved_count);
        Ok(())
    }

    pub fn build(
        self,
        tvm_usage_tree: Option<&tvm_types::UsageTree>,
    ) -> anyhow::Result<ThreadAccountsStateTransition> {
        let total_inputs = self.changed_accounts.len() + self.explicit_redirects.len();
        let old_tvm = self.original.tvm.shard_state;

        // Apply non-account updates to TVM state

        let mut new_tvm = old_tvm.clone();
        if let Some(seq_no) = self.seq_no {
            new_tvm.set_seq_no(seq_no);
        }
        let original_durable = self.original.durable.clone();
        let mut new_durable = self.original.durable;
        let mut durable_diff = HashMap::new();
        let mut new_tvm_accounts = new_tvm
            .read_accounts()
            .map_err(|err| anyhow::anyhow!("Failed to read TVM accounts: {err}"))?;
        let mut account_operations = HashMap::new();

        // Move changed accounts from TVM state to durable state if `apply_to_durable`.
        // Otherwise, update accounts in TVM state.

        if !self.changed_accounts.is_empty() {
            // Collect redirect stubs separately to deduplicate.
            // When the same account_id has both Remove (old dapp) and Insert (new dapp),
            // UpdateOrInsert takes priority over Remove at the default routing.
            let mut redirect_updates: HashMap<AccountRouting, BlockAccountOperation> =
                HashMap::new();

            if self.apply_to_durable {
                for (routing, account_update) in self.changed_accounts {
                    let default_routing = routing.account_id().redirect();
                    let routing_has_dapp =
                        *routing.dapp_id() != routing.account_id().redirect_dapp_id();

                    // Determine the effective routing and whether a redirect stub is needed.
                    // Case 1: routing already has dapp_id != account_id (e.g., from move_from_tvm)
                    // Case 2: an account inserted at default routing but has internal dapp_id != account_id
                    let (effective_routing, needs_redirect) = if routing_has_dapp {
                        (routing, true)
                    } else if let BlockAccountOperation::UpdateOrInsert(state_account) =
                        &account_update
                    {
                        if let Some(actual_dapp) = state_account.get_dapp_id() {
                            if actual_dapp != routing.account_id().redirect_dapp_id() {
                                (routing.account_id().routing(actual_dapp), true)
                            } else {
                                (routing, false)
                            }
                        } else {
                            (routing, false)
                        }
                    } else {
                        (routing, false)
                    };

                    // Generate redirect stub at (account_id, account_id) before moving an account
                    if needs_redirect {
                        let redirect_entry = match &account_update {
                            BlockAccountOperation::UpdateOrInsert(state_account) => {
                                let redirect = state_account.with_redirect()?;
                                Some(BlockAccountOperation::UpdateOrInsert(redirect))
                            }
                            BlockAccountOperation::MoveFromTvm => {
                                if let Ok(Some(tvm_acc)) = self
                                    .original
                                    .tvm
                                    .shard_accounts
                                    .account(&routing.account_id().into())
                                {
                                    let state_acc = ThreadAccount::from(tvm_acc);
                                    let redirect = state_acc.with_redirect()?;
                                    Some(BlockAccountOperation::UpdateOrInsert(redirect))
                                } else {
                                    None
                                }
                            }
                            BlockAccountOperation::Remove => Some(BlockAccountOperation::Remove),
                            BlockAccountOperation::AccountMerkleUpdate(_) => {
                                // Redirect should already exist in durable, no action needed
                                None
                            }
                        };
                        if let Some(redirect_update) = redirect_entry {
                            // UpdateOrInsert takes priority over Remove when both occur
                            // (e.g., account changes dapp: Remove at old + Insert at new)
                            let should_insert =
                                matches!(redirect_update, BlockAccountOperation::UpdateOrInsert(_))
                                    || !redirect_updates.contains_key(&default_routing);
                            if should_insert {
                                redirect_updates.insert(default_routing, redirect_update);
                            }
                        }
                    }

                    durable_diff.insert(effective_routing, account_update);

                    new_tvm_accounts.remove(&routing.account_id().into()).map_err(|err| {
                        anyhow::anyhow!("Failed to remove account from TVM state: {}", err)
                    })?;
                }
            } else {
                for (address, account) in self.changed_accounts {
                    // When not applying to durable, MoveFromTvm must be a no-op:
                    // accounts stay in TVM as-is since there's no durable target.
                    if matches!(account, BlockAccountOperation::MoveFromTvm) {
                        continue;
                    }
                    crate::thread_accounts::tvm::patch_account(
                        *address.account_id(),
                        account,
                        &mut new_tvm_accounts,
                    )?;
                }
            }

            // Append deduplicated redirect stubs after all real account entries
            durable_diff.extend(redirect_updates);
            new_tvm
                .write_accounts(&new_tvm_accounts)
                .map_err(|err| anyhow::anyhow!("Failed to patch TVM accounts: {}", err))?;
            (new_durable, account_operations) = self.repository.durable.state_update(
                &self.thread_id,
                self.block_height,
                &self.original.tvm.shard_accounts,
                &new_durable,
                &durable_diff,
            )?;
        }

        // Process explicit redirect stubs separately — these must be applied
        // even when changed_accounts is empty (e.g., postprocessing creates
        // redirects for newly created accounts with no outbound accounts).
        if self.apply_to_durable && !self.explicit_redirects.is_empty() {
            let explicit_diff: HashMap<AccountRouting, BlockAccountOperation> =
                self.explicit_redirects.into_iter().collect();
            let (updated_durable, redirect_operations) = self.repository.durable.state_update(
                &self.thread_id,
                self.block_height,
                &self.original.tvm.shard_accounts,
                &new_durable,
                &explicit_diff,
            )?;
            new_durable = updated_durable;
            account_operations.extend(redirect_operations);
            // Include explicit redirects in the serialized diff so receivers can
            // reconstruct the same durable map; otherwise the redirect stub lives
            // only in the producer's local state and descendants on other nodes
            // can't resolve the account via its default routing.
            durable_diff.extend(explicit_diff);
        }

        // Optimize durable diff: convert large UpdateOrInsert entries to merkle updates
        if self.apply_to_durable && !durable_diff.is_empty() {
            durable_diff = _optimize_durable_diff(
                &self.repository.durable,
                &original_durable,
                &self.original.tvm.shard_accounts,
                durable_diff,
            );
        }

        // Create TVM state merkle update

        let old_tvm_cell = old_tvm
            .serialize()
            .map_err(|err| anyhow::anyhow!("Failed to serialize TVM shard state: {}", err))?;
        let new_tvm_cell = new_tvm
            .serialize()
            .map_err(|err| anyhow::anyhow!("Failed to serialize TVM shard state: {}", err))?;
        let tvm_merkle_update = if let Some(usage_tree) = tvm_usage_tree {
            let usages = usage_tree.take_visited_set();
            MerkleUpdate::create_fast(&old_tvm_cell, &new_tvm_cell, |h| usages.contains(h))
                .map_err(|e| anyhow::format_err!("Failed to create merkle update: {e}"))?
        } else {
            tracing::trace!("creating TVM state merkle update start");
            let res = MerkleUpdate::create(&old_tvm_cell, &new_tvm_cell)
                .map_err(|e| anyhow::format_err!("Failed to create merkle update: {e}"))?;
            tracing::trace!("creating TVM state merkle update finish");
            res
        };

        let tvm_cell_size = new_tvm_cell.data().len();
        let merkle_update_size = tvm_merkle_update.serialize().map(|c| c.data().len()).unwrap_or(0);

        tracing::info!(
            target: "mem",
            "builder.build: inputs={total_inputs} durable_diff={} account_ops={} \
             tvm_cell_bytes={tvm_cell_size} merkle_update_bytes={merkle_update_size}",
            durable_diff.len(),
            account_operations.len(),
        );

        Ok(ThreadAccountsStateTransition {
            new_state: ThreadAccountsState::from_parts(
                new_durable,
                TvmThreadAccountsState::with_shard_state(new_tvm)?,
            ),
            diff: ThreadAccountsStateDiff {
                durable: DurableThreadAccountsStateDiff { accounts: durable_diff },
                tvm: TvmThreadAccountsStateDiff { update: tvm_merkle_update },
            },
            account_operations,
            apply_timings: Default::default(),
        })
    }
}

fn _optimize_durable_diff(
    durable_repo: &crate::thread_accounts::durable::DurableThreadAccountsRepository,
    original_durable: &DurableThreadAccountsState,
    old_tvm_accounts: &tvm_block::ShardAccounts,
    diff: HashMap<AccountRouting, BlockAccountOperation>,
) -> HashMap<AccountRouting, BlockAccountOperation> {
    diff.into_iter()
        .map(|(routing, update)| {
            let optimized = match &update {
                BlockAccountOperation::UpdateOrInsert(new_account) => _try_create_merkle_update(
                    durable_repo,
                    original_durable,
                    old_tvm_accounts,
                    &routing,
                    new_account,
                )
                .unwrap_or(update),
                _ => update,
            };
            (routing, optimized)
        })
        .collect()
}

fn _try_create_merkle_update(
    durable_repo: &crate::thread_accounts::durable::DurableThreadAccountsRepository,
    original_durable: &DurableThreadAccountsState,
    old_tvm_accounts: &tvm_block::ShardAccounts,
    routing: &AccountRouting,
    new_account: &ThreadAccount,
) -> Option<BlockAccountOperation> {
    if new_account.is_redirect() {
        return None;
    }
    if new_account.get_dapp_id().is_some_and(|dapp_id| dapp_id != *routing.dapp_id()) {
        return None;
    }

    let new_shard_acc = new_account.as_tvm()?;

    // Look up the old account from a durable state first, then TVM
    let old_account =
        durable_repo.state_account(original_durable, routing).ok()?.or_else(|| {
            old_tvm_accounts
                .account(&routing.account_id().into())
                .ok()
                .flatten()
                .map(ThreadAccount::from)
        })?;

    // Only optimize TVM old accounts
    let old_shard_acc = old_account.as_tvm()?;

    let new_shard_acc_cell = new_shard_acc.serialize().ok()?;

    let new_shard_acc_serialized_size = new_shard_acc.write_to_bytes().ok()?.len();
    if new_shard_acc_serialized_size < MERKLE_UPDATE_MIN_ACCOUNT_SIZE {
        return None;
    }
    let old_shard_acc_cell = old_shard_acc.serialize().ok()?;

    // Create merkle update
    let merkle_update = MerkleUpdate::create(&old_shard_acc_cell, &new_shard_acc_cell).ok()?;
    let update_cell = merkle_update.serialize().ok()?;
    let update_bytes = tvm_types::write_boc(&update_cell).ok()?;

    // Compare sizes
    let merkle_size = update_bytes.len() as f64;

    if merkle_size * MERKLE_UPDATE_SIZE_FACTOR < new_shard_acc_serialized_size as f64 {
        tracing::trace!(
            target: "monit",
            "Account {} merkle update: {} bytes vs full: {} bytes (saving {:.0}%)",
            routing.account_id().to_hex_string(),
            update_bytes.len(),
            new_shard_acc_serialized_size,
            (1.0 - merkle_size / new_shard_acc_serialized_size as f64) * 100.0,
        );

        tracing::trace!(
            target: "monit", "Account merkle update created, account: {}, size: {}",
            routing,
            update_bytes.len(),
        );
        Some(BlockAccountOperation::AccountMerkleUpdate(update_bytes))
    } else {
        None
    }
}
