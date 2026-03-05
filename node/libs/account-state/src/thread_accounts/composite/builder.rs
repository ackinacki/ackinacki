use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use node_types::AccountIdentifier;
use node_types::AccountRouting;
use tvm_block::MerkleUpdate;
use tvm_block::Serializable;

use crate::thread_accounts::composite::repository::CompositeThreadAccountsDiff;
use crate::thread_accounts::composite::repository::CompositeThreadAccountsRef;
use crate::thread_accounts::composite::repository_inner::CompositeThreadAccountsRepositoryInner;
use crate::thread_accounts::durable::DurableThreadAccountsDiff;
use crate::thread_accounts::tvm::TvmThreadState;
use crate::thread_accounts::tvm::TvmThreadStateDiff;
use crate::thread_accounts::AccountsRepository;
use crate::thread_accounts::DAppAccountMapRepository;
use crate::thread_accounts::ThreadAccountsBuilder;
use crate::thread_accounts::ThreadAccountsTransition;
use crate::thread_accounts::ThreadDAppMapRepository;
use crate::ThreadAccountUpdate;
use crate::ThreadStateAccount;

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
pub struct CompositeThreadAccountsBuilder<
    ThreadDApps: ThreadDAppMapRepository,
    DAppAccounts: DAppAccountMapRepository,
    Accounts: AccountsRepository,
> {
    repository: Arc<CompositeThreadAccountsRepositoryInner<ThreadDApps, DAppAccounts, Accounts>>,
    original: CompositeThreadAccountsRef<ThreadDApps::MapRef>,
    seq_no: Option<u32>,
    cached_accounts: HashMap<AccountRouting, ThreadStateAccount>,
    changed_accounts: HashMap<AccountRouting, ThreadAccountUpdate>,
    changed_tvm_accounts: HashMap<AccountIdentifier, ThreadAccountUpdate>,
    apply_to_durable: bool,
}

impl<ThreadDApps, DAppAccounts, Accounts>
    CompositeThreadAccountsBuilder<ThreadDApps, DAppAccounts, Accounts>
where
    ThreadDApps: ThreadDAppMapRepository,
    DAppAccounts: DAppAccountMapRepository,
    Accounts: AccountsRepository,
{
    pub(crate) fn new(
        repository: Arc<
            CompositeThreadAccountsRepositoryInner<ThreadDApps, DAppAccounts, Accounts>,
        >,
        original: CompositeThreadAccountsRef<ThreadDApps::MapRef>,
    ) -> Self {
        let apply_to_durable = repository.apply_to_durable;
        Self {
            repository,
            original,
            seq_no: None,
            cached_accounts: HashMap::new(),
            changed_accounts: HashMap::new(),
            changed_tvm_accounts: HashMap::new(),
            apply_to_durable,
        }
    }

    pub fn set_apply_to_durable(&mut self, apply_to_durable: bool) {
        self.apply_to_durable = apply_to_durable;
    }

    pub fn replace_with_redirect(&mut self, routing: &AccountRouting) -> anyhow::Result<()> {
        if let Some(account) = self.account(routing)? {
            self.cached_accounts.remove(routing);
            if self.apply_to_durable {
                self.changed_accounts.insert(
                    *routing,
                    ThreadAccountUpdate::UpdateOrInsert(account.with_redirect()?),
                );
            } else {
                self.changed_tvm_accounts.insert(
                    *routing.account_id(),
                    ThreadAccountUpdate::UpdateOrInsert(account.with_redirect()?),
                );
            }
        }
        Ok(())
    }
}

impl<ThreadDApps, DAppAccounts, Accounts> ThreadAccountsBuilder
    for CompositeThreadAccountsBuilder<ThreadDApps, DAppAccounts, Accounts>
where
    ThreadDApps: ThreadDAppMapRepository,
    DAppAccounts: DAppAccountMapRepository,
    Accounts: AccountsRepository,
{
    type StateDiff = CompositeThreadAccountsDiff;
    type StateRef = CompositeThreadAccountsRef<ThreadDApps::MapRef>;

    fn set_seq_no(&mut self, seq_no: u32) {
        self.seq_no = Some(seq_no);
    }

    fn account(&self, routing: &AccountRouting) -> anyhow::Result<Option<ThreadStateAccount>> {
        if let Some(ThreadAccountUpdate::UpdateOrInsert(changed)) =
            self.changed_accounts.get(routing)
        {
            Ok(Some(changed.clone()))
        } else if let Some(cached) = self.cached_accounts.get(routing) {
            Ok(Some(cached.clone()))
        } else {
            if !self.apply_to_durable {
                if let Some(ThreadAccountUpdate::UpdateOrInsert(changed)) =
                    self.changed_tvm_accounts.get(routing.account_id())
                {
                    return Ok(Some(changed.clone()));
                }
            }
            self.repository.state_account(&self.original, routing)
        }
    }

    fn insert_account(&mut self, routing: &AccountRouting, account: &ThreadStateAccount) {
        self.cached_accounts.remove(routing);
        if self.apply_to_durable {
            self.changed_accounts
                .insert(*routing, ThreadAccountUpdate::UpdateOrInsert(account.clone()));
        } else {
            self.changed_tvm_accounts.insert(
                *routing.account_id(),
                ThreadAccountUpdate::UpdateOrInsert(account.clone()),
            );
        }
    }

    fn remove_account(&mut self, routing: &AccountRouting) {
        self.cached_accounts.remove(routing);
        if self.apply_to_durable {
            self.changed_accounts.insert(*routing, ThreadAccountUpdate::Remove);
        } else {
            self.changed_tvm_accounts.insert(*routing.account_id(), ThreadAccountUpdate::Remove);
        }
    }

    fn move_from_tvm(&mut self, limit: usize) -> anyhow::Result<()> {
        if !self.apply_to_durable {
            return Ok(());
        }
        let mut accounts_included_in_changes: HashSet<AccountIdentifier> = HashSet::new();
        let mut moved_count = 0;
        for (routing, update) in &self.changed_accounts {
            accounts_included_in_changes.insert(*routing.account_id());
            if matches!(update, ThreadAccountUpdate::MoveFromTvm) {
                moved_count += 1;
            }
        }
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
                        account_id.optional_dapp_originator(tvm_acc.get_dapp_id().map(From::from));
                    self.changed_accounts.insert(routing, ThreadAccountUpdate::MoveFromTvm);
                    moved_count += 1;
                    moved_count < limit
                })
            })
            .map_err(|err| anyhow::anyhow!("Failed to iterate TVM accounts: {}", err))?;
        tracing::trace!(target: "monit", "Moved from TVM: {} accounts", moved_count);
        Ok(())
    }

    fn build(
        self,
        tvm_usage_tree: Option<&tvm_types::UsageTree>,
    ) -> anyhow::Result<ThreadAccountsTransition<Self::StateRef, Self::StateDiff>> {
        let old_tvm = self.original.tvm.shard_state;

        // Apply non-account updates to TVM state

        let mut new_tvm = old_tvm.clone();
        if let Some(seq_no) = self.seq_no {
            new_tvm.set_seq_no(seq_no);
        }
        let _original_durable = self.original.durable.clone();
        let mut new_durable = self.original.durable;
        let mut durable_diff = Vec::new();

        let mut new_tvm_accounts = new_tvm
            .read_accounts()
            .map_err(|err| anyhow::anyhow!("Failed to read TVM accounts: {err}"))?;

        // Move changed accounts from TVM state to durable state if `apply_to_durable`.
        // Otherwise, update accounts in TVM state.

        if !self.changed_accounts.is_empty() || !self.changed_tvm_accounts.is_empty() {
            // Collect redirect stubs separately to deduplicate.
            // When the same account_id has both Remove (old dapp) and Insert (new dapp),
            // UpdateOrInsert takes priority over Remove at the default routing.
            let mut redirect_updates: HashMap<AccountRouting, ThreadAccountUpdate> = HashMap::new();

            if self.apply_to_durable {
                for (routing, account) in self.changed_accounts {
                    let default_routing = routing.account_id().dapp_originator();
                    let routing_has_dapp =
                        *routing.dapp_id() != routing.account_id().use_as_dapp_id();

                    // Determine the effective routing and whether a redirect stub is needed.
                    // Case 1: routing already has dapp_id != account_id (e.g. from move_from_tvm)
                    // Case 2: account inserted at default routing but has internal dapp_id != account_id
                    let (effective_routing, needs_redirect) = if routing_has_dapp {
                        (routing, true)
                    } else if let ThreadAccountUpdate::UpdateOrInsert(ref state_account) = account {
                        if let Some(actual_dapp) = state_account.get_dapp_id() {
                            if actual_dapp != routing.account_id().use_as_dapp_id() {
                                (routing.account_id().routing_with(actual_dapp), true)
                            } else {
                                (routing, false)
                            }
                        } else {
                            (routing, false)
                        }
                    } else {
                        (routing, false)
                    };

                    // Generate redirect stub at (account_id, account_id) before moving account
                    if needs_redirect {
                        let redirect_entry = match &account {
                            ThreadAccountUpdate::UpdateOrInsert(state_account) => {
                                let redirect = state_account.with_redirect()?;
                                Some(ThreadAccountUpdate::UpdateOrInsert(redirect))
                            }
                            ThreadAccountUpdate::MoveFromTvm => {
                                if let Ok(Some(tvm_acc)) = self
                                    .original
                                    .tvm
                                    .shard_accounts
                                    .account(&routing.account_id().into())
                                {
                                    let state_acc = ThreadStateAccount::from(tvm_acc);
                                    let redirect = state_acc.with_redirect()?;
                                    Some(ThreadAccountUpdate::UpdateOrInsert(redirect))
                                } else {
                                    None
                                }
                            }
                            ThreadAccountUpdate::Remove => Some(ThreadAccountUpdate::Remove),
                            ThreadAccountUpdate::AccountMerkleUpdate(_) => {
                                // Redirect should already exist in durable, no action needed
                                None
                            }
                        };
                        if let Some(redirect_update) = redirect_entry {
                            // UpdateOrInsert takes priority over Remove when both occur
                            // (e.g. account changes dapp: Remove at old + Insert at new)
                            let should_insert =
                                matches!(redirect_update, ThreadAccountUpdate::UpdateOrInsert(_))
                                    || !redirect_updates.contains_key(&default_routing);
                            if should_insert {
                                redirect_updates.insert(default_routing, redirect_update);
                            }
                        }
                    }

                    durable_diff.push((effective_routing, account));

                    new_tvm_accounts.remove(&routing.account_id().into()).map_err(|err| {
                        anyhow::anyhow!("Failed to remove account from TVM state: {}", err)
                    })?;
                }
            } else {
                for (address, account) in self.changed_tvm_accounts {
                    // When not applying to durable, MoveFromTvm must be a no-op:
                    // accounts stay in TVM as-is since there's no durable target.
                    if matches!(account, crate::ThreadAccountUpdate::MoveFromTvm) {
                        continue;
                    }
                    crate::thread_accounts::tvm::patch_account(
                        address,
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
            new_durable = self.repository.durable.state_update(
                &self.original.tvm.shard_accounts,
                &new_durable,
                &durable_diff,
            )?;
        }

        // Optimize durable diff: convert large UpdateOrInsert entries to merkle updates
        // if self.apply_to_durable && !durable_diff.is_empty() {
        //     durable_diff = optimize_durable_diff(
        //         &self.repository.durable,
        //         &original_durable,
        //         &self.original.tvm.shard_accounts,
        //         durable_diff,
        //     );
        // }

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
            MerkleUpdate::create(&old_tvm_cell, &new_tvm_cell)
                .map_err(|e| anyhow::format_err!("Failed to create merkle update: {e}"))?
        };

        Ok(ThreadAccountsTransition {
            new_state: Self::StateRef {
                durable: new_durable,
                tvm: TvmThreadState::with_shard_state(new_tvm)?,
            },
            diff: Self::StateDiff {
                durable: DurableThreadAccountsDiff { accounts: durable_diff },
                tvm: TvmThreadStateDiff { update: tvm_merkle_update },
            },
        })
    }
}

fn _optimize_durable_diff<ThreadDApps, DAppAccounts, Accounts>(
    durable_repo: &crate::thread_accounts::durable::DurableThreadAccountsRepository<
        ThreadDApps,
        DAppAccounts,
        Accounts,
    >,
    original_durable: &ThreadDApps::MapRef,
    old_tvm_accounts: &tvm_block::ShardAccounts,
    diff: Vec<(AccountRouting, ThreadAccountUpdate)>,
) -> Vec<(AccountRouting, ThreadAccountUpdate)>
where
    ThreadDApps: ThreadDAppMapRepository,
    DAppAccounts: DAppAccountMapRepository,
    Accounts: AccountsRepository,
{
    diff.into_iter()
        .map(|(routing, update)| {
            let optimized = match &update {
                ThreadAccountUpdate::UpdateOrInsert(new_account) => _try_create_merkle_update(
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

fn _try_create_merkle_update<ThreadDApps, DAppAccounts, Accounts>(
    durable_repo: &crate::thread_accounts::durable::DurableThreadAccountsRepository<
        ThreadDApps,
        DAppAccounts,
        Accounts,
    >,
    original_durable: &ThreadDApps::MapRef,
    old_tvm_accounts: &tvm_block::ShardAccounts,
    routing: &AccountRouting,
    new_account: &ThreadStateAccount,
) -> Option<ThreadAccountUpdate>
where
    ThreadDApps: ThreadDAppMapRepository,
    DAppAccounts: DAppAccountMapRepository,
    Accounts: AccountsRepository,
{
    let ThreadStateAccount::Tvm(new_shard_acc) = new_account else {
        return None;
    };

    // Look up old account from durable state first, then TVM
    let old_account =
        durable_repo.state_account(original_durable, routing).ok()?.or_else(|| {
            old_tvm_accounts
                .account(&routing.account_id().into())
                .ok()
                .flatten()
                .map(ThreadStateAccount::from)
        })?;

    // Only optimize TVM old accounts
    let ThreadStateAccount::Tvm(old_shard_acc) = old_account else {
        return None;
    };

    let new_shard_acc_cell = new_shard_acc.serialize().ok()?;

    let new_shard_acc_serialized_size = new_shard_acc.write_to_bytes().ok()?.len();
    if new_shard_acc_serialized_size < MERKLE_UPDATE_MIN_ACCOUNT_SIZE {
        return None;
    }
    let old_shard_acc_cell = old_shard_acc.serialize().ok()?;

    // Create merkle update
    let merkle_update =
        tvm_block::MerkleUpdate::create(&old_shard_acc_cell, &new_shard_acc_cell).ok()?;
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
        Some(ThreadAccountUpdate::AccountMerkleUpdate(update_bytes))
    } else {
        None
    }
}
