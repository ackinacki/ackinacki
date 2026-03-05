use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;

use node_types::AccountHash;
use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifier;
use node_types::DAppIdentifierPath;
use node_types::ThreadAccountsHash;
use serde::Deserialize;
use serde::Serialize;
use tvm_block::Serializable;
use tvm_block::ShardAccount;
use tvm_types::UInt256;

use crate::thread_accounts::durable::accounts::AccountsRepository;
use crate::thread_accounts::durable::dapp_accounts::AccountInfo;
use crate::thread_accounts::durable::dapp_accounts::DAppAccountMapRepository;
use crate::thread_accounts::durable::snapshot::CompositeDurableStateSnapshot;
use crate::thread_accounts::durable::thread_dapps::ThreadDAppMapRepository;
use crate::thread_accounts::DurableRepoStat;
use crate::ThreadAccount;
use crate::ThreadStateAccount;

#[derive(Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum ThreadAccountUpdate {
    UpdateOrInsert(ThreadStateAccount),
    Remove,
    #[default]
    MoveFromTvm,
    /// Merkle update delta for an existing account.
    /// Contains the BOC-serialized tvm_block::MerkleUpdate bytes for ShardAccount cell.
    /// Applicable only to TVM accounts that already exist in the durable state.
    AccountMerkleUpdate(Vec<u8>),
}

#[derive(Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableThreadAccountsDiff {
    pub accounts: Vec<(AccountRouting, ThreadAccountUpdate)>,
}

impl DurableThreadAccountsDiff {
    pub fn is_empty(&self) -> bool {
        self.accounts.is_empty()
    }
}

#[derive(Clone)]
pub struct DurableThreadAccountsRepository<
    ThreadDApps: ThreadDAppMapRepository,
    DAppAccounts: DAppAccountMapRepository,
    Accounts: AccountsRepository,
> {
    thread_dapps: ThreadDApps,
    dapp_accounts: DAppAccounts,
    accounts: Accounts,
}

impl<ThreadDApps, DAppAccounts, Accounts>
    DurableThreadAccountsRepository<ThreadDApps, DAppAccounts, Accounts>
where
    ThreadDApps: ThreadDAppMapRepository,
    DAppAccounts: DAppAccountMapRepository,
    Accounts: AccountsRepository,
{
    pub fn new(thread_dapps: ThreadDApps, dapp_accounts: DAppAccounts, accounts: Accounts) -> Self {
        Self { thread_dapps, dapp_accounts, accounts }
    }

    pub fn get_stat(&self) -> DurableRepoStat {
        DurableRepoStat {
            dapp_map: self.thread_dapps.get_stat(),
            account_map: self.dapp_accounts.get_stat(),
        }
    }

    pub fn aerospike_cache_stat(
        &self,
    ) -> Option<crate::thread_accounts::durable::accounts::AerospikeAccountsCacheStat> {
        self.accounts.aerospike_cache_stat()
    }

    pub fn commit(&self) -> anyhow::Result<()> {
        // the order is important for consistency
        self.accounts.commit()?;
        self.dapp_accounts.commit()?;
        self.thread_dapps.commit()?;
        Ok(())
    }

    pub fn get_state(
        &self,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<Option<ThreadDApps::MapRef>> {
        self.thread_dapps.index_get(block_id)
    }

    pub fn new_state() -> ThreadDApps::MapRef {
        ThreadDApps::new_map()
    }

    pub fn set_state(
        &self,
        block_id: &BlockIdentifier,
        state: &ThreadDApps::MapRef,
    ) -> anyhow::Result<()> {
        self.thread_dapps.index_set(block_id, state)
    }

    pub fn state_hash(&self, state: &ThreadDApps::MapRef) -> ThreadAccountsHash {
        self.thread_dapps.map_hash(state)
    }

    pub fn state_account(
        &self,
        state: &ThreadDApps::MapRef,
        routing: &AccountRouting,
    ) -> anyhow::Result<Option<ThreadStateAccount>> {
        let Some(dapp_hash) = self.thread_dapps.map_get(state, routing.dapp_id())? else {
            return Ok(None);
        };
        let Some(dapp) = self.dapp_accounts.index_get(&dapp_hash)? else {
            return Ok(None);
        };
        let Some(account_info) = self.dapp_accounts.map_get(&dapp, routing.account_id())? else {
            return Ok(None);
        };
        // Redirect stubs have no real account data — reconstruct from metadata
        if let Some(redirect_dapp_id) = account_info.redirect_dapp_id {
            return Ok(Some(ThreadStateAccount::Tvm(
                tvm_block::ShardAccount::with_redirect(
                    account_info.last_trans_hash.into(),
                    account_info.last_trans_lt,
                    Some(redirect_dapp_id.into()),
                )
                .map_err(|err| anyhow::anyhow!("{err}"))?,
            )));
        }
        let Some(account) = self.accounts.get(&account_info.account_hash)? else {
            return Ok(None);
        };
        Ok(Some(ThreadStateAccount::new(
            account,
            account_info.last_trans_hash,
            account_info.last_trans_lt,
            Some(*routing.dapp_id()),
        )?))
    }

    pub(crate) fn state_split(
        &self,
        state: &ThreadDApps::MapRef,
        dapp_id_path: DAppIdentifierPath,
    ) -> anyhow::Result<(ThreadDApps::MapRef, ThreadDApps::MapRef)> {
        self.thread_dapps.map_split(state, dapp_id_path)
    }

    pub(crate) fn merge(
        &self,
        a: &ThreadDApps::MapRef,
        b: &ThreadDApps::MapRef,
    ) -> anyhow::Result<ThreadDApps::MapRef> {
        self.thread_dapps.merge(a, b)
    }

    pub(crate) fn state_update(
        &self,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &ThreadDApps::MapRef,
        accounts: &[(AccountRouting, ThreadAccountUpdate)],
    ) -> anyhow::Result<ThreadDApps::MapRef> {
        tracing::trace!(target: "monit", "Update accounts in durable state: {}", accounts.len());
        let (accounts, maps) = self.prepare_updates(old_tvm_accounts, state, accounts)?;
        self.accounts.update(&accounts)?;
        self.update_maps(state, maps)
    }

    fn prepare_updates(
        &self,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &ThreadDApps::MapRef,
        accounts: &[(AccountRouting, ThreadAccountUpdate)],
    ) -> anyhow::Result<(
        Vec<(AccountHash, Option<ThreadAccount>)>,
        HashMap<
            DAppIdentifier,
            (DAppAccounts::MapRef, Vec<(AccountIdentifier, Option<AccountInfo>)>),
        >,
    )> {
        let mut account_updates = Vec::new();
        let mut map_updates = HashMap::new();
        for (routing, state_account) in accounts {
            tracing::trace!(target: "builder", "Update account in durable state: {}", routing);
            let map_update = match map_updates.entry(*routing.dapp_id()) {
                Entry::Occupied(existing) => existing.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert((self.ensure_dapp_map(state, routing.dapp_id())?, Vec::new()))
                }
            };
            let (hash, account, info) =
                self.get_account_update(routing, state_account, &map_update.0, old_tvm_accounts)?;
            // hash=None + info=None means a true no-op (e.g. MoveFromTvm with no TVM account)
            let is_noop = hash.is_none() && info.is_none();
            if !is_noop {
                if let Some(hash) = hash {
                    account_updates.push((hash, account));
                }
                map_update.1.push((*routing.account_id(), info));
            }
        }

        // Deduplicate account_updates by hash.
        // The accounts table is content-addressed: if any dApp entry still
        // references a hash, the data must be kept.  When the same hash
        // appears as both insert [Some(data)] and delete [None]
        // (e.g. account changes dApp — Remove from old + Insert to new,
        // same underlying Account cell), insert must win.
        let mut deduped: HashMap<AccountHash, Option<ThreadAccount>> = HashMap::new();
        for (hash, account) in account_updates {
            match deduped.entry(hash) {
                Entry::Vacant(e) => {
                    e.insert(account);
                }
                Entry::Occupied(mut e) => {
                    // Some(data) takes priority over None
                    if account.is_some() {
                        e.insert(account);
                    }
                }
            }
        }
        let account_updates: Vec<_> = deduped.into_iter().collect();

        Ok((account_updates, map_updates))
    }

    fn ensure_dapp_map(
        &self,
        state: &ThreadDApps::MapRef,
        dapp_id: &DAppIdentifier,
    ) -> anyhow::Result<DAppAccounts::MapRef> {
        let accounts_map = match self.thread_dapps.map_get(state, dapp_id)? {
            Some(accounts_map_hash) => {
                self.dapp_accounts.index_get(&accounts_map_hash)?.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Missing required dapp state [{accounts_map_hash}] for dapp id [{}]",
                        dapp_id
                    )
                })?
            }
            None => DAppAccounts::new_map(),
        };
        Ok(accounts_map)
    }

    fn get_account_update(
        &self,
        routing: &AccountRouting,
        state_account: &ThreadAccountUpdate,
        accounts_map: &DAppAccounts::MapRef,
        old_tvm_accounts: &tvm_block::ShardAccounts,
    ) -> anyhow::Result<(Option<AccountHash>, Option<ThreadAccount>, Option<AccountInfo>)> {
        match state_account {
            ThreadAccountUpdate::UpdateOrInsert(state_account) => {
                Self::state_account_update(routing, state_account)
            }
            ThreadAccountUpdate::Remove => {
                let account_hash = self
                    .dapp_accounts
                    .map_get(accounts_map, routing.account_id())?
                    .map(|x| x.account_hash);
                Ok((account_hash, None, None))
            }
            ThreadAccountUpdate::MoveFromTvm => {
                if let Ok(Some(tvm_acc)) = old_tvm_accounts.account(&routing.account_id().into()) {
                    Self::state_account_update(routing, &tvm_acc.into())
                } else {
                    Ok((None, None, None))
                }
            }
            ThreadAccountUpdate::AccountMerkleUpdate(update_bytes) => self
                .apply_account_merkle_update(routing, update_bytes, accounts_map, old_tvm_accounts),
        }
    }

    fn apply_account_merkle_update(
        &self,
        routing: &AccountRouting,
        update_bytes: &[u8],
        accounts_map: &DAppAccounts::MapRef,
        old_tvm_accounts: &tvm_block::ShardAccounts,
    ) -> anyhow::Result<(Option<AccountHash>, Option<ThreadAccount>, Option<AccountInfo>)> {
        use tvm_block::Deserializable;

        // Look up the old account state from durable state first, then TVM
        let old_shard_acc = if let Some(account_info) =
            self.dapp_accounts.map_get(accounts_map, routing.account_id())?
        {
            let ThreadAccount::Tvm(account_cell) =
                self.accounts.get(&account_info.account_hash)?.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Missing account data for hash {} in durable state",
                        account_info.account_hash
                    )
                })?
            else {
                anyhow::bail!("Account data for hash {} is not Tvm", account_info.account_hash)
            };
            ShardAccount::with_account_root(
                account_cell,
                UInt256::from(account_info.last_trans_hash),
                account_info.last_trans_lt,
                Some(UInt256::from(routing.dapp_id())),
            )
        } else if let Ok(Some(tvm_acc)) = old_tvm_accounts.account(&routing.account_id().into()) {
            tvm_acc
        } else {
            anyhow::bail!(
                "AccountMerkleUpdate for non-existent account: {}",
                routing.account_id().to_hex_string()
            );
        };
        let old_shard_acc_cell = old_shard_acc
            .serialize()
            .map_err(|e| anyhow::anyhow!("Failed to serialize old shard account: {e}",))?;

        // Deserialize merkle update
        let update_cell = tvm_types::read_single_root_boc(update_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to read merkle update BOC: {e}"))?;
        let merkle_update = tvm_block::MerkleUpdate::construct_from_cell(update_cell)
            .map_err(|e| anyhow::anyhow!("Failed to construct merkle update: {e}"))?;

        // Apply merkle update to get new cell
        let new_shard_acc_cell = merkle_update
            .apply_for(&old_shard_acc_cell)
            .map_err(|e| anyhow::anyhow!("Failed to apply account merkle update: {e}"))?;

        let new_shard_acc = ShardAccount::construct_from_cell(new_shard_acc_cell)
            .map_err(|e| anyhow::anyhow!("Failed to construct account from cell: {e}"))?;
        let new_state_account = ThreadStateAccount::Tvm(new_shard_acc);

        Self::state_account_update(routing, &new_state_account)
    }

    fn state_account_update(
        routing: &AccountRouting,
        state_account: &ThreadStateAccount,
    ) -> anyhow::Result<(Option<AccountHash>, Option<ThreadAccount>, Option<AccountInfo>)> {
        // Redirect stubs have no inner cell — store only metadata in the trie
        if state_account.is_redirect() {
            return Ok((
                None,
                None,
                Some(AccountInfo {
                    account_hash: AccountHash::default(),
                    last_trans_hash: state_account.last_trans_hash(),
                    last_trans_lt: state_account.last_trans_lt(),
                    redirect_dapp_id: state_account.get_dapp_id(),
                }),
            ));
        }
        let account = state_account.account()?;
        if state_account.get_dapp_id() != Some(*routing.dapp_id()) {
            tracing::trace!(
                target: "monit",
                "Account dapp id mismatch: expected [{}], got [{:?}]",
                routing.dapp_id().to_hex_string(),
                state_account.get_dapp_id()
            );
        }
        let account_hash = account.hash();
        Ok((
            Some(account_hash),
            Some(account),
            Some(AccountInfo {
                account_hash,
                last_trans_hash: state_account.last_trans_hash(),
                last_trans_lt: state_account.last_trans_lt(),
                redirect_dapp_id: None,
            }),
        ))
    }

    fn update_maps(
        &self,
        state: &ThreadDApps::MapRef,
        updates: HashMap<
            DAppIdentifier,
            (DAppAccounts::MapRef, Vec<(AccountIdentifier, Option<AccountInfo>)>),
        >,
    ) -> anyhow::Result<ThreadDApps::MapRef> {
        let mut dapp_updates = Vec::new();
        for (dapp_id, (map, accounts)) in updates {
            let new_accounts_map = self.dapp_accounts.map_update(&map, accounts)?;
            let new_accounts_map_hash = self.dapp_accounts.map_hash(&new_accounts_map);
            self.dapp_accounts.index_set(&new_accounts_map_hash, &new_accounts_map)?;
            dapp_updates.push((dapp_id, Some(new_accounts_map_hash)));
        }

        self.thread_dapps.map_update(state, &dapp_updates)
    }

    pub(crate) fn state_apply_diff(
        &self,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &ThreadDApps::MapRef,
        diff: &DurableThreadAccountsDiff,
    ) -> anyhow::Result<ThreadDApps::MapRef> {
        self.state_update(old_tvm_accounts, state, &diff.accounts)
    }

    pub fn export_durable_snapshot(
        &self,
        state: &ThreadDApps::MapRef,
    ) -> anyhow::Result<CompositeDurableStateSnapshot> {
        // 1. Export thread_dapps trie
        let thread_dapps_snapshot = self.thread_dapps.export_snapshot(state);

        // 2. Iterate all DApps
        let dapp_entries = self.thread_dapps.collect_values(state);

        let mut dapp_account_snapshots = Vec::new();
        let mut all_account_hashes = HashSet::new();

        for (_dapp_id, dapp_hash) in &dapp_entries {
            // 3. Get dapp accounts map ref
            let Some(dapp_map_ref) = self.dapp_accounts.index_get(dapp_hash)? else {
                continue;
            };

            // 4. Export dapp accounts trie
            let dapp_snapshot = self.dapp_accounts.export_snapshot(&dapp_map_ref);
            dapp_account_snapshots.push((*dapp_hash, dapp_snapshot));

            // 5. Collect all account hashes
            let account_values = self.dapp_accounts.collect_values(&dapp_map_ref);
            for (_account_id, account_info) in account_values {
                all_account_hashes.insert(account_info.account_hash);
            }
        }

        // 6. Export account data
        let mut accounts = Vec::new();
        for account_hash in all_account_hashes {
            if let Some(account) = self.accounts.get(&account_hash)? {
                let bytes = account.write_bytes()?;
                accounts.push((account_hash, bytes));
            }
        }

        Ok(CompositeDurableStateSnapshot {
            thread_dapps: thread_dapps_snapshot,
            dapp_accounts: dapp_account_snapshots,
            accounts,
        })
    }

    pub fn import_durable_snapshot(
        &self,
        snapshot: CompositeDurableStateSnapshot,
    ) -> anyhow::Result<ThreadDApps::MapRef> {
        // 1. Import accounts
        let account_updates: Vec<(AccountHash, Option<ThreadAccount>)> = snapshot
            .accounts
            .into_iter()
            .map(|(hash, bytes)| {
                let account = ThreadAccount::read_bytes(&bytes)?;
                Ok((hash, Some(account)))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.accounts.update(&account_updates)?;

        // 2. Import dapp accounts tries and register in index
        for (hash, dapp_snapshot) in snapshot.dapp_accounts {
            let new_map_ref = self.dapp_accounts.import_snapshot(dapp_snapshot);
            self.dapp_accounts.index_set(&hash, &new_map_ref)?;
        }

        // 3. Import thread_dapps trie
        let new_durable_ref = self.thread_dapps.import_snapshot(snapshot.thread_dapps);

        Ok(new_durable_ref)
    }
}
