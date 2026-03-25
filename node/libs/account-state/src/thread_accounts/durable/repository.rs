use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;

use multi_map::DurableMultiMapRepository;
use multi_map::MapKey;
use multi_map::MapKeyPath;
use multi_map::MultiMapRef;
use multi_map::MultiMapRepository;
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
use crate::thread_accounts::durable::dapp_map_value::DAppMapValue;
use crate::thread_accounts::durable::snapshot::CompositeDurableStateSnapshot;
use crate::DAppAccountMapHash;
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

/// Thread-level durable state: a two-level map.
///
/// Level 1: DAppIdentifier → DAppMapValue (embeds account map directly)
/// Level 2 (embedded): AccountIdentifier → AccountInfo
///
/// States are persisted as self-contained files in `state_maps/`.
/// Old-format fallback reads from `thread_dapps/` + `dapp_accounts/` directories.
pub type ThreadStateRef = MultiMapRef<DAppMapValue>;

#[derive(Clone)]
pub struct DurableThreadAccountsRepository<Accounts: AccountsRepository> {
    durable_path: PathBuf,
    dapp_map_repo: MultiMapRepository<DAppMapValue>,
    accounts: Accounts,
    account_map_repo: MultiMapRepository<AccountInfo>,
}

impl<Accounts: AccountsRepository> DurableThreadAccountsRepository<Accounts> {
    pub fn new(durable_path: PathBuf, accounts: Accounts) -> anyhow::Result<Self> {
        Ok(Self {
            durable_path,
            dapp_map_repo: MultiMapRepository::new(),
            accounts,
            account_map_repo: MultiMapRepository::new(),
        })
    }

    pub fn aerospike_cache_stat(
        &self,
    ) -> Option<crate::thread_accounts::durable::accounts::AerospikeAccountsCacheStat> {
        self.accounts.aerospike_cache_stat()
    }

    pub fn commit(&self) -> anyhow::Result<()> {
        self.accounts.commit()
    }

    // ---- State persistence ----

    /// Save a state to disk as a self-contained file.
    /// The file contains the full two-level tree serialized with write_map.
    pub fn state_save(
        &self,
        state: &ThreadStateRef,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<()> {
        let state_dir = self.durable_path.join("state_maps");
        fs::create_dir_all(&state_dir)?;

        let filename = format!("{}.state", block_id.to_hex_string());
        let path = state_dir.join(&filename);

        // Atomic write via temp file + rename
        let tmp = path.with_extension("state.tmp");
        {
            let f = File::create(&tmp)?;
            let mut w = BufWriter::new(f);
            self.dapp_map_repo.write_map(state, &mut w)?;
            w.flush()?;
            w.into_inner()?.sync_all()?;
        }
        fs::rename(&tmp, &path)?;

        Ok(())
    }

    /// Load a state from disk. Tries new format first, falls back to old durable files.
    pub fn state_load(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<ThreadStateRef>> {
        // 1. Try new format first
        let state_dir = self.durable_path.join("state_maps");
        let filename = format!("{}.state", block_id.to_hex_string());
        let path = state_dir.join(&filename);

        if path.exists() {
            let f = File::open(&path)?;
            let state = self.dapp_map_repo.read_map(&mut BufReader::new(f))?;
            return Ok(Some(state));
        }

        // 2. Fall back to old durable files
        self.load_from_old_durable(block_id)
    }

    /// Load state from old-format durable directories (thread_dapps/ + dapp_accounts/).
    /// Any failure (missing dirs, corrupt files) is treated as "no old state".
    fn load_from_old_durable(
        &self,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<Option<ThreadStateRef>> {
        let old_thread_dapps_path = self.durable_path.join("thread_dapps");
        let old_dapp_accounts_path = self.durable_path.join("dapp_accounts");

        // Try to load old L1. Any failure (missing dir, corrupt files) → None
        let old_l1 =
            match DurableMultiMapRepository::<DAppAccountMapHash, BlockIdentifier, ()>::load(
                old_thread_dapps_path,
            ) {
                Ok(repo) => repo,
                Err(_) => return Ok(None),
            };

        let Some((old_l1_map, _)) = old_l1.index_get(block_id) else {
            return Ok(None);
        };

        // Try to load old L2. Failure → treat all dapp account maps as empty
        let old_l2 = DurableMultiMapRepository::<AccountInfo, DAppAccountMapHash, ()>::load(
            old_dapp_accounts_path,
        )
        .ok();

        // Convert: for each dapp in L1, look up its account map from L2, wrap as DAppMapValue
        let l1_entries = old_l1.collect_values(&old_l1_map);

        let mut dapp_updates: Vec<(MapKey, Option<DAppMapValue>)> = Vec::new();
        for (dapp_key, dapp_hash) in l1_entries {
            let dapp_map = old_l2
                .as_ref()
                .and_then(|l2| l2.index_get(&dapp_hash))
                .map(|(map_ref, _)| map_ref)
                .unwrap_or_else(MultiMapRepository::<AccountInfo>::new_map);
            dapp_updates.push((dapp_key, Some(DAppMapValue::new(dapp_map))));
        }

        // Build the new L1 map with embedded DAppMapValue entries
        let mut new_state = Self::new_state();
        if !dapp_updates.is_empty() {
            new_state = self.dapp_map_repo.map_update(&new_state, &dapp_updates);
        }

        // Preserve the root_path from old map
        new_state.root_path = old_l1_map.root_path;

        Ok(Some(new_state))
    }

    // ---- State accessors (legacy compat wrappers) ----

    pub fn get_state(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<ThreadStateRef>> {
        self.state_load(block_id)
    }

    pub fn new_state() -> ThreadStateRef {
        MultiMapRepository::<DAppMapValue>::new_map()
    }

    pub fn set_state(
        &self,
        block_id: &BlockIdentifier,
        state: &ThreadStateRef,
    ) -> anyhow::Result<()> {
        self.state_save(state, block_id)?;
        self.accounts.commit()?;
        Ok(())
    }

    pub fn state_hash(&self, state: &ThreadStateRef) -> ThreadAccountsHash {
        ThreadAccountsHash::new(self.dapp_map_repo.map_hash(state).0)
    }

    // ---- Map operations ----

    pub fn state_account(
        &self,
        state: &ThreadStateRef,
        routing: &AccountRouting,
    ) -> anyhow::Result<Option<ThreadStateAccount>> {
        let Some(dapp_value) =
            self.dapp_map_repo.map_get(state, &MapKey(*routing.dapp_id().as_array()))
        else {
            return Ok(None);
        };
        let Some(account_info) = self
            .account_map_repo
            .map_get(&dapp_value.map, &MapKey(*routing.account_id().as_array()))
        else {
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
        state: &ThreadStateRef,
        dapp_id_path: DAppIdentifierPath,
    ) -> anyhow::Result<(ThreadStateRef, ThreadStateRef)> {
        let DAppIdentifierPath { prefix, len } = dapp_id_path;
        Ok(self
            .dapp_map_repo
            .map_split(state, MapKeyPath { prefix: MapKey(*prefix.as_array()), len }))
    }

    pub(crate) fn merge(
        &self,
        a: &ThreadStateRef,
        b: &ThreadStateRef,
    ) -> anyhow::Result<ThreadStateRef> {
        Ok(self.dapp_map_repo.merge(a, b))
    }

    pub fn state_update(
        &self,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &ThreadStateRef,
        accounts: &[(AccountRouting, ThreadAccountUpdate)],
    ) -> anyhow::Result<ThreadStateRef> {
        tracing::trace!(target: "monit", "Update accounts in durable state: prepare update: {}", accounts.len());
        let (accounts, maps) = self.prepare_updates(old_tvm_accounts, state, accounts)?;
        tracing::trace!(target: "monit", "Update accounts in durable maps: {}", accounts.len());
        self.accounts.update(&accounts)?;
        tracing::trace!(target: "monit", "Update accounts in durable maps: {}", accounts.len());
        let res = self.update_maps(state, maps);
        tracing::trace!(target: "monit", "Update accounts in durable finished");
        res
    }

    fn prepare_updates(
        &self,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &ThreadStateRef,
        accounts: &[(AccountRouting, ThreadAccountUpdate)],
    ) -> anyhow::Result<(
        Vec<(AccountHash, Option<ThreadAccount>)>,
        HashMap<
            DAppIdentifier,
            (MultiMapRef<AccountInfo>, Vec<(AccountIdentifier, Option<AccountInfo>)>),
        >,
    )> {
        let mut account_updates = Vec::new();
        let mut map_updates = HashMap::new();
        for (routing, state_account) in accounts {
            tracing::trace!(target: "builder", "Update account in durable state: {}", routing);
            let map_update = match map_updates.entry(*routing.dapp_id()) {
                Entry::Occupied(existing) => existing.into_mut(),
                Entry::Vacant(entry) => {
                    entry.insert((self.ensure_dapp_map(state, routing.dapp_id()), Vec::new()))
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
        state: &ThreadStateRef,
        dapp_id: &DAppIdentifier,
    ) -> MultiMapRef<AccountInfo> {
        match self.dapp_map_repo.map_get(state, &MapKey(*dapp_id.as_array())) {
            Some(dapp_value) => dapp_value.map,
            None => MultiMapRepository::<AccountInfo>::new_map(),
        }
    }

    fn get_account_update(
        &self,
        routing: &AccountRouting,
        state_account: &ThreadAccountUpdate,
        accounts_map: &MultiMapRef<AccountInfo>,
        old_tvm_accounts: &tvm_block::ShardAccounts,
    ) -> anyhow::Result<(Option<AccountHash>, Option<ThreadAccount>, Option<AccountInfo>)> {
        match state_account {
            ThreadAccountUpdate::UpdateOrInsert(state_account) => {
                Self::state_account_update(routing, state_account)
            }
            ThreadAccountUpdate::Remove => {
                let account_hash = self
                    .account_map_repo
                    .map_get(accounts_map, &MapKey(*routing.account_id().as_array()))
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
        accounts_map: &MultiMapRef<AccountInfo>,
        old_tvm_accounts: &tvm_block::ShardAccounts,
    ) -> anyhow::Result<(Option<AccountHash>, Option<ThreadAccount>, Option<AccountInfo>)> {
        use tvm_block::Deserializable;

        // Look up the old account state from durable state first, then TVM
        let old_shard_acc = if let Some(account_info) =
            self.account_map_repo.map_get(accounts_map, &MapKey(*routing.account_id().as_array()))
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
        state: &ThreadStateRef,
        updates: HashMap<
            DAppIdentifier,
            (MultiMapRef<AccountInfo>, Vec<(AccountIdentifier, Option<AccountInfo>)>),
        >,
    ) -> anyhow::Result<ThreadStateRef> {
        let mut dapp_updates = Vec::new();
        for (dapp_id, (map, accounts)) in updates {
            let trie_patch: Vec<_> =
                accounts.into_iter().map(|(id, info)| (MapKey(*id.as_array()), info)).collect();
            let new_accounts_map = self.account_map_repo.map_update(&map, &trie_patch);
            dapp_updates
                .push((MapKey(*dapp_id.as_array()), Some(DAppMapValue::new(new_accounts_map))));
        }
        Ok(self.dapp_map_repo.map_update(state, &dapp_updates))
    }

    pub(crate) fn state_apply_diff(
        &self,
        old_tvm_accounts: &tvm_block::ShardAccounts,
        state: &ThreadStateRef,
        diff: &DurableThreadAccountsDiff,
    ) -> anyhow::Result<ThreadStateRef> {
        self.state_update(old_tvm_accounts, state, &diff.accounts)
    }

    // ---- Snapshot export/import ----

    pub fn export_durable_snapshot(
        &self,
        state: &ThreadStateRef,
    ) -> anyhow::Result<CompositeDurableStateSnapshot> {
        // 1. Collect all dapp entries from the thread state
        let dapp_entries = self.dapp_map_repo.collect_values(state);

        // 2. Build thread_dapps snapshot (TrieMapSnapshot<DAppAccountMapHash>)
        //    by creating a temporary map with DAppAccountMapHash values
        let hash_repo = MultiMapRepository::<DAppAccountMapHash>::new();
        let mut hash_map = MultiMapRepository::<DAppAccountMapHash>::new_map();
        let hash_updates: Vec<_> = dapp_entries
            .iter()
            .map(|(k, v)| {
                let hash = DAppAccountMapHash::new(v.map.root.hash());
                (*k, Some(hash))
            })
            .collect();
        hash_map = hash_repo.map_update(&hash_map, &hash_updates);
        let thread_dapps_snapshot = multi_map::convert::to_trie_snapshot(&hash_map);

        // 3. Build dapp_account snapshots and collect account hashes
        let mut dapp_account_snapshots = Vec::new();
        let mut all_account_hashes = HashSet::new();

        for (_dapp_key, dapp_value) in &dapp_entries {
            let dapp_hash = DAppAccountMapHash::new(dapp_value.map.root.hash());
            let dapp_snapshot = multi_map::convert::to_trie_snapshot(&dapp_value.map);
            dapp_account_snapshots.push((dapp_hash, dapp_snapshot));

            let account_values = self.account_map_repo.collect_values(&dapp_value.map);
            for (_account_id, account_info) in account_values {
                all_account_hashes.insert(account_info.account_hash);
            }
        }

        // 4. Export account data
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
    ) -> anyhow::Result<ThreadStateRef> {
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

        // 2. Import dapp accounts: convert TrieMapSnapshot<AccountInfo> → MultiMapRef<AccountInfo>
        let mut dapp_hash_to_map: HashMap<DAppAccountMapHash, MultiMapRef<AccountInfo>> =
            HashMap::new();
        for (hash, dapp_snapshot) in snapshot.dapp_accounts {
            let map = multi_map::convert::from_trie_snapshot(dapp_snapshot);
            dapp_hash_to_map.insert(hash, map);
        }

        // 3. Import thread_dapps trie (as DAppAccountMapHash) and collect entries
        let hash_map =
            multi_map::convert::from_trie_snapshot::<DAppAccountMapHash>(snapshot.thread_dapps);
        let hash_repo = MultiMapRepository::<DAppAccountMapHash>::new();
        let hash_entries = hash_repo.collect_values(&hash_map);

        // 4. Build DAppMapValue map from hash entries
        //    Note: multiple DApps may share the same hash (e.g., empty maps after account removal),
        //    so we use .get().cloned() instead of .remove().
        let mut dapp_map_updates: Vec<(MapKey, Option<DAppMapValue>)> = Vec::new();
        for (dapp_key, dapp_hash) in hash_entries {
            let account_map = dapp_hash_to_map.get(&dapp_hash).cloned().ok_or_else(|| {
                anyhow::anyhow!("Missing dapp account snapshot for hash {}", dapp_hash)
            })?;
            dapp_map_updates.push((dapp_key, Some(DAppMapValue::new(account_map))));
        }

        let mut state = Self::new_state();
        state = self.dapp_map_repo.map_update(&state, &dapp_map_updates);

        Ok(state)
    }
}
