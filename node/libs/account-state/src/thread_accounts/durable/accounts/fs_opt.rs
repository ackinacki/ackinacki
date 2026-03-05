use std::fs::File;
use std::io::BufWriter;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use node_types::AccountHash;
use serde::Deserialize;
use serde::Serialize;
use trie_map::durable::prepare_fs_commit;
use trie_map::durable::read_from_file;
use trie_map::durable::write_commit_to_file;
use trie_map::durable::DurableIndex;
use trie_map::durable::DurableIndexUpdate;
use trie_map::durable::DurableSeqCommit;

use crate::account::ThreadAccount;
use crate::thread_accounts::durable::accounts::AccountsRepository;

#[derive(Clone, Default)]
pub struct FsOptAccountsStore {
    root_path: PathBuf,
    state: Arc<parking_lot::RwLock<State>>,
    commit_guard: Arc<parking_lot::Mutex<()>>,
}

type Index = DurableIndex<AccountHash, IndexValue>;
type IndexUpdate = DurableIndexUpdate<AccountHash, IndexValue>;

#[derive(Default)]
struct State {
    index: Index,
    staging_accounts_bytes: Arc<[u8]>,
    uncommitted_accounts_bytes: Vec<u8>,
    commit: Commit,
}

fn empty_staging() -> Arc<[u8]> {
    Arc::from(&[][..])
}

impl State {
    #[inline]
    fn staging_len(&self) -> usize {
        self.staging_accounts_bytes.len()
    }

    fn prepare_fs_commit(
        &mut self,
    ) -> anyhow::Result<Option<(Commit, Vec<IndexUpdate>, Arc<[u8]>)>> {
        anyhow::ensure!(self.staging_accounts_bytes.is_empty(), "staging not empty");

        if self.index.updates.is_empty() && self.uncommitted_accounts_bytes.is_empty() {
            return Ok(None);
        }

        let base_commit = self.commit;
        let frozen_updates = std::mem::take(&mut self.index.updates);

        let bytes_vec = std::mem::take(&mut self.uncommitted_accounts_bytes);
        let bytes_arc: Arc<[u8]> = Arc::from(bytes_vec.into_boxed_slice());
        self.staging_accounts_bytes = bytes_arc.clone();

        Ok(Some((base_commit, frozen_updates, bytes_arc)))
    }

    fn apply_fs_commit(
        &mut self,
        fs_commit_result: anyhow::Result<()>,
        new_commit: Commit,
        frozen_updates: Vec<IndexUpdate>,
    ) -> anyhow::Result<()> {
        match fs_commit_result {
            Ok(()) => {
                self.commit = new_commit;
                self.staging_accounts_bytes = empty_staging();
                Ok(())
            }
            Err(e) => {
                let staging = std::mem::take(&mut self.staging_accounts_bytes);
                if !staging.is_empty() {
                    self.uncommitted_accounts_bytes.splice(0..0, staging.iter().copied());
                }
                self.index.updates.splice(0..0, frozen_updates);
                Err(e)
            }
        }
    }
}

impl FsOptAccountsStore {
    pub fn new(root_path: PathBuf) -> anyhow::Result<Self> {
        let (commit, index) =
            if let Ok(Some(commit)) = read_from_file::<Commit>(&root_path, "commit") {
                let index = Index::read_commited(root_path.join("index"), commit.index)?;
                (commit, index)
            } else {
                (Commit::default(), Index::default())
            };
        Ok(Self {
            root_path,
            state: Arc::new(parking_lot::RwLock::new(State {
                index,
                uncommitted_accounts_bytes: Vec::new(),
                staging_accounts_bytes: empty_staging(),
                commit,
            })),
            commit_guard: Arc::new(parking_lot::Mutex::new(())),
        })
    }

    pub fn commit(&self) -> anyhow::Result<()> {
        let _commit_guard = self.commit_guard.lock();

        let Some((mut commit, index_updates, account_staging_bytes)) =
            self.state.write().prepare_fs_commit()?
        else {
            return Ok(());
        };

        let fs_commit_result =
            self.commit_to_fs(&index_updates, account_staging_bytes.as_ref(), &mut commit);

        self.state.write().apply_fs_commit(fs_commit_result, commit, index_updates)
    }

    fn commit_to_fs(
        &self,
        frozen_updates: &[IndexUpdate],
        frozen_bytes: &[u8],
        new_commit: &mut Commit,
    ) -> anyhow::Result<()> {
        if !frozen_updates.is_empty() {
            append_index_updates(
                self.root_path.join("index"),
                &mut new_commit.index.bytes,
                frozen_updates,
            )?;
            new_commit.index.count += frozen_updates.len() as u32;
        }

        if !frozen_bytes.is_empty() {
            append_bytes(
                self.root_path.join("values"),
                &mut new_commit.accounts_bytes,
                frozen_bytes,
            )?;
        }

        write_commit_to_file(&self.root_path, "commit", new_commit)?;
        Ok(())
    }

    fn read_account(
        root_path: &Path,
        state: &parking_lot::RwLock<State>,
        index_value: IndexValue,
    ) -> anyhow::Result<ThreadAccount> {
        enum Src {
            Disk { offset: u64 },
            Staging { bytes: Arc<[u8]>, offset: usize, len: usize },
            UncommittedCopy(Vec<u8>),
        }

        let src = {
            let state = state.read();
            let committed_bytes = state.commit.accounts_bytes;

            if index_value.offset < committed_bytes {
                Src::Disk { offset: index_value.offset }
            } else {
                let mem_offset = (index_value.offset - committed_bytes) as usize;
                let len = index_value.len as usize;
                let staging_len = state.staging_len();

                if mem_offset < staging_len {
                    Src::Staging {
                        bytes: state.staging_accounts_bytes.clone(),
                        offset: mem_offset,
                        len,
                    }
                } else {
                    let offset = mem_offset - staging_len;
                    Src::UncommittedCopy(
                        state.uncommitted_accounts_bytes[offset..offset + len].to_vec(),
                    )
                }
            }
        };

        match src {
            Src::Disk { offset } => {
                let mut f = File::open(root_path.join("values"))?;
                f.seek(SeekFrom::Start(offset))?;
                Ok(bincode::deserialize_from(&mut f)?)
            }
            Src::Staging { bytes, offset: off, len } => {
                Ok(bincode::deserialize(&bytes[off..off + len])?)
            }
            Src::UncommittedCopy(chunk) => Ok(bincode::deserialize(&chunk)?),
        }
    }

    fn prepare_update(
        accounts: &[(AccountHash, Option<ThreadAccount>)],
    ) -> anyhow::Result<(Vec<(AccountHash, usize)>, Vec<u8>)> {
        let mut updates = Vec::with_capacity(accounts.len());
        let mut added_bytes = Vec::with_capacity(accounts.len() * 1024);
        for (hash, account) in accounts {
            let len = if let Some(account) = account {
                let save_len = added_bytes.len();
                bincode::serialize_into(&mut added_bytes, &account)?;
                added_bytes.len() - save_len
            } else {
                0
            };
            updates.push((*hash, len));
        }
        Ok((updates, added_bytes))
    }

    fn apply_update(&self, updates: Vec<(AccountHash, usize)>, added_bytes: &[u8]) {
        let mut state = self.state.write();

        let mut offset = state.commit.accounts_bytes as usize
            + state.staging_len()
            + state.uncommitted_accounts_bytes.len();

        state.uncommitted_accounts_bytes.extend_from_slice(added_bytes);

        for (hash, len) in updates {
            state.index.update(
                hash,
                if len > 0 { Some(IndexValue::new(offset as u64, len as u32)) } else { None },
            );
            offset += len;
        }
    }
}

impl AccountsRepository for FsOptAccountsStore {
    fn get(&self, hash: &AccountHash) -> anyhow::Result<Option<ThreadAccount>> {
        let index_value = { self.state.read().index.values.get(hash).copied() };
        match index_value {
            Some(v) => Ok(Some(Self::read_account(&self.root_path, &self.state, v)?)),
            None => Ok(None),
        }
    }

    fn iter_rx(
        &self,
        hashes: impl IntoIterator<Item = AccountHash> + Send,
    ) -> std::sync::mpsc::Receiver<anyhow::Result<ThreadAccount>> {
        let (read_tx, read_rx) = std::sync::mpsc::sync_channel(1000);
        let mut index_values = Vec::new();
        let state_lock = self.state.read();
        for hash in hashes {
            index_values.push(state_lock.index.values.get(&hash).copied());
        }
        drop(state_lock);
        let root_path = self.root_path.clone();
        let state = Arc::clone(&self.state);
        std::thread::Builder::new()
            .name("account-repository-iter".to_string())
            .spawn(move || {
                for index_value in index_values.into_iter().flatten() {
                    let account = Self::read_account(&root_path, &state, index_value);
                    if read_tx.send(account).is_err() {
                        break;
                    }
                }
            })
            .map_err(|err| {
                anyhow::anyhow!("Failed to spawn account repository iter thread: {}", err)
            })
            .expect("Failed to spawn account repository iter thread");
        read_rx
    }

    fn update(&self, accounts: &[(AccountHash, Option<ThreadAccount>)]) -> anyhow::Result<()> {
        let (updates, added_bytes) = Self::prepare_update(accounts)?;
        self.apply_update(updates, &added_bytes);
        Ok(())
    }

    fn commit(&self) -> anyhow::Result<()> {
        self.commit()
    }
}

#[derive(Default, Clone, Copy, Serialize, Deserialize)]
struct IndexValue {
    offset: u64,
    len: u32,
}

impl IndexValue {
    fn new(offset: u64, len: u32) -> Self {
        Self { offset, len }
    }
}

#[derive(Default, Clone, Copy, Serialize, Deserialize)]
struct Commit {
    accounts_bytes: u64,
    index: DurableSeqCommit,
}

fn append_index_updates(
    path: PathBuf,
    commit_bytes: &mut u64,
    items: &[DurableIndexUpdate<AccountHash, IndexValue>],
) -> anyhow::Result<()> {
    let f = prepare_fs_commit(path, *commit_bytes)?;
    let mut w = BufWriter::with_capacity(256 * 1024, f);
    for item in items {
        bincode::serialize_into(&mut w, item)?;
    }
    w.flush()?;

    let mut f = w.into_inner().context("into_inner BufWriter")?;
    let new_len = f.stream_position()?;
    f.sync_data()?;
    *commit_bytes = new_len;
    Ok(())
}

fn append_bytes(path: PathBuf, commit_bytes: &mut u64, bytes: &[u8]) -> anyhow::Result<()> {
    let mut f = prepare_fs_commit(path, *commit_bytes)?;
    f.write_all(bytes)?;
    let new_len = f.stream_position()?;
    f.sync_data()?;
    *commit_bytes = new_len;
    Ok(())
}
