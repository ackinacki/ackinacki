use std::path::PathBuf;

use node_types::AccountHash;

use crate::account::ThreadAccount;
use crate::fs_utils;
use crate::thread_accounts::durable::accounts::AccountsRepository;

#[derive(Clone, Default)]
pub struct FsAccountsStore {
    root_path: PathBuf,
}

impl FsAccountsStore {
    pub fn new(root_path: PathBuf) -> anyhow::Result<Self> {
        tracing::trace!(target: "monit", "Creating FS account repository");
        Ok(Self { root_path })
    }

    fn file_name(hash: &AccountHash) -> String {
        hash.to_hex_string()
    }
}

impl AccountsRepository for FsAccountsStore {
    fn get(&self, hash: &AccountHash) -> anyhow::Result<Option<ThreadAccount>> {
        fs_utils::read(&self.root_path, Self::file_name(hash))
    }

    fn iter_rx(
        &self,
        hashes: impl IntoIterator<Item = AccountHash> + Send,
    ) -> std::sync::mpsc::Receiver<anyhow::Result<ThreadAccount>> {
        let (read_tx, read_rx) = std::sync::mpsc::sync_channel(1000);
        let root_path = self.root_path.clone();
        let hashes = hashes.into_iter().collect::<Vec<_>>();
        std::thread::Builder::new()
            .name("account-repository-iter".to_string())
            .spawn(move || {
                for hash in hashes {
                    if let Some(result) =
                        fs_utils::read(root_path.clone(), Self::file_name(&hash)).transpose()
                    {
                        if read_tx.send(result).is_err() {
                            break;
                        }
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
        for (hash, account) in accounts {
            fs_utils::write(&self.root_path, Self::file_name(hash), account.as_ref())?;
        }
        Ok(())
    }

    fn commit(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
