use std::io::Write;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct AccountsRepository {
    data_dir: PathBuf,
}

impl AccountsRepository {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir: data_dir.join("accounts") }
    }

    fn account_path(
        &self,
        account_id: &tvm_types::UInt256,
        last_trans_hash: &tvm_types::UInt256,
        last_trans_lt: u64,
    ) -> PathBuf {
        self.data_dir
            .join(account_id.to_hex_string())
            .join(format!("{}_{:x}", last_trans_lt, last_trans_hash))
    }

    pub fn load_account(
        &self,
        account_id: &tvm_types::UInt256,
        last_trans_hash: &tvm_types::UInt256,
        last_trans_lt: u64,
    ) -> anyhow::Result<tvm_types::Cell> {
        let path = self.account_path(account_id, last_trans_hash, last_trans_lt);
        let data = std::fs::read(&path).map_err(|err| {
            anyhow::format_err!("Failed to read account {}: {err}", path.display())
        })?;
        tvm_types::boc::read_single_root_boc(data).map_err(|err| {
            anyhow::format_err!("Failed to deserialize account {}: {err}", path.display())
        })
    }

    pub fn store_account(
        &self,
        account_id: &tvm_types::UInt256,
        last_trans_hash: &tvm_types::UInt256,
        last_trans_lt: u64,
        account: tvm_types::Cell,
    ) -> anyhow::Result<()> {
        let path = self.account_path(account_id, last_trans_hash, last_trans_lt);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                anyhow::format_err!("Failed to create directory {}: {err}", parent.display())
            })?;
        }
        let data = tvm_types::boc::write_boc(&account).map_err(|err| {
            anyhow::format_err!("Failed to serialize account {}: {err}", path.display())
        })?;
        let mut file = std::fs::File::create(&path).map_err(|err| {
            anyhow::format_err!("Failed to write account {}: {err}", path.display())
        })?;
        file.write_all(&data).map_err(|err| {
            anyhow::format_err!("Failed to write account {}: {err}", path.display())
        })?;
        file.sync_all()?;
        tracing::trace!("File saved: {:?}", path);
        Ok(())
    }
}
