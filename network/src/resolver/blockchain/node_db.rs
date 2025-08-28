use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;

use rusqlite::OpenFlags;
use tvm_block::Account;
use tvm_block::Deserializable;
use tvm_types::SliceData;
use tvm_types::UInt256;

use crate::detailed;
use crate::resolver::blockchain::accounts::Epoch;
use crate::resolver::blockchain::accounts::Root;
use crate::resolver::blockchain::AccountProvider;
use crate::resolver::blockchain::BkSetProvider;

pub struct NodeDb {
    path: PathBuf,
}

impl NodeDb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self { path: path.as_ref().to_path_buf() }
    }

    fn open_conn(path: &PathBuf) -> anyhow::Result<rusqlite::Connection> {
        rusqlite::Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| anyhow::format_err!("Failed to open node db file: {e}"))
    }

    fn get_account(conn: &rusqlite::Connection, id: &UInt256) -> anyhow::Result<Account> {
        let mut stmt = conn
            .prepare("SELECT boc FROM accounts WHERE id = ?")
            .map_err(|e| anyhow::format_err!("Failed to prepare query: {e}"))?;
        let mut rows = stmt.query([&format!("0:{}", id.to_hex_string())])?;
        let Some(row) = rows.next()? else {
            anyhow::bail!("Account not found");
        };
        let boc_bytes = row.get::<_, Vec<u8>>(0)?;
        let cell = tvm_types::boc::read_single_root_boc(&boc_bytes)
            .map_err(|err| anyhow::anyhow!("Account BOC deserialization error: {err}"))?;
        let mut slice = SliceData::load_cell(cell)
            .map_err(|err| anyhow::anyhow!("Failed to load cell: {err}"))?;
        Account::construct_from(&mut slice)
            .map_err(|err| anyhow::anyhow!("Invalid account boc: {err}"))
    }

    fn get_account_ids_by_code_hash(
        conn: &rusqlite::Connection,
        code_hash: UInt256,
    ) -> anyhow::Result<Vec<UInt256>> {
        let mut stmt = conn.prepare("SELECT id FROM accounts WHERE code_hash = ?")?;
        let mut rows = stmt
            .query([&code_hash.to_hex_string()])
            .map_err(|err| anyhow::anyhow!("Failed to query accounts by hash code: {err}"))?;
        let mut ids = Vec::new();
        while let Some(row) = rows.next()? {
            ids.push(
                row.get::<_, String>(0)
                    .map_err(|err| anyhow::anyhow!("Invalid account id: {err}"))?
                    .parse()
                    .map_err(|err| anyhow::anyhow!("Invalid account id: {err}"))?,
            );
        }
        Ok(ids)
    }

    fn get_bk_set(conn: &rusqlite::Connection) -> anyhow::Result<Vec<UInt256>> {
        let root = Root(Self::get_account(conn, &root_addr())?);
        let epoch_code_hash = root.get_epoch_code_hash()?;
        let mut wallet_ids = HashSet::new();
        for epoch_id in Self::get_account_ids_by_code_hash(conn, epoch_code_hash)? {
            let epoch = Epoch(Self::get_account(conn, &epoch_id)?);
            wallet_ids.insert(epoch.get_owner_address()?);
        }
        Ok(wallet_ids.into_iter().collect())
    }

    fn with_connection<T>(
        &self,
        f: impl FnOnce(&rusqlite::Connection) -> anyhow::Result<T>,
    ) -> Option<T> {
        match Self::open_conn(&self.path) {
            Ok(conn) => match f(&conn) {
                Ok(res) => Some(res),
                Err(err) => {
                    tracing::error!("{}", detailed(&err));
                    None
                }
            },
            Err(e) => {
                tracing::error!("Failed to open node db file: {e}");
                None
            }
        }
    }
}

const ROOT_ADDR: [u8; 32] = [0x77u8; 32];
fn root_addr() -> UInt256 {
    UInt256::from(ROOT_ADDR)
}

impl BkSetProvider for NodeDb {
    fn get_bk_set(&self) -> Vec<UInt256> {
        self.with_connection(Self::get_bk_set).unwrap_or_default()
    }
}

impl AccountProvider for NodeDb {
    fn get_account(&self, id: &UInt256) -> Option<Account> {
        self.with_connection(|conn| Self::get_account(conn, id))
    }
}
