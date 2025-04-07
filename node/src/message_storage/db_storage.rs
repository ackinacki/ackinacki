use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use rusqlite::params;
use rusqlite::Connection;
use tvm_block::GetRepresentationHash;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::types::AccountAddress;

#[derive(Clone)]
pub struct MessageDurableStorage {
    db: Arc<Mutex<Connection>>,
}

impl MessageDurableStorage {
    pub fn new(db_path: PathBuf) -> anyhow::Result<Self> {
        if !db_path.exists() {
            if let Some(parent) = db_path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            std::fs::File::create(&db_path).expect("Failed to create database file");
        }

        let conn = Connection::open(db_path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS MessageDurableStorage (
                message_hash TEXT PRIMARY KEY,
                dest_account TEXT NOT NULL,
                message_blob BLOB NOT NULL
            )",
            [],
        )?;
        Ok(Self { db: Arc::new(Mutex::new(conn)) })
    }

    pub fn write_messages(
        &self,
        messages: HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
    ) -> anyhow::Result<()> {
        if !cfg!(feature = "messages_db") {
            return Ok(());
        }
        let mut conn = self.db.lock();
        let tx = conn.transaction()?;
        for (addr, mut messages) in messages {
            messages.sort_by(|(_, a), (_, b)| a.cmp(b));
            let dest_account = addr.0.to_hex_string();
            for message in messages {
                let message_hash = message.1.message.hash().unwrap().to_hex_string();
                let message_blob = bincode::serialize(&message.1)?;
                tx.execute(
                    "INSERT OR REPLACE INTO MessageDurableStorage (message_hash, dest_account, message_blob)
             VALUES (?1, ?2, ?3)",
                    params![message_hash, dest_account, message_blob],
                )?;
            }
        }
        tracing::trace!("Write messages to durable storage prepared");
        tx.commit()?;
        tracing::trace!("Write messages to durable storage finished");
        Ok(())
    }

    pub fn write_message(
        &self,
        dest_account: &str,
        message_blob: &[u8],
        message_hash: &str,
    ) -> anyhow::Result<()> {
        if !cfg!(feature = "messages_db") {
            return Ok(());
        }
        tracing::trace!("Write message to durable storage: {message_hash} {dest_account}");
        let conn = self.db.lock();

        conn.execute(
            "INSERT OR REPLACE INTO MessageDurableStorage (message_hash, dest_account, message_blob)
             VALUES (?1, ?2, ?3)",
            params![message_hash, dest_account, message_blob],
        )?;

        Ok(())
    }

    pub fn read_message(
        &self,
        message_hash: &str,
    ) -> anyhow::Result<Option<(i64, WrappedMessage)>> {
        if !cfg!(feature = "messages_db") {
            return Ok(None);
        }
        let conn = self.db.lock();
        let mut stmt = conn.prepare(
            "SELECT rowid, dest_account, message_blob FROM MessageDurableStorage WHERE message_hash = ?1",
        )?;

        let mut rows = stmt.query(params![message_hash])?;

        if let Some(row) = rows.next()? {
            let rowid: i64 = row.get(0)?;
            let _dest_account: String = row.get(1)?;
            let message_blob: Vec<u8> = row.get(2)?;
            let wrapped_message = bincode::deserialize(&message_blob)?;

            Ok(Some((rowid, wrapped_message)))
        } else {
            Ok(None)
        }
    }

    pub fn next(
        &self,
        dest_account: &str,
        start_cursor: i64,
        end_cursor: i64,
        limit: usize,
        skip: usize,
    ) -> anyhow::Result<Vec<WrappedMessage>> {
        if !cfg!(feature = "messages_db") {
            return Ok(vec![]);
        }
        let conn = self.db.lock();

        let mut stmt = conn.prepare(
            "SELECT message_hash, message_blob FROM MessageDurableStorage
             WHERE dest_account = ?1
             AND rowid > ?2 AND rowid < ?3
             LIMIT ?4 OFFSET ?5",
        )?;

        let mut rows = stmt.query(params![dest_account, start_cursor, end_cursor, limit, skip])?;

        let mut messages = Vec::new();

        while let Some(row) = rows.next()? {
            let _message_hash: String = row.get(0)?;
            let message_blob: Vec<u8> = row.get(1)?;
            let wrapped_message = bincode::deserialize(&message_blob)?;

            messages.push(wrapped_message);
        }

        Ok(messages)
    }

    pub fn next_simple(
        &self,
        dest_account: &str,
        start_cursor: i64,
        limit: usize,
    ) -> anyhow::Result<(Vec<WrappedMessage>, Option<i64>)> {
        if !cfg!(feature = "messages_db") {
            return Ok((vec![], None));
        }
        let conn = self.db.lock();

        let mut stmt = conn.prepare(
            "SELECT message_hash, message_blob, rowid FROM MessageDurableStorage
             WHERE dest_account = ?1
             AND rowid > ?2
             LIMIT ?3",
        )?;

        let mut rows = stmt.query(params![dest_account, start_cursor, limit])?;

        let mut messages = Vec::new();
        let mut last_row_id = None;
        while let Some(row) = rows.next()? {
            let _message_hash: String = row.get(0)?;
            let message_blob: Vec<u8> = row.get(1)?;
            let row_id: i64 = row.get(2)?;
            last_row_id = Some(row_id);
            let wrapped_message = bincode::deserialize(&message_blob)?;

            messages.push(wrapped_message);
        }

        Ok((messages, last_row_id))
    }

    pub fn get_rowid_by_hash(&self, message_hash: &str) -> anyhow::Result<Option<i64>> {
        if !cfg!(feature = "messages_db") {
            return Ok(None);
        }
        let conn = self.db.lock();
        let mut stmt =
            conn.prepare("SELECT rowid FROM MessageDurableStorage WHERE message_hash = ?1")?;
        let mut rows = stmt.query(params![message_hash])?;
        if let Some(row) = rows.next()? {
            let rowid: i64 = row.get(0)?;
            Ok(Some(rowid))
        } else {
            Ok(None)
        }
    }
}
