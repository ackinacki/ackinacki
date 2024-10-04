// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Formatter;
use std::fmt::{self};

use super::archive::ArchMessage;

#[derive(Clone, Debug)]
pub struct SerializedItem {
    pub id: String,
    pub data: serde_json::Value,
}

pub enum DBStoredRecord {
    Block(SerializedItem),
    Transactions(Vec<serde_json::Value>),
    Accounts(Vec<serde_json::Value>),
    Messages(Vec<serde_json::Value>),
    ArchMessages(Vec<ArchMessage>),
}

impl fmt::Debug for DBStoredRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DBStoredRecord::Block(val) => write!(f, "Block({})", val.id),
            DBStoredRecord::Transactions(val) => write!(f, "Transactions({})", val.len()),
            DBStoredRecord::Accounts(val) => write!(f, "Accounts({})", val.len()),
            DBStoredRecord::Messages(val) => write!(f, "Messages({})", val.len()),
            DBStoredRecord::ArchMessages(val) => write!(f, "MessagesTmp({})", val.len()),
        }
    }
}

pub trait DocumentsDb: Send + Sync {
    fn put_block(&self, item: SerializedItem) -> anyhow::Result<()>;
    fn put_accounts(&self, items: Vec<serde_json::Value>) -> anyhow::Result<()>;
    fn put_messages(&self, items: Vec<serde_json::Value>) -> anyhow::Result<()>;
    fn put_arch_messages(&self, items: Vec<ArchMessage>) -> anyhow::Result<()>;
    fn put_transactions(&self, items: Vec<serde_json::Value>) -> anyhow::Result<()>;
    fn has_delivery_problems(&self) -> bool;
}
