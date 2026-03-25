use std::sync::Arc;

use node_types::AccountCodeHash;
use node_types::AccountDataHash;
use node_types::AccountHash;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;
use node_types::TransactionHash;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug, Default)]
pub struct AvmStateAccount {
    pub account: AvmAccount,
    pub last_trans_hash: TransactionHash,
    pub last_trans_lt: u64,
    pub dapp_id: Option<DAppIdentifier>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug, Default)]
pub struct AvmAccount(pub(crate) Arc<AvmAccountInner>);

impl AvmAccount {
    pub fn new(
        hash: AccountHash,
        metadata: AvmAccountMetadata,
        code_hash: Option<AccountCodeHash>,
        data_hash: Option<AccountDataHash>,
        data: Option<AvmAccountData>,
    ) -> Self {
        Self(Arc::new(AvmAccountInner { hash, metadata, code_hash, data_hash, data }))
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug, Default)]
pub struct AvmAccountInner {
    pub hash: AccountHash,
    pub metadata: AvmAccountMetadata,
    pub code_hash: Option<AccountCodeHash>,
    pub data_hash: Option<AccountDataHash>,
    pub data: Option<AvmAccountData>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug, Default)]
pub struct AvmAccountMetadata {
    pub id: AccountIdentifier,
    pub storage_used_bytes: u32,
    pub last_paid: u32,
    pub due_payment: Option<AvmAmount>,
    pub last_trans_lt: u64,
    pub balance: AvmAmountCollection,
    pub init_code_hash: Option<AccountCodeHash>,
    pub state: AvmAccountState,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug, Default)]
pub enum AvmAccountState {
    #[default]
    Uninit,
    Active,
    Frozen {
        state_init_hash: AccountHash,
    },
}

pub type AvmAmount = u128;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug, Default)]
pub struct AvmAmountCollection {
    pub value: AvmAmount,
    pub extra: Vec<(u32, AvmAmount)>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug, Default)]
pub struct AvmAccountData {
    pub data: Vec<u8>,
}
