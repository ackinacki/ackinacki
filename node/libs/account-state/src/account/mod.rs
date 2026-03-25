pub(crate) mod avm;

use base64::Engine;
use node_types::AccountCodeHash;
use node_types::AccountDataHash;
use node_types::AccountHash;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;
use node_types::TransactionHash;
use serde::Deserializer;
use serde::Serializer;
use tvm_block::Deserializable;
use tvm_block::Serializable;
use tvm_types::read_single_root_boc;
use tvm_types::write_boc;

use crate::account::avm::AvmStateAccount;
use crate::AvmAccount;

#[macro_export]
macro_rules! impl_serde_bytes {
    ($ty:path) => {
        impl ::serde::Serialize for $ty {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let bytes =
                    self.write_bytes().map_err(|e| serde::ser::Error::custom(e.to_string()))?;
                serializer.serialize_bytes(&bytes)
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $ty {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let bytes = Vec::<u8>::deserialize(deserializer)?;
                Self::read_bytes(&bytes).map_err(|e| serde::de::Error::custom(e.to_string()))
            }
        }
    };
}

pub const AVM_TAG: [u8; 4] = [0x02, 0x00, 0x00, 0x00];

#[derive(Clone, Debug, PartialEq, Eq)]
// VM agnostic account repr.
pub enum ThreadStateAccount {
    Tvm(tvm_block::ShardAccount),
    Avm(AvmStateAccount),
}

impl Default for ThreadStateAccount {
    fn default() -> Self {
        Self::Tvm(tvm_block::ShardAccount::default())
    }
}

impl ThreadStateAccount {
    pub fn new(
        account: ThreadAccount,
        last_trans_hash: TransactionHash,
        last_trans_lt: u64,
        dapp_id: Option<DAppIdentifier>,
    ) -> anyhow::Result<Self> {
        Ok(match account {
            ThreadAccount::Tvm(account) => {
                let mut shard_account = tvm_block::ShardAccount::with_params(
                    &tvm_block::Account::default(),
                    last_trans_hash.into(),
                    last_trans_lt,
                    dapp_id.map(From::from),
                )
                .map_err(|err| anyhow::anyhow!("{err}"))?;
                shard_account.set_account_cell(account).map_err(|err| anyhow::anyhow!("{err}"))?;
                Self::Tvm(shard_account)
            }
            ThreadAccount::Avm(account) => {
                Self::Avm(AvmStateAccount { account, last_trans_hash, last_trans_lt, dapp_id })
            }
        })
    }

    pub fn write_bytes(&self) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::Avm(avm_account) => {
                let mut buf = Vec::new();
                buf.extend(AVM_TAG);
                bincode::serialize_into(&mut buf, avm_account)
                    .map_err(|err| anyhow::anyhow!("{err}"))?;
                Ok(buf)
            }
            Self::Tvm(shard_account) => {
                shard_account.write_to_bytes().map_err(|err| anyhow::anyhow!("{err}"))
            }
        }
    }

    pub fn read_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.starts_with(&AVM_TAG) {
            Ok(Self::Avm(
                bincode::deserialize(&bytes[AVM_TAG.len()..])
                    .map_err(|err| anyhow::anyhow!("{err}"))?,
            ))
        } else {
            Ok(Self::Tvm(
                tvm_block::ShardAccount::construct_from_bytes(bytes)
                    .map_err(|err| anyhow::anyhow!("{err}"))?,
            ))
        }
    }

    pub fn is_redirect(&self) -> bool {
        match self {
            Self::Avm(_) => false,
            Self::Tvm(shard_account) => shard_account.is_redirect(),
        }
    }

    pub fn last_trans_hash(&self) -> TransactionHash {
        match self {
            Self::Avm(avm_account) => avm_account.last_trans_hash,
            Self::Tvm(shard_account) => shard_account.last_trans_hash().into(),
        }
    }

    pub fn last_trans_lt(&self) -> u64 {
        match self {
            Self::Avm(state_account) => state_account.last_trans_lt,
            Self::Tvm(shard_account) => shard_account.last_trans_lt(),
        }
    }

    pub fn get_dapp_id(&self) -> Option<DAppIdentifier> {
        match self {
            Self::Avm(state_account) => state_account.dapp_id,
            Self::Tvm(shard_account) => shard_account.get_dapp_id().map(From::from),
        }
    }

    pub fn with_redirect(&self) -> anyhow::Result<Self> {
        match self {
            Self::Avm(_) => Ok(self.clone()),
            Self::Tvm(shard_account) => Ok(Self::Tvm(
                tvm_block::ShardAccount::with_redirect(
                    shard_account.last_trans_hash().clone(),
                    shard_account.last_trans_lt(),
                    shard_account.get_dapp_id().cloned(),
                )
                .map_err(|err| anyhow::anyhow!("{err}"))?,
            )),
        }
    }

    pub fn is_unloaded(&self) -> bool {
        match self {
            Self::Avm(_) => false,
            Self::Tvm(shard_account) => shard_account.is_external(),
        }
    }

    pub fn unload_account(&mut self) -> anyhow::Result<ThreadAccount> {
        match self {
            Self::Avm(_) => anyhow::bail!("Can't unload AVM state account"),
            Self::Tvm(shard_account) => {
                let state = shard_account
                    .replace_with_external()
                    .map_err(|err| anyhow::anyhow!("{err}"))?;
                Ok(ThreadAccount::Tvm(state))
            }
        }
    }

    pub fn account(&self) -> anyhow::Result<ThreadAccount> {
        match self {
            Self::Avm(avm_account) => Ok(ThreadAccount::Avm(avm_account.account.clone())),
            Self::Tvm(shard_state) => Ok(ThreadAccount::Tvm(
                shard_state.account_cell().map_err(|err| anyhow::anyhow!("{err}"))?,
            )),
        }
    }

    pub fn set_account(&mut self, account: ThreadAccount) -> anyhow::Result<()> {
        match (self, account) {
            (Self::Avm(state_account), ThreadAccount::Avm(account)) => {
                state_account.account = account;
                Ok(())
            }
            (Self::Tvm(shard_account), ThreadAccount::Tvm(cell)) => {
                shard_account.set_account_cell(cell).map_err(|err| anyhow::anyhow!("{err}"))
            }
            _ => anyhow::bail!("Can't set account: incompatible VM types"),
        }
    }
}

impl From<tvm_block::ShardAccount> for ThreadStateAccount {
    fn from(value: tvm_block::ShardAccount) -> Self {
        Self::Tvm(value)
    }
}

impl TryFrom<ThreadStateAccount> for tvm_block::ShardAccount {
    type Error = anyhow::Error;

    fn try_from(value: ThreadStateAccount) -> Result<Self, Self::Error> {
        match value {
            ThreadStateAccount::Tvm(shard_account) => Ok(shard_account),
            _ => anyhow::bail!("Can't convert non TVM state account to TVM shard account"),
        }
    }
}

impl TryFrom<&ThreadStateAccount> for tvm_block::ShardAccount {
    type Error = anyhow::Error;

    fn try_from(value: &ThreadStateAccount) -> Result<Self, Self::Error> {
        match value {
            ThreadStateAccount::Tvm(shard_account) => Ok(shard_account.clone()),
            _ => anyhow::bail!("Can't convert non TVM state account to TVM shard account"),
        }
    }
}

impl_serde_bytes!(ThreadStateAccount);

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
// VM agnostic account repr.
#[derive(Debug)]
pub enum ThreadAccount {
    Tvm(tvm_types::Cell),
    Avm(AvmAccount),
}

impl Default for ThreadAccount {
    fn default() -> Self {
        Self::Tvm(tvm_types::Cell::default())
    }
}

impl ThreadAccount {
    pub fn read_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(if bytes.starts_with(&AVM_TAG) {
            Self::Avm(
                bincode::deserialize(&bytes[AVM_TAG.len()..])
                    .map_err(|err| anyhow::anyhow!("{err}"))?,
            )
        } else {
            Self::Tvm(read_single_root_boc(bytes).map_err(|err| anyhow::anyhow!("{err}"))?)
        })
    }

    pub fn read_base64(b64: &str) -> anyhow::Result<Self> {
        let bytes = base64::engine::general_purpose::STANDARD.decode(b64)?;
        Self::read_bytes(&bytes)
    }

    pub fn write_bytes(&self) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::Tvm(cell) => write_boc(cell).map_err(|err| anyhow::anyhow!("{err}")),
            Self::Avm(account) => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&AVM_TAG);
                bincode::serialize_into(&mut buf, account)
                    .map_err(|err| anyhow::anyhow!("{err}"))?;
                Ok(buf)
            }
        }
    }
}

impl_serde_bytes!(ThreadAccount);

impl From<tvm_types::Cell> for ThreadAccount {
    fn from(value: tvm_types::Cell) -> Self {
        Self::Tvm(value)
    }
}

impl From<&tvm_types::Cell> for ThreadAccount {
    fn from(value: &tvm_types::Cell) -> Self {
        Self::Tvm(value.clone())
    }
}

impl TryFrom<tvm_block::Account> for ThreadAccount {
    type Error = anyhow::Error;

    fn try_from(value: tvm_block::Account) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl TryFrom<&tvm_block::Account> for ThreadAccount {
    type Error = anyhow::Error;

    fn try_from(value: &tvm_block::Account) -> Result<Self, Self::Error> {
        let cell = value.serialize().map_err(|err| anyhow::anyhow!("{}", err))?;
        Ok(Self::Tvm(cell))
    }
}

impl TryFrom<&ThreadAccount> for tvm_block::Account {
    type Error = anyhow::Error;

    fn try_from(value: &ThreadAccount) -> Result<Self, Self::Error> {
        Self::try_from(value.clone())
    }
}

fn account_from_cell(cell: tvm_types::Cell) -> anyhow::Result<tvm_block::Account> {
    tvm_block::Account::construct_from_cell(cell).map_err(|err| anyhow::anyhow!("{err}"))
}

impl TryFrom<ThreadAccount> for tvm_block::Account {
    type Error = anyhow::Error;

    fn try_from(value: ThreadAccount) -> Result<Self, Self::Error> {
        match value {
            ThreadAccount::Tvm(cell) => account_from_cell(cell),
            _ => anyhow::bail!("Can't convert non TVM state account to TVM account"),
        }
    }
}

impl TryFrom<&mut ThreadAccount> for tvm_block::Account {
    type Error = anyhow::Error;

    fn try_from(value: &mut ThreadAccount) -> Result<Self, Self::Error> {
        match value {
            ThreadAccount::Tvm(cell) => account_from_cell(cell.clone()),
            _ => anyhow::bail!("Can't convert non TVM state account to TVM account"),
        }
    }
}

impl TryFrom<&ThreadAccount> for tvm_types::Cell {
    type Error = anyhow::Error;

    fn try_from(value: &ThreadAccount) -> Result<Self, Self::Error> {
        match value {
            ThreadAccount::Tvm(cell) => Ok(cell.clone()),
            _ => anyhow::bail!("Can't convert non TVM state account to TVM cell"),
        }
    }
}

impl TryFrom<&mut ThreadAccount> for tvm_types::Cell {
    type Error = anyhow::Error;

    fn try_from(value: &mut ThreadAccount) -> Result<Self, Self::Error> {
        match value {
            ThreadAccount::Tvm(cell) => Ok(cell.clone()),
            _ => anyhow::bail!("Can't convert non TVM state account to TVM cell"),
        }
    }
}

impl TryFrom<ThreadAccount> for tvm_types::Cell {
    type Error = anyhow::Error;

    fn try_from(value: ThreadAccount) -> Result<Self, Self::Error> {
        match value {
            ThreadAccount::Tvm(cell) => Ok(cell),
            _ => anyhow::bail!("Can't convert non TVM state account to TVM cell"),
        }
    }
}

impl ThreadAccount {
    pub fn id(&self) -> Option<AccountIdentifier> {
        match self {
            Self::Tvm(cell) => account_from_cell(cell.clone())
                .map(|x| x.get_id().map(AccountIdentifier::from))
                .unwrap_or_default(),
            Self::Avm(account) => Some(account.0.metadata.id),
        }
    }

    pub fn hash(&self) -> AccountHash {
        match self {
            Self::Tvm(cell) => cell.repr_hash().into(),
            Self::Avm(account) => account.0.hash,
        }
    }

    pub fn data_hash(&self) -> anyhow::Result<Option<AccountDataHash>> {
        match self {
            Self::Tvm(cell) => {
                Ok(account_from_cell(cell.clone())?.get_data().map(|x| x.repr_hash().into()))
            }
            Self::Avm(account) => Ok(account.0.data_hash),
        }
    }

    pub fn code_hash(&self) -> anyhow::Result<Option<AccountCodeHash>> {
        match self {
            Self::Tvm(cell) => {
                Ok(account_from_cell(cell.clone())?.get_code().map(|x| x.repr_hash().into()))
            }
            Self::Avm(account) => Ok(account.0.code_hash),
        }
    }

    pub fn is_none(&self) -> bool {
        tvm_block::Account::try_from(self).is_ok_and(|t| t.is_none())
    }
}
