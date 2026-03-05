use std::fmt::Debug;
use std::fmt::Formatter;

use account_state::ThreadStateAccount;
use node_types::AccountIdentifier;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Clone, PartialEq)]
pub struct WrappedAccount {
    pub account_id: AccountIdentifier,
    pub account: account_state::ThreadStateAccount,
}

impl Debug for WrappedAccount {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.account_id.to_hex_string())
    }
}

#[derive(Serialize, Deserialize)]
struct WrappedAccountData {
    account_id: AccountIdentifier,
    data: Vec<u8>,
}

impl WrappedAccount {
    fn wrap_serialize(&self) -> WrappedAccountData {
        WrappedAccountData {
            account_id: self.account_id,
            data: self.account.write_bytes().unwrap(),
        }
    }

    fn wrap_deserialize(data: WrappedAccountData) -> Self {
        Self {
            account_id: data.account_id,
            account: ThreadStateAccount::read_bytes(&data.data).unwrap(),
        }
    }
}

impl Serialize for WrappedAccount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.wrap_serialize().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for WrappedAccount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = WrappedAccountData::deserialize(deserializer)?;
        let account = WrappedAccount::wrap_deserialize(data);
        Ok(account)
    }
}
