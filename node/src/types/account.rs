use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tvm_block::Deserializable;
use tvm_block::Serializable;

#[derive(Clone)]
pub struct WrappedAccount {
    pub account: tvm_block::ShardAccount,
}

impl Debug for WrappedAccount {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            self.account
                .read_account()
                .expect("Failed to read account")
                .get_id()
                .expect("Failed to get hash for account")
                .to_hex_string()
        )
    }
}

#[derive(Serialize, Deserialize)]
struct WrappedAccountData {
    data: Vec<u8>,
}

impl WrappedAccount {
    fn wrap_serialize(&self) -> WrappedAccountData {
        WrappedAccountData { data: self.account.write_to_bytes().unwrap() }
    }

    fn wrap_deserialize(data: WrappedAccountData) -> Self {
        Self { account: tvm_block::ShardAccount::construct_from_bytes(&data.data).unwrap() }
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
