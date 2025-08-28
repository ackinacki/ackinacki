// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Formatter;
use std::str::FromStr;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tvm_types::UInt256;

pub mod direct_bit_access_operations;

#[derive(Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct AccountAddress(pub UInt256);

impl AccountAddress {
    pub fn to_hex_string(&self) -> String {
        self.0.to_hex_string()
    }
}

impl std::fmt::Display for AccountAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex_string())
    }
}

impl From<AccountAddress> for tvm_types::AccountId {
    fn from(value: AccountAddress) -> Self {
        Self::from(value.0)
    }
}

impl From<&AccountAddress> for tvm_types::AccountId {
    fn from(value: &AccountAddress) -> Self {
        Self::from(value.0.clone())
    }
}

impl From<tvm_types::AccountId> for AccountAddress {
    fn from(id: tvm_types::AccountId) -> Self {
        Self(id.get_bytestring(0).into())
    }
}

impl From<&tvm_types::AccountId> for AccountAddress {
    fn from(id: &tvm_types::AccountId) -> Self {
        Self(id.get_bytestring(0).into())
    }
}

impl FromStr for AccountAddress {
    type Err = tvm_types::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(UInt256::from_str(s)?))
    }
}

impl Default for AccountAddress {
    fn default() -> Self {
        AccountAddress(UInt256::default())
    }
}

impl Debug for AccountAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.to_hex_string())
    }
}

impl Serialize for AccountAddress {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let data: [u8; 32] = *self.0.as_array();
        data.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AccountAddress {
    fn deserialize<D>(deserializer: D) -> Result<AccountAddress, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Self(UInt256::from(<[u8; 32]>::deserialize(deserializer)?)))
    }
}

// Note:
// This struct can be optimized for better performance.
impl std::ops::BitAnd for &'_ AccountAddress {
    type Output = AccountAddress;

    fn bitand(self, rhs: Self) -> Self::Output {
        let lhs_buffer = self.0.as_array();
        let rhs_buffer = rhs.0.as_array();
        let mut result_buffer: [u8; 32] = [0; 32];
        for i in 0..result_buffer.len() {
            result_buffer[i] = lhs_buffer[i] & rhs_buffer[i];
        }
        AccountAddress(result_buffer.into())
    }
}
