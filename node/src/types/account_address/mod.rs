// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tvm_types::AccountId;
use tvm_types::UInt256;

pub mod direct_bit_access_operations;

#[derive(Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct AccountAddress(pub tvm_types::AccountId);

impl From<tvm_types::AccountId> for AccountAddress {
    fn from(id: tvm_types::AccountId) -> Self {
        Self(id)
    }
}

impl Default for AccountAddress {
    fn default() -> Self {
        AccountAddress(AccountId::from(UInt256::default()))
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
        let data: [u8; 32] =
            self.0.get_bytestring(0).try_into().expect("Account address must be an uint256 value");
        data.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for AccountAddress {
    fn deserialize<D>(deserializer: D) -> Result<AccountAddress, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = <[u8; 32]>::deserialize(deserializer)?;
        Ok(Self(tvm_types::AccountId::from(data)))
    }
}

// Note:
// This struct can be optimized for better performance.
impl std::ops::BitAnd for &'_ AccountAddress {
    type Output = AccountAddress;

    fn bitand(self, rhs: Self) -> Self::Output {
        let lhs_buffer: [u8; 32] =
            self.0.get_bytestring(0).try_into().expect("Account address must be a uint256 value");
        let rhs_buffer: [u8; 32] =
            rhs.0.get_bytestring(0).try_into().expect("Account address must be a uint256 value");
        let mut result_buffer: [u8; 32] = [0; 32];
        for i in 0..result_buffer.len() {
            result_buffer[i] = lhs_buffer[i] & rhs_buffer[i];
        }
        AccountAddress(tvm_types::AccountId::from(result_buffer))
    }
}
