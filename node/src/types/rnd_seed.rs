use std::ops::BitXor;

use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::Bytes;

use crate::types::BlockIdentifier;

#[serde_as]
#[derive(Deserialize, Serialize, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Default)]
pub struct RndSeed(#[serde_as(as = "Bytes")] [u8; 32]);

impl RndSeed {
    /// This is not a modulo operation.
    /// It is some kind of a quick and dirty way to put blocks into a defined
    /// number of buckets.
    pub fn not_a_modulus(&self, divider: u32) -> u32 {
        let mut bytes = [0_u8; 4];
        bytes.clone_from_slice(&self.0[28..=31]);
        u32::from_be_bytes(bytes) % divider
    }
}

impl std::fmt::Debug for RndSeed {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::LowerHex::fmt(self, f)
    }
}

impl std::fmt::LowerHex for RndSeed {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if f.alternate() {
            write!(f, "0x{}", hex::encode(self.0))
        } else {
            write!(f, "{}", hex::encode(self.0))
            // write!(f, "{}...{}", hex::encode(&self.0[..2]),
            // hex::encode(&self.0[30..32]))
        }
    }
}

impl From<[u8; 32]> for RndSeed {
    fn from(value: [u8; 32]) -> Self {
        RndSeed(value)
    }
}

impl AsRef<[u8]> for RndSeed {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl std::str::FromStr for RndSeed {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut result = [0u8; 32];
        match value.len() {
            64 => hex::decode_to_slice(value, &mut result)?,
            66 => hex::decode_to_slice(&value[2..], &mut result)?,
            44 => tvm_types::base64_decode_to_slice(value, &mut result).map_err(|e| {
                anyhow::format_err!("Failed to decode UInt256 from base64 str: {e}")
            })?,
            _ => anyhow::bail!(
                "invalid account ID string (32 bytes expected), but got string {value}"
            ),
        }
        Ok(Self(result))
    }
}

impl BitXor<BlockIdentifier> for RndSeed {
    type Output = Self;

    fn bitxor(self, rhs: BlockIdentifier) -> Self::Output {
        const N: usize = 32;
        let lhs: [u8; N] = self.0;
        let rhs: [u8; N] = rhs.0;
        let mut result: [u8; N] = [0u8; N];
        for i in 0..N {
            result[i] = lhs[i] ^ rhs[i];
        }
        Self(result)
    }
}
