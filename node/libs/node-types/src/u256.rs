#[macro_export]
macro_rules! u256 {
    // Bytes model
    ($ty:ident, $what:expr,ser = bytes) => {
        $crate::__u256_struct_bytes!($ty);
        $crate::__u256_impls!($ty, $what);
    };

    // Fixed array model
    ($ty:ident, $what:expr,ser = array) => {
        $crate::__u256_struct_array!($ty);
        $crate::__u256_impls!($ty, $what);
    };
}

// Generates: derives + serde_with Bytes wrapper
#[macro_export]
macro_rules! __u256_struct_bytes {
    ($ty:ident) => {
        #[serde_with::serde_as]
        #[derive(
            Copy, Clone, Hash, PartialEq, Eq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
        )]
        pub struct $ty(#[serde_as(as = "serde_with::Bytes")] [u8; 32]);
    };
}

// Generates: derives + plain array (Serde default is [u8; 32] as 32 elements)
#[macro_export]
macro_rules! __u256_struct_array {
    ($ty:ident) => {
        #[derive(
            Copy, Clone, Hash, PartialEq, Eq, Ord, PartialOrd, serde::Serialize, serde::Deserialize,
        )]
        pub struct $ty([u8; 32]);
    };
}

#[macro_export]
macro_rules! __u256_impls {
    ($ty:ident, $what:expr) => {
        impl $ty {
            pub const ZERO: Self = Self([0u8; 32]);

            #[inline(always)]
            pub const fn new(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            #[inline(always)]
            pub fn as_array(&self) -> &[u8; 32] {
                &self.0
            }

            #[inline(always)]
            pub fn as_slice(&self) -> &[u8] {
                &self.0
            }

            #[allow(clippy::wrong_self_convention)]
            pub fn to_hex_string(&self) -> String {
                hex::encode(self.0)
            }

            pub fn get_bit(&self, index: usize) -> bool {
                if index >= 256 {
                    panic!("index out of bounds")
                }
                let hi = index / 8;
                let lo = index % 8;
                ((self.0[hi] >> (7 - lo)) & 1) != 0
            }

            pub fn set_bit(&mut self, index: usize) {
                if index >= 256 {
                    panic!("index out of bounds");
                }
                let hi = index / 8usize;
                let lo = 7 - index % 8usize;
                self.0[hi] |= 1 << lo;
            }
        }

        impl Default for $ty {
            fn default() -> Self {
                Self::ZERO
            }
        }

        impl tvm_block::Serializable for $ty {
            fn write_to(&self, cell: &mut tvm_types::BuilderData) -> tvm_types::Result<()> {
                tvm_types::UInt256::from(self).write_to(cell)
            }
        }

        impl std::ops::BitAnd for &'_ $ty {
            type Output = $ty;

            fn bitand(self, rhs: Self) -> Self::Output {
                let lhs_buffer = &self.0;
                let rhs_buffer = &rhs.0;
                let mut result_buffer: [u8; 32] = [0; 32];
                for i in 0..result_buffer.len() {
                    result_buffer[i] = lhs_buffer[i] & rhs_buffer[i];
                }
                $ty(result_buffer)
            }
        }

        impl From<tvm_types::UInt256> for $ty {
            fn from(value: tvm_types::UInt256) -> Self {
                Self(*value.as_array())
            }
        }

        impl From<$ty> for tvm_types::UInt256 {
            fn from(value: $ty) -> Self {
                Self::with_array(value.0)
            }
        }

        impl From<&tvm_types::UInt256> for $ty {
            fn from(value: &tvm_types::UInt256) -> Self {
                Self(*value.as_array())
            }
        }

        impl From<&$ty> for tvm_types::UInt256 {
            fn from(value: &$ty) -> Self {
                Self::with_array(value.0)
            }
        }

        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(&hex::encode(self.0))
            }
        }

        impl std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                if f.alternate() {
                    write!(f, "0x{}", hex::encode(self.0))
                } else {
                    write!(f, "{}", hex::encode(self.0))
                }
            }
        }

        impl std::str::FromStr for $ty {
            type Err = anyhow::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let raw = $crate::u256::u8_32_from_str(s, $what)?;
                Ok(Self(raw))
            }
        }

        impl std::fmt::LowerHex for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                if f.alternate() {
                    write!(f, "0x{}", hex::encode(self.0))
                } else {
                    write!(f, "{}", hex::encode(self.0))
                }
            }
        }

        impl $crate::Blake3Hashable for $ty {
            fn update_hasher(&self, hasher: &mut blake3::Hasher) {
                hasher.update(&self.0);
            }

            fn hash(&self, prefix_tag: u8) -> [u8; 32] {
                let mut buf = [0u8; 33];
                buf[0] = prefix_tag;
                buf[1..33].copy_from_slice(&self.0);
                *blake3::hash(&buf).as_bytes()
            }
        }
    };
}

pub fn u8_32_from_str(s: &str, name: &str) -> anyhow::Result<[u8; 32]> {
    let mut result = [0u8; 32];
    if s.len() == 64 {
        hex::decode_to_slice(s, &mut result)
            .map_err(|e| anyhow::anyhow!("Invalid {name} [{s}]: {e}"))?
    } else if s.len() == 66 && (s.starts_with("0x") || s.starts_with("0X") || s.starts_with("0:")) {
        hex::decode_to_slice(&s[2..], &mut result)
            .map_err(|e| anyhow::anyhow!("Invalid {name} [{s}]: {e}"))?
    } else {
        anyhow::bail!("Invalid {name} [{s}]: 64 chars expected");
    }
    Ok(result)
}
