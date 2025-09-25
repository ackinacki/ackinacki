// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::LowerHex;

use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::Bytes;

use crate::types::BlockIdentifier;

pub mod ord;

// Note:
// It must be possible to uniquely generate new thread id from any thread without
// collisions. Therefore the u16 as an underlying type for thread identifier was changed.
// The new undelrying type for the identifier is a block id and a u16. It will be generated
// by the block producer by taking the "current" block id after which the thread
// must be spawned and adding some local index in case of multiple threads have to
// be spawned simultaneously.

#[serde_as]
#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct ThreadIdentifier(#[serde_as(as = "Bytes")] [u8; 34]);

impl Default for ThreadIdentifier {
    fn default() -> Self {
        Self([0; 34])
    }
}

impl ThreadIdentifier {
    pub fn new(block_id: &BlockIdentifier, id: u16) -> Self {
        let mut res = [0; 34];
        res[0] = ((id >> 8) & 0xFF) as u8;
        res[1] = (id & 0xFF) as u8;
        res[2..34].copy_from_slice(block_id.as_ref());
        Self(res)
    }

    // Note: not the best solution to have it. Yet it is a simple quick to implement
    // solution. Seems harmless to have.
    /// Checks if a particular block was the one where the thread was spawned.
    pub fn is_spawning_block(&self, block_id: &BlockIdentifier) -> bool {
        (&self.0[2..34]) == block_id.as_ref()
    }

    pub fn spawning_block_id(&self) -> BlockIdentifier {
        BlockIdentifier::from(<[u8; 32]>::try_from(&self.0[2..34]).unwrap())
    }
}

impl From<[u8; 34]> for ThreadIdentifier {
    fn from(array: [u8; 34]) -> Self {
        Self(array)
    }
}

impl From<ThreadIdentifier> for [u8; 34] {
    fn from(val: ThreadIdentifier) -> Self {
        val.0
    }
}

impl TryFrom<std::string::String> for ThreadIdentifier {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match hex::decode(value) {
            Ok(array) => {
                let boxed_slice = array.into_boxed_slice();
                let boxed_array: Box<[u8; 34]> = match boxed_slice.try_into() {
                    Ok(array) => array,
                    Err(e) => anyhow::bail!("Expected a Vec of length 34 but it was {}", e.len()),
                };
                Ok(Self(*boxed_array))
            }
            Err(_) => anyhow::bail!("Failed to convert to ThreadIdentifier"),
        }
    }
}

impl LowerHex for ThreadIdentifier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", hex::encode(self.0))
    }
}

impl Display for ThreadIdentifier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "<T:{}>", hex::encode(self.0))
    }
}
impl Debug for ThreadIdentifier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ThreadIdentifier<{}>", hex::encode(self.0))
    }
}

impl AsRef<[u8]> for ThreadIdentifier {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

// Note:
// std::cmp::Ord notes:
// If you implement it manually, you should manually implement all four traits,
// based on the implementation of Ord
// And since there's a separate custom Ord implementation here is an implementation for the Eq
// The same applies for Hash.
impl PartialEq for ThreadIdentifier {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for ThreadIdentifier {}

impl std::hash::Hash for ThreadIdentifier {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}
