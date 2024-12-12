// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::Bytes;

use crate::types::BlockIdentifier;

// TODO: refactor according to the note
// Note: This is wrong to have u16 as an underlying type for thread identifier.
// It must be possible to uniquely generate new thread id from any thread without
// collisions. Proposed identifier: a block id and a u16. It will be generated
// by the block producer by taking prev (or current) block id after which the thread
// must be spawned and adding some local index in case of multiple threads have to
// be spawned simultaneously.

#[serde_as]
#[derive(Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, PartialOrd, Ord)]
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

    pub fn base_id(&self) -> u16 {
        (self.0[0] as u16 >> 8) + self.0[1] as u16
    }
}

impl Display for ThreadIdentifier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "<T:{}>", hex::encode(self.0))
    }
}
impl Debug for ThreadIdentifier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ThreadIdentifier({})<{}>", self.base_id(), hex::encode(self.0))
    }
}

impl AsRef<[u8]> for ThreadIdentifier {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
