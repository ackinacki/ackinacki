// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use serde::Deserialize;
use serde::Serialize;

// Note: U32 is the same as the tvm block seq no
#[derive(Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, Default, PartialOrd, Ord)]
pub struct BlockSeqNo(u32);

pub fn next_seq_no(seq_no: BlockSeqNo) -> BlockSeqNo {
    BlockSeqNo(seq_no.0 + 1)
}

impl BlockSeqNo {
    pub fn saturating_sub(self, other: u32) -> Self {
        Self(self.0.saturating_sub(other))
    }
}

impl std::convert::From<u32> for BlockSeqNo {
    fn from(seq_no: u32) -> Self {
        Self(seq_no)
    }
}

impl std::convert::From<BlockSeqNo> for u32 {
    fn from(val: BlockSeqNo) -> Self {
        val.0
    }
}

impl std::fmt::Display for BlockSeqNo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Debug for BlockSeqNo {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::ops::Sub for BlockSeqNo {
    type Output = i64;

    fn sub(self, other: Self) -> Self::Output {
        i64::from(self.0) - i64::from(other.0)
    }
}

impl std::ops::Sub<&BlockSeqNo> for BlockSeqNo {
    type Output = i64;

    fn sub(self, other: &Self) -> Self::Output {
        i64::from(self.0) - i64::from(other.0)
    }
}

// Note:
// We don't want to implement std::ops::Sub for BlockSeqNo and u32
// since it would lead to unpredictable behaviour when it should be negative numbers.
// Overall it's easier to operate in a predictable space where you can sub block seq
// numbers and add differences if needed.
impl std::ops::Add<u32> for BlockSeqNo {
    type Output = Self;

    fn add(self, other: u32) -> Self::Output {
        Self(self.0 + other)
    }
}

impl std::ops::Rem<u32> for BlockSeqNo {
    type Output = u32;

    fn rem(self, rhs: u32) -> Self::Output {
        self.0 % rhs
    }
}
