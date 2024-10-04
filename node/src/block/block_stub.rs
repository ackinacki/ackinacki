// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

#[cfg(test)]
use std::fmt;
#[cfg(test)]
use std::fmt::Display;
#[cfg(test)]
use std::fmt::Formatter;
#[cfg(test)]
use std::str::FromStr;

#[cfg(test)]
use mockall::mock;
#[cfg(test)]
use serde::Deserialize;
#[cfg(test)]
use serde::Deserializer;
#[cfg(test)]
use serde::Serialize;
#[cfg(test)]
use serde::Serializer;

#[cfg(test)]
use super::Block;
#[cfg(test)]
use crate::block::BlockIdentifier;
#[cfg(test)]
use crate::block::BlockSeqNo;
#[cfg(test)]
use crate::block::CommonSection;
#[cfg(test)]
use crate::block::Directives;
#[cfg(test)]
use crate::block::Sha256Hash;
#[cfg(test)]
use crate::bls::GoshBLS;
#[cfg(test)]
use crate::transaction::MockTransaction;

#[cfg(test)]
#[derive(Debug, Clone, Hash, PartialEq, Eq, Default)]
pub struct BlockIdentifierStub {
    data: u64,
}

#[cfg(test)]
impl Display for BlockIdentifierStub {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[cfg(test)]
impl FromStr for BlockIdentifierStub {
    type Err = anyhow::Error;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        todo!()
    }
}

#[cfg(test)]
impl AsRef<[u8]> for BlockIdentifierStub {
    fn as_ref(&self) -> &[u8] {
        todo!()
    }
}

#[cfg(test)]
impl BlockIdentifier for BlockIdentifierStub {
    fn is_zero(&self) -> bool {
        self.data == 0
    }

    fn modulus(&self, divider: u32) -> u32 {
        (self.data as u32) % divider
    }
}

#[cfg(test)]
impl Serialize for BlockIdentifierStub {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        todo!()
    }
}

#[cfg(test)]
impl<'de> Deserialize<'de> for BlockIdentifierStub {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        todo!()
    }
}

#[cfg(test)]
impl BlockSeqNo for u64 {
    fn next(&self) -> Self {
        *self + 1
    }

    fn prev(&self) -> Self {
        *self - 1
    }
}

#[cfg(test)]
#[derive(Deserialize, Serialize)]
struct MockBlockStructSerializationWorkaround {
    seq_no: u64,
    parent: u64,
    identifier: u64,
}

#[cfg(test)]
mock! {
    pub BlockStruct {
        fn workaround_serialize(&self) ->  MockBlockStructSerializationWorkaround;
        fn workaround_deserialize(data: MockBlockStructSerializationWorkaround) -> Self;
    }

    impl Block for BlockStruct {
        type Transaction = MockTransaction;
        type BlockSeqNo = u64;
        type BlockIdentifier = BlockIdentifierStub;
        type BLSSignatureScheme = GoshBLS;
        fn parent(&self) -> BlockIdentifierStub;
        fn identifier(&self) -> BlockIdentifierStub;
        fn seq_no(&self) -> u64;
        fn is_child_of(&self, other_block: &Self) -> bool;
        fn directives(&self) -> Directives;
        fn set_directives(&mut self, directives: Directives) -> anyhow::Result<()>;
        fn check_hash(&self) -> anyhow::Result<bool>;
        fn get_hash(&self) -> Sha256Hash;
        fn get_common_section(&self) -> CommonSection<GoshBLS, BlockIdentifierStub, u64>;
        fn set_common_section(&mut self, common_section: CommonSection<GoshBLS, BlockIdentifierStub, u64>) -> anyhow::Result<()>;
    }
    impl Clone for BlockStruct {
        fn clone(&self) -> Self;
    }
}

#[cfg(test)]
impl Display for MockBlockStruct {
    fn fmt(&self, _f: &mut Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

#[cfg(test)]
impl Serialize for MockBlockStruct {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.workaround_serialize().serialize(serializer)
    }
}

#[cfg(test)]
impl<'de> Deserialize<'de> for MockBlockStruct {
    fn deserialize<D>(deserializer: D) -> Result<MockBlockStruct, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = MockBlockStructSerializationWorkaround::deserialize(deserializer)?;
        let obj = MockBlockStruct::workaround_deserialize(data);
        Ok(obj)
    }
}

#[test]
fn test_mock_block() {
    let mut blocks = vec![];
    for i in 0_u64..10 {
        let mut block = MockBlockStruct::default();
        block.expect_identifier().return_const(BlockIdentifierStub { data: i });
        block.expect_seq_no().return_const(i);
        blocks.push(block);
    }
    for (i, block) in blocks.iter().enumerate() {
        assert_eq!(block.identifier(), BlockIdentifierStub { data: i as u64 });
        assert_eq!(block.seq_no(), i as u64);
    }
}
