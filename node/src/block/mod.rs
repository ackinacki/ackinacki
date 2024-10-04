// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub mod keeper;
pub mod producer;

pub mod block_stub;
pub mod common_section;

use std::cmp::Ordering;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::LowerHex;
use std::hash::Hash;
use std::str::FromStr;

#[cfg(test)]
pub use block_stub::MockBlockStruct;
pub use common_section::CommonSection;
pub use common_section::Directives;
use serde::ser::Serializer;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde_with::serde_as;
use serde_with::Bytes;
use sha2::Digest;
use sha2::Sha256;
use tvm_block::Deserializable;
use tvm_block::GetRepresentationHash;
use tvm_block::Serializable;
use tvm_types::base64_decode_to_slice;
use tvm_types::read_single_root_boc;
use tvm_types::write_boc;
use tvm_types::Cell;
use tvm_types::UInt256;

use crate::block_keeper_system::BlockKeeperSetChange;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::transaction::Transaction;

const BLOCK_SUFFIX_LEN: usize = 32;

pub trait Block: Clone + Send + Sync + 'static {
    type BLSSignatureScheme: BLSSignatureScheme;
    type BlockIdentifier: BlockIdentifier + Serialize + for<'b> Deserialize<'b> + Clone;
    type BlockSeqNo: BlockSeqNo;
    type Transaction: Transaction;

    fn parent(&self) -> Self::BlockIdentifier;
    fn identifier(&self) -> Self::BlockIdentifier;
    fn seq_no(&self) -> Self::BlockSeqNo;

    fn is_child_of(&self, other_block: &Self) -> bool;

    fn directives(&self) -> Directives;
    fn set_directives(&mut self, directives: Directives) -> anyhow::Result<()>;

    fn check_hash(&self) -> anyhow::Result<bool>;

    fn get_hash(&self) -> Sha256Hash;

    fn get_common_section(
        &self,
    ) -> CommonSection<Self::BLSSignatureScheme, Self::BlockIdentifier, Self::BlockSeqNo>;
    fn set_common_section(
        &mut self,
        common_section: CommonSection<
            Self::BLSSignatureScheme,
            Self::BlockIdentifier,
            Self::BlockSeqNo,
        >,
    ) -> anyhow::Result<()>;
}

#[derive(Clone, Debug)]
pub struct WrappedBlock {
    common_section: CommonSection<GoshBLS, WrappedUInt256, u32>,
    pub block: tvm_block::Block,
    pub processed_ext_messages_cnt: usize,
    pub tx_cnt: usize,
    hash: Sha256Hash,
    pub raw_data: Option<Vec<u8>>,
    pub block_cell: Option<Cell>,
}

impl Display for WrappedBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seq_no: {:?}, id: {:?}, tx_cnt: {}, ext_messages_cnt: {}, hash: {}, common_section: {:?}",
            self.seq_no(),
            self.identifier(),
            self.tx_cnt,
            self.processed_ext_messages_cnt,
            debug_hash(&self.hash),
            self.common_section,
        )
    }
}

fn calculate_hash(data: &[u8]) -> anyhow::Result<Sha256Hash> {
    let start = std::time::Instant::now();
    let mut hasher = Sha256::new();
    hasher.update(data);
    let res = hasher.finalize().into();
    tracing::trace!("Calculating block hash time: {}", start.elapsed().as_millis());
    Ok(res)
}

impl WrappedBlock {
    pub fn new(
        block: tvm_block::Block,
        processed_ext_messages_cnt: usize,
        producer_id: NodeIdentifier,
        tx_cnt: usize,
        block_keeper_set_changes: Vec<BlockKeeperSetChange>,
        verify_complexity: SignerIndex,
    ) -> Self {
        // TODO: according to the node logic we will update common section of every
        // block so there is no need to calculate hash here
        // res.hash = res.calculate_hash().expect("Failed to calculate wrapped block
        // hash");
        Self {
            common_section: CommonSection::new(
                producer_id,
                block_keeper_set_changes,
                verify_complexity,
            ),
            block,
            tx_cnt,
            processed_ext_messages_cnt,
            hash: [0; 32],
            raw_data: None,
            block_cell: None,
        }
    }

    pub fn raw_block_data(&self) -> anyhow::Result<(Vec<u8>, Cell)> {
        if let Some(raw_data) = &self.raw_data {
            let (common_section_len_data, rest) = raw_data.split_at(8);
            let common_section_len =
                usize::from_be_bytes(common_section_len_data.try_into().unwrap());
            let (_common_section_data, rest) = rest.split_at(common_section_len);

            let (block_len_data, rest) = rest.split_at(8);
            let block_len = usize::from_be_bytes(block_len_data.try_into().unwrap());
            let (block_data, _rest) = rest.split_at(block_len);
            if self.block_cell.is_some() {
                Ok((block_data.to_vec(), self.block_cell.clone().unwrap()))
            } else {
                let block_cell = self.block.serialize().map_err(|e| anyhow::format_err!("{e}"))?;
                Ok((block_data.to_vec(), block_cell))
            }
        } else if self.block_cell.is_some() {
            let block_cell = self.block_cell.clone().unwrap();
            let data = write_boc(&block_cell).map_err(|e| anyhow::format_err!("{e}"))?;
            Ok((data, block_cell))
        } else {
            let block_cell = self.block.serialize().map_err(|e| anyhow::format_err!("{e}"))?;
            let data = write_boc(&block_cell).map_err(|e| anyhow::format_err!("{e}"))?;
            Ok((data, block_cell))
        }
    }

    fn wrap_serialize(
        &self,
        do_calculate_hash: bool,
    ) -> anyhow::Result<(Vec<u8>, Sha256Hash, Cell)> {
        tracing::trace!("full serialize block data");
        let common_section = bincode::serialize(&self.common_section)?;
        let mut data = vec![];
        data.extend_from_slice(&common_section.len().to_be_bytes()); // 8 bytes of common section len
        data.extend_from_slice(&common_section);

        let block_cell = self.block.serialize().map_err(|e| anyhow::format_err!("{e}"))?;
        let block_data = write_boc(&block_cell).map_err(|e| anyhow::format_err!("{e}"))?;
        data.extend_from_slice(&block_data.len().to_be_bytes()); // 8 bytes of block data len
        data.extend_from_slice(&block_data);
        data.extend_from_slice(&self.processed_ext_messages_cnt.to_be_bytes()); // 8 bytes of processed_ext_messages_cnt
        data.extend_from_slice(&self.tx_cnt.to_be_bytes()); // 8 bytes of tx_cnt

        let hash = if do_calculate_hash { calculate_hash(&data)? } else { Sha256Hash::default() };
        // 32 bytes of hash
        if do_calculate_hash {
            data.extend_from_slice(hash.as_slice());
        } else {
            data.extend_from_slice(self.hash.as_slice());
        }
        Ok((data, hash, block_cell))
    }

    fn wrap_deserialize(data: &[u8]) -> Self {
        let raw_data = data.to_vec();
        let (common_section_len_data, rest) = data.split_at(8);
        let common_section_len = usize::from_be_bytes(common_section_len_data.try_into().unwrap());
        let (common_section_data, rest) = rest.split_at(common_section_len);
        let common_section: CommonSection<GoshBLS, WrappedUInt256, u32> =
            bincode::deserialize(common_section_data).unwrap();

        let (block_len_data, rest) = rest.split_at(8);
        let block_len = usize::from_be_bytes(block_len_data.try_into().unwrap());
        let (block_data, rest) = rest.split_at(block_len);
        let block_cell = read_single_root_boc(block_data).unwrap();
        let block = tvm_block::Block::construct_from_cell(block_cell.clone()).unwrap();

        let (processed_ext_messages_cnt_data, rest) = rest.split_at(8);
        let processed_ext_messages_cnt =
            usize::from_be_bytes(processed_ext_messages_cnt_data.try_into().unwrap());

        let (tx_cnt_data, rest) = rest.split_at(8);
        let tx_cnt = usize::from_be_bytes(tx_cnt_data.try_into().unwrap());

        assert_eq!(rest.len(), 32);
        let hash = rest.try_into().unwrap();
        Self {
            common_section,
            block,
            tx_cnt,
            processed_ext_messages_cnt,
            hash,
            raw_data: Some(raw_data),
            block_cell: Some(block_cell),
        }
    }
}

impl Block for WrappedBlock {
    type BLSSignatureScheme = GoshBLS;
    type BlockIdentifier = WrappedUInt256;
    type BlockSeqNo = u32;
    type Transaction = tvm_block::Transaction;

    fn parent(&self) -> Self::BlockIdentifier {
        Self::BlockIdentifier::from(
            self.block
                .info
                .read_struct()
                .unwrap()
                .read_prev_ref()
                .unwrap()
                .prev1()
                .unwrap()
                .root_hash,
        )
    }

    fn identifier(&self) -> Self::BlockIdentifier {
        self.block.hash().unwrap().into()
    }

    fn seq_no(&self) -> Self::BlockSeqNo {
        self.block.info.read_struct().unwrap().seq_no()
    }

    fn is_child_of(&self, _other_block: &Self) -> bool {
        todo!()
    }

    fn directives(&self) -> Directives {
        self.common_section.directives.clone()
    }

    fn set_directives(&mut self, directives: Directives) -> anyhow::Result<()> {
        self.common_section.directives = directives;
        let (raw_data, hash, block_cell) = self.wrap_serialize(true)?;
        self.hash = hash;
        self.raw_data = Some(raw_data);
        self.block_cell = Some(block_cell);
        Ok(())
    }

    fn check_hash(&self) -> anyhow::Result<bool> {
        tracing::trace!("Check hash for block {:?} {:?}", self.seq_no(), self.identifier());
        let real_hash = if let Some(raw_data) = &self.raw_data {
            assert!(raw_data.len() > BLOCK_SUFFIX_LEN);
            tracing::trace!("Use raw data to check hash");
            // let (data_for_hash, _) = raw_data.split_last_chunk::<40>().unwrap();
            let (data_for_hash, _) = raw_data.split_at(raw_data.len() - BLOCK_SUFFIX_LEN);
            calculate_hash(data_for_hash)?
        } else {
            tracing::trace!("Serialize data to check hash");
            let (_, hash, _) = self.wrap_serialize(true)?;
            hash
        };
        tracing::trace!(
            "Calculated hash: {}, block hash: {}",
            debug_hash(&real_hash),
            debug_hash(&self.hash)
        );
        Ok(self.hash == real_hash)
    }

    fn get_hash(&self) -> Sha256Hash {
        self.hash
    }

    fn get_common_section(
        &self,
    ) -> CommonSection<Self::BLSSignatureScheme, Self::BlockIdentifier, Self::BlockSeqNo> {
        self.common_section.clone()
    }

    fn set_common_section(
        &mut self,
        common_section: CommonSection<
            Self::BLSSignatureScheme,
            Self::BlockIdentifier,
            Self::BlockSeqNo,
        >,
    ) -> anyhow::Result<()> {
        self.common_section = common_section;
        let (raw_data, hash, block_cell) = self.wrap_serialize(true)?;
        self.hash = hash;
        self.raw_data = Some(raw_data);
        self.block_cell = Some(block_cell);
        Ok(())
    }
}

impl Serialize for WrappedBlock {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // let start = std::time::Instant::now();

        // tracing::trace!("Serialize block time: {}", start.elapsed().as_millis());
        if let Some(data) = &self.raw_data {
            // tracing::trace!("Use raw data for block serialization");
            data.serialize(serializer)
        } else {
            let (byte_serialization, _, _) =
                self.wrap_serialize(false).expect("Failed to serialize block");
            byte_serialization.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for WrappedBlock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // let start = std::time::Instant::now();
        let raw_data = Vec::<u8>::deserialize(deserializer)?;
        let block = WrappedBlock::wrap_deserialize(&raw_data);
        // tracing::trace!("Deserialize block time: {}", start.elapsed().as_millis());
        Ok(block)
    }
}

type Sha256Hash = [u8; 32];

pub(crate) fn compare_hashes(lhs: &Sha256Hash, rhs: &Sha256Hash) -> Ordering {
    for i in 0..32 {
        match lhs[i].cmp(&rhs[i]) {
            Ordering::Less => {
                return Ordering::Less;
            }
            Ordering::Greater => {
                return Ordering::Greater;
            }
            _ => {}
        }
    }
    Ordering::Equal
}

fn debug_hash(hash: &Sha256Hash) -> String {
    let mut result = String::new();
    for byte in hash {
        result = format!("{}{:02X}", result, byte);
    }
    result
}

pub trait BlockSeqNo:
    PartialOrd
    + Ord
    + Copy
    + Into<u64>
    + Debug
    + Send
    + Sync
    + Default
    + Serialize
    + for<'b> Deserialize<'b>
    + 'static
{
    fn next(&self) -> Self;
    fn prev(&self) -> Self;
}

impl BlockSeqNo for u32 {
    fn next(&self) -> Self {
        *self + 1
    }

    fn prev(&self) -> Self {
        *self - 1
    }
}

pub trait BlockIdentifier:
    Eq + Clone + Debug + ToString + FromStr + Hash + Send + Sync + AsRef<[u8]> + Default + 'static
{
    fn is_zero(&self) -> bool;

    fn modulus(&self, divider: u32) -> u32;
}

#[serde_as]
#[derive(Deserialize, Serialize, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Default)]
pub struct WrappedUInt256(#[serde_as(as = "Bytes")] [u8; 32]);

impl AsRef<[u8]> for WrappedUInt256 {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<UInt256> for WrappedUInt256 {
    fn from(value: UInt256) -> Self {
        Self(value.inner())
    }
}

impl From<WrappedUInt256> for UInt256 {
    fn from(val: WrappedUInt256) -> Self {
        UInt256::from(val.0)
    }
}

impl FromStr for WrappedUInt256 {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut result = [0u8; 32];
        match value.len() {
            64 => hex::decode_to_slice(value, &mut result)?,
            66 => hex::decode_to_slice(&value[2..], &mut result)?,
            44 => base64_decode_to_slice(value, &mut result)
                .map_err(|e| anyhow::format_err!("{e}"))?,
            _ => anyhow::bail!(
                "invalid account ID string (32 bytes expected), but got string {}",
                value
            ),
        }
        Ok(WrappedUInt256(result))
    }
}

impl Debug for WrappedUInt256 {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        LowerHex::fmt(self, f)
    }
}

impl LowerHex for WrappedUInt256 {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if f.alternate() {
            write!(f, "0x{}", hex::encode(self.0))
        } else {
            write!(f, "{}", hex::encode(self.0))
            // write!(f, "{}...{}", hex::encode(&self.0[..2]),
            // hex::encode(&self.0[30..32]))
        }
    }
}

impl Display for WrappedUInt256 {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            // "UInt256[{:X?}]", &self.0  This format is better for debug. It was used in path
            // creation and this wrap looks bad
            "{}",
            hex::encode(self.0)
        )
    }
}

impl BlockIdentifier for WrappedUInt256 {
    fn is_zero(&self) -> bool {
        *self == WrappedUInt256::default()
    }

    fn modulus(&self, divider: u32) -> u32 {
        let mut bytes = [0_u8; 4];
        bytes.clone_from_slice(&self.0[28..=31]);
        u32::from_be_bytes(bytes) % divider
    }
}
