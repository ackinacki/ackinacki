// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use serde::de::Error as DeserError;
use serde::ser::Error as SerError;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tvm_block::Deserializable;
use tvm_block::Serializable;
use tvm_types::read_single_root_boc;
use tvm_types::write_boc;

use crate::types::ackinacki_block::common_section::CommonSection;
use crate::types::AckiNackiBlock;

impl AckiNackiBlock {
    pub fn get_raw_data_without_hash(&self) -> anyhow::Result<Vec<u8>> {
        tracing::trace!("full serialize block data");
        let common_section = bincode::serialize(&self.common_section)?;
        let mut data = vec![];
        data.extend_from_slice(&common_section.len().to_be_bytes()); // 8 bytes of common section len
        data.extend_from_slice(&common_section);

        let block_cell = self
            .block
            .serialize()
            .map_err(|e| anyhow::format_err!("Failed to serialize tvm block: {e}"))?;
        let block_data = write_boc(&block_cell)
            .map_err(|e| anyhow::format_err!("Failed to serialize tvm block cell: {e}"))?;
        data.extend_from_slice(&block_data.len().to_be_bytes()); // 8 bytes of block data len
        data.extend_from_slice(&block_data);
        data.extend_from_slice(&self.tx_cnt.to_be_bytes()); // 8 bytes of tx_cnt
        Ok(data)
    }
}

impl Serialize for AckiNackiBlock {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if let Some(data) = &self.raw_data {
            data.serialize(serializer)
        } else {
            let mut data = self
                .get_raw_data_without_hash()
                .map_err(|_| S::Error::custom("Failed to get block raw data"))?;
            data.extend_from_slice(&self.hash);
            data.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for AckiNackiBlock {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw_data = Vec::<u8>::deserialize(deserializer)?;
        let (common_section_len_data, rest) = raw_data.split_at(8);
        let common_section_len = usize::from_be_bytes(
            common_section_len_data
                .try_into()
                .map_err(|_| D::Error::custom("Failed to deserialize common section len"))?,
        );
        let (common_section_data, rest) = rest.split_at(common_section_len);
        #[cfg(feature = "transitioning_node_version")]
        let common_section: CommonSection =
            Transitioning::deserialize_data_compat(common_section_data)
                .map_err(|_| D::Error::custom("Failed to deserialize common section"))?;

        #[cfg(not(feature = "transitioning_node_version"))]
        let common_section: CommonSection = bincode::deserialize(common_section_data)
            .map_err(|_| D::Error::custom("Failed to deserialize common section"))?;

        let (block_len_data, rest) = rest.split_at(8);
        let block_len = usize::from_be_bytes(
            block_len_data
                .try_into()
                .map_err(|_| D::Error::custom("Failed to decode block len"))?,
        );
        let (block_data, rest) = rest.split_at(block_len);
        let block_cell = read_single_root_boc(block_data)
            .map_err(|_| D::Error::custom("Failed to deserialize tvm block cell"))?;
        let block = tvm_block::Block::construct_from_cell(block_cell.clone())
            .map_err(|_| D::Error::custom("Failed to deserialize tvm block"))?;

        let (tx_cnt_data, rest) = rest.split_at(8);
        let tx_cnt = usize::from_be_bytes(
            tx_cnt_data
                .try_into()
                .map_err(|_| D::Error::custom("Failed to decode block field: tx_cnt"))?,
        );
        assert_eq!(rest.len(), 32);
        let hash =
            rest.try_into().map_err(|_| D::Error::custom("Failed to deserialize block hash"))?;
        Ok(Self {
            common_section,
            block,
            tx_cnt,
            hash,
            raw_data: Some(raw_data),
            block_cell: Some(block_cell),
        })
    }
}
