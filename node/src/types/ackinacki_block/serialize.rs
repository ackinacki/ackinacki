// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::time::Instant;

use account_state::DurableThreadAccountsStateDiff;
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

use crate::live_metrics::LiveAckiNackiBlockCounter;
use crate::types::common_section::CommonSection;
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

        let durable_diff_data = bincode::serialize(&self.durable_state_update)?;
        data.extend_from_slice(&durable_diff_data.len().to_be_bytes()); // 8 bytes of diff len
        data.extend_from_slice(&durable_diff_data);

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
                .map_err(|e| S::Error::custom(format!("Failed to get block raw data: {e}")))?;
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
        let total_started_at = Instant::now();
        let mut step_started_at = Instant::now();
        let raw_data = Vec::<u8>::deserialize(deserializer)?;
        tracing::trace!(
            target: "node_execution_detailed",
            "ackinacki_block_deserialize timing: step=raw_vec elapsed_ms={} raw_bytes={}",
            step_started_at.elapsed().as_millis(),
            raw_data.len(),
        );

        step_started_at = Instant::now();
        let (common_section_len_data, rest) = raw_data.split_at(8);
        let common_section_len = usize::from_be_bytes(
            common_section_len_data
                .try_into()
                .map_err(|_| D::Error::custom("Failed to deserialize common section len"))?,
        );
        let (common_section_data, rest) = rest.split_at(common_section_len);
        let common_section: CommonSection =
            versioned_struct::Transitioning::deserialize_data_compat(common_section_data)
                .map_err(|_| D::Error::custom("Failed to deserialize common section"))?
                .0;
        tracing::trace!(
            target: "node_execution_detailed",
            "ackinacki_block_deserialize timing: step=common_section elapsed_ms={} common_section_bytes={} raw_bytes={}",
            step_started_at.elapsed().as_millis(),
            common_section_len,
            raw_data.len(),
        );

        step_started_at = Instant::now();
        let (block_len_data, rest) = rest.split_at(8);
        let block_len = usize::from_be_bytes(
            block_len_data
                .try_into()
                .map_err(|_| D::Error::custom("Failed to decode block len"))?,
        );
        let (block_data, rest) = rest.split_at(block_len);
        let block_cell = read_single_root_boc(block_data)
            .map_err(|_| D::Error::custom("Failed to deserialize tvm block cell"))?;
        tracing::trace!(
            target: "node_execution_detailed",
            "ackinacki_block_deserialize timing: step=read_single_root_boc elapsed_ms={} block_bytes={} raw_bytes={}",
            step_started_at.elapsed().as_millis(),
            block_len,
            raw_data.len(),
        );

        step_started_at = Instant::now();
        let block = tvm_block::Block::construct_from_cell(block_cell.clone())
            .map_err(|_| D::Error::custom("Failed to deserialize tvm block"))?;
        tracing::trace!(
            target: "node_execution_detailed",
            "ackinacki_block_deserialize timing: step=construct_tvm_block elapsed_ms={} block_bytes={} raw_bytes={}",
            step_started_at.elapsed().as_millis(),
            block_len,
            raw_data.len(),
        );

        step_started_at = Instant::now();
        let (tx_cnt_data, rest) = rest.split_at(8);
        let tx_cnt = usize::from_be_bytes(
            tx_cnt_data
                .try_into()
                .map_err(|_| D::Error::custom("Failed to decode block field: tx_cnt"))?,
        );

        let (durable_diff, rest) = if rest.len() != 32 {
            let (durable_diff_len_data, rest) = rest.split_at(8);
            let durable_diff_len = usize::from_be_bytes(
                durable_diff_len_data
                    .try_into()
                    .map_err(|_| D::Error::custom("Failed to deserialize common section len"))?,
            );
            let (durable_diff_data, rest) = rest.split_at(durable_diff_len);
            let durable_diff: DurableThreadAccountsStateDiff =
                bincode::deserialize(durable_diff_data)
                    .map_err(|_| D::Error::custom("Failed to deserialize durable diff"))?;
            (durable_diff, rest)
        } else {
            (DurableThreadAccountsStateDiff::default(), rest)
        };
        tracing::trace!(
            target: "node_execution_detailed",
            "ackinacki_block_deserialize timing: step=durable_diff elapsed_ms={} durable_accounts={} raw_bytes={}",
            step_started_at.elapsed().as_millis(),
            durable_diff.accounts.len(),
            raw_data.len(),
        );

        step_started_at = Instant::now();
        assert_eq!(rest.len(), 32);
        let hash =
            rest.try_into().map_err(|_| D::Error::custom("Failed to deserialize block hash"))?;
        let raw_data_len = raw_data.len();
        let mut block = Self {
            common_section,
            block,
            tx_cnt,
            hash,
            raw_data: Some(raw_data),
            block_cell: Some(block_cell),
            durable_state_update: durable_diff,
            cached_block_id: None,
            _live_counter: LiveAckiNackiBlockCounter::new(),
        };
        block.cached_block_id = Some(node_types::BlockIdentifier::new(block.merkle_block_id()));
        tracing::trace!(
            target: "node_execution_detailed",
            "ackinacki_block_deserialize timing: step=merkle_block_id elapsed_ms={} seq_no={:?} block_id={:?} tx_cnt={} durable_accounts={} raw_bytes={}",
            step_started_at.elapsed().as_millis(),
            block.seq_no(),
            block.cached_block_id,
            tx_cnt,
            block.durable_state_update.accounts.len(),
            raw_data_len,
        );
        tracing::trace!(
            target: "node_execution_detailed",
            "ackinacki_block_deserialize timing: step=total elapsed_ms={} seq_no={:?} block_id={:?} tx_cnt={} durable_accounts={} raw_bytes={}",
            total_started_at.elapsed().as_millis(),
            block.seq_no(),
            block.cached_block_id,
            tx_cnt,
            block.durable_state_update.accounts.len(),
            raw_data_len,
        );

        Ok(block)
    }
}
