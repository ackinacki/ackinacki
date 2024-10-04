// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::Arc;

use tvm_block::BlkPrevInfo;
use tvm_block::Deserializable;
use tvm_block::Serializable;
use tvm_block::ShardStateUnsplit;
use tvm_types::read_single_root_boc;
use tvm_types::write_boc;
use tvm_types::Cell;

use super::repository_impl::RepositoryImpl;
use crate::block::keeper::process::prepare_prev_block_info;
use crate::block::Block;
use crate::block::WrappedBlock;
use crate::message::Message;
use crate::message::WrappedMessage;

pub trait OptimisticState: Send + Clone {
    type BlockIdentifier;
    type BlockInfo;
    type Cell;
    type Message: Message;
    type ShardState;
    type Block;

    fn get_remaining_ext_messages(
        &self,
        repository: &RepositoryImpl,
    ) -> anyhow::Result<Vec<Self::Message>>;

    fn get_block_id(&self) -> &Self::BlockIdentifier;
    fn get_shard_state(&mut self) -> Self::ShardState;
    fn get_shard_state_as_cell(&mut self) -> Self::Cell;
    fn get_block_info(&self) -> Self::BlockInfo;
    fn serialize(&self) -> anyhow::Result<Vec<u8>>;
    fn apply_block<T: Into<Self::Block>>(&mut self, block_candidate: T) -> anyhow::Result<()>;
}

#[derive(Clone)]
pub struct OptimisticStateImpl {
    pub(crate) block_id: <Self as OptimisticState>::BlockIdentifier,
    pub(crate) shard_state: Option<Arc<ShardStateUnsplit>>,
    pub(crate) shard_state_cell: Option<Cell>,
    pub(crate) last_processed_external_message_index: u32,
    block_info: BlkPrevInfo,
}

impl OptimisticStateImpl {
    pub fn from_shard_state_and_block_info(
        block_id: <Self as OptimisticState>::BlockIdentifier,
        shard_state: <Self as OptimisticState>::ShardState,
        block_info: <Self as OptimisticState>::BlockInfo,
        last_processed_external_message_index: u32,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            shard_state: Some(shard_state),
            block_id,
            last_processed_external_message_index,
            block_info,
            shard_state_cell: None,
        })
    }

    pub fn from_shard_state_cell_and_block_info(
        block_id: <Self as OptimisticState>::BlockIdentifier,
        shard_state_cell: <Self as OptimisticState>::Cell,
        block_info: <Self as OptimisticState>::BlockInfo,
        last_processed_external_message_index: u32,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            shard_state: None,
            block_id,
            last_processed_external_message_index,
            block_info,
            shard_state_cell: Some(shard_state_cell),
        })
    }

    pub fn from_shard_state_shard_state_cell_and_block_info(
        block_id: <Self as OptimisticState>::BlockIdentifier,
        shard_state: <Self as OptimisticState>::ShardState,
        shard_state_cell: <Self as OptimisticState>::Cell,
        block_info: <Self as OptimisticState>::BlockInfo,
        last_processed_external_message_index: u32,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            shard_state: Some(shard_state),
            block_id,
            last_processed_external_message_index,
            block_info,
            shard_state_cell: Some(shard_state_cell),
        })
    }

    pub fn deserialize(
        data: &[u8],
        block_id: <Self as OptimisticState>::BlockIdentifier,
    ) -> anyhow::Result<Self> {
        // First 8 bytes encode block info data length
        let (first_chunk, data) = data.split_at(8);
        let data_chunk = <[u8; 8]>::try_from(first_chunk)?;
        let length = usize::from_be_bytes(data_chunk);
        let (info_bytes, data) = data.split_at(length);
        let block_info = BlkPrevInfo::construct_from_bytes(info_bytes)
            .map_err(|e| anyhow::format_err!("Failed to construct block info: {e}"))?;
        // First 8 bytes encode shard state data length
        let (first_chunk, data) = data.split_at(8);
        let data_chunk = <[u8; 8]>::try_from(first_chunk)?;
        let length = usize::from_be_bytes(data_chunk);
        let (shard_bytes, data) = data.split_at(length);
        // let shard_state = ShardStateUnsplit::construct_from_bytes(shard_bytes)
        //     .map_err(|e| anyhow::format_err!("Failed to construct shard state:
        // {e}"))?;

        let shard_state_cell = Some(
            read_single_root_boc(shard_bytes)
                .map_err(|e| anyhow::format_err!("Failed to construct shard state: {e}"))?,
        );
        let (first_chunk, _) = data.split_at(4);
        let last_processed_external_message_index =
            u32::from_be_bytes(<[u8; 4]>::try_from(first_chunk)?);
        Ok(Self {
            shard_state: None,
            block_id,
            last_processed_external_message_index,
            block_info,
            shard_state_cell,
        })
    }

    pub fn zero() -> Self {
        Self {
            block_id: <Self as OptimisticState>::BlockIdentifier::default(),
            shard_state: Some(Arc::new(ShardStateUnsplit::default())),
            last_processed_external_message_index: 0,
            block_info: BlkPrevInfo::default(),
            shard_state_cell: None,
        }
    }

    #[cfg(test)]
    pub fn stub(_id: <Self as OptimisticState>::BlockIdentifier) -> Self {
        Self::zero()
    }
}

impl OptimisticState for OptimisticStateImpl {
    type Block = WrappedBlock;
    type BlockIdentifier = <WrappedBlock as Block>::BlockIdentifier;
    type BlockInfo = BlkPrevInfo;
    type Cell = Cell;
    type Message = WrappedMessage;
    type ShardState = Arc<ShardStateUnsplit>;

    fn get_remaining_ext_messages(
        &self,
        repository: &RepositoryImpl,
    ) -> anyhow::Result<Vec<Self::Message>> {
        let ext_messages = repository.load_ext_messages_queue()?;
        if ext_messages.queue.is_empty() {
            return Ok(vec![]);
        }
        tracing::trace!(
            "last_index: {} ext_msgs: {:?}",
            self.last_processed_external_message_index,
            ext_messages.queue.len()
        );
        let mut res = vec![];
        for message in ext_messages.queue {
            if message.index > self.last_processed_external_message_index {
                res.push(message.message)
            }
        }
        tracing::trace!("get_remaining_ext_messages result: {}", res.len());
        Ok(res)
    }

    fn get_block_id(&self) -> &Self::BlockIdentifier {
        &self.block_id
    }

    fn get_shard_state(&mut self) -> Self::ShardState {
        if self.shard_state.is_some() {
            self.shard_state.clone().unwrap()
        } else {
            assert!(self.shard_state_cell.is_some());
            let cell = self.shard_state_cell.clone().unwrap();
            let shard_state = Arc::new(
                ShardStateUnsplit::construct_from_cell(cell)
                    .expect("Failed to deserialize shard state from cell"),
            );
            self.shard_state = Some(shard_state.clone());
            shard_state
        }
    }

    fn get_shard_state_as_cell(&mut self) -> Self::Cell {
        if self.shard_state_cell.is_some() {
            self.shard_state_cell.clone().unwrap()
        } else {
            assert!(self.shard_state.is_some());
            let shard_state = self.shard_state.clone().unwrap();
            let cell = shard_state.serialize().expect("Failed to serialize shard state");
            self.shard_state_cell = Some(cell.clone());
            cell
        }
    }

    fn get_block_info(&self) -> Self::BlockInfo {
        self.block_info.clone()
    }

    fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        let mut buffer = vec![];
        let info_bytes = self
            .block_info
            .write_to_bytes()
            .map_err(|e| anyhow::format_err!("Failed to serialize block info: {e}"))?;
        buffer.extend_from_slice(&info_bytes.len().to_be_bytes());
        buffer.extend_from_slice(&info_bytes);
        let shard_data = if let Some(cell) = &self.shard_state_cell {
            // cell.write_to_bytes()
            write_boc(cell)
        } else {
            assert!(self.shard_state.is_some());
            let shard_state = self.shard_state.as_ref().unwrap().as_ref();
            shard_state.write_to_bytes()
        }
        .map_err(|e| anyhow::format_err!("Failed to serialize shard state: {e}"))?;
        buffer.extend_from_slice(&shard_data.len().to_be_bytes());
        buffer.extend_from_slice(&shard_data);
        buffer.extend_from_slice(&self.last_processed_external_message_index.to_be_bytes());
        Ok(buffer)
    }

    fn apply_block<T: Into<Self::Block>>(&mut self, block_candidate: T) -> anyhow::Result<()> {
        let block_candidate = &block_candidate.into();
        let block_id = block_candidate.identifier();
        tracing::trace!("Applying block: {:?}", block_id);
        tracing::trace!("Applying block: {:?} ?= {:?}", self.block_id, block_candidate.parent());
        assert!(
            self.block_id == block_candidate.parent(),
            "Tried to apply block that is not child"
        );
        let start = std::time::Instant::now();
        let last_processed_messages_index = self.last_processed_external_message_index
            + block_candidate.processed_ext_messages_cnt as u32;
        if block_candidate.tx_cnt == 0 {
            tracing::trace!("Incoming block has no txns, just add seq_no and block info");
            let mut prev_state = (*self.get_shard_state()).clone();
            let block_info = prepare_prev_block_info(block_candidate);
            prev_state.set_seq_no(prev_state.seq_no() + 1);

            self.block_id = block_candidate.identifier();
            self.shard_state = Some(Arc::new(prev_state));
            self.shard_state_cell = None;
            self.block_info = block_info;
            self.last_processed_external_message_index = last_processed_messages_index;
        } else {
            let prev_state = self.get_shard_state_as_cell();
            tracing::trace!("Applying block loaded state");

            log::trace!(target: "node", "apply_block: Old state hash: {:?}", prev_state.repr_hash());
            let state_update = block_candidate
                .block
                .read_state_update()
                .map_err(|e| anyhow::format_err!("Failed to read block state update: {e}"))?;
            log::trace!(target: "node", "apply_block: update = {state_update:?}");
            tracing::trace!("Applying block loaded state update");
            let new_state = state_update
                .apply_for(&prev_state)
                .map_err(|e| anyhow::format_err!("Failed to apply state update: {e}"))?;
            log::trace!(target: "node", "apply_block: New state hash: {:?}", new_state.repr_hash());

            let block_info = prepare_prev_block_info(block_candidate);
            tracing::trace!("Calculated block info");

            self.block_id = block_candidate.identifier();
            self.shard_state = None;
            self.shard_state_cell = Some(new_state);
            self.block_info = block_info;
            self.last_processed_external_message_index = last_processed_messages_index;
        }
        log::trace!("Apply block {block_id:?} time: {} ms", start.elapsed().as_millis());
        Ok(())
    }
}
