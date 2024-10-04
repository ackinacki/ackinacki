// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub mod builder;
pub mod process;

#[cfg(test)]
pub mod process_stub;

pub mod errors;
#[cfg(test)]
pub mod producer_stub;

use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::mpsc::Receiver;
use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;
use tvm_block::BlkPrevInfo;
use tvm_block::GetRepresentationHash;
use tvm_block::HashmapAugType;
use tvm_block::ShardStateUnsplit;
use tvm_executor::BlockchainConfig;
use tvm_types::Cell;
use tvm_types::HashmapType;

use crate::block::producer::builder::ActiveThread;
use crate::block::producer::builder::BlockBuilder;
use crate::block::Block;
use crate::block::WrappedBlock;
use crate::block_keeper_system::BlockKeeperData;
use crate::config::Config;
use crate::message::Message;
use crate::message::WrappedMessage;
use crate::node::SignerIndex;
use crate::transaction::Transaction;

pub const DEFAULT_VERIFY_COMPLEXITY: SignerIndex = (u16::MAX >> 5) + 1;

pub trait ThreadIdentifier: Eq + Hash + Clone + Debug + Into<u64> {}

impl ThreadIdentifier for u64 {}

pub trait BlockProducer {
    type Block: Block + Serialize + for<'a> Deserialize<'a>;
    type Cell;
    type Message: Message;
    type ShardState;
    type ThreadIdentifier: ThreadIdentifier;
    type Transaction: Transaction;

    fn produce(
        &mut self,
        control_rx_stop: Receiver<()>,
    ) -> anyhow::Result<(Self::Block, Self::ShardState, Self::Cell)>;

    fn generate_verify_block(
        &self,
        block: Self::Block,
    ) -> anyhow::Result<(Self::Block, Self::ShardState, Self::Cell)>;
}

pub struct TVMBlockProducer {
    active_threads: Vec<(Cell, ActiveThread)>,
    blockchain_config: Arc<BlockchainConfig>,
    message_queue: VecDeque<tvm_block::Message>,
    node_config: Config,
    prev_block_info: BlkPrevInfo,
    prev_block_shard_state_cell: Cell,
    epoch_block_keeper_data: Vec<BlockKeeperData>,
}

impl TVMBlockProducer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_config: Config,
        blockchain_config: Arc<BlockchainConfig>,
        message_queue: VecDeque<tvm_block::Message>,
        prev_block_shard_state_cell: Cell,
        prev_block_info: BlkPrevInfo,
        active_threads: Vec<(Cell, ActiveThread)>,
        epoch_block_keeper_data: Vec<BlockKeeperData>,
    ) -> Self {
        Self {
            active_threads,
            blockchain_config,
            message_queue,
            node_config,
            prev_block_info,
            prev_block_shard_state_cell,
            epoch_block_keeper_data,
        }
    }

    fn print_block_info(block: &tvm_block::Block) {
        let extra = block.read_extra().unwrap();
        log::info!(target: "node",
            "block: gen time = {}, in msg count = {}, out msg count = {}, account_blocks = {}",
            block.read_info().unwrap().gen_utime(),
            extra.read_in_msg_descr().unwrap().len().unwrap(),
            extra.read_out_msg_descr().unwrap().len().unwrap(),
            extra.read_account_blocks().unwrap().len().unwrap());
    }
}

impl BlockProducer for TVMBlockProducer {
    type Block = WrappedBlock;
    type Cell = Cell;
    type Message = WrappedMessage;
    type ShardState = ShardStateUnsplit;
    type ThreadIdentifier = u64;
    type Transaction = tvm_block::Transaction;

    fn produce(
        &mut self,
        control_rx_stop: Receiver<()>,
    ) -> anyhow::Result<(Self::Block, Self::ShardState, Self::Cell)> {
        tracing::trace!("Start production");
        let now = std::time::SystemTime::now();
        let time = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as u32;
        let active_threads = std::mem::take(&mut self.active_threads);
        let block_gas_limit = self.blockchain_config.get_gas_config(false).block_gas_limit;
        log::debug!(target: "node", "PARENT block: {:?}", self.prev_block_info);

        let producer = BlockBuilder::with_params(
            self.prev_block_shard_state_cell.clone(),
            self.prev_block_info.clone(),
            time,
            block_gas_limit,
            None,
            Some(control_rx_stop),
            self.node_config.clone(),
        )
        .map_err(|e| anyhow::format_err!("{e}"))?;
        let (mut prepared_block, processed_ext_msgs_cnt) = producer.build_block(
            self.message_queue.clone(),
            &self.blockchain_config,
            active_threads,
            self.epoch_block_keeper_data.clone(),
            None,
        )?;
        log::trace!(target: "node", "block generated successfully");
        Self::print_block_info(&prepared_block.block);

        tracing::trace!(
            "Block generation finished, processed_ext_msgs_cnt={processed_ext_msgs_cnt}"
        );

        self.active_threads = std::mem::take(&mut prepared_block.active_threads);
        let res = (
            WrappedBlock::new(
                prepared_block.block,
                processed_ext_msgs_cnt,
                self.node_config.local.node_id,
                prepared_block.tx_cnt,
                prepared_block.block_keeper_set_changes,
                DEFAULT_VERIFY_COMPLEXITY,
            ),
            prepared_block.state,
            prepared_block.state_cell,
        );
        tracing::trace!(
            "Finish block production: {} {} {}",
            res.0.seq_no(),
            res.0.identifier(),
            res.0.processed_ext_messages_cnt,
        );
        Ok(res)
    }

    fn generate_verify_block(
        &self,
        block: Self::Block,
    ) -> anyhow::Result<(Self::Block, Self::ShardState, Self::Cell)> {
        let time = block
            .block
            .info
            .read_struct()
            .map_err(|e| anyhow::format_err!("Failed to read block info: {e}"))?
            .gen_utime()
            .as_u32();
        let block_extra = block
            .block
            .extra
            .read_struct()
            .map_err(|e| anyhow::format_err!("Failed to read block extra: {e}"))?;
        let rand_seed = block_extra.rand_seed.clone();

        let mut ext_messages = VecDeque::new();
        let mut internal_messages_set = HashSet::new();
        block
            .block
            .read_extra()
            .unwrap_or_default()
            .read_account_blocks()
            .unwrap_or_default()
            .iterate_objects(|account_block| {
                account_block
                    .transactions()
                    .iterate_objects(|tx| {
                        let in_msg = tx.in_msg.as_ref().unwrap().read_struct().unwrap();
                        if in_msg.is_inbound_external() {
                            ext_messages.push_back(in_msg);
                        } else {
                            internal_messages_set.insert(in_msg.hash().unwrap());
                        }
                        Ok(true)
                    })
                    .unwrap();
                Ok(true)
            })
            .unwrap();

        let block_gas_limit = self.blockchain_config.get_gas_config(false).block_gas_limit;

        log::debug!(target: "node", "PARENT block: {:?}", self.prev_block_info);

        let producer = BlockBuilder::with_params(
            self.prev_block_shard_state_cell.clone(),
            self.prev_block_info.clone(),
            time,
            block_gas_limit,
            Some(rand_seed),
            None,
            self.node_config.clone(),
        )
        .map_err(|e| anyhow::format_err!("{e}"))?;
        let (verify_block, _) = producer.build_block(
            ext_messages,
            &self.blockchain_config,
            vec![],
            vec![],
            Some(internal_messages_set),
        )?;
        log::trace!(target: "node", "verify block generated successfully");
        Self::print_block_info(&verify_block.block);

        let res = (
            WrappedBlock::new(
                verify_block.block,
                0,
                block.common_section.producer_id,
                verify_block.tx_cnt,
                verify_block.block_keeper_set_changes,
                block.common_section.verify_complexity,
            ),
            verify_block.state,
            verify_block.state_cell,
        );

        Ok(res)
    }
}
