// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::mpsc::Receiver;
use std::sync::Arc;

use anyhow::ensure;
use tvm_block::GetRepresentationHash;
use tvm_block::HashmapAugType;
use tvm_executor::BlockchainConfig;
use tvm_types::Cell;
use tvm_types::HashmapType;
use typed_builder::TypedBuilder;

use crate::block::producer::builder::structs::ActiveThread;
use crate::block::producer::builder::structs::BlockBuilder;
use crate::block_keeper_system::BlockKeeperData;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::message::Message;
use crate::message::WrappedMessage;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::types::AckiNackiBlock;

pub const DEFAULT_VERIFY_COMPLEXITY: SignerIndex = (u16::MAX >> 5) + 1;

// Note: produces single block.
pub trait BlockProducer {
    type OptimisticState: OptimisticState;
    type Message: Message;

    fn produce<'a, I>(
        // This ensures this object will not be reused
        self,
        initial_state: Self::OptimisticState,
        refs: I,
        control_rx_stop: Receiver<()>,
    ) -> anyhow::Result<(AckiNackiBlock<GoshBLS>, Self::OptimisticState, Vec<(Cell, ActiveThread)>)>
    where
        I: std::iter::Iterator<Item = &'a Self::OptimisticState> + Clone,
        <Self as BlockProducer>::OptimisticState: 'a;

    fn generate_verify_block<'a, I>(
        self,
        block: &AckiNackiBlock<GoshBLS>,
        initial_state: Self::OptimisticState,
        refs: I,
    ) -> anyhow::Result<(AckiNackiBlock<GoshBLS>, Self::OptimisticState)>
    where
        // TODO: remove Clone and change to Into<>
        I: std::iter::Iterator<Item = &'a Self::OptimisticState> + Clone,
        <Self as BlockProducer>::OptimisticState: 'a;
}

#[derive(TypedBuilder)]
pub struct TVMBlockProducer {
    active_threads: Vec<(Cell, ActiveThread)>,
    blockchain_config: Arc<BlockchainConfig>,
    message_queue: VecDeque<tvm_block::Message>,
    node_config: Config,
    epoch_block_keeper_data: Vec<BlockKeeperData>,
}

impl TVMBlockProducer {
    fn print_block_info(block: &tvm_block::Block) {
        let extra = block.read_extra().unwrap();
        tracing::info!(target: "node",
            "block: gen time = {}, in msg count = {}, out msg count = {}, account_blocks = {}",
            block.read_info().unwrap().gen_utime(),
            extra.read_in_msg_descr().unwrap().len().unwrap(),
            extra.read_out_msg_descr().unwrap().len().unwrap(),
            extra.read_account_blocks().unwrap().len().unwrap());
    }
}

impl BlockProducer for TVMBlockProducer {
    type Message = WrappedMessage;
    type OptimisticState = OptimisticStateImpl;

    // TODO: embed refs
    fn produce<'a, I>(
        // This ensures this object will not be reused
        self,
        mut initial_state: Self::OptimisticState,
        refs: I,
        control_rx_stop: Receiver<()>,
    ) -> anyhow::Result<(AckiNackiBlock<GoshBLS>, Self::OptimisticState, Vec<(Cell, ActiveThread)>)>
    where
        // TODO: remove Clone and change to Into<>
        I: std::iter::Iterator<Item = &'a Self::OptimisticState> + Clone,
        <Self as BlockProducer>::OptimisticState: 'a,
    {
        tracing::trace!("Start production");
        let now = std::time::SystemTime::now();
        let mut ref_ids = vec![];
        // Merge state threads table and DAPP table with other threads
        for state in refs.clone() {
            initial_state.merge_dapp_id_tables(state)?;
            initial_state.merge_threads_table(state)?;
            ref_ids.push(state.block_id.clone());
        }
        // Collect messages from other threads to the current thread
        let mut imported_messages = vec![];
        for state in refs {
            imported_messages
                .extend_from_slice(&state.get_messages_for_another_thread(&initial_state)?);
        }
        tracing::trace!("Ref imported messages: {imported_messages:?}");
        let imported_messages =
            imported_messages.into_iter().map(|wrapped_message| wrapped_message.message).collect();

        let time = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as u32;
        let active_threads = self.active_threads;

        let block_gas_limit = self.blockchain_config.get_gas_config(false).block_gas_limit;
        tracing::debug!(target: "node", "PARENT block: {:?}", initial_state.get_block_info());

        let producer = BlockBuilder::with_params(
            initial_state,
            time,
            block_gas_limit,
            None,
            Some(control_rx_stop),
            self.node_config.clone(),
            imported_messages,
        )
        .map_err(|e| anyhow::format_err!("Failed to create block builder: {e}"))?;
        let (mut prepared_block, processed_ext_msgs_cnt) = producer.build_block(
            self.message_queue.clone(),
            &self.blockchain_config,
            active_threads,
            self.epoch_block_keeper_data.clone(),
            None,
        )?;
        tracing::trace!(target: "node", "block generated successfully");
        Self::print_block_info(&prepared_block.block);

        tracing::trace!(
            "Block generation finished, processed_ext_msgs_cnt={processed_ext_msgs_cnt}"
        );

        let active_threads = std::mem::take(&mut prepared_block.active_threads);
        let res = (
            AckiNackiBlock::new(
                // TODO: fix single thread implementation
                prepared_block.state.thread_id,
                //
                prepared_block.block,
                processed_ext_msgs_cnt,
                self.node_config.local.node_id,
                prepared_block.tx_cnt,
                prepared_block.block_keeper_set_changes,
                DEFAULT_VERIFY_COMPLEXITY,
                ref_ids,
            ),
            prepared_block.state,
            active_threads,
        );
        tracing::trace!(
            "Finish block production: {} {} {}",
            res.0.seq_no(),
            res.0.identifier(),
            res.0.processed_ext_messages_cnt(),
        );
        Ok(res)
    }

    fn generate_verify_block<'a, I>(
        self,
        block: &AckiNackiBlock<GoshBLS>,
        initial_state: Self::OptimisticState,
        refs: I,
    ) -> anyhow::Result<(AckiNackiBlock<GoshBLS>, Self::OptimisticState)>
    where
        // TODO: remove Clone and change to Into<>
        I: std::iter::Iterator<Item = &'a Self::OptimisticState> + Clone,
        <Self as BlockProducer>::OptimisticState: 'a,
    {
        let time = block
            .tvm_block()
            .info
            .read_struct()
            .map_err(|e| anyhow::format_err!("Failed to read block info: {e}"))?
            .gen_utime()
            .as_u32();
        let block_extra = block
            .tvm_block()
            .extra
            .read_struct()
            .map_err(|e| anyhow::format_err!("Failed to read block extra: {e}"))?;
        let rand_seed = block_extra.rand_seed.clone();

        let mut ext_messages = Vec::new();
        let mut check_messages_map = HashMap::new();
        ensure!(
            block
                .tvm_block()
                .read_extra()
                .unwrap_or_default()
                .read_in_msg_descr()
                .unwrap_or_default()
                .iterate_objects(|in_msg| {
                    let trans =
                        in_msg.read_transaction()?.expect("Failed to read in_msg transaction");
                    let in_msg = in_msg.read_message()?;
                    assert_eq!(
                        check_messages_map.insert(in_msg.hash().unwrap(), trans.logical_time()),
                        None,
                        "Incoming block has non unique messages"
                    );
                    if in_msg.is_inbound_external() {
                        ext_messages.push((trans.logical_time(), in_msg));
                    }
                    Ok(true)
                })
                .map_err(|e| anyhow::format_err!("Failed to parse incoming block messages: {e}"))?,
            "Failed to parse incoming block messages"
        );
        ext_messages.sort_by(|(lt_a, _), (lt_b, _)| lt_a.cmp(lt_b));
        let ext_messages = VecDeque::from_iter(ext_messages.into_iter().map(|(_lt, msg)| msg));

        let block_gas_limit = self.blockchain_config.get_gas_config(false).block_gas_limit;

        tracing::debug!(target: "node", "PARENT block: {:?}", initial_state.get_block_info());

        let mut imported_messages = vec![];
        for state in refs {
            imported_messages
                .extend_from_slice(&state.get_messages_for_another_thread(&initial_state)?);
        }
        tracing::trace!("Ref imported messages: {imported_messages:?}");
        let imported_messages =
            imported_messages.into_iter().map(|wrapped_message| wrapped_message.message).collect();

        let producer = BlockBuilder::with_params(
            initial_state,
            time,
            block_gas_limit,
            Some(rand_seed),
            None,
            self.node_config.clone(),
            imported_messages,
        )
        .map_err(|e| anyhow::format_err!("Failed to create block builder: {e}"))?;
        let (verify_block, _) = producer.build_block(
            ext_messages,
            &self.blockchain_config,
            vec![],
            vec![],
            Some(check_messages_map),
        )?;
        tracing::trace!(target: "node", "verify block generated successfully");
        Self::print_block_info(&verify_block.block);

        let res = (
            AckiNackiBlock::new(
                // TODO: fix single thread implementation
                verify_block.state.thread_id,
                //
                verify_block.block,
                0,
                block.get_common_section().producer_id,
                verify_block.tx_cnt,
                verify_block.block_keeper_set_changes,
                block.get_common_section().verify_complexity,
                block.get_common_section().refs.clone(),
            ),
            verify_block.state,
        );

        Ok(res)
    }
}
