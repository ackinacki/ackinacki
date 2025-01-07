// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::mpsc::Receiver;
use std::sync::Arc;

use tracing::instrument;
use tracing::trace_span;
use tvm_block::GetRepresentationHash;
use tvm_executor::BlockchainConfig;
use tvm_types::Cell;
use tvm_types::HashmapType;
use typed_builder::TypedBuilder;

use crate::block::producer::builder::ActiveThread;
use crate::block::producer::builder::BlockBuilder;
use crate::block_keeper_system::wallet_config::create_wallet_slash_message;
use crate::block_keeper_system::BlockKeeperData;
use crate::block_keeper_system::BlockKeeperSlashData;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::message::Message;
use crate::message::WrappedMessage;
use crate::multithreading::load_balancing_service::CheckError;
use crate::multithreading::load_balancing_service::ThreadAction;
use crate::node::associated_types::NackData;
use crate::node::shared_services::SharedServices;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::types::block_keeper_ring::BlockKeeperRing;
use crate::types::AckiNackiBlock;
use crate::types::ThreadIdentifier;

pub const DEFAULT_VERIFY_COMPLEXITY: SignerIndex = (u16::MAX >> 5) + 1;

// Note: produces single block.
pub trait BlockProducer {
    type OptimisticState: OptimisticState;
    type Message: Message;

    fn produce<'a, I>(
        // This ensures this object will not be reused
        self,
        thread_identifier: ThreadIdentifier,
        parent_block_state: Self::OptimisticState,
        refs: I,
        control_rx_stop: Receiver<()>,
    ) -> anyhow::Result<(AckiNackiBlock, Self::OptimisticState, Vec<(Cell, ActiveThread)>)>
    where
        I: std::iter::Iterator<Item = &'a CrossThreadRefData> + Clone,
        CrossThreadRefData: 'a;
}

#[derive(TypedBuilder)]
pub struct TVMBlockProducer {
    active_threads: Vec<(Cell, ActiveThread)>,
    blockchain_config: Arc<BlockchainConfig>,
    message_queue: VecDeque<tvm_block::Message>,
    node_config: Config,
    epoch_block_keeper_data: Vec<BlockKeeperData>,
    shared_services: SharedServices,
    block_nack: Vec<Envelope<GoshBLS, NackData>>,
    block_keeper_sets: BlockKeeperRing,
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

    #[instrument(skip_all)]
    fn produce<'a, I>(
        // This ensures this object will not be reused
        mut self,
        thread_identifier: ThreadIdentifier,
        parent_block_state: Self::OptimisticState,
        refs: I,
        control_rx_stop: Receiver<()>,
    ) -> anyhow::Result<(AckiNackiBlock, Self::OptimisticState, Vec<(Cell, ActiveThread)>)>
    where
        // TODO: remove Clone and change to Into<>
        I: std::iter::Iterator<Item = &'a CrossThreadRefData> + Clone,
        CrossThreadRefData: 'a,
    {
        let (initial_state, in_table, white_list_of_slashing_messages_hashes) =
            trace_span!("pre processing").in_scope(|| {
                tracing::trace!("Start production");
                let mut wrapped_slash_messages = vec![];
                let mut white_list_of_slashing_messages_hashes = HashSet::new();
                for nack in self.block_nack.iter() {
                    tracing::trace!("push nack into slash {:?}", nack);
                    let reason = nack.data().reason.clone();
                    if let Some((id, bls_key, addr)) =
                        reason.get_node_data(self.block_keeper_sets.clone())
                    {
                        let epoch_nack_data = BlockKeeperSlashData {
                            node_id: id,
                            bls_pubkey: bls_key,
                            addr: addr.0,
                            slash_type: 0,
                        };
                        let msg = create_wallet_slash_message(&epoch_nack_data)?;
                        let wrapped_message = WrappedMessage { message: msg.clone() };
                        wrapped_slash_messages.push(wrapped_message);
                        white_list_of_slashing_messages_hashes.insert(msg.hash().unwrap());
                    }
                }
                let (initial_state, in_table) = self.shared_services.exec(|container| {
                    crate::block::preprocessing::preprocess(
                        parent_block_state,
                        refs.clone(),
                        &thread_identifier,
                        &container.cross_thread_ref_data_service,
                        wrapped_slash_messages,
                    )
                })?;
                Ok::<_, anyhow::Error>((
                    initial_state,
                    in_table,
                    white_list_of_slashing_messages_hashes,
                ))
            })?;

        let ref_ids =
            refs.into_iter().map(|ref_data| ref_data.block_identifier().clone()).collect();

        let active_threads = self.active_threads;

        let block_gas_limit = self.blockchain_config.get_gas_config(false).block_gas_limit;
        tracing::debug!(target: "node", "PARENT block: {:?}", initial_state.get_block_info());

        let now = std::time::SystemTime::now();
        let time = now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as u32;

        let producer = BlockBuilder::with_params(
            initial_state,
            time,
            block_gas_limit,
            None,
            Some(control_rx_stop),
            self.node_config.clone(),
        )
        .map_err(|e| anyhow::format_err!("Failed to create block builder: {e}"))?;
        let (mut prepared_block, processed_ext_msgs_cnt) = producer.build_block(
            self.message_queue.clone(),
            &self.blockchain_config,
            active_threads,
            self.epoch_block_keeper_data.clone(),
            None,
            white_list_of_slashing_messages_hashes,
        )?;
        tracing::trace!(target: "node", "block generated successfully");
        Self::print_block_info(&prepared_block.block);

        tracing::trace!(
            "Block generation finished, processed_ext_msgs_cnt={processed_ext_msgs_cnt}"
        );

        let res = trace_span!("post production").in_scope(|| {
            let produced_block_id = prepared_block.state.block_id.clone();
            let proposed_action = {
                match self.shared_services.exec(|e| {
                    let result = e.load_balancing.check(
                        &produced_block_id,
                        &thread_identifier,
                        &in_table,
                        self.node_config.global.max_threads_count,
                    );
                    tracing::trace!("load balancing check result: {:?}", &result,);
                    result
                }) {
                    Ok(e) => e,
                    Err(CheckError::StatsAreNotReady) => ThreadAction::ContinueAsIs,
                    Err(CheckError::ThreadIsNotInTheTable) => {
                        // TODO: print trace. needs an investigation.
                        panic!("needs an investigation");
                        // Safe fallback
                        // ThreadAction::ContinueAsIs
                    }
                }
            };
            let forward_table = {
                match proposed_action {
                    ThreadAction::ContinueAsIs => None,
                    ThreadAction::Split(e) => Some(e.proposed_threads_table),
                    ThreadAction::Collapse(e) => Some(e.proposed_threads_table),
                }
            };

            let mut new_state = prepared_block.state;
            if let Some(table) = forward_table.clone() {
                new_state.set_produced_threads_table(table);
                // Cant continue with the existing block production since the threads table has changed!
                // TODO: actually send kill signal to child threads.
                // let _active_threads = std::mem::take(&mut prepared_block.active_threads);
            }

            let active_threads = std::mem::take(&mut prepared_block.active_threads);
            let res = (
                AckiNackiBlock::new(
                    thread_identifier,
                    prepared_block.block,
                    processed_ext_msgs_cnt,
                    self.node_config.local.node_id,
                    prepared_block.tx_cnt,
                    prepared_block.block_keeper_set_changes,
                    DEFAULT_VERIFY_COMPLEXITY,
                    ref_ids,
                    forward_table,
                ),
                new_state,
                active_threads,
            );
            tracing::trace!(
                "Finish block production: {} {} {}",
                res.0.seq_no(),
                res.0.identifier(),
                res.0.processed_ext_messages_cnt(),
            );
            res
        });

        Ok(res)
    }
}
