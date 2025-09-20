// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use http_server::ExtMsgFeedbackList;
use telemetry_utils::mpsc::InstrumentedReceiver;
use telemetry_utils::now_ms;
use tracing::instrument;
use tracing::trace_span;
use tvm_block::GetRepresentationHash;
use tvm_executor::BlockchainConfig;
use tvm_types::Cell;
use tvm_types::HashmapType;
use typed_builder::TypedBuilder;

use crate::block::producer::builder::ActiveThread;
use crate::block::producer::builder::BlockBuilder;
use crate::block::producer::execution_time::ExecutionTimeLimits;
use crate::block::producer::wasm::WasmNodeCache;
use crate::block_keeper_system::wallet_config::create_wallet_slash_message;
use crate::block_keeper_system::BlockKeeperData;
use crate::block_keeper_system::BlockKeeperSlashData;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::external_messages::Stamp;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message::Message;
use crate::message::WrappedMessage;
use crate::multithreading::load_balancing_service::CheckError;
use crate::multithreading::load_balancing_service::ThreadAction;
use crate::node::associated_types::NackData;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::shared_services::SharedServices;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::accounts::AccountsRepository;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::storage::MessageDurableStorage;
use crate::types::AccountAddress;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockRound;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

pub const DEFAULT_VERIFY_COMPLEXITY: SignerIndex = (u16::MAX >> 5) + 1;

// Note: produces single block.
pub trait BlockProducer {
    type OptimisticState: OptimisticState;
    type Message: Message;

    #[allow(clippy::too_many_arguments)]
    fn produce<'a, I>(
        // This ensures this object will not be reused
        self,
        thread_identifier: ThreadIdentifier,
        parent_state: Self::OptimisticState,
        refs: I,
        control_rx_stop: InstrumentedReceiver<()>,
        message_db: MessageDurableStorage,
        time_limits: &ExecutionTimeLimits,
        block_round: BlockRound,
        parent_block_state: BlockState,
    ) -> anyhow::Result<(
        AckiNackiBlock,
        Self::OptimisticState,
        Vec<(Cell, ActiveThread)>,
        CrossThreadRefData,
        Vec<Stamp>,
        ExtMsgFeedbackList,
        BlockState,
    )>
    where
        I: std::iter::Iterator<Item = &'a CrossThreadRefData> + Clone,
        CrossThreadRefData: 'a;
}

#[derive(TypedBuilder)]
pub struct TVMBlockProducer {
    active_threads: Vec<(Cell, ActiveThread)>,
    blockchain_config: Arc<BlockchainConfig>,
    message_queue: HashMap<AccountAddress, VecDeque<(Stamp, tvm_block::Message)>>,
    producer_node_id: NodeIdentifier,
    thread_count_soft_limit: usize,
    parallelization_level: usize,
    block_keeper_epoch_code_hash: String,
    block_keeper_preepoch_code_hash: String,
    epoch_block_keeper_data: Vec<BlockKeeperData>,
    shared_services: SharedServices,
    block_nack: Vec<Envelope<GoshBLS, NackData>>,
    accounts: AccountsRepository,
    block_state_repository: BlockStateRepository,
    metrics: Option<BlockProductionMetrics>,
    wasm_cache: WasmNodeCache,
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
        parent_state: Self::OptimisticState,
        refs: I,
        control_rx_stop: InstrumentedReceiver<()>,
        message_db: MessageDurableStorage,
        time_limits: &ExecutionTimeLimits,
        block_round: BlockRound,
        parent_block_state: BlockState,
    ) -> anyhow::Result<(
        AckiNackiBlock,
        Self::OptimisticState,
        Vec<(Cell, ActiveThread)>,
        CrossThreadRefData,
        Vec<Stamp>,
        ExtMsgFeedbackList,
        BlockState,
    )>
    where
        // TODO: remove Clone and change to Into<>
        I: std::iter::Iterator<Item = &'a CrossThreadRefData> + Clone,
        CrossThreadRefData: 'a,
    {
        let (initial_state, in_table, white_list_of_slashing_messages_hashes, forwarded_messages) =
            trace_span!("pre processing").in_scope(|| {
                tracing::trace!("Start production");
                tracing::trace!(
                    "Producing block for {}, parent_seq_no: {}, refs: {:?}",
                    thread_identifier,
                    parent_state.block_seq_no,
                    refs.clone()
                        .map(|e| (*e.block_thread_identifier(), *e.block_seq_no()))
                        .collect::<Vec<_>>()
                );
                let mut wrapped_slash_messages = vec![];
                let mut white_list_of_slashing_messages_hashes = HashSet::new();
                trace_span!("nacks").in_scope(|| {
                    for nack in self.block_nack.iter() {
                        tracing::trace!("push nack into slash {:?}", nack);
                        let reason = nack.data().reason.clone();
                        if let Some((id, bls_key, addr)) =
                            reason.get_node_data(self.block_state_repository.clone())
                        {
                            let epoch_nack_data = BlockKeeperSlashData {
                                node_id: id,
                                bls_pubkey: bls_key,
                                addr,
                                slash_type: 0,
                            };
                            let msg = create_wallet_slash_message(&epoch_nack_data)?;
                            let wrapped_message = Arc::new(WrappedMessage { message: msg.clone() });
                            wrapped_slash_messages.push(wrapped_message);
                            white_list_of_slashing_messages_hashes.insert(msg.hash().unwrap());
                        }
                    }
                    Ok::<_, anyhow::Error>(())
                })?;
                let cross_thread_ref_data_service = self
                    .shared_services
                    .exec(|container| container.cross_thread_ref_data_service.clone());
                let preprocessing_result = crate::block::preprocessing::preprocess(
                    parent_state,
                    refs.clone(),
                    &thread_identifier,
                    &cross_thread_ref_data_service,
                    wrapped_slash_messages,
                    self.epoch_block_keeper_data.clone(),
                    message_db.clone(),
                    self.metrics.clone(),
                )?;
                Ok::<_, anyhow::Error>((
                    preprocessing_result.state,
                    preprocessing_result.threads_table,
                    white_list_of_slashing_messages_hashes,
                    preprocessing_result.redirected_messages,
                ))
            })?;

        let ref_ids: Vec<BlockIdentifier> =
            refs.into_iter().map(|ref_data| ref_data.block_identifier().clone()).collect();
        let active_threads = self.active_threads;
        let block_gas_limit = self.blockchain_config.get_gas_config(false).block_gas_limit;

        tracing::debug!(target: "node", "PARENT block: {:?}", initial_state.get_block_info());
        tracing::trace!(target: "node", "ref_ids: {:?}", ref_ids);

        let time = now_ms();

        let producer = BlockBuilder::with_params(
            thread_identifier,
            initial_state,
            time,
            block_gas_limit,
            None,
            Some(control_rx_stop),
            self.accounts,
            self.block_keeper_epoch_code_hash.clone(),
            self.block_keeper_preepoch_code_hash.clone(),
            self.parallelization_level,
            forwarded_messages,
            self.metrics.clone(),
            self.wasm_cache,
        )
        .map_err(|e| anyhow::format_err!("Failed to create block builder: {e}"))?;
        let (mut prepared_block, processed_stamps, ext_message_feedbacks) = producer.build_block(
            std::mem::take(&mut self.message_queue),
            &self.blockchain_config,
            active_threads,
            None,
            white_list_of_slashing_messages_hashes,
            message_db.clone(),
            time_limits,
        )?;
        tracing::trace!(target: "node", "block generated successfully");
        Self::print_block_info(&prepared_block.block);

        tracing::trace!(
            "Block generation finished, processed_ext_msgs_cnt={}",
            processed_stamps.len()
        );

        let res = trace_span!("post production").in_scope(|| {
            let mut cross_thread_ref_data = prepared_block.cross_thread_ref_data.clone();
            let produced_block_id = prepared_block.state.block_id.clone();
            let proposed_action = {
                match self.shared_services.exec(|e| {
                    let result = e.load_balancing.check(
                        &produced_block_id,
                        &thread_identifier,
                        &in_table,
                        self.thread_count_soft_limit,
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
                new_state.set_produced_threads_table(table.clone());
                cross_thread_ref_data.set_threads_table(table);
                // Cant continue with the existing block production since the threads table has changed!
                // TODO: actually send kill signal to child threads.
                // let _active_threads = std::mem::take(&mut prepared_block.active_threads);
            }

            // let producer_selector = self.block_state_repository.get(&parent_block_id).expect("Must be set").guarded(|e| e.producer_selector_data().clone()).expect("Must be set");
            // self.block_state_repository.get(&produced_block_id).expect("Can't fail").guarded_mut(|e|
            //     e.set_producer_selector_data(producer_selector.clone())
            // ).expect("Must be able to set producer selector");
            let active_threads = std::mem::take(&mut prepared_block.active_threads);
            cross_thread_ref_data.set_block_refs(ref_ids.clone());
            let processed_ext_msg_cnt = processed_stamps.len();

            let block_height = parent_block_state
                .guarded(|e| *e.block_height())
                .expect("Parent block does not have block height set")
                .next(&thread_identifier);

            let produced_block_state =
                self.block_state_repository.get(&produced_block_id).expect("Can't fail");
            produced_block_state.guarded_mut(|e| {
                e.set_block_height(block_height).expect("Failed to set block_height");
                e.set_block_round(block_round).expect("Failed to set round for the block state")
            });

            let res = (
                AckiNackiBlock::new(
                    thread_identifier,
                    prepared_block.block,
                    self.producer_node_id,
                    prepared_block.tx_cnt,
                    prepared_block.block_keeper_set_changes,
                    DEFAULT_VERIFY_COMPLEXITY,
                    ref_ids,
                    forward_table,
                    block_round,
                    block_height,
                    #[cfg(feature = "monitor-accounts-number")]
                    prepared_block.accounts_number_diff,
                ),
                new_state,
                active_threads,
                cross_thread_ref_data,
                processed_stamps,
                ext_message_feedbacks,
                produced_block_state,
            );

            tracing::trace!(
                "Finish block production: {} {} {}",
                res.0.seq_no(),
                res.0.identifier(),
                processed_ext_msg_cnt,
            );
            res
        });

        Ok(res)
    }
}
