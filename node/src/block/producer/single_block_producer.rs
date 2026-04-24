// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use http_server::ExtMsgFeedbackList;
use node_types::BlockIdentifier;
use node_types::ParentRef;
use node_types::TemporaryBlockId;
use node_types::ThreadIdentifier;
use telemetry_utils::mpsc::InstrumentedReceiver;
use telemetry_utils::now_ms;
use tracing::instrument;
use tracing::trace_span;
use tvm_block::GetRepresentationHash;
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
use crate::config::config_read::ConfigRead;
use crate::config::BlockchainConfigHash;
use crate::config::BlockchainConfigRead;
use crate::external_messages::ExtMessageDst;
use crate::external_messages::QueuedExtMessage;
use crate::external_messages::Stamp;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message::Message;
use crate::message::WrappedMessage;
use crate::multithreading::load_balancing_service::CheckError;
use crate::multithreading::load_balancing_service::ThreadAction;
use crate::node::associated_types::NackData;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::shared_services::SharedServices;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::accounts::AccountsRepository;
use crate::repository::accounts::NodeThreadAccountsRepository;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::storage::MessageDurableStorage;
use crate::types::AckiNackiBlock;
use crate::types::AckiNackiBlockOld;
use crate::types::AckiNackiBlockVersioned;
use crate::types::BlockRound;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::versioning::ProtocolVersion;

pub const DEFAULT_VERIFY_COMPLEXITY: SignerIndex = (u16::MAX >> 5) + 1;

pub type Block = AckiNackiBlockVersioned;

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
        parent_ref: ParentRef,
        protocol_version: ProtocolVersion,
    ) -> anyhow::Result<(
        Block,
        Self::OptimisticState,
        Vec<(Cell, ActiveThread)>,
        CrossThreadRefData,
        Vec<Stamp>,
        ExtMsgFeedbackList,
        TemporaryBlockId,
    )>
    where
        I: std::iter::Iterator<Item = &'a CrossThreadRefData> + Clone,
        CrossThreadRefData: 'a;
}

#[derive(TypedBuilder)]
pub struct TVMBlockProducer {
    active_threads: Vec<(Cell, ActiveThread)>,
    node_config_read: ConfigRead,
    blockchain_config: BlockchainConfigRead,
    message_queue: HashMap<ExtMessageDst, VecDeque<(Stamp, QueuedExtMessage)>>,
    producer_node_id: NodeIdentifier,
    thread_count_soft_limit: usize,
    parallelization_level: usize,
    block_keeper_epoch_code_hash: String,
    block_keeper_preepoch_code_hash: String,
    epoch_block_keeper_data: Vec<BlockKeeperData>,
    shared_services: SharedServices,
    block_nack: Vec<Envelope<NackData>>,
    accounts: AccountsRepository,
    block_state_repository: BlockStateRepository,
    thread_accounts_repository: NodeThreadAccountsRepository,
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
        parent_ref: ParentRef,
        protocol_version: ProtocolVersion,
    ) -> anyhow::Result<(
        Block,
        Self::OptimisticState,
        Vec<(Cell, ActiveThread)>,
        CrossThreadRefData,
        Vec<Stamp>,
        ExtMsgFeedbackList,
        TemporaryBlockId,
    )>
    where
        // TODO: remove Clone and change to Into<>
        I: std::iter::Iterator<Item = &'a CrossThreadRefData> + Clone,
        CrossThreadRefData: 'a,
    {
        let (initial_state, _in_table, white_list_of_slashing_messages_hashes, forwarded_messages) =
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
                let is_block_of_retired_version =
                    self.node_config_read.is_retired(&protocol_version);
                let is_split_related =
                    thread_identifier.is_spawning_block(parent_state.get_block_id());
                let preprocessing_result = crate::block::preprocessing::preprocess(
                    parent_state,
                    refs.clone(),
                    &thread_identifier,
                    None,
                    is_split_related,
                    &cross_thread_ref_data_service,
                    wrapped_slash_messages,
                    self.epoch_block_keeper_data.clone(),
                    message_db.clone(),
                    self.metrics.clone(),
                    &self.thread_accounts_repository,
                    !is_block_of_retired_version,
                )?;
                Ok::<_, anyhow::Error>((
                    preprocessing_result.state,
                    preprocessing_result.threads_table,
                    white_list_of_slashing_messages_hashes,
                    preprocessing_result.redirected_messages,
                ))
            })?;

        let ref_ids: Vec<BlockIdentifier> =
            refs.into_iter().map(|ref_data| *ref_data.block_identifier()).collect();
        let active_threads = self.active_threads;
        let is_block_of_retired_version = self.node_config_read.is_retired(&protocol_version);
        let global_config = self.node_config_read.get(&protocol_version).ok_or(
            anyhow::format_err!("Failed to load config for block version: {}", protocol_version),
        )?;
        let bc_config_hash: BlockchainConfigHash =
            global_config.blockchain_config_hash.clone().into();
        let blockchain_config = self.blockchain_config.get(&bc_config_hash)?;
        let block_gas_limit = blockchain_config.get_gas_config(false).block_gas_limit;

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
            self.thread_accounts_repository.clone(),
            self.block_keeper_epoch_code_hash.clone(),
            self.block_keeper_preepoch_code_hash.clone(),
            self.parallelization_level,
            forwarded_messages,
            self.metrics.clone(),
            self.wasm_cache,
            false,
            is_block_of_retired_version,
        )
        .map_err(|e| anyhow::format_err!("Failed to create block builder: {e}"))?;
        let (mut prepared_block, processed_stamps, ext_message_feedbacks) = producer.build_block(
            std::mem::take(&mut self.message_queue),
            &blockchain_config,
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
            // Use the post-merge threads table for the split decision.
            // in_table is the parent's table before merging cross-thread refs.
            // prepared_block.state.threads_table includes threads discovered from
            // other threads' splits via cross-thread references, so the split check
            // sees all existing threads and avoids proposing a duplicate mask.
            let merged_threads_table = prepared_block.state.threads_table.clone();
            let proposed_action = {
                match self.shared_services.exec(|e| {
                    let result = e.load_balancing.check(
                        &thread_identifier,
                        &merged_threads_table,
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
            let forward_prefab = match proposed_action {
                ThreadAction::ContinueAsIs => None,
                ThreadAction::Split(e) => Some(e.proposed_threads_table),
                ThreadAction::Collapse(e) => Some(e.proposed_threads_table),
            };

            let active_threads = std::mem::take(&mut prepared_block.active_threads);
            cross_thread_ref_data.set_block_refs(ref_ids.clone());
            let processed_ext_msg_cnt = processed_stamps.len();

            // Resolve parent info from ParentRef
            let (parent_block_id, parent_block_height) = match &parent_ref {
                ParentRef::Block(id) => {
                    let parent_bs = self.block_state_repository.get(id)?;
                    let height = parent_bs.guarded(|e| *e.block_height()).ok_or(
                        anyhow::format_err!("Parent block does not have block height set"),
                    )?;
                    (*id, height)
                }
                ParentRef::Temporary(temp_id) => {
                    let temp_arc = self.block_state_repository.get_temporary(temp_id).ok_or(
                        anyhow::format_err!("Temporary parent state {temp_id:?} not found"),
                    )?;
                    let temp = temp_arc.read();

                    if let Some(promoted_id) = temp.promoted_block_id() {
                        // Parent already promoted — use the real block state
                        let parent_bs = self.block_state_repository.get(promoted_id)?;
                        let height = parent_bs.guarded(|e| *e.block_height()).ok_or(
                            anyhow::format_err!("Promoted parent does not have block height set"),
                        )?;
                        (*promoted_id, height)
                    } else {
                        // The payload-level parent is explicit even before promotion.
                        // For an in-flight temporary parent, carry the protocol's existing
                        // synthetic default id and replace it with the canonical parent id
                        // in on_production_timeout before sealing/broadcast.
                        let height = temp.block_height().cloned().ok_or(anyhow::format_err!(
                            "Temporary parent does not have block height set"
                        ))?;
                        (BlockIdentifier::default(), height)
                    }
                }
            };
            let block_height = parent_block_height.next(&thread_identifier);

            let producer_node_id = self.producer_node_id;
            let an_block = if !self.node_config_read.is_retired(&protocol_version) {
                tracing::trace!("Generate new block");

                AckiNackiBlockVersioned::New(AckiNackiBlock::new(
                    parent_block_id,
                    thread_identifier,
                    prepared_block.block,
                    producer_node_id.clone(),
                    prepared_block.tx_cnt,
                    prepared_block.block_keeper_set_changes,
                    DEFAULT_VERIFY_COMPLEXITY,
                    ref_ids,
                    forward_prefab.clone(),
                    block_round,
                    block_height,
                    #[cfg(feature = "monitor-accounts-number")]
                    prepared_block.accounts_number_diff,
                    #[cfg(feature = "protocol_version_hash_in_block")]
                    protocol_version.hash(),
                    prepared_block.durable_state_update,
                ))
            } else {
                tracing::trace!("Generate old block");
                AckiNackiBlockVersioned::Old(AckiNackiBlockOld::new(
                    thread_identifier,
                    prepared_block.block,
                    producer_node_id.clone(),
                    prepared_block.tx_cnt,
                    prepared_block.block_keeper_set_changes,
                    DEFAULT_VERIFY_COMPLEXITY,
                    ref_ids,
                    forward_prefab.clone(),
                    block_round,
                    block_height,
                    #[cfg(feature = "monitor-accounts-number")]
                    prepared_block.accounts_number_diff,
                    #[cfg(feature = "protocol_version_hash_in_block")]
                    protocol_version.hash(),
                    prepared_block.durable_state_update,
                ))
            };

            let new_state = prepared_block.state;

            // Create temporary block state (real BlockState is created on promotion)
            let temp_id = TemporaryBlockId::generate();
            let temp_state = self.block_state_repository.create_temporary(
                temp_id,
                parent_ref.clone(),
                thread_identifier,
            );
            {
                let mut ts = temp_state.write();
                ts.set_block_height(block_height);
                ts.set_block_round(block_round);
                ts.set_block_seq_no(an_block.seq_no());
                ts.set_producer(producer_node_id);
            }

            // Register child in parent
            match &parent_ref {
                ParentRef::Block(id) => {
                    let parent_bs = self.block_state_repository.get(id)?;
                    parent_bs
                        .guarded_mut(|e| e.add_incomplete_child(thread_identifier, temp_id))?;
                }
                ParentRef::Temporary(parent_temp_id) => {
                    let parent_temp = self
                        .block_state_repository
                        .get_temporary(parent_temp_id)
                        .ok_or(anyhow::format_err!(
                            "Temporary parent {parent_temp_id:?} not found for child registration"
                        ))?;
                    parent_temp.write().add_incomplete_child(thread_identifier, temp_id);
                }
            }

            tracing::trace!(
                "Finish block production: {} temp_id={} {}",
                an_block.seq_no(),
                temp_id,
                processed_ext_msg_cnt,
            );

            Ok((
                an_block,
                new_state,
                active_threads,
                cross_thread_ref_data,
                processed_stamps,
                ext_message_feedbacks,
                temp_id,
            ))
        });

        res
    }
}
