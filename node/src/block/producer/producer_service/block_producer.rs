// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

#[cfg(feature = "history_proofs")]
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
#[cfg(feature = "history_proofs")]
use std::ops::Div;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use http_server::ExtMsgFeedbackList;
use network::channel::NetBroadcastSender;
use node_types::BlockIdentifier;
use node_types::ParentRef;
use node_types::TemporaryBlockId;
use node_types::ThreadIdentifier;
use parking_lot::Mutex;
use telemetry_utils::instrumented_channel_ext::WrappedItem;
use telemetry_utils::instrumented_channel_ext::XInstrumentedSender;
use telemetry_utils::mpsc::InstrumentedSender;
use typed_builder::TypedBuilder;

use crate::block::producer::process::TVMBlockProducerProcess;
use crate::block::producer::process::FORCE_SYNC_STATE_BLOCK_FREQUENCY;
use crate::block::producer::producer_service::memento::Assumptions;
use crate::block::producer::producer_service::memento::BlockProducerMemento;
use crate::block_keeper_system::bk_set::update_block_keeper_set_from_common_section;
use crate::block_keeper_system::BlockKeeperSetChange::BlockKeeperAdded;
use crate::block_keeper_system::BlockKeeperSetChange::BlockKeeperRemoved;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::bls::try_seal::TrySeal;
use crate::config::config_read::ConfigRead;
use crate::config::must_save_state_on_seq_no;
use crate::external_messages::ExternalMessagesThreadState;
use crate::helper::block_flow_trace;
use crate::helper::SHUTDOWN_FLAG;
#[cfg(feature = "misbehave")]
use crate::misbehavior::misbehave_rules;
use crate::node::associated_types::AckData;
use crate::node::associated_types::NackData;
use crate::node::associated_types::NodeCredentials;
use crate::node::associated_types::SynchronizationResult;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::block_state::tools::promote_temporary_to_block_state;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocksSelectError;
use crate::node::services::attestations_target::service::AttestationTargetsService;
use crate::node::services::attestations_target::service::EvaluateIfNextBlockAncestorsRequiredAttestationsWillBeMetSuccess;
use crate::node::shared_services::SharedServices;
use crate::node::NetBlock;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::node::LOOP_PAUSE_DURATION;
use crate::protocol::authority_switch::action_lock::BlockProducerCommand;
use crate::protocol::authority_switch::action_lock::StartBlockProducerThreadInitialParameters;
use crate::protocol::authority_switch::network_message::AuthoritySwitch;
use crate::protocol::authority_switch::network_message::NextRoundSuccess;
use crate::repository::optimistic_state::OptimisticStateSaveCommand;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::as_signatures_map::AsSignaturesMap;
use crate::types::bp_selector::ProducerSelector;
use crate::types::common_section::CommonSectionVersioned;
use crate::types::common_section::Directives;
use crate::types::common_section::DirectivesOld;
use crate::types::common_section::DirectivesVersioned;
#[cfg(feature = "history_proofs")]
use crate::types::history_proof::LayerNumber;
#[cfg(feature = "history_proofs")]
use crate::types::history_proof::ProofLayerRootHash;
#[cfg(feature = "history_proofs")]
use crate::types::history_proof::HISTORY_PROOF_WINDOW_SIZE;
use crate::types::AckiNackiBlock;
use crate::types::AckiNackiBlockVersioned;
use crate::types::AggregateFilter;
use crate::types::BlockRound;
use crate::types::BlockSeqNo;
use crate::types::CollectedAttestations;
use crate::types::RndSeed;
use crate::utilities::all_elements_same::AllElementsSame;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::versioning::block_protocol_version_state::BlockProtocolVersionState;
use crate::versioning::ProtocolVersion;
use crate::versioning::ProtocolVersionSupport;

#[cfg(feature = "rotate_after_sync")]
lazy_static::lazy_static!(
    static ref SHARED_STATE_SEQ_NO: Arc<Mutex<Option<BlockSeqNo>>> = Arc::new(Mutex::new(None));
);

#[derive(Debug)]
struct ProductionStatus {
    init_params: StartBlockProducerThreadInitialParameters,
    last_produced: Option<(BlockIdentifier, BlockSeqNo)>,
}

#[derive(TypedBuilder)]
pub struct BlockProducer {
    self_addr: SocketAddr,
    thread_id: ThreadIdentifier,
    repository: RepositoryImpl,
    block_state_repository: BlockStateRepository,
    production_process: TVMBlockProducerProcess,
    feedback_sender: InstrumentedSender<ExtMsgFeedbackList>,
    received_acks: Arc<Mutex<Vec<Envelope<AckData>>>>,
    received_nacks: Arc<Mutex<Vec<Envelope<NackData>>>>,
    shared_services: SharedServices,
    bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
    #[builder(default = std::time::Instant::now())]
    last_broadcasted_produced_candidate_block_time: std::time::Instant,
    last_block_attestations: Arc<Mutex<CollectedAttestations>>,
    attestations_target_service: AttestationTargetsService,
    self_tx: XInstrumentedSender<(NetworkMessage, SocketAddr)>,
    self_authority_tx: XInstrumentedSender<(NetworkMessage, SocketAddr)>,
    broadcast_tx: NetBroadcastSender<NodeIdentifier, NetworkMessage>,

    node_credentials: NodeCredentials,
    production_timeout: Duration,
    save_state_frequency: u32,

    bp_production_count: Arc<AtomicI32>,

    #[builder(default)]
    production_status: Option<ProductionStatus>,

    #[builder(default)]
    producing_status: bool,

    is_producing: Arc<AtomicBool>,

    external_messages: ExternalMessagesThreadState,
    is_state_sync_requested: Arc<Mutex<Option<BlockSeqNo>>>,
    control_rx: std::sync::mpsc::Receiver<BlockProducerCommand>,

    save_optimistic_service_sender: InstrumentedSender<OptimisticStateSaveCommand>,
    #[allow(unused)]
    config_read: ConfigRead,
}

enum UpdateCommonSectionResult {
    Success {
        /// Parent's block version state — used to compute the child's state
        /// once the final block identifier is known.
        parent_block_version: BlockProtocolVersionState,
        /// `descendant_bk_set.iter().map(|e| e.protocol_support).all_elements_same()`
        /// pre-computed so we don't have to thread the whole BK set out.
        all_bk_same_version: Option<ProtocolVersionSupport>,
        /// Ancestor blocks that passed finalization checkpoints.
        passed_checkpoints: Vec<BlockIdentifier>,
    },
    FailedToBuildChainToTheLastFinalizedBlock,
    NotReadyYet,
    AncestorIsNotReadyYet,
    // NotEnoughAttestations(u32),
    AbortNotInBKSet,
}

impl BlockProducer {
    /// Stop the production thread and clean up any promoted temporary states.
    fn stop_and_cleanup(&mut self) -> anyhow::Result<()> {
        let result = self.production_process.stop_thread_production(&self.thread_id);
        self.block_state_repository.cleanup_temporaries();
        result
    }

    pub fn main_loop(&mut self) -> anyhow::Result<()> {
        // if let Some((block_id_to_continue, block_seq_no_to_continue)) =
        // self.find_thread_last_block_id_this_node_can_continue()?
        // {
        // self.execute_restarted_producer(block_id_to_continue, block_seq_no_to_continue)?;
        // }

        let mut in_flight_productions = None;
        let mut memento = None;
        let mut next_bp_command = None;
        loop {
            if let Ok(bp_command) = self.control_rx.try_recv() {
                in_flight_productions = None;
                memento = None;
                let _ = self.stop_and_cleanup();
                next_bp_command = Some(bp_command);
            }
            if in_flight_productions.is_none() && memento.is_none() {
                in_flight_productions = self.start_production(next_bp_command.take())?;
                if in_flight_productions.is_some() {
                    let (block_id_to_continue, block_seq_no_to_continue) =
                        in_flight_productions.unwrap();
                    if let Err(e) = self
                        .execute_restarted_producer(block_id_to_continue, block_seq_no_to_continue)
                    {
                        tracing::error!(
                            "Failed to restart producer from {} {:?}: {e}",
                            block_id_to_continue,
                            block_seq_no_to_continue
                        );
                        let e = self.stop_and_cleanup();
                        tracing::trace!("Production stop result: {e:?}");
                        in_flight_productions = None;
                    }
                }
                if in_flight_productions.is_none() {
                    // Reset thread load
                    let was_producer = self.producing_status;
                    self.producing_status = false;
                    self.is_producing.store(false, Ordering::Release);
                    if was_producer {
                        self.bp_production_count.fetch_sub(1, Ordering::Relaxed);
                        if let Err(err) = self.external_messages.clear_queue_for_non_producer() {
                            tracing::error!(
                                target: "ext_messages",
                                "Failed to clear ext message queue on producer stop for {:?}: {err:?}",
                                self.thread_id
                            );
                        }
                    }
                    self.repository
                        .get_metrics()
                        .inspect(|m| m.report_thread_load(0, &self.thread_id));
                }
            }
            self.block_state_repository.touch();
            let (broadcasted, cleared_memento, post_production_timestamp) =
                if in_flight_productions.is_some() || memento.is_some() {
                    tracing::debug!("Cut off block producer");

                    // Cut off block producer. Send whatever it has
                    let post_production_timestamp = std::time::Instant::now();
                    let had_memento = memento.is_some();
                    let (broadcasted, next_memento) =
                        self.on_production_timeout(&mut in_flight_productions, memento)?;
                    let cleared_memento = next_memento.is_none() && had_memento;
                    memento = next_memento;
                    (broadcasted, cleared_memento, Some(post_production_timestamp))
                } else {
                    (false, false, None)
                };

            let pause_duration = if broadcasted && cleared_memento {
                // Note: if node successfully broadcasted block and cleared memento, it has sent
                // memento and has production process stopped. So need to start production right
                // now
                Duration::default()
            } else if broadcasted || (in_flight_productions.is_none() && memento.is_none()) {
                let mut duration = self.production_timeout;
                if let Some(timestamp) = post_production_timestamp {
                    duration = duration.saturating_sub(timestamp.elapsed());
                }
                duration
            } else {
                LOOP_PAUSE_DURATION
            };
            sleep(pause_duration);
        }
    }

    pub fn start_production(
        &mut self,
        next_bp_command: Option<BlockProducerCommand>,
    ) -> anyhow::Result<Option<(BlockIdentifier, BlockSeqNo)>> {
        tracing::trace!("start_block_production");
        // Produce whatever threads it has to produce
        let mut producer_tails = None;
        if let Ok(Some((block_id_to_continue, block_seq_no_to_continue, initial_round))) =
            self.find_thread_last_block_id_this_node_can_continue(next_bp_command)
        {
            tracing::debug!(
                "Requesting producer to continue block id {:?}; seq_no {:?}, node_id {:?}",
                block_id_to_continue,
                block_seq_no_to_continue,
                self.node_credentials.node_id(),
            );
            let Ok(parent_block_state) = self.block_state_repository.get(&block_id_to_continue)
            else {
                tracing::error!("Parent block ({block_id_to_continue:?}) state is not ready");
                return Ok(producer_tails);
            };
            let Some(parent_block_version) =
                parent_block_state.guarded(|e| e.block_version_state().clone())
            else {
                tracing::error!(
                    "Parent block ({block_id_to_continue:?}) block_version_state is not ready"
                );
                return Ok(producer_tails);
            };
            let block_version = parent_block_version.to_use().clone();
            match self.production_process.start_thread_production(
                &self.thread_id,
                &block_id_to_continue,
                self.received_acks.clone(),
                self.received_nacks.clone(),
                self.block_state_repository.clone(),
                self.external_messages.clone(),
                self.is_state_sync_requested.clone(),
                initial_round,
                block_version,
            ) {
                Ok(()) => {
                    tracing::info!("Producer started successfully");
                    let was_producer = self.producing_status;
                    self.producing_status = true;
                    self.is_producing.store(true, Ordering::Release);
                    if !was_producer {
                        self.bp_production_count.fetch_add(1, Ordering::Relaxed);
                    }
                    producer_tails = Some((block_id_to_continue, block_seq_no_to_continue));
                }
                Err(e) => {
                    tracing::warn!("failed to start producer process: {e}");
                }
            };
        }
        Ok(producer_tails)
    }

    pub fn restart_production(
        &mut self,
        parent_block_id: BlockIdentifier,
        initial_round: BlockRound,
        block_version: ProtocolVersion,
    ) -> anyhow::Result<Option<(BlockIdentifier, BlockSeqNo)>> {
        tracing::trace!("restart_block_production");
        self.stop_and_cleanup()?;
        let mut producer_tails = None;
        let Some(parent_block_seq_no) =
            self.block_state_repository.get(&parent_block_id)?.guarded(|e| *e.block_seq_no())
        else {
            tracing::warn!("Parent block for production restart has no seq_no set. Skipping");
            return Ok(None);
        };
        tracing::info!(
            "Requesting producer to restart production from block id {:?}",
            parent_block_id,
        );
        match self.production_process.start_thread_production(
            &self.thread_id,
            &parent_block_id,
            self.received_acks.clone(),
            self.received_nacks.clone(),
            self.block_state_repository.clone(),
            self.external_messages.clone(),
            self.is_state_sync_requested.clone(),
            initial_round,
            block_version,
        ) {
            Ok(()) => {
                tracing::info!("Producer started successfully");
                let was_producer = self.producing_status;
                self.producing_status = true;
                self.is_producing.store(true, Ordering::Release);
                if !was_producer {
                    self.bp_production_count.fetch_add(1, Ordering::Relaxed);
                }
                producer_tails = Some((parent_block_id, parent_block_seq_no));
            }
            Err(e) => {
                tracing::warn!("failed to start producer process: {e}");
            }
        };
        Ok(producer_tails)
    }

    pub(crate) fn find_thread_last_block_id_this_node_can_continue(
        &mut self,
        next_bp_command: Option<BlockProducerCommand>,
    ) -> anyhow::Result<Option<(BlockIdentifier, BlockSeqNo, BlockRound)>> {
        tracing::trace!(
            "find_thread_last_block_id_this_node_can_continue start {:?}",
            self.thread_id
        );
        let mut update = next_bp_command;
        let mut last_update = loop {
            match self.control_rx.try_recv() {
                Ok(params) => {
                    tracing::trace!(
                        "find_thread_last_block_id_this_node_can_continue control_rx received {:?}",
                        params
                    );
                    update = Some(params);
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    // No updates
                    break update;
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    // do a graceful shutdown. program is shutting down.
                    return if SHUTDOWN_FLAG.get() == Some(&true) {
                        // NOTE: to prevent unexpected block finalization during shutdown
                        Ok(None)
                    } else {
                        Err(anyhow::anyhow!("Control tx was disconnected"))
                    };
                }
            }
        };
        tracing::trace!(
            "find_thread_last_block_id_this_node_can_continue last_update {:?}",
            last_update
        );
        if self.production_status.is_none() && last_update.is_none() {
            // await a command.
            let Ok(update) = self.control_rx.recv() else {
                // do a graceful shutdown. program is shutting down.
                return if SHUTDOWN_FLAG.get() == Some(&true) {
                    // NOTE: to prevent unexpected block finalization during shutdown
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!("Control tx was disconnected"))
                };
            };
            tracing::trace!(
                "find_thread_last_block_id_this_node_can_continue control_rx received {:?}",
                update
            );
            last_update = Some(update);
        }

        if let Some(update) = last_update {
            match update {
                BlockProducerCommand::Start(params) => {
                    tracing::trace!("find_thread_last_block_id_this_node_can_continue last_update has start cmd");
                    if let Some(production_status) = &self.production_status {
                        if production_status.init_params != params {
                            self.production_status =
                                Some(ProductionStatus { init_params: params, last_produced: None });
                        }
                        // Otherwise do nothing.
                    } else {
                        self.production_status =
                            Some(ProductionStatus { init_params: params, last_produced: None });
                    }
                }
            }
        }
        tracing::trace!("find_thread_last_block_id_this_node_can_continue last_update self.production_status {:?}", self.production_status);
        let Some(production_status) = self.production_status.as_ref() else {
            return Ok(None);
        };
        let (block_id, seq_no, round) =
            if let Some((block_id, block_seq_no)) = production_status.last_produced {
                (block_id, block_seq_no, 0)
            } else {
                (
                    *production_status.init_params.parent_block_identifier(),
                    *production_status.init_params.parent_block_seq_no(),
                    *production_status.init_params.round(),
                )
            };
        let (_last_finalized_id, last_finalized_seq_no) = self
            .repository
            .select_thread_last_finalized_block(&self.thread_id)?
            .ok_or(anyhow::format_err!("Failed to select last finalized block"))?;
        if seq_no < last_finalized_seq_no {
            self.production_status = None;
            Ok(None)
        } else {
            Ok(Some((block_id, seq_no, round)))
        }
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn on_production_timeout(
        &mut self,
        producer_tails: &mut Option<(BlockIdentifier, BlockSeqNo)>,
        mut memento: Option<BlockProducerMemento>,
    ) -> anyhow::Result<(bool, Option<BlockProducerMemento>)> {
        tracing::trace!("on_production_timeout start");
        if let Some(memento_ref) = memento.as_ref() {
            if let Some(saved_notification) = memento_ref.last_attestation_notification() {
                tracing::trace!("memento is set, wait for notification");

                let (lock, cvar) =
                    &*self.last_block_attestations.guarded(|e| Arc::clone(e.notifications()));
                let mut notification = lock.lock();
                cvar.wait_while(&mut notification, |e| e == saved_notification);
                tracing::trace!("memento is set, wait for notification end");
            }
        }

        let mut did_broadcast_something = false;

        let mut produced_data = memento.take().unwrap_or_else(|| {
            BlockProducerMemento::builder()
                .produced_blocks(self.production_process.get_produced_blocks())
                .build()
        });

        // While producer process was generating blocks, the node could receive valid
        // blocks, and the generated blocks could be discarded.
        // Get minimal block seq no that can be accepted from producer process.
        let minimal_seq_no_that_can_be_accepted_from_producer_process = self
            .repository
            .select_thread_last_finalized_block(&self.thread_id)?
            .unwrap_or_default()
            .1;
        tracing::trace!("on_production_timeout: minimal_seq_no_that_can_be_accepted_from_producer_process = {minimal_seq_no_that_can_be_accepted_from_producer_process:?}");
        while let Some(produced_block) = produced_data.produced_blocks().first() {
            let mut block = produced_block.block().clone();
            let temp_id = *produced_block.temporary_block_id();

            // Resolve parent from TemporaryBlockState and update block's parent_block_id
            {
                let temp_arc = self
                    .block_state_repository
                    .get_temporary(&temp_id)
                    .ok_or(anyhow::format_err!("Temporary state {temp_id:?} not found"))?;
                let parent_ref = temp_arc.read().parent_ref().clone();
                let parent_block_id = match parent_ref {
                    ParentRef::Block(id) => id,
                    ParentRef::Temporary(parent_temp_id) => {
                        // The parent temporary may have been promoted after this
                        // block was produced but before this block's parent_ref
                        // was updated (race between the production loop
                        // registering children and the main thread promoting
                        // the parent). Check if the parent temp was promoted
                        // and use its final block ID.
                        let parent_temp_arc = self
                            .block_state_repository
                            .get_temporary(&parent_temp_id)
                            .ok_or(anyhow::format_err!(
                                "Parent temporary {parent_temp_id:?} not found for child {temp_id:?}"
                            ))?;
                        let parent_temp = parent_temp_arc.read();
                        match parent_temp.promoted_block_id() {
                            Some(promoted_id) => {
                                let id = *promoted_id;
                                drop(parent_temp);
                                temp_arc.write().set_parent_ref(ParentRef::Block(id));
                                id
                            }
                            None => {
                                anyhow::bail!(
                                    "Parent {parent_temp_id:?} is not yet promoted for child {temp_id:?}"
                                );
                            }
                        }
                    }
                };
                block.update_parent_block_id(parent_block_id);
            }
            tracing::info!(
                "Got block from producer. temp_id: {}; seq_no: {:?}, parent: {:?}",
                temp_id,
                block.seq_no(),
                block.parent(),
            );

            if block.seq_no() <= minimal_seq_no_that_can_be_accepted_from_producer_process {
                tracing::trace!("Produced block is older than last finalized block. Stop production for thread: {:?}", self.thread_id);
                self.stop_and_cleanup()?;
                *producer_tails = None;
                return Ok((false, None));
            }
            let share_resulting_state = self.is_state_sync_requested.guarded(|e| *e);
            let should_share_state = share_resulting_state == Some(block.seq_no())
                || (<u32>::from(block.seq_no()) % FORCE_SYNC_STATE_BLOCK_FREQUENCY == 0);
            if should_share_state {
                tracing::trace!("Node should share state for last block");
            }
            let block_will_share_state = should_share_state;
            let attestations_notifications_stamp: u32 = {
                let (lock, _cvar) =
                    &*self.last_block_attestations.guarded(|e| e.notifications().clone());
                let n = *lock.lock();
                n
            };
            let update_result =
                self.update_candidate_common_section(&mut block, should_share_state, &temp_id);
            #[allow(unused_assignments)]
            let mut success_block_version_data: Option<(
                BlockProtocolVersionState,
                Option<ProtocolVersionSupport>,
                Vec<BlockIdentifier>,
            )> = None;
            let mut produced_block = match update_result {
                Ok(UpdateCommonSectionResult::FailedToBuildChainToTheLastFinalizedBlock) => {
                    tracing::trace!("Failed to update common section and our block seems to be invalidated, stop production");
                    self.stop_and_cleanup()?;
                    *producer_tails = None;
                    return Ok((false, None));
                }
                Err(e) => {
                    tracing::trace!("Update common section error {e:?}");
                    self.stop_and_cleanup()?;
                    self.repository
                        .store_optimistic_in_cache(produced_block.optimistic_state().clone())?;
                    let _ = self.save_optimistic_service_sender.send(
                        OptimisticStateSaveCommand::Save(produced_block.optimistic_state().clone()),
                    );
                    *producer_tails = None;
                    return Ok((false, Some(produced_data)));
                }
                Ok(UpdateCommonSectionResult::AncestorIsNotReadyYet) => {
                    // Memento
                    tracing::trace!("Failed to update common section, AncestorIsNotReadyYet");
                    // self.production_process.stop_thread_production(&self.thread_id)?;
                    // self.repository.store_optimistic(optimistic_state)?;
                    // *producer_tails = None;
                    produced_data
                        .set_last_attestation_notification(attestations_notifications_stamp + 1);
                    return Ok((false, Some(produced_data)));
                }
                Ok(UpdateCommonSectionResult::NotReadyYet) => {
                    // Memento
                    tracing::trace!("Failed to update common section, NotReadyYet");
                    // self.production_process.stop_thread_production(&self.thread_id)?;
                    // self.repository.store_optimistic(optimistic_state)?;
                    // *producer_tails = None;
                    produced_data
                        .set_last_attestation_notification(attestations_notifications_stamp);
                    return Ok((false, Some(produced_data)));
                }
                Ok(UpdateCommonSectionResult::Success {
                    parent_block_version,
                    all_bk_same_version,
                    passed_checkpoints,
                }) => {
                    // Derive the protocol version without the block identifier.
                    // `next().to_use()` never depends on block_id.
                    let actual_required_version = match &parent_block_version {
                        BlockProtocolVersionState::CompleteTransition { to, .. } => to.clone(),
                        other => other.to_use().clone(),
                    };
                    let mut producer_is_in_bk_set = true;
                    for change in produced_block.block().common_section().block_keeper_set_changes()
                    {
                        if let BlockKeeperRemoved((_, removed)) = change {
                            if removed.node_id() == *self.node_credentials.node_id() {
                                producer_is_in_bk_set = false;
                            }
                        }
                    }
                    for change in produced_block.block().common_section().block_keeper_set_changes()
                    {
                        if let BlockKeeperAdded((_, added)) = change {
                            if added.node_id() == *self.node_credentials.node_id() {
                                producer_is_in_bk_set = true;
                            }
                        }
                    }
                    let actual_assumptions = Assumptions::builder()
                        .new_to_bk_set(
                            BTreeSet::new(), // TODO: set real value with preattestaions implementation
                        )
                        .block_version(actual_required_version.clone())
                        .producer_is_in_bk_set(producer_is_in_bk_set)
                        .build();
                    let is_assumptions_correct =
                        produced_block.assumptions().eq(&actual_assumptions);
                    if is_assumptions_correct {
                        if let Some(seq_no) = share_resulting_state {
                            if seq_no <= block.seq_no() {
                                self.is_state_sync_requested.guarded_mut(|e| *e = None);
                            }
                        }
                        success_block_version_data =
                            Some((parent_block_version, all_bk_same_version, passed_checkpoints));
                        produced_data.produced_blocks_mut().remove(0)
                    } else {
                        tracing::trace!(
                            "Block assumptions: {:?}, actual: {actual_assumptions:?}",
                            produced_block.assumptions()
                        );
                        tracing::trace!("Assumptions were not met, restart production");
                        if let Some(seq_no) = share_resulting_state {
                            if seq_no < block.seq_no() {
                                self.is_state_sync_requested.guarded_mut(|e| *e = None);
                            }
                        }
                        produced_data.produced_blocks_mut().clear();
                        let parent_block_id = block.parent();
                        if producer_is_in_bk_set {
                            let block_round = *block.common_section().round();

                            *producer_tails = self.restart_production(
                                parent_block_id,
                                block_round,
                                actual_required_version.clone(),
                            )?;
                        }

                        #[cfg(feature = "history_proofs")]
                        if !self.config_read.is_retired(&actual_required_version) {
                            if let Some(parent_block_height) = self
                                .block_state_repository
                                .get(&parent_block_id)?
                                .guarded(|e| *e.block_height())
                            {
                                self.repository.init_history_proof_data(parent_block_height)?;
                            }
                        }
                        return Ok((false, None));
                    }
                }
                Ok(UpdateCommonSectionResult::AbortNotInBKSet) => {
                    tracing::trace!("Stop production: not in bk set");
                    self.stop_and_cleanup()?;
                    *producer_tails = None;
                    return Ok((false, None));
                }
            };
            // --- Finalize block ID dependent data ---
            // After set_common_section(_, true) the block has its final Merkle hash.
            // Promote temporary state to real BlockState, then resolve threads-table
            // prefab with the final ID so that ThreadIdentifiers match what validators
            // will compute in apply_block.
            let final_block_id = block.identifier();

            // Now that the final block_id is known, compute the actual
            // BlockProtocolVersionState (the milestone in TransitionLocked
            // must reference the real Merkle hash, not an intermediate one).
            let (parent_bvs, all_bk_same, checkpoints) = success_block_version_data
                .expect("success_block_version_data must be set on Success path");
            let success_block_version_state =
                parent_bvs.next(final_block_id, all_bk_same, &checkpoints);

            // Promote TemporaryBlockState → real BlockState
            let produced_block_state = promote_temporary_to_block_state(
                &temp_id,
                final_block_id,
                &self.block_state_repository,
            )?;
            let final_parent_block_id = block.parent();
            produced_block_state.guarded(|e| {
                anyhow::ensure!(
                    e.parent_block_identifier()
                        .expect("promoted block state must have parent block identifier")
                        == final_parent_block_id,
                    "Promoted block state parent mismatch: state={:?}, block={final_parent_block_id:?}, temp_id={temp_id}",
                    e.parent_block_identifier()
                );
                anyhow::ensure!(
                    e.thread_identifier().expect("promoted block state must have thread id")
                        == self.thread_id,
                    "Promoted block state thread mismatch: state={:?}, expected={:?}, block={final_block_id}",
                    e.thread_identifier(),
                    self.thread_id
                );
                Ok::<_, anyhow::Error>(())
            })?;

            // Write data from update_candidate_common_section to the promoted BlockState
            {
                let attestations = block.common_section().block_attestations();
                produced_block_state.guarded_mut(|e| {
                    for attestation in attestations {
                        e.add_verified_attestations_for(
                            attestation.data(),
                            HashSet::from_iter(
                                attestation.clone_signature_occurrences().keys().cloned(),
                            ),
                        )?;
                    }
                    e.set_block_version_state(success_block_version_state.clone())?;
                    Ok::<_, anyhow::Error>(())
                })?;
            }

            {
                let mut cross_thread_ref_data = produced_block.cross_thread_ref_data().clone();

                let mut updated_state = produced_block.optimistic_state().as_ref().clone();
                updated_state.block_id = final_block_id;
                updated_state
                    .thread_refs_state
                    .update(self.thread_id, (self.thread_id, final_block_id, block.seq_no()));

                let resolved_threads_table =
                    if let Some(prefab) = block.common_section().threads_table() {
                        let resolved_table = prefab.resolve(&final_block_id)?;
                        updated_state.threads_table = resolved_table.clone();
                        Some(resolved_table)
                    } else {
                        None
                    };

                cross_thread_ref_data.finalize_block_identifier_dependencies(
                    final_block_id,
                    final_parent_block_id,
                    self.thread_id,
                    resolved_threads_table,
                )?;

                produced_block.update_optimistic_state(Arc::new(updated_state));

                self.shared_services.exec(|e| {
                    e.cross_thread_ref_data_service.set_cross_thread_ref_data(cross_thread_ref_data)
                })?;
                produced_block_state.guarded_mut(|e| e.set_has_cross_thread_ref_data_prepared())?;
            }

            #[cfg(feature = "rotate_after_sync")]
            {
                if block.common_section().directives().share_state_resources() {
                    tracing::trace!("Save share state block seq_no: {:?}", block.seq_no());
                    let mut shared_state_seq_no = SHARED_STATE_SEQ_NO.lock();
                    *shared_state_seq_no = Some(block.seq_no());
                }
                if let Some(seq_no) = SHARED_STATE_SEQ_NO.lock().as_ref() {
                    if <u32>::from(seq_no.clone()) + 100 == <u32>::from(block.seq_no().clone()) {
                        self.stop_and_cleanup()?;
                        *producer_tails = None;
                        return Ok((false, None));
                    }
                }
            }

            let parent_state = self.block_state_repository.get(&block.parent())?;

            let must_save_state = block_will_share_state
                || block.is_thread_splitting()
                || must_save_state_on_seq_no(
                    block.seq_no(),
                    parent_state.guarded(|x| *x.block_seq_no()),
                    self.save_state_frequency,
                );
            if must_save_state {
                self.repository
                    .store_optimistic_in_cache(produced_block.optimistic_state().clone())?;
                let _ = self.save_optimistic_service_sender.send(OptimisticStateSaveCommand::Save(
                    produced_block.optimistic_state().clone(),
                ));
            }

            // NOTE: Issue here: This blocks accepting new blocks!
            let mut notifications = self.block_state_repository.notifications().clone();
            loop {
                tracing::trace!("loop get parent bk set");
                let stamp = notifications.stamp();
                if parent_state.guarded(|e| e.descendant_bk_set().is_some()) {
                    break;
                }
                if parent_state.guarded(|e| e.is_invalidated()) {
                    // Stop production in this case
                    tracing::trace!("parent block state was invalidated");
                    self.stop_and_cleanup()?;
                    return Ok((false, None));
                }
                notifications.wait_for_updates(stamp);
            }
            tracing::trace!("Got parent bk set");
            let bk_set = parent_state.guarded(|e| e.descendant_bk_set().clone().unwrap());

            let secrets = self.bls_keys_map.guarded(|map| map.clone());
            let production_status = self.production_status.as_ref().expect("must be in prod");
            let protocol_version = success_block_version_state.to_use().clone();

            let envelope = block.clone().try_seal(
                &self.node_credentials,
                &bk_set,
                &secrets,
                &protocol_version,
            );
            if let Err(e) = &envelope {
                tracing::error!("Failed to sign block: {e}");
                return Ok((false, None));
            };
            let envelope = envelope?;

            let net_block = NetBlock::with_versioned(&envelope)?;

            // Check if this node has already signed block of the same height
            // Note: Not valid anymore. Can't sign blocks of the same round though.
            // TODO: add corrected check.
            // if self.does_block_have_a_valid_sibling(&envelope)? {
            // tracing::trace!("Don't accept produced block because this node has already signed a block of the same height");
            // self.production_process.stop_thread_production(&self.thread_id)?;
            // return Ok((false, None));
            // }

            let producer_selector =
                block.common_section().producer_selector().clone().expect("Must be set");
            let parent_height = parent_state
                .guarded(|e| *e.block_height())
                .expect("Parent block height must be set");
            produced_block_state.guarded_mut(|e| {
                e.set_validated(true)?;
                e.set_producer_selector_data(producer_selector)?;
                e.set_block_height(parent_height.next(&self.thread_id))
            })?;

            let (send_res, net_message) = {
                if production_status.last_produced.is_none()
                    && *production_status.init_params.round() != 0
                {
                    let block_height =
                        produced_block_state.guarded(|e| (*e.block_height()).unwrap());
                    let message =
                        NetworkMessage::AuthoritySwitchProtocol(AuthoritySwitch::Switched(
                            NextRoundSuccess::builder()
                                .node_identifier(self.node_credentials.node_id().clone())
                                .round(*production_status.init_params.round())
                                .block_height(block_height)
                                .proposed_block(net_block)
                                .attestations_aggregated(None)
                                // TODO: Must include a proof!
                                .requests_aggregated(vec![])
                                .build().try_seal(
                                &self.node_credentials,
                                &bk_set,
                                &secrets,
                                &protocol_version,
                            )
                            .expect("must work"),
                        ));
                    (
                        self.self_authority_tx.send(WrappedItem {
                            payload: (message.clone(), self.self_addr),
                            label: self.thread_id.to_string(),
                        }),
                        message,
                    )
                } else {
                    let message = NetworkMessage::Candidate(net_block);
                    (
                        self.self_tx.send(WrappedItem {
                            payload: (message.clone(), self.self_addr),
                            label: self.thread_id.to_string(),
                        }),
                        message,
                    )
                }
            };
            match send_res {
                Ok(()) => {}
                _ => {
                    if SHUTDOWN_FLAG.get() != Some(&true) {
                        anyhow::bail!("Failed to send produced message");
                    }
                }
            }
            self.shared_services.metrics.as_ref().inspect(|m| {
                if let Some(metrics_memento_init_time) = produced_block.metrics_memento_init_time()
                {
                    m.report_memento_duration(
                        metrics_memento_init_time.elapsed().as_millis(),
                        &self.thread_id,
                    );
                }
            });

            let block_id = envelope.data().identifier();
            let block_seq_no = envelope.data().seq_no();

            tracing::trace!("insert to cache_forward_optimistic {:?} {:?}", block_seq_no, block_id);
            if let Some(ref mut production_status) = self.production_status {
                production_status.last_produced = Some((block_id, block_seq_no));
            } else {
                panic!("Something is wrong");
            };

            // Note: we have already checked parent block BK set, need to check whether this node is
            // in set for descendant blocks
            // if self.is_this_node_in_block_keeper_set(envelope.data().parent()) != Some(true) {
            //     self.production_process.stop_thread_production(&self.thread_id)?;
            // }

            self.last_broadcasted_produced_candidate_block_time = std::time::Instant::now();
            self.broadcast_candidate_block(
                &block_id,
                net_message,
                produced_block.feedbacks().clone(),
            )?;
            self.repository.store_optimistic_in_cache(produced_block.optimistic_state().clone())?;
            did_broadcast_something = true;
        }

        Ok((did_broadcast_something, None))
    }

    pub(crate) fn _does_block_have_a_valid_sibling(
        &self,
        candidate_block: &Envelope<AckiNackiBlock>,
    ) -> anyhow::Result<bool> {
        let parent = candidate_block.data().parent();
        let parent_state = self.block_state_repository.get(&parent)?;
        Ok(parent_state.guarded(|e| {
            if let Some(x) = e.known_children(&self.thread_id) {
                !x.is_empty()
            } else {
                false
            }
        }))
    }

    fn update_candidate_common_section(
        &mut self,
        candidate_block: &mut AckiNackiBlockVersioned,
        should_share_state: bool,
        temp_id: &TemporaryBlockId,
    ) -> anyhow::Result<UpdateCommonSectionResult> {
        tracing::trace!(
            "update_candidate_common_section: share_state {} block_seq_no: {:?}, temp_id: {}",
            should_share_state,
            candidate_block.seq_no(),
            temp_id
        );
        let mut received_attestations = self.last_block_attestations.guarded(|e| e.clone());

        let parent_block_state = self.block_state_repository.get(&candidate_block.parent())?;
        let Some(ancestor_blocks_finalization_checkpoints) =
            parent_block_state.guarded(|e| e.ancestor_blocks_finalization_checkpoints().clone())
        else {
            tracing::trace!(
                "update_candidate_common_section: ancestor_blocks_finalization_checkpoints missing"
            );
            // anyhow::bail!("ancestor_blocks_finalization_checkpoints missing");
            return Ok(UpdateCommonSectionResult::AncestorIsNotReadyYet);
        };
        let Some(parent_block_version) =
            parent_block_state.guarded(|e| e.block_version_state().clone())
        else {
            tracing::trace!("update_candidate_common_section: parent block_version is missing");
            return Ok(UpdateCommonSectionResult::AncestorIsNotReadyYet);
        };

        let attestations_required: Vec<(BlockState, AggregateFilter)> = {
            let mut buffer = vec![];
            let primary_only: Vec<BlockIdentifier> = {
                ancestor_blocks_finalization_checkpoints
                    .primary()
                    .keys()
                    .filter(|e| {
                        !ancestor_blocks_finalization_checkpoints.fallback().contains_key(*e)
                    })
                    .cloned()
                    .collect()
            };
            for ancestor_id in primary_only.into_iter() {
                let checkpoint =
                    ancestor_blocks_finalization_checkpoints.primary().get(&ancestor_id).unwrap();
                let ancestor = self.block_state_repository.get(&ancestor_id)?;
                buffer.push((ancestor, AggregateFilter::from(checkpoint)));
            }
            for (ancestor_id, checkpoints) in
                ancestor_blocks_finalization_checkpoints.fallback().iter()
            {
                let ancestor = self.block_state_repository.get(ancestor_id)?;
                for checkpoint in checkpoints.iter() {
                    buffer.push((ancestor.clone(), AggregateFilter::from(checkpoint)));
                }
            }
            buffer
        };
        let (Some(_parent_statistics), Some(bk_set), Some(future_bk_set)) = parent_block_state
            .guarded(|e| {
                (
                    e.block_stats().clone(),
                    e.descendant_bk_set().clone(),
                    e.descendant_future_bk_set().clone(),
                )
            })
        else {
            tracing::trace!("update_candidate_common_section: Failed to get parent block data");
            return Ok(UpdateCommonSectionResult::NotReadyYet);
        };
        if !bk_set.contains_node(self.node_credentials.node_id()) {
            return Ok(UpdateCommonSectionResult::AbortNotInBKSet);
        }

        let trace_attestations_required = format!("{attestations_required:?}");
        let aggregated_attestations = received_attestations.aggregate(&attestations_required)?;

        let proposed_attestations = aggregated_attestations.as_signatures_map();
        tracing::trace!(
            "update_candidate_common_section ({}):  waiting for finalization: {}, proposed_attestations: {:?}",
            temp_id,
            trace_attestations_required,
            &proposed_attestations,
        );
        tracing::trace!(
            "update_candidate_common_section ({}): dependants: {:?}",
            temp_id,
            attestations_required,
        );
        let mut common_section = candidate_block.common_section().clone();
        common_section.set_block_attestations(aggregated_attestations);

        // TODO: update parent_producer_selector
        // 1 step check if parent has spawned this thread

        let mut producer_selector = self.get_producer_selector(&candidate_block.parent())?;

        // 2 step: update index if we have rotated BP:
        let find_bp = producer_selector.get_producer_node_id(&bk_set);
        if find_bp.is_err() || find_bp.unwrap() != *self.node_credentials.node_id() {
            if let Some(bp_distance_for_this_node) =
                producer_selector.get_distance_from_bp(&bk_set, self.node_credentials.node_id())
            {
                // move index
                producer_selector =
                    producer_selector.move_index(bp_distance_for_this_node, bk_set.len());
            } else {
                // If previous selector was invalid (due to bk removal) generate a new one.
                // Use parent block ID as seed — it is already finalized and stable.
                producer_selector = ProducerSelector::builder()
                    .rng_seed_block_id(candidate_block.parent())
                    .index(0)
                    .build();
                let Some(bp_distance_for_this_node) = producer_selector
                    .get_distance_from_bp(&bk_set, self.node_credentials.node_id())
                else {
                    tracing::trace!("Producer: This node is not in the bk-set");
                    return Ok(UpdateCommonSectionResult::AbortNotInBKSet);
                };
                producer_selector =
                    producer_selector.move_index(bp_distance_for_this_node, bk_set.len());
            }
        }
        common_section.set_producer_selector(Some(producer_selector));

        if should_share_state {
            tracing::trace!(
                "Set share state directive for block {:?} temp_id={}",
                candidate_block.seq_no(),
                temp_id
            );
            let directives = match &common_section {
                CommonSectionVersioned::New(_) => DirectivesVersioned::New(
                    Directives::builder().share_state_resources(true).build(),
                ),
                CommonSectionVersioned::Old(_) => {
                    // Old blocks use TVM hash (stable), but they are retired.
                    // Use parent ID as a stable placeholder.
                    let directives: HashMap<ThreadIdentifier, BlockIdentifier> = HashMap::from_iter(
                        [(common_section.thread_id(), candidate_block.parent())],
                    );
                    DirectivesVersioned::Old(
                        DirectivesOld::builder().share_state_resources(Some(directives)).build(),
                    )
                }
            };
            common_section.set_directives(directives);
        }
        #[cfg(feature = "history_proofs")]
        let Some(parent_block_height) = parent_block_state.guarded(|state| *state.block_height()) else {
            tracing::trace!("update_candidate_common_section: parent block height is missing");
            return Ok(UpdateCommonSectionResult::AncestorIsNotReadyYet);
        };

        candidate_block.set_common_section(common_section, true)?;

        //
        let res = self
            .attestations_target_service
            .evaluate_if_next_block_ancestors_required_attestations_will_be_met(
                self.thread_id,
                candidate_block.parent(),
                proposed_attestations,
            );
        tracing::trace!(
            "evaluate_if_next_block_ancestors_required_attestations_will_be_met: res:{res:?}"
        );
        match res {
            e @ Ok(
                EvaluateIfNextBlockAncestorsRequiredAttestationsWillBeMetSuccess::ThreadSpawn
                | EvaluateIfNextBlockAncestorsRequiredAttestationsWillBeMetSuccess::Passed { .. },
            ) => {
                let passed_checkpoints = match e.unwrap() {
                    EvaluateIfNextBlockAncestorsRequiredAttestationsWillBeMetSuccess::ThreadSpawn => vec![],
                    EvaluateIfNextBlockAncestorsRequiredAttestationsWillBeMetSuccess::Passed{
                        primary,
                        fallback
                    } => primary.iter().chain(fallback.iter()).cloned().collect::<Vec<BlockIdentifier>>(),
                    _ => unreachable!(),
                };

                // why?
                // assert!(!candidate_block.common_section().block_keeper_set_changes.is_empty());

                let (descendant_bk_set, _descendant_future_bk_set) =
                    update_block_keeper_set_from_common_section(
                        candidate_block,
                        bk_set.clone(),
                        future_bk_set.clone(),
                    )?
                    .unwrap_or((bk_set.clone(), future_bk_set.clone()));

                // Pre-compute the BK-set version agreement.  The actual
                // BlockProtocolVersionState is computed later (in on_production_timeout)
                // once the final block identifier is known.
                let all_bk_same_version: Option<ProtocolVersionSupport> = descendant_bk_set
                    .iter()
                    .map(|e| e.protocol_support.clone())
                    .all_elements_same();

                #[cfg(feature = "history_proofs")]
                if *candidate_block.common_section().block_height() % HISTORY_PROOF_WINDOW_SIZE == 0
                    && *candidate_block.common_section().block_height().height() != 0
                    && !self.config_read.is_retired(match &parent_block_version {
                        BlockProtocolVersionState::CompleteTransition { to, .. } => to,
                        other => other.to_use(),
                    })
                {
                    let mut common_section = candidate_block.common_section().clone();
                    let history_proof_data = self.repository.get_history_proof_data();
                    let data_lock = history_proof_data.read();
                    let Some(thread_history) = data_lock.get(&self.thread_id).cloned() else {
                        anyhow::bail!("Thread history can't be empty when block with non zero height is is being produced");
                    };
                    drop(data_lock);
                    let history_lock = thread_history.read();
                    let Some(mut base_layer_data) = history_lock.get(&(0 as LayerNumber)).cloned()
                    else {
                        anyhow::bail!("Thread history can't be empty when block with non zero height is is being produced");
                    };
                    let mut data = vec![];
                    let mut cursor = parent_block_state.clone();
                    if *base_layer_data.data_len() != 0 {
                        for _ in *base_layer_data.data_len()..HISTORY_PROOF_WINDOW_SIZE {
                            let (envelope_hash, parent, block_height) = cursor.guarded(|e| {
                                (
                                    e.envelope_hash().clone().expect("Must be set"),
                                    (*e.parent_block_identifier()).expect("Must be set"),
                                    (*e.block_height()).expect("Must be set"),
                                )
                            });
                            data.push((*cursor.block_identifier(), envelope_hash, block_height));
                            cursor = self.block_state_repository.get(&parent)?;
                        }
                        for (block_id, envelope_hash, block_height) in data.iter().rev() {
                            base_layer_data.update_from_pure_data(
                                block_id.into(),
                                envelope_hash.into(),
                                *block_height,
                            )?;
                        }
                    }
                    let mut prev_layer_root_hash = None;
                    let mut prev_layer_block_height = None;
                    for (layer_num, data) in history_lock.iter() {
                        if *layer_num > 0 {
                            if let Some(height) = prev_layer_block_height.as_ref() {
                                if height > data.last_processed_block_height().height() {
                                    break;
                                }
                            }
                            prev_layer_block_height =
                                Some(*data.last_processed_block_height().height());
                            prev_layer_root_hash = data.data().get(*data.data_len() - 1).cloned();
                        }
                    }

                    let root_hash = base_layer_data.calculate_root_hash(prev_layer_root_hash)?;
                    let mut proofs = BTreeMap::new();
                    let data = ProofLayerRootHash::builder()
                        .layer(1)
                        .root_hash(root_hash)
                        .block_height(parent_block_height)
                        .block_id(candidate_block.identifier())
                        .build();

                    proofs.insert(1 as LayerNumber, data);

                    let additional_layers = common_section
                        .block_height()
                        .height()
                        .ilog(HISTORY_PROOF_WINDOW_SIZE as u64)
                        .saturating_sub(1);
                    let mut height_cursor = *common_section.block_height().height();
                    for i in 0..additional_layers {
                        let layer = (i + 1) as LayerNumber;
                        height_cursor = height_cursor.div(HISTORY_PROOF_WINDOW_SIZE as u64);
                        if height_cursor % HISTORY_PROOF_WINDOW_SIZE as u64 == 0
                            && height_cursor != 0
                        {
                            let Some(mut layer_data) = history_lock.get(&layer).cloned() else {
                                anyhow::bail!("Thread history can't be empty when block with non zero height is is being produced");
                            };
                            let previous_root = *proofs
                                .get(&layer)
                                .ok_or(anyhow::format_err!("Failed to get previous layer data"))?
                                .root_hash();
                            layer_data.update_from_pure_data(
                                previous_root,
                                previous_root,
                                parent_block_height,
                            )?;
                            let mut prev_layer_root_hash = None;
                            let mut prev_layer_block_height = None;
                            for (layer_num, data) in history_lock.iter() {
                                if *layer_num > layer {
                                    if let Some(height) = prev_layer_block_height.as_ref() {
                                        if height > data.last_processed_block_height().height() {
                                            break;
                                        }
                                    }
                                    prev_layer_block_height =
                                        Some(*data.last_processed_block_height().height());
                                    prev_layer_root_hash =
                                        data.data().get(*data.data_len() - 1).cloned();
                                }
                            }

                            let root_hash = layer_data.calculate_root_hash(prev_layer_root_hash)?;
                            let data = ProofLayerRootHash::builder()
                                .layer(layer + 1)
                                .root_hash(root_hash)
                                .block_height(parent_block_height)
                                .block_id(candidate_block.identifier())
                                .build();

                            proofs.insert(layer + 1, data);
                        } else {
                            break;
                        }
                    }
                    common_section.set_history_proofs(proofs);
                    candidate_block.set_common_section(common_section, true)?;
                }
                // if let Some((last_dependant_block_seq_no, last_dependant_block_id)) =
                //     dependent_ancestor_blocks
                //         .dependent_ancestor_blocks()
                //         .iter()
                //         .map(|e| {
                //             e.guarded(|e| {
                //                 (
                //                     (*e.block_seq_no()).expect("Must be set"),
                //                     e.block_identifier().clone(),
                //                 )
                //             })
                //         })
                //         .sorted_by(|a, b| a.0.cmp(&b.0))
                //         .next_back()
                // {
                //     self.clear_old_attestations(
                //         last_dependant_block_id,
                //         last_dependant_block_seq_no,
                //     )?;
                // }
                Ok(UpdateCommonSectionResult::Success {
                    parent_block_version,
                    all_bk_same_version,
                    passed_checkpoints,
                })
            }
            Ok(EvaluateIfNextBlockAncestorsRequiredAttestationsWillBeMetSuccess::SomeFailed) => {
                tracing::trace!(
                    "update_candidate_common_section: Failed to meet ancestors attestation targets"
                );
                Ok(UpdateCommonSectionResult::NotReadyYet)
            }
            Err(UnfinalizedAncestorBlocksSelectError::IncompleteHistory)
            | Err(UnfinalizedAncestorBlocksSelectError::FailedToLoadBlockState) => {
                Ok(UpdateCommonSectionResult::NotReadyYet)
            }
            Err(UnfinalizedAncestorBlocksSelectError::InvalidatedParent(_chain))
            | Err(UnfinalizedAncestorBlocksSelectError::BlockSeqNoCutoff(_chain)) => {
                Ok(UpdateCommonSectionResult::FailedToBuildChainToTheLastFinalizedBlock)
            }
        }
    }

    pub fn get_producer_selector(
        &self,
        parent_block_id: &BlockIdentifier,
    ) -> anyhow::Result<ProducerSelector> {
        // Check if this thread was started from the last finalized block
        if self.thread_id.is_spawning_block(parent_block_id) {
            let selector =
                ProducerSelector::builder().rng_seed_block_id(*parent_block_id).index(0).build();
            Ok(selector)
        } else {
            self.block_state_repository
                .get(parent_block_id)?
                .guarded(|e| e.producer_selector_data().clone())
                .ok_or(anyhow::format_err!("Producer selector must be set for parent block"))
        }
    }

    pub(crate) fn broadcast_candidate_block(
        &self,
        block_id: &BlockIdentifier,
        candidate_block: NetworkMessage,
        mut ext_msg_feedbacks: ExtMsgFeedbackList,
    ) -> anyhow::Result<()> {
        tracing::info!("broadcasting block: {:?}", candidate_block);

        block_flow_trace("broadcasting candidate", block_id, self.node_credentials.node_id(), []);
        match self.broadcast_tx.send(candidate_block) {
            Ok(_) => {}
            _ => {
                if SHUTDOWN_FLAG.get() != Some(&true) {
                    panic!("Failed to broadcast block");
                }
            }
        }

        if !ext_msg_feedbacks.0.is_empty() {
            ext_msg_feedbacks.0.iter_mut().for_each(|feedback| {
                feedback.block_hash = Some(block_id.to_string());
            });
            let _ = self.feedback_sender.send(ext_msg_feedbacks);
        }

        Ok(())
    }

    pub(crate) fn _broadcast_candidate_block_that_was_possibly_produced_by_another_node(
        &self,
        candidate_block: Envelope<AckiNackiBlock>,
    ) -> anyhow::Result<()> {
        tracing::debug!("rebroadcasting block: {}", candidate_block,);
        self.broadcast_tx.send(NetworkMessage::resent_candidate(
            &candidate_block,
            self.node_credentials.node_id().clone(),
        )?)?;
        Ok(())
    }

    fn execute_restarted_producer(
        &mut self,
        _block_id_to_continue: BlockIdentifier,
        _block_seq_no_to_continue: BlockSeqNo,
    ) -> anyhow::Result<SynchronizationResult<NetworkMessage>> {
        Ok(SynchronizationResult::Ok)
        // Note: dirty hack for not to broadcast blocks too often
        // if self.last_broadcasted_produced_candidate_block_time.elapsed()
        // < self.production_timeout.mul(4)
        // {
        // return Ok(SynchronizationResult::Ok);
        // }
        // tracing::info!(
        // "Restarted producer: {} {:?}",
        // block_seq_no_to_continue,
        // block_id_to_continue
        // );
        //
        // Continue BP from the latest applied block
        // Resend blocks from the chosen chain
        // let (finalized_block_id, finalized_block_seq_no) = self
        // .repository
        // .select_thread_last_finalized_block(&self.thread_id)?
        // .ok_or(anyhow::format_err!("Thread was not initialized"))?;
        //
        // let block_state_to_continue = self.block_state_repository.get(&block_id_to_continue)?;
        // let mut chain = self
        // .block_state_repository
        // .select_unfinalized_ancestor_blocks(&block_state_to_continue, finalized_block_seq_no)?;
        //
        // let finalized_block_state = self.block_state_repository.get(&finalized_block_id)?;
        // chain.insert(0, finalized_block_state);
        //
        // let unprocessed_blocks = self.repository.unprocessed_blocks_cache().clone_queue();
        // for block in chain.iter() {
        // block.guarded_mut(|e| e.try_add_attestations_interest(self.node_identifier.clone()))?;
        // let block_id = block.block_identifier();
        // if let Some((_, (_, block))) =
        // unprocessed_blocks.iter().find(|(index, _)| index.block_identifier() == block_id)
        // {
        // self.broadcast_candidate_block_that_was_possibly_produced_by_another_node(
        // block.as_ref().clone(),
        // )?;
        // } else {
        // For default block id there is no block to broadcast, so skip it
        // if block_id != &BlockIdentifier::default() {
        // let block = self.repository.get_block(block_id)?.expect("block must exist");
        // self.broadcast_candidate_block_that_was_possibly_produced_by_another_node(
        // block.as_ref().clone(),
        // )?;
        // }
        // }
        // }
        //
        // Save latest block id and seq_no in cache
        // self.cache_forward_optimistic = Some(OptimisticForwardState::ProducedBlock(
        // block_id_to_continue,
        // block_seq_no_to_continue,
        // ));
        //
        // Ok(SynchronizationResult::Ok)
    }
}
