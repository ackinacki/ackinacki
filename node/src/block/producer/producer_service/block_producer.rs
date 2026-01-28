// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use http_server::ExtMsgFeedbackList;
use network::channel::NetBroadcastSender;
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
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::bls::try_seal::TrySeal;
use crate::bls::GoshBLS;
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
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::optimistic_state::OptimisticStateSaveCommand;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::as_signatures_map::AsSignaturesMap;
use crate::types::bp_selector::ProducerSelector;
use crate::types::common_section::Directives;
use crate::types::AckiNackiBlock;
#[cfg(feature = "transitioning_node_version")]
use crate::types::AckiNackiBlockVersioned;
use crate::types::AggregateFilter;
use crate::types::BlockIdentifier;
use crate::types::BlockRound;
use crate::types::BlockSeqNo;
use crate::types::CollectedAttestations;
use crate::types::RndSeed;
use crate::types::ThreadIdentifier;
use crate::utilities::all_elements_same::AllElementsSame;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::versioning::ProtocolVersion;

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
    received_acks: Arc<Mutex<Vec<Envelope<GoshBLS, AckData>>>>,
    received_nacks: Arc<Mutex<Vec<Envelope<GoshBLS, NackData>>>>,
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

    external_messages: ExternalMessagesThreadState,
    is_state_sync_requested: Arc<Mutex<Option<BlockSeqNo>>>,
    control_rx: std::sync::mpsc::Receiver<BlockProducerCommand>,

    save_optimistic_service_sender: InstrumentedSender<OptimisticStateSaveCommand>,
}

#[derive(Debug, PartialEq)]
enum UpdateCommonSectionResult {
    Success,
    FailedToBuildChainToTheLastFinalizedBlock,
    NotReadyYet,
    AncestorIsNotReadyYet,
    // NotEnoughAttestations(u32),
    AbortNotInBKSet,
}

impl BlockProducer {
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
                let _ = self.production_process.stop_thread_production(&self.thread_id);
                next_bp_command = Some(bp_command);
            }
            if in_flight_productions.is_none() && memento.is_none() {
                in_flight_productions = self.start_production(next_bp_command.take())?;
                if in_flight_productions.is_some() {
                    let (block_id_to_continue, block_seq_no_to_continue) =
                        in_flight_productions.clone().unwrap();
                    if let Err(e) = self.execute_restarted_producer(
                        block_id_to_continue.clone(),
                        block_seq_no_to_continue,
                    ) {
                        tracing::error!(
                            "Failed to restart producer from {} {:?}: {e}",
                            block_id_to_continue,
                            block_seq_no_to_continue
                        );
                        let e = self.production_process.stop_thread_production(&self.thread_id);
                        tracing::trace!("Production stop result: {e:?}");
                        in_flight_productions = None;
                    }
                }
                if in_flight_productions.is_none() {
                    // Reset thread load
                    let was_producer = self.producing_status;
                    self.producing_status = false;
                    if was_producer {
                        self.bp_production_count.fetch_sub(1, Ordering::Relaxed);
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
                    if !was_producer {
                        self.bp_production_count.fetch_add(1, Ordering::Relaxed);
                    }
                    producer_tails = Some((block_id_to_continue.clone(), block_seq_no_to_continue));
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
        self.production_process.stop_thread_production(&self.thread_id)?;
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
            if let Some((block_id, block_seq_no)) = production_status.last_produced.clone() {
                (block_id, block_seq_no, 0)
            } else {
                (
                    production_status.init_params.parent_block_identifier().clone(),
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
            block_flow_trace(
                "check produced",
                &block.identifier(),
                self.node_credentials.node_id(),
                [],
            );
            tracing::info!(
                "Got block from producer. id: {:?}; seq_no: {:?}, parent: {:?}",
                block.identifier(),
                block.seq_no(),
                block.parent(),
            );

            if block.seq_no() <= minimal_seq_no_that_can_be_accepted_from_producer_process {
                tracing::trace!("Produced block is older than last finalized block. Stop production for thread: {:?}", self.thread_id);
                self.production_process.stop_thread_production(&self.thread_id)?;
                *producer_tails = None;
                return Ok((false, None));
            }
            let share_resulting_state = self.is_state_sync_requested.guarded(|e| *e);
            let share_state_ids = if share_resulting_state == Some(block.seq_no())
                || (<u32>::from(block.seq_no()) % FORCE_SYNC_STATE_BLOCK_FREQUENCY == 0)
            {
                tracing::trace!("Node should share state for last block");
                Some(produced_block.optimistic_state().get_share_stare_refs())
            } else {
                None
            };
            let block_will_share_state = share_state_ids.is_some();
            let attestations_notifications_stamp: u32 = {
                let (lock, _cvar) =
                    &*self.last_block_attestations.guarded(|e| e.notifications().clone());
                let n = *lock.lock();
                n
            };
            let update_result = self.update_candidate_common_section(
                &mut block,
                share_state_ids,
                produced_block.optimistic_state(),
            );
            let produced_block = match update_result {
                Ok(UpdateCommonSectionResult::FailedToBuildChainToTheLastFinalizedBlock) => {
                    tracing::trace!("Failed to update common section and our block seems to be invalidated, stop production");
                    self.production_process.stop_thread_production(&self.thread_id)?;
                    *producer_tails = None;
                    return Ok((false, None));
                }
                Err(e) => {
                    tracing::trace!("Update common section error {e:?}");
                    self.production_process.stop_thread_production(&self.thread_id)?;
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
                Ok(UpdateCommonSectionResult::Success) => {
                    let actual_required_version = produced_block
                        .block_state()
                        .guarded(|e| e.block_version_state().clone())
                        .unwrap()
                        .to_use()
                        .clone();
                    let actual_assumptions = Assumptions::builder()
                        .new_to_bk_set(
                            BTreeSet::new(), // TODO: set real value with preattestaions implementation
                        )
                        .block_version(actual_required_version.clone())
                        .build();
                    let is_assumptions_correct =
                        produced_block.assumptions().eq(&actual_assumptions);
                    if is_assumptions_correct {
                        tracing::trace!("Speed up production");
                        if let Some(seq_no) = share_resulting_state {
                            if seq_no <= block.seq_no() {
                                self.is_state_sync_requested.guarded_mut(|e| *e = None);
                            }
                        }
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
                        let block_round = block.get_common_section().round();
                        *producer_tails = self.restart_production(
                            parent_block_id,
                            block_round,
                            actual_required_version,
                        )?;
                        return Ok((false, None));
                    }
                }
                Ok(UpdateCommonSectionResult::AbortNotInBKSet) => {
                    tracing::trace!("Stop production: not in bk set");
                    self.production_process.stop_thread_production(&self.thread_id)?;
                    *producer_tails = None;
                    return Ok((false, None));
                }
            };

            #[cfg(feature = "rotate_after_sync")]
            {
                if block.get_common_section().directives().share_state_resources().is_some() {
                    tracing::trace!("Save share state block seq_no: {:?}", block.seq_no());
                    let mut shared_state_seq_no = SHARED_STATE_SEQ_NO.lock();
                    *shared_state_seq_no = Some(block.seq_no());
                }
                if let Some(seq_no) = SHARED_STATE_SEQ_NO.lock().as_ref() {
                    if <u32>::from(seq_no.clone()) + 100 == <u32>::from(block.seq_no().clone()) {
                        self.production_process.stop_thread_production(&self.thread_id)?;
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
                    self.production_process.stop_thread_production(&self.thread_id)?;
                    return Ok((false, None));
                }
                notifications.wait_for_updates(stamp);
            }
            tracing::trace!("Got parent bk set");
            let bk_set = parent_state.guarded(|e| e.descendant_bk_set().clone().unwrap());

            let secrets = self.bls_keys_map.guarded(|map| map.clone());
            let production_status = self.production_status.as_ref().expect("must be in prod");
            let protocol_version = produced_block
                .block_state()
                .guarded(|e| e.block_version_state().clone())
                .unwrap()
                .to_use()
                .clone();

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
            // Check if this node has already signed block of the same height
            // Note: Not valid anymore. Can't sign blocks of the same round though.
            // TODO: add corrected check.
            // if self.does_block_have_a_valid_sibling(&envelope)? {
            // tracing::trace!("Don't accept produced block because this node has already signed a block of the same height");
            // self.production_process.stop_thread_production(&self.thread_id)?;
            // return Ok((false, None));
            // }

            let producer_selector =
                block.get_common_section().producer_selector().expect("Must be set");
            let parent_height = parent_state
                .guarded(|e| *e.block_height())
                .expect("Parent block height must be set");
            produced_block.block_state().guarded_mut(|e| {
                e.set_validated(true)?;
                e.set_producer_selector_data(producer_selector)?;
                e.set_block_height(parent_height.next(&self.thread_id))
            })?;

            let (send_res, net_message) = {
                if production_status.last_produced.is_none()
                    && *production_status.init_params.round() != 0
                {
                    let block_height =
                        produced_block.block_state().guarded(|e| (*e.block_height()).unwrap());
                    let message =
                        NetworkMessage::AuthoritySwitchProtocol(AuthoritySwitch::Switched(
                            NextRoundSuccess::builder()
                                .node_identifier(self.node_credentials.node_id().clone())
                                .round(*production_status.init_params.round())
                                .block_height(block_height)
                                .proposed_block(NetBlock::with_envelope(&envelope)?)
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
                    let message = NetworkMessage::Candidate(NetBlock::with_envelope(&envelope)?);
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
                production_status.last_produced = Some((block_id.clone(), block_seq_no));
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
        candidate_block: &Envelope<GoshBLS, AckiNackiBlock>,
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
        candidate_block: &mut AckiNackiBlock,
        share_state_ids: Option<HashMap<ThreadIdentifier, BlockIdentifier>>,
        optimistic_state: &OptimisticStateImpl,
    ) -> anyhow::Result<UpdateCommonSectionResult> {
        tracing::trace!(
            "update_candidate_common_section: share_state {} block_seq_no: {:?}, id: {:?}",
            share_state_ids.is_some(),
            candidate_block.seq_no(),
            candidate_block.identifier()
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
            candidate_block.identifier(),
            trace_attestations_required,
            &proposed_attestations,
        );
        tracing::trace!(
            "update_candidate_common_section ({:?}): dependants: {:?}",
            candidate_block.identifier(),
            attestations_required,
        );
        let mut common_section = candidate_block.get_common_section().clone();
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
                // If previous selector was invalid (due to bk removal) generate a new one
                producer_selector = ProducerSelector::builder()
                    .rng_seed_block_id(candidate_block.identifier().clone())
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

        if let Some(resource_address) = share_state_ids {
            let directive = resource_address;
            tracing::trace!(
                "Set share state directive for block {:?} {:?}: {directive:?}",
                candidate_block.seq_no(),
                candidate_block.identifier()
            );
            common_section.set_directives(
                Directives::builder().share_state_resources(Some(directive)).build(),
            );
            common_section
                .set_threads_table(Some(optimistic_state.get_produced_threads_table().clone()));
        }

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
                // assert!(!candidate_block.get_common_section().block_keeper_set_changes.is_empty());

                let (descendant_bk_set, _descendant_future_bk_set) =
                    update_block_keeper_set_from_common_section(
                        candidate_block,
                        bk_set.clone(),
                        future_bk_set.clone(),
                    )?
                    .unwrap_or((bk_set.clone(), future_bk_set.clone()));

                let attestations = candidate_block.get_common_section().block_attestations();
                let block_state = self.block_state_repository.get(&candidate_block.identifier())?;
                block_state.guarded_mut(|e| {
                    for attestation in attestations {
                        e.add_verified_attestations_for(
                            attestation.data().block_id().clone(),
                            *attestation.data().target_type(),
                            HashSet::from_iter(
                                attestation.clone_signature_occurrences().keys().cloned(),
                            ),
                        )?;
                    }
                    e.set_block_version_state(
                        parent_block_version.next(
                            candidate_block.identifier(),
                            descendant_bk_set
                                .iter()
                                .map(|e| e.protocol_support.clone())
                                .all_elements_same(),
                            &passed_checkpoints,
                        ),
                    )?;
                    Ok::<(), anyhow::Error>(())
                })?;

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
                Ok(UpdateCommonSectionResult::Success)
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
            let selector = ProducerSelector::builder()
                .rng_seed_block_id(parent_block_id.clone())
                .index(0)
                .build();
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
        candidate_block: Envelope<GoshBLS, AckiNackiBlock>,
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
