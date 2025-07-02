use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use std::time::Instant;

use http_server::ExtMsgFeedbackList;
use itertools::Itertools;
use network::channel::NetBroadcastSender;
use parking_lot::Mutex;
use telemetry_utils::mpsc::InstrumentedSender;
use typed_builder::TypedBuilder;

use crate::block::producer::process::TVMBlockProducerProcess;
use crate::bls::create_signed::CreateSealed;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::bls::GoshBLS;
use crate::config::must_save_state_on_seq_no;
use crate::external_messages::ExternalMessagesThreadState;
use crate::helper::block_flow_trace;
#[cfg(feature = "misbehave")]
use crate::misbehavior::misbehave_rules;
use crate::node::associated_types::AckData;
use crate::node::associated_types::NackData;
use crate::node::associated_types::SynchronizationResult;
use crate::node::block_state::dependent_ancestor_blocks::DependentAncestorBlocks;
use crate::node::block_state::dependent_ancestor_blocks::DependentBlocks;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocksSelectError;
use crate::node::services::attestations_target::service::AttestationsTargetService;
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
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::as_signatures_map::AsSignaturesMap;
use crate::types::bp_selector::ProducerSelector;
use crate::types::common_section::Directives;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::CollectedAttestations;
use crate::types::RndSeed;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[derive(Debug)]
struct ProductionStatus {
    init_params: StartBlockProducerThreadInitialParameters,
    last_produced: Option<(BlockIdentifier, BlockSeqNo)>,
}

#[derive(TypedBuilder)]
pub struct BlockProducer {
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
    attestations_target_service: AttestationsTargetService,
    self_tx: Sender<NetworkMessage>,
    broadcast_tx: NetBroadcastSender<NetworkMessage>,

    node_identifier: NodeIdentifier,
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
}

#[derive(Debug, PartialEq)]
enum UpdateCommonSectionResult {
    Success,
    FailedToBuildChainToTheLastFinalizedBlock,
    NotReadyYet,
}

impl BlockProducer {
    pub fn main_loop(&mut self) -> anyhow::Result<()> {
        // if let Some((block_id_to_continue, block_seq_no_to_continue)) =
        // self.find_thread_last_block_id_this_node_can_continue()?
        // {
        // self.execute_restarted_producer(block_id_to_continue, block_seq_no_to_continue)?;
        // }

        let mut in_flight_productions = self.start_production()?;
        let mut memento = None;
        loop {
            if in_flight_productions.is_none() && memento.is_none() {
                in_flight_productions = self.start_production()?;
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
                    tracing::info!("Cut off block producer");

                    // Cut off block producer. Send whatever it has
                    let post_production_timestamp = std::time::Instant::now();
                    let (broadcasted, next_memento) =
                        self.on_production_timeout(&mut in_flight_productions, memento.clone())?;
                    let cleared_memento = next_memento.is_none() && memento.is_some();
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

    pub fn start_production(&mut self) -> anyhow::Result<Option<(BlockIdentifier, BlockSeqNo)>> {
        tracing::trace!("start_block_production");
        // Produce whatever threads it has to produce
        let mut producer_tails = None;
        if let Some((block_id_to_continue, block_seq_no_to_continue, initial_round)) =
            self.find_thread_last_block_id_this_node_can_continue()?
        {
            tracing::info!(
                "Requesting producer to continue block id {:?}; seq_no {:?}, node_id {:?}",
                block_id_to_continue,
                block_seq_no_to_continue,
                self.node_identifier,
            );
            match self.production_process.start_thread_production(
                &self.thread_id,
                &block_id_to_continue,
                self.received_acks.clone(),
                self.received_nacks.clone(),
                self.block_state_repository.clone(),
                self.external_messages.clone(),
                self.is_state_sync_requested.clone(),
                initial_round,
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

    pub(crate) fn find_thread_last_block_id_this_node_can_continue(
        &mut self,
    ) -> anyhow::Result<Option<(BlockIdentifier, BlockSeqNo, u16)>> {
        tracing::trace!(
            "find_thread_last_block_id_this_node_can_continue start {:?}",
            self.thread_id
        );
        let mut update = None;
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
                    unimplemented!();
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
                unimplemented!();
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
        let production_status = self.production_status.as_ref().expect("it must be set by now");
        if let Some((block_id, block_seq_no)) = production_status.last_produced.clone() {
            Ok(Some((block_id, block_seq_no, 0)))
        } else {
            Ok(Some((
                production_status.init_params.parent_block_identifier().clone(),
                *production_status.init_params.parent_block_seq_no(),
                *production_status.init_params.round(),
            )))
        }
    }

    #[allow(clippy::needless_range_loop)]
    pub(crate) fn on_production_timeout(
        &mut self,
        producer_tails: &mut Option<(BlockIdentifier, BlockSeqNo)>,
        mut memento: Option<(
            Vec<(AckiNackiBlock, OptimisticStateImpl, ExtMsgFeedbackList, Instant)>,
            Option<DependentBlocks>,
        )>,
    ) -> anyhow::Result<(
        bool,
        Option<(
            Vec<(AckiNackiBlock, OptimisticStateImpl, ExtMsgFeedbackList, Instant)>,
            Option<DependentBlocks>,
        )>,
    )> {
        tracing::trace!("on_production_timeout start");
        let mut did_broadcast_something = false;

        let (mut produced_data, mut dependent_blocks) =
            memento.take().unwrap_or_else(|| (self.production_process.get_produced_blocks(), None));

        produced_data.sort_by(|a, b| a.0.seq_no().cmp(&b.0.seq_no()));

        // While producer process was generating blocks, the node could receive valid
        // blocks, and the generated blocks could be discarded.
        // Get minimal block seq no that can be accepted from producer process.
        let minimal_seq_no_that_can_be_accepted_from_producer_process = self
            .repository
            .select_thread_last_finalized_block(&self.thread_id)?
            .unwrap_or_default()
            .1;
        tracing::trace!("on_production_timeout: minimal_seq_no_that_can_be_accepted_from_producer_process = {minimal_seq_no_that_can_be_accepted_from_producer_process:?}");
        while !produced_data.is_empty() {
            let (mut block, optimistic_state, ext_msg_feedbacks, produced_instant) =
                produced_data.first().unwrap().clone();
            block_flow_trace("check produced", &block.identifier(), &self.node_identifier, []);
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
            self.shared_services.on_block_appended(&block);
            let share_resulting_state = self.is_state_sync_requested.guarded(|e| *e);
            let share_state_ids = if let Some(seq_no) = &share_resulting_state {
                if seq_no == &block.seq_no() {
                    tracing::trace!("Node should share state for last block");
                    Some(optimistic_state.get_share_stare_refs())
                } else {
                    None
                }
            } else {
                None
            };
            let block_will_share_state = share_state_ids.is_some();
            let update_result = self.update_candidate_common_section(
                &mut block,
                share_state_ids,
                &optimistic_state,
                dependent_blocks.take(),
            );
            dependent_blocks =
                update_result.as_ref().map(|(_res, cache)| cache.clone()).ok().flatten();
            match update_result.map(|(res, _cache)| res) {
                Ok(UpdateCommonSectionResult::FailedToBuildChainToTheLastFinalizedBlock) => {
                    tracing::trace!("Failed to update common section and our block seems to be invalidated, stop production");
                    self.production_process.stop_thread_production(&self.thread_id)?;
                    *producer_tails = None;
                    return Ok((false, None));
                }
                Err(e) => {
                    tracing::trace!("Update common section error {e:?}");
                    self.production_process.stop_thread_production(&self.thread_id)?;
                    self.repository.store_optimistic(optimistic_state)?;
                    *producer_tails = None;
                    return Ok((false, Some((produced_data, dependent_blocks))));
                }
                Ok(UpdateCommonSectionResult::NotReadyYet) => {
                    // Memento
                    tracing::trace!("Failed to update common section, stop production");
                    // self.production_process.stop_thread_production(&self.thread_id)?;
                    // self.repository.store_optimistic(optimistic_state)?;
                    // *producer_tails = None;
                    return Ok((false, Some((produced_data, dependent_blocks))));
                }
                Ok(UpdateCommonSectionResult::Success) => {
                    tracing::trace!("Speed up production");
                    self.production_process.set_timeout(self.production_timeout);
                    produced_data.remove(0);
                    if let Some(seq_no) = share_resulting_state {
                        if seq_no <= block.seq_no() {
                            self.is_state_sync_requested.guarded_mut(|e| *e = None);
                        }
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
                self.repository.store_optimistic(optimistic_state.clone())?;
            }

            // NOTE: Issue here: This blocks accepting new blocks!
            loop {
                tracing::trace!("loop get parent bk set");
                let notifications =
                    self.block_state_repository.notifications().load(Ordering::Relaxed);
                if parent_state.guarded(|e| e.descendant_bk_set().is_some()) {
                    break;
                }
                if parent_state.guarded(|e| e.is_invalidated()) {
                    // Stop production in this case
                    tracing::trace!("parent block state was invalidated");
                    self.production_process.stop_thread_production(&self.thread_id)?;
                    return Ok((false, None));
                }
                atomic_wait::wait(self.block_state_repository.notifications(), notifications);
            }
            tracing::trace!("Got parent bk set");
            let bk_set = parent_state.guarded(|e| e.descendant_bk_set().clone().unwrap());

            let secrets = self.bls_keys_map.guarded(|map| map.clone());
            let production_status = self.production_status.as_ref().expect("must be in prod");
            let envelope = Envelope::<GoshBLS, AckiNackiBlock>::sealed(
                &self.node_identifier,
                &bk_set,
                &secrets,
                block.clone(),
            )?;

            // Check if this node has already signed block of the same height
            // Note: Not valid anymore. Can't sign blocks of the same round though.
            // TODO: add corrected check.
            // if self.does_block_have_a_valid_sibling(&envelope)? {
            // tracing::trace!("Don't accept produced block because this node has already signed a block of the same height");
            // self.production_process.stop_thread_production(&self.thread_id)?;
            // return Ok((false, None));
            // }

            let producer_selector =
                block.get_common_section().producer_selector.clone().expect("Must be set");
            let parent_height = parent_state
                .guarded(|e| *e.block_height())
                .expect("Parent block height must be set");
            self.block_state_repository.get(&block.identifier())?.guarded_mut(|e| {
                e.set_validated(true)?;
                e.set_producer_selector_data(producer_selector.clone())?;
                e.set_block_height(parent_height.next(&self.thread_id))
            })?;

            let net_message = {
                if production_status.last_produced.is_none()
                    && *production_status.init_params.round() != 0
                {
                    let block_id = envelope.data().identifier();
                    let block_height = self
                        .block_state_repository
                        .get(&block_id)
                        .unwrap()
                        .guarded(|e| (*e.block_height()).unwrap());
                    NetworkMessage::AuthoritySwitchProtocol(AuthoritySwitch::Switched(
                        Envelope::sealed(
                            &self.node_identifier,
                            &bk_set,
                            &secrets,
                            NextRoundSuccess::builder()
                                .node_identifier(self.node_identifier.clone())
                                .round(*production_status.init_params.round())
                                .block_height(block_height)
                                .proposed_block(NetBlock::with_envelope(&envelope)?)
                                .attestations_aggregated(None)
                                // TODO: Must include a proof!
                                .requests_aggregated(vec![])
                                .build(),
                        )
                        .expect("must work"),
                    ))
                } else {
                    NetworkMessage::candidate(&envelope)?
                }
            };
            self.self_tx.send(net_message.clone())?;
            self.shared_services.metrics.as_ref().inspect(|m| {
                m.report_memento_duration(produced_instant.elapsed().as_millis(), &self.thread_id)
            });

            let block_id = envelope.data().identifier();
            let block_seq_no = envelope.data().seq_no();

            self.production_process
                .write_block_to_db(envelope.clone(), optimistic_state.clone())?;
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
            self.broadcast_candidate_block(&block_id, net_message, ext_msg_feedbacks)?;
            self.repository.store_optimistic_in_cache(optimistic_state)?;
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
        dependent_ancestor_blocks: Option<DependentBlocks>,
    ) -> anyhow::Result<(UpdateCommonSectionResult, Option<DependentBlocks>)> {
        tracing::trace!(
            "update_candidate_common_section: share_state {} block_seq_no: {:?}, id: {:?}",
            share_state_ids.is_some(),
            candidate_block.seq_no(),
            candidate_block.identifier()
        );

        let parent_block_state = self.block_state_repository.get(&candidate_block.parent())?;
        use UnfinalizedAncestorBlocksSelectError::*;
        let dependent_ancestor_blocks = match dependent_ancestor_blocks {
            Some(v) => v,
            None => {
                match self
                    .block_state_repository
                    .select_dependent_ancestor_blocks(&parent_block_state)
                {
                    Ok(blocks) => blocks,
                    Err(IncompleteHistory) => {
                        tracing::trace!(
                            "update_candidate_common_section: Failed to build unfinalized chain: Incomplete history"
                        );
                        return Ok((UpdateCommonSectionResult::NotReadyYet, None));
                    }
                    Err(e) => {
                        tracing::trace!(
                            "update_candidate_common_section: Failed to build unfinalized chain: {e}"
                        );
                        return Ok((
                            UpdateCommonSectionResult::FailedToBuildChainToTheLastFinalizedBlock,
                            None,
                        ));
                    }
                }
            }
        };
        let (Some(parent_statistics), Some(bk_set)) = parent_block_state
            .guarded(|e| (e.block_stats().clone(), e.descendant_bk_set().clone()))
        else {
            tracing::trace!("update_candidate_common_section: Failed to get parent block data");
            return Ok((UpdateCommonSectionResult::NotReadyYet, Some(dependent_ancestor_blocks)));
        };
        let attestations_watched_for_statistics = parent_statistics.attestations_watched();

        let ancestor_blocks_chain = HashSet::from_iter(
            dependent_ancestor_blocks
                .dependent_ancestor_chain()
                .iter()
                .map(|e| e.block_identifier().clone()),
        );
        let trace_attestations_required = format!("{ancestor_blocks_chain:?}");
        let mut attestations_required = ancestor_blocks_chain;
        attestations_required.extend(attestations_watched_for_statistics);
        let mut received_attestations = self.last_block_attestations.guarded(|e| e.clone());
        let aggregated_attestations = received_attestations.aggregate(
            attestations_required,
            |block_id| {
                let Ok(block_state) = self.block_state_repository.get(block_id) else {
                    return None;
                };
                block_state.guarded(|e| e.bk_set().clone())
            },
            |block_id| {
                let Ok(block_state) = self.block_state_repository.get(block_id) else {
                    return None;
                };
                block_state.guarded(|e| e.envelope_hash().clone())
            },
        )?;

        let proposed_attestations = aggregated_attestations.as_signatures_map();
        tracing::trace!(
            "update_candidate_common_section ({}):  waiting for finalization: {}, proposed_attestations: {:?}",
            candidate_block.identifier(),
            trace_attestations_required,
            &proposed_attestations,
        );
        let dependants = dependent_ancestor_blocks
            .dependent_ancestor_blocks()
            .iter()
            .map(|e| e.block_identifier().clone())
            .collect::<Vec<_>>();
        tracing::trace!(
            "update_candidate_common_section ({:?}): dependants: {:?}",
            candidate_block.identifier(),
            dependants,
        );
        let mut common_section = candidate_block.get_common_section().clone();
        common_section.block_attestations = aggregated_attestations;

        // TODO: update parent_producer_selector
        // 1 step check if parent has spawned this thread

        let mut producer_selector = self.get_producer_selector(&candidate_block.parent())?;

        // 2 step: update index if we have rotated BP:
        let find_bp = producer_selector.get_producer_node_id(&bk_set);
        if find_bp.is_err() || find_bp.unwrap() != self.node_identifier {
            if let Some(bp_distance_for_this_node) =
                producer_selector.get_distance_from_bp(&bk_set, &self.node_identifier)
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
                let bp_distance_for_this_node = producer_selector
                    .get_distance_from_bp(&bk_set, &self.node_identifier)
                    .expect("Must be able to find bp_distance_for_this_node");
                producer_selector =
                    producer_selector.move_index(bp_distance_for_this_node, bk_set.len());
            }
        }
        common_section.producer_selector = Some(producer_selector);

        if let Some(resource_address) = share_state_ids {
            let directive = resource_address;
            tracing::trace!(
                "Set share state directive for block {:?} {:?}: {directive:?}",
                candidate_block.seq_no(),
                candidate_block.identifier()
            );
            common_section.directives =
                Directives::builder().share_state_resources(Some(directive)).build();
            common_section.threads_table =
                Some(optimistic_state.get_produced_threads_table().clone());
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
            Ok(true) => {
                let attestations = candidate_block.get_common_section().block_attestations.clone();
                let block_state = self.block_state_repository.get(&candidate_block.identifier())?;
                block_state.guarded_mut(|e| {
                    for attestation in attestations {
                        e.add_verified_attestations_for(
                            attestation.data().block_id().clone(),
                            HashSet::from_iter(
                                attestation.clone_signature_occurrences().keys().cloned(),
                            ),
                        )?;
                    }
                    Ok::<(), anyhow::Error>(())
                })?;
                if let Some((last_dependant_block_seq_no, last_dependant_block_id)) =
                    dependent_ancestor_blocks
                        .dependent_ancestor_blocks()
                        .iter()
                        .map(|e| {
                            e.guarded(|e| {
                                (
                                    (*e.block_seq_no()).expect("Must be set"),
                                    e.block_identifier().clone(),
                                )
                            })
                        })
                        .sorted_by(|a, b| a.0.cmp(&b.0))
                        .next_back()
                {
                    self.clear_old_attestations(
                        last_dependant_block_id,
                        last_dependant_block_seq_no,
                    )?;
                }
                Ok((UpdateCommonSectionResult::Success, None))
            }
            Ok(false) => {
                tracing::trace!(
                    "update_candidate_common_section: Failed to meet ancestors attestation targets"
                );
                Ok((UpdateCommonSectionResult::NotReadyYet, Some(dependent_ancestor_blocks)))
            }
            Err(UnfinalizedAncestorBlocksSelectError::IncompleteHistory)
            | Err(UnfinalizedAncestorBlocksSelectError::FailedToLoadBlockState) => {
                Ok((UpdateCommonSectionResult::NotReadyYet, Some(dependent_ancestor_blocks)))
            }
            Err(UnfinalizedAncestorBlocksSelectError::InvalidatedParent(_chain))
            | Err(UnfinalizedAncestorBlocksSelectError::BlockSeqNoCutoff(_chain)) => Ok((
                UpdateCommonSectionResult::FailedToBuildChainToTheLastFinalizedBlock,
                Some(dependent_ancestor_blocks),
            )),
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

    fn clear_old_attestations(
        &mut self,
        last_block_id: BlockIdentifier,
        last_block_seq_no: BlockSeqNo,
    ) -> anyhow::Result<()> {
        self.last_block_attestations
            .guarded_mut(|e| e.move_cutoff(last_block_seq_no, last_block_id));
        Ok(())
    }

    pub(crate) fn broadcast_candidate_block(
        &self,
        block_id: &BlockIdentifier,
        candidate_block: NetworkMessage,
        mut ext_msg_feedbacks: ExtMsgFeedbackList,
    ) -> anyhow::Result<()> {
        tracing::info!("broadcasting block: {block_id}");

        block_flow_trace("broadcasting candidate", block_id, &self.node_identifier, []);
        self.broadcast_tx.send(candidate_block)?;

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
        tracing::info!("rebroadcasting block: {}", candidate_block,);
        self.broadcast_tx.send(NetworkMessage::resent_candidate(
            &candidate_block,
            self.node_identifier.clone(),
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
