use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Mul;
use std::sync::atomic::AtomicBool;
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
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::config::must_save_state_on_seq_no;
use crate::external_messages::ExternalMessagesThreadState;
use crate::helper::block_flow_trace;
#[cfg(feature = "misbehave")]
use crate::misbehavior::misbehave_rules;
use crate::node::associated_types::AckData;
use crate::node::associated_types::NackData;
use crate::node::associated_types::OptimisticForwardState;
use crate::node::associated_types::SynchronizationResult;
use crate::node::block_state::dependent_ancestor_blocks::DependentAncestorBlocks;
use crate::node::block_state::dependent_ancestor_blocks::DependentBlocks;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocks;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocksSelectError;
use crate::node::services::attestations_target::service::AttestationsTargetService;
use crate::node::services::block_processor::rules;
use crate::node::services::fork_resolution::service::ForkResolutionService;
use crate::node::shared_services::SharedServices;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::node::LOOP_PAUSE_DURATION;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::as_signatures_map::AsSignaturesMap;
use crate::types::bp_selector::BlockGap;
use crate::types::bp_selector::ProducerSelector;
use crate::types::common_section::Directives;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::CollectedAttestations;
use crate::types::ForkResolution;
use crate::types::RndSeed;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[derive(TypedBuilder)]
pub struct BlockProducer {
    thread_id: ThreadIdentifier,
    repository: RepositoryImpl,
    block_state_repository: BlockStateRepository,
    #[builder(default)]
    cache_forward_optimistic: Option<OptimisticForwardState>,
    block_gap: BlockGap,
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
    fork_resolution_service: ForkResolutionService,
    self_tx: Sender<NetworkMessage>,
    broadcast_tx: NetBroadcastSender<NetworkMessage>,

    node_identifier: NodeIdentifier,
    producer_change_gap_size: usize,
    production_timeout: Duration,
    save_state_frequency: u32,

    bp_production_count: Arc<AtomicI32>,
    producing_status: Arc<AtomicBool>,

    external_messages: ExternalMessagesThreadState,
    is_state_sync_requested: Arc<Mutex<Option<BlockSeqNo>>>,
}

#[derive(Debug, PartialEq)]
enum UpdateCommonSectionResult {
    Success,
    FailedToBuildChainToTheLastFinalizedBlock,
    NotReadyYet,
}

impl BlockProducer {
    pub fn main_loop(&mut self) -> anyhow::Result<()> {
        if let Some((block_id_to_continue, block_seq_no_to_continue)) =
            self.find_thread_last_block_id_this_node_can_continue()?
        {
            self.execute_restarted_producer(block_id_to_continue, block_seq_no_to_continue)?;
        }

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
                    let was_producer = self.producing_status.swap(false, Ordering::Relaxed);
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
        if let Some((block_id_to_continue, block_seq_no_to_continue)) =
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
            ) {
                Ok(()) => {
                    tracing::info!("Producer started successfully");
                    let was_producer = self.producing_status.swap(true, Ordering::Relaxed);
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
    ) -> anyhow::Result<Option<(BlockIdentifier, BlockSeqNo)>> {
        tracing::trace!(
            "find_thread_last_block_id_this_node_can_continue start {:?}",
            self.thread_id
        );
        let Some((finalized_block_id, finalized_block_seq_no)) =
            self.repository.select_thread_last_finalized_block(&self.thread_id)?
        else {
            return Ok(None);
        };
        if let Some(OptimisticForwardState::ProducedBlock(
            ref block_id_to_continue,
            block_seq_no_to_continue,
        )) = self.cache_forward_optimistic
        {
            if block_seq_no_to_continue >= finalized_block_seq_no {
                tracing::trace!(
                    "find_thread_last_block_id_this_node_can_continue take from cache: {:?} {:?}",
                    block_seq_no_to_continue,
                    block_id_to_continue
                );
                return Ok(Some((block_id_to_continue.clone(), block_seq_no_to_continue)));
            }
        }
        tracing::trace!("find_thread_last_block_id_this_node_can_continue cache is not valid for continue or empty, clear it");
        self.cache_forward_optimistic = None;

        #[allow(clippy::mutable_key_type)]
        let mut unprocessed_blocks = self.repository.unprocessed_blocks_cache().clone_queue();
        unprocessed_blocks.retain(|_, (block_state, _)| {
            block_state.guarded(|e| {
                if !e.is_invalidated()
                    && !e.is_finalized()
                    && e.is_block_already_applied()
                    && e.descendant_bk_set().is_some()
                {
                    let bk_set = e.get_descendant_bk_set();
                    let node_in_bk_set =
                        bk_set.iter_node_ids().any(|node_id| node_id == &self.node_identifier);
                    let block_seq_no = (*e.block_seq_no()).expect("must be set");
                    node_in_bk_set && block_seq_no > finalized_block_seq_no
                } else {
                    false
                }
            })
        });

        tracing::trace!(
            "find_thread_last_block_id_this_node_can_continue unprocessed applied blocks len: {}",
            unprocessed_blocks.len()
        );
        let mut applied_unprocessed_tails: HashSet<BlockIdentifier> = HashSet::from_iter(
            unprocessed_blocks
                .keys()
                .map(|block_index| block_index.block_identifier())
                .cloned()
                .collect::<Vec<_>>(),
        );
        for (_, (block_state, _)) in unprocessed_blocks.iter() {
            let parent_id = block_state
                .guarded(|e| e.parent_block_identifier().clone())
                .expect("Applied block must have parent id set");
            applied_unprocessed_tails.remove(&parent_id);
        }
        tracing::trace!(
            "find_thread_last_block_id_this_node_can_continue applied unprocessed tails: {:?}",
            applied_unprocessed_tails
        );

        // Filter chains that do not lead to the finalized block
        unprocessed_blocks.retain(|_, (block_state, _)| {
            applied_unprocessed_tails.contains(block_state.block_identifier())
                && self
                    .block_state_repository
                    .select_unfinalized_ancestor_blocks(block_state, finalized_block_seq_no)
                    .is_ok()
        });

        // TODO:
        // REWORK THIS METHOD!
        // unprocessed_blocks.shuffle(&mut rand::thread_rng());

        // Check if one of tails was produced by this node
        for (block_state, _) in unprocessed_blocks.values() {
            if block_state
                .guarded(|e| e.producer().clone())
                .expect("Applied block must have parent id set")
                == self.node_identifier
            {
                let (block_id, block_seq_no) = block_state.guarded(|e| {
                    (e.block_identifier().clone(), (*e.block_seq_no()).expect("must be set"))
                });
                tracing::trace!("find_thread_last_block_id_this_node_can_continue found tail produced by this node seq_no: {} block_id:{:?}", block_seq_no, block_id);
                return Ok(Some((block_id, block_seq_no)));
            }
        }

        // Check is one of tails can be continued by this node based on blocks gap
        let gap = { self.block_gap.load(Ordering::Relaxed) as usize };
        let offset = gap / self.producer_change_gap_size;
        let already_producer_for_n_threads: i32 = self.bp_production_count.load(Ordering::Relaxed);
        let already_producer_for_n_threads: usize = if already_producer_for_n_threads < 0 {
            0_usize
        } else {
            already_producer_for_n_threads as usize
        };

        tracing::trace!(
            "find_thread_last_block_id_this_node_can_continue ({:?}) check with offset={}, already_producer_for_n_threads={}",
            self.thread_id,
            offset,
            already_producer_for_n_threads,
        );

        #[cfg(feature = "misbehave")]
        {
            match misbehave_rules() {
                Ok(Some(rules)) => {
                    for block_state in unprocessed_blocks.clone() {
                        let (block_id, block_seq_no) = block_state.guarded(|e| {
                            (
                                e.block_identifier().clone(),
                                (*e.block_seq_no()).expect("must be set"),
                            )
                        });

                        // Enable misbehaviour if block seqno is in a range
                        let seq_no: u32 = block_seq_no.into();
                        if seq_no >= rules.fork_test.from_seq && seq_no <= rules.fork_test.to_seq {
                            tracing::trace!(
                                "Misbehaving, unprocessed block seq_no: {} block_id:{:?}",
                                block_seq_no,
                                block_id
                            );
                            return Ok(Some((block_id, block_seq_no)));
                        }
                    }
                    let seq_no: u32 = finalized_block_seq_no.into();
                    if seq_no >= rules.fork_test.from_seq && seq_no <= rules.fork_test.to_seq {
                        tracing::trace!(
                            "Misbehaving, finalized block seq_no: {} block_id:{:?}",
                            finalized_block_seq_no,
                            finalized_block_id
                        );
                        return Ok(Some((finalized_block_id, finalized_block_seq_no)));
                    }
                }
                Ok(None) => {
                    // this host should nor apply misbehave rules
                }
                Err(error) => {
                    tracing::error!(
                        "Can not parse misbehaving rules from provided file, fatal error: {:?}",
                        error
                    );
                    std::process::exit(1);
                }
            }
        } //-- end of misbehave block

        for (_, (block_state, _)) in unprocessed_blocks {
            if block_state.guarded(|e| {
                let bk_set = e.get_descendant_bk_set();
                let producer_selector = e.producer_selector_data().clone().expect("must be set");
                if offset < bk_set.len() * already_producer_for_n_threads {
                    return false;
                }
                producer_selector.check_whether_this_node_is_bp_based_on_bk_set_and_index_offset(
                    &bk_set,
                    &self.node_identifier,
                    offset,
                )
            }) {
                let (block_id, block_seq_no) = block_state.guarded(|e| {
                    (e.block_identifier().clone(), (*e.block_seq_no()).expect("must be set"))
                });
                tracing::trace!("find_thread_last_block_id_this_node_can_continue found tail this node can continue base on offset seq_no: {} block_id:{:?}", block_seq_no, block_id);
                return Ok(Some((block_id, block_seq_no)));
            }
        }
        tracing::trace!("find_thread_last_block_id_this_node_can_continue check last finalized block seq_no: {} block_id: {:?}", finalized_block_seq_no, finalized_block_id);
        let block_state = self.block_state_repository.get(&finalized_block_id)?;
        rules::descendant_bk_set::set_descendant_bk_set(&block_state, &self.repository);
        let Some(bk_set) = block_state.guarded(|e| e.descendant_bk_set().clone()) else {
            return Ok(None);
        };
        if offset < bk_set.len() * already_producer_for_n_threads {
            return Ok(None);
        }
        let producer_selector = self.get_producer_selector(&finalized_block_id)?;
        if producer_selector.check_whether_this_node_is_bp_based_on_bk_set_and_index_offset(
            &bk_set,
            &self.node_identifier,
            offset,
        ) {
            tracing::trace!("find_thread_last_block_id_this_node_can_continue node can continue last finalized block");
            return Ok(Some((finalized_block_id, finalized_block_seq_no)));
        }

        Ok(None)
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
            let bk_set = parent_state.guarded(|e| e.get_descendant_bk_set());

            let Some(bk_data) = bk_set.get_by_node_id(&self.node_identifier).cloned() else {
                tracing::trace!("BP is not in the bk set");
                self.production_process.stop_thread_production(&self.thread_id)?;
                return Ok((false, None));
            };
            let secret = self
                .bls_keys_map
                .guarded(|map| map.get(&bk_data.pubkey).cloned())
                .expect("Failed to get signer secret for BP")
                .0;
            let signature = <GoshBLS as BLSSignatureScheme>::sign(&secret, &block)?;
            let mut signature_occurrences = HashMap::new();
            signature_occurrences.insert(bk_data.signer_index, 1);
            let envelope = Envelope::<GoshBLS, AckiNackiBlock>::create(
                signature,
                signature_occurrences,
                block.clone(),
            );

            // Check if this node has already signed block of the same height
            if self.does_block_have_a_valid_sibling(&envelope)? {
                tracing::trace!("Don't accept produced block because this node has already signed a block of the same height");
                self.production_process.stop_thread_production(&self.thread_id)?;
                return Ok((false, None));
            }

            let producer_selector =
                block.get_common_section().producer_selector.clone().expect("Must be set");
            self.block_state_repository.get(&block.identifier())?.guarded_mut(|e| {
                e.set_validated(true)?;
                e.set_producer_selector_data(producer_selector.clone())
            })?;

            self.self_tx.send(NetworkMessage::candidate(&envelope)?)?;

            self.shared_services.metrics.as_ref().inspect(|m| {
                m.report_memento_duration(produced_instant.elapsed().as_millis(), &self.thread_id)
            });

            let block_id = envelope.data().identifier();
            let block_seq_no = envelope.data().seq_no();

            self.production_process
                .write_block_to_db(envelope.clone(), optimistic_state.clone())?;
            tracing::trace!("insert to cache_forward_optimistic {:?} {:?}", block_seq_no, block_id);
            self.cache_forward_optimistic =
                Some(OptimisticForwardState::ProducedBlock(block_id.clone(), block_seq_no));

            self.clear_block_gap();
            // Note: we have already checked parent block BK set, need to check whether this node is
            // in set for descendant blocks
            // if self.is_this_node_in_block_keeper_set(envelope.data().parent()) != Some(true) {
            //     self.production_process.stop_thread_production(&self.thread_id)?;
            // }

            self.last_broadcasted_produced_candidate_block_time = std::time::Instant::now();
            self.broadcast_candidate_block(envelope, ext_msg_feedbacks)?;
            self.repository.store_optimistic_in_cache(optimistic_state)?;
            did_broadcast_something = true;
        }

        Ok((did_broadcast_something, None))
    }

    pub(crate) fn does_block_have_a_valid_sibling(
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

    fn clear_block_gap(&self) {
        tracing::trace!("Clear block gap");
        self.block_gap.store(0, Ordering::Relaxed);
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
        let trace_attestations_required = format!("{:?}", ancestor_blocks_chain);
        let mut attestations_required = ancestor_blocks_chain;
        attestations_required.extend(attestations_watched_for_statistics);
        let mut received_attestations = self.last_block_attestations.guarded(|e| e.clone());
        let aggregated_attestations =
            received_attestations.aggregate(attestations_required, |block_id| {
                let Ok(block_state) = self.block_state_repository.get(block_id) else {
                    return None;
                };
                block_state.guarded(|e| e.bk_set().clone())
            })?;

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
        let fork_resolutions: Vec<ForkResolution> = {
            let siblings = parent_block_state.guarded(|e| {
                let mut children = e.known_children(&self.thread_id).cloned().unwrap_or_default();
                children.remove(&candidate_block.identifier());
                children
            });
            if !siblings.is_empty() {
                // there's already a child exists.
                self.fork_resolution_service.found_fork(parent_block_state.block_identifier())?;
            }

            #[allow(clippy::mutable_key_type)]
            let unprocessed_blocks_cache = self.repository.unprocessed_blocks_cache().clone_queue();
            self.fork_resolution_service.evaluate(&unprocessed_blocks_cache);
            let dependants_and_siblings =
                dependants.iter().fold(HashSet::new(), |aggregated, e| {
                    let siblings = {
                        let state =
                            self.block_state_repository.get(e).expect("Must be able to load");
                        let (parent_block_id, state_thread_identifier) = state.guarded(|x| {
                            (
                                x.parent_block_identifier().clone().expect("Must be set"),
                                x.thread_identifier().expect("Must be set"),
                            )
                        });
                        let parent = self
                            .block_state_repository
                            .get(&parent_block_id)
                            .expect("Must be able to load");
                        parent
                            .guarded(|x| x.known_children(&state_thread_identifier).cloned())
                            .expect("Must be set")
                    };
                    aggregated.union(&siblings).cloned().collect()
                });
            let dependants_aggregated_attestations =
                received_attestations.aggregate(dependants_and_siblings, |block_id| {
                    let Ok(block_state) = self.block_state_repository.get(block_id) else {
                        return None;
                    };
                    block_state.guarded(|e| e.bk_set().clone())
                })?;
            dependants
                .into_iter()
                .filter_map(|e| {
                    self.fork_resolution_service
                        .resolve_fork(e, &dependants_aggregated_attestations)
                })
                .collect()
        };

        let mut common_section = candidate_block.get_common_section().clone();
        common_section.block_attestations = aggregated_attestations;
        common_section.fork_resolutions = fork_resolutions.clone();

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
                fork_resolutions.clone(),
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
        candidate_block: Envelope<GoshBLS, AckiNackiBlock>,
        mut ext_msg_feedbacks: ExtMsgFeedbackList,
    ) -> anyhow::Result<()> {
        let block_id = candidate_block.data().identifier();
        tracing::info!("broadcasting block: {block_id}");

        block_flow_trace("broadcasting candidate", &block_id, &self.node_identifier, []);
        self.broadcast_tx.send(NetworkMessage::candidate(&candidate_block)?)?;

        if !ext_msg_feedbacks.0.is_empty() {
            ext_msg_feedbacks.0.iter_mut().for_each(|feedback| {
                feedback.block_hash = Some(block_id.to_string());
            });
            let _ = self.feedback_sender.send(ext_msg_feedbacks);
        }

        Ok(())
    }

    pub(crate) fn broadcast_candidate_block_that_was_possibly_produced_by_another_node(
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
        block_id_to_continue: BlockIdentifier,
        block_seq_no_to_continue: BlockSeqNo,
    ) -> anyhow::Result<SynchronizationResult<NetworkMessage>> {
        // Note: dirty hack for not to broadcast blocks too often
        if self.last_broadcasted_produced_candidate_block_time.elapsed()
            < self.production_timeout.mul(4)
        {
            return Ok(SynchronizationResult::Ok);
        }
        tracing::info!(
            "Restarted producer: {} {:?}",
            block_seq_no_to_continue,
            block_id_to_continue
        );

        // Continue BP from the latest applied block
        // Resend blocks from the chosen chain
        let (finalized_block_id, finalized_block_seq_no) = self
            .repository
            .select_thread_last_finalized_block(&self.thread_id)?
            .ok_or(anyhow::format_err!("Thread was not initialized"))?;

        let block_state_to_continue = self.block_state_repository.get(&block_id_to_continue)?;
        let mut chain = self
            .block_state_repository
            .select_unfinalized_ancestor_blocks(&block_state_to_continue, finalized_block_seq_no)?;

        let finalized_block_state = self.block_state_repository.get(&finalized_block_id)?;
        chain.insert(0, finalized_block_state);

        let unprocessed_blocks = self.repository.unprocessed_blocks_cache().clone_queue();
        for block in chain.iter() {
            block.guarded_mut(|e| e.try_add_attestations_interest(self.node_identifier.clone()))?;
            let block_id = block.block_identifier();
            if let Some((_, (_, block))) =
                unprocessed_blocks.iter().find(|(index, _)| index.block_identifier() == block_id)
            {
                self.broadcast_candidate_block_that_was_possibly_produced_by_another_node(
                    block.as_ref().clone(),
                )?;
            } else {
                // For default block id there is no block to broadcast, so skip it
                if block_id != &BlockIdentifier::default() {
                    let block = self.repository.get_block(block_id)?.expect("block must exist");
                    self.broadcast_candidate_block_that_was_possibly_produced_by_another_node(
                        block.as_ref().clone(),
                    )?;
                }
            }
        }

        // Save latest block id and seq_no in cache
        self.cache_forward_optimistic = Some(OptimisticForwardState::ProducedBlock(
            block_id_to_continue,
            block_seq_no_to_continue,
        ));

        Ok(SynchronizationResult::Ok)
    }
}
