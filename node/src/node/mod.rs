// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

mod acki_nacki;
pub mod associated_types;
mod block_keeper_system;
mod block_processing;
pub mod block_state;
mod crypto;
use crate::helper::metrics::BlockProductionMetrics;
mod execution;
use block_request_service::BlockRequestParams;
pub use execution::LOOP_PAUSE_DURATION;
use http_server::ExtMsgFeedbackList;
use telemetry_utils::instrumented_channel_ext::XInstrumentedReceiver;
use telemetry_utils::instrumented_channel_ext::XInstrumentedSender;

use crate::utilities::guarded::GuardedMut;
pub mod block_request_service;
pub mod network_message;

mod send;
pub(crate) use send::broadcast_node_joining;
pub mod services;
mod synchronization;
mod threads;
use std::sync::Arc;
#[macro_use]
pub mod impl_trait_macro;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;
use std::time::Duration;

pub use associated_types::SignerIndex;
pub use network_message::NetBlock;
pub use network_message::NetworkMessage;
use services::sync::StateSyncService;
use tvm_types::UInt256;
use typed_builder::TypedBuilder;

use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::services::attestations_target::service::AttestationTargetsService;
use crate::protocol::authority_switch::action_lock::Authority;
pub use crate::protocol::authority_switch::network_message::AuthoritySwitch;
use crate::repository::repository_impl::RepositoryImpl;
use crate::storage::MessageDurableStorage;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::CollectedAttestations;
use crate::types::RndSeed;
use crate::utilities::thread_spawn_critical::SpawnCritical;
pub mod shared_services;
pub mod unprocessed_blocks_collection;
use block_state::repository::BlockStateRepository;
use network::channel::NetBroadcastSender;
use network::channel::NetDirectSender;
use parking_lot::Mutex;
use shared_services::SharedServices;
use telemetry_utils::mpsc::InstrumentedSender;

use crate::block::producer::process::TVMBlockProducerProcess;
use crate::block::producer::ProducerService;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::external_messages::ExternalMessagesThreadState;
use crate::helper::FINALIZATION_LOOPS_COUNTER;
pub use crate::node::associated_types::NodeIdentifier;
use crate::node::block_state::repository::BlockState;
use crate::node::services::block_processor::chain_pulse::events::ChainPulseEvent;
use crate::node::services::block_processor::service::BlockProcessorService;
use crate::node::services::send_attestations::AttestationSendServiceHandler;
use crate::node::services::validation::service::ValidationServiceInterface;
use crate::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::types::ThreadIdentifier;

#[allow(dead_code)]
#[derive(TypedBuilder)]
pub struct Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService,
    TRandomGenerator: rand::Rng,
{
    repository: RepositoryImpl,
    shared_services: SharedServices,
    state_sync_service: TStateSyncService,
    // TODO: @AleksandrS Add priority rx
    network_rx: XInstrumentedReceiver<(NetworkMessage, SocketAddr)>,
    network_broadcast_tx: NetBroadcastSender<NetworkMessage>,
    network_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
    raw_block_tx: InstrumentedSender<(NodeIdentifier, Vec<u8>)>,
    bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
    last_block_attestations: Arc<Mutex<CollectedAttestations>>,
    pub received_acks: Arc<Mutex<Vec<Envelope<GoshBLS, AckData>>>>,
    sent_acks: BTreeMap<BlockSeqNo, Envelope<GoshBLS, AckData>>,
    pub received_nacks: Arc<Mutex<Vec<Envelope<GoshBLS, NackData>>>>,
    config: Config,
    pub received_attestations: BTreeMap<BlockSeqNo, HashMap<BlockIdentifier, HashSet<SignerIndex>>>,
    block_keeper_rng: TRandomGenerator,
    producer_election_rng: TRandomGenerator,
    attestations_to_send: BTreeMap<BlockSeqNo, Vec<Envelope<GoshBLS, AttestationData>>>,
    ack_cache: BTreeMap<BlockSeqNo, Vec<Envelope<GoshBLS, AckData>>>,
    nack_cache: BTreeMap<BlockSeqNo, Vec<Envelope<GoshBLS, NackData>>>,
    thread_id: ThreadIdentifier,
    // Note: hack. check usage
    pub is_spawned_from_node_sync: bool,

    // Note: signer index map is initially empty,
    // it is filled after BK epoch contract deploy or loaded from zerostate
    block_state_repository: BlockStateRepository,
    block_processor_service: BlockProcessorService,
    attestation_send_service: AttestationSendServiceHandler,
    validation_service: ValidationServiceInterface,
    skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
    pub message_db: MessageDurableStorage,

    last_broadcasted_produced_candidate_block_time: std::time::Instant,
    finalization_loop: std::thread::JoinHandle<()>,
    producer_service: ProducerService,
    metrics: Option<BlockProductionMetrics>,
    external_messages: ExternalMessagesThreadState,

    is_state_sync_requested: Arc<Mutex<Option<BlockSeqNo>>>,
    // Channel (sender) for block requests
    blk_req_tx: Sender<BlockRequestParams>,
    ext_msg_receiver: JoinHandle<()>,
    authority_state: Arc<Mutex<Authority>>,
    unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection,
    stop_result_tx: Sender<()>,

    stalled_threads: Arc<Mutex<HashSet<ThreadIdentifier>>>,
    last_synced_state:
        Option<(BlockIdentifier, BlockSeqNo, HashMap<ThreadIdentifier, BlockIdentifier>)>,
    chain_pulse_monitor: Sender<ChainPulseEvent>,

    authority_handler: JoinHandle<()>,
}

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl> + Clone + Send + 'static,
    TRandomGenerator: rand::Rng,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shared_services: SharedServices,
        state_sync_service: TStateSyncService,
        production_process: TVMBlockProducerProcess,
        repository: RepositoryImpl,
        network_rx: XInstrumentedReceiver<(NetworkMessage, SocketAddr)>,
        network_broadcast_tx: NetBroadcastSender<NetworkMessage>,
        network_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
        raw_block_tx: InstrumentedSender<(NodeIdentifier, Vec<u8>)>,
        bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
        config: Config,
        block_keeper_rng: TRandomGenerator,
        producer_election_rng: TRandomGenerator,
        thread_id: ThreadIdentifier,
        feedback_sender: InstrumentedSender<ExtMsgFeedbackList>,
        _update_producer_group: bool,
        block_state_repository: BlockStateRepository,
        block_processor_service: BlockProcessorService,
        attestations_target_service: AttestationTargetsService,
        validation_service: ValidationServiceInterface,
        skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
        metrics: Option<BlockProductionMetrics>,
        self_tx: XInstrumentedSender<(NetworkMessage, SocketAddr)>,
        external_messages: ExternalMessagesThreadState,
        message_db: MessageDurableStorage,
        last_block_attestations: Arc<Mutex<CollectedAttestations>>,
        bp_production_count: Arc<AtomicI32>,
        blk_req_tx: Sender<BlockRequestParams>,
        attestation_send_service: AttestationSendServiceHandler,
        ext_msg_receiver: JoinHandle<()>,
        authority_state: Arc<Mutex<Authority>>,
        unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection,
        stop_result_tx: Sender<()>,
        stalled_threads: Arc<Mutex<HashSet<ThreadIdentifier>>>,
        chain_pulse_monitor: Sender<ChainPulseEvent>,
        authority_handler: JoinHandle<()>,
        self_authority_tx: XInstrumentedSender<(NetworkMessage, SocketAddr)>,
        save_optimistic_service_sender: InstrumentedSender<Arc<OptimisticStateImpl>>,
    ) -> Self {
        tracing::trace!("Start node for thread: {thread_id:?}");
        if let Some(metrics) = &metrics {
            metrics.report_thread_count();
        }

        let (block_producer_control_tx, block_producer_control_rx) = std::sync::mpsc::channel();

        // Note:
        // Let the authority switch do it's job. Do not start block production on restart.
        // It is possible that other nodes have already selected another BP.
        //
        // TODO: this code is bad for graceful restart, node should continue not the last finalized,
        // but the last valid block.
        // if let Ok(Some((last_finalized_block_id, last_finalized_block_seq_no))) = repository.select_thread_last_finalized_block(&thread_id) {
        // let finalized_block_state = block_state_repository.get(&last_finalized_block_id).expect("Should not fail");
        // if let (Some(producer_selector), Some(bk_set), Some(round)) = finalized_block_state.guarded(|e| (e.producer_selector_data().clone(), e.bk_set().clone(), e.round().clone())) {
        // if let Ok(true) = producer_selector.is_node_bp(&bk_set, &config.local.node_id) {
        // tracing::trace!("Sending BP start command: {last_finalized_block_id:?} {last_finalized_block_seq_no:?}");
        // block_producer_control_tx.send(BlockProducerCommand::Start(
        // BlockProducerThreadInitialParameters::builder()
        // .thread_identifier(thread_id.clone())
        // .parent_block_identifier(last_finalized_block_id)
        // .parent_block_seq_no(last_finalized_block_seq_no)
        // .round(round)
        // .build()
        // )).expect("Should not fail");
        // }
        // }
        // }

        authority_state.guarded_mut(|e| e.get_thread_authority(&thread_id)).guarded_mut(|e| {
            e.register_block_producer(block_producer_control_tx);
            e.register_self_node_authority_tx(self_authority_tx.clone());
        });
        let received_acks = Arc::new(Mutex::new(Vec::new()));
        let received_nacks = Arc::new(Mutex::new(Vec::new()));
        let metrics_clone = metrics.clone();
        let is_state_sync_requested = Arc::new(Mutex::new(None));
        let unprocessed_blocks_cache_clone = unprocessed_blocks_cache.clone();
        let last_block_attestations_clone = last_block_attestations.clone();
        let chain_pulse_monitor_clone = chain_pulse_monitor.clone();
        let thread_id_clone = thread_id;
        Self {
            shared_services: shared_services.clone(),
            state_sync_service: state_sync_service.clone(),
            repository: repository.clone(),
            network_rx,
            network_broadcast_tx: network_broadcast_tx.clone(),
            network_direct_tx: network_direct_tx.clone(),
            raw_block_tx: raw_block_tx.clone(),
            bls_keys_map: bls_keys_map.clone(),
            last_block_attestations: last_block_attestations.clone(),
            config: config.clone(),
            received_attestations: Default::default(),
            block_keeper_rng,
            received_acks: received_acks.clone(),
            received_nacks: received_nacks.clone(),
            sent_acks: Default::default(),
            attestations_to_send: Default::default(),
            producer_election_rng,
            ack_cache: Default::default(),
            nack_cache: Default::default(),
            thread_id,
            is_spawned_from_node_sync: false,
            block_state_repository: block_state_repository.clone(),
            block_processor_service,
            validation_service,
            skipped_attestation_ids,
            message_db: message_db.clone(),
            last_broadcasted_produced_candidate_block_time: std::time::Instant::now(),
            finalization_loop: std::thread::Builder::new()
                .name(format!("Block finalization loop {thread_id}"))
                .spawn_critical({
                    let repository_clone = repository.clone();
                    let block_state_repository_clone = block_state_repository.clone();
                    let shared_services_clone = shared_services.clone();
                    let message_db_clone = message_db.clone();
                    let node_id = config.local.node_id.clone();
                    let authority = authority_state.clone();
                    move || {
                        {
                            FINALIZATION_LOOPS_COUNTER.fetch_add(1, Ordering::Relaxed);
                        }
                        crate::node::services::finalization::finalization_loop(
                            repository_clone,
                            block_state_repository_clone,
                            shared_services_clone,
                            raw_block_tx,
                            state_sync_service,
                            metrics_clone,
                            message_db_clone,
                            &node_id,
                            authority,
                            unprocessed_blocks_cache_clone,
                            last_block_attestations_clone,
                            chain_pulse_monitor_clone,
                            thread_id_clone,
                        );
                        {
                            FINALIZATION_LOOPS_COUNTER.fetch_sub(1, Ordering::Relaxed);
                        }
                        Ok(())
                    }
                })
                .expect("Failed to start finalization inner loop"),
            producer_service: ProducerService::start(
                config.network.node_advertise_addr,
                thread_id,
                repository.clone(),
                block_state_repository.clone(),
                block_producer_control_rx,
                production_process,
                feedback_sender.clone(),
                received_acks.clone(),
                received_nacks.clone(),
                shared_services.clone(),
                bls_keys_map.clone(),
                last_block_attestations.clone(),
                attestations_target_service,
                self_tx,
                self_authority_tx,
                network_broadcast_tx,
                config.local.node_id.clone(),
                Duration::from_millis(config.global.time_to_produce_block_millis),
                config.global.save_state_frequency,
                external_messages.clone(),
                is_state_sync_requested.clone(),
                bp_production_count,
                save_optimistic_service_sender,
            )
            .expect("Failed to start producer service"),
            metrics,
            external_messages,
            is_state_sync_requested,
            blk_req_tx,
            attestation_send_service,
            ext_msg_receiver,
            authority_state,
            unprocessed_blocks_cache,
            stop_result_tx,
            stalled_threads,
            last_synced_state: None,
            chain_pulse_monitor,
            authority_handler,
        }
    }
}
