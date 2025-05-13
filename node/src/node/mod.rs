// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
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
pub mod block_request_service;
use tvm_types::AccountId;
mod network_message;

mod restart;
mod send;
pub mod services;
mod synchronization;
mod threads;
use std::sync::Arc;
#[macro_use]
pub mod impl_trait_macro;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicI32;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
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
use crate::message_storage::MessageDurableStorage;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::services::attestations_target::service::AttestationsTargetService;
use crate::repository::repository_impl::RepositoryImpl;
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
pub use crate::node::associated_types::NodeIdentifier;
use crate::node::block_state::repository::BlockState;
use crate::node::services::block_processor::service::BlockProcessorService;
use crate::node::services::db_serializer::DBSerializeService;
use crate::node::services::fork_resolution::service::ForkResolutionService;
use crate::node::services::validation::service::ValidationServiceInterface;
use crate::types::bp_selector::BlockGap;
use crate::types::ThreadIdentifier;

pub(crate) const DEFAULT_PRODUCTION_TIME_MULTIPLIER: u64 = 1;

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
    network_rx: Receiver<NetworkMessage>,
    network_broadcast_tx: NetBroadcastSender<NetworkMessage>,
    network_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
    raw_block_tx: InstrumentedSender<(AccountId, Vec<u8>)>,
    bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,

    production_timeout_multiplier: u64,
    last_block_attestations: Arc<Mutex<CollectedAttestations>>,
    pub received_acks: Arc<Mutex<Vec<Envelope<GoshBLS, AckData>>>>,
    sent_acks: BTreeMap<BlockSeqNo, Envelope<GoshBLS, AckData>>,
    pub received_nacks: Arc<Mutex<Vec<Envelope<GoshBLS, NackData>>>>,
    config: Config,
    block_gap_length: BlockGap,
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
    validation_service: ValidationServiceInterface,
    skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
    pub message_db: MessageDurableStorage,

    last_broadcasted_produced_candidate_block_time: std::time::Instant,
    finalization_loop: std::thread::JoinHandle<()>,
    producer_service: ProducerService,
    fork_resolution_service: ForkResolutionService,
    metrics: Option<BlockProductionMetrics>,
    external_messages: ExternalMessagesThreadState,

    is_state_sync_requested: Arc<Mutex<Option<BlockSeqNo>>>,
    db_service: DBSerializeService,
    // Channel (sender) for block requests
    blk_req_tx: Sender<BlockRequestParams>,
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
        network_rx: Receiver<NetworkMessage>,
        network_broadcast_tx: NetBroadcastSender<NetworkMessage>,
        network_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
        raw_block_tx: InstrumentedSender<(AccountId, Vec<u8>)>,
        bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
        config: Config,
        block_keeper_rng: TRandomGenerator,
        producer_election_rng: TRandomGenerator,
        thread_id: ThreadIdentifier,
        feedback_sender: InstrumentedSender<ExtMsgFeedbackList>,
        _update_producer_group: bool,
        block_state_repository: BlockStateRepository,
        block_processor_service: BlockProcessorService,
        attestations_target_service: AttestationsTargetService,
        validation_service: ValidationServiceInterface,
        skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
        fork_resolution_service: ForkResolutionService,
        block_gap: BlockGap,
        metrics: Option<BlockProductionMetrics>,
        self_tx: Sender<NetworkMessage>,
        external_messages: ExternalMessagesThreadState,
        message_db: MessageDurableStorage,
        last_block_attestations: Arc<Mutex<CollectedAttestations>>,
        bp_production_count: Arc<AtomicI32>,
        db_service: DBSerializeService,
        blk_req_tx: Sender<BlockRequestParams>,
    ) -> Self {
        tracing::trace!("Start node for thread: {thread_id:?}");
        if let Some(metrics) = &metrics {
            metrics.report_thread_count();
        }

        let received_acks = Arc::new(Mutex::new(Vec::new()));
        let received_nacks = Arc::new(Mutex::new(Vec::new()));
        let metrics_clone = metrics.clone();
        let is_state_sync_requested = Arc::new(Mutex::new(None));
        Self {
            shared_services: shared_services.clone(),
            state_sync_service: state_sync_service.clone(),
            repository: repository.clone(),
            network_rx,
            network_broadcast_tx: network_broadcast_tx.clone(),
            network_direct_tx: network_direct_tx.clone(),
            raw_block_tx: raw_block_tx.clone(),
            bls_keys_map: bls_keys_map.clone(),
            production_timeout_multiplier: DEFAULT_PRODUCTION_TIME_MULTIPLIER,
            last_block_attestations: last_block_attestations.clone(),
            config: config.clone(),
            block_gap_length: block_gap.clone(),
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
                .name(format!("Block finalization loop {}", thread_id))
                .spawn_critical({
                    let repository_clone = repository.clone();
                    let block_state_repository_clone = block_state_repository.clone();
                    let block_gap_clone = block_gap.clone();
                    let shared_services_clone = shared_services.clone();
                    let external_messages = external_messages.clone();
                    let message_db_clone = message_db.clone();
                    let node_id = config.local.node_id.clone();
                    move || {
                        crate::node::services::finalization::finalization_loop(
                            repository_clone,
                            block_state_repository_clone,
                            shared_services_clone,
                            raw_block_tx,
                            state_sync_service,
                            block_gap_clone,
                            metrics_clone,
                            external_messages,
                            message_db_clone,
                            &node_id,
                        );
                        Ok(())
                    }
                })
                .expect("Failed to start finalization inner loop"),
            fork_resolution_service: fork_resolution_service.clone(),
            producer_service: ProducerService::start(
                thread_id,
                repository.clone(),
                block_state_repository.clone(),
                block_gap.clone(),
                production_process,
                feedback_sender.clone(),
                received_acks.clone(),
                received_nacks.clone(),
                shared_services.clone(),
                bls_keys_map.clone(),
                last_block_attestations.clone(),
                attestations_target_service,
                fork_resolution_service.clone(),
                self_tx,
                network_broadcast_tx,
                config.local.node_id.clone(),
                config.global.producer_change_gap_size,
                Duration::from_millis(config.global.time_to_produce_block_millis),
                config.global.save_state_frequency,
                external_messages.clone(),
                is_state_sync_requested.clone(),
                bp_production_count,
            )
            .expect("Failed to start producer service"),
            metrics,
            external_messages,
            is_state_sync_requested,
            db_service,
            blk_req_tx,
        }
    }
}
