// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

mod acki_nacki;
pub mod associated_types;
mod attestations;
mod block_keeper_system;
mod block_processing;
pub mod block_state;
mod crypto;
use crate::node::services::send_attestations::AttestationSendService;
use crate::repository::optimistic_state::OptimisticStateImpl;
mod execution;
pub use execution::LOOP_PAUSE_DURATION;
pub mod leader_election;
mod network_message;
mod producer;
mod repository;
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
use std::collections::VecDeque;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;

pub use associated_types::NodeIdentifier;
pub use associated_types::SignerIndex;
use http_server::ExtMsgFeedback;
pub use network_message::NetworkMessage;
use services::sync::StateSyncService;
use tvm_types::UInt256;
use typed_builder::TypedBuilder;

use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::associated_types::OptimisticForwardState;
use crate::node::services::attestations_target::service::AttestationsTargetService;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::RndSeed;
use crate::utilities::FixedSizeHashSet;
pub mod shared_services;
use block_state::repository::BlockStateRepository;
use parking_lot::Mutex;
use shared_services::SharedServices;

use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::node::block_state::repository::BlockState;
use crate::node::services::block_processor::service::BlockProcessorService;
use crate::node::services::fork_resolution::service::ForkResolutionService;
use crate::node::services::validation::service::ValidationServiceInterface;
use crate::types::ThreadIdentifier;

pub(crate) const DEFAULT_PRODUCTION_TIME_MULTIPLIER: u64 = 1;

#[allow(dead_code)]
#[derive(TypedBuilder)]
pub struct Node<
    TStateSyncService,
    TBlockProducerProcess,
    TRandomGenerator,
> where
    TBlockProducerProcess: BlockProducerProcess,
    <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: 'static,
    <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
        <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
    >,
    TStateSyncService: StateSyncService,
    TRandomGenerator: rand::Rng,
{
    repository: RepositoryImpl,
    shared_services: SharedServices,
    state_sync_service: TStateSyncService,
    // validation_process: TValidationProcess,
    production_process: TBlockProducerProcess,
    // TODO: @AleksandrS Add priority rx
    rx: Receiver<NetworkMessage>,
    tx: Sender<NetworkMessage>,
    single_tx: Sender<(
        NodeIdentifier,
        NetworkMessage,
    )>,
    raw_block_tx: Sender<Vec<u8>>,
    bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
    #[builder(default)]
    cache_forward_optimistic: Option<OptimisticForwardState>,

    #[builder(default)]
    unprocessed_blocks_cache: Arc<Mutex<Vec<BlockState>>>,

    production_timeout_multiplier: u64,
    pub last_block_attestations: Vec<Envelope<GoshBLS, AttestationData>>,
    pub received_acks: Arc<Mutex<Vec<Envelope<GoshBLS, AckData>>>>,
    sent_acks: BTreeMap<BlockSeqNo, Envelope<GoshBLS, AckData>>,
    pub received_nacks: Arc<Mutex<Vec<Envelope<GoshBLS, NackData>>>>,
    config: Config,
    block_gap_length: Arc<Mutex<usize>>,
    pub received_attestations: BTreeMap<BlockSeqNo, HashMap<BlockIdentifier, HashSet<SignerIndex>>>,
    blocks_for_resync_broadcasting: VecDeque<Envelope<GoshBLS, AckiNackiBlock>>,
    block_keeper_rng: TRandomGenerator,
    producer_election_rng: TRandomGenerator,
    attestations_to_send: BTreeMap<BlockSeqNo, Vec<Envelope<GoshBLS, AttestationData>>>,
    ack_cache: BTreeMap<BlockSeqNo, Vec<Envelope<GoshBLS, AckData>>>,
    nack_cache: BTreeMap<BlockSeqNo, Vec<Envelope<GoshBLS, NackData>>>,
    thread_id: ThreadIdentifier,
    feedback_sender: Sender<Vec<ExtMsgFeedback>>,
    // Note: hack. check usage
    pub is_spawned_from_node_sync: bool,
    nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,

    // Note: signer index map is initially empty,
    // it is filled after BK epoch contract deploy or loaded from zerostate
    blocks_states: BlockStateRepository,
    block_processor_service: BlockProcessorService,
    attestations_target_service: AttestationsTargetService,
    validation_service: ValidationServiceInterface,
    skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
    fork_resolution_service: ForkResolutionService,
    attestation_sender_service: AttestationSendService,
}

impl<TStateSyncService, TBlockProducerProcess, TRandomGenerator>
Node<TStateSyncService, TBlockProducerProcess, TRandomGenerator>
    where
        TBlockProducerProcess:
        BlockProducerProcess< Repository = RepositoryImpl>,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateImpl,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = RepositoryImpl
        >,
        TRandomGenerator: rand::Rng,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shared_services: SharedServices,
        state_sync_service: TStateSyncService,
        production_process: TBlockProducerProcess,
        repository: RepositoryImpl,
        rx: Receiver<NetworkMessage>,
        tx: Sender<NetworkMessage>,
        single_tx: Sender<(
            NodeIdentifier,
            NetworkMessage,
        )>,
        raw_block_tx: Sender<Vec<u8>>,
        bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
        config: Config,
        block_keeper_rng: TRandomGenerator,
        producer_election_rng: TRandomGenerator,
        thread_id: ThreadIdentifier,
        feedback_sender: Sender<Vec<ExtMsgFeedback>>,
        _update_producer_group: bool,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        blocks_states: BlockStateRepository,
        unprocessed_blocks_cache: Arc<Mutex<Vec<BlockState>>>,
        block_processor_service: BlockProcessorService,
        attestations_target_service: AttestationsTargetService,
        validation_service: ValidationServiceInterface,
        skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
        fork_resolution_service: ForkResolutionService,
        block_gap: Arc<Mutex<usize>>,
    ) -> Self {
        /*let signer_map = BTreeMap::from_iter({

            let initial_bk_sets_guarded = block_keeper_sets.sets.lock();
            let (_, initial_bk_set) = initial_bk_sets_guarded.last_key_value().expect("Zerostate must contain initial BK set");
            if let Some(signer_index) = initial_bk_set.values().find(|data| data.wallet_index == config.local.node_id).map(|data| data.signer_index) {
                vec![(BlockSeqNo::default(), signer_index)]
            } else {
                vec![]
            }
        });*/

        // TODO: skip load of sent attestations for now
        // let sent_attestations = repository.load_sent_attestations().expect("Failed to load sent attestations");
        // tracing::trace!("Block keeper loaded sent attestations len: {}", sent_attestations.len());


        tracing::trace!("Start node for thread: {thread_id:?}");

        // let (last_finalized_block_id, _last_finalized_block_seq_no) = res.repository.select_thread_last_finalized_block(&thread_id).expect("Failed to load last finalized block data");
        // if last_finalized_block_id != BlockIdentifier::default() {
        //     let finalized_block = res.repository.get_block_from_repo_or_archive(&last_finalized_block_id).expect("Failed to load finalized block");
        //     // Start it from zero block to be able to process old blocks or attestations it the come
        //     res.set_producer_groups_from_finalized_state(thread_id, BlockSeqNo::default(), finalized_block.data().get_common_section().producer_group.clone());
        // } else {
        //     if update_producer_group {
                // res.update_producer_group(vec![]).expect("Failed to initialize producer group");
            // }
        // }
        Self {
            shared_services,
            state_sync_service,
            repository,
            rx,
            tx,
            single_tx: single_tx.clone(),
            raw_block_tx,
            production_process,
            bls_keys_map: bls_keys_map.clone(),
            cache_forward_optimistic: Default::default(),
            production_timeout_multiplier: DEFAULT_PRODUCTION_TIME_MULTIPLIER,
            last_block_attestations: vec![],
            config: config.clone(),
            unprocessed_blocks_cache,
            block_gap_length: block_gap,
            attestation_sender_service: AttestationSendService::builder()
                .pulse_timeout(std::time::Duration::from_millis(config.global.time_to_produce_block_millis))
                .node_id(config.local.node_id.clone())
                .thread_id(thread_id)
                .bls_keys_map(bls_keys_map.clone())
                .block_state_repository(blocks_states.clone())
                .send_tx(single_tx.clone())
                .build(),
            received_attestations: Default::default(),
            blocks_for_resync_broadcasting: Default::default(),
            block_keeper_rng,
            received_acks: Default::default(),
            received_nacks: Default::default(),
            sent_acks: Default::default(),
            attestations_to_send: Default::default(),
            producer_election_rng,
            ack_cache: Default::default(),
            nack_cache: Default::default(),
            thread_id,
            feedback_sender,
            is_spawned_from_node_sync: false,
            nack_set_cache: Arc::clone(&nack_set_cache),
            blocks_states,
            block_processor_service,
            attestations_target_service,
            validation_service,
            skipped_attestation_ids,
            fork_resolution_service,
        }
    }
}
