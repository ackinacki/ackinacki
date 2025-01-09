// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

mod acki_nacki;
pub mod associated_types;
pub mod attestation_processor;
mod attestations;
mod block_keeper_system;
mod block_processing;
pub mod block_state;
mod crypto;
pub mod events;
mod execution;
mod fork_choice;
pub mod leader_election;
mod network_message;
mod producer;
mod repository;
mod restart;
mod send;
pub mod services;
mod synchronization;
mod threads;
mod verifier;
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

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::associated_types::OptimisticForwardState;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;
use crate::types::block_keeper_ring::BlockKeeperRing;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::utilities::FixedSizeHashSet;
pub mod shared_services;
use block_state::repository::BlockStateRepository;
use parking_lot::Mutex;
use shared_services::SharedServices;

use crate::types::ThreadIdentifier;

pub(crate) const DEFAULT_PRODUCTION_TIME_MULTIPLIER: u64 = 1;

#[allow(dead_code)]
#[derive(TypedBuilder)]
pub struct Node<
    TStateSyncService,
    TBlockProducerProcess,
    TValidationProcess,
    TRepository,
    TAttestationProcessor,
    TRandomGenerator,
> where
    TBlockProducerProcess: BlockProducerProcess,
    <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: 'static,
    TValidationProcess: BlockKeeperProcess<
            BLSSignatureScheme = GoshBLS,
        CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,

        OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
    >,
    TRepository: Repository<
        BLS = GoshBLS,
        EnvelopeSignerIndex = SignerIndex,

        CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
        OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        NodeIdentifier = NodeIdentifier,
    >,
    <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
        <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
    >,
    TStateSyncService: StateSyncService,
    TAttestationProcessor: AttestationProcessor<
        BlockAttestation = Envelope<GoshBLS, AttestationData>,
        CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
    >,
    TRandomGenerator: rand::Rng,
{
    repository: TRepository,
    shared_services: SharedServices,
    state_sync_service: TStateSyncService,
    validation_process: TValidationProcess,
    production_process: TBlockProducerProcess,
    // TODO: @AleksandrS Add priority rx
    rx: Receiver<
        NetworkMessage<
            GoshBLS,
            AckData,
            NackData,
            AttestationData,
            <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
        >,
    >,
    tx: Sender<
        NetworkMessage<
            GoshBLS,
            AckData,
            NackData,
            AttestationData,
            <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
        >,
    >,
    single_tx: Sender<(
        NodeIdentifier,
        NetworkMessage<
            GoshBLS,
            AckData,
            NackData,
            AttestationData,
            <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
        >,
    )>,
    raw_block_tx: Sender<Vec<u8>>,
    #[allow(dead_code)]
    pubkey: <GoshBLS as BLSSignatureScheme>::PubKey,
    secret: <GoshBLS as BLSSignatureScheme>::Secret,
    #[builder(default)]
    cache_forward_optimistic: HashMap<
        ThreadIdentifier,
        OptimisticForwardState,
    >,

    #[builder(default)]
    unprocessed_blocks_cache:
    BTreeMap<BlockSeqNo, HashSet<BlockIdentifier>>,

    production_timeout_multiplier: u64,
    pub last_block_attestations: Vec<Envelope<GoshBLS, AttestationData>>,
    pub received_acks: Arc<Mutex<Vec<Envelope<GoshBLS, AckData>>>>,
    sent_acks: BTreeMap<BlockSeqNo, Envelope<GoshBLS, AckData>>,
    pub received_nacks: Arc<Mutex<Vec<Envelope<GoshBLS, NackData>>>>,
    config: Config,
    block_keeper_sets: BlockKeeperRing,
    block_producer_groups: HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, Vec<NodeIdentifier>>>,
    block_gap_length: HashMap<ThreadIdentifier, usize>,
    pub sent_attestations: HashMap<ThreadIdentifier,
        Vec<(BlockSeqNo, Envelope<GoshBLS, AttestationData>)>>,
    pub received_attestations: BTreeMap<BlockSeqNo, HashMap<BlockIdentifier, HashSet<SignerIndex>>>,
    attestation_processor: TAttestationProcessor,
    blocks_for_resync_broadcasting: VecDeque<Envelope<GoshBLS, AckiNackiBlock>>,
    block_keeper_rng: TRandomGenerator,
    producer_election_rng: TRandomGenerator,
    attestations_to_send: BTreeMap<BlockSeqNo, Vec<Envelope<GoshBLS, AttestationData>>>,
    last_sent_attestation: Option<(BlockSeqNo, std::time::Instant)>,
    ack_cache: BTreeMap<BlockSeqNo, Vec<Envelope<GoshBLS, AckData>>>,
    nack_cache: BTreeMap<BlockSeqNo, Vec<Envelope<GoshBLS, NackData>>>,
    thread_id: ThreadIdentifier,
    feedback_sender: Sender<Vec<ExtMsgFeedback>>,
    // Note: hack. check usage
    pub is_spawned_from_node_sync: bool,
    nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,

    // Note: signer index map is initially empty,
    // it is filled after BK epoch contract deploy or loaded from zerostate
    signer_index_map: BTreeMap<BlockSeqNo, SignerIndex>,

    blocks_states: BlockStateRepository,
}

impl<TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
Node<TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBlockProducerProcess:
        BlockProducerProcess< Repository = TRepository>,
        TValidationProcess: BlockKeeperProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,

            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,

        >,
        TRepository: Repository<
            BLS = GoshBLS,
            EnvelopeSignerIndex = SignerIndex,

            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<GoshBLS, AttestationData>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = TRepository
        >,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<GoshBLS, AttestationData>,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
        >,
        TRandomGenerator: rand::Rng,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shared_services: SharedServices,
        state_sync_service: TStateSyncService,
        production_process: TBlockProducerProcess,
        validation_process: TValidationProcess,
        repository: TRepository,
        rx: Receiver<
            NetworkMessage<
                GoshBLS,
                AckData,
                NackData,
                AttestationData,
                <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
            >,
        >,
        tx: Sender<
            NetworkMessage<
                GoshBLS,
                AckData,
                NackData,
                AttestationData,
                <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
            >,
        >,
        single_tx: Sender<(
            NodeIdentifier,
            NetworkMessage<
                GoshBLS,
                AckData,
                NackData,
                AttestationData,
                <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
            >,
        )>,
        raw_block_tx: Sender<Vec<u8>>,
        pubkey: <GoshBLS as BLSSignatureScheme>::PubKey,
        secret: <GoshBLS as BLSSignatureScheme>::Secret,
        config: Config,
        attestation_processor: TAttestationProcessor,
        mut block_keeper_sets: BlockKeeperRing,
        block_keeper_rng: TRandomGenerator,
        producer_election_rng: TRandomGenerator,
        thread_id: ThreadIdentifier,
        feedback_sender: Sender<Vec<ExtMsgFeedback>>,
        update_producer_group: bool,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        blocks_states: BlockStateRepository,
    ) -> Self {
        let signer_map = BTreeMap::from_iter({

            let initial_bk_sets_guarded = block_keeper_sets.sets.lock();
            let (_, initial_bk_set) = initial_bk_sets_guarded.last_key_value().expect("Zerostate must contain initial BK set");
            if let Some(signer_index) = initial_bk_set.values().find(|data| data.wallet_index == config.local.node_id).map(|data| data.signer_index) {
                vec![(BlockSeqNo::default(), signer_index)]
            } else {
                vec![]
            }
        });
        if cfg!(test) {
            if let Some((_, signer)) = signer_map.last_key_value() {
                block_keeper_sets.with_last_entry(|e| {
                    let block_keeper_sets = e.expect("must be there");
                    tracing::trace!("Check that zerostate contains block keeper key");
                    let bk_set = block_keeper_sets.get();
                    assert_eq!(bk_set.get(signer).map(|data| &data.pubkey), Some(&pubkey));
                });
            }
        }
        tracing::trace!("Block keeper key ring: {:?}", block_keeper_sets);

        let sent_attestations = repository.load_sent_attestations().expect("Failed to load sent attestations");
        tracing::trace!("Block keeper loaded sent attestations len: {}", sent_attestations.len());
        tracing::trace!("Start node for thread: {thread_id:?}");
        let mut res = Self {
            shared_services,
            state_sync_service,
            repository,
            rx,
            tx,
            single_tx,
            raw_block_tx,
            validation_process,
            production_process,
            pubkey,
            secret,
            cache_forward_optimistic: Default::default(),
            block_keeper_sets,
            production_timeout_multiplier: DEFAULT_PRODUCTION_TIME_MULTIPLIER,
            last_block_attestations: vec![],
            config,
            block_producer_groups: Default::default(),
            unprocessed_blocks_cache: Default::default(),
            block_gap_length: Default::default(),
            sent_attestations,
            received_attestations: Default::default(),
            attestation_processor,
            blocks_for_resync_broadcasting: Default::default(),
            block_keeper_rng,
            received_acks: Default::default(),
            received_nacks: Default::default(),
            sent_acks: Default::default(),
            attestations_to_send: Default::default(),
            last_sent_attestation: None,
            producer_election_rng,
            ack_cache: Default::default(),
            nack_cache: Default::default(),
            thread_id,
            feedback_sender,
            is_spawned_from_node_sync: false,
            nack_set_cache: Arc::clone(&nack_set_cache),
            signer_index_map: signer_map,
            blocks_states,
        };
        // let (last_finalized_block_id, _last_finalized_block_seq_no) = res.repository.select_thread_last_finalized_block(&thread_id).expect("Failed to load last finalized block data");
        // if last_finalized_block_id != BlockIdentifier::default() {
        //     let finalized_block = res.repository.get_block_from_repo_or_archive(&last_finalized_block_id).expect("Failed to load finalized block");
        //     // Start it from zero block to be able to process old blocks or attestations it the come
        //     res.set_producer_groups_from_finalized_state(thread_id, BlockSeqNo::default(), finalized_block.data().get_common_section().producer_group.clone());
        // } else {
            if update_producer_group {
                res.update_producer_group(&thread_id, vec![]).expect("Failed to initialize producer group");
            }
        // }
        res
    }
}
