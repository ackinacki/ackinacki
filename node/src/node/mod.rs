// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

mod acki_nacki;
pub mod associated_types;
pub mod attestation_processor;
mod attestations;
mod block_keeper_system;
mod block_processing;
mod crypto;
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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub use associated_types::NodeIdentifier;
pub use associated_types::SignerIndex;
pub use network_message::NetworkMessage;
use parking_lot::Mutex;
use services::sync::StateSyncService;
use typed_builder::TypedBuilder;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::config::Config;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::associated_types::OptimisticForwardState;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

pub(crate) const DEFAULT_PRODUCTION_TIME_MULTIPLIER: u64 = 1;

#[allow(dead_code)]
#[derive(TypedBuilder)]
pub struct Node<
    TBLSSignatureScheme,
    TStateSyncService,
    TBlockProducerProcess,
    TValidationProcess,
    TRepository,
    TAttestationProcessor,
    TRandomGenerator,
> where
    TBLSSignatureScheme: BLSSignatureScheme<PubKey = PubKey> + Clone,
    <TBLSSignatureScheme as BLSSignatureScheme>::PubKey: PartialEq,
    TBlockProducerProcess: BlockProducerProcess,
    <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: 'static,
    TValidationProcess: BlockKeeperProcess<
            BLSSignatureScheme = TBLSSignatureScheme,
        CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,

        OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
    >,
    TRepository: Repository<
        BLS = TBLSSignatureScheme,
        EnvelopeSignerIndex = SignerIndex,

        CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,
        OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        NodeIdentifier = NodeIdentifier,
    >,
    <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
        <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
    >,
    TStateSyncService: StateSyncService,
    TAttestationProcessor: AttestationProcessor<
        BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData>,
        CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,
    >,
    TRandomGenerator: rand::Rng,
{
    repository: TRepository,
    state_sync_service: TStateSyncService,
    validation_process: TValidationProcess,
    production_process: TBlockProducerProcess,
    rx: Receiver<
        NetworkMessage<
            TBLSSignatureScheme,
            AckData,
            NackData,
            AttestationData,
            <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
            NodeIdentifier,
        >,
    >,
    tx: Sender<
        NetworkMessage<
            TBLSSignatureScheme,
            AckData,
            NackData,
            AttestationData,
            <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
            NodeIdentifier,
        >,
    >,
    single_tx: Sender<(
        NodeIdentifier,
        NetworkMessage<
            TBLSSignatureScheme,
            AckData,
            NackData,
            AttestationData,
            <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
            NodeIdentifier,
        >,
    )>,
    raw_block_tx: Sender<Vec<u8>>,
    #[allow(dead_code)]
    pubkey: <TBLSSignatureScheme as BLSSignatureScheme>::PubKey,
    secret: <TBLSSignatureScheme as BLSSignatureScheme>::Secret,
    #[builder(default)]
    cache_forward_optimistic: HashMap<
        ThreadIdentifier,
        OptimisticForwardState,
    >,

    #[builder(default)]
    unprocessed_blocks_cache:
    BTreeMap<BlockSeqNo, HashSet<BlockIdentifier>>,

    production_timeout_multiplier: u64,
    last_block_attestations: Vec<Envelope<TBLSSignatureScheme, AttestationData>>,
    received_acks: Vec<Envelope<TBLSSignatureScheme, AckData>>,
    sent_acks: BTreeMap<BlockSeqNo, Envelope<TBLSSignatureScheme, AckData>>,
    received_nacks: Vec<Envelope<TBLSSignatureScheme, NackData>>,
    config: Config,
    block_keeper_sets: Arc<Mutex<HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>>>,
    block_producer_groups: HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, Vec<NodeIdentifier>>>,
    block_gap_length: HashMap<ThreadIdentifier, usize>,
    sent_attestations: HashMap<ThreadIdentifier,
        Vec<(BlockSeqNo, Envelope<TBLSSignatureScheme, AttestationData>)>>,
    received_attestations: BTreeMap<BlockSeqNo, HashMap<BlockIdentifier, HashSet<SignerIndex>>>,
    attestation_processor: TAttestationProcessor,
    blocks_for_resync_broadcasting: VecDeque<Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>>,
    block_keeper_rng: TRandomGenerator,
    producer_election_rng: TRandomGenerator,
    attestations_to_send: BTreeMap<BlockSeqNo, Vec<Envelope<TBLSSignatureScheme, AttestationData>>>,
    last_sent_attestation: Option<(BlockSeqNo, std::time::Instant)>,
    ack_cache: BTreeMap<BlockSeqNo, Vec<Envelope<TBLSSignatureScheme, AckData>>>,
    nack_cache: BTreeMap<BlockSeqNo, Vec<Envelope<TBLSSignatureScheme, NackData>>>,
    threads_table: ThreadsTable,
    thread_id: ThreadIdentifier,
}

impl<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
Node<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBLSSignatureScheme: BLSSignatureScheme<PubKey = PubKey> + Clone,
        <TBLSSignatureScheme as BLSSignatureScheme>::PubKey: PartialEq,
        TBlockProducerProcess:
        BlockProducerProcess< Repository = TRepository>,
        TValidationProcess: BlockKeeperProcess<
            BLSSignatureScheme = TBLSSignatureScheme,
            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,

            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = TBLSSignatureScheme,
            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,

        >,
        TRepository: Repository<
            BLS = TBLSSignatureScheme,
            EnvelopeSignerIndex = SignerIndex,

            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<TBLSSignatureScheme, AttestationData>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = TRepository
        >,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData>,
            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,
        >,
        TRandomGenerator: rand::Rng,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        state_sync_service: TStateSyncService,
        production_process: TBlockProducerProcess,
        validation_process: TValidationProcess,
        repository: TRepository,
        rx: Receiver<
            NetworkMessage<
                TBLSSignatureScheme,
                AckData,
                NackData,
                AttestationData,
                <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
                NodeIdentifier,
            >,
        >,
        tx: Sender<
            NetworkMessage<
                TBLSSignatureScheme,
                AckData,
                NackData,
                AttestationData,
                <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
                NodeIdentifier,
            >,
        >,
        single_tx: Sender<(
            NodeIdentifier,
            NetworkMessage<
                TBLSSignatureScheme,
                AckData,
                NackData,
                AttestationData,
                <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
                NodeIdentifier,
            >,
        )>,
        raw_block_tx: Sender<Vec<u8>>,
        pubkey: <TBLSSignatureScheme as BLSSignatureScheme>::PubKey,
        secret: <TBLSSignatureScheme as BLSSignatureScheme>::Secret,
        config: Config,
        attestation_processor: TAttestationProcessor,
        block_keeper_sets: Arc<Mutex<HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>>>,
        block_keeper_rng: TRandomGenerator,
        producer_election_rng: TRandomGenerator,
        threads_table: ThreadsTable,
        thread_id: ThreadIdentifier,
    ) -> Self {
        let signer = config.local.node_id as SignerIndex;
        if cfg!(test) {
            let block_keeper_sets = block_keeper_sets.lock().get(&thread_id).expect("Failed to get block keepers for thread").clone();
            assert!(!block_keeper_sets.is_empty(), "Initial BK set is empty");
            tracing::trace!("Check that zerostate contains block keeper key");
            let (_seq_no, bk_set) = block_keeper_sets.last_key_value().unwrap();
            assert_eq!(bk_set.get(&signer).map(|data| &data.pubkey), Some(&pubkey));
        }
        tracing::trace!("Block keeper key ring: {:?}", block_keeper_sets);

        let sent_attestations = repository.load_sent_attestations().expect("Failed to load sent attestations");
        tracing::trace!("Block keeper loaded sent attestations len: {}", sent_attestations.len());
        tracing::trace!("Start node for thread: {thread_id:?}");
        tracing::trace!("Threads table: {threads_table:?}");
        let mut res = Self {
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
            threads_table,
            thread_id,
        };

        let (last_finalized_block_id, _last_finalized_block_seq_no) = res.repository.select_thread_last_finalized_block(&thread_id).expect("Failed to load last finalized block data");
        if last_finalized_block_id != BlockIdentifier::default() {
            let finalized_block = res.repository.get_block_from_repo_or_archive(&last_finalized_block_id).expect("Failed to load finalized block");
            // Start it from zero block to be able to process old blocks or attestations it the come
            res.set_producer_groups_from_finalized_state(thread_id, BlockSeqNo::default(), finalized_block.data().get_common_section().producer_group.clone());
        } else {
            res.update_producer_group(&thread_id, vec![]).expect("Failed to initialize producer group");
        }
        res
    }
}
