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
use std::fmt::Display;
use std::hash::Hash;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;

pub use associated_types::NodeIdentifier;
pub use associated_types::SignerIndex;
pub use network_message::NetworkMessage;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use services::sync::StateSyncService;
use typed_builder::TypedBuilder;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block::Block;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::config::Config;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::BlockFor;
use crate::node::associated_types::BlockIdentifierFor;
use crate::node::associated_types::BlockSeqNoFor;
use crate::node::associated_types::NackData;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::associated_types::OptimisticForwardState;
use crate::node::associated_types::OptimisticStateFor;
use crate::node::associated_types::ThreadIdentifierFor;
use crate::node::attestation_processor::AttestationProcessor;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;

pub(crate) const DEFAULT_PRODUCTION_TIME_MULTIPLIER: u64 = 1;

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
    <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block:
    Block<BlockIdentifier = BlockIdentifierFor<TBlockProducerProcess>>,
    <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block:
    Block<BLSSignatureScheme = TBLSSignatureScheme>,
    <<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block as Block>::BlockSeqNo:
    Eq + Hash,
    BlockFor<TBlockProducerProcess>: Clone + Display,
    BlockIdentifierFor<TBlockProducerProcess>: Serialize + for<'de> Deserialize<'de>,
    <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: 'static,
    TValidationProcess: BlockKeeperProcess<
        CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
        Block = BlockFor<TBlockProducerProcess>,
        BlockSeqNo = BlockSeqNoFor<TBlockProducerProcess>,
        BlockIdentifier = BlockIdentifierFor<TBlockProducerProcess>,
    >,
    TRepository: Repository<
        BLS = TBLSSignatureScheme,
        EnvelopeSignerIndex = SignerIndex,
        ThreadIdentifier = ThreadIdentifierFor<TBlockProducerProcess>,
        Block = BlockFor<TBlockProducerProcess>,
        CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
        OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        NodeIdentifier = NodeIdentifier,
    >,
    <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
        <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
    >,
    TStateSyncService: StateSyncService,
    TAttestationProcessor: AttestationProcessor<
        BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>,
        CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
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
            BlockFor<TBlockProducerProcess>,
            AckData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
            NackData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
            AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
            <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
            BlockIdentifierFor<TBlockProducerProcess>,
            BlockSeqNoFor<TBlockProducerProcess>,
            NodeIdentifier,
        >,
    >,
    tx: Sender<
        NetworkMessage<
            TBLSSignatureScheme,
            BlockFor<TBlockProducerProcess>,
            AckData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
            NackData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
            AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
            <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
            BlockIdentifierFor<TBlockProducerProcess>,
            BlockSeqNoFor<TBlockProducerProcess>,
            NodeIdentifier,
        >,
    >,
    single_tx: Sender<(
        NodeIdentifier,
        NetworkMessage<
            TBLSSignatureScheme,
            BlockFor<TBlockProducerProcess>,
            AckData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
            NackData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
            AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
            <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
            BlockIdentifierFor<TBlockProducerProcess>,
            BlockSeqNoFor<TBlockProducerProcess>,
            NodeIdentifier,
        >,
    )>,
    raw_block_tx: Sender<Vec<u8>>,
    block_keeper_ring_pubkeys: Arc<Mutex<BlockKeeperSet>>,
    #[allow(dead_code)]
    pubkey: <TBLSSignatureScheme as BLSSignatureScheme>::PubKey,
    secret: <TBLSSignatureScheme as BLSSignatureScheme>::Secret,

    #[builder(default)]
    cache_forward_optimistic: HashMap<
        ThreadIdentifierFor<TBlockProducerProcess>,
        OptimisticForwardState<
            BlockIdentifierFor<TBlockProducerProcess>,
            BlockSeqNoFor<TBlockProducerProcess>,
        >,
    >,

    #[builder(default)]
    unprocessed_blocks_cache:
    BTreeMap<BlockSeqNoFor<TBlockProducerProcess>, HashSet<BlockIdentifierFor<TBlockProducerProcess>>>,
    production_timeout_multiplier: u64,
    last_block_attestations: Vec<Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>>,
    received_acks: Vec<Envelope<TBLSSignatureScheme, AckData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>>,
    sent_acks: BTreeMap<BlockSeqNoFor<TBlockProducerProcess>, Envelope<TBLSSignatureScheme, AckData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>>,
    received_nacks: Vec<Envelope<TBLSSignatureScheme, NackData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>>,
    config: Config,
    block_producer_groups: HashMap<ThreadIdentifierFor<TBlockProducerProcess>, BTreeMap<BlockSeqNoFor<TBlockProducerProcess>, Vec<NodeIdentifier>>>,
    block_gap_length: HashMap<ThreadIdentifierFor<TBlockProducerProcess>, usize>,
    sent_attestations: HashMap<ThreadIdentifierFor<TBlockProducerProcess>,
        Vec<(BlockSeqNoFor<TBlockProducerProcess>, Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>)>>,
    received_attestations: BTreeMap<BlockSeqNoFor<TBlockProducerProcess>, HashMap<BlockIdentifierFor<TBlockProducerProcess>, HashSet<SignerIndex>>>,
    attestation_processor: TAttestationProcessor,
    blocks_for_resync_broadcasting: VecDeque<Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>>,
    block_keeper_rng: TRandomGenerator,
    attestations_to_send: BTreeMap<BlockSeqNoFor<TBlockProducerProcess>, Vec<Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>>>,
    last_sent_attestation: Option<(BlockSeqNoFor<TBlockProducerProcess>, std::time::Instant)>,
}

impl<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
Node<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBLSSignatureScheme: BLSSignatureScheme<PubKey = PubKey> + Clone,
        <TBLSSignatureScheme as BLSSignatureScheme>::PubKey: PartialEq,
        TBlockProducerProcess:
        BlockProducerProcess<Block = BlockFor<TBlockProducerProcess>, Repository = TRepository>,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block:
        Block<BlockIdentifier = BlockIdentifierFor<TBlockProducerProcess>>,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block:
        Block<BLSSignatureScheme = TBLSSignatureScheme>,
        <<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block as Block>::BlockSeqNo:
        Eq + Hash,
        ThreadIdentifierFor<TBlockProducerProcess>: Default,
        BlockFor<TBlockProducerProcess>: Clone + Display,
        BlockIdentifierFor<TBlockProducerProcess>: Serialize + for<'de> Deserialize<'de>,
        TValidationProcess: BlockKeeperProcess<
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            Block = BlockFor<TBlockProducerProcess>,
            BlockSeqNo = BlockSeqNoFor<TBlockProducerProcess>,
            BlockIdentifier = BlockIdentifierFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            Block = BlockFor<TBlockProducerProcess>,
        >,
        TRepository: Repository<
            BLS = TBLSSignatureScheme,
            EnvelopeSignerIndex = SignerIndex,
            ThreadIdentifier = ThreadIdentifierFor<TBlockProducerProcess>,
            Block = BlockFor<TBlockProducerProcess>,
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>,
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Block: From<<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block>,
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
                BlockFor<TBlockProducerProcess>,
                AckData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
                NackData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
                AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
                <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
                BlockIdentifierFor<TBlockProducerProcess>,
                BlockSeqNoFor<TBlockProducerProcess>,
                NodeIdentifier,
            >,
        >,
        tx: Sender<
            NetworkMessage<
                TBLSSignatureScheme,
                BlockFor<TBlockProducerProcess>,
                AckData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
                NackData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
                AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
                <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
                BlockIdentifierFor<TBlockProducerProcess>,
                BlockSeqNoFor<TBlockProducerProcess>,
                NodeIdentifier,
            >,
        >,
        single_tx: Sender<(
            NodeIdentifier,
            NetworkMessage<
                TBLSSignatureScheme,
                BlockFor<TBlockProducerProcess>,
                AckData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
                NackData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
                AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
                <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
                BlockIdentifierFor<TBlockProducerProcess>,
                BlockSeqNoFor<TBlockProducerProcess>,
                NodeIdentifier,
            >,
        )>,
        raw_block_tx: Sender<Vec<u8>>,
        pubkey: <TBLSSignatureScheme as BLSSignatureScheme>::PubKey,
        secret: <TBLSSignatureScheme as BLSSignatureScheme>::Secret,
        config: Config,
        attestation_processor: TAttestationProcessor,
        block_keeper_ring_pubkeys: Arc<Mutex<BlockKeeperSet>>,
        block_keeper_rng: TRandomGenerator,
    ) -> Self {
        let signer = config.local.node_id as SignerIndex;
        if cfg!(test) {
            let block_keeper_set = block_keeper_ring_pubkeys.lock();
            tracing::trace!("Check that zerostate contains block keeper key");
            assert_eq!(block_keeper_set.get(&signer).map(|data| &data.pubkey), Some(&pubkey));
        }
        tracing::trace!("Block keeper key ring: {:?}", block_keeper_ring_pubkeys);

        let sent_attestations = repository.load_sent_attestations().expect("Failed to load sent attestations");
        tracing::trace!("Block keeper loaded sent attestations len: {}", sent_attestations.len());
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
            block_keeper_ring_pubkeys,
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
        };

        let starting_thread_id = ThreadIdentifierFor::<TBlockProducerProcess>::default();
        let (last_finalized_block_id, last_finalized_block_seq_no) = res.repository.select_thread_last_finalized_block(&starting_thread_id).expect("Failed to load last finalized block data");
        if last_finalized_block_id != BlockIdentifierFor::<TBlockProducerProcess>::default() {
            let finalized_block = res.repository.get_block_from_repo_or_archive(&last_finalized_block_id).expect("Failed to load finalized block");
            res.set_producer_groups_from_finalized_state(starting_thread_id, last_finalized_block_seq_no, finalized_block.data().get_common_section().producer_group);
        } else {
            res.update_producer_group(&starting_thread_id, vec![]).expect("Failed to initialize producer group");
        }
        res
    }

    fn on_ack(&mut self, ack: <Self as NodeAssociatedTypes>::Ack) -> anyhow::Result<()> {
        let block_id: &BlockIdentifierFor<TBlockProducerProcess> = &ack.data().block_id;
        let block_seq_no = match self.repository.get_block(block_id)? {
            None => {
                // log: unknown block.
                // TODO: save in cache in case of future block incoming
                return Ok(());
            }
            Some(block) => block.data().seq_no(),
        };
        let signatures_map = self.block_keeper_ring_signatures_map_for(&block_seq_no);
        let _is_valid = ack
            .verify_signatures(&signatures_map)
            .expect("Signatures verification should not crash.");
        if self.is_this_node_a_producer_for(
            &ThreadIdentifierFor::<TBlockProducerProcess>::default(),
            &block_seq_no,
        ) {
            self.received_acks.push(ack);
        }
        Ok(())
    }

    fn on_nack(&mut self, nack: <Self as NodeAssociatedTypes>::Nack) -> anyhow::Result<()> {
        let block_id: &BlockIdentifierFor<TBlockProducerProcess> = &nack.data().block_id;
        let block = match self.repository.get_block(block_id)? {
            None => {
                // log: unknown block.
                // TODO: save in cache in case of future block incoming
                return Ok(());
            }
            Some(block) => block,
        };
        let block_seq_no = block.data().seq_no();
        let block_producer_id = block.data().get_common_section().producer_id;
        let signatures_map = self.block_keeper_ring_signatures_map_for(&block_seq_no);
        let is_valid = nack
            .verify_signatures(&signatures_map)
            .expect("Signatures verification should not crash.");
        if is_valid {
            // TODO: we should not blindly believe and check Nacked block
            let thread_id = ThreadIdentifierFor::<TBlockProducerProcess>::default();
            self.slash_bp_stake(&thread_id, block_seq_no, block_producer_id)?;
            if self.is_this_node_a_producer_for(
                &thread_id,
                &block_seq_no,
            ) {
                self.received_nacks.push(nack);
            }
        }
        Ok(())
    }
}
