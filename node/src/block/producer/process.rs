// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::VecDeque;
use std::mem;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::sleep;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use database::documents_db::DocumentsDb;
use http_server::ExtMsgFeedback;
use parking_lot::Mutex;
use serde_json::Value;
use tracing::instrument;
use tracing::trace_span;
use tvm_block::Message;
use tvm_executor::BlockchainConfig;
use tvm_types::Cell;
use tvm_types::UInt256;

use crate::block::producer::builder::ActiveThread;
use crate::block::producer::BlockProducer;
use crate::block::producer::TVMBlockProducer;
use crate::block_keeper_system::BlockKeeperData;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::database::write_to_db;
use crate::node::associated_types::AckData;
use crate::node::associated_types::NackData;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::shared_services::SharedServices;
use crate::repository::cross_thread_ref_repository::CrossThreadRefDataRead;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::utilities::FixedSizeHashSet;

// Note: multiple blocks production service.
pub trait BlockProducerProcess {
    type BLSSignatureScheme: BLSSignatureScheme;
    type BlockProducer: BlockProducer;
    type CandidateBlock;
    type OptimisticState: OptimisticState;
    type Repository: Repository;
    type Ack: BLSSignedEnvelope;
    type Nack: BLSSignedEnvelope;

    #[allow(clippy::too_many_arguments)]
    fn start_thread_production(
        &mut self,
        thread_id: &ThreadIdentifier,
        prev_block_id: &BlockIdentifier,
        feedback_sender: Sender<Vec<ExtMsgFeedback>>,
        received_acks: Arc<Mutex<Vec<Envelope<Self::BLSSignatureScheme, AckData>>>>,
        received_nacks: Arc<Mutex<Vec<Envelope<Self::BLSSignatureScheme, NackData>>>>,
        blocks_state: BlockStateRepository,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        //        last_block_attestations: Arc<Mutex<Vec<Envelope<Self::BLSSignatureScheme, AttestationData>>>>,
        //        sent_attestations: Arc<Mutex<HashMap<ThreadIdentifier,
        //        Vec<(BlockSeqNo, Envelope<Self::BLSSignatureScheme, AttestationData>)>>>>,
    ) -> anyhow::Result<()>;

    fn stop_thread_production(&mut self, thread_id: &ThreadIdentifier) -> anyhow::Result<()>;

    fn get_produced_blocks(
        &mut self,
        thread_id: &ThreadIdentifier,
    ) -> Vec<(AckiNackiBlock, Self::OptimisticState, usize)>;

    fn get_production_iteration_start(&self) -> Instant;

    fn set_timeout(&mut self, timeout: Duration);

    fn write_block_to_db(
        &self,
        block: Self::CandidateBlock,
        optimistic_state: Self::OptimisticState,
    ) -> anyhow::Result<()>;

    fn send_epoch_message(&self, thread_id: &ThreadIdentifier, data: BlockKeeperData);

    fn add_state_to_cache(&mut self, thread_id: ThreadIdentifier, state: Self::OptimisticState);
}

pub struct TVMBlockProducerProcess {
    blockchain_config: Arc<BlockchainConfig>,
    repository: <Self as BlockProducerProcess>::Repository,
    archive: Option<Arc<dyn DocumentsDb>>,
    node_config: Config,
    produced_blocks:
        Arc<Mutex<HashMap<ThreadIdentifier, Vec<(AckiNackiBlock, OptimisticStateImpl, usize)>>>>,
    active_producer_threads: HashMap<
        ThreadIdentifier,
        (JoinHandle<<Self as BlockProducerProcess>::OptimisticState>, Sender<()>),
    >,
    block_produce_timeout: Arc<Mutex<Duration>>,
    iteration_start: Arc<Mutex<Instant>>,
    optimistic_state_cache:
        HashMap<ThreadIdentifier, <Self as BlockProducerProcess>::OptimisticState>,
    epoch_block_keeper_data_senders: HashMap<ThreadIdentifier, Sender<BlockKeeperData>>,
    // TODO: remove
    _new_blocks_from_other_threads: Arc<Mutex<Vec<BlockIdentifier>>>,
    shared_services: SharedServices,
}

impl TVMBlockProducerProcess {
    pub fn new(
        config: Config,
        repository: <Self as BlockProducerProcess>::Repository,
        archive: Option<Arc<dyn DocumentsDb>>,
        new_blocks_from_other_threads: Arc<Mutex<Vec<BlockIdentifier>>>,
        shared_services: SharedServices,
    ) -> anyhow::Result<Self> {
        let json =
            std::fs::read_to_string(&config.local.blockchain_config_path).unwrap_or_else(|_| {
                panic!(
                    "Failed to load blockchain config params from file: {}",
                    config.local.blockchain_config_path.display()
                )
            });
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(&json)?;
        let config_params = tvm_block_json::parse_config(&map).map_err(|e| {
            anyhow::format_err!(
                "Failed to parse config params from file {:?}: {e}",
                config.local.blockchain_config_path,
            )
        })?;
        let blockchain_config = BlockchainConfig::with_config(config_params)
            .map_err(|e| anyhow::format_err!("Failed to create blockchain config: {e}"))?;

        let block_produce_timeout =
            Arc::new(Mutex::new(Duration::from_millis(config.global.time_to_produce_block_millis)));
        Ok(TVMBlockProducerProcess {
            blockchain_config: Arc::new(blockchain_config),
            repository,
            archive,
            node_config: config,
            produced_blocks: Arc::new(Mutex::new(HashMap::new())),
            active_producer_threads: HashMap::new(),
            block_produce_timeout,
            iteration_start: Arc::new(Mutex::new(std::time::Instant::now())),
            optimistic_state_cache: HashMap::new(),
            epoch_block_keeper_data_senders: HashMap::new(),
            _new_blocks_from_other_threads: new_blocks_from_other_threads,
            shared_services,
        })
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    fn produce_next(
        initial_state: &mut OptimisticStateImpl,
        blockchain_config: Arc<BlockchainConfig>,
        repo_clone: &mut RepositoryImpl,
        node_config: &Config,
        produced_blocks: Arc<
            Mutex<HashMap<ThreadIdentifier, Vec<(AckiNackiBlock, OptimisticStateImpl, usize)>>>,
        >,
        timeout: Arc<Mutex<Duration>>,
        thread_id_clone: ThreadIdentifier,
        iteration_start_clone: Arc<Mutex<Instant>>,
        epoch_block_keeper_data_rx: &Receiver<BlockKeeperData>,
        shared_services: &mut SharedServices,
        active_block_producer_threads: &mut Vec<(Cell, ActiveThread)>,
        received_acks: Arc<
            Mutex<Vec<Envelope<<Self as BlockProducerProcess>::BLSSignatureScheme, AckData>>>,
        >,
        received_nacks: Arc<
            Mutex<Vec<Envelope<<Self as BlockProducerProcess>::BLSSignatureScheme, NackData>>>,
        >,
        block_state_repo: BlockStateRepository,
        feedback_sender: Sender<Vec<ExtMsgFeedback>>,
        external_control_rx: &Receiver<()>,
    ) -> bool {
        tracing::trace!("Start block production process iteration");

        let (message_queue, epoch_block_keeper_data, block_nack, aggregated_acks, aggregated_nacks) =
            trace_span!("read messages").in_scope(|| {
                let messages = initial_state.get_remaining_ext_messages(repo_clone).unwrap();
                let message_queue: VecDeque<Message> =
                    messages.clone().into_iter().map(|wrap| wrap.message).collect();
                let mut received_acks_in = received_acks.lock();
                let received_acks_copy = received_acks_in.clone();
                received_acks_in.clear();
                drop(received_acks_in);
                // TODO: filter that nacks are not older than last finalized block
                let mut received_nacks_in = received_nacks.lock();
                let received_nacks_copy = received_nacks_in.clone();
                received_nacks_in.clear();
                drop(received_nacks_in);
                let aggregated_acks =
                    aggregate_acks(received_acks_copy).expect("Failed to aggregate acks");
                let aggregated_nacks =
                    aggregate_nacks(received_nacks_copy).expect("Failed to aggregate nacks");
                let block_nack = aggregated_nacks.clone();
                let mut epoch_block_keeper_data = vec![];
                while let Ok(data) = epoch_block_keeper_data_rx.try_recv() {
                    tracing::trace!("Received data for epoch: {data:?}");
                    epoch_block_keeper_data.push(data);
                }

                tracing::Span::current().record("messages.len", messages.len());
                (
                    message_queue,
                    epoch_block_keeper_data,
                    block_nack,
                    aggregated_acks,
                    aggregated_nacks,
                )
            });

        let producer = TVMBlockProducer::builder()
            .active_threads(mem::take(active_block_producer_threads))
            .blockchain_config(blockchain_config.clone())
            .message_queue(message_queue)
            .node_config(node_config.clone())
            .epoch_block_keeper_data(epoch_block_keeper_data)
            .shared_services(shared_services.clone())
            .block_nack(block_nack.clone())
            .blocks_states(block_state_repo)
            .build();

        let (control_tx, control_rx) = std::sync::mpsc::channel();
        {
            let mut iteration_start = iteration_start_clone.lock();
            *iteration_start = std::time::Instant::now();
        }
        let initial_state_clone = initial_state.clone();
        tracing::trace!(
            "produce_block refs: initial state ThreadReferencesState: {:?}",
            initial_state_clone.thread_refs_state
        );

        let refs = trace_span!("read thread refs").in_scope(||{
        let mut refs = vec![];
        let mut try_refs = vec![];
        {
            let buffer = shared_services
                .exec(|services| {
                    services.thread_sync.list_blocks_sending_messages_to_thread(&thread_id_clone)
                })
                .expect("Failed to list potential ref block");
            tracing::trace!("Loaded sync info buffer: {buffer:?}");
            // Note:
            // trying to add refs one by one and ensure they're not conflicting
            for block_id in buffer.iter() {
                try_refs.push(block_id.clone());
                tracing::trace!("Add ref for block_id: {:?}", block_id);
                use crate::multithreading::cross_thread_messaging::thread_references_state::CanRefQueryResult;
                let can_ref_query_result = shared_services
                    .exec(|e| {
                        let references = try_refs.clone();
                        tracing::trace!("produce_block refs: can_ref: refs: {references:?}");
                        let can_ref = initial_state_clone.thread_refs_state.can_reference(
                            references,
                            |block_id| {
                                e.cross_thread_ref_data_service.get_cross_thread_ref_data(block_id)
                            },
                        );
                        tracing::trace!("produce_block refs: can_ref: res: {can_ref:?}");
                        can_ref
                    })
                    .expect("Can query block must work");
                match can_ref_query_result {
                    CanRefQueryResult::No => {
                        let _ = try_refs.pop();
                    }
                    CanRefQueryResult::Yes(e) => {
                        refs = e.explicitly_referenced_blocks;
                    }
                }
            }
        }
        tracing::trace!("produce_block refs: try_refs: {try_refs:?}");

        // TODO: reuse previously loaded cross thread ref data
        let refs = shared_services.exec(|e| {
            refs.into_iter()
                .map(|r| {
                    e.cross_thread_ref_data_service
                        .get_cross_thread_ref_data(&r)
                        .expect("Failed to load ref state")
                })
                .collect::<Vec<_>>()
        });

            if tracing::level_filters::STATIC_MAX_LEVEL >= tracing::Level::TRACE {
                let span = tracing::Span::current();
                span.record("refs.len",  refs.len() as i64);
                span.record("try_refs.len", try_refs.len() as i64);
            }
            refs
        });

        let current_span = tracing::Span::current().clone();

        let thread = std::thread::Builder::new()
            .name(format!("Produce block {}", &thread_id_clone))
            .stack_size(16 * 1024 * 1024)
            .spawn(move || {
                let _current_span_scope = current_span.enter();

                tracing::trace!("start production thread");
                let (block, result_state, active_block_producer_threads) = producer
                    // TODO: add refs to other thread states in case of sync
                    .produce(
                        thread_id_clone,
                        initial_state_clone,
                        refs.iter(),
                        control_rx,
                        feedback_sender,
                    )
                    .expect("Failed to produce block");

                (block, result_state, active_block_producer_threads)
            })
            .unwrap();
        let cur_timeout = { *timeout.lock() };
        tracing::trace!("Sleep for {cur_timeout:?}");

        trace_span!("sleep").in_scope(|| {
            sleep(cur_timeout);
        });

        tracing::trace!("Send signal to stop production");
        let _ = control_tx.send(());
        let (mut block, result_state, new_active_block_producer_threads) =
            thread.join().expect("Failed to join producer thread");
        tracing::trace!("Produced block: {}", block);
        if let Ok(()) = external_control_rx.try_recv() {
            return true;
        }
        // Update common section
        let mut common_section = block.get_common_section().clone();
        common_section.acks = aggregated_acks;
        common_section.nacks = aggregated_nacks.clone();
        block.set_common_section(common_section, false).expect("Failed to set common section");

        // if let Some(documents_db) = documents_db.clone() {
        //     write_to_db(
        //         documents_db,
        //         block.clone(),
        //         shard_state.clone(),
        //         repo_clone.clone(),
        //     )
        //     .expect("Failed to write block production results to DB");
        // }

        if tracing::level_filters::STATIC_MAX_LEVEL >= tracing::Level::TRACE {
            if let Ok(info) = block.tvm_block().info.read_struct() {
                use tvm_block::GetRepresentationHash;
                let span = tracing::Span::current();
                span.record("block.seq_no", info.seq_no() as i64);
                span.record("block.hash", info.hash().unwrap_or_default().as_hex_string());
            }
        }

        // DEBUGGING
        // let mut parent_state = repo_clone.get_optimistic_state(&block.parent())
        // .expect("Must not fail")
        // .expect("Must have state");
        // assert!(block.get_common_section().refs.is_empty());
        // tracing::trace!(
        // "debug_apply_state: block {} on {:?}",
        // block.identifier(),
        // parent_state.get_shard_state_as_cell().repr_hash(),
        // );
        // parent_state.apply_block(&block, [].iter()).unwrap();
        //

        *active_block_producer_threads = new_active_block_producer_threads;
        *initial_state = result_state;

        trace_span!("save cross thread refs").in_scope(|| {
            let cross_thread_ref_data =
                CrossThreadRefData::from_ackinacki_block(&block, initial_state)
                    .expect("Failed to create cross-thread ref data");

            if tracing::level_filters::STATIC_MAX_LEVEL >= tracing::Level::TRACE {
                tracing::Span::current()
                    .record("refs_len", cross_thread_ref_data.refs().len() as i64);
            }
            shared_services
                .exec(|e| {
                    e.cross_thread_ref_data_service.set_cross_thread_ref_data(cross_thread_ref_data)
                })
                .expect("Failed to save cross-thread ref data");
        });

        trace_span!("save state").in_scope(|| {
            tracing::trace!("Save produced block");
            let mut blocks = produced_blocks.lock();
            let processed_ext_messages = block.processed_ext_messages_cnt();
            let produced_data = (block, initial_state.clone(), processed_ext_messages);
            let saves = blocks.entry(thread_id_clone).or_default();
            saves.push(produced_data);
        });
        tracing::trace!("End block production process iteration");
        false
    }
}

impl BlockProducerProcess for TVMBlockProducerProcess {
    type Ack = Envelope<Self::BLSSignatureScheme, AckData>;
    type BLSSignatureScheme = GoshBLS;
    type BlockProducer = TVMBlockProducer;
    type CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>;
    type Nack = Envelope<Self::BLSSignatureScheme, NackData>;
    type OptimisticState = OptimisticStateImpl;
    type Repository = RepositoryImpl;

    fn start_thread_production(
        &mut self,
        thread_id: &ThreadIdentifier,
        prev_block_id: &BlockIdentifier,
        feedback_sender: Sender<Vec<ExtMsgFeedback>>,
        received_acks: Arc<Mutex<Vec<Envelope<Self::BLSSignatureScheme, AckData>>>>,
        received_nacks: Arc<Mutex<Vec<Envelope<Self::BLSSignatureScheme, NackData>>>>,
        blocks_states: BlockStateRepository,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        // mut initial_state: Self::OptimisticState,
    ) -> anyhow::Result<()> {
        tracing::trace!(
            "BlockProducerProcess start production for thread: {:?}, initial_block_id:{:?}",
            thread_id,
            prev_block_id,
        );
        if self.active_producer_threads.contains_key(thread_id) {
            tracing::error!(
                "Logic failure: node should not start production for the same thread several times"
            );
            return Ok(());
        }
        tracing::trace!("start_thread_production: loading state to start production");
        let mut initial_state = match self.optimistic_state_cache.remove(thread_id) {
            Some(state) => {
                tracing::trace!(
                    "start_thread_production: producer process contains state cache for this thread: {:?} {prev_block_id:?}",
                    state.block_id
                );
                if &state.block_id == prev_block_id {
                    tracing::trace!("start_thread_production: use cached state");
                    state
                } else {
                    tracing::trace!(
                        "start_thread_production: cached state block id is not appropriate. Load state from repo"
                    );
                    if let Some(state) = self
                        .repository
                        .get_optimistic_state(prev_block_id, Arc::clone(&nack_set_cache))?
                    {
                        state
                    } else if prev_block_id == &BlockIdentifier::default() {
                        self.repository.get_zero_state_for_thread(thread_id)?
                    } else {
                        panic!(
                            "Failed to find optimistic in repository for block {:?}",
                            &prev_block_id
                        )
                    }
                }
            }
            None => {
                tracing::trace!(
                    "start_thread_production: cache does not contain state for this thread. Load state from repo"
                );
                if let Some(state) = self
                    .repository
                    .get_optimistic_state(prev_block_id, Arc::clone(&nack_set_cache))?
                {
                    state
                } else if prev_block_id == &BlockIdentifier::default() {
                    self.repository.get_zero_state_for_thread(thread_id)?
                } else {
                    panic!("Failed to find optimistic in repository for block {:?}", &prev_block_id)
                }
            }
        };

        let blockchain_config = Arc::clone(&self.blockchain_config);

        let mut repo_clone = self.repository.clone();
        let node_config = self.node_config.clone();
        let produced_blocks = self.produced_blocks.clone();
        let timeout = self.block_produce_timeout.clone();
        let thread_id_clone = *thread_id;
        let iteration_start_clone = self.iteration_start.clone();
        let (control_tx, external_control_rx) = std::sync::mpsc::channel();
        let (epoch_block_keeper_data_tx, epoch_block_keeper_data_rx) = std::sync::mpsc::channel();
        let mut shared_services = self.shared_services.clone();
        let handler = std::thread::Builder::new()
            .name(format!("Production {}", &thread_id_clone))
            .spawn(move || {
                let mut active_block_producer_threads = vec![];
                // Note:
                // This loop runs infinitely till the interrupt signal generating new blocks
                //
                // Note:
                // Repository in this case can be assumed a shared state between all threads.
                // Using repository threads can find messages incoming from other threads.
                // It is also possible to track blocks dependencies through repository.
                // TODO: think if it is the best solution given all circumstances
                loop {
                    let stopped = Self::produce_next(
                        &mut initial_state,
                        blockchain_config.clone(),
                        &mut repo_clone,
                        &node_config,
                        produced_blocks.clone(),
                        timeout.clone(),
                        thread_id_clone,
                        iteration_start_clone.clone(),
                        &epoch_block_keeper_data_rx,
                        &mut shared_services,
                        &mut active_block_producer_threads,
                        received_acks.clone(),
                        received_nacks.clone(),
                        blocks_states.clone(),
                        feedback_sender.clone(),
                        &external_control_rx,
                    );
                    if stopped {
                        trace_span!("store optimistic").in_scope(|| {
                            repo_clone
                                .store_optimistic(initial_state.clone())
                                .expect("Failed to store optimistic state");
                            tracing::trace!("Stop production process: {}", &thread_id_clone);
                        });
                        return initial_state;
                    }
                }
            })?;
        self.active_producer_threads.insert(*thread_id, (handler, control_tx));
        self.epoch_block_keeper_data_senders.insert(*thread_id, epoch_block_keeper_data_tx);
        Ok(())
    }

    fn write_block_to_db(
        &self,
        block: Self::CandidateBlock,
        mut optimistic_state: Self::OptimisticState,
    ) -> anyhow::Result<()> {
        if let Some(archive) = self.archive.clone() {
            write_to_db(
                archive,
                block,
                Some(optimistic_state.get_shard_state()),
                None,
                // self.repository.clone(),
            )?;
        }
        Ok(())
    }

    fn stop_thread_production(&mut self, thread_id: &ThreadIdentifier) -> anyhow::Result<()> {
        tracing::trace!("stop_thread_production: {thread_id:?}");
        if let Some((thread, control)) = self.active_producer_threads.remove(thread_id) {
            tracing::trace!("Stop production for thread {thread_id:?}");
            control.send(())?;
            let last_optimistic_state = thread
                .join()
                .map_err(|_| anyhow::format_err!("Failed to get last state on production stop"))?;
            self.optimistic_state_cache.insert(*thread_id, last_optimistic_state);
            let _ = self.get_produced_blocks(thread_id);
        }
        Ok(())
    }

    fn get_produced_blocks(
        &mut self,
        thread_id: &ThreadIdentifier,
    ) -> Vec<(AckiNackiBlock, OptimisticStateImpl, usize)> {
        if self.active_producer_threads.contains_key(thread_id) {
            assert!(!self
                .active_producer_threads
                .get(thread_id)
                .expect("get_produced_blocks was called for non existing thread")
                .0
                .is_finished());
        }
        let mut blocks = self.produced_blocks.lock();
        if let Some(mut blocks_vec) = blocks.remove(thread_id) {
            blocks_vec.sort_by(|a, b| a.0.seq_no().cmp(&b.0.seq_no()));
            blocks_vec
        } else {
            vec![]
        }
    }

    fn get_production_iteration_start(&self) -> Instant {
        *self.iteration_start.lock()
    }

    fn set_timeout(&mut self, timeout: Duration) {
        tracing::trace!("set timeout for production process: {timeout:?}");
        let mut block_produce_timeout = self.block_produce_timeout.lock();
        *block_produce_timeout = timeout;
    }

    fn send_epoch_message(&self, thread_id: &ThreadIdentifier, data: BlockKeeperData) {
        if let Some(sender) = self.epoch_block_keeper_data_senders.get(thread_id) {
            let res = sender.send(data);
            tracing::trace!("send epoch message to producer res: {res:?}");
        }
    }

    fn add_state_to_cache(&mut self, thread_id: ThreadIdentifier, state: Self::OptimisticState) {
        tracing::trace!(
            "producer process: set cached state: {:?} {:?}",
            state.block_seq_no,
            state.block_id
        );
        self.optimistic_state_cache.insert(thread_id, state);
    }
}

fn aggregate_acks(
    mut received_acks: Vec<
        Envelope<<TVMBlockProducerProcess as BlockProducerProcess>::BLSSignatureScheme, AckData>,
    >,
) -> anyhow::Result<Vec<<TVMBlockProducerProcess as BlockProducerProcess>::Ack>> {
    let mut aggregated_acks = HashMap::new();
    tracing::trace!("Aggregate acks start len: {}", received_acks.len());
    for ack in &received_acks {
        let block_id = ack.data().block_id.clone();
        tracing::trace!("Aggregate acks block id: {:?}", block_id);
        aggregated_acks
            .entry(block_id)
            .and_modify(|aggregated_ack: &mut <TVMBlockProducerProcess as BlockProducerProcess>::Ack| {
                let mut merged_signatures_occurences =
                    aggregated_ack.clone_signature_occurrences();
                let initial_signatures_count = merged_signatures_occurences.len();
                let incoming_signature_occurences = ack.clone_signature_occurrences();
                for signer_index in incoming_signature_occurences.keys() {
                    let new_count =
                        (*merged_signatures_occurences.get(signer_index).unwrap_or(&0))
                            + (*incoming_signature_occurences.get(signer_index).unwrap());
                    merged_signatures_occurences.insert(*signer_index, new_count);
                }
                merged_signatures_occurences.retain(|_k, count| *count > 0);

                if merged_signatures_occurences.len() > initial_signatures_count {
                    let aggregated_signature = aggregated_ack.aggregated_signature();
                    let merged_aggregated_signature = <TVMBlockProducerProcess as BlockProducerProcess>::BLSSignatureScheme::merge(
                        aggregated_signature,
                        ack.aggregated_signature(),
                    )
                    .expect("Failed to merge attestations");
                    *aggregated_ack = <TVMBlockProducerProcess as BlockProducerProcess>::Ack::create(
                        merged_aggregated_signature,
                        merged_signatures_occurences,
                        aggregated_ack.data().clone(),
                    );
                }
            })
            .or_insert(ack.clone());
    }
    tracing::trace!("Aggregate acks result len: {:?}", aggregated_acks.len());
    received_acks.clear();
    Ok(aggregated_acks.values().cloned().collect())
}

// TODO: fix this function nacks can't be aggregated based on block id
fn aggregate_nacks(
    mut received_nacks: Vec<
        Envelope<<TVMBlockProducerProcess as BlockProducerProcess>::BLSSignatureScheme, NackData>,
    >,
) -> anyhow::Result<Vec<<TVMBlockProducerProcess as BlockProducerProcess>::Nack>> {
    let mut aggregated_nacks = HashMap::new();
    tracing::trace!("Aggregate nacks start len: {}", received_nacks.len());
    for nack in &received_nacks {
        let block_id = nack.data().block_id.clone();
        tracing::trace!("Aggregate nacks block id: {:?}", block_id);
        aggregated_nacks
            .entry(block_id)
            .and_modify(|aggregated_nack: &mut <TVMBlockProducerProcess as BlockProducerProcess>::Nack| {
                let mut merged_signatures_occurences =
                    aggregated_nack.clone_signature_occurrences();
                let initial_signatures_count = merged_signatures_occurences.len();
                let incoming_signature_occurences = nack.clone_signature_occurrences();
                for signer_index in incoming_signature_occurences.keys() {
                    let new_count =
                        (*merged_signatures_occurences.get(signer_index).unwrap_or(&0))
                            + (*incoming_signature_occurences.get(signer_index).unwrap());
                    merged_signatures_occurences.insert(*signer_index, new_count);
                }
                merged_signatures_occurences.retain(|_k, count| *count > 0);

                if merged_signatures_occurences.len() > initial_signatures_count {
                    let aggregated_signature = aggregated_nack.aggregated_signature();
                    let merged_aggregated_signature = <TVMBlockProducerProcess as BlockProducerProcess>::BLSSignatureScheme::merge(
                        aggregated_signature,
                        nack.aggregated_signature(),
                    )
                    .expect("Failed to merge attestations");
                    *aggregated_nack = <TVMBlockProducerProcess as BlockProducerProcess>::Nack::create(
                        merged_aggregated_signature,
                        merged_signatures_occurences,
                        aggregated_nack.data().clone(),
                    );
                }
            })
            .or_insert(nack.clone());
    }
    tracing::trace!("Aggregate nacks result len: {:?}", aggregated_nacks.len());
    received_nacks.clear();
    Ok(aggregated_nacks.values().cloned().collect())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use parking_lot::Mutex;

    use crate::block::producer::process::BlockProducerProcess;
    use crate::block::producer::process::TVMBlockProducerProcess;
    use crate::multithreading::routing::service::RoutingService;
    use crate::node::shared_services::SharedServices;
    use crate::node::NodeIdentifier;
    use crate::repository::repository_impl::RepositoryImpl;
    use crate::repository::Repository;
    use crate::types::ThreadIdentifier;
    use crate::utilities::FixedSizeHashSet;

    #[test]
    #[ignore]
    fn test_producer() -> anyhow::Result<()> {
        crate::tests::init_db("./tmp/node-data-5").expect("Failed to init DB 1");
        let config = crate::tests::default_config(NodeIdentifier::test(1));
        let blocks_states = crate::node::block_state::repository::BlockStateRepository::new(
            PathBuf::from("./tmp/node-data-5/block-state"),
        );
        let repository = RepositoryImpl::new(
            PathBuf::from("./tmp/node-data-5"),
            Some(config.local.zerostate_path.clone()),
            1,
            1,
            SharedServices::test_start(RoutingService::stub().0),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            blocks_states.clone(),
        );
        let (router, _router_rx) = crate::multithreading::routing::service::RoutingService::stub();
        let mut production_process = TVMBlockProducerProcess::new(
            config.clone(),
            repository.clone(),
            None,
            Arc::new(Mutex::new(vec![])),
            SharedServices::test_start(router),
        )?;

        let thread_id = ThreadIdentifier::default();
        let (prev_block_id, _prev_block_seq_no) =
            repository.select_thread_last_main_candidate_block(&thread_id)?;
        let (feedback_sender, _feedback_receiver) = std::sync::mpsc::channel();

        production_process.start_thread_production(
            &thread_id,
            &prev_block_id,
            feedback_sender,
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(Vec::new())),
            blocks_states.clone(),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
        )?;

        for _i in 0..10 {
            sleep(Duration::from_millis(config.global.time_to_produce_block_millis + 10));
            let blocks = production_process.get_produced_blocks(&thread_id);
            assert_eq!(blocks.len(), 1);
            println!("blocks: {}", blocks[0].0);
        }

        production_process.stop_thread_production(&thread_id)?;

        Ok(())
    }
}
