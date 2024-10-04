// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::sleep;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tvm_block::BlkPrevInfo;
use tvm_block::ExtBlkRef;
use tvm_block::Message;
use tvm_block::Serializable;
use tvm_executor::BlockchainConfig;
use tvm_types::UInt256;

use crate::block::producer::BlockProducer;
use crate::block::producer::TVMBlockProducer;
use crate::block::Block;
use crate::block::BlockIdentifier;
use crate::block::WrappedBlock;
use crate::block_keeper_system::BlockKeeperData;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::database::documents_db::DocumentsDb;
use crate::database::write_to_db;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;

pub trait BlockProducerProcess {
    type Block: Block<BlockIdentifier = Self::BlockIdentifier> + Serialize + for<'a> Deserialize<'a>;
    type BlockIdentifier: BlockIdentifier;
    type BlockProducer: BlockProducer;
    type CandidateBlock;
    type OptimisticState: OptimisticState;
    type Repository: Repository;

    fn start_thread_production(
        &mut self,
        thread_id: &<Self::BlockProducer as BlockProducer>::ThreadIdentifier,
        prev_block_id: &Self::BlockIdentifier,
    ) -> anyhow::Result<()>;

    fn stop_thread_production(
        &mut self,
        thread_id: &<Self::BlockProducer as BlockProducer>::ThreadIdentifier,
    ) -> anyhow::Result<()>;

    fn get_produced_blocks(
        &mut self,
        thread_id: &<Self::BlockProducer as BlockProducer>::ThreadIdentifier,
    ) -> Vec<(Self::Block, Self::OptimisticState, usize)>;

    fn get_production_iteration_start(&self) -> Instant;

    fn set_timeout(&mut self, timeout: Duration);

    fn write_block_to_db(
        &self,
        block: Self::CandidateBlock,
        optimistic_state: Self::OptimisticState,
    ) -> anyhow::Result<()>;

    fn send_epoch_message(
        &self,
        thread_id: &<Self::BlockProducer as BlockProducer>::ThreadIdentifier,
        data: BlockKeeperData,
    );
}

pub struct TVMBlockProducerProcess {
    blockchain_config: Arc<BlockchainConfig>,
    repository: <Self as BlockProducerProcess>::Repository,
    archive: Option<Arc<dyn DocumentsDb>>,
    node_config: Config,
    produced_blocks: Arc<
        Mutex<
            HashMap<
                <<Self as BlockProducerProcess>::BlockProducer as BlockProducer>::ThreadIdentifier,
                Vec<(WrappedBlock, OptimisticStateImpl, usize)>,
            >,
        >,
    >,
    active_producer_threads: HashMap<
        <<Self as BlockProducerProcess>::BlockProducer as BlockProducer>::ThreadIdentifier,
        (JoinHandle<<Self as BlockProducerProcess>::OptimisticState>, Sender<()>),
    >,
    block_produce_timeout: Arc<Mutex<Duration>>,
    iteration_start: Arc<Mutex<Instant>>,
    optimistic_state_cache: HashMap<
        <<Self as BlockProducerProcess>::BlockProducer as BlockProducer>::ThreadIdentifier,
        <Self as BlockProducerProcess>::OptimisticState,
    >,
    epoch_block_keeper_data_senders: HashMap<
        <<Self as BlockProducerProcess>::BlockProducer as BlockProducer>::ThreadIdentifier,
        Sender<BlockKeeperData>,
    >,
}

impl TVMBlockProducerProcess {
    pub fn new(
        config: Config,
        repository: <Self as BlockProducerProcess>::Repository,
        archive: Option<Arc<dyn DocumentsDb>>,
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
        })
    }
}

impl BlockProducerProcess for TVMBlockProducerProcess {
    type Block = <Self::OptimisticState as OptimisticState>::Block;
    type BlockIdentifier = <Self::Block as Block>::BlockIdentifier;
    type BlockProducer = TVMBlockProducer;
    type CandidateBlock = Envelope<GoshBLS, Self::Block>;
    type OptimisticState = OptimisticStateImpl;
    type Repository = RepositoryImpl;

    fn start_thread_production(
        &mut self,
        thread_id: &<Self::BlockProducer as BlockProducer>::ThreadIdentifier,
        prev_block_id: &Self::BlockIdentifier,
        // mut initial_state: Self::OptimisticState,
    ) -> anyhow::Result<()> {
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
                    "start_thread_production: producer process contains state cacha for this thread: {:?} {prev_block_id:?}",
                    state.block_id
                );
                if &state.block_id == prev_block_id {
                    tracing::trace!("start_thread_production: use cached state");
                    state
                } else {
                    tracing::trace!(
                        "start_thread_production: cached state block id is not appropriate. Load state from repo"
                    );
                    self.repository.get_optimistic_state(prev_block_id)?.unwrap_or_else(|| {
                        panic!(
                            "Failed to find optimistic in repository for block {:?}",
                            &prev_block_id
                        )
                    })
                }
            }
            None => {
                tracing::trace!(
                    "start_thread_production: cache does not contain state for this thread. Load state from repo"
                );
                self.repository.get_optimistic_state(prev_block_id)?.unwrap_or_else(|| {
                    panic!("Failed to find optimistic in repository for block {:?}", &prev_block_id)
                })
            }
        };
        let initial_block_id = initial_state.block_id.clone();
        tracing::trace!(
            "BlockProducerProcess start production for thread: {:?}, initial_block_id:{:?}",
            thread_id,
            initial_block_id,
        );
        let blockchain_config = Arc::clone(&self.blockchain_config);

        let mut repo_clone = self.repository.clone();
        let node_config = self.node_config.clone();
        let produced_blocks = self.produced_blocks.clone();
        let timeout = self.block_produce_timeout.clone();
        let thread_id_clone = *thread_id;
        let iteration_start_clone = self.iteration_start.clone();
        let (control_tx, external_control_rx) = std::sync::mpsc::channel();
        let (epoch_block_keeper_data_tx, epoch_block_keeper_data_rx) = std::sync::mpsc::channel();
        let handler =
            std::thread::Builder::new().name("Production".to_string()).spawn(move || {
                let mut active_threads = vec![];
                loop {
                    tracing::trace!("Start block production process iteration");
                    let initial_ext_msg_index = initial_state.last_processed_external_message_index;

                    let messages = initial_state.get_remaining_ext_messages(&repo_clone).unwrap();
                    let message_queue: VecDeque<Message> =
                        messages.clone().into_iter().map(|wrap| wrap.message).collect();
                    let prev_shard_state_cell = initial_state.get_shard_state_as_cell();
                    let prev_block_info = initial_state.get_block_info();
                    let mut epoch_block_keeper_data = vec![];
                    while let Ok(data) = epoch_block_keeper_data_rx.try_recv() {
                        tracing::trace!("Received data for epoch: {data:?}");
                        epoch_block_keeper_data.push(data);
                    }

                    let mut producer = TVMBlockProducer::new(
                        node_config.clone(),
                        blockchain_config.clone(),
                        message_queue,
                        prev_shard_state_cell,
                        prev_block_info,
                        active_threads,
                        epoch_block_keeper_data,
                    );

                    let (control_tx, control_rx) = std::sync::mpsc::channel();
                    {
                        let mut iteration_start = iteration_start_clone.lock();
                        *iteration_start = std::time::Instant::now();
                    }
                    let thread = std::thread::Builder::new()
                        .name("Produce block".into())
                        .stack_size(16 * 1024 * 1024)
                        .spawn(|| {
                            tracing::trace!("start production thread");
                            let (block, state, state_cell) =
                                producer.produce(control_rx).expect("Failed to produce block");
                            let active_threads = producer.active_threads;
                            (block, state, state_cell, active_threads)
                        })
                        .unwrap();
                    let cur_timeout = { *timeout.lock() };
                    tracing::trace!("Sleep for {cur_timeout:?}");
                    sleep(cur_timeout);

                    tracing::trace!("Send signal to stop production");
                    let _ = control_tx.send(());
                    let (block, shard_state, state_cell, new_active_threads) =
                        thread.join().expect("Failed to join producer thread");
                    if let Ok(()) = external_control_rx.try_recv() {
                        repo_clone.store_optimistic(initial_state.clone()).unwrap();
                        tracing::trace!("Stop production process: {thread_id_clone}");
                        return initial_state;
                    }
                    tracing::trace!("Produced block: {}", block);
                    active_threads = new_active_threads;

                    let info = block.block.read_info().unwrap();
                    let cell = block.block.serialize().unwrap();
                    let root_hash = cell.repr_hash();

                    let serialized_block = tvm_types::write_boc(&cell).unwrap();
                    let file_hash = UInt256::calc_file_hash(&serialized_block);
                    let block_info = BlkPrevInfo::Block {
                        prev: ExtBlkRef {
                            end_lt: info.end_lt(),
                            seq_no: info.seq_no(),
                            root_hash,
                            file_hash,
                        },
                    };
                    initial_state =
                        OptimisticStateImpl::from_shard_state_shard_state_cell_and_block_info(
                            block.identifier(),
                            Arc::new(shard_state.clone()),
                            state_cell,
                            block_info,
                            initial_ext_msg_index + block.processed_ext_messages_cnt as u32,
                        )
                        .expect("Failed to build OptimisticState");
                    // if let Some(documents_db) = documents_db.clone() {
                    //     write_to_db(
                    //         documents_db,
                    //         block.clone(),
                    //         shard_state.clone(),
                    //         repo_clone.clone(),
                    //     )
                    //     .expect("Failed to write block production results to DB");
                    // }
                    {
                        tracing::trace!("Save produced block");
                        let mut blocks = produced_blocks.lock();
                        let processed_ext_messages = block.processed_ext_messages_cnt;
                        let produced_data = (block, initial_state.clone(), processed_ext_messages);
                        let saves = blocks.entry(thread_id_clone).or_default();
                        saves.push(produced_data);
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

    fn stop_thread_production(
        &mut self,
        thread_id: &<Self::BlockProducer as BlockProducer>::ThreadIdentifier,
    ) -> anyhow::Result<()> {
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
        thread_id: &<Self::BlockProducer as BlockProducer>::ThreadIdentifier,
    ) -> Vec<(WrappedBlock, OptimisticStateImpl, usize)> {
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

    fn send_epoch_message(
        &self,
        thread_id: &<Self::BlockProducer as BlockProducer>::ThreadIdentifier,
        data: BlockKeeperData,
    ) {
        if let Some(sender) = self.epoch_block_keeper_data_senders.get(thread_id) {
            let res = sender.send(data);
            tracing::trace!("send epoch message to producer res: {res:?}");
        }
    }
}
