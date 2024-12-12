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

use database::documents_db::DocumentsDb;
use parking_lot::Mutex;
use serde_json::Value;
use tvm_block::Message;
use tvm_executor::BlockchainConfig;

use crate::block::producer::BlockProducer;
use crate::block::producer::TVMBlockProducer;
use crate::block_keeper_system::BlockKeeperData;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::database::write_to_db;
use crate::multithreading::thread_synchrinization_service::ThreadSyncInfo;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

// Note: multiple blocks production service.
pub trait BlockProducerProcess {
    type BLSSignatureScheme: BLSSignatureScheme;
    type BlockProducer: BlockProducer;
    type CandidateBlock;
    type OptimisticState: OptimisticState;
    type Repository: Repository;

    fn start_thread_production(
        &mut self,
        thread_id: &ThreadIdentifier,
        threads_table: &ThreadsTable,
        prev_block_id: &BlockIdentifier,
    ) -> anyhow::Result<()>;

    fn stop_thread_production(&mut self, thread_id: &ThreadIdentifier) -> anyhow::Result<()>;

    fn get_produced_blocks(
        &mut self,
        thread_id: &ThreadIdentifier,
    ) -> Vec<(AckiNackiBlock<Self::BLSSignatureScheme>, Self::OptimisticState, usize)>;

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
    produced_blocks: Arc<
        Mutex<
            HashMap<
                ThreadIdentifier,
                Vec<(
                    AckiNackiBlock<<Self as BlockProducerProcess>::BLSSignatureScheme>,
                    OptimisticStateImpl,
                    usize,
                )>,
            >,
        >,
    >,
    active_producer_threads: HashMap<
        ThreadIdentifier,
        (JoinHandle<<Self as BlockProducerProcess>::OptimisticState>, Sender<()>),
    >,
    block_produce_timeout: Arc<Mutex<Duration>>,
    iteration_start: Arc<Mutex<Instant>>,
    optimistic_state_cache:
        HashMap<ThreadIdentifier, <Self as BlockProducerProcess>::OptimisticState>,
    epoch_block_keeper_data_senders: HashMap<ThreadIdentifier, Sender<BlockKeeperData>>,
    thread_sync_buffer: Arc<Mutex<Vec<ThreadSyncInfo>>>,
}

impl TVMBlockProducerProcess {
    pub fn new(
        config: Config,
        repository: <Self as BlockProducerProcess>::Repository,
        archive: Option<Arc<dyn DocumentsDb>>,
        thread_sync_buffer: Arc<Mutex<Vec<ThreadSyncInfo>>>,
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
            thread_sync_buffer,
        })
    }
}

impl BlockProducerProcess for TVMBlockProducerProcess {
    type BLSSignatureScheme = GoshBLS;
    type BlockProducer = TVMBlockProducer;
    type CandidateBlock =
        Envelope<GoshBLS, AckiNackiBlock<<Self as BlockProducerProcess>::BLSSignatureScheme>>;
    type OptimisticState = OptimisticStateImpl;
    type Repository = RepositoryImpl;

    fn start_thread_production(
        &mut self,
        thread_id: &ThreadIdentifier,
        threads_table: &ThreadsTable,
        prev_block_id: &BlockIdentifier,
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
                    if let Some(state) = self.repository.get_optimistic_state(prev_block_id)? {
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
                if let Some(state) = self.repository.get_optimistic_state(prev_block_id)? {
                    state
                } else if prev_block_id == &BlockIdentifier::default() {
                    self.repository.get_zero_state_for_thread(thread_id)?
                } else {
                    panic!("Failed to find optimistic in repository for block {:?}", &prev_block_id)
                }
            }
        };

        // It is possible that the prev block state is from another thread.
        // An actual block production could be one of 3 cases:
        // - normal continuation. No interaction with other threads.
        // - split. It's a new thread.
        // - sync. This is a sync operation when this thread consumes a part or a full state
        //   from another thread.
        if initial_state.get_thread_id() == thread_id
            && initial_state.get_threads_table() == threads_table
        {
            // Normal continuation.
        }

        let blockchain_config = Arc::clone(&self.blockchain_config);

        let mut repo_clone = self.repository.clone();
        let node_config = self.node_config.clone();
        let produced_blocks = self.produced_blocks.clone();
        let timeout = self.block_produce_timeout.clone();
        let thread_id_clone = *thread_id;
        let iteration_start_clone = self.iteration_start.clone();
        let (control_tx, external_control_rx) = std::sync::mpsc::channel();
        let (epoch_block_keeper_data_tx, epoch_block_keeper_data_rx) = std::sync::mpsc::channel();
        let thread_sync_buffer_clone = self.thread_sync_buffer.clone();
        let handler =
            std::thread::Builder::new().name("Production".to_string()).spawn(move || {
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
                    tracing::trace!("Start block production process iteration");

                    let messages = initial_state.get_remaining_ext_messages(&repo_clone).unwrap();
                    let message_queue: VecDeque<Message> =
                        messages.clone().into_iter().map(|wrap| wrap.message).collect();
                    let mut epoch_block_keeper_data = vec![];
                    while let Ok(data) = epoch_block_keeper_data_rx.try_recv() {
                        tracing::trace!("Received data for epoch: {data:?}");
                        epoch_block_keeper_data.push(data);
                    }

                    let producer = TVMBlockProducer::builder()
                        .active_threads(active_block_producer_threads)
                        .blockchain_config(blockchain_config.clone())
                        .message_queue(message_queue)
                        .node_config(node_config.clone())
                        .epoch_block_keeper_data(epoch_block_keeper_data)
                        .build();

                    let (control_tx, control_rx) = std::sync::mpsc::channel();
                    {
                        let mut iteration_start = iteration_start_clone.lock();
                        *iteration_start = std::time::Instant::now();
                    }
                    let mut refs = vec![];
                    {
                        let mut buffer = thread_sync_buffer_clone.lock();
                        tracing::trace!("Loaded sync info buffer: {buffer:?}");
                        for info in buffer.iter() {
                            if info.destination_thread_ids.contains(&thread_id_clone) {
                                tracing::trace!("Add ref for block_id: {:?}", info.block_id);
                                let state = repo_clone
                                    .get_optimistic_state(&info.block_id)
                                    .expect("Failed to load ref state")
                                    .expect("Failed to load ref state");
                                refs.push(state);
                            }
                        }
                        buffer.clear();
                    }
                    let initial_state_clone = initial_state.clone();
                    let thread = std::thread::Builder::new()
                        .name("Produce block".into())
                        .stack_size(16 * 1024 * 1024)
                        .spawn(move || {
                            tracing::trace!("start production thread");
                            let (block, result_state, active_block_producer_threads) = producer
                                // TODO: add refs to other thread states in case of sync
                                .produce(initial_state_clone, refs.iter(), control_rx)
                                .expect("Failed to produce block");
                            (block, result_state, active_block_producer_threads)
                        })
                        .unwrap();
                    let cur_timeout = { *timeout.lock() };
                    tracing::trace!("Sleep for {cur_timeout:?}");
                    sleep(cur_timeout);

                    tracing::trace!("Send signal to stop production");
                    let _ = control_tx.send(());
                    let (block, result_state, new_active_block_producer_threads) =
                        thread.join().expect("Failed to join producer thread");
                    if let Ok(()) = external_control_rx.try_recv() {
                        repo_clone.store_optimistic(initial_state.clone()).unwrap();
                        tracing::trace!("Stop production process: {}", &thread_id_clone);
                        return initial_state;
                    }
                    tracing::trace!("Produced block: {}", block);
                    active_block_producer_threads = new_active_block_producer_threads;

                    initial_state = result_state;
                    {
                        tracing::trace!("Save produced block");
                        let mut blocks = produced_blocks.lock();
                        let processed_ext_messages = block.processed_ext_messages_cnt();
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
    ) -> Vec<(
        AckiNackiBlock<<Self as BlockProducerProcess>::BLSSignatureScheme>,
        OptimisticStateImpl,
        usize,
    )> {
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use parking_lot::Mutex;

    use crate::block::producer::process::BlockProducerProcess;
    use crate::block::producer::process::TVMBlockProducerProcess;
    use crate::repository::repository_impl::RepositoryImpl;
    use crate::repository::Repository;
    use crate::types::ThreadIdentifier;
    use crate::types::ThreadsTable;

    #[test]
    #[ignore]
    fn test_producer() -> anyhow::Result<()> {
        crate::tests::init_db("./tmp/node-data-5").expect("Failed to init DB 1");
        let config = crate::tests::default_config(1);
        let repository = RepositoryImpl::new(
            PathBuf::from("./tmp/node-data-5"),
            Some(config.local.zerostate_path.clone()),
            1,
        );

        let mut production_process = TVMBlockProducerProcess::new(
            config.clone(),
            repository.clone(),
            None,
            Arc::new(Mutex::new(vec![])),
        )?;

        let thread_id = ThreadIdentifier::default();
        let threads_table = ThreadsTable::new();
        let (prev_block_id, _prev_block_seq_no) =
            repository.select_thread_last_main_candidate_block(&thread_id)?;

        production_process.start_thread_production(&thread_id, &threads_table, &prev_block_id)?;

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
