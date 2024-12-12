// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::VecDeque;
use std::path::Path;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use database::documents_db::DocumentsDb;
use parking_lot::Mutex;
use serde_json::Value;
use tvm_block::BlkPrevInfo;
use tvm_block::ExtBlkRef;
use tvm_executor::BlockchainConfig;
use tvm_types::UInt256;

use crate::block::producer::errors::VerifyError;
use crate::block::producer::errors::BP_DID_NOT_PROCESS_ALL_MESSAGES_FROM_PREVIOUS_BLOCK;
use crate::block::producer::BlockProducer;
use crate::block::producer::TVMBlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::database::write_to_db;
use crate::multithreading::thread_synchrinization_service::ThreadSyncInfo;
use crate::node::attestation_processor::LOOP_PAUSE_DURATION;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockInfo;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

pub trait BlockKeeperProcess {
    type BLSSignatureScheme;
    type CandidateBlock;
    type OptimisticState;

    fn validate<T: Into<Self::CandidateBlock>>(&mut self, block: T) -> anyhow::Result<()>;
    fn apply_block<T: Into<Self::CandidateBlock>>(&mut self, block: T) -> anyhow::Result<()>;
    fn is_candidate_block_can_be_applied(&self, parent_block_id: &BlockIdentifier) -> bool;
    fn clear_queue(&mut self) -> anyhow::Result<()>;
    fn get_verification_results(&self) -> anyhow::Result<Vec<(BlockIdentifier, BlockSeqNo, bool)>>;
    fn get_last_state(&self) -> Self::OptimisticState;
}

pub struct TVMBlockKeeperProcess {
    // queue_of_blocks_to_process (block, do_verification)
    blocks_queue: Arc<Mutex<VecDeque<(<Self as BlockKeeperProcess>::CandidateBlock, bool)>>>,
    last_state_id: Arc<Mutex<BlockIdentifier>>,
    verified_blocks_with_result: Arc<Mutex<Vec<(BlockIdentifier, BlockSeqNo, bool)>>>,
    handler: std::thread::JoinHandle<()>,
    last_state: Arc<Mutex<OptimisticStateImpl>>,
}

impl TVMBlockKeeperProcess {
    pub fn new<P: AsRef<Path>>(
        blockchain_config_path: P,
        mut repository: RepositoryImpl,
        node_config: Config,
        archive: Option<Arc<dyn DocumentsDb>>,
        thread_id: ThreadIdentifier,
        thread_sync_tx: Sender<ThreadSyncInfo>,
        threads_table: ThreadsTable,
    ) -> anyhow::Result<Self> {
        let blocks_queue = Arc::new(Mutex::new(VecDeque::<(
            Envelope<GoshBLS, AckiNackiBlock<<Self as BlockKeeperProcess>::BLSSignatureScheme>>,
            bool,
        )>::new()));
        let blockchain_config_path = blockchain_config_path.as_ref();
        let json = std::fs::read_to_string(blockchain_config_path)?;
        let map = serde_json::from_str::<serde_json::Map<String, Value>>(&json)?;
        let config_params = tvm_block_json::parse_config(&map).map_err(|e| {
            anyhow::format_err!(
                "Failed to load config params from file {blockchain_config_path:?}: {e}"
            )
        })?;
        let blockchain_config = BlockchainConfig::with_config(config_params)
            .map_err(|e| anyhow::format_err!("Failed to create blockchain config: {e}"))?;

        let blocks_queue_clone = blocks_queue.clone();
        let verified_blocks_with_result = Arc::new(Mutex::new(vec![]));
        let verified_blocks_with_result_clone = verified_blocks_with_result.clone();
        let last_state_id = Arc::new(Mutex::new(BlockIdentifier::default()));
        let last_state_id_clone = last_state_id.clone();
        let blockchain_config = Arc::new(blockchain_config);
        let last_state = Arc::new(Mutex::new(
            repository
                .get_zero_state_for_thread(&thread_id)
                .expect("Failed to get optimistic state from zerostate"),
        ));
        let operational_state = last_state.clone();
        let handler: std::thread::JoinHandle<()> = std::thread::Builder::new()
            .name("Block keeper process".to_string())
            .spawn(move || {
                loop {
                    let mut last_state = { operational_state.lock().clone() };
                    #[cfg(feature = "timing")]
                    let start = std::time::Instant::now();
                    let next_envelope = {
                        let mut blocks_queue = blocks_queue_clone.lock();

                        // tracing::trace!("Popped from queue: {block:?}");
                        blocks_queue.pop_front()
                    };
                    if next_envelope.is_none() {
                        std::thread::sleep(LOOP_PAUSE_DURATION);
                        continue;
                    }
                    let (next_envelope, do_verify) = next_envelope.unwrap();
                    let next_block = next_envelope.data().clone();
                    // let (next_block, do_verify): (_, _) = next_block.unwrap();
                    tracing::trace!(
                        "Block keeper process: apply and verify({do_verify}) block: {:?}, seq_no: {}",
                        next_block.identifier(),
                        next_block.seq_no(),
                    );

                    let prev_block_id = next_block.parent();
                    let last_block_id = { last_state_id_clone.lock().clone() };
                    if prev_block_id != last_block_id {
                        tracing::trace!(
                            "last state is not valid for incoming block, load from repo"
                        );
                        last_state = if let Some(last_state) = repository
                            .get_optimistic_state(&prev_block_id)
                            .expect("Failed to get optimistic state of the previous block")
                        {
                            last_state
                        } else {
                            continue;
                        };
                    }
                    if do_verify {
                        let mut refs = vec![];
                        for block_id in &next_block.get_common_section().refs {
                            let state = repository
                                .get_optimistic_state(block_id)
                                .expect("Failed to load ref state")
                                .expect("Failed to load ref state");
                            refs.push(state);
                        }
                        let verify_res = verify_block(
                            &next_block,
                            blockchain_config.clone(),
                            &mut last_state,
                            node_config.clone(),
                            refs,
                        )
                        .expect("Failed to verify block");
                        if !verify_res {
                            tracing::trace!("Block verification failed");
                        }
                        let mut dump = verified_blocks_with_result_clone.lock();
                        dump.push((next_block.identifier(), next_block.seq_no(), verify_res));
                        // }
                    } else {
                        last_state.apply_block(&next_block).expect("Failed to apply block");
                    }
                    if last_state
                        .does_state_has_messages_to_other_threads()
                        .expect("Failed to check optimistic state for crossthread messages")
                    {
                        tracing::trace!(
                            "State has messages for other threads: {:?}",
                            last_state.block_id
                        );
                        // TODO: mark all threads as destination for now, fix it later
                        let destination_thread_ids = threads_table
                            .list_threads()
                            .filter(|id| **id != thread_id)
                            .cloned()
                            .collect();
                        let sync_info = ThreadSyncInfo {
                            source_thread_id: thread_id,
                            block_id: last_state.block_id.clone(),
                            destination_thread_ids,
                        };
                        tracing::trace!("send ThreadSyncInfo: {:?}", sync_info);
                        thread_sync_tx.send(sync_info).expect("Failed to send sync info");
                    }
                    {
                        let mut last_state_id = last_state_id_clone.lock();
                        *last_state_id = next_block.identifier();
                        let mut saved_state = operational_state.lock();
                        *saved_state = last_state.clone();
                    }
                    let (_last_block_id, _last_seq_no) =
                        (next_block.identifier(), next_block.seq_no());
                    if let Some(archive) = archive.clone() {
                        write_to_db(
                            archive,
                            next_envelope,
                            last_state.shard_state.shard_state.clone(),
                            last_state.shard_state.shard_state_cell.clone(),
                            // repository.clone(),
                        )
                        .expect("Failed to write block data to db");
                    }
                    let seq_no = next_block.seq_no();
                    if next_block.directives().share_state_resource_address.is_some()
                        || (seq_no % node_config.global.save_state_frequency == 0)
                    {
                        repository
                            .store_optimistic(last_state.clone())
                            .expect("Failed to store optimistic state");
                    }
                    #[cfg(feature = "timing")]
                    tracing::trace!(
                        "Block keeper process: applied block: {_last_seq_no} {_last_block_id} {}ms",
                        start.elapsed().as_millis()
                    );
                }
            })?;

        Ok(TVMBlockKeeperProcess {
            handler,
            blocks_queue,
            verified_blocks_with_result,
            last_state_id,
            last_state,
        })
    }
}

impl BlockKeeperProcess for TVMBlockKeeperProcess {
    type BLSSignatureScheme = GoshBLS;
    type CandidateBlock = Envelope<GoshBLS, AckiNackiBlock<Self::BLSSignatureScheme>>;
    type OptimisticState = OptimisticStateImpl;

    fn validate<T: Into<Self::CandidateBlock>>(&mut self, block: T) -> anyhow::Result<()> {
        let block_candidate: Self::CandidateBlock = block.into();
        let mut blocks_queue = self.blocks_queue.lock();
        tracing::trace!("Add block to verify queue: {:?}", block_candidate.data().identifier());
        blocks_queue.push_back((block_candidate, true));
        Ok(())
    }

    fn apply_block<T: Into<Self::CandidateBlock>>(&mut self, block: T) -> anyhow::Result<()> {
        let block_candidate: Self::CandidateBlock = block.into();
        let mut blocks_queue = self.blocks_queue.lock();
        tracing::trace!("Add block to apply queue: {:?}", block_candidate.data().identifier());
        blocks_queue.push_back((block_candidate, false));
        Ok(())
    }

    fn is_candidate_block_can_be_applied(&self, parent_block_id: &BlockIdentifier) -> bool {
        let last_state_id = { self.last_state_id.lock().clone() };
        if &last_state_id == parent_block_id {
            true
        } else {
            // Parent block can be in the queue
            let queue = self.blocks_queue.lock();
            for (block, _) in queue.iter() {
                if &block.data().identifier() == parent_block_id {
                    return true;
                }
            }
            false
        }
    }

    fn clear_queue(&mut self) -> anyhow::Result<()> {
        tracing::trace!("Clear keeper process queue");
        let mut blocks_queue = self.blocks_queue.lock();
        blocks_queue.clear();
        Ok(())
    }

    fn get_verification_results(&self) -> anyhow::Result<Vec<(BlockIdentifier, BlockSeqNo, bool)>> {
        if self.handler.is_finished() {
            anyhow::bail!("Validation process should not stop");
        }
        let mut verified_blocks = self.verified_blocks_with_result.lock();
        let res = verified_blocks.clone();
        verified_blocks.clear();
        Ok(res)
    }

    fn get_last_state(&self) -> Self::OptimisticState {
        self.last_state.lock().clone()
    }
}

fn verify_block(
    block_candidate: &AckiNackiBlock<GoshBLS>,
    blockchain_config: Arc<BlockchainConfig>,
    prev_block_optimistic_state: &mut OptimisticStateImpl,
    node_config: Config,
    refs: Vec<OptimisticStateImpl>,
) -> anyhow::Result<bool> {
    #[cfg(feature = "timing")]
    let start = std::time::Instant::now();
    tracing::trace!("Verifying block: {:?}", block_candidate.identifier());

    let producer = TVMBlockProducer::builder()
        .active_threads(vec![])
        .blockchain_config(blockchain_config)
        .message_queue(VecDeque::new())
        .node_config(node_config)
        .epoch_block_keeper_data(vec![])
        .build();

    // TODO: need to refactor this point to reuse generated verify block
    let verification_block_production_result = producer.generate_verify_block(
        block_candidate,
        prev_block_optimistic_state.clone(),
        refs.iter(),
    );
    tracing::trace!(
        "Verify block generation result: {:?}",
        verification_block_production_result.as_ref().map(|(block, _)| block.identifier())
    );
    if let Err(error) = &verification_block_production_result {
        if let Some(verify_error) = error.downcast_ref::<VerifyError>() {
            // TODO: need to set Nack reason in this case
            tracing::trace!("verify block generation returned VerifyError: {verify_error:?}");
            if verify_error.code == BP_DID_NOT_PROCESS_ALL_MESSAGES_FROM_PREVIOUS_BLOCK {
                return Ok(false);
            }
        }
    }

    let (verify_block, mut verify_state) = verification_block_production_result?;
    let mut res = verify_block.tvm_block() == block_candidate.tvm_block();
    if !res {
        tracing::trace!("Verify block is not equal to the incoming, do the partial eq check");
        // There could be a special case when blocks have slightly different state
        // updates with the same result. So change blocks without state updates
        // and result state.
        let mut partial_compare_res = true;
        if verify_block.tvm_block().global_id != block_candidate.tvm_block().global_id {
            partial_compare_res = false;
            tracing::trace!(
                "Unequal global id: {} {}",
                verify_block.tvm_block().global_id,
                block_candidate.tvm_block().global_id,
            );
        }

        if verify_block.tvm_block().info != block_candidate.tvm_block().info {
            partial_compare_res = false;
            tracing::trace!(
                "Unequal info: {:?} {:?}",
                verify_block.tvm_block().read_info().expect("failed to read block info"),
                block_candidate.tvm_block().read_info().expect("failed to read block info"),
            );
        }
        if verify_block.tvm_block().value_flow != block_candidate.tvm_block().value_flow {
            partial_compare_res = false;
            tracing::trace!(
                "Unequal value_flow: {:?} {:?}",
                verify_block.tvm_block().read_value_flow().expect("failed to read block info"),
                block_candidate.tvm_block().read_value_flow().expect("failed to read block info"),
            );
        }
        if verify_block.tvm_block().out_msg_queue_updates
            != block_candidate.tvm_block().out_msg_queue_updates
        {
            partial_compare_res = false;
            tracing::trace!(
                "Unequal out_msg_queue_updates: {:?} {:?}",
                verify_block.tvm_block().out_msg_queue_updates,
                block_candidate.tvm_block().out_msg_queue_updates,
            );
        }
        if verify_block.tvm_block().extra != block_candidate.tvm_block().extra {
            partial_compare_res = false;
            tracing::trace!(
                "Unequal extra: {:?} {:?}",
                verify_block.tvm_block().read_extra().expect("failed to read block info"),
                block_candidate.tvm_block().read_extra().expect("failed to read block info"),
            );
        }
        let candidate_block_state_update =
            block_candidate.tvm_block().read_state_update().map_err(|e| {
                anyhow::format_err!("Failed to read state update of candidate block: {e}")
            })?;
        let verify_block_state_update = verify_block
            .tvm_block()
            .read_state_update()
            .map_err(|e| anyhow::format_err!("Failed to read state update of verify block: {e}"))?;

        if candidate_block_state_update.new_hash != verify_block_state_update.new_hash {
            partial_compare_res = false;
            tracing::trace!(
                "Unequal update new_hash: {:?} {:?}",
                verify_block_state_update.new_hash,
                candidate_block_state_update.new_hash,
            );
        }
        if candidate_block_state_update.old_hash != verify_block_state_update.old_hash {
            partial_compare_res = false;
            tracing::trace!(
                "Unequal update old_hash: {:?} {:?}",
                verify_block_state_update.old_hash,
                candidate_block_state_update.old_hash,
            );
        }

        res = partial_compare_res;
        if !res {
            tracing::trace!("Verification failed");
            tracing::trace!("{:?}", verify_block.tvm_block());
            tracing::trace!("{:?}", block_candidate.tvm_block());
        }

        // In this case block hashes are not equal so set up verify state block id to match parent id
        verify_state.block_id = block_candidate.identifier();
        verify_state.block_info = prepare_prev_block_info(block_candidate);
    }

    *prev_block_optimistic_state = verify_state;

    #[cfg(feature = "timing")]
    tracing::trace!(
        "Verify block {:?} time: {} ms",
        block_candidate.identifier(),
        start.elapsed().as_millis()
    );
    Ok(res)
}

pub fn prepare_prev_block_info(block_candidate: &AckiNackiBlock<GoshBLS>) -> BlockInfo {
    #[cfg(feature = "timing")]
    let start = std::time::Instant::now();
    let info = block_candidate.tvm_block().read_info().unwrap();
    let (serialized_block, cell) = block_candidate.raw_block_data().unwrap();
    let root_hash = cell.repr_hash();

    let file_hash = UInt256::calc_file_hash(&serialized_block);
    #[cfg(feature = "timing")]
    tracing::trace!("prepare_prev_block_info time: {} ms", start.elapsed().as_millis());
    BlkPrevInfo::Block {
        prev: ExtBlkRef { end_lt: info.end_lt(), seq_no: info.seq_no(), root_hash, file_hash },
    }
    .into()
}
