// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;

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
use crate::block::Block;
use crate::block::BlockIdentifier;
use crate::block::BlockSeqNo;
use crate::block::WrappedBlock;
use crate::block::WrappedUInt256;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::database::documents_db::DocumentsDb;
use crate::database::write_to_db;
use crate::node::attestation_processor::LOOP_PAUSE_DURATION;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;

pub trait BlockKeeperProcess {
    type CandidateBlock;
    type Block: Block<BlockSeqNo = Self::BlockSeqNo, BlockIdentifier = Self::BlockIdentifier>;
    type BlockIdentifier: BlockIdentifier;
    type BlockSeqNo: BlockSeqNo;

    fn validate<T: Into<Self::CandidateBlock>>(&mut self, block: T) -> anyhow::Result<()>;
    fn apply_block<T: Into<Self::CandidateBlock>>(&mut self, block: T) -> anyhow::Result<()>;
    fn is_candidate_block_can_be_applied(&self, parent_block_id: &Self::BlockIdentifier) -> bool;
    fn clear_queue(&mut self) -> anyhow::Result<()>;
    fn get_verification_results(
        &self,
    ) -> anyhow::Result<Vec<(Self::BlockIdentifier, Self::BlockSeqNo, bool)>>;
}

pub struct TVMBlockKeeperProcess {
    // queue_of_blocks_to_process (block, do_verification)
    blocks_queue: Arc<Mutex<VecDeque<(<Self as BlockKeeperProcess>::CandidateBlock, bool)>>>,
    last_state_id: Arc<Mutex<WrappedUInt256>>,
    verified_blocks_with_result: Arc<Mutex<Vec<(WrappedUInt256, u32, bool)>>>,
    handler: std::thread::JoinHandle<()>,
}

impl TVMBlockKeeperProcess {
    pub fn new<P: AsRef<Path>>(
        blockchain_config_path: P,
        mut repository: RepositoryImpl,
        node_config: Config,
        archive: Option<Arc<dyn DocumentsDb>>,
    ) -> anyhow::Result<Self> {
        let blocks_queue =
            Arc::new(Mutex::new(VecDeque::<(Envelope<GoshBLS, WrappedBlock>, bool)>::new()));
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
        let last_state_id = Arc::new(Mutex::new(WrappedUInt256::default()));
        let last_state_id_clone = last_state_id.clone();
        let blockchain_config = Arc::new(blockchain_config);
        let handler: std::thread::JoinHandle<()> = std::thread::Builder::new()
            .name("Block keeper process".to_string())
            .spawn(move || {
                let mut last_state = repository
                    .get_optimistic_state(&WrappedUInt256::default())
                    .expect("Failed to get optimistic state of zero block")
                    .expect("Failed to get optimistic state of zero block");
                loop {
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
                        "Block keeper process: apply and verify({do_verify}) block: {next_block}"
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
                        let verify_res = verify_block(
                            &next_block,
                            blockchain_config.clone(),
                            &mut last_state,
                            node_config.clone(),
                        )
                        .expect("Failed to verify block");
                        if !verify_res {
                            tracing::trace!("Block verification failed");
                        }
                        let mut dump = verified_blocks_with_result_clone.lock();
                        dump.push((next_block.identifier(), next_block.seq_no(), verify_res));
                        // }
                    } else {
                        apply_block(&next_block, &mut last_state).expect("Failed to apply block");
                    }
                    {
                        let mut last_state_id = last_state_id_clone.lock();
                        *last_state_id = next_block.identifier();
                    }
                    let (last_block_id, last_seq_no) =
                        (next_block.identifier(), next_block.seq_no());
                    if let Some(archive) = archive.clone() {
                        write_to_db(
                            archive,
                            next_envelope,
                            last_state.shard_state.clone(),
                            last_state.shard_state_cell.clone(),
                            // repository.clone(),
                        )
                        .expect("Failed to write block data to db");
                    }
                    let seq_no: u64 = next_block.seq_no().into();
                    if next_block.directives().share_state_resource_address.is_some()
                        || (seq_no % node_config.global.save_state_frequency == 0)
                    {
                        repository
                            .store_optimistic(last_state.clone())
                            .expect("Failed to store optimistic state");
                    }
                    tracing::trace!(
                        "Block keeper process: applied block: {last_seq_no} {last_block_id} {}ms",
                        start.elapsed().as_millis()
                    );
                }
            })?;

        Ok(TVMBlockKeeperProcess {
            handler,
            blocks_queue,
            verified_blocks_with_result,
            last_state_id,
        })
    }
}

impl BlockKeeperProcess for TVMBlockKeeperProcess {
    type Block = WrappedBlock;
    type BlockIdentifier = <Self::Block as Block>::BlockIdentifier;
    type BlockSeqNo = <Self::Block as Block>::BlockSeqNo;
    type CandidateBlock = Envelope<GoshBLS, Self::Block>;

    fn validate<T: Into<Self::CandidateBlock>>(&mut self, block: T) -> anyhow::Result<()> {
        let block_candidate: Self::CandidateBlock = block.into();
        let mut blocks_queue = self.blocks_queue.lock();
        tracing::trace!("Add block to verify queue: {}", block_candidate);
        blocks_queue.push_back((block_candidate, true));
        Ok(())
    }

    fn apply_block<T: Into<Self::CandidateBlock>>(&mut self, block: T) -> anyhow::Result<()> {
        let block_candidate: Self::CandidateBlock = block.into();
        let mut blocks_queue = self.blocks_queue.lock();
        tracing::trace!("Add block to apply queue: {}", block_candidate);
        blocks_queue.push_back((block_candidate, false));
        Ok(())
    }

    fn is_candidate_block_can_be_applied(&self, parent_block_id: &Self::BlockIdentifier) -> bool {
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

    fn get_verification_results(&self) -> anyhow::Result<Vec<(Self::BlockIdentifier, u32, bool)>> {
        if self.handler.is_finished() {
            anyhow::bail!("Validation process should not stop");
        }
        let mut verified_blocks = self.verified_blocks_with_result.lock();
        let res = verified_blocks.clone();
        verified_blocks.clear();
        Ok(res)
    }
}

fn verify_block(
    block_candidate: &WrappedBlock,
    blockchain_config: Arc<BlockchainConfig>,
    prev_block_optimistic_state: &mut OptimisticStateImpl,
    node_config: Config,
) -> anyhow::Result<bool> {
    let start = std::time::Instant::now();
    tracing::trace!("Verifying block: {:?}", block_candidate.identifier());

    let prev_shard_state_cell = prev_block_optimistic_state.get_shard_state_as_cell();
    let prev_block_info = prev_block_optimistic_state.get_block_info();

    let producer = TVMBlockProducer::new(
        node_config,
        blockchain_config,
        VecDeque::new(),
        prev_shard_state_cell,
        prev_block_info,
        vec![],
        vec![],
    );

    // TODO: need to refactor this point to reuse generated verify block
    let verification_block_production_result =
        producer.generate_verify_block(block_candidate.clone());
    tracing::trace!("Verify block generation result: {verification_block_production_result:?}");
    if let Err(error) = &verification_block_production_result {
        if let Some(verify_error) = error.downcast_ref::<VerifyError>() {
            // TODO: need to set Nack reason in this case
            tracing::trace!("verify block generation returned VerifyError: {verify_error:?}");
            if verify_error.code == BP_DID_NOT_PROCESS_ALL_MESSAGES_FROM_PREVIOUS_BLOCK {
                return Ok(false);
            }
        }
    }

    let (verify_block, verify_state, state_cell) = verification_block_production_result?;
    let mut res = verify_block.block == block_candidate.block;
    if !res {
        // There could be a special case when blocks have slightly different state
        // updates with the same result. So change blocks without state updates
        // and result state.
        let mut partial_compare_res = true;
        partial_compare_res = partial_compare_res
            && (verify_block.block.global_id == block_candidate.block.global_id);
        partial_compare_res =
            partial_compare_res && (verify_block.block.info == block_candidate.block.info);
        partial_compare_res = partial_compare_res
            && (verify_block.block.value_flow == block_candidate.block.value_flow);
        partial_compare_res = partial_compare_res
            && (verify_block.block.out_msg_queue_updates
                == block_candidate.block.out_msg_queue_updates);
        partial_compare_res =
            partial_compare_res && (verify_block.block.extra == block_candidate.block.extra);
        let candidate_block_state_update =
            block_candidate.block.read_state_update().map_err(|e| {
                anyhow::format_err!("Failed to read state update of candidate block: {e}")
            })?;
        let verify_block_state_update = verify_block
            .block
            .read_state_update()
            .map_err(|e| anyhow::format_err!("Failed to read state update of verify block: {e}"))?;
        partial_compare_res = partial_compare_res
            && (candidate_block_state_update.new_hash == verify_block_state_update.new_hash);
        partial_compare_res = partial_compare_res
            && (candidate_block_state_update.old_hash == verify_block_state_update.old_hash);

        res = partial_compare_res;
        if !res {
            tracing::trace!("Verification failed");
            tracing::trace!("{:?}", verify_block.block);
            tracing::trace!("{:?}", block_candidate.block);
        }
    }

    let block_info = prepare_prev_block_info(block_candidate);
    let last_processed_messages_index = prev_block_optimistic_state
        .last_processed_external_message_index
        + block_candidate.processed_ext_messages_cnt as u32;
    *prev_block_optimistic_state =
        OptimisticStateImpl::from_shard_state_shard_state_cell_and_block_info(
            block_candidate.identifier(),
            Arc::new(verify_state),
            state_cell,
            block_info,
            last_processed_messages_index,
        )?;

    log::trace!(
        "Verify block {:?} time: {} ms",
        block_candidate.identifier(),
        start.elapsed().as_millis()
    );
    Ok(res)
}

fn apply_block(
    block_candidate: &WrappedBlock,
    prev_block_optimistic_state: &mut OptimisticStateImpl,
) -> anyhow::Result<()> {
    let block_id = block_candidate.identifier();
    tracing::trace!("Applying block: {:?}", block_id);
    let start = std::time::Instant::now();
    let last_processed_messages_index = prev_block_optimistic_state
        .last_processed_external_message_index
        + block_candidate.processed_ext_messages_cnt as u32;
    if block_candidate.tx_cnt == 0 {
        tracing::trace!("Incoming block has no txns, just add seq_no and block info");
        let mut prev_state = (*prev_block_optimistic_state.get_shard_state()).clone();
        let block_info = prepare_prev_block_info(block_candidate);
        prev_state.set_seq_no(prev_state.seq_no() + 1);
        *prev_block_optimistic_state = OptimisticStateImpl::from_shard_state_and_block_info(
            block_candidate.identifier(),
            Arc::new(prev_state),
            block_info,
            last_processed_messages_index,
        )?;
    } else {
        let prev_state = prev_block_optimistic_state.get_shard_state_as_cell();
        tracing::trace!("Applying block loaded state");

        log::trace!(target: "node", "apply_block: Old state hash: {:?}", prev_state.repr_hash());
        let state_update = block_candidate
            .block
            .read_state_update()
            .map_err(|e| anyhow::format_err!("Failed to read block state update: {e}"))?;
        tracing::trace!("Applying block loaded state update");
        let apply_timer = std::time::Instant::now();
        let new_state = state_update
            .apply_for(&prev_state)
            .map_err(|e| anyhow::format_err!("Failed to apply state update: {e}"))?;
        log::trace!(target: "node", "apply_block: update has taken {}ms", apply_timer.elapsed().as_millis());
        log::trace!(target: "node", "apply_block: New state hash: {:?}", new_state.repr_hash());

        let block_info = prepare_prev_block_info(block_candidate);
        tracing::trace!("Calculated block info");

        *prev_block_optimistic_state = OptimisticStateImpl::from_shard_state_cell_and_block_info(
            block_candidate.identifier(),
            new_state,
            block_info,
            last_processed_messages_index,
        )?;
    }
    log::trace!("Apply block {block_id:?} time: {} ms", start.elapsed().as_millis());
    Ok(())
}

pub fn prepare_prev_block_info(block_candidate: &WrappedBlock) -> BlkPrevInfo {
    let start = std::time::Instant::now();
    let info = block_candidate.block.read_info().unwrap();
    let (serialized_block, cell) = block_candidate.raw_block_data().unwrap();
    let root_hash = cell.repr_hash();

    let file_hash = UInt256::calc_file_hash(&serialized_block);
    tracing::trace!("prepare_prev_block_info time: {} ms", start.elapsed().as_millis());
    BlkPrevInfo::Block {
        prev: ExtBlkRef { end_lt: info.end_lt(), seq_no: info.seq_no(), root_hash, file_hash },
    }
}
