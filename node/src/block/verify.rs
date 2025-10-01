use std::sync::Arc;

use tvm_block::BlkPrevInfo;
use tvm_block::ExtBlkRef;
use tvm_block::GetRepresentationHash;
use tvm_block::HashmapAugType;
use tvm_executor::BlockchainConfig;
use tvm_executor::ExecutorError;
use tvm_types::UInt256;

use crate::block::producer::errors::VerifyError;
use crate::block::producer::errors::BP_DID_NOT_PROCESS_ALL_MESSAGES_FROM_PREVIOUS_BLOCK;
use crate::block::producer::wasm::WasmNodeCache;
use crate::block::producer::BlockVerifier;
use crate::block::producer::TVMBlockVerifier;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::helper::metrics::BlockProductionMetrics;
use crate::node::associated_types::NackData;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::shared_services::SharedServices;
use crate::repository::accounts::AccountsRepository;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::storage::MessageDurableStorage;
use crate::types::AckiNackiBlock;
use crate::types::BlockInfo;

#[derive(PartialEq)]
pub enum VerificationResult {
    ValidBlock,
    BadBlock,
    TooComplexExecution,
}

impl VerificationResult {
    pub fn is_valid(&self) -> bool {
        *self == VerificationResult::ValidBlock
    }
}

#[allow(clippy::too_many_arguments)]
pub fn verify_block(
    block_candidate: &AckiNackiBlock,
    blockchain_config: Arc<BlockchainConfig>,
    prev_block_optimistic_state: &mut OptimisticStateImpl,
    node_config: Config,
    refs: Vec<CrossThreadRefData>,
    shared_services: SharedServices,
    block_nack: Vec<Envelope<GoshBLS, NackData>>,
    block_state_repo: BlockStateRepository,
    accounts_repo: AccountsRepository,
    metrics: Option<BlockProductionMetrics>,
    wasm_cache: WasmNodeCache,
    message_db: MessageDurableStorage,
) -> anyhow::Result<VerificationResult> {
    #[cfg(feature = "timing")]
    let start = std::time::Instant::now();
    tracing::trace!(
        "Verifying block: {:?} {:?}",
        block_candidate.identifier(),
        block_candidate.seq_no()
    );

    let producer = TVMBlockVerifier::builder()
        .blockchain_config(blockchain_config)
        .node_config(node_config.clone())
        .shared_services(shared_services)
        .epoch_block_keeper_data(vec![])
        .block_nack(block_nack)
        .block_state_repository(block_state_repo)
        .accounts_repository(accounts_repo)
        .metrics(metrics)
        .wasm_cache(wasm_cache)
        .build();

    // TODO: need to refactor this point to reuse generated verify block
    let verification_block_production_result = producer.generate_verify_block(
        block_candidate,
        prev_block_optimistic_state.clone(),
        refs.iter(),
        message_db,
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
                return Ok(VerificationResult::BadBlock);
            }
        }
    }

    if let Err(error) = &verification_block_production_result {
        if let Some(ExecutorError::TerminationDeadlineReached) = error.downcast_ref() {
            tracing::trace!("verify block generation returned TerminationDeadlineReached");
            return Ok(VerificationResult::TooComplexExecution);
        }
    }

    let (verify_block, mut verify_state) = verification_block_production_result?;
    // TODO: validate common section
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

        if verify_block.tx_cnt() != block_candidate.tx_cnt() {
            tracing::trace!(
                "Unequal tx_cnt: verify: {:?} candidate: {:?}",
                verify_block.tx_cnt(),
                block_candidate.tx_cnt()
            );
            tracing::trace!("Candidate txns:");
            display_block_transactions(block_candidate);
            tracing::trace!("verify block txns:");
            display_block_transactions(&verify_block);
        }

        res = partial_compare_res;
        if !res {
            tracing::trace!("Verification failed");
            tracing::trace!("{:?}", verify_block.tvm_block());
            tracing::trace!("{:?}", block_candidate.tvm_block());
            return Ok(VerificationResult::BadBlock);
        }

        // In this case block hashes are not equal so set up verify state block id to match parent id
        verify_state.block_id = block_candidate.identifier();
        verify_state.block_info = prepare_prev_block_info(block_candidate);
        return Ok(VerificationResult::ValidBlock);
    }

    *prev_block_optimistic_state = verify_state;

    #[cfg(feature = "timing")]
    tracing::trace!(
        "Verify block {:?} time: {} ms",
        block_candidate.identifier(),
        start.elapsed().as_millis()
    );
    Ok(VerificationResult::ValidBlock)
}

pub fn prepare_prev_block_info(block_candidate: &AckiNackiBlock) -> BlockInfo {
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

fn display_block_transactions(block: &AckiNackiBlock) {
    let Ok(extra) = block.tvm_block().read_extra() else {
        tracing::trace!("failed to read block extra");
        return;
    };
    let Ok(account_blocks) = extra.read_account_blocks() else {
        tracing::trace!("failed to read account blocks");
        return;
    };
    let Ok(_) = account_blocks.iterate_objects(|account_block| {
        let account_id = account_block.account_id().clone().to_hex_string();
        let Ok(_) = account_block.transactions().iterate_objects(|transaction| {
            let Ok(in_msg) = transaction.read_in_msg() else {
                tracing::trace!("failed to read in msg");
                return Ok(true);
            };
            tracing::trace!(
                "Acc: {account_id} message_hash: {:?} message: {in_msg:?} tx: {transaction:?}",
                in_msg.as_ref().map(|msg| msg.hash().unwrap().to_hex_string())
            );
            Ok(true)
        }) else {
            tracing::trace!("failed to iterate block txns");
            return Ok(true);
        };
        Ok(true)
    }) else {
        tracing::trace!("failed to iterate account blocks");
        return;
    };
}
