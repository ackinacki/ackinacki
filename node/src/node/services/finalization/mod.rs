// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::sync::atomic::Ordering;
use std::sync::Arc;

use telemetry_utils::mpsc::InstrumentedSender;
use tracing::trace_span;
use tvm_types::AccountId;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::helper::block_flow_trace;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message_storage::MessageDurableStorage;
use crate::node::services::sync::StateSyncService;
use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::node::ExternalMessagesThreadState;
use crate::node::NodeIdentifier;
use crate::node::SharedServices;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::CrossThreadRefDataRead;
use crate::repository::Repository;
use crate::types::bp_selector::BlockGap;
use crate::types::AckiNackiBlock;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[allow(clippy::too_many_arguments)]
pub fn finalization_loop(
    mut repository: RepositoryImpl,
    block_state_repository: BlockStateRepository,
    mut shared_services: SharedServices,
    mut raw_block_tx: InstrumentedSender<(AccountId, Vec<u8>)>,
    _state_sync_service: impl StateSyncService<Repository = RepositoryImpl>,
    block_gap: BlockGap,
    metrics: Option<BlockProductionMetrics>,
    external_messages: ExternalMessagesThreadState,
    _message_db: MessageDurableStorage,
    node_id: &NodeIdentifier,
) {
    tracing::trace!("try_finalize_blocks start");
    loop {
        let before = repository
            .unprocessed_blocks_cache()
            .notifications()
            .load(std::sync::atomic::Ordering::Relaxed);

        #[allow(clippy::mutable_key_type)]
        let unprocessed_blocks = { repository.unprocessed_blocks_cache().clone_queue() };
        for (_, (block_state, candidate_block)) in unprocessed_blocks {
            try_finalize(
                &block_state,
                &mut repository,
                &block_state_repository,
                &mut shared_services,
                &mut raw_block_tx,
                &block_gap,
                &metrics,
                &external_messages,
                candidate_block,
                node_id,
            )
            .expect("try_finalize iteration failed");
        }
        let after = repository
            .unprocessed_blocks_cache()
            .notifications()
            .load(std::sync::atomic::Ordering::Relaxed);
        if after == before {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn try_finalize(
    block_state: &BlockState,
    repository: &mut RepositoryImpl,
    block_state_repository: &BlockStateRepository,
    shared_services: &mut SharedServices,
    raw_block_tx: &mut InstrumentedSender<(AccountId, Vec<u8>)>,
    block_gap: &BlockGap,
    metrics: &Option<BlockProductionMetrics>,
    external_messages: &ExternalMessagesThreadState,
    candidate_block: Arc<Envelope<GoshBLS, AckiNackiBlock>>,
    node_id: &NodeIdentifier,
) -> anyhow::Result<()> {
    tracing::trace!(
        "try_finalize_blocks: process: {:?}",
        block_state.guarded(|e| (*e.block_seq_no(), e.block_identifier().clone()))
    );
    let block_id = candidate_block.data().identifier();
    let parent_id = candidate_block.data().parent();
    let parent_state = block_state_repository.get(&parent_id)?;
    if parent_state.guarded(|e| !e.is_finalized()) {
        tracing::trace!("try_finalize_blocks parent is not finalized {parent_id:?}");
        return Ok(());
    }
    block_state_repository.get(&block_id)?.lock().set_has_parent_finalized()?;
    if block_state.guarded(|e| e.has_cross_thread_ref_data_prepared() != &Some(true)) {
        let block_ref_data_exists = shared_services.exec(|services| {
            services.cross_thread_ref_data_service.get_cross_thread_ref_data(&block_id).is_ok()
        });
        if block_ref_data_exists {
            block_state.guarded_mut(|e| e.set_has_cross_thread_ref_data_prepared()).map_err(
                |e| {
                    tracing::trace!(
                        "try_finalize_blocks failed to load cross thread ref data for block: {e:?}"
                    );
                    e
                },
            )?;
        }
    }
    if block_state.guarded(|e| e.can_finalize()) {
        on_block_finalized(
            shared_services,
            &candidate_block,
            repository,
            block_state_repository,
            raw_block_tx,
            block_gap,
            external_messages,
        )?;
        let block_seq_no = candidate_block.data().seq_no();
        let block_id = candidate_block.data().identifier();

        block_flow_trace("finalized", &block_id, node_id, []);
        metrics.as_ref().inspect(|x| {
            let thread_id = candidate_block.data().get_common_section().thread_id;
            let seq_no: u32 = block_seq_no.into();
            let tx_count = candidate_block.data().tx_cnt();
            x.report_finalization(seq_no, tx_count, &thread_id);
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
// This funciton is public for benchmarking
pub fn on_block_finalized(
    shared_services: &mut SharedServices,
    block: &Envelope<GoshBLS, AckiNackiBlock>,
    repository: &mut RepositoryImpl,
    block_state_repository: &BlockStateRepository,
    raw_block_tx: &mut InstrumentedSender<(AccountId, Vec<u8>)>,
    block_gap: &BlockGap,
    external_messages: &ExternalMessagesThreadState,
) -> anyhow::Result<()> {
    trace_span!("on_block_finalized").in_scope(|| {
        let block_seq_no = block.data().seq_no();
        let block_id = block.data().identifier();
        let thread_id = block.data().get_common_section().thread_id;
        tracing::info!("on_block_finalized: {:?} {:?}", block_seq_no, block_id);
        repository.mark_block_as_finalized(
            block,
            block_state_repository.get(&block_id)?
        )?;
        block_gap.store(0, Ordering::Relaxed);
        tracing::info!("Block marked as finalized: {:?} {:?} {:?}", block_seq_no, block_id, thread_id);
        let block = repository.get_block(&block_id)?.expect("Just finalized");
        let producer_id = block.data().get_common_section().producer_id.clone();
        tracing::info!(
            "Last finalized block data: seq_no: {:?}, block_id: {:?}, producer_id: {}, signatures: {:?}, thread_id: {:?}, tx_cnt: {}, time: {}",
            block.data().seq_no(),
            block.data().identifier(),
            producer_id,
            block.clone_signature_occurrences(),
            block.data().get_common_section().thread_id,
            block.data().tx_cnt(),
            block.data().time().unwrap_or(0),
        );
        let serialized_block = bincode::serialize(&block)?;
        let bm_bcast_set = (producer_id.into(), serialized_block.clone());
        raw_block_tx.send(bm_bcast_set)?;
        // Share finalized state, producer of this block has already shared this state after block production
        // if block.data().directives().share_state_resources().is_some() {
        //     let _resource_address = state_sync_service.add_share_state_task(
        //         &block,
        //         message_db,
        //         repository,
        //         block_state_repository,
        //         shared_services,
        //     )?;
        //     // broadcast_candidate_block(block.clone())?;
        // }
        shared_services.on_block_finalized(
            block.data(),
            &mut repository.get_optimistic_state(&block.data().identifier(), &thread_id, None)?
                .ok_or(anyhow::anyhow!("Block must be in the repo"))?
        );
        let finalized_progress = external_messages.get_progress(&block_id)?
            .expect("Progress must be stored");
        external_messages.erase_outdated(&finalized_progress)?;
        Ok(())
    })
}
