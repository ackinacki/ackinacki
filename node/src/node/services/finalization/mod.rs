// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::cmp::max;
use std::sync::mpsc::Sender;
use std::sync::Arc;

use parking_lot::Mutex;
use telemetry_utils::mpsc::InstrumentedSender;
use tracing::trace_span;
use transport_layer::server::RawBlockSaveCommand;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::helper::block_flow_trace;
use crate::helper::metrics::BlockProductionMetrics;
use crate::helper::SHUTDOWN_FINALIZATION_FLAG;
use crate::helper::SHUTDOWN_FLAG;
use crate::node::block_state::tools::connect;
use crate::node::block_state::tools::invalidate_branch;
use crate::node::services::block_processor::chain_pulse::events::ChainPulseEvent;
use crate::node::services::sync::StateSyncService;
use crate::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::node::NodeIdentifier;
use crate::node::SharedServices;
use crate::protocol::authority_switch::action_lock::Authority;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::storage::MessageDurableStorage;
use crate::types::AckiNackiBlock;
use crate::types::BlockSeqNo;
use crate::types::CollectedAttestations;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[allow(clippy::too_many_arguments)]
pub fn finalization_loop(
    mut repository: RepositoryImpl,
    block_state_repository: BlockStateRepository,
    mut shared_services: SharedServices,
    mut raw_block_tx: InstrumentedSender<RawBlockSaveCommand<(NodeIdentifier, Vec<u8>)>>,
    state_sync_service: impl StateSyncService<Repository = RepositoryImpl>,
    metrics: Option<BlockProductionMetrics>,
    _message_db: MessageDurableStorage,
    node_id: &NodeIdentifier,
    authority: Arc<Mutex<Authority>>,
    unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection,
    last_block_attestations: Arc<Mutex<CollectedAttestations>>,
    chain_pulse_monitor: Sender<ChainPulseEvent>,
    thread_identifier: ThreadIdentifier,
) {
    tracing::trace!("try_finalize_blocks start");
    let state_sync_service = Arc::new(state_sync_service);
    loop {
        if SHUTDOWN_FINALIZATION_FLAG.get() == Some(&true) {
            tracing::trace!("break finalization loop");
            return;
        }
        let before = unprocessed_blocks_cache.notifications().stamp();

        #[allow(clippy::mutable_key_type)]
        let (unprocessed_blocks, _) = { unprocessed_blocks_cache.clone_queue() };
        let Ok(Some((last_finalized_block_id, _))) =
            repository.select_thread_last_finalized_block(&thread_identifier)
        else {
            unprocessed_blocks_cache
                .notifications()
                .clone()
                .wait_for_updates_timeout(before, std::time::Duration::from_millis(50));
            continue;
        };
        let last_finalized_block = block_state_repository
            .get(&last_finalized_block_id)
            .expect("Finalized block state must exist");
        let last_finalized_block_height = last_finalized_block
            .guarded(|e| *e.block_height())
            .expect("Finalized block height must be set");
        let next_height = last_finalized_block_height.next(&thread_identifier);
        let last_finalized_block_children_ids = last_finalized_block
            .guarded(|e| e.known_children(&thread_identifier).cloned())
            .unwrap_or_default();
        let mut max_child_deadline = 0;
        for child in last_finalized_block_children_ids {
            let Some(attestation_target) = block_state_repository
                .get(&child)
                .expect("Block state must exist")
                .guarded(|e| *e.attestation_target())
            else {
                continue;
            };
            let deadline = max(
                *attestation_target.primary().generation_deadline() as u64,
                *attestation_target.fallback().generation_deadline() as u64,
            );
            if deadline > max_child_deadline {
                max_child_deadline = deadline;
            }
        }
        // + 1 - add 1 because checked block is a child block from the last finalized
        // + 1 - add 1 because we need a child block from the one that has enough attestations to finalize block
        //       we need an extra candidate block to look in the parents finalizes_blocks field. (beta + 1 protocol)

        let mut height_cutoff = max_child_deadline + 1 + *last_finalized_block_height.height() + 1;
        for (block_state, _candidate_block) in unprocessed_blocks.blocks().values() {
            if SHUTDOWN_FINALIZATION_FLAG.get() == Some(&true) {
                tracing::trace!("break finalization loop");
                return;
            }
            if let Some(height) = block_state.guarded(|e| *e.block_height()) {
                if height == next_height {
                    if let Some(parent) =
                        block_state.guarded(|e| e.parent_block_identifier().clone())
                    {
                        let Ok(parent_block_state) = block_state_repository.get(&parent) else {
                            continue;
                        };
                        connect!(
                            parent = parent_block_state,
                            child = block_state,
                            &block_state_repository
                        );
                    }
                }
                if *height.height() > height_cutoff {
                    break;
                }
            } else {
                continue;
            }
            if let Some(new_border) = try_finalize(
                block_state,
                &mut repository,
                &block_state_repository,
                &mut shared_services,
                &mut raw_block_tx,
                &metrics,
                node_id,
                authority.clone(),
                state_sync_service.clone(),
                last_block_attestations.clone(),
                &unprocessed_blocks_cache,
                &chain_pulse_monitor,
            )
            .expect("try_finalize iteration failed")
            {
                height_cutoff = new_border;
            }
        }
        unprocessed_blocks_cache
            .notifications()
            .clone()
            .wait_for_updates_timeout(before, std::time::Duration::from_millis(50));
    }
}

#[allow(clippy::too_many_arguments)]
fn try_finalize(
    block_state: &BlockState,
    repository: &mut RepositoryImpl,
    block_state_repository: &BlockStateRepository,
    shared_services: &mut SharedServices,
    raw_block_tx: &mut InstrumentedSender<RawBlockSaveCommand<(NodeIdentifier, Vec<u8>)>>,
    metrics: &Option<BlockProductionMetrics>,
    node_id: &NodeIdentifier,
    authority: Arc<Mutex<Authority>>,
    state_sync_service: Arc<impl StateSyncService<Repository = RepositoryImpl>>,
    last_block_attestations: Arc<Mutex<CollectedAttestations>>,
    unprocessed_blocks_cache: &UnfinalizedCandidateBlockCollection,
    chain_pulse_monitor: &Sender<ChainPulseEvent>,
) -> anyhow::Result<Option<u64>> {
    tracing::trace!(
        "try_finalize_blocks: process: {:?}",
        block_state.guarded(|e| (*e.block_seq_no(), e.block_identifier().clone()))
    );
    let mut finalized_block_height_border = None;
    let (block_id, Some(parent_id)) = block_state
        .guarded(|e| (e.block_identifier().clone(), e.parent_block_identifier().clone()))
    else {
        tracing::trace!("try_finalize_blocks: block_id not found");
        return Ok(finalized_block_height_border);
    };
    let parent_state = block_state_repository.get(&parent_id)?;
    if block_state.guarded(|e| e.has_cross_thread_ref_data_prepared() != &Some(true)) {
        tracing::trace!(
            "try_finalize_blocks failed to load cross thread ref data for block: {block_id}"
        );
        return Ok(finalized_block_height_border);
    }
    let Some(blocks_finalized_by_parent) = parent_state.guarded(|e| e.finalizes_blocks().clone())
    else {
        tracing::trace!("try_finalize_blocks parent({parent_id:?}) finalizes_blocks is not set");
        return Ok(finalized_block_height_border);
    };
    if SHUTDOWN_FLAG.get() == Some(&true) {
        return Ok(finalized_block_height_border);
    }
    let mut blocks_finalized_by_parent = blocks_finalized_by_parent
        .into_iter()
        .map(|index| {
            block_state_repository.get(index.block_identifier()).expect("Failed to get block state")
        })
        .collect::<Vec<_>>();
    blocks_finalized_by_parent.sort_by(|a, b| {
        a.guarded(|e| (*e.block_seq_no()).unwrap())
            .cmp(&b.guarded(|e| (*e.block_seq_no()).unwrap()))
    });

    let mut max_finalized_seq_no_before_snapshot = BlockSeqNo::default();
    for block_state in blocks_finalized_by_parent {
        if block_state.guarded(|e| !e.is_finalized() && e.can_be_finalized()) {
            let (Some(block_height), Some(block_seq_no), Some(attestation_target)) = block_state
                .guarded(|e| (*e.block_height(), *e.block_seq_no(), *e.attestation_target()))
            else {
                tracing::trace!(
                    "try_finalize_blocks: block_seq_no, attestation_target and block_height were not set for block: {:?}",
                    block_state.block_identifier()
                );
                continue;
            };
            if block_seq_no > max_finalized_seq_no_before_snapshot {
                max_finalized_seq_no_before_snapshot = block_seq_no;
            }
            let Some(candidate_block) =
                unprocessed_blocks_cache.get_block_by_id(block_state.block_identifier())
            else {
                tracing::trace!(
                    "try_finalize_blocks: block not found: {:?}",
                    block_state.block_identifier()
                );
                continue;
            };
            let _ = chain_pulse_monitor.send(ChainPulseEvent::block_finalized(
                candidate_block.data().get_common_section().thread_id,
                Some(block_height),
            ));
            on_block_finalized(
                shared_services,
                &candidate_block,
                repository,
                block_state_repository,
                raw_block_tx,
                state_sync_service.clone(),
                last_block_attestations.clone(),
                node_id,
                metrics,
            )?;
            let new_height_border = *block_height.height()
                + *attestation_target.primary().generation_deadline() as u64 * 2
                + 2;
            if finalized_block_height_border.map(|v| v < new_height_border).unwrap_or(true) {
                finalized_block_height_border = Some(new_height_border);
            };
            let thread_id = candidate_block.data().get_common_section().thread_id;
            authority.guarded_mut(|e| e.get_thread_authority(&thread_id)).guarded_mut(|e| {
                e.on_block_finalized(&block_height);
            });
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
    }
    if max_finalized_seq_no_before_snapshot != BlockSeqNo::default() {
        let (snapshot, filter) = unprocessed_blocks_cache.clone_queue();
        for (block, _) in snapshot.blocks().values() {
            if !block.guarded(|e| e.is_finalized()) {
                let Some(block_seq_no) = block_state.guarded(|e| *e.block_seq_no()) else {
                    continue;
                };
                if block_seq_no <= max_finalized_seq_no_before_snapshot {
                    tracing::trace!(target: "monit", "{block_state:?} Invalidate block, a block with greater or equal seq_no was finalized");
                    invalidate_branch(block.clone(), block_state_repository, &filter);
                } else {
                    break;
                }
            }
        }
    }

    Ok(finalized_block_height_border)
}

#[allow(clippy::too_many_arguments)]
// This function is public for benchmarking
pub fn on_block_finalized(
    shared_services: &mut SharedServices,
    block: &Envelope<GoshBLS, AckiNackiBlock>,
    repository: &mut RepositoryImpl,
    block_state_repository: &BlockStateRepository,
    raw_block_tx: &mut InstrumentedSender<RawBlockSaveCommand<(NodeIdentifier, Vec<u8>)>>,
    state_sync_service: Arc<impl StateSyncService<Repository = RepositoryImpl>>,
    last_block_attestations: Arc<Mutex<CollectedAttestations>>,
    node_id: &NodeIdentifier,
    metrics: &Option<BlockProductionMetrics>,
) -> anyhow::Result<()> {
    trace_span!("on_block_finalized").in_scope(|| {
        let block_seq_no = block.data().seq_no();
        let block_id = block.data().identifier();
        let block_state = block_state_repository.get(&block_id)?;
        if let Some(cutoff) = block_state.guarded(|e|e.moves_attestation_list_cutoff().clone()) {
            last_block_attestations.guarded_mut(|e| e.move_cutoff(cutoff.0, cutoff.1));
        }
        let thread_id = block.data().get_common_section().thread_id;
        tracing::debug!("on_block_finalized: {:?} {:?}", block_seq_no, block_id.clone());
        repository.mark_block_as_finalized(
            block,
            block_state_repository.get(&block_id)?,
            Some(state_sync_service),
        )?;
        tracing::debug!("Block marked as finalized: {:?} {:?} {:?}", block_seq_no, block_id, thread_id);
        let producer_id = block.data().get_common_section().producer_id.clone();
        #[cfg(feature = "protocol_version_hash_in_block")]
        tracing::debug!(
            target: "monit",
            "Last finalized block data: seq_no: {:?}, block_id: {:?}, producer_id: {}, signatures: {:?}, thread_id: {:?}, tx_cnt: {}, time: {}, version_hash: {}",
            block.data().seq_no(),
            block.data().identifier(),
            producer_id,
            block.clone_signature_occurrences(),
            block.data().get_common_section().thread_id,
            block.data().tx_cnt(),
            block.data().time().unwrap_or(0),
            block.data().get_common_section().protocol_version_hash(),
        );
        #[cfg(not(feature = "protocol_version_hash_in_block"))]
        tracing::debug!(
            target: "monit",
            "Last finalized block data: seq_no: {:?}, block_id: {:?}, producer_id: {}, signatures: {:?}, thread_id: {:?}, tx_cnt: {}, time: {}",
            block.data().seq_no(),
            block.data().identifier(),
            producer_id,
            block.clone_signature_occurrences(),
            block.data().get_common_section().thread_id,
            block.data().tx_cnt(),
            block.data().time().unwrap_or(0),
        );
        let block_version = block_state.guarded(|e| e.block_version_state().clone()).expect("Finalized block does not have version state");
        tracing::info!("Finalized block version: {:?}", block_version.to_use());

        if let Some(m) = metrics  {
            tracing::trace!("Report finalized block version: {:?}", block_version.to_use());
            m.report_block_protocol_version(block_version.to_use());

            block_state.guarded(|e| {
                if let Some(set) = e.bk_set()  {
                    if let Some(bk_data) = set.get_by_node_id(node_id) {
                        tracing::trace!("Report bk_set epoch versions: {:?}", bk_data.protocol_support);
                        m.report_bkset_epoch_protocol_versions(&bk_data.protocol_support);
                    }
                };
            });
        };

        let serialized_block = bincode::serialize(&block)?;
        let bm_bcast_set = (producer_id, serialized_block.clone());
        match raw_block_tx.send(RawBlockSaveCommand::Save(bm_bcast_set))  {
            Ok(()) => {},
            Err(e) => {
                if SHUTDOWN_FLAG.get() != Some(&true) {
                    anyhow::bail!("Failed to send block: {e}");
                }
            }
        }
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
            repository.get_optimistic_state(&block.data().identifier(), &thread_id, None)?
                .ok_or(anyhow::anyhow!("Block must be in the repo"))?
        );

        let children = block_state.guarded(|e| e.known_children(&thread_id).cloned()).unwrap_or_default();
        for child in children {
            block_state_repository.get(&child)?.guarded_mut(|e| e.set_has_parent_finalized())?;
        }

        Ok(())
    })
}
