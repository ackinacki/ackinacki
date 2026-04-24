// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;

use node_types::BlockIdentifier;
use parking_lot::Mutex;
use telemetry_utils::mpsc::InstrumentedReceiver;

use crate::block::producer::wasm::WasmNodeCache;
use crate::block::verify::verify_block;
use crate::block::verify::VerificationResult;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::config::config_read::ConfigRead;
use crate::config::BlockchainConfigRead;
use crate::config::Config;
use crate::helper::metrics::BlockProductionMetrics;
use crate::helper::SHUTDOWN_FLAG;
use crate::node::associated_types::NackReason;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::block_state::tools::invalidate_branch;
use crate::node::services::validation::feedback::AckiNackiSend;
use crate::node::shared_services::SharedServices;
use crate::node::unprocessed_blocks_collection::FilterPrehistoric;
// use std::thread::sleep;
use crate::node::{BlockState, LOOP_PAUSE_DURATION};
use crate::protocol::authority_switch::action_lock::Authority;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::CrossThreadRefDataRead;
use crate::repository::Repository;
use crate::storage::MessageDurableStorage;
use crate::types::AckiNackiBlock;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::versioning::block_protocol_version_state::BlockProtocolVersionState;

fn read_into_buffer(
    rx: &mut InstrumentedReceiver<(BlockState, Envelope<AckiNackiBlock>)>,
    buffer: &mut VecDeque<(BlockState, Envelope<AckiNackiBlock>)>,
) -> bool {
    while let Ok(v) = rx.try_recv() {
        buffer.push_back(v);
    }
    match rx.try_recv() {
        Ok(next) => buffer.push_back(next),
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            return false;
        }
    }
    if buffer.is_empty() {
        match rx.recv() {
            Ok(next) => buffer.push_back(next),
            Err(RecvError) => {
                return false;
            }
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
pub(super) fn inner_loop(
    mut rx: InstrumentedReceiver<(BlockState, Envelope<AckiNackiBlock>)>,
    block_state_repo: BlockStateRepository,
    repository: RepositoryImpl,
    blockchain_config: BlockchainConfigRead,
    node_config: Config,
    node_global_config_read: ConfigRead,
    mut shared_services: SharedServices,
    send: AckiNackiSend,
    metrics: Option<BlockProductionMetrics>,
    wasm_cache: WasmNodeCache,
    message_db: MessageDurableStorage,
    authority: Arc<Mutex<Authority>>,
) {
    let mut buffer = VecDeque::<(BlockState, Envelope<AckiNackiBlock>)>::new();
    let mut blocks_with_unsupported_version = HashSet::<BlockIdentifier>::new();
    loop {
        if SHUTDOWN_FLAG.get() == Some(&true) {
            return;
        }
        if !read_into_buffer(&mut rx, &mut buffer) {
            return;
        }

        // retain all blocks that:
        // - are not validated yet
        // - were not invalidated
        // - are not finalized
        // - haven't failed producer signature check
        buffer.retain(|e| {
            e.0.guarded(|x| {
                !x.is_finalized()
                    && !x.is_invalidated()
                    && x.validated().is_none()
                    && *x.envelope_block_producer_signature_verified() != Some(false)
                    && !blocks_with_unsupported_version.contains(x.block_identifier())
            })
        });

        blocks_with_unsupported_version.clear();

        for (state, next_envelope) in buffer.iter() {
            if state.guarded(|e| e.is_finalized() || e.is_invalidated()) {
                continue;
            }
            if state.guarded(|e| {
                e.must_be_validated() != &Some(true)
                    && e.validated().is_none()
                    && e.has_bad_block_nacks_resolved()
            }) {
                continue;
            }
            let block_identifier = state.guarded(|e| *e.block_identifier());
            if !state.guarded(|e| {
                *e.stored() == Some(true)
                    && *e.has_all_cross_thread_ref_data_available() == Some(true)
                    && *e.envelope_block_producer_signature_verified() == Some(true)
            }) {
                continue;
            }
            let parent_id =
                state.guarded(|e| *e.parent_block_identifier()).expect("Parent id must be set");

            let parent_block_state =
                block_state_repo.get(&parent_id).expect("Parent block state must exist");
            if !parent_block_state.guarded(|e| e.is_block_already_applied()) {
                continue;
            }
            let next_block = next_envelope.data().clone();
            tracing::trace!(
                "Block validation process: verify block: {:?}, seq_no: {}",
                next_block.identifier(),
                next_block.seq_no(),
            );
            let prev_block_id = next_block.parent();
            let Ok(Some(prev_state)) = repository.get_optimistic_state(
                &prev_block_id,
                next_block.common_section().thread_id(),
                None,
            ) else {
                tracing::trace!(
                    "Skip block validation process: verify block: {:?}, parent state is not available",
                    next_block.identifier(),
                );
                continue;
            };
            let mut prev_state = Arc::unwrap_or_clone(prev_state);
            let refs = shared_services.exec(|service| {
                let mut refs = vec![];
                for block_id in next_block.common_section().refs() {
                    let state = service
                        .cross_thread_ref_data_service
                        .get_cross_thread_ref_data(block_id)
                        .unwrap_or_else(|error| {
                            panic!(
                                "Failed to load direct cross-thread ref state while validating block {} in thread {:?}: direct_ref_id={block_id}, split_related={}, error={error:#}",
                                next_block.identifier(),
                                next_block.common_section().thread_id(),
                                next_block.is_thread_splitting(),
                            )
                        });
                    refs.push(state);
                }
                refs
            });

            // Wait for block protocol version
            let protocol_version_state: BlockProtocolVersionState = loop {
                let Some(protocol_version) = state.guarded(|e| e.block_version_state().clone())
                else {
                    tracing::trace!(
                        "block validation process: verify block: {:?}, block protocol version is not available",
                        next_block.identifier(),
                    );
                    std::thread::sleep(LOOP_PAUSE_DURATION);
                    continue;
                };
                break protocol_version;
            };
            let protocol_version = protocol_version_state.to_use().clone();
            let Some(node_global_config) = node_global_config_read.get(&protocol_version) else {
                tracing::trace!("Skip block validation process: node config is not available");
                blocks_with_unsupported_version.insert(*state.block_identifier());
                continue;
            };

            let node_global_config = Arc::unwrap_or_clone(node_global_config);

            let block_nack = next_block.common_section().nacks().clone();
            let verify_res = verify_block(
                &next_block,
                blockchain_config.clone(),
                &mut prev_state,
                node_config.clone(),
                node_global_config,
                refs,
                shared_services.clone(),
                block_nack,
                block_state_repo.clone(),
                repository.accounts_repository().clone(),
                repository.thread_accounts_repository().clone(),
                metrics.clone(),
                wasm_cache.clone(),
                message_db.clone(),
                node_global_config_read.is_retired(&protocol_version),
            )
            .unwrap_or_else(|error| {
                panic!(
                    "Failed to verify block {} in thread {:?} (parent={}, split_related={}): {error:#}",
                    next_block.identifier(),
                    next_block.common_section().thread_id(),
                    next_block.parent(),
                    next_block.is_thread_splitting(),
                )
            });
            if !verify_res.is_valid() {
                tracing::warn!("Block verification failed: {:?}", block_identifier);
            }
            state.guarded_mut(|e| {
                if e.validated().is_none() {
                    let _ = e.set_validated(verify_res.is_valid());
                }
            });
            if SHUTDOWN_FLAG.get() == Some(&true) {
                return;
            }
            if verify_res.is_valid() {
                let _ = send.send_ack(state.clone());
            } else {
                let thread_id = *next_block.common_section().thread_id();
                let has_bad_block_accusers = state.guarded(|e| !e.bad_block_accusers().is_empty());
                match (verify_res, has_bad_block_accusers) {
                    (VerificationResult::TooComplexExecution, false) => {
                        // TODO: send Nack here
                        tracing::warn!("Verification failed: TooComplexExecution");
                    }
                    (VerificationResult::BadBlock, _)
                    | (VerificationResult::TooComplexExecution, true) => {
                        tracing::trace!(target: "monit", "{state:?} Block verification failed");
                        invalidate_branch(
                            state.clone(),
                            &block_state_repo,
                            &FilterPrehistoric::builder()
                                .block_seq_no(BlockSeqNo::default())
                                .build(),
                        );
                        let nack_reason = NackReason::BadBlock { envelope: next_envelope.clone() };
                        let _ = send.send_nack(state.clone(), nack_reason);
                        authority
                            .guarded_mut(|e| e.get_thread_authority(&thread_id))
                            .guarded_mut(|e| e.on_bad_block_nack_confirmed(state.clone()));
                    }
                    (VerificationResult::ValidBlock, _) => {
                        unreachable!();
                    }
                };
            }
        }
    }
}
