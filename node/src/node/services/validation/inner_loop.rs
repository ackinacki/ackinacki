// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::VecDeque;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use parking_lot::Mutex;
use telemetry_utils::mpsc::InstrumentedReceiver;
use tvm_executor::BlockchainConfig;

use crate::block::verify::verify_block;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::config::Config;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message_storage::MessageDurableStorage;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::services::validation::feedback::AckiNackiSend;
use crate::node::shared_services::SharedServices;
// use std::thread::sleep;
use crate::node::BlockState;
use crate::protocol::authority_switch::action_lock::Authority;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::CrossThreadRefDataRead;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

fn read_into_buffer(
    rx: &mut InstrumentedReceiver<(BlockState, Envelope<GoshBLS, AckiNackiBlock>)>,
    buffer: &mut VecDeque<(BlockState, Envelope<GoshBLS, AckiNackiBlock>)>,
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
    mut rx: InstrumentedReceiver<(BlockState, Envelope<GoshBLS, AckiNackiBlock>)>,
    block_state_repo: BlockStateRepository,
    repository: RepositoryImpl,
    blockchain_config: Arc<BlockchainConfig>,
    node_config: Config,
    mut shared_services: SharedServices,
    send: AckiNackiSend,
    metrics: Option<BlockProductionMetrics>,
    message_db: MessageDurableStorage,
    authority: Arc<Mutex<Authority>>,
) {
    let mut buffer = VecDeque::<(BlockState, Envelope<GoshBLS, AckiNackiBlock>)>::new();
    loop {
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
            })
        });
        if buffer.is_empty() {
            sleep(Duration::from_millis(10));
        }

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
            let block_identifier = state.guarded(|e| e.block_identifier().clone());
            if !state.guarded(|e| {
                *e.stored() == Some(true)
                    && *e.has_all_cross_thread_ref_data_available() == Some(true)
                    && *e.envelope_block_producer_signature_verified() == Some(true)
            }) {
                continue;
            }
            let parent_id = state
                .guarded(|e| e.parent_block_identifier().clone())
                .expect("Parent id must be set");

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
            let Ok(Some(mut prev_state)) = repository.get_optimistic_state(
                &prev_block_id,
                &next_block.get_common_section().thread_id,
                None,
            ) else {
                continue;
            };
            let refs = shared_services.exec(|service| {
                let mut refs = vec![];
                for block_id in &next_block.get_common_section().refs {
                    let state = service
                        .cross_thread_ref_data_service
                        .get_cross_thread_ref_data(block_id)
                        .expect("Failed to load ref state");
                    refs.push(state);
                }
                refs
            });

            let block_nack = next_block.get_common_section().nacks.clone();
            let verify_res = verify_block(
                &next_block,
                blockchain_config.clone(),
                &mut prev_state,
                node_config.clone(),
                refs,
                shared_services.clone(),
                block_nack,
                block_state_repo.clone(),
                repository.accounts_repository().clone(),
                metrics.clone(),
                message_db.clone(),
            )
            .expect("Failed to verify block");
            if !verify_res {
                tracing::warn!("Block verification failed: {:?}", block_identifier);
            }
            state.guarded_mut(|e| {
                if e.validated().is_none() {
                    let _ = e.set_validated(verify_res);
                }
            });
            if verify_res {
                let _ = send.send_ack(state.clone());
            } else {
                authority.guarded_mut(|e| e.on_bad_block_nack_confirmed(state.clone()));
                let _ = send.send_nack_bad_block(state.clone(), next_envelope.clone());
            }
        }
    }
}
