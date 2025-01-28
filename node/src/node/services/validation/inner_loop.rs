// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::VecDeque;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::TryRecvError;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use parking_lot::Mutex;
use tvm_executor::BlockchainConfig;
use tvm_types::UInt256;

use crate::block::keeper::process::verify_block;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::config::Config;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::services::validation::feedback::AckiNackiSend;
use crate::node::shared_services::SharedServices;
// use std::thread::sleep;
use crate::node::BlockState;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::CrossThreadRefDataRead;
use crate::repository::Repository;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::utilities::FixedSizeHashSet;

fn read_into_buffer(rx: &mut Receiver<BlockState>, buffer: &mut VecDeque<BlockState>) -> bool {
    buffer.extend(rx.try_iter());
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
    mut rx: Receiver<BlockState>,
    block_state_repo: BlockStateRepository,
    repository: RepositoryImpl,
    blockchain_config: Arc<BlockchainConfig>,
    node_config: Config,
    mut shared_services: SharedServices,
    // TODO: !!! THIS MUST BE KILLED
    nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
    send: AckiNackiSend,
) {
    let mut buffer = VecDeque::<BlockState>::new();
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
            e.guarded(|x| {
                !x.is_finalized()
                    && !x.is_invalidated()
                    && x.validated().is_none()
                    && *x.envelope_block_producer_signature_verified() != Some(false)
            })
        });
        if buffer.is_empty() {
            sleep(Duration::from_millis(10));
        }

        for state in buffer.iter() {
            if state.guarded(|e| e.must_be_validated() != &Some(true)) {
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

            let Some(next_envelope) = repository.get_block(&block_identifier).ok().flatten() else {
                continue;
            };
            let next_block = next_envelope.data().clone();
            tracing::trace!(
                "Block validation process: verify block: {:?}, seq_no: {}",
                next_block.identifier(),
                next_block.seq_no(),
            );
            let prev_block_id = next_block.parent();
            let mut prev_state = repository
                .get_optimistic_state(&prev_block_id, Arc::clone(&nack_set_cache))
                .expect("Failed to get optimistic state of the previous block")
                .unwrap_or_else(|| {
                    panic!("Failed to get optimistic state for a block: {prev_block_id:?}")
                });
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
                let _ = send.send_nack_bad_block(state.clone(), next_envelope);
            }
        }
    }
}
