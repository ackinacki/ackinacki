use std::collections::HashMap;

use telemetry_utils::mpsc::InstrumentedReceiver;

use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateSaveCommand;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;

pub fn start_optimistic_state_save_service(
    repository: RepositoryImpl,
    receiver: InstrumentedReceiver<OptimisticStateSaveCommand>,
) -> anyhow::Result<()> {
    loop {
        match receiver.recv()? {
            OptimisticStateSaveCommand::Save(state) => {
                let mut states_to_save = HashMap::new();
                states_to_save.insert(state.thread_id, state);

                while let Ok(value) = receiver.try_recv() {
                    match value {
                        OptimisticStateSaveCommand::Save(state) => {
                            if states_to_save.contains_key(&state.thread_id) {
                                let prev_state =
                                    states_to_save.get(&state.thread_id).unwrap().clone();
                                if prev_state.block_seq_no < state.block_seq_no {
                                    states_to_save.insert(state.thread_id, state);
                                } else {
                                    let block_id = state.get_block_id();
                                    let block_seq_no = state.get_block_seq_no();
                                    let thread_id = state.get_thread_id();
                                    tracing::trace!("Optimistic State saving service skipped state: {thread_id} {block_seq_no} {block_id:?}");
                                }
                            } else {
                                states_to_save.insert(state.thread_id, state);
                            }
                        }
                        OptimisticStateSaveCommand::Shutdown => {
                            // Note: There is no need to save states from the queue, the last finalized state will be saved on repository dump
                            tracing::trace!("Optimistic State saving service shutting down!!");
                            return Ok(());
                        }
                    }
                }
                for state in states_to_save.into_values() {
                    let block_id = state.get_block_id().clone();
                    let block_seq_no = *state.get_block_seq_no();
                    let thread_id = *state.get_thread_id();
                    tracing::trace!("Optimistic State saving service received state: {thread_id} {block_seq_no} {block_id:?}");
                    repository.store_optimistic(state)?;
                    tracing::trace!(
                        "Optimistic State saving successfully saved state: {thread_id} {block_seq_no} {block_id:?}"
                    );
                }
            }
            OptimisticStateSaveCommand::Shutdown => {
                tracing::trace!("Optimistic State saving service shutting down!!");
                return Ok(());
            }
        }
    }
}
