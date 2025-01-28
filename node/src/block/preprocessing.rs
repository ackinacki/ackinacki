use crate::message::WrappedMessage;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::CrossThreadRefData;
use crate::repository::CrossThreadRefDataRead;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

type State = crate::repository::optimistic_state::OptimisticStateImpl;

pub fn preprocess<'a, I, TRepo>(
    parent_block_state: State,
    refs: I,
    descendant_thread_identifier: &ThreadIdentifier,
    repository: &TRepo,
    slashing_messages: Vec<WrappedMessage>,
) -> anyhow::Result<(State, ThreadsTable)>
where
    I: std::iter::Iterator<Item = &'a CrossThreadRefData>,
    CrossThreadRefData: 'a,
    TRepo: CrossThreadRefDataRead,
{
    tracing::trace!(
        "preprocessing: {} slashing_messages: {slashing_messages:?}",
        slashing_messages.len()
    );

    let in_table = parent_block_state.get_produced_threads_table().clone();
    let mut preprocessed_state = parent_block_state;

    if cfg!(feature = "allow-threads-merge") {
        // Merge state threads table and DAPP table with other threads
        todo!("need to think through");
        // for state in refs {
        // preprocessed_state.merge_dapp_id_tables(state)?;
        // in_table.merge(state.get_produced_threads_table())?;
        // }
    }
    let mut ref_data = vec![];
    for state in refs {
        preprocessed_state.merge_dapp_id_tables(state.dapp_id_table())?;
        ref_data.push(state.as_reference_state_data());
    }

    let current_thread_last_block = (
        preprocessed_state.thread_id,
        preprocessed_state.block_id.clone(),
        preprocessed_state.block_seq_no,
    );

    // Note: block has threads table changes, add new thread to the all_thread_refs
    for thread_id in preprocessed_state.threads_table.list_threads() {
        if thread_id.is_spawning_block(&preprocessed_state.block_id) {
            preprocessed_state
                .thread_refs_state
                .update(*thread_id, current_thread_last_block.clone());
        }
    }

    let all_referenced_blocks = preprocessed_state
        .thread_refs_state
        .move_refs(ref_data.into_iter().map(|e| e.1).collect(), |block_id| {
            repository.get_cross_thread_ref_data(block_id)
        })?;
    let all_referenced_cross_thread_blocks: Vec<_> = all_referenced_blocks
        .into_iter()
        .filter(|e| e.block_thread_identifier() != descendant_thread_identifier)
        .collect();
    for block_data in all_referenced_cross_thread_blocks {
        preprocessed_state.add_messages_from_ref(&block_data)?;
        preprocessed_state.add_accounts_from_ref(&block_data)?;
    }
    preprocessed_state.crop(descendant_thread_identifier, &in_table)?;
    preprocessed_state.add_slashing_messages(slashing_messages)?;

    Ok((preprocessed_state, in_table))
}
