use super::AggregatedLoad;
use super::CheckError;
use super::Load;
use super::Proposal;
use super::ThreadAction;
use crate::bitmask::mask::Bitmask;
use crate::types::AccountRouting;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

#[allow(clippy::too_many_arguments)]
pub fn try_threads_split(
    produced_block_id: &BlockIdentifier,
    this_thread_id: &ThreadIdentifier,
    this_thread_row_index: usize,
    this_thread_aggregated_load: &AggregatedLoad,
    this_thread_bitmask: &Bitmask<AccountRouting>,
    max_load: Load,
    min_load: Load,
    max_load_disproportion_coefficient: Load,
    thread_with_max_load: &ThreadIdentifier,
    threads_table: &ThreadsTable,
    max_table_size: usize,
) -> anyhow::Result<ThreadAction, CheckError> {
    if thread_with_max_load != this_thread_id {
        return Ok(ThreadAction::ContinueAsIs);
    }
    if (threads_table.len() >= max_table_size)
        && (max_load <= (max_load_disproportion_coefficient * min_load))
    {
        return Ok(ThreadAction::ContinueAsIs);
    }

    // Split no matter what and let the least used threads to suicide.
    let proposed_mask = this_thread_aggregated_load.propose_new_bitmask(this_thread_bitmask);
    if proposed_mask.is_none() {
        return Ok(ThreadAction::ContinueAsIs);
    }

    let proposed_mask = proposed_mask.unwrap();

    if threads_table.rows().any(|(mask, _)| *mask == proposed_mask) {
        tracing::trace!("Proposed mask already exists: {:?}", proposed_mask);
        return Ok(ThreadAction::ContinueAsIs);
    }
    let new_thread_id = ThreadIdentifier::new(produced_block_id, 0u16);
    let mut proposed_threads_table = threads_table.clone();
    let _: () = proposed_threads_table
        .insert_above(this_thread_row_index, proposed_mask, new_thread_id)
        .unwrap();
    Ok(ThreadAction::Split(Proposal { proposed_threads_table }))
}
