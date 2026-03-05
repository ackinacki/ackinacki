use node_types::AccountRouting;
use node_types::ThreadIdentifier;

use super::AggregatedLoad;
use super::CheckError;
use super::Load;
use super::Proposal;
use super::ThreadAction;
use crate::bitmask::mask::Bitmask;
use crate::types::ThreadsTable;
use crate::types::ThreadsTablePrefab;

#[allow(clippy::too_many_arguments)]
pub fn try_threads_split(
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
    if max_table_size == 1 {
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
    let prefab =
        ThreadsTablePrefab::with_split(threads_table.clone(), this_thread_row_index, proposed_mask);
    Ok(ThreadAction::Split(Proposal { proposed_threads_table: prefab }))
}
