use node_types::ThreadIdentifier;

use super::CheckError;
#[cfg(feature = "allow-threads-merge")]
use super::Proposal;
use super::ThreadAction;
use crate::types::ThreadsTable;
#[cfg(feature = "allow-threads-merge")]
use crate::types::ThreadsTablePrefab;

#[cfg(not(feature = "allow-threads-merge"))]
pub fn try_threads_merge(
    _this_thread_id: &ThreadIdentifier,
    _this_thread_row_index: usize,
    _thread_with_min_load: &ThreadIdentifier,
    _pre_default_thread: Option<ThreadIdentifier>,
    _default_thread_id: &ThreadIdentifier,
    _threads_table: &ThreadsTable,
) -> anyhow::Result<ThreadAction, CheckError> {
    // TODO: implement logic
    Ok(ThreadAction::ContinueAsIs)
}

#[cfg(feature = "allow-threads-merge")]
pub fn try_threads_merge(
    this_thread_id: &ThreadIdentifier,
    this_thread_row_index: usize,
    thread_with_min_load: &ThreadIdentifier,
    pre_default_thread: Option<ThreadIdentifier>,
    default_thread_id: &ThreadIdentifier,
    threads_table: &ThreadsTable,
) -> anyhow::Result<ThreadAction, CheckError> {
    if default_thread_id == this_thread_id {
        // Default (last) thread never collapses
        return Ok(ThreadAction::ContinueAsIs);
    }
    if pre_default_thread.is_none() {
        return Ok(ThreadAction::ContinueAsIs);
    }
    let pre_default_thread = pre_default_thread.unwrap();
    let is_this_the_least_used_thread: bool = thread_with_min_load == this_thread_id;
    let is_the_default_thread_is_the_least_used_and_this_thread_right_above_it: bool =
        (thread_with_min_load == default_thread_id && this_thread_id == &pre_default_thread);
    if is_this_the_least_used_thread
        || is_the_default_thread_is_the_least_used_and_this_thread_right_above_it
    {
        let mut collapsed_table = threads_table.clone();
        let _ = collapsed_table.remove(this_thread_row_index).unwrap();
        let prefab = ThreadsTablePrefab::resolved(collapsed_table);
        return Ok(ThreadAction::Collapse(Proposal { proposed_threads_table: prefab }));
    } else {
        return Ok(ThreadAction::ContinueAsIs);
    }
}
