use crate::node::BlockState;
use crate::node::BlockStateRepository;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;

/// Find potentially shortest gap that MAY fix all chains.
/// Note on usecases:
/// When searching for the gap to fix from the last finalized
/// it makes sence to put min_seq_no as the next seq no from
/// the last finalized block, since if there's a candidate block that
/// misses a parent block it is sure that the block it is referencing
/// to may not be the last finalized => it will be invalid.
/// It also worth moving this cutoff (min_seq_no) forward once some range was
/// requested as it is possible that the actual missing gap is later than
/// the initially requested range.
pub fn find_shortest_gap(
    thread_identifier: &ThreadIdentifier,
    unprocessed_blocks_cache: &[BlockState],
    blocks_states: &BlockStateRepository,
    min_seq_no: BlockSeqNo,
) -> Option<BlockSeqNo> {
    let gaps = find_all_gaps(thread_identifier, unprocessed_blocks_cache, blocks_states);
    let mut shortest = Option::<BlockSeqNo>::None;
    for last in gaps {
        let Some(seq_no) = last.guarded(|e| *e.block_seq_no()) else {
            // This block state has incomplete data to work with;
            continue;
        };
        if seq_no <= min_seq_no {
            // Skip the block. It is below the threshold
            continue;
        }
        if shortest.is_none() || shortest.unwrap() > seq_no {
            shortest = Some(seq_no);
        }
    }
    shortest
}

/// Identifies gaps
/// Returns a list of block states that have missing parents.
pub fn find_all_gaps(
    thread_identifier: &ThreadIdentifier,
    unprocessed_blocks_cache: &[BlockState],
    blocks_states: &BlockStateRepository,
) -> Vec<BlockState> {
    let this_thread = &Some(*thread_identifier);
    unprocessed_blocks_cache
        .iter()
        .filter(|b| {
            if b.guarded(|e| e.thread_identifier() != this_thread) {
                return false;
            }
            let Some(parent_block_id) = b.guarded(|e| e.parent_block_identifier().clone()) else {
                return false;
            };
            blocks_states.get(&parent_block_id).is_ok_and(|e| !e.lock().is_stored())
        })
        .cloned()
        .collect()
}
