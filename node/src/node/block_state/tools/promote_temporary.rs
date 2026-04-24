// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use node_types::BlockIdentifier;
use node_types::ParentRef;
use node_types::TemporaryBlockId;

use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::utilities::guarded::GuardedMut;

/// Promotes a temporary block state into a full AckiNackiBlockState.
///
/// Lock ordering (to prevent deadlocks):
/// 1. Read data from temporary state (in-memory only)
/// 2. Lock parent → update known_children
/// 3. Lock new block state → fill with data from temporary
/// 4. Lock each temporary child → update parent_ref
/// 5. Remove temporary state
///
/// Parent → self → children — always root-to-leaf, same order as link_parent_child.
pub fn promote_temporary_to_block_state(
    temp_id: &TemporaryBlockId,
    block_id: BlockIdentifier,
    block_state_repository: &BlockStateRepository,
) -> anyhow::Result<BlockState> {
    // ── Step 0: Extract data from temporary (read lock) ──
    let temp_arc = block_state_repository
        .get_temporary(temp_id)
        .ok_or_else(|| anyhow::anyhow!("Temporary state {temp_id:?} not found"))?;

    let snapshot = {
        let temp = temp_arc.read();
        TemporarySnapshot {
            parent_ref: temp.parent_ref().clone(),
            thread_id: temp
                .thread_identifier()
                .copied()
                .expect("Thread identifier must be set on temporary state"),
            block_seq_no: temp.block_seq_no().copied(),
            block_round: temp.block_round().copied(),
            block_height: temp.block_height().cloned(),
            producer: temp.producer().cloned(),
            block_time_ms: temp.block_time_ms(),
            bk_set: temp.bk_set().cloned(),
            descendant_bk_set: temp.descendant_bk_set().cloned(),
            future_bk_set: temp.future_bk_set().cloned(),
            descendant_future_bk_set: temp.descendant_future_bk_set().cloned(),
            producer_selector_data: temp.producer_selector_data().cloned(),
            block_version_state: temp.block_version_state().cloned(),
            known_children_incomplete: temp.known_children_incomplete().clone(),
        }
    };

    // ── Step 1: Update parent ──
    let real_parent_id = match &snapshot.parent_ref {
        ParentRef::Block(id) => *id,
        ParentRef::Temporary(parent_temp_id) => {
            // If parent is still temporary, it must be promoted first.
            // In normal flow, blocks are promoted from parent to child.
            anyhow::bail!(
                "Parent {parent_temp_id:?} is still temporary. \
                 Promote parent first before promoting child {temp_id:?}"
            );
        }
    };

    let parent_state = block_state_repository.get(&real_parent_id)?;
    parent_state
        .guarded_mut(|e| e.promote_incomplete_child(snapshot.thread_id, temp_id, block_id))?;

    // ── Step 2: Create/fill the real block state ──
    let block_state = block_state_repository.get(&block_id)?;
    block_state.guarded_mut(|state| {
        state.set_parent_block_identifier(real_parent_id)?;
        state.set_thread_identifier(snapshot.thread_id)?;
        if let Some(v) = snapshot.block_seq_no {
            state.set_block_seq_no(v)?;
        }
        if let Some(v) = snapshot.block_round {
            state.set_block_round(v)?;
        }
        if let Some(v) = snapshot.block_height {
            state.set_block_height(v)?;
        }
        if let Some(v) = snapshot.producer {
            state.set_producer(v)?;
        }
        if let Some(v) = snapshot.block_time_ms {
            state.set_block_time_ms(v)?;
        }
        if let Some(bk_set) = snapshot.bk_set {
            state.update_bk_sets(bk_set, snapshot.descendant_bk_set);
        }
        if let Some(v) = snapshot.producer_selector_data {
            state.set_producer_selector_data(v)?;
        }
        if let Some(v) = snapshot.block_version_state {
            state.set_block_version_state(v)?;
        }

        // Transfer known_children_incomplete
        state.known_children_incomplete = snapshot.known_children_incomplete.clone();
        Ok::<(), anyhow::Error>(())
    })?;

    // ── Step 3: Update parent_ref on all temporary children ──
    for (_thread, child_temp_ids) in snapshot.known_children_incomplete.iter() {
        for child_temp_id in child_temp_ids {
            if let Some(child_temp_arc) = block_state_repository.get_temporary(child_temp_id) {
                let mut child_temp = child_temp_arc.write();
                child_temp.set_parent_ref(ParentRef::Block(block_id));
            }
            // If child is no longer temporary, it was already promoted — nothing to do
        }
    }

    // ── Step 4: Mark temporary as promoted (do NOT remove) ──
    // The production loop may still reference this temp_id as a parent for
    // the next block. Instead of removing, record the real block_id so that
    // the next produce() call can resolve it via promoted_block_id().
    {
        let temp_arc = block_state_repository
            .get_temporary(temp_id)
            .expect("Temporary state must still exist during promotion");
        let mut temp = temp_arc.write();
        temp.set_promoted_block_id(Some(block_id));
    }

    Ok(block_state)
}

/// Snapshot of data extracted from a TemporaryBlockState for transfer to a real state.
#[allow(dead_code)]
struct TemporarySnapshot {
    parent_ref: ParentRef,
    thread_id: node_types::ThreadIdentifier,
    block_seq_no: Option<crate::types::BlockSeqNo>,
    block_round: Option<crate::types::BlockRound>,
    block_height: Option<crate::types::BlockHeight>,
    producer: Option<crate::node::NodeIdentifier>,
    block_time_ms: Option<u64>,
    bk_set: Option<std::sync::Arc<crate::block_keeper_system::BlockKeeperSet>>,
    descendant_bk_set: Option<std::sync::Arc<crate::block_keeper_system::BlockKeeperSet>>,
    future_bk_set: Option<std::sync::Arc<crate::block_keeper_system::BlockKeeperSet>>,
    descendant_future_bk_set: Option<std::sync::Arc<crate::block_keeper_system::BlockKeeperSet>>,
    producer_selector_data: Option<crate::types::bp_selector::ProducerSelector>,
    block_version_state:
        Option<crate::versioning::block_protocol_version_state::BlockProtocolVersionState>,
    known_children_incomplete: std::collections::HashMap<
        node_types::ThreadIdentifier,
        std::collections::HashSet<node_types::TemporaryBlockId>,
    >,
}
