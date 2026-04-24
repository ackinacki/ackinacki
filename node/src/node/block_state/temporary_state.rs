// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use node_types::BlockIdentifier;
use node_types::ParentRef;
use node_types::TemporaryBlockId;
use node_types::ThreadIdentifier;

use crate::block_keeper_system::BlockKeeperSet;
use crate::node::NodeIdentifier;
use crate::types::bp_selector::ProducerSelector;
use crate::types::BlockHeight;
use crate::types::BlockRound;
use crate::types::BlockSeqNo;
use crate::versioning::block_protocol_version_state::BlockProtocolVersionState;

/// Temporary block state that exists only within the block-producer.
/// Created BEFORE block production starts, allowing early linking with the parent.
/// When the block is fully produced, promoted to AckiNackiBlockState.
///
/// Differences from AckiNackiBlockState:
/// - No block_identifier (unknown until block is produced)
/// - parent_ref is a ParentRef enum (can reference a temporary block)
/// - Only known_children_incomplete (no known_children with BlockIdentifier)
/// - No verification, finalization, or attestation fields
#[derive(Default)]
pub struct TemporaryBlockState {
    temporary_id: TemporaryBlockId,

    // --- Fields copied to AckiNackiBlockState on promotion ---
    thread_identifier: Option<ThreadIdentifier>,
    parent_ref: ParentRef,
    block_seq_no: Option<BlockSeqNo>,
    block_round: Option<BlockRound>,
    block_height: Option<BlockHeight>,
    producer: Option<NodeIdentifier>,
    block_time_ms: Option<u64>,

    // BK set data — needed for producing next blocks in the chain
    bk_set: Option<Arc<BlockKeeperSet>>,
    descendant_bk_set: Option<Arc<BlockKeeperSet>>,
    future_bk_set: Option<Arc<BlockKeeperSet>>,
    descendant_future_bk_set: Option<Arc<BlockKeeperSet>>,

    producer_selector_data: Option<ProducerSelector>,
    block_version_state: Option<BlockProtocolVersionState>,

    // --- Temporary-specific fields ---
    /// When set, this temporary state has been promoted to a real BlockState
    /// with this BlockIdentifier. The production loop can use this to resolve
    /// the parent reference instead of finding the temp state removed.
    promoted_block_id: Option<BlockIdentifier>,

    /// References to children that are also temporary.
    /// On promotion, transferred to AckiNackiBlockState.known_children_incomplete.
    known_children_incomplete: HashMap<ThreadIdentifier, HashSet<TemporaryBlockId>>,
}

impl TemporaryBlockState {
    pub fn new(temporary_id: TemporaryBlockId, parent_ref: ParentRef) -> Self {
        Self { temporary_id, parent_ref, ..Default::default() }
    }

    pub fn temporary_id(&self) -> &TemporaryBlockId {
        &self.temporary_id
    }

    pub fn parent_ref(&self) -> &ParentRef {
        &self.parent_ref
    }

    pub fn set_parent_ref(&mut self, parent_ref: ParentRef) {
        self.parent_ref = parent_ref;
    }

    pub fn thread_identifier(&self) -> Option<&ThreadIdentifier> {
        self.thread_identifier.as_ref()
    }

    pub fn set_thread_identifier(&mut self, thread_id: ThreadIdentifier) {
        self.thread_identifier = Some(thread_id);
    }

    pub fn block_seq_no(&self) -> Option<&BlockSeqNo> {
        self.block_seq_no.as_ref()
    }

    pub fn set_block_seq_no(&mut self, seq_no: BlockSeqNo) {
        self.block_seq_no = Some(seq_no);
    }

    pub fn block_round(&self) -> Option<&BlockRound> {
        self.block_round.as_ref()
    }

    pub fn set_block_round(&mut self, round: BlockRound) {
        self.block_round = Some(round);
    }

    pub fn block_height(&self) -> Option<&BlockHeight> {
        self.block_height.as_ref()
    }

    pub fn set_block_height(&mut self, height: BlockHeight) {
        self.block_height = Some(height);
    }

    pub fn producer(&self) -> Option<&NodeIdentifier> {
        self.producer.as_ref()
    }

    pub fn set_producer(&mut self, producer: NodeIdentifier) {
        self.producer = Some(producer);
    }

    pub fn block_time_ms(&self) -> Option<u64> {
        self.block_time_ms
    }

    pub fn set_block_time_ms(&mut self, time_ms: u64) {
        self.block_time_ms = Some(time_ms);
    }

    pub fn bk_set(&self) -> Option<&Arc<BlockKeeperSet>> {
        self.bk_set.as_ref()
    }

    pub fn set_bk_set(&mut self, bk_set: Arc<BlockKeeperSet>) {
        self.bk_set = Some(bk_set);
    }

    pub fn descendant_bk_set(&self) -> Option<&Arc<BlockKeeperSet>> {
        self.descendant_bk_set.as_ref()
    }

    pub fn set_descendant_bk_set(&mut self, bk_set: Arc<BlockKeeperSet>) {
        self.descendant_bk_set = Some(bk_set);
    }

    pub fn future_bk_set(&self) -> Option<&Arc<BlockKeeperSet>> {
        self.future_bk_set.as_ref()
    }

    pub fn set_future_bk_set(&mut self, bk_set: Arc<BlockKeeperSet>) {
        self.future_bk_set = Some(bk_set);
    }

    pub fn descendant_future_bk_set(&self) -> Option<&Arc<BlockKeeperSet>> {
        self.descendant_future_bk_set.as_ref()
    }

    pub fn set_descendant_future_bk_set(&mut self, bk_set: Arc<BlockKeeperSet>) {
        self.descendant_future_bk_set = Some(bk_set);
    }

    pub fn producer_selector_data(&self) -> Option<&ProducerSelector> {
        self.producer_selector_data.as_ref()
    }

    pub fn set_producer_selector_data(&mut self, data: ProducerSelector) {
        self.producer_selector_data = Some(data);
    }

    pub fn block_version_state(&self) -> Option<&BlockProtocolVersionState> {
        self.block_version_state.as_ref()
    }

    pub fn set_block_version_state(&mut self, state: BlockProtocolVersionState) {
        self.block_version_state = Some(state);
    }

    pub fn promoted_block_id(&self) -> Option<&BlockIdentifier> {
        self.promoted_block_id.as_ref()
    }

    pub fn set_promoted_block_id(&mut self, id: Option<BlockIdentifier>) {
        self.promoted_block_id = id;
    }

    pub fn known_children_incomplete(
        &self,
    ) -> &HashMap<ThreadIdentifier, HashSet<TemporaryBlockId>> {
        &self.known_children_incomplete
    }

    /// Add a reference to a temporary child block.
    pub fn add_incomplete_child(&mut self, thread_id: ThreadIdentifier, temp_id: TemporaryBlockId) {
        self.known_children_incomplete.entry(thread_id).or_default().insert(temp_id);
    }
}

impl std::fmt::Debug for TemporaryBlockState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporaryBlockState")
            .field("temporary_id", &self.temporary_id)
            .field("thread_identifier", &self.thread_identifier)
            .field("block_seq_no", &self.block_seq_no)
            .finish()
    }
}
