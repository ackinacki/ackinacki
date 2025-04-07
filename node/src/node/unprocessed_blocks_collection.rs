use std::collections::BTreeMap;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::block_state::repository::BlockState;
use crate::types::AckiNackiBlock;
use crate::types::BlockIndex;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[allow(clippy::mutable_key_type)]
#[derive(Clone)]
// DO NOT ALLOW DEFAULTS. Requires notifications from a common repo set.
pub struct UnfinalizedCandidateBlockCollection {
    candidates: Arc<Mutex<UnfinalizedBlocksSnapshot>>,
    notifications: Arc<AtomicU32>,
}

#[allow(clippy::mutable_key_type)]
// Change to BTreeMap( Struct(seq_no, id) -> (BlockState, Envelope<Block>)
pub type UnfinalizedBlocksSnapshot =
    BTreeMap<BlockIndex, (BlockState, Envelope<GoshBLS, AckiNackiBlock>)>;

impl AllowGuardedMut for UnfinalizedBlocksSnapshot {}

impl UnfinalizedCandidateBlockCollection {
    pub fn new(
        states: impl Iterator<Item = (BlockState, Envelope<GoshBLS, AckiNackiBlock>)>,
        notifications: Arc<AtomicU32>,
    ) -> Self {
        let states_with_indexes =
            states.map(|(state, block)| (BlockIndex::from(&block), (state, block)));
        Self {
            candidates: Arc::new(Mutex::new(BTreeMap::from_iter(states_with_indexes))),
            notifications,
        }
    }

    #[allow(clippy::mutable_key_type)]
    pub fn clone_queue(&self) -> UnfinalizedBlocksSnapshot {
        self.candidates.guarded(|e| e.clone())
    }

    pub fn insert(&self, block_state: BlockState, block: Envelope<GoshBLS, AckiNackiBlock>) {
        let index = BlockIndex::from(&block);
        self.candidates.guarded_mut(|e| e.insert(index, (block_state, block)));
        self.notifications.fetch_add(1, Ordering::Relaxed);
        atomic_wait::wake_all(self.notifications.as_ref());
    }

    pub fn remove_finalized_and_invalidated_blocks(&self) {
        self.retain(|candidate| candidate.guarded(|e| !e.is_finalized() && !e.is_invalidated()));
    }

    pub fn retain<F>(&self, mut action: F)
    where
        F: FnMut(&BlockState) -> bool,
    {
        let mut has_removed = false;
        self.candidates.guarded_mut(|e| {
            e.retain(|_, (block_state, _)| {
                let retain = action(block_state);
                if !retain {
                    has_removed = true;
                }
                retain
            });
        });
        if has_removed {
            self.notifications.fetch_add(1, Ordering::Relaxed);
            atomic_wait::wake_all(self.notifications.as_ref());
        }
    }

    pub fn notifications(&self) -> &AtomicU32 {
        self.notifications.as_ref()
    }
}
