use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use derive_debug::Dbg;
use parking_lot::Mutex;

use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::block_state::repository::BlockState;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockIndex;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[allow(clippy::mutable_key_type)]
pub type UnfinalizedBlocksSnapshot =
    BTreeMap<BlockIndex, (BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>)>;

impl AllowGuardedMut for UnfinalizedBlocksSnapshot {}

#[derive(Default, Dbg)]
pub struct UnfinalizedBlocksData {
    #[dbg(skip)]
    main_map: UnfinalizedBlocksSnapshot,
    identifier_to_seqno: HashMap<BlockIdentifier, BlockSeqNo>,
}

impl UnfinalizedBlocksData {
    fn insert(
        &mut self,
        index: BlockIndex,
        value: (BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>),
    ) {
        self.identifier_to_seqno.insert(index.block_identifier().clone(), *index.block_seq_no());
        self.main_map.insert(index, value);
    }

    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&BlockIndex, &mut (BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>)) -> bool,
    {
        let mut to_remove = vec![];
        for (index, value) in self.main_map.iter_mut() {
            if !f(index, value) {
                to_remove.push(index.clone());
            }
        }
        for index in to_remove {
            self.identifier_to_seqno.remove(index.block_identifier());
            self.main_map.remove(&index);
        }
    }
}

#[allow(clippy::mutable_key_type)]
#[derive(Clone, Dbg)]
// DO NOT ALLOW DEFAULTS. Requires notifications from a common repo set.
pub struct UnfinalizedCandidateBlockCollection {
    candidates: Arc<Mutex<UnfinalizedBlocksData>>,
    #[dbg(skip)]
    notifications: Arc<AtomicU32>,
}

impl AllowGuardedMut for UnfinalizedBlocksData {}

impl UnfinalizedCandidateBlockCollection {
    pub fn new(
        states: impl Iterator<Item = (BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>)>,
        notifications: Arc<AtomicU32>,
    ) -> Self {
        let mut data = UnfinalizedBlocksData::default();

        for (state, block) in states {
            let index = BlockIndex::from(block.as_ref());
            data.insert(index, (state, block));
        }

        Self { candidates: Arc::new(Mutex::new(data)), notifications }
    }

    pub fn get_block_by_id(
        &self,
        identifier: &BlockIdentifier,
    ) -> Option<Arc<Envelope<GoshBLS, AckiNackiBlock>>> {
        let data = self.candidates.lock();
        data.identifier_to_seqno.get(identifier).and_then(|seq_no| {
            let index = BlockIndex::new(*seq_no, identifier.clone());
            data.main_map.get(&index).map(|(_, block)| Arc::clone(block))
        })
    }

    #[allow(clippy::mutable_key_type)]
    pub fn clone_queue(&self) -> UnfinalizedBlocksSnapshot {
        self.candidates.guarded(|e| e.main_map.clone())
    }

    pub fn insert(&self, block_state: BlockState, block: Envelope<GoshBLS, AckiNackiBlock>) {
        let index = BlockIndex::from(&block);
        let mut data = self.candidates.lock();
        data.insert(index, (block_state, block.into()));
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
