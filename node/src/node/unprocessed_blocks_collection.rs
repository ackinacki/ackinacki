use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use derive_debug::Dbg;
use derive_getters::Getters;
use parking_lot::Mutex;

use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::block_state::repository::BlockState;
use crate::types::notification::Notification;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockIndex;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[allow(clippy::mutable_key_type)]
#[derive(Default, Getters)]
pub struct UnfinalizedBlocksSnapshot {
    blocks: BTreeMap<BlockIndex, (BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>)>,
    notifications_stamp: u32,
    block_id_set: HashSet<BlockIdentifier>,
}

impl AllowGuardedMut for UnfinalizedBlocksSnapshot {}

#[derive(Default, Dbg)]
struct UnfinalizedBlocksData {
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
        let block_id = index.block_identifier().clone();
        self.identifier_to_seqno.insert(block_id.clone(), *index.block_seq_no());
        self.main_map.blocks.insert(index, value);
        self.main_map.block_id_set.insert(block_id);
    }

    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&BlockIndex, &mut (BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>)) -> bool,
    {
        let mut to_remove = vec![];
        for (index, value) in self.main_map.blocks.iter_mut() {
            if !f(index, value) {
                to_remove.push(index.clone());
            }
        }
        for index in to_remove {
            self.identifier_to_seqno.remove(index.block_identifier());
            let block_id = index.block_identifier().clone();
            self.main_map.blocks.remove(&index);
            self.main_map.block_id_set.remove(&block_id);
        }
    }
}

#[allow(clippy::mutable_key_type)]
#[derive(Clone, Dbg)]
#[allow(clippy::disallowed_types)]
// DO NOT ALLOW DEFAULTS. Requires notifications from a common repo set.
pub struct UnfinalizedCandidateBlockCollection {
    candidates: Arc<Mutex<UnfinalizedBlocksData>>,
    #[dbg(skip)]
    notifications: Notification,
}

impl AllowGuardedMut for UnfinalizedBlocksData {}

impl UnfinalizedCandidateBlockCollection {
    pub fn new(
        states: impl Iterator<Item = (BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>)>,
    ) -> Self {
        let mut data = UnfinalizedBlocksData::default();
        let notifications = Notification::new();

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
            data.main_map.blocks.get(&index).map(|(_, block)| Arc::clone(block))
        })
    }

    #[allow(clippy::mutable_key_type)]
    pub fn clone_queue(&self) -> UnfinalizedBlocksSnapshot {
        self.candidates.guarded(|e| {
            let notifications_stamp = self.notifications.stamp();
            UnfinalizedBlocksSnapshot {
                blocks: e.main_map.blocks().clone(),
                notifications_stamp,
                block_id_set: e.main_map.block_id_set.clone(),
            }
        })
    }

    pub fn insert(&mut self, block_state: BlockState, block: Envelope<GoshBLS, AckiNackiBlock>) {
        let index = BlockIndex::from(&block);
        let mut data_lock = self.candidates.lock();
        block_state.guarded_mut(|e| e.add_subscriber(self.notifications().clone()));
        data_lock.insert(index, (block_state, block.into()));
        drop(data_lock);
        self.touch();
    }

    pub fn remove_finalized_and_invalidated_blocks(&mut self) {
        self.retain(|candidate| candidate.guarded(|e| !e.is_finalized() && !e.is_invalidated()));
    }

    pub fn remove_old_blocks(&mut self, last_finalized_block_seq_no: &BlockSeqNo) {
        self.retain(|candidate| {
            candidate.guarded(|e| {
                e.block_seq_no().map(|seq_no| seq_no > *last_finalized_block_seq_no).unwrap_or(true)
            })
        });
    }

    pub fn retain<F>(&mut self, mut action: F)
    where
        F: FnMut(&BlockState) -> bool,
    {
        let mut has_removed = false;
        let mut removed = vec![];
        self.candidates.guarded_mut(|e| {
            e.retain(|_, (block_state, _)| {
                let retain = action(block_state);
                if !retain {
                    tracing::trace!(
                        "remove block from UnfinalizedCandidateBlockCollection: {:?}",
                        block_state.block_identifier()
                    );
                    has_removed = true;
                    removed.push(block_state.clone());
                }
                retain
            });
        });
        if has_removed {
            self.touch();
        }
        for block_state in removed.into_iter() {
            block_state.guarded_mut(|e| e.remove_subscriber(self.notifications()));
        }
    }

    pub fn notifications(&self) -> &Notification {
        &self.notifications
    }

    pub fn touch(&mut self) {
        self.notifications.touch();
    }
}
