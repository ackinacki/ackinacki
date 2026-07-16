use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use derive_debug::Dbg;
use derive_getters::Getters;
use node_types::BlockIdentifier;
use parking_lot::Mutex;
use typed_builder::TypedBuilder;

use crate::bls::envelope::Envelope;
use crate::node::block_state::repository::BlockState;
use crate::types::notification::Notification;
use crate::types::AckiNackiBlock;
use crate::types::BlockHeight;
use crate::types::BlockIndex;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[allow(clippy::mutable_key_type)]
#[derive(Default, Getters)]
pub struct UnfinalizedBlocksSnapshot {
    blocks: BTreeMap<BlockIndex, (BlockState, Arc<Envelope<AckiNackiBlock>>)>,
    notifications_stamp: u32,
    block_id_set: HashSet<BlockIdentifier>,
}

impl AllowGuardedMut for UnfinalizedBlocksSnapshot {}

#[derive(Default, Dbg)]
struct UnfinalizedBlocksData {
    #[dbg(skip)]
    main_map: UnfinalizedBlocksSnapshot,
    identifier_to_seqno: HashMap<BlockIdentifier, BlockSeqNo>,
    filter: FilterPrehistoric,
}

impl UnfinalizedBlocksData {
    fn set_filter(&mut self, filter: FilterPrehistoric) {
        self.filter = filter;
    }

    fn insert(&mut self, index: BlockIndex, value: (BlockState, Arc<Envelope<AckiNackiBlock>>)) {
        let block_id = *index.block_identifier();
        self.identifier_to_seqno.insert(block_id, *index.block_seq_no());
        self.main_map.blocks.insert(index, value);
        self.main_map.block_id_set.insert(block_id);
    }

    fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&BlockIndex, &mut (BlockState, Arc<Envelope<AckiNackiBlock>>)) -> bool,
    {
        let mut to_remove = vec![];
        for (index, value) in self.main_map.blocks.iter_mut() {
            if !f(index, value) {
                to_remove.push(index.clone());
            }
        }
        for index in to_remove {
            self.identifier_to_seqno.remove(index.block_identifier());
            let block_id = *index.block_identifier();
            self.main_map.blocks.remove(&index);
            self.main_map.block_id_set.remove(&block_id);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnfinalizedCutoff {
    Height(BlockHeight),
    SeqNo(BlockSeqNo),
}

#[derive(Default, Clone, PartialEq, Eq, Getters, TypedBuilder, Dbg)]
pub struct FilterPrehistoric {
    block_seq_no: BlockSeqNo,
    #[builder(default)]
    cutoff: Option<UnfinalizedCutoff>,
}

impl FilterPrehistoric {
    pub fn rejects(&self, block_state: &BlockState) -> bool {
        if let Some(seq_no) = block_state.guarded(|e| *e.block_seq_no()) {
            if self.block_seq_no != BlockSeqNo::default() && seq_no <= self.block_seq_no {
                return true;
            }
        }
        match self.cutoff {
            Some(UnfinalizedCutoff::Height(cutoff)) => block_state
                .guarded(|e| *e.block_height())
                .and_then(|height| height.signed_distance_to(&cutoff))
                .map(|distance| distance > 0)
                .unwrap_or(false),
            Some(UnfinalizedCutoff::SeqNo(cutoff)) => block_state
                .guarded(|e| *e.block_seq_no())
                .map(|seq_no| seq_no < cutoff)
                .unwrap_or(false),
            None => false,
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
    pub fn new(states: impl Iterator<Item = (BlockState, Arc<Envelope<AckiNackiBlock>>)>) -> Self {
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
    ) -> Option<Arc<Envelope<AckiNackiBlock>>> {
        let data = self.candidates.lock();
        data.identifier_to_seqno.get(identifier).and_then(|seq_no| {
            let index = BlockIndex::new(*seq_no, *identifier);
            data.main_map.blocks.get(&index).map(|(_, block)| Arc::clone(block))
        })
    }

    #[allow(clippy::mutable_key_type)]
    pub fn clone_queue(&self) -> (UnfinalizedBlocksSnapshot, FilterPrehistoric) {
        self.candidates.guarded(|e| {
            let notifications_stamp = self.notifications.stamp();
            (
                UnfinalizedBlocksSnapshot {
                    blocks: e.main_map.blocks().clone(),
                    notifications_stamp,
                    block_id_set: e.main_map.block_id_set.clone(),
                },
                e.filter.clone(),
            )
        })
    }

    pub fn insert_arc(&self, block_state: BlockState, block: Arc<Envelope<AckiNackiBlock>>) {
        let index = BlockIndex::from(block.as_ref());
        tracing::trace!(target: "node", "UnfinalizedCandidateBlockCollection insert {:?}", index);
        let mut data_lock = self.candidates.lock();
        if data_lock.filter.rejects(&block_state) {
            tracing::debug!(
                target: "node",
                "UnfinalizedCandidateBlockCollection rejected prehistoric block insert: {:?}",
                index,
            );
            return;
        }
        block_state.guarded_mut(|e| e.add_subscriber(self.notifications().clone()));
        data_lock.insert(index, (block_state, block));
        drop(data_lock);
        self.touch();
    }

    pub fn insert(&self, block_state: BlockState, block: Envelope<AckiNackiBlock>) {
        self.insert_arc(block_state, Arc::new(block))
    }

    pub fn remove_finalized_and_invalidated_blocks(&self) {
        self.retain(|candidate| candidate.guarded(|e| !e.is_finalized() && !e.is_invalidated()));
    }

    pub fn remove_old_blocks(&self, last_finalized_block_seq_no: &BlockSeqNo) {
        self.retain(|candidate| {
            candidate.guarded(|e| {
                e.block_seq_no().map(|seq_no| seq_no > *last_finalized_block_seq_no).unwrap_or(true)
            })
        });
    }

    pub fn retain<F>(&self, mut action: F)
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

    pub fn prune_before_snapshot(&self, cutoff: UnfinalizedCutoff) -> usize {
        let mut removed = vec![];
        let mut pruned = 0usize;
        self.candidates.guarded_mut(|e| {
            e.filter.cutoff = merge_cutoff(e.filter.cutoff, cutoff);
            e.retain(|_, (block_state, _)| {
                let keep = match cutoff {
                    UnfinalizedCutoff::Height(cutoff_height) => block_state
                        .guarded(|state| *state.block_height())
                        .and_then(|height| height.signed_distance_to(&cutoff_height))
                        .map(|distance| distance <= 0)
                        .unwrap_or(true),
                    UnfinalizedCutoff::SeqNo(cutoff_seq_no) => block_state
                        .guarded(|state| *state.block_seq_no())
                        .map(|seq_no| seq_no >= cutoff_seq_no)
                        .unwrap_or(true),
                };
                if !keep {
                    pruned += 1;
                    removed.push(block_state.clone());
                }
                keep
            });
        });
        if pruned > 0 {
            self.touch();
        }
        for block_state in removed.into_iter() {
            block_state.guarded_mut(|e| e.remove_subscriber(self.notifications()));
        }
        pruned
    }

    pub fn notifications(&self) -> &Notification {
        &self.notifications
    }

    pub fn touch(&self) {
        let mut notifications = self.notifications.clone();
        notifications.touch();
    }

    pub fn update_filter(&self, filter_prehistoric: FilterPrehistoric) -> bool {
        let should_update = self.candidates.guarded_mut(|e| {
            let old_filter = e.filter.clone();
            if old_filter.block_seq_no() < filter_prehistoric.block_seq_no() {
                let mut next_filter = filter_prehistoric.clone();
                next_filter.cutoff = old_filter.cutoff;
                e.set_filter(next_filter);
                true
            } else {
                false
            }
        });
        if should_update {
            self.retain(|candidate| {
                candidate.guarded(|e| {
                    e.block_seq_no()
                        .map(|seq_no| seq_no > *filter_prehistoric.block_seq_no())
                        .unwrap_or(true)
                })
            });
        }
        should_update
    }
}

fn merge_cutoff(
    current: Option<UnfinalizedCutoff>,
    incoming: UnfinalizedCutoff,
) -> Option<UnfinalizedCutoff> {
    match (current, incoming) {
        (None, incoming) => Some(incoming),
        (Some(UnfinalizedCutoff::Height(current)), UnfinalizedCutoff::Height(incoming)) => {
            let is_newer =
                current.signed_distance_to(&incoming).map(|distance| distance > 0).unwrap_or(false);
            Some(UnfinalizedCutoff::Height(if is_newer { incoming } else { current }))
        }
        (Some(UnfinalizedCutoff::SeqNo(current)), UnfinalizedCutoff::SeqNo(incoming)) => {
            Some(UnfinalizedCutoff::SeqNo(current.max(incoming)))
        }
        (Some(UnfinalizedCutoff::SeqNo(_)), UnfinalizedCutoff::Height(incoming)) => {
            Some(UnfinalizedCutoff::Height(incoming))
        }
        (Some(current @ UnfinalizedCutoff::Height(_)), UnfinalizedCutoff::SeqNo(_)) => {
            Some(current)
        }
    }
}

impl Drop for UnfinalizedCandidateBlockCollection {
    fn drop(&mut self) {
        if Arc::strong_count(&self.candidates) != 1 {
            return;
        }

        let blocks = self.candidates.guarded(|data| {
            data.main_map.blocks.values().map(|(state, _)| state.clone()).collect::<Vec<_>>()
        });

        for block_state in blocks {
            block_state.guarded_mut(|e| e.remove_subscriber(&self.notifications));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use account_state::DurableThreadAccountsStateDiff;
    use node_types::BlockIdentifier;
    use node_types::ThreadIdentifier;
    use tvm_block::Block;
    use tvm_block::BlockExtra;
    use tvm_block::BlockInfo;
    use tvm_block::MerkleUpdate;
    use tvm_block::ValueFlow;

    use super::*;
    use crate::bls::envelope::BLSSignedEnvelope;
    use crate::node::NodeIdentifier;
    use crate::node::SignerIndex;
    use crate::types::bp_selector::ProducerSelector;
    use crate::types::BlockRound;

    fn block_id(seed: u8) -> BlockIdentifier {
        BlockIdentifier::new([seed; 32])
    }

    fn thread_id() -> ThreadIdentifier {
        ThreadIdentifier::new(&block_id(u8::MAX), 7)
    }

    fn make_tvm_block(seq_no: u32) -> Block {
        let mut info = BlockInfo::new();
        info.set_seq_no(seq_no).unwrap();
        info.set_gen_utime_ms(1_770_201_296_000);
        Block::with_params(
            0,
            info,
            ValueFlow::default(),
            MerkleUpdate::default(),
            BlockExtra::default(),
        )
        .unwrap()
    }

    fn make_height(height: u64) -> BlockHeight {
        BlockHeight::builder().thread_identifier(thread_id()).height(height).build()
    }

    fn make_selector(parent_block_id: BlockIdentifier) -> ProducerSelector {
        ProducerSelector::builder().rng_seed_block_id(parent_block_id).index(0).build()
    }

    fn make_block_state(seq_no: u32, height: u64) -> (BlockState, Arc<Envelope<AckiNackiBlock>>) {
        let block_height = make_height(height);
        let parent_block_id = block_id(seq_no as u8);
        let mut block = AckiNackiBlock::new(
            parent_block_id,
            thread_id(),
            make_tvm_block(seq_no),
            NodeIdentifier::some_id(),
            0,
            vec![],
            SignerIndex::default(),
            vec![],
            None,
            BlockRound::default(),
            block_height,
            #[cfg(feature = "monitor-accounts-number")]
            0,
            #[cfg(feature = "protocol_version_hash_in_block")]
            Default::default(),
            DurableThreadAccountsStateDiff::default(),
            Default::default(),
            Default::default(),
        );
        let mut common_section = block.common_section().clone();
        common_section.set_producer_selector(Some(make_selector(parent_block_id)));
        block.set_common_section(common_section, true).unwrap();
        let envelope = Arc::new(Envelope::create(Default::default(), HashMap::new(), block));
        let block_state = BlockState::test();
        block_state
            .guarded_mut(|state| {
                state.set_block_seq_no(BlockSeqNo::from(seq_no))?;
                state.set_block_height(block_height)?;
                state.set_thread_identifier(thread_id())?;
                Ok::<(), anyhow::Error>(())
            })
            .unwrap();
        (block_state, envelope)
    }

    fn collection_with_blocks(
        values: impl IntoIterator<Item = (u32, u64)>,
    ) -> UnfinalizedCandidateBlockCollection {
        UnfinalizedCandidateBlockCollection::new(
            values.into_iter().map(|(seq_no, height)| make_block_state(seq_no, height)),
        )
    }

    #[test]
    fn prune_before_snapshot_by_height_keeps_cutoff_and_newer() {
        let collection = collection_with_blocks([(8, 8), (9, 9), (10, 10), (11, 11)]);

        let pruned = collection.prune_before_snapshot(UnfinalizedCutoff::Height(make_height(10)));
        let (snapshot, _) = collection.clone_queue();

        assert_eq!(pruned, 2);
        assert_eq!(snapshot.blocks().len(), 2);
        assert!(snapshot.blocks().values().all(|(state, _)| state.guarded(|s| s
            .block_height()
            .unwrap()
            .height()
            >= &10)));
    }

    #[test]
    fn prune_before_snapshot_by_seq_no_keeps_cutoff_and_newer() {
        let collection = collection_with_blocks([(8, 8), (9, 9), (10, 10), (11, 11)]);

        let pruned =
            collection.prune_before_snapshot(UnfinalizedCutoff::SeqNo(BlockSeqNo::from(10)));
        let (snapshot, _) = collection.clone_queue();

        assert_eq!(pruned, 2);
        assert_eq!(snapshot.blocks().len(), 2);
        assert!(
            snapshot
                .blocks()
                .values()
                .all(|(state, _)| state
                    .guarded(|s| s.block_seq_no().unwrap() >= BlockSeqNo::from(10)))
        );
    }

    #[test]
    fn filter_rejects_old_inserts_after_height_prune() {
        let collection = collection_with_blocks([(10, 10)]);
        collection.prune_before_snapshot(UnfinalizedCutoff::Height(make_height(10)));

        let (old_state, old_block) = make_block_state(9, 9);
        collection.insert_arc(old_state, old_block);
        let (snapshot, _) = collection.clone_queue();

        assert_eq!(snapshot.blocks().len(), 1);
    }
}
