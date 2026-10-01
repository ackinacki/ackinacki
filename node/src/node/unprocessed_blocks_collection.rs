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

    fn remove_indices(&mut self, indices: impl IntoIterator<Item = BlockIndex>) -> Vec<BlockState> {
        let mut removed = Vec::new();
        for index in indices {
            let block_id = *index.block_identifier();
            if let Some((block_state, _)) = self.main_map.blocks.remove(&index) {
                if self.identifier_to_seqno.get(&block_id) == Some(index.block_seq_no()) {
                    self.identifier_to_seqno.remove(&block_id);
                }
                self.main_map.block_id_set.remove(&block_id);
                removed.push(block_state);
            }
        }
        removed
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
    fn rejects(&self, block_seq_no: Option<BlockSeqNo>, block_height: Option<BlockHeight>) -> bool {
        if let Some(seq_no) = block_seq_no {
            if self.block_seq_no != BlockSeqNo::default() && seq_no <= self.block_seq_no {
                return true;
            }
        }
        match self.cutoff {
            Some(UnfinalizedCutoff::Height(cutoff)) => block_height
                .and_then(|height| height.signed_distance_to(&cutoff))
                .map(|distance| distance > 0)
                .unwrap_or(false),
            Some(UnfinalizedCutoff::SeqNo(cutoff)) => {
                block_seq_no.map(|seq_no| seq_no < cutoff).unwrap_or(false)
            }
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
            state.guarded_mut(|e| e.add_subscriber(notifications.clone()));
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
        let (block_seq_no, block_height) =
            block_state.guarded(|e| (*e.block_seq_no(), *e.block_height()));
        block_state.guarded_mut(|e| e.add_subscriber(self.notifications().clone()));
        let rejected = self.candidates.guarded_mut(|data| {
            if data.filter.rejects(block_seq_no, block_height) {
                true
            } else {
                data.insert(index.clone(), (block_state.clone(), block));
                false
            }
        });
        if rejected {
            tracing::debug!(
                target: "node",
                "UnfinalizedCandidateBlockCollection rejected prehistoric block insert: {:?}",
                index,
            );
            block_state.guarded_mut(|e| e.remove_subscriber(self.notifications()));
            return;
        }
        self.touch();
    }

    pub fn insert(&self, block_state: BlockState, block: Envelope<AckiNackiBlock>) {
        self.insert_arc(block_state, Arc::new(block))
    }

    pub fn remove_finalized_and_invalidated_blocks(&self) {
        self.retain(|candidate| candidate.guarded(|e| !e.is_finalized() && !e.is_invalidated()));
    }

    pub fn remove_old_blocks(&self, last_finalized_block_seq_no: &BlockSeqNo) {
        let removed = self.candidates.guarded_mut(|data| {
            let indices = data
                .main_map
                .blocks
                .keys()
                .take_while(|index| index.block_seq_no() <= last_finalized_block_seq_no)
                .cloned()
                .collect::<Vec<_>>();
            data.remove_indices(indices)
        });
        self.finish_removal(removed);
    }

    pub fn retain<F>(&self, mut action: F)
    where
        F: FnMut(&BlockState) -> bool,
    {
        self.retain_inner(&mut action);
    }

    fn retain_inner<F>(&self, action: &mut F) -> usize
    where
        F: FnMut(&BlockState) -> bool,
    {
        // Never acquire a BlockState lock while holding the collection lock. During catch-up a
        // BlockState can be busy for a long time, and holding the collection lock here also blocks
        // finalization's clone_queue().
        let candidates = self.candidates.guarded(|data| {
            data.main_map
                .blocks
                .iter()
                .map(|(index, (block_state, _))| (index.clone(), block_state.clone()))
                .collect::<Vec<_>>()
        });
        let indices = candidates
            .into_iter()
            .filter_map(|(index, block_state)| {
                if action(&block_state) {
                    None
                } else {
                    tracing::trace!(
                        "remove block from UnfinalizedCandidateBlockCollection: {:?}",
                        block_state.block_identifier()
                    );
                    Some(index)
                }
            })
            .collect::<Vec<_>>();
        let removed = self.candidates.guarded_mut(|data| data.remove_indices(indices));
        let removed_count = removed.len();
        self.finish_removal(removed);
        removed_count
    }

    fn finish_removal(&self, removed: Vec<BlockState>) {
        if !removed.is_empty() {
            self.touch();
        }
        for block_state in removed {
            block_state.guarded_mut(|e| e.remove_subscriber(self.notifications()));
        }
    }

    pub fn prune_before_snapshot(&self, cutoff: UnfinalizedCutoff) -> usize {
        self.candidates.guarded_mut(|data| {
            data.filter.cutoff = merge_cutoff(data.filter.cutoff, cutoff);
        });
        self.retain_inner(&mut |block_state| match cutoff {
            UnfinalizedCutoff::Height(cutoff_height) => block_state
                .guarded(|state| *state.block_height())
                .and_then(|height| height.signed_distance_to(&cutoff_height))
                .map(|distance| distance <= 0)
                .unwrap_or(true),
            UnfinalizedCutoff::SeqNo(cutoff_seq_no) => block_state
                .guarded(|state| *state.block_seq_no())
                .map(|seq_no| seq_no >= cutoff_seq_no)
                .unwrap_or(true),
        })
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
    use std::time::Duration;

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
        common_section.set_descendant_producer_selector(Some(make_selector(parent_block_id)));
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
        collection.insert_arc(old_state.clone(), old_block);
        let (snapshot, _) = collection.clone_queue();

        assert_eq!(snapshot.blocks().len(), 1);

        let after_rejection = collection.notifications().stamp();
        old_state.guarded_mut(|e| e.set_finalized()).unwrap();
        assert_eq!(collection.notifications().stamp(), after_rejection);
    }

    #[test]
    fn remove_old_blocks_uses_block_index() {
        let collection = collection_with_blocks([(8, 18), (9, 17), (10, 16), (11, 15)]);

        collection.remove_old_blocks(&BlockSeqNo::from(10));
        let (snapshot, _) = collection.clone_queue();

        assert_eq!(snapshot.blocks().len(), 1);
        assert_eq!(
            snapshot.blocks().first_key_value().unwrap().0.block_seq_no(),
            &BlockSeqNo::from(11)
        );
    }

    #[test]
    fn retain_does_not_hold_collection_lock_while_inspecting_state() {
        let collection = collection_with_blocks([(10, 10)]);
        let retained_collection = collection.clone();
        let (predicate_entered_tx, predicate_entered_rx) = std::sync::mpsc::channel();
        let (release_predicate_tx, release_predicate_rx) = std::sync::mpsc::channel();
        let retain_thread = std::thread::Builder::new()
            .name("test".to_string())
            .spawn(move || {
                let mut predicate_entered_tx = Some(predicate_entered_tx);
                let mut release_predicate_rx = Some(release_predicate_rx);
                retained_collection.retain(move |_| {
                    if let Some(tx) = predicate_entered_tx.take() {
                        tx.send(()).unwrap();
                        release_predicate_rx.take().unwrap().recv().unwrap();
                    }
                    true
                });
            })
            .unwrap();
        predicate_entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();

        let cloned_collection = collection.clone();
        let (clone_finished_tx, clone_finished_rx) = std::sync::mpsc::channel();
        let clone_thread = std::thread::Builder::new()
            .name("test".to_string())
            .spawn(move || {
                let (snapshot, _) = cloned_collection.clone_queue();
                clone_finished_tx.send(snapshot.blocks().len()).unwrap();
            })
            .unwrap();
        let clone_result = clone_finished_rx.recv_timeout(Duration::from_secs(5));

        release_predicate_tx.send(()).unwrap();
        retain_thread.join().unwrap();
        clone_thread.join().unwrap();
        assert_eq!(clone_result.unwrap(), 1);
    }

    #[test]
    fn new_subscribes_to_loaded_block_state_changes() {
        let (state, block) = make_block_state(10, 10);
        let collection =
            UnfinalizedCandidateBlockCollection::new([(state.clone(), block)].into_iter());

        let before = collection.notifications().stamp();
        state.guarded_mut(|e| e.set_finalized()).unwrap();

        assert_ne!(collection.notifications().stamp(), before);
    }
}
