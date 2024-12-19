use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::gosh_bls::PubKey;
use crate::node::SignerIndex;
use crate::types::BlockSeqNo;

#[derive(Clone, Debug)]
pub struct BlockKeeperRing {
    sets: Arc<Mutex<BTreeMap<BlockSeqNo, BlockKeeperSet>>>,
}

impl BlockKeeperRing {
    pub fn new(block_seq_no: BlockSeqNo, block_keeper_set: BlockKeeperSet) -> Self {
        let mut result = Self { sets: Arc::new(Mutex::new(BTreeMap::default())) };
        result.insert(block_seq_no, block_keeper_set);
        result
    }

    // pub fn get_block_keeper_set(
    // &self,
    // block_seq_no: &BlockSeqNo,
    // thread_id: &ThreadIdentifier,
    // ) -> BlockKeeperSet {
    // The same set, yet rehashed based on the thread id
    // ...
    // }

    pub fn get_block_keeper_pubkeys(
        &self,
        block_seq_no: &BlockSeqNo,
    ) -> HashMap<SignerIndex, PubKey> {
        let bk_sets = { self.sets.lock().unwrap().clone() };
        for (seq_no, bk_set) in bk_sets.iter().rev() {
            if seq_no > block_seq_no {
                continue;
            }
            return bk_set.iter().map(|(k, v)| (*k, v.pubkey.clone())).collect();
        }
        panic!("Failed to find BK set for block with seq_no: {block_seq_no:?}")
    }

    pub fn insert(&mut self, block_seq_no: BlockSeqNo, block_keeper_set: BlockKeeperSet) {
        self.sets.lock().unwrap().insert(block_seq_no, block_keeper_set);
    }

    pub fn clone_inner(&self) -> BTreeMap<BlockSeqNo, BlockKeeperSet> {
        self.sets.lock().unwrap().clone()
    }

    pub fn replace(&mut self, block_keeper_sets: BTreeMap<BlockSeqNo, BlockKeeperSet>) {
        assert!(!block_keeper_sets.is_empty());
        let mut guarded = self.sets.lock().unwrap();
        let _ = std::mem::replace(&mut *guarded, block_keeper_sets);
    }

    pub fn with_last_entry<F, T>(&mut self, f: F) -> T
    where
        F: FnOnce(
            Option<std::collections::btree_map::OccupiedEntry<'_, BlockSeqNo, BlockKeeperSet>>,
        ) -> T,
    {
        let mut guard = self.sets.lock().unwrap();
        f(guard.last_entry())
    }
}
