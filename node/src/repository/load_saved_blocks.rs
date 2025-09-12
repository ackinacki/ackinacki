use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;

use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

pub trait SavedBlocksLoader {
    fn load_saved_blocks(
        &mut self,
        block_state_repository: &BlockStateRepository,
    ) -> anyhow::Result<
        HashMap<ThreadIdentifier, Vec<(BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>)>>,
    >;
}

impl SavedBlocksLoader for RepositoryImpl {
    fn load_saved_blocks(
        &mut self,
        block_state_repository: &BlockStateRepository,
    ) -> anyhow::Result<
        HashMap<ThreadIdentifier, Vec<(BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>)>>,
    > {
        let mut result: HashMap<
            ThreadIdentifier,
            Vec<(BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>)>,
        > = HashMap::new();
        let blocks_dir = self.get_blocks_dir_path();
        if !blocks_dir.exists() {
            return Ok(result);
        }
        let paths = fs::read_dir(&blocks_dir)?;
        let max_seq_no_diff = self.finalized_blocks().guarded(|repo| repo.cache_size()) as u32;
        let mut last_finalized_seq_nos = HashMap::new();
        for (thread_id, metadata) in self.get_all_metadata().lock().iter() {
            let seq_no = metadata.guarded(|m| m.last_finalized_block_seq_no);
            tracing::trace!("Last finalized seq no for {thread_id:?} is {seq_no:?}");
            last_finalized_seq_nos.insert(*thread_id, seq_no);
        }
        for path in paths.flatten() {
            if let Ok(block_id) =
                BlockIdentifier::from_str(path.file_name().to_str().unwrap_or_default())
            {
                let Ok(Some(block)) = Self::load_block(&blocks_dir, &block_id) else {
                    tracing::trace!("Failed to load block {:?}", block_id);
                    continue;
                };

                if let Ok(state) = block_state_repository.get(&block_id) {
                    if state.guarded(|e| e.is_finalized() && !e.is_invalidated()) {
                        let (Some(block_seq_no), Some(thread_id)) =
                            state.guarded(|e| (*e.block_seq_no(), *e.thread_identifier()))
                        else {
                            tracing::trace!("finalized block {:?} does not have seq_no or thread_id set. Skip it", block_id);
                            continue;
                        };
                        if let Some(last_finalized_seq_no) = last_finalized_seq_nos.get(&thread_id)
                        {
                            if block_seq_no + max_seq_no_diff <= *last_finalized_seq_no {
                                tracing::trace!(
                                    "finalized block {:?} is too old. Skip it",
                                    block_id
                                );
                                continue;
                            }
                        }
                        tracing::trace!("add finalized block {:?}", block_id);
                        self.finalized_blocks_mut().guarded_mut(|e| e.store(state.clone(), block));
                        continue;
                    }
                    if let Some(thread_id) = state.guarded(|e| *e.thread_identifier()) {
                        tracing::trace!("add unfinalized block {:?}", block_id);
                        result
                            .entry(thread_id)
                            .and_modify(|v| v.push((state.clone(), Arc::new(block.clone()))))
                            .or_insert(vec![(state.clone(), Arc::new(block.clone()))]);
                    }
                }
            }
        }
        Ok(result)
    }
}
