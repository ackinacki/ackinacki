use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;

use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::repository::repository_impl::RepositoryImpl;
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
