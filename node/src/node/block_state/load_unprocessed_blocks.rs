use std::collections::HashMap;
use std::fs;
use std::str::FromStr;

use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;

pub trait UnprocessedBlocksLoader {
    fn load_unprocessed_blocks(&self)
        -> anyhow::Result<HashMap<ThreadIdentifier, Vec<BlockState>>>;
}

impl UnprocessedBlocksLoader for BlockStateRepository {
    fn load_unprocessed_blocks(
        &self,
    ) -> anyhow::Result<HashMap<ThreadIdentifier, Vec<BlockState>>> {
        let mut result: HashMap<ThreadIdentifier, Vec<BlockState>> = HashMap::new();
        let paths = fs::read_dir(self.block_state_repo_data_dir())?;

        for path in paths.flatten() {
            if let Ok(block_id) =
                BlockIdentifier::from_str(path.file_name().to_str().unwrap_or_default())
            {
                if let Ok(state) = self.get(&block_id) {
                    if state.guarded(|e| e.is_finalized() || e.is_invalidated() || !e.is_stored()) {
                        continue;
                    }
                    if let Some(thread_id) = state.guarded(|e| *e.thread_identifier()) {
                        result
                            .entry(thread_id)
                            .and_modify(|v| v.push(state.clone()))
                            .or_insert(vec![state.clone()]);
                    }
                }
            }
        }
        Ok(result)
    }
}
