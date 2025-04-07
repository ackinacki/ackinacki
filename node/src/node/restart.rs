// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use crate::bls::envelope::BLSSignedEnvelope;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::repository::repository_impl::RepositoryImpl;
use crate::utilities::guarded::Guarded;

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    pub(crate) fn restart_bk(&mut self) -> anyhow::Result<()> {
        tracing::trace!("Restart BK");

        for (block_state, candidate_block) in self.unprocessed_blocks_cache.clone_queue().values() {
            // If block was applied, perform on_block_appended
            if block_state.guarded(|e| e.is_block_already_applied()) {
                self.shared_services.on_block_appended(candidate_block.data());
            }
        }
        Ok(())
    }
}
