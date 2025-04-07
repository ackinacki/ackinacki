// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use crate::bls::envelope::BLSSignedEnvelope;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::ThreadIdentifier;

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    pub(crate) fn get_block_thread_id(
        &self,
        block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<ThreadIdentifier> {
        Ok(block.data().get_common_section().thread_id)
    }
}
