// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::Arc;

use super::associated_types::NodeAssociatedTypes;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::gosh_bls::PubKey;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::node::SignerIndex;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::BlockIdentifier;
use crate::utilities::guarded::Guarded;

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    pub fn get_block_keeper_set_for_block_id(
        &self,
        block_id: BlockIdentifier,
    ) -> Option<Arc<BlockKeeperSet>> {
        let state = self.block_state_repository.get(&block_id).unwrap();
        state.guarded(|e| e.bk_set().clone())
    }

    pub fn get_block_keeper_pubkeys(
        &self,
        block_id: BlockIdentifier,
    ) -> Option<HashMap<SignerIndex, PubKey>> {
        self.get_block_keeper_set_for_block_id(block_id)
            .as_ref()
            .map(|set| set.get_pubkeys_by_signers().clone())
    }

    pub(crate) fn check_block_signature(
        &self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> Option<bool> {
        let signatures_map = self.get_block_keeper_pubkeys(candidate_block.data().parent())?;
        let is_valid = candidate_block
            .verify_signatures(&signatures_map)
            .expect("Signatures verification should not crash.");
        if !is_valid {
            tracing::trace!("Signature verification failed: {}", candidate_block);
        }
        Some(is_valid)
    }
}
