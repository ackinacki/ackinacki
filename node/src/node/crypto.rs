// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::sync::Arc;

use super::associated_types::NodeAssociatedTypes;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
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
    pub(crate) fn is_candidate_block_signed_by_this_node(
        &self,
        candidate_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<bool> {
        let block_id = candidate_block.data().identifier();
        let signature_occurrences = candidate_block.clone_signature_occurrences();
        if let Some(self_signer_index) = self.get_node_signer_index_for_block_id(block_id) {
            return Ok({
                match signature_occurrences.get(&self_signer_index) {
                    None | Some(0) => false,
                    Some(_count) => true,
                }
            });
        }
        Ok(false)
    }

    pub(crate) fn get_node_signer_index_for_block_id(
        &self,
        block_id: BlockIdentifier,
    ) -> Option<SignerIndex> {
        let state = self.block_state_repository.get(&block_id).unwrap();
        let state_in = state.lock();
        state_in.get_signer_index_for_node_id(&self.config.local.node_id)
    }

    pub(crate) fn _get_signer_data_for_block_id(
        &self,
        block_id: BlockIdentifier,
    ) -> Option<(SignerIndex, Secret)> {
        let state = self.block_state_repository.get(&block_id).unwrap();
        let state_in = state.lock();
        if let Some(bk_data) = state_in.get_bk_data_for_node_id(&self.config.local.node_id) {
            if let Some(secret) = self.bls_keys_map.guarded(|map| map.get(&bk_data.pubkey).cloned())
            {
                return Some((bk_data.signer_index, secret.0));
            }
        }
        None
    }

    pub fn get_block_keeper_set_for_block_id(
        &self,
        block_id: BlockIdentifier,
    ) -> Option<Arc<BlockKeeperSet>> {
        let state = self.block_state_repository.get(&block_id).unwrap();
        let state_in = state.lock();
        state_in.bk_set().clone()
    }

    pub fn get_block_keeper_pubkeys(
        &self,
        block_id: BlockIdentifier,
    ) -> Option<HashMap<SignerIndex, PubKey>> {
        self.get_block_keeper_set_for_block_id(block_id)
            .as_ref()
            .map(|set| set.get_pubkeys_by_signers().clone())
    }

    pub(crate) fn _is_this_node_in_block_keeper_set(
        &self,
        block_id: BlockIdentifier,
    ) -> Option<bool> {
        match self.get_block_keeper_set_for_block_id(block_id.clone()).as_ref() {
            Some(block_keeper_set) => {
                let res = {
                    if let Some(signer_index) =
                        self.get_node_signer_index_for_block_id(block_id.clone())
                    {
                        block_keeper_set.contains_signer(&signer_index)
                    } else {
                        false
                    }
                };
                tracing::trace!("is_this_node_in_block_keeper_set {}", res);
                Some(res)
            }
            None => None,
        }
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
