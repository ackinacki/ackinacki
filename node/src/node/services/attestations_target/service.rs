// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::vec::Vec;

use typed_builder::TypedBuilder;

use crate::node::block_state::repository::BlockState;
use crate::node::block_state::state::AttestationsCount;
use crate::node::block_state::state::DescendantsChainLength;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocks;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocksSelectError;
use crate::node::BlockStateRepository;
use crate::node::SignerIndex;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ForkResolution;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

#[derive(TypedBuilder, Clone)]
pub struct AttestationsTargetService {
    repository: RepositoryImpl,
    blocks_states: BlockStateRepository,
}

impl AttestationsTargetService {
    pub fn evaluate(&mut self, blocks: Vec<BlockState>) {
        for block_state in blocks {
            let Some(thread_id) = block_state.guarded(|e| *e.thread_identifier()) else {
                continue;
            };
            let Ok((_, thread_last_finalized_block_seq_no)) =
                self.repository.select_thread_last_finalized_block(&thread_id)
            else {
                continue;
            };
            let _ = self.evaluate_chain(block_state, thread_last_finalized_block_seq_no);
        }
    }

    // Evaluate blocks.
    // Cutoff is set to the last finalized block in the **thread**
    // In case of a chain goes earlier than the cut off we can consider this chain
    // to be invalid.
    fn evaluate_chain(&mut self, tail: BlockState, cutoff: BlockSeqNo) -> anyhow::Result<()> {
        tracing::trace!("evaluate_chain: {:?}, cutoff: {:?}", tail.lock(), cutoff);
        use UnfinalizedAncestorBlocksSelectError::*;
        match self.blocks_states.select_unfinalized_ancestor_blocks(tail, cutoff) {
            Ok(chain) => self.evaluate_attestations(chain.into()),
            Err(IncompleteHistory) => Ok(()),

            // The block is earlier than the cutoff for the thread.
            //
            // We didn't hit any finalized block
            // AND this chain is referencing something earlier than the last finalized.
            // This means that this chain CAN NOT be finalized (it means invalid).
            Err(BlockSeqNoCutoff(chain)) => {
                tracing::error!("evaluate_chain: BlockSeqNoCutoff: {chain:?}");
                self.invalidate_blocks(chain)
            }
            Err(InvalidatedParent(chain)) => {
                tracing::error!("evaluate_chain: InvalidatedParent: {chain:?}");
                self.invalidate_blocks(chain)
            }

            // No worries we will try again in a few.
            // May be shoud touch repository though
            Err(FailedToLoadBlockState) => Ok(()),
        }
    }

    fn invalidate_blocks(&mut self, blocks: Vec<BlockState>) -> anyhow::Result<()> {
        for block in blocks {
            tracing::trace!("invalidate blocks {:?}", block.lock().block_identifier());
            block.lock().set_invalidated()?;
        }
        Ok(())
    }

    fn evaluate_attestations(&mut self, mut chain: VecDeque<BlockState>) -> anyhow::Result<()> {
        tracing::trace!(
            "evaluating attestations for chain: {:?}",
            chain
                .iter()
                .map(|e| e.guarded(|e| (
                    e.block_identifier().clone(),
                    *e.block_seq_no(),
                    e.verified_attestations().clone()
                )))
                .collect::<Vec<(
                    BlockIdentifier,
                    Option<BlockSeqNo>,
                    HashMap<BlockIdentifier, HashSet<SignerIndex>>
                )>>()
        );
        loop {
            let Some(block) = chain.pop_front() else {
                return Ok(());
            };
            if block.lock().has_attestations_target_met() {
                continue;
            }
            let (has_initial_attestations_target_met, initial_attestations_target) =
                block.guarded(|e| {
                    (*e.has_initial_attestations_target_met(), *e.initial_attestations_target())
                });
            if initial_attestations_target.is_none() {
                return Ok(());
            }
            if has_initial_attestations_target_met.is_none() {
                self.evaluate_initial_attestations_target(
                    block.clone(),
                    &chain,
                    initial_attestations_target.unwrap(),
                )?;
            }
        }
    }

    fn evaluate_initial_attestations_target(
        &mut self,
        block: BlockState,
        descendants_chain: &VecDeque<BlockState>,
        (descendants_chain_length_required, min_attestations_count_required): (
            DescendantsChainLength,
            AttestationsCount,
        ),
    ) -> anyhow::Result<()> {
        assert!(descendants_chain_length_required > 0);
        if descendants_chain.len() < descendants_chain_length_required {
            return Ok(());
        }
        let initial_target = descendants_chain.get(descendants_chain_length_required - 1).unwrap();
        let Some(fork_resolutions) = initial_target.guarded(|e| e.resolves_forks().clone()) else {
            // Must be set before proceeding
            return Ok(());
        };

        let block_id = block.lock().block_identifier().clone();
        let mut block_attestations_signers = HashSet::<SignerIndex>::new();
        for i in 0..descendants_chain_length_required {
            let Some(attestations_signers) = descendants_chain
                .get(i)
                .unwrap()
                .guarded(|e| e.verified_attestations_for(&block_id))
            else {
                // Attestations for the block was not verified yet.
                // Can't proceed.
                return Ok(());
            };
            block_attestations_signers.extend(&attestations_signers);
        }
        // We had all the required information to check if target was met or not.
        let is_target_met = block_attestations_signers.len() >= min_attestations_count_required;

        if is_target_met {
            let (block_seq_no, thread_id) =
                block.guarded(|e| (*e.block_seq_no(), *e.thread_identifier()));
            tracing::trace!("attestation target met for block_id: {:?}, block_seq_no: {}, thread_id: {:?}, attestation_signers: {:?}", block_id, block_seq_no.unwrap(), thread_id.unwrap(), block_attestations_signers);
            block.guarded_mut(|e| -> anyhow::Result<()> {
                e.set_has_initial_attestations_target_met()
            })?;
            return Ok(());
        }
        if fork_resolutions.iter().any(|e| *e.winner() == block_id) {
            // Fork resolution scenario
            if initial_target.guarded(|e| e.has_attestations_target_met()) {
                block.guarded_mut(|e| -> anyhow::Result<()> {
                    e.set_has_attestations_target_met_in_a_resolved_fork_case()
                })?;
            }
            return Ok(());
        }
        initial_target.guarded_mut(|e| {
            tracing::trace!(
                "evaluate_initial_attestations_target set invalidated {:?}",
                e.block_identifier()
            );
            e.set_invalidated()
        })?;
        Ok(())
    }

    // Note: this may change with more information added in the next blocks
    pub fn find_next_block_known_dependants(
        &self,
        parent_block_identifier: BlockIdentifier,
    ) -> anyhow::Result<Vec<BlockIdentifier>> {
        let mut chain = self.prepare_chain(parent_block_identifier)?;
        let mut result = vec![];
        while !chain.is_empty() {
            let cursor = chain.remove(0);
            let ((required_chain_length, _count), block_identifier) = cursor.guarded(|e| {
                anyhow::ensure!(e.initial_attestations_target().is_some());
                Ok((e.initial_attestations_target().unwrap(), e.block_identifier().clone()))
            })?;
            if required_chain_length != chain.len() + 1 {
                continue;
            }
            result.push(block_identifier);
        }
        Ok(result)
    }

    pub fn evaluate_if_next_block_ancestors_required_attestations_will_be_met(
        &self,
        parent_block_identifier: BlockIdentifier,
        next_block_attestations: HashMap<BlockIdentifier, HashSet<SignerIndex>>,
        fork_resolutions: &[ForkResolution],
    ) -> anyhow::Result<bool> {
        tracing::trace!("evaluate_if_next_block_ancestors_required_attestations_will_be_met: parent_block_identifier: {parent_block_identifier:?}, next_block_attestations: {next_block_attestations:?}");
        let mut chain = self.prepare_chain(parent_block_identifier)?;
        tracing::trace!(
            "evaluate_if_next_block_ancestors_required_attestations_will_be_met: {chain:?}"
        );
        while !chain.is_empty() {
            let cursor = chain.remove(0);
            let ((required_chain_length, count), block_identifier) = cursor.guarded(|e| {
                anyhow::ensure!(e.initial_attestations_target().is_some());
                Ok((e.initial_attestations_target().unwrap(), e.block_identifier().clone()))
            })?;
            if required_chain_length != chain.len() + 1 {
                continue;
            }
            if fork_resolutions.iter().any(|e| *e.winner() == block_identifier) {
                continue;
            }
            let mut signers: HashSet<SignerIndex> =
                next_block_attestations.get(&block_identifier).cloned().unwrap_or_default();
            for descendant in chain.iter() {
                descendant.guarded(|e| {
                    if let Some(signatures) = e.verified_attestations_for(&block_identifier) {
                        signers.extend(signatures);
                    }
                });
            }
            if count > signers.len() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // Creates a chain starting from the first non-finalized block to the tail (inclusive)
    fn prepare_chain(&self, tail: BlockIdentifier) -> anyhow::Result<Vec<BlockState>> {
        let parent_state = self.blocks_states.get(&tail)?;
        let (is_parent_finalized, thread_id) = parent_state.guarded(|e| {
            anyhow::ensure!(!e.is_invalidated());
            anyhow::ensure!(e.thread_identifier().is_some());
            Ok((e.is_finalized(), e.thread_identifier().unwrap()))
        })?;
        if is_parent_finalized {
            return Ok(vec![]);
        }
        let (_, thread_last_finalized_block_seq_no) =
            self.repository.select_thread_last_finalized_block(&thread_id)?;
        let mut chain = vec![];
        let mut cursor = parent_state;
        loop {
            let (is_finalized, parent_id, cursor_seq_no) = cursor.guarded(|e| {
                anyhow::ensure!(!e.is_invalidated());
                if e.is_finalized() {
                    Ok((true, None, None))
                } else {
                    anyhow::ensure!(e.parent_block_identifier().is_some());
                    anyhow::ensure!(e.block_seq_no().is_some());
                    Ok((false, e.parent_block_identifier().clone(), *e.block_seq_no()))
                }
            })?;
            if is_finalized {
                chain.reverse();
                return Ok(chain);
            }
            chain.push(cursor);
            let parent_id = parent_id.unwrap();
            let cursor_seq_no = cursor_seq_no.unwrap();
            anyhow::ensure!(cursor_seq_no > thread_last_finalized_block_seq_no);
            cursor = self.blocks_states.get(&parent_id)?;
        }
    }
}
