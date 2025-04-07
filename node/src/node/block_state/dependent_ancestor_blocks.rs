use derive_getters::Getters;
use typed_builder::TypedBuilder;

use super::repository::BlockStateRepository;
use crate::node::block_state::state::AttestationsTarget;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocksSelectError;
use crate::node::BlockState;
use crate::types::BlockIdentifier;
use crate::utilities::guarded::Guarded;

const MAX_ALLOWED_DESTINATION: usize = 100;

pub trait DependentAncestorBlocks {
    fn select_dependent_ancestor_blocks(
        &self,
        tail: &BlockState,
    ) -> anyhow::Result<DependentBlocks, UnfinalizedAncestorBlocksSelectError>;
}

#[derive(Debug, TypedBuilder, Getters, Clone)]
pub struct DependentBlocks {
    dependent_ancestor_blocks: Vec<BlockState>,
    dependent_ancestor_chain: Vec<BlockState>,
}

impl DependentAncestorBlocks for BlockStateRepository {
    fn select_dependent_ancestor_blocks(
        &self,
        tail_parent: &BlockState,
    ) -> anyhow::Result<DependentBlocks, UnfinalizedAncestorBlocksSelectError> {
        use UnfinalizedAncestorBlocksSelectError::*;
        let mut chain = vec![];
        let mut cursor = tail_parent.clone();
        let mut generation_distance = 1; // NOTE: start with 1, because we start from tail parent, which already has distance 1
        let mut dependent_ancestor_blocks = vec![];
        loop {
            if cursor.block_identifier() == &BlockIdentifier::default() {
                return Ok(DependentBlocks::builder()
                    .dependent_ancestor_chain(chain)
                    .dependent_ancestor_blocks(dependent_ancestor_blocks)
                    .build());
            }
            let (parent, attestation_target) = cursor.guarded(
                |e| -> Result<
                    (BlockIdentifier, AttestationsTarget),
                    UnfinalizedAncestorBlocksSelectError,
                > {
                    if e.is_invalidated() && !e.is_finalized() {
                        chain.push(cursor.clone());
                        return Err(InvalidatedParent(chain.clone()));
                    }
                    let attestation_target =
                        (*e.initial_attestations_target()).ok_or(IncompleteHistory)?;
                    let parent_block_identifier =
                        e.parent_block_identifier().clone().ok_or(IncompleteHistory)?;
                    Ok((parent_block_identifier, attestation_target))
                },
            )?;
            if attestation_target.descendant_generations < generation_distance {
                return Ok(DependentBlocks::builder()
                    .dependent_ancestor_chain(chain)
                    .dependent_ancestor_blocks(dependent_ancestor_blocks)
                    .build());
            }
            chain.push(cursor.clone());
            if attestation_target.descendant_generations == generation_distance {
                dependent_ancestor_blocks.push(cursor);
            }
            generation_distance += 1;
            if generation_distance >= MAX_ALLOWED_DESTINATION {
                return Err(BlockSeqNoCutoff(chain.clone()));
            }
            cursor = self.get(&parent).map_err(|_e| FailedToLoadBlockState)?;
        }
    }
}
