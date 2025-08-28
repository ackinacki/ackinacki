// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;

use typed_builder::TypedBuilder;

use crate::node::associated_types::AttestationTargetType;
use crate::node::block_state::attestation_target_checkpoints::inherit_checkpoint;
use crate::node::block_state::attestation_target_checkpoints::AncestorBlocksFinalizationCheckpoints;
use crate::node::block_state::attestation_target_checkpoints::AncestorBlocksFinalizationCheckpointsConstructor;
use crate::node::block_state::unfinalized_ancestor_blocks::UnfinalizedAncestorBlocksSelectError;
use crate::node::BlockStateRepository;
use crate::node::SignerIndex;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;

#[derive(TypedBuilder, Clone)]
pub struct AttestationTargetsService {
    block_state_repository: BlockStateRepository,
}

pub enum AttestationsFailure {
    ChainIsTooShort,
    NotAllInitialAttestationTargetsSet,
    ThreadIdentifierIsNotSet,
    AttestationsAreNotVerifiedYet,
    FailedToSaveBlockState,
    #[allow(non_camel_case_types)]
    InvalidBlock_TailDoesNotMeetCriteria,
}

impl AttestationTargetsService {
    // TODO: expand errors set. Return actual errors instead of Ok(false)
    pub fn evaluate_if_next_block_ancestors_required_attestations_will_be_met(
        &self,
        thread_identifier: ThreadIdentifier,
        parent_block_identifier: BlockIdentifier,
        next_block_attestations: HashMap<
            (BlockIdentifier, AttestationTargetType),
            HashSet<SignerIndex>,
        >,
    ) -> anyhow::Result<bool, UnfinalizedAncestorBlocksSelectError> {
        tracing::trace!("evaluate_if_next_block_ancestors_required_attestations_will_be_met: parent_block_identifier: {parent_block_identifier:?}, next_block_attestations: {next_block_attestations:?}");
        let parent_block_state =
            self.block_state_repository.get(&parent_block_identifier).expect("It must not fail");
        tracing::trace!("evaluate_if_next_block_ancestors_required_attestations_will_be_met: parent state: {parent_block_state:?}");
        let Some(parent_block_thread_identifier) =
            parent_block_state.guarded(|e| *e.thread_identifier())
        else {
            // anyhow::bail!("parent block is not ready");
            return Err(UnfinalizedAncestorBlocksSelectError::FailedToLoadBlockState);
        };
        let Some(ancestor_blocks_finalization_distances_prototype) =
            parent_block_state.guarded(|e| e.ancestor_blocks_finalization_checkpoints().clone())
        else {
            // anyhow::bail!("parent block is not ready");
            return Err(UnfinalizedAncestorBlocksSelectError::FailedToLoadBlockState);
        };
        if thread_identifier != parent_block_thread_identifier {
            return Ok(true);
        }
        let mut ancestor_blocks_finalization_primary_checkpoints =
            ancestor_blocks_finalization_distances_prototype.primary().clone();
        let mut ancestor_blocks_finalization_fallback_checkpoints =
            ancestor_blocks_finalization_distances_prototype.fallback().clone();
        for checkpoint in ancestor_blocks_finalization_primary_checkpoints.values_mut() {
            *checkpoint = inherit_checkpoint(*checkpoint);
        }
        for checkpoint in ancestor_blocks_finalization_fallback_checkpoints.values_mut() {
            *checkpoint = checkpoint.iter().map(|e| inherit_checkpoint(*e)).collect();
        }
        let checkpoint_result = AncestorBlocksFinalizationCheckpointsConstructor::builder()
            .inherited_checkpoints(
                AncestorBlocksFinalizationCheckpoints::builder()
                    .primary(ancestor_blocks_finalization_primary_checkpoints)
                    .fallback(ancestor_blocks_finalization_fallback_checkpoints)
                    .build(),
            )
            .passed_primary(vec![])
            .passed_fallback(vec![])
            .passed_fallback_preattestation_checkpoint(vec![])
            .build()
            .update(next_block_attestations.into_iter().map(|(k, v)| (k, v.len())).collect());

        if !checkpoint_result.failed.is_empty() {
            tracing::trace!("evaluate_if_next_block_ancestors_required_attestations_will_be_met: failed full result: {checkpoint_result:?}");
        }
        Ok(checkpoint_result.failed.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use tracing_test::traced_test;

    use super::*;
    use crate::node::associated_types::AttestationTargetType;
    use crate::node::block_state::attestation_target_checkpoints::AttestationTargetCheckpoint;
    use crate::utilities::guarded::GuardedMut;

    #[traced_test]
    #[test]
    fn bug_evaluation_failed_jul_19_2025() {
        // Note: this is a repro from a log. Somehow it did fail in a load test.
        // This test confims that it was a side effect from something else.

        // Prepare test
        let thread_identifier = ThreadIdentifier::try_from(
            "00000000000000000000000000000000000000000000000000000000000000000000".to_owned(),
        )
        .unwrap();
        let parent_block_identifier = BlockIdentifier::from_str(
            "cf90c7b6f94c80595c6b43590b1d64cb1a55e98360f96ca6008acb551d1b0619",
        )
        .unwrap();
        let ancestor_block_identifier = BlockIdentifier::from_str(
            "abe5f32c13ca775f9c4458e5ac750f4637e3a0d4c15bdcde5c87094355a42e0f",
        )
        .unwrap();

        let tmp_dir = tempfile::tempdir().unwrap().path().to_owned();
        let block_state_repository = BlockStateRepository::test(tmp_dir);

        // Prepare checkpoints
        let mut next_block_attestations: HashMap<
            (BlockIdentifier, AttestationTargetType),
            HashSet<SignerIndex>,
        > = HashMap::new();
        next_block_attestations.insert(
            (parent_block_identifier.clone(), AttestationTargetType::Primary),
            HashSet::from_iter(vec![
                1, 9, 5, 14, 8, 13, 6, 10, 19, 16, 3, 7, 11, 17, 12, 15, 18, 4, 2,
            ]),
        );
        next_block_attestations.insert(
            (ancestor_block_identifier.clone(), AttestationTargetType::Primary),
            HashSet::from_iter(vec![
                6, 0, 14, 7, 1, 15, 9, 10, 19, 3, 18, 8, 12, 5, 17, 4, 11, 16, 2,
            ]),
        );
        let mut ancestor_blocks_finalization_primary_checkpoints = HashMap::new();
        // a69ac728fa4f2b797a1fde2a5b98c7c36281af4b73b616d0da8a8345b1502943
        ancestor_blocks_finalization_primary_checkpoints.insert(
            ancestor_block_identifier.clone(),
            AttestationTargetCheckpoint::builder()
                .current_distance(2)
                .deadline(3)
                .required_attestation_count(14)
                .attestation_target_type(AttestationTargetType::Primary)
                .build(),
        );

        // cf90c7b6f94c80595c6b43590b1d64cb1a55e98360f96ca6008acb551d1b0619
        ancestor_blocks_finalization_primary_checkpoints.insert(
            parent_block_identifier.clone(),
            AttestationTargetCheckpoint::builder()
                .current_distance(0)
                .deadline(3)
                .required_attestation_count(14)
                .attestation_target_type(AttestationTargetType::Primary)
                .build(),
        );

        let mut ancestor_blocks_finalization_fallback_checkpoints = HashMap::new();
        ancestor_blocks_finalization_fallback_checkpoints.insert(
            // cf90c7b6f94c80595c6b43590b1d64cb1a55e98360f96ca6008acb551d1b0619
            parent_block_identifier.clone(),
            vec![
                AttestationTargetCheckpoint::builder()
                    .current_distance(0)
                    .deadline(3)
                    .required_attestation_count(11)
                    .attestation_target_type(AttestationTargetType::Primary)
                    .build(),
                AttestationTargetCheckpoint::builder()
                    .current_distance(0)
                    .deadline(7)
                    .required_attestation_count(11)
                    .attestation_target_type(AttestationTargetType::Fallback)
                    .build(),
            ],
        );
        ancestor_blocks_finalization_fallback_checkpoints.insert(
            // a69ac728fa4f2b797a1fde2a5b98c7c36281af4b73b616d0da8a8345b1502943
            ancestor_block_identifier,
            vec![
                AttestationTargetCheckpoint::builder()
                    .current_distance(2)
                    .deadline(3)
                    .required_attestation_count(11)
                    .attestation_target_type(AttestationTargetType::Primary)
                    .build(),
                AttestationTargetCheckpoint::builder()
                    .current_distance(2)
                    .deadline(7)
                    .required_attestation_count(11)
                    .attestation_target_type(AttestationTargetType::Fallback)
                    .build(),
            ],
        );
        {
            let parent_block_state = block_state_repository.get(&parent_block_identifier).unwrap();

            parent_block_state
                .guarded_mut(|e| {
                    e.set_thread_identifier(thread_identifier)?;
                    e.set_ancestor_blocks_finalization_checkpoints(
                        AncestorBlocksFinalizationCheckpoints::builder()
                            .primary(ancestor_blocks_finalization_primary_checkpoints)
                            .fallback(ancestor_blocks_finalization_fallback_checkpoints)
                            .build(),
                    )
                })
                .unwrap();
        }

        let service = AttestationTargetsService::builder()
            .block_state_repository(block_state_repository)
            .build();

        let actual_result = service
            .evaluate_if_next_block_ancestors_required_attestations_will_be_met(
                thread_identifier,
                parent_block_identifier,
                next_block_attestations,
            );
        assert!(actual_result.is_ok());
        let actual_result = actual_result.unwrap();
        assert!(actual_result);
    }

    #[test]
    fn bug_evaluation_failed_jul_18_2025() {
        // Note: this is a repro from a log. Somehow it did fail in a load test.
        // This test confims that it was a side effect from something else.

        // Prepare test
        let thread_identifier = ThreadIdentifier::try_from(
            "0000eac840d2c8b877128b16f2cac07cc58be00e67567f7926da716a9868db570283".to_owned(),
        )
        .unwrap();
        let parent_block_identifier = BlockIdentifier::from_str(
            "7058438073b47201a18aa556b095cc568206d723d7511dad119998c29cf4fb08",
        )
        .unwrap();
        let ancestor_block_identifier = BlockIdentifier::from_str(
            "bb38d25c01ef8878b4f7585ff618930f9a7263d33ba054ffcc37ab77ef475925",
        )
        .unwrap();

        let tmp_dir = tempfile::tempdir().unwrap().path().to_owned();
        let block_state_repository = BlockStateRepository::test(tmp_dir);

        // Prepare checkpoints
        let mut next_block_attestations: HashMap<
            (BlockIdentifier, AttestationTargetType),
            HashSet<SignerIndex>,
        > = HashMap::new();
        next_block_attestations.insert(
            (parent_block_identifier.clone(), AttestationTargetType::Primary),
            HashSet::from_iter(vec![
                17, 19, 9, 2, 8, 10, 3, 15, 5, 16, 18, 6, 13, 11, 14, 7, 0, 1, 12, 4,
            ]),
        );
        next_block_attestations.insert(
            (ancestor_block_identifier.clone(), AttestationTargetType::Primary),
            HashSet::from_iter(vec![
                9, 1, 16, 17, 18, 8, 3, 2, 6, 11, 13, 7, 5, 19, 0, 14, 4, 10, 12, 15,
            ]),
        );
        next_block_attestations.insert(
            (ancestor_block_identifier.clone(), AttestationTargetType::Fallback),
            HashSet::from_iter(vec![
                0, 19, 1, 11, 14, 2, 9, 4, 8, 16, 10, 17, 18, 13, 5, 15, 12, 6, 3, 7,
            ]),
        );

        let mut ancestor_blocks_finalization_primary_checkpoints = HashMap::new();
        ancestor_blocks_finalization_primary_checkpoints.insert(
            parent_block_identifier.clone(),
            AttestationTargetCheckpoint::builder()
                .current_distance(0)
                .deadline(1)
                .required_attestation_count(14)
                .attestation_target_type(AttestationTargetType::Primary)
                .build(),
        );

        let mut ancestor_blocks_finalization_fallback_checkpoints = HashMap::new();
        ancestor_blocks_finalization_fallback_checkpoints.insert(
            parent_block_identifier.clone(),
            vec![AttestationTargetCheckpoint::builder()
                .current_distance(2)
                .deadline(3)
                .required_attestation_count(11)
                .attestation_target_type(AttestationTargetType::Fallback)
                .build()],
        );
        ancestor_blocks_finalization_fallback_checkpoints.insert(
            ancestor_block_identifier,
            vec![AttestationTargetCheckpoint::builder()
                .current_distance(1)
                .deadline(3)
                .required_attestation_count(11)
                .attestation_target_type(AttestationTargetType::Fallback)
                .build()],
        );
        {
            let parent_block_state = block_state_repository.get(&parent_block_identifier).unwrap();

            parent_block_state
                .guarded_mut(|e| {
                    e.set_thread_identifier(thread_identifier)?;
                    e.set_ancestor_blocks_finalization_checkpoints(
                        AncestorBlocksFinalizationCheckpoints::builder()
                            .primary(ancestor_blocks_finalization_primary_checkpoints)
                            .fallback(ancestor_blocks_finalization_fallback_checkpoints)
                            .build(),
                    )
                })
                .unwrap();
            drop(parent_block_state);
        }

        let service = AttestationTargetsService::builder()
            .block_state_repository(block_state_repository)
            .build();

        let actual_result = service
            .evaluate_if_next_block_ancestors_required_attestations_will_be_met(
                thread_identifier,
                parent_block_identifier,
                next_block_attestations,
            );
        assert!(actual_result.is_ok());
        let actual_result = actual_result.unwrap();
        assert!(actual_result);
    }
}
