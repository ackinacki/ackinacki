use std::collections::HashMap;
use std::collections::HashSet;

use derive_getters::Getters;
use serde::Deserialize;
use serde::Serialize;
use typed_builder::TypedBuilder;

use crate::node::associated_types::AttestationTargetType;
use crate::node::BlockState;
use crate::types::BlockIdentifier;
use crate::utilities::guarded::Guarded;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Copy, Getters, TypedBuilder)]
pub struct AttestationTargetCheckpoint {
    current_distance: usize,
    deadline: usize,
    required_attestation_count: usize,
    attestation_target_type: AttestationTargetType,
}

pub enum AttestationTargetCheckpointCheckResult {
    CheckpointFailed,
    CheckpointPassed,
    NoAction,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Getters, TypedBuilder)]
pub struct AncestorBlocksFinalizationCheckpoints {
    primary: HashMap<BlockIdentifier, AttestationTargetCheckpoint>,
    fallback: HashMap<BlockIdentifier, Vec<AttestationTargetCheckpoint>>,
}

pub struct InheritedAncestorBlocksFinalizationCheckpoints(AncestorBlocksFinalizationCheckpoints);

#[derive(TypedBuilder)]
pub struct AncestorBlocksFinalizationCheckpointsConstructor {
    inherited_checkpoints: AncestorBlocksFinalizationCheckpoints,
    passed_primary: Vec<BlockIdentifier>,
    passed_fallback: Vec<BlockIdentifier>,
    passed_fallback_preattestation_checkpoint: Vec<BlockIdentifier>,
}

#[derive(Debug)]
pub struct AncestorBlocksFinalizationCheckpointsConstructorResults {
    pub remaining_checkpoints: AncestorBlocksFinalizationCheckpoints,
    pub passed_primary: Vec<BlockIdentifier>,
    pub passed_fallback: Vec<BlockIdentifier>,
    pub passed_fallback_preattestation_checkpoint: Vec<BlockIdentifier>,
    pub transitioned_to_fallback: Vec<BlockIdentifier>,
    pub failed: Vec<BlockIdentifier>,
}

pub fn inherit_checkpoint(
    mut checkpoint: AttestationTargetCheckpoint,
) -> AttestationTargetCheckpoint {
    checkpoint.current_distance += 1;
    checkpoint
}

impl InheritedAncestorBlocksFinalizationCheckpoints {
    pub fn into_builder(self) -> AncestorBlocksFinalizationCheckpointsConstructor {
        AncestorBlocksFinalizationCheckpointsConstructor {
            inherited_checkpoints: self.0,
            passed_primary: vec![],
            passed_fallback: vec![],
            passed_fallback_preattestation_checkpoint: vec![],
        }
    }
}

impl AttestationTargetCheckpoint {
    pub fn check(
        &self,
        attestation_target_type: &AttestationTargetType,
        verified_signatures_count: usize,
    ) -> AttestationTargetCheckpointCheckResult {
        if self.current_distance > self.deadline {
            return AttestationTargetCheckpointCheckResult::CheckpointFailed;
        }
        if &self.attestation_target_type != attestation_target_type {
            return AttestationTargetCheckpointCheckResult::NoAction;
        }
        if verified_signatures_count >= self.required_attestation_count {
            return AttestationTargetCheckpointCheckResult::CheckpointPassed;
        }
        AttestationTargetCheckpointCheckResult::NoAction
    }
}

impl AncestorBlocksFinalizationCheckpointsConstructor {
    pub fn update(
        mut self,
        attestation_count: HashMap<(BlockIdentifier, AttestationTargetType), usize>,
    ) -> AncestorBlocksFinalizationCheckpointsConstructorResults {
        for ((block_id, attestation_target_type), count) in attestation_count.into_iter() {
            self.note(block_id, attestation_target_type, count);
        }
        self.complete()
    }

    fn complete(mut self) -> AncestorBlocksFinalizationCheckpointsConstructorResults {
        let mut transitioned_to_fallback: HashSet<BlockIdentifier> = HashSet::new();
        let failed_primary = self
            .inherited_checkpoints
            .primary
            .iter()
            .filter_map(|(block_id, checkpoint)| {
                // if checkpoint is still in the list and it reached the deadline
                // it has failed on the primary target.
                if checkpoint.current_distance >= checkpoint.deadline {
                    // if can still be ok if there's a fallback exists for the block id
                    if !self.inherited_checkpoints.fallback.contains_key(block_id) {
                        return Some(block_id.clone());
                    } else {
                        transitioned_to_fallback.insert(block_id.clone());
                    }
                }
                None
            })
            .collect::<Vec<BlockIdentifier>>();
        for block_id in transitioned_to_fallback.iter() {
            self.inherited_checkpoints.primary.remove(block_id);
        }
        let failed_fallback = self
            .inherited_checkpoints
            .fallback
            .iter()
            .filter_map(|(block_id, checkpoints)| {
                if checkpoints.iter().any(|e| e.current_distance >= e.deadline) {
                    return Some(block_id.clone());
                }
                None
            })
            .collect::<Vec<BlockIdentifier>>();
        let failed = HashSet::<BlockIdentifier>::from_iter(
            failed_primary.into_iter().chain(failed_fallback),
        );
        for block_id in failed.iter() {
            self.inherited_checkpoints.primary.remove(block_id);
            self.inherited_checkpoints.fallback.remove(block_id);
        }

        AncestorBlocksFinalizationCheckpointsConstructorResults {
            remaining_checkpoints: self.inherited_checkpoints,
            failed: failed.into_iter().collect(),
            passed_primary: self.passed_primary,
            passed_fallback: self.passed_fallback,
            passed_fallback_preattestation_checkpoint: self
                .passed_fallback_preattestation_checkpoint,
            transitioned_to_fallback: transitioned_to_fallback.into_iter().collect(),
        }
    }

    fn note(
        &mut self,
        attested_block: BlockIdentifier,
        attestation_target_type: AttestationTargetType,
        verified_signatures_count: usize,
    ) {
        use AttestationTargetCheckpointCheckResult::*;
        if let Some(primary_target_checkpoint) =
            self.inherited_checkpoints.primary().get(&attested_block)
        {
            match primary_target_checkpoint
                .check(&attestation_target_type, verified_signatures_count)
            {
                NoAction => {}
                CheckpointPassed => {
                    self.inherited_checkpoints.fallback.remove(&attested_block);
                    self.inherited_checkpoints.primary.remove(&attested_block);
                    self.passed_primary.push(attested_block.clone());
                }
                CheckpointFailed => {
                    // Do nothing. All failed results will be collected in the method <complete>.
                }
            }
        }
        if let Some(fallback_checkpoints) =
            self.inherited_checkpoints.fallback.get_mut(&attested_block)
        {
            const RETAIN: bool = true;
            const REMOVE: bool = false;
            fallback_checkpoints.retain(|checkpoint| {
                match checkpoint.check(&attestation_target_type, verified_signatures_count) {
                    NoAction => RETAIN,
                    CheckpointPassed => {
                        if attestation_target_type == AttestationTargetType::Primary {
                            self.passed_fallback_preattestation_checkpoint
                                .push(attested_block.clone());
                        }
                        REMOVE
                    }
                    // Do nothing. All failed results will be collected in the method <complete>.
                    CheckpointFailed => RETAIN,
                }
            });
            if fallback_checkpoints.is_empty() {
                self.inherited_checkpoints.fallback.remove(&attested_block);
                self.inherited_checkpoints.primary.remove(&attested_block);
                self.passed_fallback.push(attested_block.clone());
            }
        }
    }
}

pub fn inherit_ancestor_blocks_finalization_distances(
    parent_block_state: &BlockState,
    block_state: &BlockState,
) -> anyhow::Result<InheritedAncestorBlocksFinalizationCheckpoints> {
    let Some(parent_block_thread_identifier) =
        parent_block_state.guarded(|e| *e.thread_identifier())
    else {
        anyhow::bail!("parent block is not ready");
    };

    let Some(mut ancestor_blocks_finalization_distances_prototype) =
        parent_block_state.guarded(|e| e.ancestor_blocks_finalization_checkpoints().clone())
    else {
        anyhow::bail!("parent block is not ready");
    };

    let Some(thread_identifier) = block_state.guarded(|e| *e.thread_identifier()) else {
        anyhow::bail!("block is not ready: thread id is not set");
    };
    let Some(attestation_target) = block_state.guarded(|e| *e.attestation_target()) else {
        anyhow::bail!("block is no ready: attestation target is not set");
    };

    let mut next = if thread_identifier == parent_block_thread_identifier {
        for checkpoint in ancestor_blocks_finalization_distances_prototype.primary.values_mut() {
            *checkpoint = inherit_checkpoint(*checkpoint);
        }
        for checkpoint in ancestor_blocks_finalization_distances_prototype.fallback.values_mut() {
            *checkpoint = checkpoint.iter().map(|e| inherit_checkpoint(*e)).collect();
        }

        ancestor_blocks_finalization_distances_prototype
    } else {
        AncestorBlocksFinalizationCheckpoints { primary: HashMap::new(), fallback: HashMap::new() }
    };

    // Insert self
    next.primary.insert(
        block_state.block_identifier().clone(),
        AttestationTargetCheckpoint::builder()
            .current_distance(0)
            .deadline(*attestation_target.primary().generation_deadline())
            .required_attestation_count(*attestation_target.primary().required_attestation_count())
            .attestation_target_type(AttestationTargetType::Primary)
            .build(),
    );

    next.fallback.insert(
        block_state.block_identifier().clone(),
        vec![
            AttestationTargetCheckpoint::builder()
                .current_distance(0)
                .deadline(*attestation_target.primary().generation_deadline())
                .required_attestation_count(
                    *attestation_target.fallback().required_attestation_count(),
                )
                .attestation_target_type(AttestationTargetType::Primary)
                .build(),
            AttestationTargetCheckpoint::builder()
                .current_distance(0)
                .deadline(*attestation_target.fallback().generation_deadline())
                .required_attestation_count(
                    *attestation_target.fallback().required_attestation_count(),
                )
                .attestation_target_type(AttestationTargetType::Fallback)
                .build(),
        ],
    );

    Ok(InheritedAncestorBlocksFinalizationCheckpoints(next))
}
