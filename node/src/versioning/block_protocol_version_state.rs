use serde::Deserialize;
use serde::Serialize;

use super::ProtocolVersion;
use super::ProtocolVersionSupport;
use crate::types::BlockIdentifier;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum BlockProtocolVersionState {
    Current(ProtocolVersion),
    TransitionLocked { from: ProtocolVersion, to: ProtocolVersion, milestone: BlockIdentifier },
    CompleteTransition { from: ProtocolVersion, to: ProtocolVersion },
}

impl BlockProtocolVersionState {
    pub fn to_use(&self) -> &ProtocolVersion {
        match self {
            BlockProtocolVersionState::Current(e) => e,
            BlockProtocolVersionState::TransitionLocked { from, .. } => from,
            BlockProtocolVersionState::CompleteTransition { from, .. } => from,
        }
    }

    pub fn next(
        &self,
        current_block_id: BlockIdentifier,
        all_nodes_in_descendant_bk_set_have_the_same_version: Option<ProtocolVersionSupport>,
        ancestor_blocks_that_passed_checkpoints: &[BlockIdentifier],
    ) -> Self {
        match self.clone() {
            BlockProtocolVersionState::Current(version) => {
                let Some(node_support) = all_nodes_in_descendant_bk_set_have_the_same_version
                else {
                    return BlockProtocolVersionState::Current(version);
                };
                let Some(next_version) = node_support.target() else {
                    return BlockProtocolVersionState::Current(version);
                };
                if &version != node_support.base() || next_version == &version {
                    return BlockProtocolVersionState::Current(version);
                }
                BlockProtocolVersionState::TransitionLocked {
                    from: version,
                    to: next_version.clone(),
                    milestone: current_block_id,
                }
            }
            BlockProtocolVersionState::TransitionLocked { from, to, milestone } => {
                if ancestor_blocks_that_passed_checkpoints.contains(&milestone) {
                    BlockProtocolVersionState::CompleteTransition { from, to }
                } else {
                    self.clone()
                }
            }
            BlockProtocolVersionState::CompleteTransition { to, .. } => {
                BlockProtocolVersionState::Current(to)
            }
        }
    }

    pub fn none() -> Self {
        Self::Current(ProtocolVersion::parse("None").unwrap())
    }
}
