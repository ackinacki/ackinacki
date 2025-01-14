// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

use derive_setters::*;
use serde::Deserialize;
use serde::Serialize;

use super::private::save;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

pub type AttestationsCount = usize;
pub type DescendantsChainLength = usize;

// The main point of this structure is to collect signals from
// various places in the system.
// All signals are intentionally made in a write-once pattern,
// this way any rule applied to a block can not be un-ruled (and should not).
// Assume this is a table of facts. Once fact is discovered it is written into the table.
//
// Note:
// All fields related to the block itself
// are prefixed with is_*
// All fields related to other blocks states
// are prefixed with has_*
// All signals coming from other nodes
// are stored in fields prexied with was_*
// It is important:
// All fields must be able to set once and only once.
#[derive(Serialize, Deserialize, Default, Clone, Debug, Setters)]
#[setters(strip_option, into, borrow_self, prefix = "set_")]
#[setters(postprocess = "save(self)", result = "anyhow::Result<()>")]
pub struct AckiNackiBlockState {
    block_identifier: BlockIdentifier,

    thread_identifier: Option<ThreadIdentifier>,
    parent_block_identifier: Option<BlockIdentifier>,
    block_seq_no: Option<BlockSeqNo>,

    // Flag indicates it has signatures checked.
    #[setters(bool)]
    signatures_verified: Option<bool>,

    // Flag indicates that block was correctly applied to the parent block state.
    #[setters(bool)]
    applied: Option<bool>,

    // Flag indicates that block was validated.
    #[setters(bool)]
    validated: Option<bool>,

    // This indicated that block has been validated by the validation process
    // is_validated: Option<bool>,
    #[setters(bool)]
    finalized: Option<bool>,

    // Note:
    // is_invalidated and is_validated ARE NOTE mutually exclusive.
    // For example, it is possible to have a valid block invalidated
    // due to a fork condition.
    #[setters(bool)]
    invalidated: Option<bool>,

    #[setters(skip)]
    nacks_count: u64,

    #[setters(skip)]
    resolved_nacks_count: u64,

    // has_parent_optimistic_state: Option<bool>,
    #[setters(bool)]
    has_parent_finalized: Option<bool>,

    #[setters(bool)]
    has_all_cross_thread_ref_data_available: Option<bool>,

    #[setters(bool)]
    has_all_cross_thread_references_finalized: Option<bool>,

    // Flag indicates that cross thread ref data for this block was prepared and saved
    #[setters(bool)]
    has_cross_thread_ref_data_prepared: Option<bool>,

    // has_attestations_target_met: Option<bool>,

    // Calculated baseline for finalization. Must be calculated based on prev
    // history.
    initial_attestations_target: Option<(DescendantsChainLength, AttestationsCount)>,

    #[serde(skip)]
    #[setters(skip)]
    pub(super) file_path: PathBuf,
}

impl AckiNackiBlockState {
    pub fn new(block_identifier: BlockIdentifier) -> Self {
        Self {
            block_identifier,
            // Note: now we ref only finalized blocks from other threads
            has_all_cross_thread_references_finalized: Some(true),
            ..Default::default()
        }
    }

    pub fn block_identifier(&self) -> &BlockIdentifier {
        &self.block_identifier
    }

    pub fn parent_block_identifier(&self) -> &Option<BlockIdentifier> {
        &self.parent_block_identifier
    }

    pub fn thread_identifier(&self) -> &Option<ThreadIdentifier> {
        &self.thread_identifier
    }

    pub fn can_finalize(&self) -> bool {
        tracing::trace!("BlockState::can_finalize: {self:?}");
        self.is_signatures_verified()
        && !self.is_invalidated()
        && !self.has_unresolved_nacks()
        && self.is_block_already_applied()
        && self.has_parent_finalized == Some(true)
        && self.has_all_cross_thread_references_finalized == Some(true)
        && self.has_all_cross_thread_ref_data_available == Some(true)
        // && self.has_attestations_target_met == Some(true)
        && self.has_cross_thread_ref_data_prepared == Some(true)
        && !self.is_finalized()
    }

    pub fn has_unresolved_nacks(&self) -> bool {
        self.nacks_count > self.resolved_nacks_count
    }

    pub fn is_invalidated(&self) -> bool {
        self.invalidated == Some(true)
    }

    pub fn is_signatures_verified(&self) -> bool {
        self.signatures_verified == Some(true)
    }

    pub fn is_finalized(&self) -> bool {
        self.finalized == Some(true)
    }

    pub fn initial_attestations_target(
        &self,
    ) -> &Option<(DescendantsChainLength, AttestationsCount)> {
        &self.initial_attestations_target
    }

    // Function checks whether block was successfully applied or validated
    pub fn is_block_already_applied(&self) -> bool {
        self.validated == Some(true) || self.applied == Some(true)
    }

    pub fn add_suspicious(&mut self) -> anyhow::Result<()> {
        self.nacks_count += 1;
        save(self)
    }

    pub fn resolve_suspicious(&mut self) -> anyhow::Result<()> {
        self.resolved_nacks_count += 1;
        save(self)
    }
}
