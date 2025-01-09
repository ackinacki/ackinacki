// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use super::private::save;
use crate::types::BlockIdentifier;

#[allow(dead_code)]
type AttestationsCount = usize;

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
#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct AckiNackiBlockState {
    block_identifier: BlockIdentifier,

    // This indicates it has signatures checked.
    // TODO: check usage. At the current moment the usage is confusing.
    is_verified: Option<bool>,
    // This indicated that block has been validated by the validation process
    // is_validated: Option<bool>,
    is_finalized: Option<bool>,
    // Note:
    // is_invalidated and is_validated ARE NOTE mutually exclusive.
    // For example, it is possible to have a valid block invalidated
    // due to a fork condition.
    // is_invalidated: Option<bool>,
    nacks_count: u64,
    resolved_nacks_count: u64,
    // has_parent_optimistic_state: Option<bool>,
    // has_parent_finalized: Option<bool>,
    // has_all_chross_thread_ref_data_available: Option<bool>,
    // has_all_cross_thread_references_finalized: Option<bool>,

    // has_attestations_target_met: Option<bool>,

    // initial_attestations_target: Option<(BlockSeqNo, AttestationsCount)>,
    #[serde(skip)]
    pub(super) file_path: PathBuf,
}

impl AckiNackiBlockState {
    pub fn new(block_identifier: BlockIdentifier) -> Self {
        Self { block_identifier, ..Default::default() }
    }

    // pub fn can_finalize(&self) -> bool {
    // return self.is_verified()
    // && !self.is_invalidated()
    // && !self.has_unresolved_nacks()
    // && self.has_parent_finalized == Some(true)
    // && self.has_all_cross_thread_references_finalized == Some(true)
    // && self.has_all_chross_thread_ref_data_available == Some(true)
    // && self.has_attestations_target_met == Some(true)
    // && !self.is_finalized();
    // }

    pub fn has_unresolved_nacks(&self) -> bool {
        self.nacks_count > self.resolved_nacks_count
    }

    // pub fn is_invalidated(&self) -> bool {
    // self.is_invalidated == Some(true)
    // }
    //
    // pub fn is_validated(&self) -> bool {
    // self.is_validated == Some(true)
    // }
    pub fn is_verified(&self) -> bool {
        self.is_verified == Some(true)
    }

    pub fn is_finalized(&self) -> bool {
        self.is_finalized == Some(true)
    }

    pub fn add_suspicious(&mut self) -> anyhow::Result<()> {
        self.nacks_count += 1;
        save(self)
    }

    pub fn resolve_suspicious(&mut self) -> anyhow::Result<()> {
        self.resolved_nacks_count += 1;
        save(self)
    }

    pub fn set_finalized(&mut self) -> anyhow::Result<()> {
        self.is_finalized = Some(true);
        save(self)
    }

    // pub fn set_validated(&mut self) -> anyhow::Result<()> {
    // self.is_validated = Some(true);
    // save(self)
    // }
    pub fn set_verified(&mut self) -> anyhow::Result<()> {
        self.is_verified = Some(true);
        save(self)
    }
}
