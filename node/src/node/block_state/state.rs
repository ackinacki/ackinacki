// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use derive_getters::Getters;
use derive_setters::*;
use serde::Deserialize;
use serde::Serialize;

use crate::block_keeper_system::BlockKeeperData;
use crate::block_keeper_system::BlockKeeperSet;
use crate::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BlockStatistics;
use crate::node::AttestationData;
use crate::node::Envelope;
use crate::node::GoshBLS;
use crate::node::NackData;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::types::bp_selector::ProducerSelector;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ForkResolution;
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
#[derive(Serialize, Deserialize, Default, Clone, Debug, Getters, Setters)]
#[setters(strip_option, into, borrow_self, assert_none, prefix = "set_", trace)]
#[setters(postprocess = "self.save()", result = "anyhow::Result<()>", no_change_action = "Ok(())")]
pub struct AckiNackiBlockState {
    #[setters(skip)]
    block_identifier: BlockIdentifier,

    thread_identifier: Option<ThreadIdentifier>,
    parent_block_identifier: Option<BlockIdentifier>,
    block_seq_no: Option<BlockSeqNo>,

    producer: Option<NodeIdentifier>,

    // Flag indicates it has signatures checked.
    #[setters(bool)]
    signatures_verified: Option<bool>,

    envelope_block_producer_signature_verified: Option<bool>,

    // Flag indicates that block was correctly applied to the parent block state.
    #[setters(bool)]
    applied: Option<bool>,

    // Flag indicates that block was validated and validation result.
    validated: Option<bool>,

    resolves_forks: Option<Vec<ForkResolution>>,

    // When this flag is set:
    // - On initial target set. This block can't change the history,
    //   so this immutable.
    // - On the target block when fork is resolved in favour of this block.
    //   Similar to the previous case, it is immutable.
    #[setters(bool, assert_none = false)]
    must_be_validated: Option<bool>,

    // This indicated that block has been validated by the validation process
    // is_validated: Option<bool>,
    #[setters(bool)]
    finalized: Option<bool>,

    // Flag indicated that block has been already received and stored in repo
    #[setters(bool)]
    stored: Option<bool>,

    // Note:
    // is_invalidated and is_validated ARE NOTE mutually exclusive.
    // For example, it is possible to have a valid block invalidated
    // due to a fork condition.
    #[setters(bool, assert_none = false)]
    invalidated: Option<bool>,

    #[setters(skip)]
    nacks_count: u64,

    #[setters(skip)]
    resolved_nacks_count: u64,

    // BlockKeeper set for this block it's taken from parent block without any BK set changes
    // happened in this block
    bk_set: Option<Arc<BlockKeeperSet>>,

    // BlockKeeper set for descendant blocks with all BK set changes happened in this block
    descendant_bk_set: Option<Arc<BlockKeeperSet>>,

    // has_parent_optimistic_state: Option<bool>,

    // TODO: fix for not to set several times
    #[setters(bool, assert_none = false)]
    has_parent_finalized: Option<bool>,

    // Data to define Block Producer for descendant blocks
    producer_selector_data: Option<ProducerSelector>,

    #[setters(bool)]
    has_all_cross_thread_ref_data_available: Option<bool>,

    #[setters(bool)]
    has_all_cross_thread_references_finalized: Option<bool>,

    // Flag indicates that cross thread ref data for this block was prepared and saved
    // TODO: fix for  not to set several times
    #[setters(bool, assert_none = false)]
    has_cross_thread_ref_data_prepared: Option<bool>,

    // Flag indicated that there's at least one chain that has all required attestations
    // for this block. Overall it means that regardless of further forks this block
    // is accepted as ready to be finalized by the required number of block keepers.
    // This flag is for the basic scenario only.
    // Since it is not possible to set it to false since can't be certain that
    // there's no other chain that meets the initia attestation target.
    #[setters(bool, assert_none = false)]
    has_initial_attestations_target_met: Option<bool>,

    // This flag indicates that there was at least one chain that resolved a fork for
    // this block (this block is the winner) and the block that resolved this fork
    // is also in a shape that it can be finalized (his initial attestation target is met
    // or it has the same story with the chain)
    #[setters(bool, assert_none = false)]
    has_attestations_target_met_in_a_resolved_fork_case: Option<bool>,

    // Calculated baseline for finalization. Must be calculated based on prev
    // history.
    // The DescendantsChainLength is the exact descendant when it will be checked
    // if this chain has collected the required number of attestations.
    #[setters(postprocess = "self.assert_initial_attestations_target_and_save()")]
    initial_attestations_target: Option<(DescendantsChainLength, AttestationsCount)>,

    #[setters(skip)]
    verified_attestations: HashMap<BlockIdentifier, HashSet<SignerIndex>>,

    block_stats: Option<BlockStatistics>,

    #[setters(skip)]
    known_children: HashSet<BlockIdentifier>,

    // This must be set with a helper method.
    // It is intentionally made this way since it requires syncronization between other
    // potential children.
    #[setters(skip)]
    pub(super) attestation: Option<Envelope<GoshBLS, AttestationData>>,

    retracted_attestation: Option<Envelope<GoshBLS, NackData>>,

    // Nodes that had sent messages indicating their interest in getting attestations for the block.
    #[setters(skip)]
    known_attestation_interested_parties: HashSet<NodeIdentifier>,

    #[serde(skip)]
    #[getter(skip)]
    #[setters(skip)]
    pub(super) file_path: PathBuf,

    #[serde(skip)]
    #[getter(skip)]
    #[setters(skip)]
    pub(super) notifications: Arc<AtomicU32>,
}

impl std::fmt::Display for AckiNackiBlockState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "BlockState (block_id: {:?}, block_seq_no: {:?})",
            self.block_identifier, self.block_seq_no
        )
    }
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

    #[allow(clippy::nonminimal_bool)]
    pub fn has_attestations_target_met(&self) -> bool {
        self.has_initial_attestations_target_met == Some(true)
            || self.has_attestations_target_met_in_a_resolved_fork_case == Some(true)
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
            && self.has_attestations_target_met()
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

    pub fn is_stored(&self) -> bool {
        self.stored == Some(true)
    }

    // Function checks whether block was successfully applied or validated
    pub fn is_block_already_applied(&self) -> bool {
        self.applied == Some(true)
    }

    pub fn add_suspicious(&mut self) -> anyhow::Result<()> {
        self.nacks_count += 1;
        self.save()
    }

    pub fn resolve_suspicious(&mut self) -> anyhow::Result<()> {
        self.resolved_nacks_count += 1;
        self.save()
    }

    // Selects attestations sent on behalf of the given block id
    // from all attestations sent with this block.
    // Returns None if signatures were not yet verified.
    // Returns an empty result if no attestations for the given block  were sent with this block.
    pub fn verified_attestations_for(
        &self,
        block_identifier: &BlockIdentifier,
    ) -> Option<HashSet<SignerIndex>> {
        if self.is_signatures_verified() {
            self.verified_attestations.get(block_identifier).cloned().or(Some(HashSet::new()))
        } else {
            None
        }
    }

    // If the interested parties set did not previously contain this value, true is returned.
    // False in case of no new information were added.
    pub fn try_add_attestations_interest(
        &mut self,
        node_identifier: NodeIdentifier,
    ) -> anyhow::Result<bool> {
        tracing::trace!(
            "try_add_attestations_interest: {:?} {:?} {node_identifier:?}",
            self.block_seq_no,
            self.block_identifier
        );
        let result = if self.known_attestation_interested_parties.insert(node_identifier) {
            self.save()?;
            true
        } else {
            false
        };
        Ok(result)
    }

    pub fn add_verified_attestations_for(
        &mut self,
        block_identifier: BlockIdentifier,
        attestation_signers: HashSet<SignerIndex>,
    ) -> anyhow::Result<()> {
        let _prev = self.verified_attestations.insert(block_identifier, attestation_signers);
        // assert!(prev.is_none());
        self.save()
    }

    pub fn add_child(&mut self, child_block_identifier: BlockIdentifier) -> anyhow::Result<()> {
        tracing::trace!(
            "add child for {:?} child: {child_block_identifier:?}",
            self.block_identifier
        );
        self.known_children.insert(child_block_identifier);
        self.save()
    }

    pub fn get_signer_index_for_node_id(&self, node_id: &NodeIdentifier) -> Option<SignerIndex> {
        if let Some(bk_set) = &self.bk_set {
            bk_set.get_by_node_id(node_id).map(|x| x.signer_index)
        } else {
            None
        }
    }

    pub fn get_bk_data_for_node_id(&self, node_id: &NodeIdentifier) -> Option<BlockKeeperData> {
        self.bk_set.as_ref().and_then(|x| x.get_by_node_id(node_id)).cloned()
    }

    pub fn get_descendant_bk_set(&self) -> Arc<BlockKeeperSet> {
        if let Some(bk_set) = &self.descendant_bk_set {
            bk_set.clone()
        } else {
            assert!(self.bk_set.is_some());
            self.bk_set.clone().unwrap()
        }
    }

    // It is made pub super to allow helper methods to explicitly call it.
    pub(super) fn save(&self) -> anyhow::Result<()> {
        super::private::save(self)?;
        self.notifications.fetch_add(1, Ordering::Relaxed);
        atomic_wait::wake_all(self.notifications.as_ref());
        Ok(())
    }

    fn assert_initial_attestations_target_and_save(&self) -> anyhow::Result<()> {
        assert!(self.initial_attestations_target.unwrap().0 > 0);
        self.save()
    }
}
