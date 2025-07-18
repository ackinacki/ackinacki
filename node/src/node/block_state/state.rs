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
use typed_builder::TypedBuilder;

use crate::block_keeper_system::BlockKeeperData;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::BLSSignatureScheme;
use crate::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BlockStatistics;
use crate::node::AttestationData;
use crate::node::Envelope;
use crate::node::GoshBLS;
use crate::node::NackData;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::types::bp_selector::ProducerSelector;
use crate::types::envelope_hash::AckiNackiEnvelopeHash;
use crate::types::AckiNackiBlock;
use crate::types::BlockHeight;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::AllowGuardedMut;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Copy, Getters, TypedBuilder)]
pub struct AttestationTarget {
    generation_deadline: usize, // = beta + 1, Check if it is useful
    required_attestation_count: usize,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Copy, Getters, TypedBuilder)]
pub struct AttestationTargets {
    primary: AttestationTarget,
    fallback: AttestationTarget,
}

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
#[derive(Serialize, Deserialize, Default, Clone, Getters, Setters, Debug)]
#[setters(strip_option, borrow_self, assert_none, prefix = "set_", trace)]
#[setters(postprocess = "self.save()", result = "anyhow::Result<()>", no_change_action = "Ok(())")]
pub struct AckiNackiBlockState {
    #[setters(skip)]
    #[getter(skip)]
    bulk_change: bool,

    #[setters(skip)]
    #[getter(skip)]
    save_requested: bool,

    #[setters(skip)]
    block_identifier: BlockIdentifier,

    thread_identifier: Option<ThreadIdentifier>,
    parent_block_identifier: Option<BlockIdentifier>,
    block_seq_no: Option<BlockSeqNo>,

    // Lists authority switch rounds this block was proposed in.
    #[setters(skip)]
    proposed_in_round: HashSet<u16>,
    block_height: Option<BlockHeight>,
    producer: Option<NodeIdentifier>,
    block_time_ms: Option<u64>,

    prefinalization_proof: Option<Envelope<GoshBLS, AttestationData>>,

    // Flag indicates it has signatures checked.
    #[setters(bool)]
    signatures_verified: Option<bool>,

    // Flag indicates that block time, seq_no and hash were checked
    #[setters(bool)]
    common_checks_passed: Option<bool>,

    envelope_block_producer_signature_verified: Option<bool>,

    // Flag indicates that block was correctly applied to the parent block state.
    #[setters(bool)]
    applied: Option<bool>,

    // Flag indicates that block was validated and validation result.
    validated: Option<bool>,

    #[setters(skip)]
    finalizes_blocks: Option<HashSet<BlockIdentifier>>,

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

    // Prefinalization flag, that means that block has more than a half of bk set attestations.
    // Can be set separately from attestation target in block processing or authority switch process
    #[setters(bool)]
    prefinalized: Option<bool>,

    ancestor_blocks_finalization_distances: Option<HashMap<BlockIdentifier, usize>>,

    // Flag indicated that block has been already received and stored in repo
    #[setters(skip)]
    stored: Option<bool>,

    // Note:
    // is_invalidated and is_validated ARE NOTE mutually exclusive.
    // For example, it is possible to have a valid block invalidated
    // due to a fork condition.
    #[setters(bool, assert_none = false)]
    invalidated: Option<bool>,

    // Note:
    // We must keep every BLS signature separate.
    // Otherwise it will not be possible to blame anyone
    // if there were an attacker that faked his signature thus broke all signatures if mixed.
    // Also, it is not possible to verify signatures upfront
    // since it could be that Nack comes before block itself.
    #[setters(skip)]
    bad_block_accusers:
        Vec<(HashMap<SignerIndex, u16>, <GoshBLS as BLSSignatureScheme>::Signature)>,

    at_least_one_verified_bad_block_accuser: Option<bool>,

    #[setters(skip)]
    resolved_nacks_count: u64,

    // BlockKeeper set for this block it's taken from parent block without any BK set changes
    // happened in this block
    bk_set: Option<Arc<BlockKeeperSet>>,

    // Set of Block Keepers that has deployed pre epoch contract and will be added to the BK set
    // after pre epoch contract destruction
    future_bk_set: Option<Arc<BlockKeeperSet>>,

    // BlockKeeper set for descendant blocks with all BK set changes happened in this block
    descendant_bk_set: Option<Arc<BlockKeeperSet>>,

    // Set of future Block Keepers for descendant blocks with all BK set changes happened in this
    // block
    descendant_future_bk_set: Option<Arc<BlockKeeperSet>>,

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
    has_primary_attestation_target_met: Option<bool>,

    #[setters(bool, assert_none = false)]
    has_fallback_attestation_target_met: Option<bool>,

    // Calculated baseline for finalization. Must be calculated based on prev
    // history.
    // The DescendantsChainLength is the exact descendant when it will be checked
    // if this chain has collected the required number of attestations.
    attestation_target: Option<AttestationTargets>,

    #[setters(skip)]
    verified_attestations: HashMap<BlockIdentifier, HashSet<SignerIndex>>,

    block_stats: Option<BlockStatistics>,

    #[setters(skip)]
    #[getter(skip)]
    known_children: HashMap<ThreadIdentifier, HashSet<BlockIdentifier>>,

    own_attestation: Option<Envelope<GoshBLS, AttestationData>>,

    retracted_attestation: Option<Envelope<GoshBLS, NackData>>,

    // Nodes that had sent messages indicating their interest in getting attestations for the block.
    #[setters(skip)]
    known_attestation_interested_parties: HashSet<NodeIdentifier>,

    envelope_hash: Option<AckiNackiEnvelopeHash>,

    #[serde(skip)]
    #[getter(skip)]
    #[setters(skip)]
    pub(super) file_path: PathBuf,

    #[serde(skip)]
    #[getter(skip)]
    #[setters(skip)]
    pub(super) notifications: Arc<AtomicU32>,

    #[getter(skip)]
    #[setters(skip)]
    pub event_timestamps: EventTimestamps,
}

// impl std::fmt::Debug for AckiNackiBlockState {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         f.debug_struct("BlockState")
//             .field("block_identifier", &self.block_identifier)
//             .field("thread_id", &self.thread_identifier)
//             .field("parent_block_identifier", &self.parent_block_identifier)
//             .field("block_seq_no", &self.block_seq_no)
//             .field("signatures_verified", &self.signatures_verified)
//             .field(
//                 "envelope_block_producer_signature_verified",
//                 &self.envelope_block_producer_signature_verified,
//             )
//             .field("applied", &self.applied)
//             .field("validated", &self.validated)
//             .field("resolves_forks", &self.resolves_forks)
//             .field("must_be_validated", &self.must_be_validated)
//             .field("finalized", &self.finalized)
//             .field("stored", &self.stored)
//             .field("invalidated", &self.invalidated)
//             .field("has_parent_finalized", &self.has_parent_finalized)
//             .field(
//                 "has_all_cross_thread_ref_data_available",
//                 &self.has_all_cross_thread_ref_data_available,
//             )
//             .field("has_cross_thread_ref_data_prepared", &self.has_cross_thread_ref_data_prepared)
//             .field("has_primary_attestation_target_met", &self.has_primary_attestation_target_met)
//             .field(
//                 "has_attestations_target_met_in_a_resolved_fork_case",
//                 &self.has_attestations_target_met_in_a_resolved_fork_case,
//             )
//             .field("attestation_target.primary()", &self.attestation_target.primary())
//             .field("verified_attestations", &self.verified_attestations)
//             .field("block_stats", &self.block_stats)
//             .field("known_children", &self.known_children)
//             .field("attestation", &self.attestation)
//             .field("retracted_attestation", &self.retracted_attestation)
//             .field(
//                 "known_attestation_interested_parties",
//                 &self.known_attestation_interested_parties,
//             )
//             .field("event_timestamps", &self.event_timestamps)
//             .finish()
//     }
// }

impl std::fmt::Display for AckiNackiBlockState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "BlockState (block_id: {:?}, block_seq_no: {:?})",
            self.block_identifier, self.block_seq_no
        )
    }
}

impl AllowGuardedMut for AckiNackiBlockState {
    fn inner_guarded_mut<F, T>(&mut self, action: F) -> T
    where
        F: FnOnce(&mut Self) -> T,
    {
        if !self.bulk_change {
            self.set_bulk_change(true).expect("Should not fail");
            let res = action(self);
            self.set_bulk_change(false).expect("failed to save AckiNackiBlockState");
            res
        } else {
            action(self)
        }
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
        self.has_primary_attestation_target_met == Some(true)
    }

    pub fn can_be_finalized(&self) -> bool {
        let res = self.is_signatures_verified()
            && !self.is_invalidated()
            && self.has_all_nacks_resolved()
            && self.is_block_already_applied()
            && self.has_parent_finalized == Some(true)
            && self.has_all_cross_thread_references_finalized == Some(true)
            && self.has_all_cross_thread_ref_data_available == Some(true)
            && self.has_attestations_target_met()
            && self.has_cross_thread_ref_data_prepared == Some(true)
            && !self.is_finalized();
        tracing::trace!("BlockState::can_finalize(res={res}): {self:?}");
        res
    }

    pub fn has_bad_block_nacks_resolved(&self) -> bool {
        if self.bad_block_accusers.is_empty() {
            true
        } else {
            self.validated == Some(true)
        }
    }

    pub fn set_stored(&mut self, block: &Envelope<GoshBLS, AckiNackiBlock>) -> anyhow::Result<()> {
        self.stored = Some(true);
        self.envelope_hash = Some(crate::types::envelope_hash::envelope_hash(block));
        self.save()
    }

    pub fn set_stored_zero_state(&mut self) -> anyhow::Result<()> {
        self.stored = Some(true);
        self.envelope_hash = Some(AckiNackiEnvelopeHash([0; 32]));
        self.save()
    }

    pub fn has_all_nacks_resolved(&self) -> bool {
        self.has_bad_block_nacks_resolved()
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

    pub fn is_prefinalized(&self) -> bool {
        self.prefinalized == Some(true)
    }

    pub fn is_stored(&self) -> bool {
        self.stored == Some(true)
    }

    // Function checks whether block was successfully applied or validated
    pub fn is_block_already_applied(&self) -> bool {
        self.applied == Some(true)
    }

    pub fn add_proposed_in_round(&mut self, round: u16) -> anyhow::Result<()> {
        self.proposed_in_round.insert(round);
        self.save()
    }

    pub fn add_suspicious(
        &mut self,
        accusers: HashMap<SignerIndex, u16>,
        signatures: <GoshBLS as BLSSignatureScheme>::Signature,
    ) -> anyhow::Result<()> {
        self.bad_block_accusers.push((accusers, signatures));
        self.save()
    }

    // pub fn resolve_suspicious(&mut self) -> anyhow::Result<()> {
    // self.resolved_nacks_count += 1;
    // self.save()
    // }

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

    pub fn add_child(
        &mut self,
        child_block_thread_identifier: ThreadIdentifier,
        child_block_identifier: BlockIdentifier,
    ) -> anyhow::Result<()> {
        tracing::trace!(
            "add child for {:?} child: {child_block_identifier:?}, {child_block_thread_identifier}",
            self.block_identifier
        );
        self.known_children
            .entry(child_block_thread_identifier)
            .or_default()
            .insert(child_block_identifier);
        self.save()
    }

    pub fn known_children(
        &self,
        child_block_thread_identifier: &ThreadIdentifier,
    ) -> Option<&HashSet<BlockIdentifier>> {
        self.known_children.get(child_block_thread_identifier)
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

    // This function is WRONG! It will change depending on the descendant_bk_set thus makes it mutable. MUST BE REMOVED
    // pub fn get_descendant_bk_set(&self) -> Arc<BlockKeeperSet> {
    // if let Some(bk_set) = &self.descendant_bk_set {
    // bk_set.clone()
    // } else {
    // assert!(self.bk_set.is_some());
    // self.bk_set.clone().unwrap()
    // }
    // }

    // It is made pub super to allow helper methods to explicitly call it.
    pub(super) fn save(&mut self) -> anyhow::Result<()> {
        if self.bulk_change {
            self.save_requested = true;
            return Ok(());
        }
        super::private::save(self)?;
        self.notifications.fetch_add(1, Ordering::Relaxed);
        atomic_wait::wake_all(self.notifications.as_ref());
        Ok(())
    }

    pub fn set_bulk_change(&mut self, bulk_change: bool) -> anyhow::Result<()> {
        self.bulk_change = bulk_change;
        if !self.bulk_change && self.save_requested {
            self.save()
        } else {
            self.save_requested = false;
            Ok(())
        }
    }

    pub fn update_finalizes_blocks(&mut self, block_id: BlockIdentifier) {
        tracing::trace!(
            "update_finalizes_blocks: self={:?}, add={:?}",
            self.block_identifier,
            block_id
        );
        let mut current_value = self.finalizes_blocks.clone().unwrap_or_default();
        current_value.insert(block_id);
        self.finalizes_blocks = Some(current_value);
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, Default)]
pub struct EventTimestamps {
    pub received_ms: Option<u64>,
    pub attestation_sent_ms: Option<u64>,
    pub verify_all_block_signatures_ms_total: Option<u128>,
    pub block_process_timestamp_was_reported: bool,
    pub block_applied_timestamp_ms: Option<u64>,
}
