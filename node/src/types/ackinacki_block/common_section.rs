// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

#[cfg(feature = "history_proofs")]
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use derive_getters::Getters;
use derive_setters::Setters;
use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use typed_builder::TypedBuilder;
use versioned_struct::versioned;
use versioned_struct::Transitioning;

use crate::block_keeper_system::BlockKeeperSetChange;
use crate::bls::envelope::Envelope;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::types::bp_selector::ProducerSelector;
#[cfg(feature = "history_proofs")]
use crate::types::history_proof::LayerNumber;
#[cfg(feature = "history_proofs")]
use crate::types::history_proof::ProofLayerRootHash;
use crate::types::BlockHeight;
use crate::types::BlockRound;
use crate::types::ThreadsTablePrefab;
#[cfg(feature = "protocol_version_hash_in_block")]
use crate::versioning::protocol_version::ProtocolVersionHash;

#[versioned]
#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq, TypedBuilder, Getters)]
pub struct Directives {
    #[future]
    share_state_resources: bool,
    #[legacy]
    share_state_resources: Option<HashMap<ThreadIdentifier, BlockIdentifier>>,
}

pub enum DirectivesVersioned {
    New(Directives),
    Old(DirectivesOld),
}

impl DirectivesVersioned {
    pub fn share_state_resources(&self) -> bool {
        match self {
            DirectivesVersioned::New(directives) => directives.share_state_resources,
            DirectivesVersioned::Old(directives) => directives.share_state_resources.is_some(),
        }
    }
}

impl Debug for Directives {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("").field("share_state_resources", &self.share_state_resources).finish()
    }
}

impl Debug for DirectivesOld {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("").field("share_state_resources", &self.share_state_resources).finish()
    }
}

#[versioned]
#[derive(Clone, PartialEq, Eq, Getters, TypedBuilder, Setters)]
#[setters(prefix = "set_", borrow_self)]
pub struct CommonSection {
    // Parent Block ID (Merkle hash). Stored here so that Block ID can be computed
    // from stable data only, without relying on TVM block's prev_ref.
    #[future]
    parent_block_id: BlockIdentifier,
    block_height: BlockHeight,
    #[future]
    directives: Directives,
    #[legacy]
    directives: DirectivesOld,
    // TODO: we assume that all attestations are aggregated and there are no duplicates on (block_id, attestation_target_type)
    // otherwise it may break block processing cycle
    block_attestations: Vec<Envelope<AttestationData>>,
    round: BlockRound,
    producer_id: NodeIdentifier,
    thread_id: ThreadIdentifier,
    threads_table: Option<ThreadsTablePrefab>,
    /// Extra references this block depends on.
    /// This can be any combination of:
    /// - sources of internal messages from another thread
    /// - source of an update of the threads state
    /// - source of updates for account migrations due to a new dapp set.
    /// - (?)
    refs: Vec<BlockIdentifier>,
    block_keeper_set_changes: Vec<BlockKeeperSetChange>,
    // Dynamic parameter: an expected number of Acki-Nacki for this block
    verify_complexity: SignerIndex,
    acks: Vec<Envelope<AckData>>,
    nacks: Vec<Envelope<NackData>>,
    // This field must be set, but it is option, because we can't set it up on block creation and update it later
    producer_selector: Option<ProducerSelector>,

    #[cfg(feature = "monitor-accounts-number")]
    accounts_number_diff: i64,

    #[cfg(feature = "protocol_version_hash_in_block")]
    protocol_version_hash: ProtocolVersionHash,

    // Mapping of Layer root hashes
    // root mapping key <K> is number of layer [1-N]
    // child mapping value is root hash #L<K>()
    #[cfg(feature = "history_proofs")]
    history_proofs: BTreeMap<LayerNumber, ProofLayerRootHash>,
}

impl Transitioning for CommonSection {
    type Old = CommonSectionOld;

    fn from(old: Self::Old) -> Self {
        let directives =
            Directives { share_state_resources: old.directives.share_state_resources.is_some() };
        Self {
            parent_block_id: BlockIdentifier::default(),
            block_height: old.block_height,
            directives,
            block_attestations: old.block_attestations,
            round: old.round,
            producer_id: old.producer_id,
            thread_id: old.thread_id,
            threads_table: old.threads_table,
            refs: old.refs,
            block_keeper_set_changes: old.block_keeper_set_changes,
            verify_complexity: old.verify_complexity,
            acks: old.acks,
            nacks: old.nacks,
            producer_selector: old.producer_selector,
            #[cfg(feature = "monitor-accounts-number")]
            accounts_number_diff: old.accounts_number_diff,
            #[cfg(feature = "protocol_version_hash_in_block")]
            protocol_version_hash: old.protocol_version_hash,
            #[cfg(feature = "history_proofs")]
            history_proofs: old.history_proofs,
        }
    }
}

impl CommonSection {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent_block_id: BlockIdentifier,
        thread_id: ThreadIdentifier,
        round: BlockRound,
        producer_id: NodeIdentifier,
        block_keeper_set_changes: Vec<BlockKeeperSetChange>,
        verify_complexity: SignerIndex,
        refs: Vec<BlockIdentifier>,
        threads_table: Option<ThreadsTablePrefab>,
        block_height: BlockHeight,
        #[cfg(feature = "monitor-accounts-number")] accounts_number_diff: i64,
        #[cfg(feature = "protocol_version_hash_in_block")]
        protocol_version_hash: ProtocolVersionHash,
    ) -> Self {
        CommonSection {
            parent_block_id,
            block_height,
            round,
            block_attestations: vec![],
            directives: Directives::default(),
            thread_id,
            threads_table,
            refs,
            producer_id,
            block_keeper_set_changes,
            verify_complexity,
            acks: vec![],
            nacks: vec![],
            producer_selector: None,
            #[cfg(feature = "monitor-accounts-number")]
            accounts_number_diff,
            #[cfg(feature = "protocol_version_hash_in_block")]
            protocol_version_hash,
            #[cfg(feature = "history_proofs")]
            history_proofs: BTreeMap::new(),
        }
    }

    fn wrap_serialize(&self) -> WrappedCommonSection {
        let block_attestations_data = bincode::serialize(&self.block_attestations)
            .expect("Failed to serialize last finalized blocks");
        let acks_data =
            bincode::serialize(&self.acks).expect("Failed to serialize last finalized blocks");
        let nacks_data =
            bincode::serialize(&self.nacks).expect("Failed to serialize last finalized blocks");

        let builder = WrappedCommonSection::builder()
            .parent_block_id(self.parent_block_id)
            .round(self.round)
            .directives(self.directives.clone())
            .block_attestations(block_attestations_data)
            .producer_id(self.producer_id.clone())
            .block_keeper_set_changes(self.block_keeper_set_changes.clone())
            .verify_complexity(self.verify_complexity)
            .acks(acks_data)
            .nacks(nacks_data)
            .producer_selector(
                self.producer_selector
                    .clone()
                    .expect("Producer selector must be set before serialization"),
            )
            .thread_identifier(self.thread_id)
            .refs(self.refs.clone())
            .threads_table(self.threads_table.clone())
            .block_height(self.block_height);
        #[cfg(feature = "history_proofs")]
        let builder = builder.history_proofs(self.history_proofs.clone());

        #[cfg(feature = "monitor-accounts-number")]
        let builder = builder.accounts_number_diff(self.accounts_number_diff);

        #[cfg(feature = "protocol_version_hash_in_block")]
        let builder = builder.protocol_version_hash(self.protocol_version_hash.clone());

        builder.build()
    }

    fn wrap_deserialize(data: WrappedCommonSection) -> Self {
        let block_attestations: Vec<Envelope<AttestationData>> =
            bincode::deserialize(&data.block_attestations)
                .expect("Failed to deserialize block attestations");
        let acks: Vec<Envelope<AckData>> =
            bincode::deserialize(&data.acks).expect("Failed to deserialize acks");
        let nacks: Vec<Envelope<NackData>> =
            bincode::deserialize(&data.nacks).expect("Failed to deserialize nacks");

        let builder = Self::builder()
            .parent_block_id(data.parent_block_id)
            .round(data.round)
            .directives(data.directives)
            .block_attestations(block_attestations)
            .producer_id(data.producer_id)
            .block_keeper_set_changes(data.block_keeper_set_changes)
            .verify_complexity(data.verify_complexity)
            .acks(acks)
            .nacks(nacks)
            .producer_selector(Some(data.producer_selector))
            .refs(data.refs)
            .thread_id(data.thread_identifier)
            .threads_table(data.threads_table)
            .block_height(data.block_height);

        #[cfg(feature = "history_proofs")]
        let builder = builder.history_proofs(data.history_proofs);

        #[cfg(feature = "monitor-accounts-number")]
        let builder = builder.accounts_number_diff(data.accounts_number_diff);

        #[cfg(feature = "protocol_version_hash_in_block")]
        let builder = builder.protocol_version_hash(data.protocol_version_hash);

        builder.build()
    }

    pub fn full_hash_data(&self) -> Vec<u8> {
        let data = self.wrap_serialize();
        bincode::serialize(&data).expect("AllFieldsCommonSection must serialize")
    }
}

#[derive(TypedBuilder, Serialize, Deserialize)]
struct WrappedCommonSection {
    pub parent_block_id: BlockIdentifier,
    pub round: BlockRound,
    pub directives: Directives,
    pub block_attestations: Vec<u8>,
    pub producer_id: NodeIdentifier,
    pub block_keeper_set_changes: Vec<BlockKeeperSetChange>,
    pub verify_complexity: SignerIndex,
    pub acks: Vec<u8>,
    pub nacks: Vec<u8>,
    pub producer_selector: ProducerSelector,
    pub thread_identifier: ThreadIdentifier,
    pub refs: Vec<BlockIdentifier>,
    pub threads_table: Option<ThreadsTablePrefab>,
    pub block_height: BlockHeight,
    #[cfg(feature = "monitor-accounts-number")]
    pub accounts_number_diff: i64,
    #[cfg(feature = "protocol_version_hash_in_block")]
    pub protocol_version_hash: ProtocolVersionHash,
    #[cfg(feature = "history_proofs")]
    pub history_proofs: BTreeMap<LayerNumber, ProofLayerRootHash>,
}

impl Serialize for CommonSection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.wrap_serialize().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CommonSection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wrapped_data = WrappedCommonSection::deserialize(deserializer)?;
        Ok(CommonSection::wrap_deserialize(wrapped_data))
    }
}

impl Debug for CommonSection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut fmt = f.debug_struct("");

        fmt.field("parent_block_id", &self.parent_block_id)
            .field("thread_id", &self.thread_id)
            .field("round", &self.round)
            .field("producer_id", &self.producer_id)
            .field("directives", &self.directives)
            .field("block_attestations", &self.block_attestations)
            .field("block_keeper_set_changes", &self.block_keeper_set_changes)
            .field("verify_complexity", &self.verify_complexity)
            .field("acks", &self.acks)
            .field("nacks", &self.nacks)
            .field("producer_selector", &self.producer_selector)
            .field("refs", &self.refs)
            .field("threads_table", &self.threads_table)
            .field("block_height", &self.block_height.height());

        #[cfg(feature = "history_proofs")]
        fmt.field("history_proofs", &self.history_proofs);

        #[cfg(feature = "monitor-accounts-number")]
        fmt.field("accounts_number_diff", &self.accounts_number_diff);
        #[cfg(feature = "protocol_version_hash_in_block")]
        fmt.field("protocol_version_hash", &self.protocol_version_hash);

        fmt.finish()
    }
}

impl CommonSectionOld {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        thread_id: ThreadIdentifier,
        round: BlockRound,
        producer_id: NodeIdentifier,
        block_keeper_set_changes: Vec<BlockKeeperSetChange>,
        verify_complexity: SignerIndex,
        refs: Vec<BlockIdentifier>,
        threads_table: Option<ThreadsTablePrefab>,
        block_height: BlockHeight,
        #[cfg(feature = "monitor-accounts-number")] accounts_number_diff: i64,
        #[cfg(feature = "protocol_version_hash_in_block")]
        protocol_version_hash: ProtocolVersionHash,
    ) -> Self {
        CommonSectionOld {
            block_height,
            round,
            block_attestations: vec![],
            directives: DirectivesOld::default(),
            thread_id,
            threads_table,
            refs,
            producer_id,
            block_keeper_set_changes,
            verify_complexity,
            acks: vec![],
            nacks: vec![],
            producer_selector: None,
            #[cfg(feature = "monitor-accounts-number")]
            accounts_number_diff,
            #[cfg(feature = "protocol_version_hash_in_block")]
            protocol_version_hash,
            #[cfg(feature = "history_proofs")]
            history_proofs: BTreeMap::new(),
        }
    }

    fn wrap_serialize(&self) -> WrappedCommonSectionOld {
        let block_attestations_data = bincode::serialize(&self.block_attestations)
            .expect("Failed to serialize last finalized blocks");
        let acks_data =
            bincode::serialize(&self.acks).expect("Failed to serialize last finalized blocks");
        let nacks_data =
            bincode::serialize(&self.nacks).expect("Failed to serialize last finalized blocks");

        let builder = WrappedCommonSectionOld::builder()
            .round(self.round)
            .directives(self.directives.clone())
            .block_attestations(block_attestations_data)
            .producer_id(self.producer_id.clone())
            .block_keeper_set_changes(self.block_keeper_set_changes.clone())
            .verify_complexity(self.verify_complexity)
            .acks(acks_data)
            .nacks(nacks_data)
            .producer_selector(
                self.producer_selector
                    .clone()
                    .expect("Producer selector must be set before serialization"),
            )
            .thread_identifier(self.thread_id)
            .refs(self.refs.clone())
            .threads_table(self.threads_table.clone())
            .block_height(self.block_height);
        #[cfg(feature = "history_proofs")]
        let builder = builder.history_proofs(self.history_proofs.clone());

        #[cfg(feature = "monitor-accounts-number")]
        let builder = builder.accounts_number_diff(self.accounts_number_diff);

        #[cfg(feature = "protocol_version_hash_in_block")]
        let builder = builder.protocol_version_hash(self.protocol_version_hash.clone());

        builder.build()
    }

    fn wrap_deserialize(data: WrappedCommonSectionOld) -> Self {
        let block_attestations: Vec<Envelope<AttestationData>> =
            bincode::deserialize(&data.block_attestations)
                .expect("Failed to deserialize block attestations");
        let acks: Vec<Envelope<AckData>> =
            bincode::deserialize(&data.acks).expect("Failed to deserialize acks");
        let nacks: Vec<Envelope<NackData>> =
            bincode::deserialize(&data.nacks).expect("Failed to deserialize nacks");

        let builder = Self::builder()
            .round(data.round)
            .directives(data.directives)
            .block_attestations(block_attestations)
            .producer_id(data.producer_id)
            .block_keeper_set_changes(data.block_keeper_set_changes)
            .verify_complexity(data.verify_complexity)
            .acks(acks)
            .nacks(nacks)
            .producer_selector(Some(data.producer_selector))
            .refs(data.refs)
            .thread_id(data.thread_identifier)
            .threads_table(data.threads_table)
            .block_height(data.block_height);

        #[cfg(feature = "history_proofs")]
        let builder = builder.history_proofs(data.history_proofs);

        #[cfg(feature = "monitor-accounts-number")]
        let builder = builder.accounts_number_diff(data.accounts_number_diff);

        #[cfg(feature = "protocol_version_hash_in_block")]
        let builder = builder.protocol_version_hash(data.protocol_version_hash);

        builder.build()
    }

    pub fn full_hash_data(&self) -> Vec<u8> {
        let data = self.wrap_serialize();
        bincode::serialize(&data).expect("AllFieldsCommonSection must serialize")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_block_height() -> BlockHeight {
        BlockHeight::builder().thread_identifier(ThreadIdentifier::default()).height(7).build()
    }

    fn make_selector(parent_block_id: BlockIdentifier) -> ProducerSelector {
        ProducerSelector::builder().rng_seed_block_id(parent_block_id).index(0).build()
    }

    #[test]
    fn common_section_roundtrip_preserves_parent_block_id() {
        let parent_block_id = BlockIdentifier::default();
        let mut common_section = CommonSection::new(
            parent_block_id,
            ThreadIdentifier::default(),
            BlockRound::default(),
            NodeIdentifier::some_id(),
            vec![],
            SignerIndex::default(),
            vec![],
            None,
            make_block_height(),
            #[cfg(feature = "monitor-accounts-number")]
            0,
            #[cfg(feature = "protocol_version_hash_in_block")]
            Default::default(),
        );
        common_section.set_producer_selector(Some(make_selector(parent_block_id)));

        let encoded = bincode::serialize(&common_section).unwrap();
        let decoded: CommonSection = bincode::deserialize(&encoded).unwrap();

        assert_eq!(decoded.parent_block_id(), &parent_block_id);
    }

    #[test]
    fn transitioned_common_section_uses_explicit_default_parent() {
        let transitioned = <CommonSection as Transitioning>::from(CommonSectionOld::new(
            ThreadIdentifier::default(),
            BlockRound::default(),
            NodeIdentifier::some_id(),
            vec![],
            SignerIndex::default(),
            vec![],
            None,
            make_block_height(),
            #[cfg(feature = "monitor-accounts-number")]
            0,
            #[cfg(feature = "protocol_version_hash_in_block")]
            Default::default(),
        ));

        assert_eq!(*transitioned.parent_block_id(), BlockIdentifier::default());
    }

    #[test]
    fn explicit_default_parent_roundtrips_for_special_case() {
        let parent_block_id = BlockIdentifier::default();
        let mut common_section = CommonSection::new(
            parent_block_id,
            ThreadIdentifier::default(),
            BlockRound::default(),
            NodeIdentifier::some_id(),
            vec![],
            SignerIndex::default(),
            vec![],
            None,
            make_block_height(),
            #[cfg(feature = "monitor-accounts-number")]
            0,
            #[cfg(feature = "protocol_version_hash_in_block")]
            Default::default(),
        );
        common_section.set_producer_selector(Some(make_selector(parent_block_id)));

        let encoded = bincode::serialize(&common_section).unwrap();
        let decoded: CommonSection = bincode::deserialize(&encoded).unwrap();

        assert_eq!(*decoded.parent_block_id(), BlockIdentifier::default());
    }
}

#[derive(TypedBuilder, Serialize, Deserialize)]
struct WrappedCommonSectionOld {
    pub round: BlockRound,
    pub directives: DirectivesOld,
    pub block_attestations: Vec<u8>,
    pub producer_id: NodeIdentifier,
    pub block_keeper_set_changes: Vec<BlockKeeperSetChange>,
    pub verify_complexity: SignerIndex,
    pub acks: Vec<u8>,
    pub nacks: Vec<u8>,
    pub producer_selector: ProducerSelector,
    pub thread_identifier: ThreadIdentifier,
    pub refs: Vec<BlockIdentifier>,
    pub threads_table: Option<ThreadsTablePrefab>,
    pub block_height: BlockHeight,
    #[cfg(feature = "monitor-accounts-number")]
    pub accounts_number_diff: i64,
    #[cfg(feature = "protocol_version_hash_in_block")]
    pub protocol_version_hash: ProtocolVersionHash,
    #[cfg(feature = "history_proofs")]
    pub history_proofs: BTreeMap<LayerNumber, ProofLayerRootHash>,
}

impl Serialize for CommonSectionOld {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.wrap_serialize().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CommonSectionOld {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wrapped_data = WrappedCommonSectionOld::deserialize(deserializer)?;
        Ok(CommonSectionOld::wrap_deserialize(wrapped_data))
    }
}

impl Debug for CommonSectionOld {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut fmt = f.debug_struct("");

        fmt.field("thread_id", &self.thread_id)
            .field("round", &self.round)
            .field("producer_id", &self.producer_id)
            .field("directives", &self.directives)
            .field("block_attestations", &self.block_attestations)
            .field("block_keeper_set_changes", &self.block_keeper_set_changes)
            .field("verify_complexity", &self.verify_complexity)
            .field("acks", &self.acks)
            .field("nacks", &self.nacks)
            .field("producer_selector", &self.producer_selector)
            .field("refs", &self.refs)
            .field("threads_table", &self.threads_table)
            .field("block_height", &self.block_height.height());

        #[cfg(feature = "history_proofs")]
        fmt.field("history_proofs", &self.history_proofs);

        #[cfg(feature = "monitor-accounts-number")]
        fmt.field("accounts_number_diff", &self.accounts_number_diff);
        #[cfg(feature = "protocol_version_hash_in_block")]
        fmt.field("protocol_version_hash", &self.protocol_version_hash);

        fmt.finish()
    }
}

#[derive(Clone)]
pub enum CommonSectionVersioned {
    New(CommonSection),
    Old(CommonSectionOld),
}

impl CommonSectionVersioned {
    pub fn block_keeper_set_changes(&self) -> &Vec<BlockKeeperSetChange> {
        match self {
            CommonSectionVersioned::New(common_section) => {
                common_section.block_keeper_set_changes()
            }
            CommonSectionVersioned::Old(common_section) => {
                common_section.block_keeper_set_changes()
            }
        }
    }

    pub fn set_acks(&mut self, acks: Vec<Envelope<AckData>>) {
        match self {
            CommonSectionVersioned::New(common_section) => {
                common_section.acks = acks;
            }
            CommonSectionVersioned::Old(common_section) => {
                common_section.acks = acks;
            }
        }
    }

    pub fn set_nacks(&mut self, nacks: Vec<Envelope<NackData>>) {
        match self {
            CommonSectionVersioned::New(common_section) => {
                common_section.nacks = nacks;
            }
            CommonSectionVersioned::Old(common_section) => {
                common_section.nacks = nacks;
            }
        }
    }

    pub fn set_block_attestations(&mut self, block_attestations: Vec<Envelope<AttestationData>>) {
        match self {
            CommonSectionVersioned::New(common_section) => {
                common_section.block_attestations = block_attestations;
            }
            CommonSectionVersioned::Old(common_section) => {
                common_section.block_attestations = block_attestations;
            }
        }
    }

    pub fn set_producer_selector(&mut self, producer_selector: Option<ProducerSelector>) {
        match self {
            CommonSectionVersioned::New(common_section) => {
                common_section.producer_selector = producer_selector;
            }
            CommonSectionVersioned::Old(common_section) => {
                common_section.producer_selector = producer_selector;
            }
        }
    }

    pub fn set_directives(&mut self, directives: DirectivesVersioned) {
        match self {
            CommonSectionVersioned::New(common_section) => {
                if let DirectivesVersioned::New(directives) = directives {
                    common_section.directives = directives;
                } else {
                    unreachable!("Directives and common section vestions mismatch")
                }
            }
            CommonSectionVersioned::Old(common_section) => {
                if let DirectivesVersioned::Old(directives) = directives {
                    common_section.directives = directives;
                } else {
                    unreachable!("Directives and common section vestions mismatch")
                }
            }
        }
    }

    #[cfg(feature = "history_proofs")]
    pub fn set_history_proofs(
        &mut self,
        history_proofs: BTreeMap<LayerNumber, ProofLayerRootHash>,
    ) {
        if let CommonSectionVersioned::New(common_section) = self {
            common_section.history_proofs = history_proofs;
        }
    }

    pub fn round(&self) -> &u64 {
        match self {
            CommonSectionVersioned::New(common_section) => &common_section.round,
            CommonSectionVersioned::Old(common_section) => &common_section.round,
        }
    }

    pub fn producer_selector(&self) -> Option<ProducerSelector> {
        match self {
            CommonSectionVersioned::New(common_section) => common_section.producer_selector.clone(),
            CommonSectionVersioned::Old(common_section) => common_section.producer_selector.clone(),
        }
    }

    pub fn block_attestations(&self) -> Vec<Envelope<AttestationData>> {
        match self {
            CommonSectionVersioned::New(common_section) => {
                common_section.block_attestations.clone()
            }
            CommonSectionVersioned::Old(common_section) => {
                common_section.block_attestations.clone()
            }
        }
    }

    pub fn producer_id(&self) -> NodeIdentifier {
        match self {
            CommonSectionVersioned::New(common_section) => common_section.producer_id.clone(),
            CommonSectionVersioned::Old(common_section) => common_section.producer_id.clone(),
        }
    }

    pub fn thread_id(&self) -> ThreadIdentifier {
        match self {
            CommonSectionVersioned::New(common_section) => common_section.thread_id,
            CommonSectionVersioned::Old(common_section) => common_section.thread_id,
        }
    }

    pub fn directives(&self) -> DirectivesVersioned {
        match self {
            CommonSectionVersioned::New(common_section) => {
                DirectivesVersioned::New(common_section.directives.clone())
            }
            CommonSectionVersioned::Old(common_section) => {
                DirectivesVersioned::Old(common_section.directives.clone())
            }
        }
    }

    pub fn block_height(&self) -> &BlockHeight {
        match self {
            CommonSectionVersioned::New(common_section) => &common_section.block_height,
            CommonSectionVersioned::Old(common_section) => &common_section.block_height,
        }
    }

    pub fn threads_table(&self) -> &Option<ThreadsTablePrefab> {
        match self {
            CommonSectionVersioned::New(common_section) => &common_section.threads_table,
            CommonSectionVersioned::Old(common_section) => &common_section.threads_table,
        }
    }
}
