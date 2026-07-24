// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use derive_getters::Getters;
use derive_setters::Setters;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::TemporaryBlockId;
use node_types::ThreadIdentifier;
use serde::ser::Error as SerError;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use typed_builder::TypedBuilder;

use crate::block_keeper_system::BlockKeeperSetChange;
use crate::block_keeper_system::BlockKeeperSetTransitionHashes;
use crate::bls::envelope::Envelope;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::types::bp_selector::ProducerSelector;
use crate::types::history_proof::LayerNumber;
use crate::types::history_proof::ProofLayerRootHash;
use crate::types::BlockHeight;
use crate::types::BlockRound;
use crate::types::ThreadsTablePrefab;
#[cfg(feature = "protocol_version_hash_in_block")]
use crate::versioning::protocol_version::ProtocolVersionHash;

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq, TypedBuilder, Getters)]
pub struct Directives {
    share_state_resources: bool,
}

impl Debug for Directives {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("").field("share_state_resources", &self.share_state_resources).finish()
    }
}

#[derive(Clone, PartialEq, Eq, Getters, TypedBuilder, Setters, Serialize, Deserialize)]
#[setters(prefix = "set_", borrow_self)]
pub struct BlockKeeperSetChangeProofData {
    transition_hashes: BlockKeeperSetTransitionHashes,
    history_proof_layer_hashes: BTreeMap<LayerNumber, (BlockHeight, [u8; 32])>,
}

#[derive(Clone, PartialEq, Eq, Getters, TypedBuilder, Setters)]
#[setters(prefix = "set_", borrow_self)]
pub struct CommonSection {
    // Parent Block ID (Merkle hash). Stored here so that Block ID can be computed
    // from stable data only, without relying on TVM block's prev_ref.
    parent_block_id: ParentBlockId,
    block_height: BlockHeight,
    directives: Directives,
    // TODO: we assume that all attestations are aggregated and there are no duplicates on (block_id, attestation_target_type)
    // otherwise it may break block processing cycle
    block_attestations: Vec<Envelope<AttestationData>>,
    round: BlockRound,
    producer_id: NodeIdentifier,
    thread_id: ThreadIdentifier,
    threads_table: Option<ThreadsTablePrefab>,
    /// Extra references this block depends on.
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
    history_proofs: BTreeMap<LayerNumber, ProofLayerRootHash>,

    /// Merkle root of outbound external messages from the tracked address in this block.
    /// Zero if there were no such messages.
    #[builder(default)]
    tracked_ext_out_messages_root: [u8; 32],

    /// Serialized hashes of outbound external messages from tracked addresses in this block.
    /// Account routings are canonically ordered; message order within a routing is preserved.
    #[builder(default)]
    tracked_ext_out_messages: BTreeMap<AccountRouting, Vec<[u8; 32]>>,

    #[builder(default)]
    block_keeper_set_change_proof_data: Option<BlockKeeperSetChangeProofData>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParentBlockId {
    Block(BlockIdentifier),
    Unresolved(TemporaryBlockId),
}

impl ParentBlockId {
    pub fn block_id(&self) -> Option<&BlockIdentifier> {
        match self {
            Self::Block(block_id) => Some(block_id),
            Self::Unresolved(_) => None,
        }
    }

    pub fn expect_block_id(&self) -> &BlockIdentifier {
        self.block_id().expect("Parent block ID must be resolved before use")
    }

    fn serialize_block_id(&self) -> Result<BlockIdentifier, String> {
        match self {
            Self::Block(block_id) => Ok(*block_id),
            Self::Unresolved(temp_id) => Err(format!(
                "Cannot serialize common section with unresolved parent block ID: {temp_id:?}"
            )),
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
        tracked_ext_out_messages: BTreeMap<AccountRouting, Vec<[u8; 32]>>,
        tracked_ext_out_messages_root: [u8; 32],
    ) -> Self {
        Self::new_with_parent_block_id(
            ParentBlockId::Block(parent_block_id),
            thread_id,
            round,
            producer_id,
            block_keeper_set_changes,
            verify_complexity,
            refs,
            threads_table,
            block_height,
            #[cfg(feature = "monitor-accounts-number")]
            accounts_number_diff,
            #[cfg(feature = "protocol_version_hash_in_block")]
            protocol_version_hash,
            tracked_ext_out_messages,
            tracked_ext_out_messages_root,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_parent_block_id(
        parent_block_id: ParentBlockId,
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
        tracked_ext_out_messages: BTreeMap<AccountRouting, Vec<[u8; 32]>>,
        tracked_ext_out_messages_root: [u8; 32],
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
            history_proofs: BTreeMap::new(),
            tracked_ext_out_messages_root,
            tracked_ext_out_messages,
            block_keeper_set_change_proof_data: None,
        }
    }

    fn wrap_serialize(&self) -> Result<WrappedCommonSection, String> {
        let block_attestations_data = bincode::serialize(&self.block_attestations)
            .expect("Failed to serialize last finalized blocks");
        let acks_data =
            bincode::serialize(&self.acks).expect("Failed to serialize last finalized blocks");
        let nacks_data =
            bincode::serialize(&self.nacks).expect("Failed to serialize last finalized blocks");

        let builder = WrappedCommonSection::builder()
            .parent_block_id(self.parent_block_id.serialize_block_id()?)
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
            .block_height(self.block_height)
            .history_proofs(self.history_proofs.clone())
            .tracked_ext_out_messages_root(self.tracked_ext_out_messages_root)
            .tracked_ext_out_messages(self.tracked_ext_out_messages.clone())
            .block_keeper_set_change_proof_data(self.block_keeper_set_change_proof_data.clone());

        #[cfg(feature = "monitor-accounts-number")]
        let builder = builder.accounts_number_diff(self.accounts_number_diff);

        #[cfg(feature = "protocol_version_hash_in_block")]
        let builder = builder.protocol_version_hash(self.protocol_version_hash.clone());

        Ok(builder.build())
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
            .parent_block_id(ParentBlockId::Block(data.parent_block_id))
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
            .block_height(data.block_height)
            .history_proofs(data.history_proofs)
            .tracked_ext_out_messages_root(data.tracked_ext_out_messages_root)
            .tracked_ext_out_messages(data.tracked_ext_out_messages)
            .block_keeper_set_change_proof_data(data.block_keeper_set_change_proof_data);

        #[cfg(feature = "monitor-accounts-number")]
        let builder = builder.accounts_number_diff(data.accounts_number_diff);

        #[cfg(feature = "protocol_version_hash_in_block")]
        let builder = builder.protocol_version_hash(data.protocol_version_hash);

        builder.build()
    }

    pub fn full_hash_data(&self) -> Vec<u8> {
        let data = self
            .wrap_serialize()
            .expect("Parent block ID must be resolved before hash calculation");
        bincode::serialize(&data).expect("CommonSection must serialize")
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
    pub history_proofs: BTreeMap<LayerNumber, ProofLayerRootHash>,
    pub tracked_ext_out_messages_root: [u8; 32],
    pub tracked_ext_out_messages: BTreeMap<AccountRouting, Vec<[u8; 32]>>,
    pub block_keeper_set_change_proof_data: Option<BlockKeeperSetChangeProofData>,
}

impl Serialize for CommonSection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.wrap_serialize().map_err(S::Error::custom)?.serialize(serializer)
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
            .field("block_height", &self.block_height.height())
            .field("history_proofs", &self.history_proofs)
            .field(
                "tracked_ext_out_messages_root",
                &hex::encode(self.tracked_ext_out_messages_root),
            )
            .field(
                "tracked_ext_out_messages",
                &self
                    .tracked_ext_out_messages
                    .clone()
                    .into_iter()
                    .map(|(a, b)| (a, b.into_iter().map(hex::encode).collect::<Vec<_>>()))
                    .collect::<HashMap<_, _>>(),
            );

        #[cfg(feature = "monitor-accounts-number")]
        fmt.field("accounts_number_diff", &self.accounts_number_diff);
        #[cfg(feature = "protocol_version_hash_in_block")]
        fmt.field("protocol_version_hash", &self.protocol_version_hash);

        fmt.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use node_types::AccountIdentifier;
    use node_types::DAppIdentifier;

    use super::*;

    fn make_block_height() -> BlockHeight {
        BlockHeight::builder().thread_identifier(ThreadIdentifier::default()).height(7).build()
    }

    fn make_selector(parent_block_id: BlockIdentifier) -> ProducerSelector {
        ProducerSelector::builder().rng_seed_block_id(parent_block_id).index(0).build()
    }

    fn make_routing(dapp: u8, account: u8) -> AccountRouting {
        AccountIdentifier::new([account; 32]).routing(DAppIdentifier::new([dapp; 32]))
    }

    fn make_common_section(
        tracked_ext_out_messages: BTreeMap<AccountRouting, Vec<[u8; 32]>>,
    ) -> CommonSection {
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
            tracked_ext_out_messages,
            Default::default(),
        );
        common_section.set_producer_selector(Some(make_selector(parent_block_id)));
        common_section
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
            Default::default(),
            Default::default(),
        );
        common_section.set_producer_selector(Some(make_selector(parent_block_id)));

        let encoded = bincode::serialize(&common_section).unwrap();
        let decoded: CommonSection = bincode::deserialize(&encoded).unwrap();

        assert_eq!(decoded.parent_block_id().block_id(), Some(&parent_block_id));
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
            Default::default(),
            Default::default(),
        );
        common_section.set_producer_selector(Some(make_selector(parent_block_id)));

        let encoded = bincode::serialize(&common_section).unwrap();
        let decoded: CommonSection = bincode::deserialize(&encoded).unwrap();

        assert_eq!(*decoded.parent_block_id().expect_block_id(), BlockIdentifier::default());
    }

    #[test]
    fn unresolved_parent_block_id_cannot_serialize() {
        let mut common_section = CommonSection::new_with_parent_block_id(
            ParentBlockId::Unresolved(TemporaryBlockId::generate()),
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
            Default::default(),
            Default::default(),
        );
        common_section.set_producer_selector(Some(make_selector(BlockIdentifier::default())));

        let err = bincode::serialize(&common_section).unwrap_err();
        assert!(err.to_string().contains("unresolved parent block ID"));
    }

    #[test]
    fn common_section_hash_data_is_stable_for_tracked_message_insertion_order() {
        let first = make_routing(1, 1);
        let second = make_routing(2, 2);

        let mut forward = BTreeMap::new();
        forward.insert(first, vec![[0x11; 32], [0x12; 32]]);
        forward.insert(second, vec![[0x21; 32]]);

        let mut reverse = BTreeMap::new();
        reverse.insert(second, vec![[0x21; 32]]);
        reverse.insert(first, vec![[0x11; 32], [0x12; 32]]);

        assert_eq!(
            make_common_section(forward).full_hash_data(),
            make_common_section(reverse).full_hash_data()
        );
    }
}
