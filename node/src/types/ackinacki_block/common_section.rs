// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use derive_getters::Getters;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use typed_builder::TypedBuilder;

use crate::block_keeper_system::BlockKeeperSetChange;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::types::bp_selector::ProducerSelector;
use crate::types::BlockHeight;
use crate::types::BlockIdentifier;
use crate::types::BlockRound;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;
#[cfg(feature = "protocol_version_hash_in_block")]
use crate::versioning::protocol_version::ProtocolVersionHash;

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq, TypedBuilder, Getters)]
pub struct Directives {
    share_state_resources: Option<HashMap<ThreadIdentifier, BlockIdentifier>>,
}

impl Debug for Directives {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("").field("share_state_resources", &self.share_state_resources).finish()
    }
}

#[cfg_attr(feature = "transitioning_node_version", versioned)]
#[derive(Clone, PartialEq, Eq, TypedBuilder)]
pub struct CommonSection {
    pub block_height: BlockHeight,
    pub directives: Directives,
    // TODO: we assume that all attestations are aggregated and there are no duplicates on (block_id, attestation_target_type)
    // otherwise it may break block processing cycle
    pub block_attestations: Vec<Envelope<GoshBLS, AttestationData>>,
    pub round: BlockRound,
    pub producer_id: NodeIdentifier,
    pub thread_id: ThreadIdentifier,
    pub threads_table: Option<ThreadsTable>,
    /// Extra references this block depends on.
    /// This can be any combination of:
    /// - sources of internal messages from another thread
    /// - source of an update of the threads state
    /// - source of updates for account migrations due to a new dapp set.
    /// - (?)
    pub refs: Vec<BlockIdentifier>,
    pub block_keeper_set_changes: Vec<BlockKeeperSetChange>,
    // Dynamic parameter: an expected number of Acki-Nacki for this block
    pub verify_complexity: SignerIndex,
    pub acks: Vec<Envelope<GoshBLS, AckData>>,
    pub nacks: Vec<Envelope<GoshBLS, NackData>>,
    // This field must be set, but it is option, because we can't set it up on block creation and update it later
    pub producer_selector: Option<ProducerSelector>,

    #[cfg(feature = "monitor-accounts-number")]
    pub accounts_number_diff: i64,

    #[cfg(feature = "protocol_version_hash_in_block")]
    protocol_version_hash: ProtocolVersionHash,
}

#[cfg(feature = "transitioning_node_version")]
impl Transitioning for CommonSection {
    type Old = CommonSectionOld;

    fn from(old: Self::Old) -> Self {
        todo!() // should be set for a particular transitioning version if required
    }
}

impl CommonSection {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        thread_id: ThreadIdentifier,
        round: BlockRound,
        producer_id: NodeIdentifier,
        block_keeper_set_changes: Vec<BlockKeeperSetChange>,
        verify_complexity: SignerIndex,
        refs: Vec<BlockIdentifier>,
        threads_table: Option<ThreadsTable>,
        block_height: BlockHeight,
        #[cfg(feature = "monitor-accounts-number")] accounts_number_diff: i64,
        #[cfg(feature = "protocol_version_hash_in_block")]
        protocol_version_hash: ProtocolVersionHash,
    ) -> Self {
        CommonSection {
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

        #[cfg(feature = "monitor-accounts-number")]
        let builder = builder.accounts_number_diff(self.accounts_number_diff);

        #[cfg(feature = "protocol_version_hash_in_block")]
        let builder = builder.protocol_version_hash(self.protocol_version_hash.clone());

        builder.build()
    }

    fn wrap_deserialize(data: WrappedCommonSection) -> Self {
        let block_attestations: Vec<Envelope<GoshBLS, AttestationData>> =
            bincode::deserialize(&data.block_attestations)
                .expect("Failed to deserialize block attestations");
        let acks: Vec<Envelope<GoshBLS, AckData>> =
            bincode::deserialize(&data.acks).expect("Failed to deserialize acks");
        let nacks: Vec<Envelope<GoshBLS, NackData>> =
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
        #[cfg(feature = "monitor-accounts-number")]
        let builder = builder.accounts_number_diff(data.accounts_number_diff);

        #[cfg(feature = "protocol_version_hash_in_block")]
        let builder = builder.protocol_version_hash(data.protocol_version_hash);

        builder.build()
    }

    pub fn set_acks(&mut self, acks: Vec<Envelope<GoshBLS, AckData>>) {
        self.acks = acks;
    }

    pub fn set_nacks(&mut self, nacks: Vec<Envelope<GoshBLS, NackData>>) {
        self.nacks = nacks;
    }

    pub fn set_block_attestations(
        &mut self,
        block_attestations: Vec<Envelope<GoshBLS, AttestationData>>,
    ) {
        self.block_attestations = block_attestations;
    }

    pub fn set_producer_selector(&mut self, producer_selector: Option<ProducerSelector>) {
        self.producer_selector = producer_selector;
    }

    pub fn set_directives(&mut self, directives: Directives) {
        self.directives = directives;
    }

    pub fn set_threads_table(&mut self, threads_table: Option<ThreadsTable>) {
        self.threads_table = threads_table;
    }

    pub fn round(&self) -> u64 {
        self.round
    }

    pub fn producer_selector(&self) -> Option<ProducerSelector> {
        self.producer_selector.clone()
    }

    pub fn block_attestations(&self) -> Vec<Envelope<GoshBLS, AttestationData>> {
        self.block_attestations.clone()
    }

    pub fn producer_id(&self) -> NodeIdentifier {
        self.producer_id.clone()
    }

    pub fn thread_id(&self) -> ThreadIdentifier {
        self.thread_id
    }

    pub fn directives(&self) -> Directives {
        self.directives.clone()
    }
}

#[derive(TypedBuilder, Serialize, Deserialize)]
struct WrappedCommonSection {
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
    pub threads_table: Option<ThreadsTable>,
    pub block_height: BlockHeight,
    #[cfg(feature = "monitor-accounts-number")]
    pub accounts_number_diff: i64,
    #[cfg(feature = "protocol_version_hash_in_block")]
    pub protocol_version_hash: ProtocolVersionHash,
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
            .field("threads_table", &self.threads_table);

        #[cfg(feature = "monitor-accounts-number")]
        fmt.field("accounts_number_diff", &self.accounts_number_diff);
        #[cfg(feature = "protocol_version_hash_in_block")]
        fmt.field("protocol_version_hash", &self.protocol_version_hash);

        fmt.finish()
    }
}
