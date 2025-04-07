// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use super::fork_resolution::ForkResolution;
use crate::block_keeper_system::BlockKeeperSetChange;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::types::bp_selector::ProducerSelector;
use crate::types::AccountAddress;
use crate::types::BlockEndLT;
use crate::types::BlockIdentifier;
use crate::types::DAppIdentifier;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;

#[derive(Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Directives {
    pub share_state_resource_address: Option<String>,
}

impl Debug for Directives {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("")
            .field("share_state_resource_address", &self.share_state_resource_address)
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct CommonSection {
    pub directives: Directives,
    pub block_attestations: Vec<Envelope<GoshBLS, AttestationData>>,
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

    // Each ForkResolution contains a resolution for a single fork.
    // It can happen that a single block has to resolve several forks at once.
    pub fork_resolutions: Vec<ForkResolution>,

    pub changed_dapp_ids: HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)>,
}

impl CommonSection {
    pub fn new(
        thread_id: ThreadIdentifier,
        producer_id: NodeIdentifier,
        block_keeper_set_changes: Vec<BlockKeeperSetChange>,
        verify_complexity: SignerIndex,
        refs: Vec<BlockIdentifier>,
        threads_table: Option<ThreadsTable>,
        changed_dapp_ids: HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)>,
    ) -> Self {
        CommonSection {
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
            fork_resolutions: vec![],
            changed_dapp_ids,
        }
    }

    fn wrap_serialize(&self) -> WrappedCommonSection {
        let block_attestations_data = bincode::serialize(&self.block_attestations)
            .expect("Failed to serialize last finalized blocks");
        let acks_data =
            bincode::serialize(&self.acks).expect("Failed to serialize last finalized blocks");
        let nacks_data =
            bincode::serialize(&self.nacks).expect("Failed to serialize last finalized blocks");
        WrappedCommonSection {
            directives: self.directives.clone(),
            block_attestations: block_attestations_data,
            producer_id: self.producer_id.clone(),
            block_keeper_set_changes: self.block_keeper_set_changes.clone(),
            verify_complexity: self.verify_complexity,
            acks: acks_data,
            nacks: nacks_data,
            producer_selector: self
                .producer_selector
                .clone()
                .expect("Producer selector must be set before serialization"),
            thread_identifier: self.thread_id,
            refs: self.refs.clone(),
            threads_table: self.threads_table.clone(),
            fork_resolutions: self.fork_resolutions.clone(),
            changed_dapp_ids: self.changed_dapp_ids.clone(),
        }
    }

    fn wrap_deserialize(data: WrappedCommonSection) -> Self {
        let block_attestations: Vec<Envelope<GoshBLS, AttestationData>> =
            bincode::deserialize(&data.block_attestations)
                .expect("Failed to deserialize block attestations");
        let acks: Vec<Envelope<GoshBLS, AckData>> =
            bincode::deserialize(&data.acks).expect("Failed to deserialize acks");
        let nacks: Vec<Envelope<GoshBLS, NackData>> =
            bincode::deserialize(&data.nacks).expect("Failed to deserialize nacks");
        Self {
            directives: data.directives,
            block_attestations,
            producer_id: data.producer_id,
            block_keeper_set_changes: data.block_keeper_set_changes,
            verify_complexity: data.verify_complexity,
            acks,
            nacks,
            producer_selector: Some(data.producer_selector),
            refs: data.refs,
            thread_id: data.thread_identifier,
            threads_table: data.threads_table,
            fork_resolutions: data.fork_resolutions,
            changed_dapp_ids: data.changed_dapp_ids,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct WrappedCommonSection {
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
    pub fork_resolutions: Vec<ForkResolution>,
    pub changed_dapp_ids: HashMap<AccountAddress, (Option<DAppIdentifier>, BlockEndLT)>,
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
        f.debug_struct("")
            .field("thread_id", &self.thread_id)
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
            .field("changed_dapp_ids.len", &self.changed_dapp_ids.len())
            .field("fork_resolutions", &self.fork_resolutions)
            .finish()
    }
}
