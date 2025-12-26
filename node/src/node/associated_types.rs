// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use derive_getters::Getters;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use typed_builder::TypedBuilder;

use super::block_request_service::BlockRequestService;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::GoshBLS;
use crate::node::services::sync::StateSyncService;
use crate::node::BlockStateRepository;
use crate::node::Node;
use crate::node::UInt256;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::envelope_hash::AckiNackiEnvelopeHash;
use crate::types::AccountAddress;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::Guarded;
use crate::versioning::ProtocolVersionSupport;

pub type SignerIndex = u16;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct NodeIdentifier(AccountAddress);

impl NodeIdentifier {
    #[cfg(any(test, feature = "nack_test"))]
    pub fn some_id() -> Self {
        Self::from_str("81a6bea128f5e03843362e55fd574c42a8e457dd553498cbc8ec7e14966d20a3").unwrap()
    }
}

#[derive(Debug, Clone, TypedBuilder, Getters)]
pub struct NodeCredentials {
    node_id: NodeIdentifier,
    protocol_version_support: ProtocolVersionSupport,
}

impl NodeIdentifier {
    #[cfg(any(test, feature = "nack_test"))]
    pub fn test(seed: usize) -> NodeIdentifier {
        AccountAddress(UInt256::from_be_bytes(&seed.to_be_bytes())).into()
    }
}

impl Serialize for NodeIdentifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for NodeIdentifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let str = String::deserialize(deserializer)?;
        Self::from_str(&str).map_err(serde::de::Error::custom)
    }
}

impl From<AccountAddress> for NodeIdentifier {
    fn from(value: AccountAddress) -> Self {
        Self(value)
    }
}

impl From<tvm_types::AccountId> for NodeIdentifier {
    fn from(value: tvm_types::AccountId) -> Self {
        Self(AccountAddress::from(value))
    }
}

impl From<NodeIdentifier> for AccountAddress {
    fn from(value: NodeIdentifier) -> Self {
        value.0
    }
}

impl FromStr for NodeIdentifier {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> anyhow::Result<Self> {
        Ok(Self(value.parse().map_err(|err| anyhow!("Invalid node identifier: {err}"))?))
    }
}

impl Display for NodeIdentifier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.to_hex_string())
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum NackReason {
    // SameHeightBlock {
    // first_envelope: Envelope<GoshBLS, AckiNackiBlock>,
    // second_envelope: Envelope<GoshBLS, AckiNackiBlock>,
    // },
    BadBlock { envelope: Envelope<GoshBLS, AckiNackiBlock> },
    TooComplexExecution { envelope: Envelope<GoshBLS, AckiNackiBlock> },
    WrongNack { nack_data_envelope: Arc<Envelope<GoshBLS, NackData>> },
}

impl NackReason {
    pub fn get_hash_nack(&self) -> anyhow::Result<UInt256> {
        // TODO: revise this code, Nack hash seems to be unused
        match self {
            // NackReason::SameHeightBlock { first_envelope, second_envelope } => {
            // let mut hasher = Sha256::new();
            // hasher.update(first_envelope.data().get_hash());
            // hasher.update(second_envelope.data().get_hash());
            // let result_hash = hasher.finalize();
            // let combined_hash: [u8; 32] = result_hash;
            // Ok(combined_hash.into())
            // }
            NackReason::TooComplexExecution { envelope } => Ok(envelope.data().get_hash().into()),
            NackReason::BadBlock { envelope } => Ok(envelope.data().get_hash().into()),
            NackReason::WrongNack { nack_data_envelope: _ } => {
                tracing::trace!("WrongNack nack");
                Err(anyhow!("No allow WrongNack"))
            }
        }
    }

    pub fn get_node_data(
        &self,
        block_state_repository: BlockStateRepository,
    ) -> Option<(NodeIdentifier, PubKey, AccountAddress)> {
        let nack_target_node_id;
        let nack_key;
        let nack_wallet_addr;
        match self {
            /*
            NackReason::SameHeightBlock { first_envelope, second_envelope: _ } => {
                nack_target_node_id =
                    first_envelope.data().get_common_section().producer_id.clone();
                // TODO: think of possible attacks base on impossibility of finding BK key
                let state = block_state_repository.get(&first_envelope.data().parent()).unwrap();
                let state_in = state.lock();
                let bk_set = state_in.bk_set().clone().unwrap();
                drop(state_in);
                if let Some(data) = bk_set.get_by_node_id(&nack_target_node_id) {
                    nack_key = data.pubkey.clone();
                    nack_wallet_addr = data.owner_address.clone();
                    return Some((nack_target_node_id, nack_key, nack_wallet_addr));
                }
            }
            */
            NackReason::TooComplexExecution { envelope } => {
                nack_target_node_id = envelope.data().get_common_section().producer_id.clone();
                // TODO: think of possible attacks base on impossibility of finding BK key
                let state = block_state_repository.get(&envelope.data().parent()).unwrap();
                let bk_set = state.guarded(|e| e.bk_set().clone()).unwrap();
                if let Some(data) = bk_set.get_by_node_id(&nack_target_node_id) {
                    nack_key = data.pubkey.clone();
                    nack_wallet_addr = data.owner_address.clone();
                    return Some((nack_target_node_id, nack_key, nack_wallet_addr));
                }
            }
            NackReason::BadBlock { envelope } => {
                nack_target_node_id = envelope.data().get_common_section().producer_id.clone();
                // TODO: think of possible attacks base on impossibility of finding BK key
                let state = block_state_repository.get(&envelope.data().parent()).unwrap();
                let bk_set = state.guarded(|e| e.bk_set().clone()).unwrap();
                if let Some(data) = bk_set.get_by_node_id(&nack_target_node_id) {
                    nack_key = data.pubkey.clone();
                    nack_wallet_addr = data.owner_address.clone();
                    return Some((nack_target_node_id, nack_key, nack_wallet_addr));
                }
            }
            NackReason::WrongNack { nack_data_envelope } => {
                if let NackReason::WrongNack { nack_data_envelope: _ } =
                    nack_data_envelope.data().reason
                {
                    return None;
                }
                if let Some((nack_target_node_id, nack_key, nack_wallet_addr)) =
                    nack_data_envelope.data().reason.get_node_data(block_state_repository)
                {
                    return Some((nack_target_node_id, nack_key, nack_wallet_addr));
                }
            }
        };
        None
    }
}

impl Debug for NackReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let data = match self {
            // NackReason::SameHeightBlock { first_envelope: block1, second_envelope: block2 } => {
            // format!("SameHeightBlock, {:?}, {:?}", block1, block2)
            // }
            NackReason::BadBlock { envelope: block } => {
                format!("BadBlock {block:?}")
            }
            NackReason::TooComplexExecution { envelope } => {
                format!("TooComplexExecution {envelope:?}")
            }
            NackReason::WrongNack { nack_data_envelope: nack } => {
                format!("nack {nack:?}")
            }
        };
        Display::fmt(&data, f)
    }
}

pub(crate) trait NodeAssociatedTypes {
    type CandidateBlock: BLSSignedEnvelope;
    type Ack: BLSSignedEnvelope;
    type Nack: BLSSignedEnvelope;
    type BlockAttestation: BLSSignedEnvelope;
}

impl<TStateSyncService, TRandomGenerator> NodeAssociatedTypes
    for Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    type Ack = Envelope<GoshBLS, AckData>;
    type BlockAttestation = Envelope<GoshBLS, AttestationData>;
    type CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>;
    type Nack = Envelope<GoshBLS, NackData>;
}

impl NodeAssociatedTypes for BlockRequestService {
    type Ack = Envelope<GoshBLS, AckData>;
    type BlockAttestation = Envelope<GoshBLS, AttestationData>;
    type CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>;
    type Nack = Envelope<GoshBLS, NackData>;
}

/// Result of node synchronization process
#[derive(Debug)]
pub(crate) enum SynchronizationResult<TNetworkMessage: Sized> {
    /// Synchronization process finished successfully.
    Ok,
    /// Synchronization process finished successfully and returned a block to
    /// continue on.
    Forward(TNetworkMessage),
    /// Node rx channel has become disconnected, that means there will never be
    /// any more messages received on it and synchronization process can't be
    /// finished.
    Interrupted,
}

/// Result of node execution process
#[derive(Debug)]
pub(crate) enum ExecutionResult {
    /// Node has received a valid block that has a big difference in seq no with
    /// the latest processed block. It means that node has possibly lost too
    /// many blocks and needs to perform synchronization.
    SynchronizationRequired,
    /// Node rx channel has become disconnected, that means there will never be
    /// any more messages received on it.
    Disconnected,
}

/// Block processing status
#[derive(PartialEq, Debug)]
#[allow(dead_code)]
pub(crate) enum BlockStatus {
    /// Block was successfully processed.
    Ok,
    /// Block was skipped, because it is too old.
    Skipped,
    /// Block hash or signatures check failed.
    BadBlock,
    /// Received block can't be applied.
    BlockCantBeApplied,
    /// Block processing triggerred synchronization,
    SynchronizationRequired,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct AckData {
    pub block_id: BlockIdentifier,
    pub block_seq_no: BlockSeqNo,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NackData {
    pub block_id: BlockIdentifier,
    pub block_seq_no: BlockSeqNo,
    pub reason: NackReason,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(u8)]
pub enum AttestationTargetType {
    Primary,
    Fallback,
}

#[derive(TypedBuilder, Eq, PartialEq, Debug, Clone, Serialize, Deserialize, Getters)]
pub struct AttestationData {
    parent_block_id: BlockIdentifier,
    block_id: BlockIdentifier,
    block_seq_no: BlockSeqNo,
    envelope_hash: AckiNackiEnvelopeHash,
    target_type: AttestationTargetType,
}

#[derive(TypedBuilder, Deserialize, Debug, Clone, Serialize, Getters)]
pub struct SyncFinalizedData {
    block_identifier: BlockIdentifier,
    block_seq_no: BlockSeqNo,
    thread_refs: BTreeMap<ThreadIdentifier, BlockIdentifier>,
}
