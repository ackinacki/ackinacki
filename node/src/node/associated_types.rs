// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use anyhow::anyhow;
use serde::Deserialize;
use serde::Serialize;
use tvm_types::Sha256;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::GoshBLS;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::node::UInt256;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;
use crate::types::block_keeper_ring::BlockKeeperRing;
use crate::types::AccountAddress;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;

pub type SignerIndex = u16;

// Note: making it compatible with the inner type of the TVM BlockProducer
pub type NodeIdentifier = i32;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Serialize, Deserialize)]
pub enum NackReason {
    SameHeightBlock {
        first_envelope: Envelope<GoshBLS, AckiNackiBlock>,
        second_envelope: Envelope<GoshBLS, AckiNackiBlock>,
    },
    BadBlock {
        envelope: Envelope<GoshBLS, AckiNackiBlock>,
    },
    WrongNack {
        nack_data_envelope: Arc<Envelope<GoshBLS, NackData>>,
    },
}

impl NackReason {
    pub fn get_hash_nack(&self) -> anyhow::Result<UInt256> {
        match self {
            NackReason::SameHeightBlock { first_envelope, second_envelope } => {
                let mut hasher = Sha256::new();
                hasher.update(first_envelope.data().get_hash());
                hasher.update(second_envelope.data().get_hash());
                let result_hash = hasher.finalize();
                let combined_hash: [u8; 32] = result_hash;
                Ok(combined_hash.into())
            }
            NackReason::BadBlock { envelope } => Ok(envelope.data().get_hash().into()),
            NackReason::WrongNack { nack_data_envelope: _ } => {
                tracing::trace!("WrongNack nack");
                Err(anyhow!("No allow WrongNack"))
            }
        }
    }

    pub fn get_node_data(
        &self,
        block_keeper_sets: BlockKeeperRing,
    ) -> Option<(i32, PubKey, AccountAddress)> {
        let nack_target_node_id;
        let nack_key;
        let nack_wallet_addr;
        match self {
            NackReason::SameHeightBlock { first_envelope, second_envelope: _ } => {
                let block_seq_no = first_envelope.data().parent_seq_no();
                nack_target_node_id = first_envelope.data().get_common_section().producer_id;
                // TODO: think of possible attacks base on impossibility of finding BK key
                let bk_set = block_keeper_sets.get_block_keeper_data(&block_seq_no);
                if let Some(data) = bk_set.get(&(nack_target_node_id as u16)) {
                    nack_key = data.pubkey.clone();
                    nack_wallet_addr = data.owner_address.clone();
                    return Some((nack_target_node_id, nack_key, nack_wallet_addr));
                }
            }
            NackReason::BadBlock { envelope } => {
                nack_target_node_id = envelope.data().get_common_section().producer_id;
                let block_seq_no = envelope.data().parent_seq_no();
                // TODO: think of possible attacks base on impossibility of finding BK key
                let bk_set = block_keeper_sets.get_block_keeper_data(&block_seq_no);
                if let Some(data) = bk_set.get(&(nack_target_node_id as u16)) {
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
                    nack_data_envelope.data().reason.get_node_data(block_keeper_sets)
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
            NackReason::SameHeightBlock { first_envelope: block1, second_envelope: block2 } => {
                format!("SameHeightBlock, {:?}, {:?}", block1, block2)
            }
            NackReason::BadBlock { envelope: block } => {
                format!("BadBlock {:?}", block)
            }
            NackReason::WrongNack { nack_data_envelope: nack } => {
                format!("nack {:?}", nack)
            }
        };
        Display::fmt(&data, f)
    }
}

pub(crate) type OptimisticStateFor<TBlockProducerProcess> =
    <TBlockProducerProcess as BlockProducerProcess>::OptimisticState;

pub(crate) trait NodeAssociatedTypes {
    type CandidateBlock: BLSSignedEnvelope;
    type Ack: BLSSignedEnvelope;
    type Nack: BLSSignedEnvelope;
    type NetworkMessage;
    type BlockAttestation: BLSSignedEnvelope;
}

impl<TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
NodeAssociatedTypes
for Node<
    TStateSyncService,
    TBlockProducerProcess,
    TValidationProcess,
    TRepository,
    TAttestationProcessor,
    TRandomGenerator,
>
    where
        TBlockProducerProcess: BlockProducerProcess,
        TValidationProcess: BlockKeeperProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,

            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = GoshBLS,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,

        >,
        TRepository: Repository<
            BLS = GoshBLS,
            EnvelopeSignerIndex = SignerIndex,

            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<GoshBLS, AttestationData>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = TRepository
        >,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<GoshBLS, AttestationData>,
            CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>,
        >,
        TRandomGenerator: rand::Rng,
{
    type CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>;
    type Ack = Envelope<GoshBLS, AckData>;
    type Nack = Envelope<GoshBLS, NackData>;
    type NetworkMessage = NetworkMessage<
        GoshBLS,
        AckData,
        NackData,
        AttestationData,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
    >;
    type BlockAttestation = Envelope<GoshBLS, AttestationData>;
}

pub(crate) enum OptimisticForwardState {
    ProducingNextBlock(BlockIdentifier, BlockSeqNo),
    ProducedBlock(BlockIdentifier, BlockSeqNo),
    None,
}

impl Default for OptimisticForwardState {
    fn default() -> Self {
        Self::None
    }
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

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct AckData {
    pub block_id: BlockIdentifier,
    pub block_seq_no: BlockSeqNo,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NackData {
    pub block_id: BlockIdentifier,
    pub block_seq_no: BlockSeqNo,
    pub reason: NackReason,
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct AttestationData {
    pub block_id: BlockIdentifier,
    pub block_seq_no: BlockSeqNo,
}
