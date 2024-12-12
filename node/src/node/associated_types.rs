// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use serde::Deserialize;
use serde::Serialize;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::BLSSignatureScheme;
use crate::node::attestation_processor::AttestationProcessor;
use crate::node::services::sync::StateSyncService;
use crate::node::NetworkMessage;
use crate::node::Node;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;

pub type SignerIndex = u16;

// Note: making it compatible with the inner type of the TVM BlockProducer
pub type NodeIdentifier = i32;

pub(crate) type OptimisticStateFor<TBlockProducerProcess> =
    <TBlockProducerProcess as BlockProducerProcess>::OptimisticState;

pub(crate) trait NodeAssociatedTypes {
    type CandidateBlock: BLSSignedEnvelope;
    type Ack: BLSSignedEnvelope;
    type Nack: BLSSignedEnvelope;
    type NetworkMessage;
    type BlockAttestation: BLSSignedEnvelope;
}

impl<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
NodeAssociatedTypes
for Node<
    TBLSSignatureScheme,
    TStateSyncService,
    TBlockProducerProcess,
    TValidationProcess,
    TRepository,
    TAttestationProcessor,
    TRandomGenerator,
>
    where
        TBLSSignatureScheme: BLSSignatureScheme<PubKey = PubKey> + Clone,
        <TBLSSignatureScheme as BLSSignatureScheme>::PubKey: PartialEq,
        TBlockProducerProcess: BlockProducerProcess,
        TValidationProcess: BlockKeeperProcess<
            BLSSignatureScheme = TBLSSignatureScheme,
            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,

            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            BLSSignatureScheme = TBLSSignatureScheme,
            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,

        >,
        TRepository: Repository<
            BLS = TBLSSignatureScheme,
            EnvelopeSignerIndex = SignerIndex,

            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<TBLSSignatureScheme, AttestationData>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService<
            Repository = TRepository
        >,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData>,
            CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>,
        >,
        TRandomGenerator: rand::Rng,
{
    type CandidateBlock = Envelope<TBLSSignatureScheme, AckiNackiBlock<TBLSSignatureScheme>>;
    type Ack = Envelope<TBLSSignatureScheme, AckData>;
    type Nack = Envelope<TBLSSignatureScheme, NackData>;
    type NetworkMessage = NetworkMessage<
        TBLSSignatureScheme,
        AckData,
        NackData,
        AttestationData,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
        NodeIdentifier,
    >;
    type BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData>;
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

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct NackData {
    pub block_id: BlockIdentifier,
    pub block_seq_no: BlockSeqNo,
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct AttestationData {
    pub block_id: BlockIdentifier,
    pub block_seq_no: BlockSeqNo,
}
