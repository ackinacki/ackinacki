// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Display;
use std::hash::Hash;

use serde::Deserialize;
use serde::Serialize;

use crate::block::keeper::process::BlockKeeperProcess;
use crate::block::producer::process::BlockProducerProcess;
use crate::block::producer::BlockProducer;
use crate::block::Block;
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

pub type SignerIndex = u16;

// Note: making it compatible with the inner type of the TVM BlockProducer
pub type NodeIdentifier = i32;

pub(crate) type BlockSeqNoFor<TBlockProducerProcess> =
<<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block as Block>::BlockSeqNo;

pub(crate) type BlockIdentifierFor<TBlockProducerProcess> =
    <TBlockProducerProcess as BlockProducerProcess>::BlockIdentifier;

pub(crate) type ThreadIdentifierFor<TBlockProducerProcess> =
<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::ThreadIdentifier;

pub(crate) type BlockFor<TBlockProducerProcess> =
    <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block;

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
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block:
        Block<BlockIdentifier = BlockIdentifierFor<TBlockProducerProcess>>,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block:
        Block<BLSSignatureScheme = TBLSSignatureScheme>,
        <<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block as Block>::BlockSeqNo:
        Eq + Hash,
        ThreadIdentifierFor<TBlockProducerProcess>: Default,
        BlockFor<TBlockProducerProcess>: Clone + Display,
        BlockIdentifierFor<TBlockProducerProcess>: Serialize + for<'de> Deserialize<'de>,
        TValidationProcess: BlockKeeperProcess<
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            Block = BlockFor<TBlockProducerProcess>,
            BlockSeqNo = BlockSeqNoFor<TBlockProducerProcess>,
            BlockIdentifier = BlockIdentifierFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: BlockProducerProcess<
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            Block = BlockFor<TBlockProducerProcess>,
        >,
        TRepository: Repository<
            BLS = TBLSSignatureScheme,
            EnvelopeSignerIndex = SignerIndex,
            ThreadIdentifier = ThreadIdentifierFor<TBlockProducerProcess>,
            Block = BlockFor<TBlockProducerProcess>,
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
            OptimisticState = OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = NodeIdentifier,
            Attestation = Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message: Into<
            <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Message,
        >,
        TStateSyncService: StateSyncService,
        TAttestationProcessor: AttestationProcessor<
            BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>,
            CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>,
        >,
        <<TBlockProducerProcess as BlockProducerProcess>::OptimisticState as OptimisticState>::Block: From<<<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Block>,
        TRandomGenerator: rand::Rng,
{
    type CandidateBlock = Envelope<TBLSSignatureScheme, BlockFor<TBlockProducerProcess>>;
    type Ack = Envelope<TBLSSignatureScheme, AckData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>;
    type Nack = Envelope<TBLSSignatureScheme, NackData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>;
    type NetworkMessage = NetworkMessage<
        TBLSSignatureScheme,
        BlockFor<TBlockProducerProcess>,
        AckData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
        NackData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
        AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>,
        <<TBlockProducerProcess as BlockProducerProcess>::BlockProducer as BlockProducer>::Message,
        BlockIdentifierFor<TBlockProducerProcess>,
        BlockSeqNoFor<TBlockProducerProcess>,
        NodeIdentifier,
    >;
    type BlockAttestation = Envelope<TBLSSignatureScheme, AttestationData<BlockIdentifierFor<TBlockProducerProcess>, BlockSeqNoFor<TBlockProducerProcess>>>;
}

pub(crate) enum OptimisticForwardState<TBlockIdentifier: Clone + Sized, TBlockSeqNo: Clone + Sized>
{
    ProducingNextBlock(TBlockIdentifier, TBlockSeqNo),
    ProducedBlock(TBlockIdentifier, TBlockSeqNo),
    None,
}

impl<TBlockIdentifier, TBlockSeqNo> Default
    for OptimisticForwardState<TBlockIdentifier, TBlockSeqNo>
where
    TBlockIdentifier: Clone + Sized,
    TBlockSeqNo: Clone + Sized,
{
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
pub struct AckData<TBlockId, TBlockSeqNo> {
    pub block_id: TBlockId,
    pub block_seq_no: TBlockSeqNo,
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct NackData<TBlockId, TBlockSeqNo> {
    pub block_id: TBlockId,
    pub block_seq_no: TBlockSeqNo,
}

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct AttestationData<TBlockId, TBlockSeqNo> {
    pub block_id: TBlockId,
    pub block_seq_no: TBlockSeqNo,
}
