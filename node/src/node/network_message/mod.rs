// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::bls;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

mod serde_network_message;

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkMessage<BLS, TAck, TNack, TAttestation, TExternalMessage, TNodeIdentifier>
where
    BLS: bls::BLSSignatureScheme,
    BLS::Signature: Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TAck: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNack: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAttestation: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TExternalMessage: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNodeIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    //  SelectedAsBlockProducerForThread(ThreadId),
    //  DeseletedAsBlockProducerForThread(ThreadId),
    Candidate(Envelope<BLS, AckiNackiBlock<BLS>>),
    Ack((Envelope<BLS, TAck>, ThreadIdentifier)),
    Nack((Envelope<BLS, TNack>, ThreadIdentifier)),
    ExternalMessage(TExternalMessage),
    NodeJoining((TNodeIdentifier, ThreadIdentifier)),
    BlockAttestation((Envelope<BLS, TAttestation>, ThreadIdentifier)),
    // TODO need to remake not seq_no but identifier
    // TODO: consider std::ops::Range<BlockSeqNo>
    BlockRequest((BlockSeqNo, BlockSeqNo, TNodeIdentifier, ThreadIdentifier)),
    SyncFrom((BlockSeqNo, ThreadIdentifier)),
    SyncFinalized((BlockIdentifier, BlockSeqNo, String, ThreadIdentifier)),
}

impl<BLS, TAck, TNack, TAttestation, TExternalMessage, TNodeIdentifier> Debug
    for NetworkMessage<BLS, TAck, TNack, TAttestation, TExternalMessage, TNodeIdentifier>
where
    BLS: bls::BLSSignatureScheme,
    BLS::Signature: Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TAck: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNack: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAttestation: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TExternalMessage: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + Debug + 'static,
    TNodeIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use NetworkMessage::*;
        let enum_type = match self {
            Candidate(block) => {
                &format!("Candidate ({:?}, {:?})", block.data().seq_no(), block.data().identifier())
            }
            Ack(_) => "Ack",
            Nack(_) => "Nack",
            ExternalMessage(msg) => &format!("ExternalMessage: {:?}", msg),
            NodeJoining(_) => "NodeJoining",
            BlockAttestation(_) => "BlockAttestation",
            BlockRequest(_) => "BlockRequest",
            SyncFinalized(_) => "SyncFinalized",
            SyncFrom(_) => "SyncFrom",
        };
        write!(f, "NetworkMessage::{}", enum_type)
    }
}
