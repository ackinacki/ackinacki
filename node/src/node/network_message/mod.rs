// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::bls;
use crate::bls::envelope::Envelope;

mod serde_network_message;

#[derive(Clone)]
pub enum NetworkMessage<
    BLS,
    TBlock,
    TAck,
    TNack,
    TAttestation,
    TExternalMessage,
    TBlockIdentifier,
    TBlockSeqNo,
    TNodeIdentifier,
> where
    BLS: bls::BLSSignatureScheme,
    BLS::Signature: Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TBlock: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAck: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNack: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAttestation: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TExternalMessage: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockSeqNo: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNodeIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    //  SelectedAsBlockProducerForThread(ThreadId),
    //  DeseletedAsBlockProducerForThread(ThreadId),
    Candidate(Envelope<BLS, TBlock>),
    Ack(Envelope<BLS, TAck>),
    Nack(Envelope<BLS, TNack>),
    ExternalMessage(TExternalMessage),
    NodeJoining(TNodeIdentifier),
    BlockAttestation(Envelope<BLS, TAttestation>),
    // TODO need to remake not seq_no but identifier
    BlockRequest((TBlockSeqNo, TBlockSeqNo, TNodeIdentifier)),
    SyncFrom(TBlockSeqNo),
    SyncFinalized((TBlockIdentifier, TBlockSeqNo, String)),
}

impl<
        BLS,
        TBlock,
        TAck,
        TNack,
        TAttestation,
        TExternalMessage,
        TBlockIdentifier,
        TBlockSeqNo,
        TNodeIdentifier,
    > Debug
    for NetworkMessage<
        BLS,
        TBlock,
        TAck,
        TNack,
        TAttestation,
        TExternalMessage,
        TBlockIdentifier,
        TBlockSeqNo,
        TNodeIdentifier,
    >
where
    BLS: bls::BLSSignatureScheme,
    BLS::Signature: Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    TBlock: Display + Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAck: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNack: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAttestation: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TExternalMessage: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockSeqNo: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNodeIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use NetworkMessage::*;
        let enum_type = match self {
            Candidate(block) => &format!("Candidate {block}"),
            Ack(_) => "Ack",
            Nack(_) => "Nack",
            ExternalMessage(_) => "ExternalMessage",
            NodeJoining(_) => "NodeJoining",
            BlockAttestation(_) => "BlockAttestation",
            BlockRequest(_) => "BlockRequest",
            SyncFinalized(_) => "SyncFinalized",
            SyncFrom(_) => "SyncFrom",
        };
        write!(f, "NetworkMessage::{}", enum_type)
    }
}
