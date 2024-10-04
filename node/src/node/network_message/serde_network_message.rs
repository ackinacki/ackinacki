// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt;
use std::marker::PhantomData;

use serde::de;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;

use crate::bls;
use crate::node::NetworkMessage;

// There is a strum cargo package exists that does similar thing
// However their implementation and use makes code less readable.
// Skipping that package with a direct implementation.

const TYPE: &str = "NetworkMessage";

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
    > Serialize
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
    TBlock: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAck: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNack: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAttestation: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TExternalMessage: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockSeqNo: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNodeIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use NetworkMessage::*;
        match self {
            Candidate(e) => serializer.serialize_newtype_variant(TYPE, 0, "Candidate", &e),
            Ack(e) => serializer.serialize_newtype_variant(TYPE, 1, "Ack", &e),
            Nack(e) => serializer.serialize_newtype_variant(TYPE, 2, "Nack", &e),
            ExternalMessage(e) => {
                serializer.serialize_newtype_variant(TYPE, 3, "ExternalMessage", &e)
            }
            NodeJoining(e) => serializer.serialize_newtype_variant(TYPE, 4, "NodeJoining", &e),
            BlockAttestation(e) => {
                serializer.serialize_newtype_variant(TYPE, 5, "BlockAttestation", &e)
            }
            BlockRequest(e) => serializer.serialize_newtype_variant(TYPE, 6, "BlockRequest", &e),
            SyncFrom(e) => serializer.serialize_newtype_variant(TYPE, 7, "SyncFrom", &e),
            SyncFinalized(e) => serializer.serialize_newtype_variant(TYPE, 8, "SyncFinalized", &e),
        }
    }
}

impl<
        'de,
        BLS,
        TBlock,
        TAck,
        TNack,
        TAttestation,
        TExternalMessage,
        TBlockIdentifier,
        TBlockSeqNo,
        TNodeIdentifier,
    > Deserialize<'de>
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
    TBlock: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAck: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNack: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAttestation: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TExternalMessage: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockSeqNo: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNodeIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_enum(
            "NetworkMessage",
            &[
                "Candidate",
                "Ack",
                "Nack",
                "ExternalMessage",
                "NodeJoining",
                "BlockAttestation",
                "BlockRequest",
                "SyncFrom",
                "SyncFinalized",
            ],
            NetworkMessageVisitor::<
                BLS,
                TBlock,
                TAck,
                TNack,
                TAttestation,
                TExternalMessage,
                TBlockIdentifier,
                TBlockSeqNo,
                TNodeIdentifier,
            >::new(),
        )
    }
}

struct NetworkMessageVisitor<
    BLS,
    TBlock,
    TAck,
    TNack,
    TAttestation,
    TExternalMessage,
    TBlockIdentifier,
    TBlockSeqNo,
    TNodeIdentifier,
> {
    _phantom_data_bls: PhantomData<BLS>,
    _phantom_data_tblock: PhantomData<TBlock>,
    _phantom_data_tack: PhantomData<TAck>,
    _phantom_data_tnack: PhantomData<TNack>,
    _phantom_data_tattestation: PhantomData<TAttestation>,
    _phantom_data_texternalmessage: PhantomData<TExternalMessage>,
    _phantom_data_tblockidentifier: PhantomData<TBlockIdentifier>,
    _phantom_data_tblockseqno: PhantomData<TBlockSeqNo>,
    _phantom_data_tnodeidentifier: PhantomData<TNodeIdentifier>,
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
    >
    NetworkMessageVisitor<
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
{
    pub fn new() -> Self {
        Self {
            _phantom_data_bls: PhantomData,
            _phantom_data_tblock: PhantomData,
            _phantom_data_tack: PhantomData,
            _phantom_data_tnack: PhantomData,
            _phantom_data_tattestation: PhantomData,
            _phantom_data_texternalmessage: PhantomData,
            _phantom_data_tblockidentifier: PhantomData,
            _phantom_data_tblockseqno: PhantomData,
            _phantom_data_tnodeidentifier: PhantomData,
        }
    }
}

impl<
        'de,
        BLS,
        TBlock,
        TAck,
        TNack,
        TAttestation,
        TExternalMessage,
        TBlockIdentifier,
        TBlockSeqNo,
        TNodeIdentifier,
    > de::Visitor<'de>
    for NetworkMessageVisitor<
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
    TBlock: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAck: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNack: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TAttestation: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TExternalMessage: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TBlockSeqNo: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
    TNodeIdentifier: Serialize + for<'b> Deserialize<'b> + Clone + Send + Sync + 'static,
{
    type Value = NetworkMessage<
        BLS,
        TBlock,
        TAck,
        TNack,
        TAttestation,
        TExternalMessage,
        TBlockIdentifier,
        TBlockSeqNo,
        TNodeIdentifier,
    >;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a NetworkMessage type.")
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: de::EnumAccess<'de>,
    {
        use serde::de::VariantAccess;
        use NetworkMessage::*;
        match data.variant()? {
            (0, v) => v.newtype_variant().map(Candidate),
            (1, v) => v.newtype_variant().map(Ack),
            (2, v) => v.newtype_variant().map(Nack),
            (3, v) => v.newtype_variant().map(ExternalMessage),
            (4, v) => v.newtype_variant().map(NodeJoining),
            (5, v) => v.newtype_variant().map(BlockAttestation),
            (6, v) => v.newtype_variant().map(BlockRequest),
            (7, v) => v.newtype_variant().map(SyncFrom),
            (8, v) => v.newtype_variant().map(SyncFinalized),
            _ => unreachable!(),
        }
    }
}
