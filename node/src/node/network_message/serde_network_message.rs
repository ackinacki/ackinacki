// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt;

use serde::de;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;

use crate::node::NetworkMessage;
// There is a strum cargo package exists that does similar thing
// However their implementation and use makes code less readable.
// Skipping that package with a direct implementation.

const TYPE: &str = "NetworkMessage";

impl Serialize for NetworkMessage {
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
            BlockRequest {
                inclusive_from,
                exclusive_to,
                requester,
                thread_id,
                at_least_n_blocks,
            } => serializer.serialize_newtype_variant(
                TYPE,
                6,
                "BlockRequest",
                &(inclusive_from, exclusive_to, requester, thread_id, at_least_n_blocks),
            ),
            SyncFrom(e) => serializer.serialize_newtype_variant(TYPE, 7, "SyncFrom", &e),
            SyncFinalized(e) => serializer.serialize_newtype_variant(TYPE, 8, "SyncFinalized", &e),
            ResentCandidate(e) => {
                serializer.serialize_newtype_variant(TYPE, 9, "ResentCandidate", &e)
            }
            AuthoritySwitchProtocol(e) => {
                serializer.serialize_newtype_variant(TYPE, 10, "AuthoritySwitchProtocol", &e)
            }
        }
    }
}

impl<'de> Deserialize<'de> for NetworkMessage {
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
                "ResentCandidate",
                "AuthoritySwitchProtocol",
            ],
            NetworkMessageVisitor::new(),
        )
    }
}

struct NetworkMessageVisitor;

impl NetworkMessageVisitor {
    pub fn new() -> Self {
        Self {}
    }
}

impl<'de> de::Visitor<'de> for NetworkMessageVisitor {
    type Value = NetworkMessage;

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
            (6, v) => v.newtype_variant().map(|e| {
                let (inclusive_from, exclusive_to, requester, thread_id, at_least_n_blocks) = e;
                BlockRequest {
                    inclusive_from,
                    exclusive_to,
                    requester,
                    thread_id,
                    at_least_n_blocks,
                }
            }),
            (7, v) => v.newtype_variant().map(SyncFrom),
            (8, v) => v.newtype_variant().map(SyncFinalized),
            (9, v) => v.newtype_variant().map(ResentCandidate),
            (10, v) => v.newtype_variant().map(AuthoritySwitchProtocol),
            _ => unreachable!(),
        }
    }
}
