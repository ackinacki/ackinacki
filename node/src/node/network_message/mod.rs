// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Formatter;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::message::WrappedMessage;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::NodeIdentifier;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

mod serde_network_message;

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkMessage {
    Candidate(Envelope<GoshBLS, AckiNackiBlock>),

    // Candidate block envelope that was re-sent by a replacing producer
    ResentCandidate((Envelope<GoshBLS, AckiNackiBlock>, NodeIdentifier)),

    Ack((Envelope<GoshBLS, AckData>, ThreadIdentifier)),

    // TODO: @AleksandrS Move nack to a priority queue
    // Full stake only
    Nack((Envelope<GoshBLS, NackData>, ThreadIdentifier)),

    // Bleeding nack
    // Bleeding((Envelope<GoshBLS, NackData>, ThreadIdentifier)),
    ExternalMessage((WrappedMessage, ThreadIdentifier)),

    NodeJoining((NodeIdentifier, ThreadIdentifier)),

    BlockAttestation((Envelope<GoshBLS, AttestationData>, ThreadIdentifier)),

    BlockRequest {
        inclusive_from: BlockSeqNo,
        exclusive_to: BlockSeqNo,
        requester: NodeIdentifier,
        thread_id: ThreadIdentifier,
        at_least_n_blocks: Option<usize>,
    },

    SyncFrom((BlockSeqNo, ThreadIdentifier)),

    // SyncFinalized is broadcasted when network is not running to restart
    // it from the same state.
    SyncFinalized((BlockIdentifier, BlockSeqNo, String, ThreadIdentifier)),
}

impl Debug for NetworkMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use NetworkMessage::*;
        if f.alternate() {
            match self {
                Candidate(_) => f.write_str("Candidate"),
                ResentCandidate(_) => f.write_str("ResentCandidate"),
                Ack(_) => f.write_str("Ack"),
                Nack(_) => f.write_str("Nack"),
                ExternalMessage(_) => f.write_str("ExternalMessage"),
                NodeJoining(_) => f.write_str("NodeJoining"),
                BlockAttestation(_) => f.write_str("BlockAttestation"),
                BlockRequest { .. } => f.write_str("BlockRequest"),
                SyncFinalized(_) => f.write_str("SyncFinalized"),
                SyncFrom(_) => f.write_str("SyncFrom"),
            }
        } else {
            let enum_type = match self {
                Candidate(block) => &format!(
                    "Candidate ({:?}, {:?})",
                    block.data().seq_no(),
                    block.data().identifier()
                ),
                ResentCandidate((block, node_id)) => &format!(
                    "ResentCandidate from {node_id:?} ({:?}, {:?})",
                    block.data().seq_no(),
                    block.data().identifier()
                ),
                Ack(_) => "Ack",
                Nack(_) => "Nack",
                ExternalMessage((msg, _)) => &format!("ExternalMessage: {:?}", msg),
                NodeJoining(_) => "NodeJoining",
                BlockAttestation(_) => "BlockAttestation",
                BlockRequest { .. } => "BlockRequest",
                SyncFinalized(_) => "SyncFinalized",
                SyncFrom(_) => "SyncFrom",
            };
            write!(f, "NetworkMessage::{}", enum_type)
        }
    }
}
