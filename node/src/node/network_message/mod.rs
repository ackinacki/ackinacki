// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::message::WrappedMessage;
use crate::node::associated_types::AckData;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::NackData;
use crate::node::NodeIdentifier;
use crate::types::bp_selector::ProducerSelector;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

mod serde_network_message;

#[derive(Clone, Serialize, Deserialize)]
pub struct NetBlock {
    pub producer_id: NodeIdentifier,
    pub producer_selector: Option<ProducerSelector>,
    pub thread_id: ThreadIdentifier,
    pub identifier: BlockIdentifier,
    pub seq_no: BlockSeqNo,
    pub envelope_data: Vec<u8>,
}

impl NetBlock {
    pub fn with_envelope(value: &Envelope<GoshBLS, AckiNackiBlock>) -> anyhow::Result<Self> {
        let envelope_data = bincode::serialize(value)?;
        let block = value.data();
        let common_section = block.get_common_section();
        Ok(Self {
            producer_id: common_section.producer_id.clone(),
            producer_selector: common_section.producer_selector.clone(),
            thread_id: common_section.thread_id,
            identifier: block.identifier(),
            seq_no: block.seq_no(),
            envelope_data,
        })
    }

    pub fn get_envelope(&self) -> anyhow::Result<Envelope<GoshBLS, AckiNackiBlock>> {
        Ok(bincode::deserialize(&self.envelope_data)?)
    }
}

impl Display for NetBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seq_no: {:?}, id: {:?}, thread: {:?}",
            self.seq_no, self.identifier, self.thread_id,
        )
    }
}

impl Debug for NetBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seq_no: {:?}, id: {:?}, thread: {:?}",
            self.seq_no, self.identifier, self.thread_id,
        )
    }
}

#[derive(Clone)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkMessage {
    Candidate(NetBlock),

    // Candidate block envelope that was re-sent by a replacing producer
    ResentCandidate((NetBlock, NodeIdentifier)),

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
    SyncFinalized(
        (BlockIdentifier, BlockSeqNo, HashMap<ThreadIdentifier, BlockIdentifier>, ThreadIdentifier),
    ),
}

impl NetworkMessage {
    pub fn candidate(envelope: &Envelope<GoshBLS, AckiNackiBlock>) -> anyhow::Result<Self> {
        Ok(Self::Candidate(NetBlock::with_envelope(envelope)?))
    }

    pub fn resent_candidate(
        envelope: &Envelope<GoshBLS, AckiNackiBlock>,
        node_id: NodeIdentifier,
    ) -> anyhow::Result<Self> {
        Ok(Self::ResentCandidate((NetBlock::with_envelope(envelope)?, node_id)))
    }
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
                Candidate(block) => {
                    &format!("Candidate ({:?}, {:?})", block.seq_no, block.identifier)
                }
                ResentCandidate((block, node_id)) => &format!(
                    "ResentCandidate from {node_id:?} ({:?}, {:?})",
                    block.seq_no, block.identifier
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
