use std::collections::HashSet;

use derive_getters::Getters;
use serde::Deserialize;
use serde::Serialize;
use typed_builder::TypedBuilder;

use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::node::associated_types::AttestationData;
use crate::node::NetBlock;
use crate::node::NodeIdentifier;
use crate::types::BlockHeight;
use crate::types::BlockIdentifier;
use crate::types::BlockRound;
use crate::types::ThreadIdentifier;

// Note for the lock:
// All nodes will have the same lock value in the same round.
// If a node received round result and it was successfull
// it will update the lock for all nodes.
// If it was a failure it will update the lock for all nodes aswell.
// ---
// In case the NextRound request collects 51% of nodes AND it does not have the block
// from the highest locked round: BP produces Failed response.
// In this response BP includes a block from the highest known locked_round for this node.
// Block + locked_round value.
// All nodes that have had locked round below the one specified in the responce will update
// their lock with the new block and the lock_round with the highest known locked round.
// It must also include the proof of the locked_round block validity. (Proof that it was produced right)
// In case the NextRound request collects 51% of the same locked_round values AND it has a locked value: BP disregards all other locks and produce a single proof that the values is the one to be finalized.
// In case the NextRound request collects 51% of None locked_round values: BP produces a block.

// TODO: ensure parent block is the same in the locked block
#[derive(Clone, Serialize, Deserialize, Getters, TypedBuilder, Debug, PartialEq, Eq)]
pub struct Lock {
    parent_block: BlockIdentifier,
    height: BlockHeight,
    next_auth_node_id: NodeIdentifier,
    locked_round: BlockRound,
    locked_block: Option<BlockIdentifier>,

    // Invalid block NACK handling:
    // This field contains only block on this height that were detected as invalid
    // AND the node confirms that the NACK was a valid one.
    nack_bad_block: HashSet<BlockIdentifier>,
}

#[derive(Clone, Serialize, Deserialize, Getters, TypedBuilder, Debug)]
pub struct NextRound {
    lock: Envelope<GoshBLS, Lock>,
    locked_block_attestation: Option<Envelope<GoshBLS, AttestationData>>,
    // Attestations for blocks that were not yet finalized in this chain.
    // This field is required when locked block is None
    attestations_for_ancestors: Vec<Envelope<GoshBLS, AttestationData>>,
}

#[derive(Clone, Serialize, Deserialize, Getters, TypedBuilder, Debug)]
pub struct NextRoundSuccess {
    node_identifier: NodeIdentifier,
    round: BlockRound,
    block_height: BlockHeight,
    proposed_block: NetBlock, // Envelope<GoshBLS, AckiNackiBlock>,
    // This field MUST exist when proposed_block is a winner in the voting.
    attestations_aggregated: Option<Envelope<GoshBLS, AttestationData>>,
    // proof that this round was successful
    requests_aggregated: Vec<Envelope<GoshBLS, Lock>>,
}

#[derive(Clone, Serialize, Deserialize, Getters, TypedBuilder)]
pub struct NextRoundFailed {
    node_identifier: NodeIdentifier,
    round: BlockRound,
    block_height: BlockHeight,
    proposed_block: NetBlock, // Envelope<GoshBLS, AckiNackiBlock>,
    attestations_aggregated: Option<Envelope<GoshBLS, AttestationData>>,
    // proof that this round was successful
    requests_aggregated: Vec<Envelope<GoshBLS, Lock>>,
}

#[derive(Clone, Serialize, Deserialize, Getters, TypedBuilder, Debug)]
pub struct NextRoundReject {
    thread_identifier: ThreadIdentifier,
    prefinalized_block: NetBlock,
    proof_of_prefinalization: Envelope<GoshBLS, AttestationData>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum AuthoritySwitch {
    /// This message is send when the current Block Producer is stalled.
    /// Note:
    /// NextRound Request MUST be signed by the signature of a node
    /// on the requested block height. It means the descendant bk set
    /// of the parent block in the Lock. This is done this way since
    /// the only block keepers that can sent attestations are those on
    /// the requested height therefore it's their responsibility to make
    /// sure correct blocks are finalized.
    Request(NextRound),

    /// This message is send in a reply to the AuthoritySwitch::Request
    /// in the case when a node does have a proof of a particular block
    /// prefinalized on the given height.
    /// TLDR; this height has a prefinalized block.
    Reject(NextRoundReject),

    /// This message is send in a reply to the AuthoritySwitch::Request
    /// in the case when a request containt too old block and next block
    /// was fianlized long ago.
    RejectTooOld(ThreadIdentifier),

    /// This message is send in a reply to the AuthoritySwitch::Request
    /// if node has collected a required quorum to finish the round.
    /// This round successful result may have the following outcomes:
    /// - There were a majority of votes for a block and the node
    ///   had seen this block in the past. So it can accept it as a winner
    ///   and resend the winning block.
    /// - There were a majority of votes that had no block voted for.
    ///   A new block were produced.
    /// - It have a quorum (50%+1) of votes. No decisive resultion yet.
    ///   Sends the best known block.
    ///   It either resends a locked block from any previous round
    ///   that has more than 50% + 1 of requested votes for this block,
    ///   or produces a new one if 50% + 1 of requests had no block voted for.
    Switched(Envelope<GoshBLS, NextRoundSuccess>),

    /// This message is sent in a reply to the AuthoritySwitch::Request
    /// in case the node were not able to collect enough requests
    /// and sends whatever block it is aware of.
    /// It is possible that the node had some requests with an unknown block locked
    Failed(Envelope<GoshBLS, NextRoundFailed>),
}
