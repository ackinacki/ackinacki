use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use derive_getters::Getters;
use derive_setters::Setters;
use network::channel::NetDirectSender;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use typed_builder::TypedBuilder;

use super::find_last_prefinalized::find_last_prefinalized;
use super::find_last_prefinalized::find_next_prefinalized;
use crate::bls::create_signed::CreateSealed;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::node::associated_types::AttestationData;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use crate::node::NetBlock;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::protocol::authority_switch::network_message::AuthoritySwitch;
use crate::protocol::authority_switch::network_message::Lock;
use crate::protocol::authority_switch::network_message::NextRound;
use crate::protocol::authority_switch::network_message::NextRoundReject;
use crate::protocol::authority_switch::network_message::NextRoundSuccess;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::bp_selector::ProducerSelector;
use crate::types::AckiNackiBlock;
use crate::types::BlockHeight;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::RndSeed;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;

// This structure tells what actions had been done and prevents taking mutually exclusive actions.
// At any moment in time node either sends an attestation or request an authority switch.
//
// If authority switch request was sent it prevents sending an attestation.

pub type Round = u16;

// Note: std::time::Instant is not serializable
pub type Timeout = std::time::SystemTime;

#[derive(Serialize, Deserialize, Getters, TypedBuilder, Clone, PartialEq, Eq)]
pub struct BlockRef {
    block_seq_no: BlockSeqNo,
    block_identifier: BlockIdentifier,
}

#[derive(Debug)]
pub enum BlockRefTryFromBlockStateError {
    MissingSeqNo,
    MissingRound,
}

impl std::convert::TryFrom<&BlockState> for BlockRef {
    type Error = BlockRefTryFromBlockStateError;

    fn try_from(block_state: &BlockState) -> Result<Self, Self::Error> {
        block_state.guarded(|e| {
            let Some(block_seq_no) = *e.block_seq_no() else {
                return Err(BlockRefTryFromBlockStateError::MissingSeqNo);
            };
            let block_identifier = e.block_identifier().clone();
            Ok(BlockRef { block_seq_no, block_identifier })
        })
    }
}

#[derive(Getters, Clone, TypedBuilder, Eq, PartialEq, Debug)]
pub struct StartBlockProducerThreadInitialParameters {
    thread_identifier: ThreadIdentifier,
    parent_block_identifier: BlockIdentifier,
    parent_block_seq_no: BlockSeqNo,
    round: u16,
    nacked_blocks_bad_block: Vec<Arc<Envelope<GoshBLS, AckiNackiBlock>>>,
    // Aggregated BLS for the same lock
    proof_of_valid_start: Vec<Envelope<GoshBLS, Lock>>,
}

#[derive(Debug)]
pub enum BlockProducerCommand {
    Start(StartBlockProducerThreadInitialParameters),
    // TODO:
    // Stop(thread),
}

#[derive(Serialize, Deserialize, Getters, Clone, TypedBuilder, Setters)]
#[setters(strip_option, prefix = "set_", borrow_self)]
pub struct ActionLock {
    parent_block_producer_selector_data: ProducerSelector,
    parent_block: BlockRef,
    parent_prefinalization_proof: Option<Envelope<GoshBLS, AttestationData>>,
    locked_round: u16,
    locked_block: Option<BlockRef>,

    // It is possible that there were a block that was prefinalized and was invalidated
    // later due to the NACK. So this list will contain all of those blocks on the same hieght.
    // Those nacks MUST be included into the block produced or it will not be attestated.
    locked_bad_block_nacks: HashSet<BlockIdentifier>,

    current_round: u16,
}

#[derive(Clone)]
struct CollectedAuthoritySwitchRoundRequests {
    _round_timeout: Timeout,
    requests: Vec<NextRound>,
}

#[derive(Clone, Hash, Eq, PartialEq, TypedBuilder)]
struct SiblingsBlockHeightKey {
    parent_block_identifier: BlockIdentifier,
    height: BlockHeight,
}

// Thread state from the node perspective.
// Note:
// ActionLock MUST be durable. Any time lock is set it must be stored on a disk.
// Fail to do so may lead to stake slashing.
#[derive(Getters, TypedBuilder)]
pub struct Authority {
    data_dir: PathBuf,
    node_identifier: NodeIdentifier,
    bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,

    action_lock: HashMap<BlockHeight, ActionLock>,

    // This field contains NACKs sent/confirmed by THIS node.
    #[builder(setter(skip))]
    #[builder(default = HashMap::new())]
    confirmed_bad_block_nacks: HashMap<SiblingsBlockHeightKey, HashSet<BlockIdentifier>>,

    #[builder(setter(skip))]
    #[builder(default = HashMap::new())]
    collecting_next_round:
        HashMap<(SiblingsBlockHeightKey, Round), CollectedAuthoritySwitchRoundRequests>,

    // TODO: Critical: must be durable. Otherwise this node may be slashed on reboot.
    #[builder(setter(skip))]
    #[builder(default = HashMap::new())]
    closed_round: HashMap<SiblingsBlockHeightKey, Round>,

    block_producers: HashMap<ThreadIdentifier, std::sync::mpsc::Sender<BlockProducerCommand>>,

    block_repository: RepositoryImpl,

    block_state_repository: BlockStateRepository,
    network_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
    bp_production_count: Arc<AtomicI32>,
}

impl std::fmt::Debug for Authority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Authority")
    }
}

pub enum ActionLockResult {
    OkSendAttestation,
    Rejected,
}

pub enum OnNextRoundIncomingRequestResult {
    DoNothing,
    StartingBlockProducer,
    Broadcast(Envelope<GoshBLS, NextRoundSuccess>),
    Reject((NextRoundReject, NodeIdentifier)),
    RejectTooOldRequest(NodeIdentifier),
}

#[derive(Getters)]
pub struct OnBlockProducerStalledResult {
    next_deadline: std::time::Instant,
    next_round: Option<NextRound>,
}

impl AllowGuardedMut for Authority {}

impl Authority {
    // This does not move any authority. It only checks if the node can send
    // an attestation for the given block.
    // It is equivalent to the on PROPOSED / RESEND handler.
    pub fn try_lock_send_attestation_action(
        &mut self,
        block_identifier: &BlockIdentifier,
    ) -> ActionLockResult {
        // TODO: @andrew check not the block round but the last authority switch success round
        tracing::trace!("try_lock_send_attestation_action: start");
        let block_state = self.block_state_repository.get(block_identifier).unwrap();
        let block_proposed_in_rounds = block_state.guarded(|e| e.proposed_in_round().clone());
        if block_state.guarded(|e| e.is_invalidated()) {
            tracing::trace!("try_lock_send_attestation_action: invalidated block");
            return ActionLockResult::Rejected;
        }
        if block_state.guarded(|e| !e.has_all_nacks_resolved()) {
            tracing::trace!("try_lock_send_attestation_action: nack verification is in progress");
            return ActionLockResult::Rejected;
        }
        let Some(block_height) = block_state.guarded(|e| *e.block_height()) else {
            tracing::trace!("try_lock_send_attestation_action: height not set");
            return ActionLockResult::Rejected;
        };

        if block_proposed_in_rounds.is_empty() {
            tracing::trace!(
                "try_lock_send_attestation_action: block is not set to be a part of any round yet"
            );
            return ActionLockResult::Rejected;
        }

        let Some(parent_block_id) = block_state.guarded(|e| e.parent_block_identifier().clone())
        else {
            tracing::trace!("try_lock_send_attestation_action: parent is not set");
            return ActionLockResult::Rejected;
        };

        let parent_block = self.block_state_repository.get(&parent_block_id).unwrap();

        let block_ref = BlockRef::try_from(&block_state).unwrap();

        self.preload(&block_height);
        if let Some(lock) = self.action_lock.get_mut(&block_height) {
            if block_proposed_in_rounds.contains(&lock.current_round) {
                lock.locked_block = Some(block_ref);
                lock.locked_round = lock.current_round;
                tracing::trace!(
                    "try_lock_send_attestation_action: locked round is the same as attn target"
                );
                return ActionLockResult::OkSendAttestation;
            }
            let Some(locked_block) = lock.locked_block.clone() else {
                tracing::trace!("try_lock_send_attestation_action: no block was locked");
                return ActionLockResult::Rejected;
            };
            if locked_block.block_identifier() == block_identifier {
                tracing::trace!(
                    "try_lock_send_attestation_action: locked block is the same as attn target"
                );
                ActionLockResult::OkSendAttestation
            } else {
                tracing::trace!(
                    "try_lock_send_attestation_action: locked block is not the same as attn target"
                );
                ActionLockResult::Rejected
            }
        } else {
            let is_default_chain: bool = {
                block_proposed_in_rounds.contains(&0_u16) && block_proposed_in_rounds.len() == 1
            };

            if !is_default_chain {
                tracing::trace!("try_lock_send_attestation_action: block round is not 0");
                return ActionLockResult::Rejected;
            }

            let Some(parent_block_producer_selector_data) =
                parent_block.guarded(|e| e.producer_selector_data().clone())
            else {
                tracing::trace!("try_lock_send_attestation_action: parent selector is not set");
                return ActionLockResult::Rejected;
            };
            // TODO: this code seems to block sending attestation, parent block can not be prefinalized
            // let Some(parent_prefinalization_proof) =
            //     parent_block.guarded(|e| e.prefinalization_proof().clone())
            // else {
            //     tracing::trace!("try_lock_send_attestation_action: parent proof is not set");
            //     return ActionLockResult::Rejected;
            // };
            let lock: ActionLock = ActionLock {
                parent_block_producer_selector_data,
                parent_block: BlockRef::try_from(&parent_block).unwrap(),
                parent_prefinalization_proof: None,
                locked_round: 0,
                locked_block: Some(block_ref),
                locked_bad_block_nacks: self
                    .confirmed_bad_block_nacks
                    .get(
                        &SiblingsBlockHeightKey::builder()
                            .parent_block_identifier(parent_block_id)
                            .height(block_height)
                            .build(),
                    )
                    .cloned()
                    .unwrap_or_default(),
                current_round: 0,
            };
            self.save(&block_height, &lock);
            self.action_lock.insert(block_height, lock);
            ActionLockResult::OkSendAttestation
        }
    }

    pub fn register_block_producer(
        &mut self,
        thread_identifier: ThreadIdentifier,
        block_producer_control_tx: std::sync::mpsc::Sender<BlockProducerCommand>,
    ) {
        tracing::trace!("register_block_producer for {:?}", thread_identifier);
        self.block_producers.insert(thread_identifier, block_producer_control_tx);
    }

    pub fn on_block_producer_stalled(
        &mut self,
        thread_identifier: &ThreadIdentifier,
    ) -> OnBlockProducerStalledResult {
        tracing::trace!("on_block_producer_stalled: start");
        let Ok(last_prefinalized) = find_last_prefinalized(
            thread_identifier,
            &self.block_repository,
            &self.block_state_repository,
        ) else {
            tracing::warn!(
                "on_block_producer_stalled: Failed to get the last prefinalized block for the thread: {}",
                thread_identifier
            );
            return OnBlockProducerStalledResult {
                // TODO: config.
                next_deadline: std::time::Instant::now() + std::time::Duration::from_millis(330),
                next_round: None,
            };
        };
        let parent_block: BlockState = last_prefinalized;
        let parent_block_height: BlockHeight = parent_block.guarded(|e| e.block_height().unwrap());
        let block_height = parent_block_height.next(thread_identifier);
        self.start_next_round(parent_block, block_height)
    }

    pub fn on_bad_block_nack_confirmed(&mut self, block: BlockState) {
        let (Some(parent_block_identifier), Some(block_height)) =
            block.guarded(|e| (e.parent_block_identifier().clone(), *e.block_height()))
        else {
            panic!("Somehow we got a block confirmed to be malicious yet some of the mandatory state fields missing.")
        };
        let parent = self.block_state_repository.get(&parent_block_identifier).unwrap();
        let _ = self.start_next_round(parent, block_height);
    }

    fn start_next_round(
        &mut self,
        parent_block: BlockState,
        block_height: BlockHeight,
    ) -> OnBlockProducerStalledResult {
        // Check if we have an active action_lock for the height
        self.preload(&block_height);
        {
            let active_lock = self.action_lock.get(&block_height);
            let mut is_active_lock_valid = false;
            if let Some(active_lock) = active_lock {
                // Check if this lock is still valid: parent block is the one prefinalized
                if active_lock.parent_block.block_identifier() == parent_block.block_identifier() {
                    // The lock is valid
                    is_active_lock_valid = true;
                }
            }
            if !is_active_lock_valid {
                self.action_lock.remove(&block_height);
            }
        }

        let Some(bk_set) = parent_block.guarded(|e| e.descendant_bk_set().clone()) else {
            // The outer part of the program must ensure that all blocks were proceeded and bk_set values are set before setting some BP as stalled.
            tracing::trace!("on_block_producer_stalled: parent block descendant bk set is not set");
            return OnBlockProducerStalledResult {
                // TODO: config.
                next_deadline: std::time::Instant::now() + std::time::Duration::from_millis(330),
                next_round: None,
            };
        };

        let parent_prefinalization_proof =
            match parent_block.guarded(|e| e.prefinalization_proof().clone()) {
                Some(prefinalization_proof) => Some(prefinalization_proof),
                None => {
                    if parent_block.block_identifier() == &BlockIdentifier::default() {
                        None
                    } else {
                        panic!("prefinalization_proof must be set");
                    }
                }
            };
        // TODO: check that the next candidate can be used with the current round.
        // May be we should move the lock accordingly.
        let current_lock = self
            .action_lock
            .entry(block_height)
            .and_modify(|e| {
                let prev_locked_round = *e.locked_round();
                let locked_round = prev_locked_round + 1;
                e.set_locked_round(locked_round);
                let prev_cur_round = *e.current_round();
                e.set_current_round(prev_cur_round + 1);
            })
            .or_insert_with(|| {
                let parent_block_producer_selector_data =
                    parent_block.guarded(|e| e.producer_selector_data().clone().unwrap());
                ActionLock::builder()
                    .parent_prefinalization_proof(parent_prefinalization_proof)
                    .parent_block_producer_selector_data(parent_block_producer_selector_data)
                    .parent_block(BlockRef::try_from(&parent_block).unwrap())
                    .locked_round(1)
                    .locked_block(None) // TODO: none for not BP, if this node is BP it should set block after production
                    .locked_bad_block_nacks(self.confirmed_bad_block_nacks.get(
                        &SiblingsBlockHeightKey::builder()
                            .parent_block_identifier(parent_block.block_identifier().clone())
                            .height(block_height)
                            .build()
                    ).cloned().unwrap_or_default())
                    .current_round(1)
                    .build()
            })
            .clone();

        let round = *current_lock.locked_round();
        assert!(round >= 1);
        self.save(&block_height, &current_lock);
        let next_producer_selector = current_lock
            .parent_block_producer_selector_data()
            .clone()
            .move_index(round as usize, bk_set.len());
        let next_producer_node_id = next_producer_selector.get_producer_node_id(&bk_set).unwrap();
        let next_candidate_ref = current_lock.locked_block().clone();
        let (block, attestations) = match next_candidate_ref {
            Some(candidate_ref) => {
                let candidate =
                    self.block_state_repository.get(candidate_ref.block_identifier()).unwrap();
                let attestations = candidate.guarded(|e| {
                    if let Some(proof) = e.prefinalization_proof().clone() {
                        Some(proof)
                    } else {
                        e.attestation().clone()
                    }
                });
                (Some(candidate.block_identifier().clone()), attestations)
            }
            None => (None, None),
        };
        let lock = Lock::builder()
            .height(block_height)
            .locked_round(round)
            .locked_block(block)
            .next_auth_node_id(next_producer_node_id.clone())
            .parent_block(parent_block.block_identifier().clone())
            .nack_bad_block(current_lock.locked_bad_block_nacks().clone())
            .build();
        let secrets = self.bls_keys_map.guarded(|map| map.clone());
        let Ok(lock) =
            Envelope::sealed(&self.node_identifier, &bk_set, &secrets, lock).map_err(|e| {
                tracing::warn!("Failed to sign a lock: {}", e);
            })
        else {
            return OnBlockProducerStalledResult {
                // TODO: config.
                next_deadline: std::time::Instant::now() + std::time::Duration::from_millis(330),
                next_round: None,
            };
        };
        tracing::trace!(
            "sending next round message to {next_producer_node_id}: lock {:?}, attns: {:?}",
            lock.data(),
            attestations,
        );
        let next_round =
            NextRound::builder().lock(lock).locked_block_attestation(attestations).build();
        let _ = self.network_direct_tx.send((
            next_producer_node_id,
            NetworkMessage::AuthoritySwitchProtocol(AuthoritySwitch::Request(next_round.clone())),
        ));
        OnBlockProducerStalledResult {
            // TODO: config.
            next_deadline: std::time::Instant::now()
                + (round as u32 * std::time::Duration::from_secs(3)),
            next_round: Some(next_round),
        }
    }

    pub fn on_next_round_incoming_request(
        &mut self,
        next_round_message: NextRound,
        unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection,
    ) -> OnNextRoundIncomingRequestResult {
        // TODO: ensure that only requests from the last round are collected

        tracing::trace!(
            "on_next_round_incoming_request: next_round_message = {next_round_message:?}"
        );
        let lock = next_round_message.lock().data().clone();
        let block_height = *lock.height();
        let thread_identifier = block_height.thread_identifier();
        let parent_state = self.block_state_repository.get(lock.parent_block()).unwrap();
        // - double check that the parent block prefinalized is the same.
        //  Skipping: if this node didn't have parent prefinalized check the proof and add it for the parent.
        if parent_state.guarded(|e| !e.is_prefinalized()) {
            tracing::trace!("on_next_round_incoming_request: parent is not prefinalized");
            return OnNextRoundIncomingRequestResult::DoNothing;
        }
        if parent_state.guarded(|e| e.is_invalidated()) {
            tracing::trace!("on_next_round_incoming_request: parent block was invalidated");
            return OnNextRoundIncomingRequestResult::DoNothing;
        }
        if parent_state.guarded(|e| !e.has_all_nacks_resolved()) {
            tracing::trace!(
                "on_next_round_incoming_request: parent block nack verification is in progress"
            );
            return OnNextRoundIncomingRequestResult::DoNothing;
        }
        let Some(bk_set) = parent_state.guarded(|e| e.descendant_bk_set().clone()) else {
            tracing::trace!("on_next_round_incoming_request: parent block has no descendant bk set, but confirmed to be prefinalized (race condition)");
            return OnNextRoundIncomingRequestResult::DoNothing;
        };
        {
            match next_round_message.lock().verify_signatures(bk_set.get_pubkeys_by_signers()) {
                Ok(true) => {}
                Ok(false) => {
                    tracing::trace!("on_next_round_incoming_request: invalid signature");
                    return OnNextRoundIncomingRequestResult::DoNothing;
                }
                Err(e) => {
                    tracing::trace!(
                        "on_next_round_incoming_request: signature verification error: {e}"
                    );
                    return OnNextRoundIncomingRequestResult::DoNothing;
                }
            }
        }

        // TODO: add signature verification of the request. Low pri: hard to tell what is cheaper for the node: reply to all of malicious request and spam another node or to spend cpu verifying signatures.
        let prefinalized =
            find_next_prefinalized(&parent_state, thread_identifier, &self.block_state_repository);
        if let Some(prefinalized) = prefinalized {
            tracing::trace!(
                "on_next_round_incoming_request: reject, we already have a prefinalized block on this height: {:?} {:?}",
                prefinalized.block_identifier(),
                prefinalized.guarded(|e| *e.block_seq_no()),
            );
            let signers = next_round_message
                .lock()
                .clone_signature_occurrences()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            if signers.len() != 1 {
                tracing::trace!(
                    "on_next_round_incoming_request: lock was signed by more than one bk, skip malicious block."
                );
                return OnNextRoundIncomingRequestResult::DoNothing;
            }
            let Some(node_id) = bk_set.get_by_signer(&signers[0]).map(|data| data.node_id()) else {
                tracing::trace!("on_next_round_incoming_request: lock signer is not in the bk set");
                return OnNextRoundIncomingRequestResult::DoNothing;
            };
            let prefinalized_block = match unprocessed_blocks_cache
                .get_block_by_id(prefinalized.block_identifier())
            {
                Some(block) => block,
                _ => {
                    let Ok(Some(prefinalized_block)) =
                        self.block_repository.get_finalized_block(prefinalized.block_identifier())
                    else {
                        return if prefinalized.guarded(|e| e.is_finalized()) {
                            OnNextRoundIncomingRequestResult::RejectTooOldRequest(node_id)
                        } else {
                            tracing::trace!(
                            "on_next_round_incoming_request: can't load next prefinalized child"
                        );
                            OnNextRoundIncomingRequestResult::DoNothing
                        };
                    };
                    prefinalized_block
                }
            };
            return OnNextRoundIncomingRequestResult::Reject((
                NextRoundReject::builder()
                    .thread_identifier(*thread_identifier)
                    .known_locked_block(
                        NetBlock::with_envelope(prefinalized_block.as_ref()).unwrap(),
                    )
                    .known_attestations(prefinalized.guarded(|e| e.prefinalization_proof().clone()))
                    .build(),
                node_id,
            ));
        }

        // - Take the block height.
        // - Check if it has a proof of prefinalization for a block on that height.
        //   if so, just reject and return a proof of a block prefinalized with the block itself.
        // - Append the request to the collection of requests
        // = Check if this collection has grown above the threshold of 50%+1.
        //   if so, create and send a broadcast for a successful round.
        self.preload(&block_height);
        if let Some(current_lock) = self.action_lock.get(&block_height) {
            if current_lock.locked_round() > lock.locked_round() {
                tracing::trace!(
                    "on_next_round_incoming_request: reject. Round is lower than our locked"
                );
                return OnNextRoundIncomingRequestResult::DoNothing;
            }
        }
        let round = lock.locked_round();
        let round_key = (
            SiblingsBlockHeightKey::builder()
                .parent_block_identifier(parent_state.block_identifier().clone())
                .height(block_height)
                .build(),
            *round,
        );
        if let Some(last_closed_round) = self.closed_round.get(&round_key.0) {
            if last_closed_round >= round {
                tracing::trace!(
                    "on_next_round_incoming_request: reject. This round is already closed"
                );
                return OnNextRoundIncomingRequestResult::DoNothing;
            }
        }
        let collected_requests = self
            .collecting_next_round
            .entry(round_key.clone())
            .and_modify(|e| {
                e.requests.push(next_round_message.clone());
            })
            .or_insert_with(|| {
                let timeout = Timeout::now(); // ignored: todo!("start the timer");
                let requests = vec![next_round_message.clone()];
                CollectedAuthoritySwitchRoundRequests { _round_timeout: timeout, requests }
            })
            .clone();
        let mut collected_requests = collected_requests.requests;
        let Some(bk_set) = parent_state.guarded(|e| e.descendant_bk_set().clone()) else {
            tracing::trace!("on_next_round_incoming_request: bk_set is not set");
            return OnNextRoundIncomingRequestResult::DoNothing;
        };
        let nacked_blocks_bad_block = self
            .confirmed_bad_block_nacks
            .get(
                &SiblingsBlockHeightKey::builder()
                    .parent_block_identifier(lock.parent_block().clone())
                    .height(block_height)
                    .build(),
            )
            .cloned()
            .unwrap_or_default();
        collected_requests.retain(|_e| {
            // TODO: retain requests with valid signatures only
            true
        });
        collected_requests.retain(|e| {
            // Keep requests that have the same set of nacked blocks. Ignore other.
            e.lock().data().nack_bad_block() == &nacked_blocks_bad_block
        });

        let mut collected_voters = HashSet::default();
        let mut collected_attestations = vec![];
        for e in collected_requests.iter() {
            let signatures: HashSet<SignerIndex> =
                e.lock().clone_signature_occurrences().keys().cloned().collect();
            if signatures.is_subset(&collected_voters) {
                continue;
            }
            // new values were added
            collected_voters.extend(signatures);
            if let Some(attestation) = e.locked_block_attestation() {
                collected_attestations.push(attestation.clone());
            }
        }

        // TODO: revert back to 50% + 1 after adding secondary attestation target
        // if bk_set.len() >= 2 * collected_voters.len() {
        // Less than 50% + 1 of signers yet.

        // Note: temporary restriction to 66%
        let votes_target = (2 * bk_set.len()).div_ceil(3);
        if collected_voters.len() < votes_target {
            tracing::trace!(
                "on_next_round_incoming_request: not enough signers ({}), bk_set.len()={}",
                collected_voters.len(),
                bk_set.len()
            );
            return OnNextRoundIncomingRequestResult::DoNothing;
        }
        let mut max_locked_block: Option<Arc<Envelope<GoshBLS, AckiNackiBlock>>> = None;
        let mut max_locked_block_round: Option<u16> = None;
        for e in collected_requests.iter() {
            if let Some(block_id) = e.lock().data().locked_block() {
                let block = match unprocessed_blocks_cache.get_block_by_id(block_id) {
                    Some(block) => block,
                    _ => {
                        let Ok(Some(block)) = self.block_repository.get_finalized_block(block_id)
                        else {
                            panic!("must have a block. collected rounds can not have an unknown block. checked above.");
                        };
                        block
                    }
                };

                let block_round = block.data().get_common_section().round;
                if let Some(current_max_round) = &max_locked_block_round {
                    if block_round > *current_max_round {
                        max_locked_block = Some(block);
                        max_locked_block_round = Some(block_round);
                    }
                } else {
                    max_locked_block = Some(block);
                    max_locked_block_round = Some(block_round);
                }
            }
        }

        let max_locked_block = max_locked_block.map(Arc::unwrap_or_clone);
        // TODO:
        let proof_of_valid_round = vec![];

        tracing::trace!("on_next_round_incoming_request: max_locked_block: {max_locked_block:?}");

        let number_of_threads_this_node_currently_produces =
            self.bp_production_count.load(Ordering::Relaxed);
        let current_round_generation = (*round as usize).div_ceil(bk_set.len()) as i32;
        tracing::trace!("Check if this node can start BP for a new thread: number_of_threads_this_node_currently_produces={number_of_threads_this_node_currently_produces}, current_round_generation={current_round_generation}");
        if current_round_generation <= number_of_threads_this_node_currently_produces {
            tracing::trace!("This node is already a BP for {number_of_threads_this_node_currently_produces} threads, do not accept current round");
            return OnNextRoundIncomingRequestResult::DoNothing;
        }

        if let Some(block) = max_locked_block {
            let max_locked_block_status =
                self.block_state_repository.get(&block.data().identifier()).expect("must work");
            if max_locked_block_status
                .guarded(|e| !e.has_all_nacks_resolved() || e.is_invalidated())
            {
                if nacked_blocks_bad_block.contains(max_locked_block_status.block_identifier()) {
                    tracing::trace!("on_next_round_incoming_request: next round. Nacked block");
                    // TODO:
                    return OnNextRoundIncomingRequestResult::DoNothing;
                } else {
                    tracing::trace!("on_next_round_incoming_request: the best next candidate is not ready or invalid");
                    return OnNextRoundIncomingRequestResult::DoNothing;
                }
            }
            let mut aggregated_attestations: Option<Envelope<GoshBLS, AttestationData>> = None;
            let envelope_hash =
                max_locked_block_status.guarded(|e| e.envelope_hash().clone()).unwrap();
            let attestation_data = AttestationData::builder()
                .parent_block_id(block.data().parent())
                .block_id(block.data().identifier())
                .block_seq_no(block.data().seq_no())
                .envelope_hash(envelope_hash)
                .build();
            for attestation in collected_attestations.iter() {
                if attestation.data() != &attestation_data {
                    continue;
                }
                if let Some(ref mut aggregated_attestations) = &mut aggregated_attestations {
                    let mut merged_signatures_occurences =
                        aggregated_attestations.clone_signature_occurrences();
                    for (key, value) in attestation.clone_signature_occurrences().iter() {
                        merged_signatures_occurences
                            .entry(*key)
                            .and_modify(|e| *e += *value)
                            .or_insert(*value);
                    }
                    let merged_aggregated_signature = GoshBLS::merge(
                        aggregated_attestations.aggregated_signature(),
                        attestation.aggregated_signature(),
                    )
                    .unwrap();
                    *aggregated_attestations = Envelope::create(
                        merged_aggregated_signature,
                        merged_signatures_occurences,
                        attestation_data.clone(),
                    );
                } else {
                    aggregated_attestations = Some(attestation.clone());
                }
            }

            tracing::trace!(
                "on_next_round_incoming_request: broadcast block: round:{round}, block: {block:?}"
            );
            let secrets = self.bls_keys_map.guarded(|map| map.clone());
            // TODO: <closed_round> Must be durable.
            self.closed_round.insert(round_key.0, *round);

            // Produce block for this round AND broadcast it as a NextRoundSuccess
            tracing::trace!("Resend existing locked block from a top round {thread_identifier:?}");
            // Note: block production is not needed so far.
            // Block producer kicks in when there were no block in a previous round or the majority
            // of the block keepers had no block locked.
            return OnNextRoundIncomingRequestResult::Broadcast(
                Envelope::sealed(
                    &self.node_identifier,
                    &bk_set,
                    &secrets,
                    NextRoundSuccess::builder()
                        .node_identifier(self.node_identifier.clone())
                        .round(*round)
                        .block_height(block_height)
                        .proposed_block(NetBlock::with_envelope(&block).unwrap())
                        .attestations_aggregated(aggregated_attestations)
                        .requests_aggregated(proof_of_valid_round)
                        .build(),
                )
                .expect("must work"),
            );
        }

        // Produce block for this round AND broadcast it as a NextRoundSuccess
        self.block_producers
            .get(thread_identifier)
            .expect("must have producer service for this thread registered already")
            .send(BlockProducerCommand::Start(
                StartBlockProducerThreadInitialParameters::builder()
                    .thread_identifier(*thread_identifier)
                    .parent_block_identifier(parent_state.block_identifier().clone())
                    .parent_block_seq_no(parent_state.guarded(|e| (*e.block_seq_no()).unwrap()))
                    .round(*round)
                    .nacked_blocks_bad_block(
                        nacked_blocks_bad_block
                            .iter()
                            .map(|id| {
                                match unprocessed_blocks_cache.get_block_by_id(id) {
                                    Some(block) => block,
                                    _ => {
                                        let Ok(Some(block)) =
                                            self.block_repository.get_finalized_block(id)
                                        else {
                                            panic!("must have a block. collected rounds can not have an unknown block. checked above.");
                                        };
                                        block
                                    }
                                }
                            })
                            .collect(),
                    )
                    .proof_of_valid_start(proof_of_valid_round)
                    .build(),
            ))
            .unwrap();
        tracing::trace!("on_next_round_incoming_request: StartingBlockProducer: {round}");
        OnNextRoundIncomingRequestResult::StartingBlockProducer
    }

    pub fn on_block_finalized(&mut self, block_height: &BlockHeight) {
        self.action_lock.retain(|k, _| {
            k.thread_identifier() != block_height.thread_identifier()
                || k.height() >= block_height.height()
        });
    }

    pub fn on_next_round_failed(
        &mut self,
        _proof: Vec<Envelope<GoshBLS, ActionLock>>,
        _proposed_block: Envelope<GoshBLS, AckiNackiBlock>,
    ) {
        // Not used for now. This part is not mandatory for the protocol.
        todo!("store proposed block just in case");
    }

    pub fn is_block_producer(
        &mut self,
        _thread_identifier: ThreadIdentifier,
    ) -> Option<StartBlockProducerThreadInitialParameters> {
        todo!("Watch");
    }

    pub fn on_next_round_success(&mut self, _message: NextRoundSuccess) {
        // the happy path implementation is in node/execute.
        // - double check that the parent block prefinalized is the same.
        //   if this node didn't have parent prefinalized check the proof and add it for the parent.
        // - check if proposed block in this round has enough attestations to be prefinalized.
        //   if so, do prefinalization.
        // - otherwise do the regular work:
        // let is_block_prefinalized: bool = todo!("check if there already was a block that had enough attestations to be finalized on this height.");
        // if is_block_prefinalized {
        // todo!("Reply to the node sent a proposal with the proof of a prefinalized block");
        // return;
        // }
        //
        // let parent_block_identifier = proposed_block.data().parent();
        // let parent_block: BlockState = todo!("find from parent_block_identifier");
        //
        // let proposed_block_round = proposed_block.data().get_common_section().round;
        //
        // todo!("check the proof for quantity of the next round sent");
        // todo!("(?) check that the proposed block is the right one according to the proof");
        // todo!(
        // "check that the block is produced and signed by the correct bp accordint to the round"
        // );
        // todo!("ensure all nacks included");
        // let block_height = todo!("Get the height of the block in this thread!");
        //
        // let current_lock = self
        // .action_lock
        // .entry(block_height)
        // .and_modify(|e| {
        // if *e.locked_round() < round {
        // e.set_locked_round(round);
        // e.set_locked_block(BlockRef {
        // block_seq_no: proposed_block.data().seq_no(),
        // block_identifier: proposed_block.data().identifier(),
        // });
        // }
        // })
        // .or_insert_with(|| {
        // let parent_block_producer_selector_data = todo!("Get from the block state");
        // ActionLock::builder()
        // .parent_prefinalization_proof(
        // parent_block
        // .guarded(|e| e.prefinalization_proof().clone())
        // .expect("prefinalization_proof must be set"),
        // )
        // .parent_block_producer_selector_data(parent_block_producer_selector_data)
        // .parent_block(BlockRef::try_from(&parent_block).unwrap())
        // .locked_round(round)
        // .locked_block(Some(
        // BlockRef::builder()
        // .block_identifier(proposed_block.data().identifier())
        // .block_seq_no(proposed_block.data().seq_no())
        // .build(),
        // ))
        // .locked_nacks(vec![])
        // .current_round(round)
        // .current_block_producer(todo!())
        // .build()
        // });
        // Note:
        todo!("Push the proposed block for processing");
    }

    fn path_to(&self, height: &BlockHeight) -> PathBuf {
        self.data_dir.clone().join(format!("{}-{}", height.thread_identifier(), height.height()))
    }

    fn save(&self, block_height: &BlockHeight, data: &ActionLock) {
        let file_path = self.path_to(block_height);
        crate::repository::repository_impl::save_to_file(&file_path, data, false)
            .expect("must work");
    }

    fn preload(&mut self, block_height: &BlockHeight) {
        if self.action_lock.contains_key(block_height) {
            return;
        }
        let file_path = self.path_to(block_height);
        let Some(lock) =
            crate::repository::repository_impl::load_from_file(&file_path).expect("must work")
        else {
            return;
        };
        self.action_lock.insert(*block_height, lock);
    }
}
