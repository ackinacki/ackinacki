use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use derive_getters::Getters;
use derive_setters::Setters;
use network::channel::NetBroadcastSender;
use network::channel::NetDirectSender;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use telemetry_utils::instrumented_channel_ext::WrappedItem;
use telemetry_utils::instrumented_channel_ext::XInstrumentedSender;
use typed_builder::TypedBuilder;

use super::find_last_prefinalized::find_last_prefinalized;
use super::find_last_prefinalized::find_next_prefinalized;
use super::round_time::RoundTime;
use crate::bls::create_signed::CreateSealed;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::helper::start_shutdown;
use crate::helper::SHUTDOWN_FLAG;
use crate::node::associated_types::AttestationData;
use crate::node::associated_types::AttestationTargetType;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::block_state::tools::invalidate_branch;
use crate::node::broadcast_node_joining;
use crate::node::services::send_attestations::AttestationSendService;
use crate::node::unprocessed_blocks_collection::FilterPrehistoric;
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
use crate::protocol::authority_switch::round_time::CalculateRoundResult;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::storage::ActionLockStorage;
use crate::types::bp_selector::ProducerSelector;
use crate::types::AckiNackiBlock;
use crate::types::BlockHeight;
use crate::types::BlockIdentifier;
use crate::types::BlockRound;
use crate::types::BlockSeqNo;
use crate::types::RndSeed;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

// Note: std::time::Instant is not serializable
pub type Timeout = std::time::SystemTime;

#[derive(Serialize, Deserialize, Getters, TypedBuilder, Clone, PartialEq, Eq, Debug)]
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
    round: BlockRound,
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

#[derive(Serialize, Deserialize, Getters, Clone, TypedBuilder, Setters, Debug)]
#[setters(strip_option, prefix = "set_", borrow_self)]
pub struct ActionLock {
    parent_block_producer_selector_data: ProducerSelector,
    parent_block: BlockRef,
    parent_prefinalization_proof: Option<Envelope<GoshBLS, AttestationData>>,
    locked_round: BlockRound,
    locked_block: Option<(BlockRound, BlockRef)>,

    // It is possible that there were a block that was prefinalized and was invalidated
    // later due to the NACK. So this list will contain all of those blocks on the same hieght.
    // Those nacks MUST be included into the block produced or it will not be attestated.
    locked_bad_block_nacks: HashSet<BlockIdentifier>,
    // current_round: BlockRound,
}

fn trace_lock(action_lock: &ActionLock, message: &str) {
    tracing::trace!("{message}: {action_lock:?}");
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

#[derive(TypedBuilder)]
pub struct Authority {
    #[builder(setter(skip))]
    #[builder(default = HashMap::new())]
    authorities: HashMap<ThreadIdentifier, Arc<Mutex<ThreadAuthority>>>,
    round_buckets: RoundTime,
    data_dir: PathBuf,
    node_identifier: NodeIdentifier,
    bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
    block_repository: RepositoryImpl,
    block_state_repository: BlockStateRepository,
    network_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
    bp_production_count: Arc<AtomicI32>,
    network_broadcast_tx: NetBroadcastSender<NetworkMessage>,
    node_joining_timeout: Duration,
    action_lock_db: ActionLockStorage,
    max_lookback_block_height_distance: usize,
    self_addr: SocketAddr,
    action_lock_collections: HashMap<ThreadIdentifier, ActionLockCollection>,
}

impl Authority {
    pub fn get_thread_authority(
        &mut self,
        thread_id: &ThreadIdentifier,
    ) -> Arc<Mutex<ThreadAuthority>> {
        Arc::clone(
            self.authorities.entry(*thread_id).or_insert(Arc::new(Mutex::new(
                ThreadAuthority::builder()
                    .thread_id(*thread_id)
                    .round_buckets(self.round_buckets.clone())
                    .action_lock(self.action_lock_collections.remove(thread_id).unwrap_or(
                        ActionLockCollection::new(
                            self.data_dir.clone().join(format!("{thread_id:?}")),
                            self.action_lock_db.clone(),
                        ),
                    ))
                    .node_identifier(self.node_identifier.clone())
                    .bls_keys_map(self.bls_keys_map.clone())
                    .block_repository(self.block_repository.clone())
                    .block_state_repository(self.block_state_repository.clone())
                    .network_direct_tx(self.network_direct_tx.clone())
                    .bp_production_count(self.bp_production_count.clone())
                    .network_broadcast_tx(self.network_broadcast_tx.clone())
                    .node_joining_timeout(self.node_joining_timeout)
                    .max_lookback_block_height_distance(self.max_lookback_block_height_distance)
                    .self_addr(self.self_addr)
                    .build(),
            ))),
        )
    }
}

impl AllowGuardedMut for ThreadAuthority {}

pub struct ActionLockCollection {
    preloaded: HashMap<BlockHeight, Option<ActionLock>>,
    data_dir: PathBuf,
    action_lock_db: ActionLockStorage,
}

impl ActionLockCollection {
    pub fn new(data_dir: PathBuf, action_lock_db: ActionLockStorage) -> Self {
        Self { data_dir, preloaded: HashMap::new(), action_lock_db }
    }

    pub fn get_mut(
        &mut self,
        block_height: &BlockHeight,
        block_state_repository: &BlockStateRepository,
    ) -> Option<&mut ActionLock> {
        self.preload(block_height);
        if !self.is_active(block_height, block_state_repository) {
            self.preloaded.insert(*block_height, None);
        }
        match self.preloaded.get_mut(block_height) {
            None => None,
            Some(inner) => inner.as_mut(),
        }
    }

    pub fn get(
        &mut self,
        block_height: &BlockHeight,
        block_state_repository: &BlockStateRepository,
    ) -> Option<&ActionLock> {
        self.preload(block_height);
        if !self.is_active(block_height, block_state_repository) {
            return None;
        }
        match self.preloaded.get(block_height) {
            None => None,
            Some(inner) => inner.as_ref(),
        }
    }

    pub fn insert(&mut self, block_height: BlockHeight, action_lock: ActionLock) {
        self.preloaded.insert(block_height, Some(action_lock));
    }

    pub fn remove(&mut self, block_height: &BlockHeight) {
        self.preloaded.insert(*block_height, None);
    }

    pub fn drop_old_locks(&mut self, block_height: &BlockHeight) {
        self.preloaded.retain(|k, _| {
            k.thread_identifier() != block_height.thread_identifier()
                || k.height() >= block_height.height()
        });
    }

    fn is_active(
        &self,
        block_height: &BlockHeight,
        block_state_repository: &BlockStateRepository,
    ) -> bool {
        if let Some(Some(action_lock)) = self.preloaded.get(block_height) {
            if let Some(block_ref) = &action_lock.locked_block {
                let locked_block_state =
                    block_state_repository.get(&block_ref.1.block_identifier).unwrap();
                if locked_block_state.guarded(|e| e.is_invalidated()) {
                    return false;
                }
            }
        }
        true
    }

    fn preload(&mut self, block_height: &BlockHeight) {
        if self.preloaded.contains_key(block_height) {
            return;
        }

        let file_path = self.path_to(block_height);

        let maybe_lock = if cfg!(feature = "messages_db") {
            self.action_lock_db
                .read_blob(&file_path.to_string_lossy())
                .unwrap_or_else(|_| panic!("Failed to load record: {}", file_path.display()))
        } else {
            crate::repository::repository_impl::load_from_file(&file_path)
                .unwrap_or_else(|_| panic!("Failed to load file: {}", file_path.display()))
        };

        let Some(lock) = maybe_lock else {
            return;
        };
        self.preloaded.insert(*block_height, lock);
    }

    fn save(&self, block_height: &BlockHeight) {
        let Some(data) = self.preloaded.get(block_height) else {
            return;
        };
        let file_path = self.path_to(block_height);
        if cfg!(feature = "messages_db") {
            // `true` → keep retrying writes until they succeed
            self.action_lock_db
                .write_blob(&file_path.to_string_lossy(), data, true)
                .expect("Can't fail if data is serializable")
        } else {
            crate::repository::repository_impl::save_to_file(&file_path, data, false)
                .expect("must work");
        }
    }

    fn path_to(&self, height: &BlockHeight) -> PathBuf {
        self.data_dir.clone().join(format!("{}-{}", height.thread_identifier(), height.height()))
    }
}

#[derive(Getters, TypedBuilder)]
pub struct ThreadAuthority {
    thread_id: ThreadIdentifier,
    round_buckets: RoundTime,
    node_identifier: NodeIdentifier,
    bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,

    // TODO: load on restart
    // #[builder(setter(
    //     suffix="_data_dir",
    //     transform = |data_dir: PathBuf, action_lock_db: ActionLockStorage| ActionLockCollection::new(data_dir, action_lock_db)
    // ))]
    action_lock: ActionLockCollection,

    // This field contains NACKs sent/confirmed by THIS node.
    #[builder(setter(skip))]
    #[builder(default = HashMap::new())]
    confirmed_bad_block_nacks: HashMap<SiblingsBlockHeightKey, HashSet<BlockIdentifier>>,

    #[builder(setter(skip))]
    #[builder(default = HashMap::new())]
    collecting_next_round:
        HashMap<(SiblingsBlockHeightKey, BlockRound), CollectedAuthoritySwitchRoundRequests>,

    // TODO: Critical: must be durable. Otherwise this node may be slashed on reboot.
    #[builder(setter(skip))]
    #[builder(default = HashMap::new())]
    closed_round: HashMap<SiblingsBlockHeightKey, BlockRound>,

    #[builder(setter(skip))]
    #[builder(default = None)]
    block_producers: Option<std::sync::mpsc::Sender<BlockProducerCommand>>,

    block_repository: RepositoryImpl,
    block_state_repository: BlockStateRepository,
    network_direct_tx: NetDirectSender<NodeIdentifier, NetworkMessage>,
    bp_production_count: Arc<AtomicI32>,

    network_broadcast_tx: NetBroadcastSender<NetworkMessage>,
    node_joining_timeout: Duration,
    #[builder(setter(skip))]
    #[builder(default = std::time::Instant::now().checked_sub(Duration::from_secs(10000)).unwrap())]
    last_node_joining_sent: Instant,

    max_lookback_block_height_distance: usize,

    #[builder(setter(skip))]
    #[builder(default = None)]
    self_node_authority_tx: Option<XInstrumentedSender<(NetworkMessage, SocketAddr)>>,

    self_addr: SocketAddr,
}

impl std::fmt::Debug for Authority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Authority")
    }
}

#[derive(Eq, PartialEq)]
pub enum ActionLockResult {
    OkSendAttestation,
    Rejected,
}

#[allow(clippy::large_enum_variant)]
pub enum OnNextRoundIncomingRequestResult {
    DoNothing,
    StartingBlockProducer,
    Broadcast(Envelope<GoshBLS, NextRoundSuccess>),
    Reject((NextRoundReject, NodeIdentifier)),
    RejectTooOldRequest(network::DirectReceiver<NodeIdentifier>),
}

#[derive(Getters, Debug)]
pub struct OnBlockProducerStalledResult {
    next_deadline: std::time::Instant,
    next_round: Option<NextRound>,
    block_height: Option<BlockHeight>,
}

impl OnBlockProducerStalledResult {
    pub fn retry_later(block_height: Option<BlockHeight>) -> Self {
        OnBlockProducerStalledResult {
            // TODO: config.
            next_deadline: std::time::Instant::now() + std::time::Duration::from_millis(30),
            next_round: None,
            block_height,
        }
    }

    pub fn retry_after(block_height: Option<BlockHeight>, timeout: std::time::Duration) -> Self {
        OnBlockProducerStalledResult {
            // TODO: config.
            next_deadline: std::time::Instant::now() + timeout,
            next_round: None,
            block_height,
        }
    }
}

impl AllowGuardedMut for Authority {}

impl ThreadAuthority {
    // This does not move any authority. It only checks if the node can send
    // an attestation for the given block.
    // It is equivalent to the on PROPOSED / RESEND handler.

    pub fn try_lock_send_attestation_action(
        &mut self,
        block_identifier: &BlockIdentifier,
    ) -> ActionLockResult {
        tracing::trace!("try_lock_send_attestation_action: start {block_identifier:?}");
        let block_state = self.block_state_repository.get(block_identifier).unwrap();
        if block_state.guarded(|e| e.is_finalized() || e.is_prefinalized()) {
            return ActionLockResult::OkSendAttestation;
        }
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

        let Some(block_round) = block_state.guarded(|e| *e.block_round()) else {
            tracing::trace!(
                "try_lock_send_attestation_action: block is not set to be a part of any round yet"
            );
            return ActionLockResult::Rejected;
        };

        let Some(parent_block_id) = block_state.guarded(|e| e.parent_block_identifier().clone())
        else {
            tracing::trace!("try_lock_send_attestation_action: parent is not set");
            return ActionLockResult::Rejected;
        };

        let parent_block = self.block_state_repository.get(&parent_block_id).unwrap();
        let Some(bk_set) = parent_block.guarded(|e| e.descendant_bk_set().clone()) else {
            // The outer part of the program must ensure that all blocks were proceeded and bk_set values are set before setting some BP as stalled.
            tracing::trace!(
                "try_lock_send_attestation_action: parent block descendant bk set is not set"
            );
            return ActionLockResult::Rejected;
        };

        let Some(parent_block_time) = parent_block.guarded(|e| *e.block_time_ms()) else {
            tracing::trace!("try_lock_send_attestation_action: parent block time is not set");
            return ActionLockResult::Rejected;
        };
        let now = {
            let start = SystemTime::now();
            let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("time should go forward");
            TryInto::<u64>::try_into(since_the_epoch.as_millis()).unwrap()
        };
        if parent_block_time > now {
            tracing::warn!(
                "try_lock_send_attestation_action: local clock and BP clock is out of sync"
            );
            return ActionLockResult::Rejected;
        }
        let duration_from_parent_block = Duration::from_millis(now - parent_block_time);
        let local_round = self
            .round_buckets
            .calculate_round(duration_from_parent_block, bk_set.len().try_into().unwrap())
            .round;
        // if local_round > *block_proposed_in_rounds.iter().min().unwrap() {
        //     tracing::trace!("Can not lock a block from a future round, local_round={local_round:?} > {:?}", *block_proposed_in_rounds.iter().min().unwrap());
        //     return ActionLockResult::Rejected;
        // }
        let block_ref = BlockRef::try_from(&block_state).unwrap();

        let mut is_lock_updated = false;
        let result = || -> ActionLockResult {
            if let Some(lock) =
                self.action_lock.get_mut(&block_height, &self.block_state_repository)
            {
                trace_lock(lock, "lock state in");
                if block_round > lock.locked_round {
                    lock.locked_block = Some((block_round, block_ref));
                    lock.locked_round = block_round;
                    trace_lock(
                    lock,
                    "try_lock_send_attestation_action: (updating) block_round is newer than the locked round."
                );
                    is_lock_updated = true;
                    return ActionLockResult::OkSendAttestation;
                }
                if block_round == lock.locked_round && lock.locked_block.is_none() {
                    lock.locked_block = Some((block_round, block_ref));
                    lock.locked_round = local_round + 1;
                    trace_lock(
                        lock,
                        "try_lock_send_attestation_action: (updating) locked round is the same as attn target"
                    );
                    is_lock_updated = true;
                    return ActionLockResult::OkSendAttestation;
                }
                let Some(locked_block) = lock.locked_block.clone() else {
                    tracing::trace!("try_lock_send_attestation_action: no block was locked");
                    return ActionLockResult::Rejected;
                };
                if locked_block.1.block_identifier() == block_identifier {
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
                let Some(parent_block_producer_selector_data) =
                    parent_block.guarded(|e| e.producer_selector_data().clone())
                else {
                    tracing::trace!("try_lock_send_attestation_action: parent selector is not set");
                    return ActionLockResult::Rejected;
                };

                let lock: ActionLock = ActionLock {
                    parent_block_producer_selector_data,
                    parent_block: BlockRef::try_from(&parent_block).unwrap(),
                    parent_prefinalization_proof: None,
                    locked_round: local_round,
                    locked_block: Some((block_round, block_ref)),
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
                    // current_round: round,
                };
                trace_lock(
                    &lock,
                    "try_lock_send_attestation_action: locked a block on a new height.",
                );
                is_lock_updated = true;
                self.action_lock.insert(block_height, lock);
                ActionLockResult::OkSendAttestation
            }
        }();
        if is_lock_updated {
            self.action_lock.save(&block_height);
        }
        result
    }

    pub fn register_block_producer(
        &mut self,
        block_producer_control_tx: std::sync::mpsc::Sender<BlockProducerCommand>,
    ) {
        tracing::trace!("register_block_producer for {:?}", self.thread_id);
        self.block_producers = Some(block_producer_control_tx);
    }

    pub fn register_self_node_authority_tx(
        &mut self,
        self_node_authority_tx: XInstrumentedSender<(NetworkMessage, SocketAddr)>,
    ) {
        tracing::trace!("register_self_node_authority_tx for {:?}", self.thread_id);
        self.self_node_authority_tx = Some(self_node_authority_tx);
    }

    pub fn on_block_producer_stalled(&mut self) -> OnBlockProducerStalledResult {
        let thread_identifier = self.thread_id;
        tracing::trace!("on_block_producer_stalled: start {thread_identifier:?}");
        let Ok(last_prefinalized) = find_last_prefinalized(
            &thread_identifier,
            &self.block_repository,
            &self.block_state_repository,
        ) else {
            tracing::warn!(
                "on_block_producer_stalled: Failed to get the last prefinalized block for the thread: {}",
                thread_identifier
            );
            return OnBlockProducerStalledResult::retry_later(None);
        };
        tracing::trace!("on_block_producer_stalled: last_prefinalized={last_prefinalized:?}");
        let parent_block: BlockState = last_prefinalized;
        let (seq_no, parent_block_height) = parent_block
            .guarded(|e| (e.block_seq_no().unwrap_or_default(), e.block_height().unwrap()));

        if let Some(m) = self.block_repository.get_metrics() {
            let seq_no: u32 = seq_no.into();
            m.report_last_prefinalized_seqno(seq_no as u64, &thread_identifier);
        }

        let block_height = parent_block_height.next(&thread_identifier);
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
        {
            let active_lock = self.action_lock.get(&block_height, &self.block_state_repository);
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
        if let Some(lock) = self.action_lock.get(&block_height, &self.block_state_repository) {
            trace_lock(lock, "start_next_round: in lock (exists)");
        } else {
            tracing::trace!("start_next_round: no lock on block height: {block_height:?}");
        }

        let Some(bk_set) = parent_block.guarded(|e| e.descendant_bk_set().clone()) else {
            // The outer part of the program must ensure that all blocks were proceeded and bk_set values are set before setting some BP as stalled.
            tracing::trace!("start_next_round: parent block descendant bk set is not set");
            return OnBlockProducerStalledResult::retry_later(Some(block_height));
        };
        let Some(parent_block_time) = parent_block.guarded(|e| *e.block_time_ms()) else {
            tracing::trace!("start_next_round: parent block time is not set");
            return OnBlockProducerStalledResult::retry_later(Some(block_height));
        };
        let now = {
            let start = SystemTime::now();
            let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("time should go forward");
            TryInto::<u64>::try_into(since_the_epoch.as_millis()).unwrap()
        };
        if parent_block_time > now {
            tracing::warn!("start_next_round: local clock and BP clock is out of sync");
            return OnBlockProducerStalledResult::retry_later(Some(block_height));
        }
        let duration_from_parent_block = Duration::from_millis(now - parent_block_time);
        let CalculateRoundResult { round: local_round, round_remaining_time } = self
            .round_buckets
            .calculate_round(duration_from_parent_block, bk_set.len().try_into().unwrap());

        if local_round == 0 {
            tracing::warn!("start_next_round: (failed) still in the round zero.");
            return OnBlockProducerStalledResult {
                next_deadline: Instant::now() + round_remaining_time,
                next_round: None,
                block_height: Some(block_height),
            };
        }

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
        let mut was_lock_updated = false;
        let current_lock_snapshot = {
            if let Some(existing) =
                self.action_lock.get_mut(&block_height, &self.block_state_repository)
            {
                let prev_locked_round = *existing.locked_round();
                // Note: adding equals case so that a lock
                // updated in a previous round with a new block
                // can send a request in the next round.
                if local_round >= prev_locked_round {
                    existing.set_locked_round(local_round);
                    was_lock_updated = true;
                }
                existing.clone()
            } else {
                was_lock_updated = true;
                let parent_block_producer_selector_data =
                    parent_block.guarded(|e| e.producer_selector_data().clone().unwrap());
                let new_lock = ActionLock::builder()
                    .parent_prefinalization_proof(parent_prefinalization_proof)
                    .parent_block_producer_selector_data(parent_block_producer_selector_data)
                    .parent_block(BlockRef::try_from(&parent_block).unwrap())
                    .locked_round(local_round)
                    // Note: if we ended up at this line it is clear that no attestation
                    // for any block were send yet. safe to assume that there's no block
                    // candidate available
                    .locked_block(None)
                    .locked_bad_block_nacks(self.confirmed_bad_block_nacks.get(
                        &SiblingsBlockHeightKey::builder()
                            .parent_block_identifier(parent_block.block_identifier().clone())
                            .height(block_height)
                            .build()
                    ).cloned().unwrap_or_default())
                    //.current_round(round)
                    .build();
                self.action_lock.insert(block_height, new_lock.clone());
                new_lock
            }
        };
        if !was_lock_updated {
            tracing::warn!("start_next_round: round was not updated.");
            return OnBlockProducerStalledResult {
                next_deadline: Instant::now() + round_remaining_time,
                next_round: None,
                block_height: Some(block_height),
            };
        }
        trace_lock(&current_lock_snapshot, "start_next_round: preparing a request.");
        self.action_lock.save(&block_height);
        let cur_producer_selector =
            current_lock_snapshot.parent_block_producer_selector_data().clone().move_index(
                TryInto::<usize>::try_into((local_round as i64) - 1)
                    .expect("local round must be greater or equal to 1"),
                bk_set.len(),
            );
        let cur_producer_node_id = cur_producer_selector.get_producer_node_id(&bk_set).unwrap();
        let next_producer_selector = current_lock_snapshot
            .parent_block_producer_selector_data()
            .clone()
            .move_index(local_round as usize, bk_set.len());
        let next_producer_node_id = next_producer_selector.get_producer_node_id(&bk_set).unwrap();
        let next_candidate_ref = current_lock_snapshot.locked_block().clone().map(|e| e.1);

        if !bk_set.contains_node(&self.node_identifier) {
            let Some(future_bk_set) =
                parent_block.guarded(|e| e.descendant_future_bk_set().clone())
            else {
                tracing::trace!(
                    "start_next_round: parent block descendant future bk set is not set"
                );
                return OnBlockProducerStalledResult::retry_later(Some(block_height));
            };
            if !future_bk_set.contains_node(&self.node_identifier) {
                tracing::trace!("start_next_round: parent block future bk set is not set");
            }
            // TODO: Try requesting blocks directly. Count attempts
            // Do full sync if it reached max attempts count.

            if self.last_node_joining_sent.elapsed() > self.node_joining_timeout {
                self.last_node_joining_sent = std::time::Instant::now();
                let thread_id = parent_block
                    .guarded(|e| *e.thread_identifier())
                    .expect("Thread id must be set for parent block");
                let _ = broadcast_node_joining(
                    &self.network_broadcast_tx,
                    self.block_repository.get_metrics(),
                    self.node_identifier.clone(),
                    thread_id,
                );
                let _ = self.self_node_authority_tx.as_ref().map(|e| {
                    e.send(WrappedItem {
                        payload: (
                            NetworkMessage::AuthoritySwitchProtocol(AuthoritySwitch::RejectTooOld(
                                self.thread_id,
                            )),
                            self.self_addr,
                        ),
                        label: self.thread_id.to_string(),
                    })
                });
                return OnBlockProducerStalledResult::retry_after(
                    Some(block_height),
                    self.node_joining_timeout,
                );
            }
            return OnBlockProducerStalledResult::retry_later(Some(block_height));
        }

        let mut has_all_attestations_locked = true;
        let (block, attestations) = match next_candidate_ref {
            Some(candidate_ref) => {
                let candidate =
                    self.block_state_repository.get(candidate_ref.block_identifier()).unwrap();
                let prefinalization_proof =
                    candidate.guarded(|e| e.prefinalization_proof().clone());
                let attestations = {
                    if prefinalization_proof.is_none() {
                        if self.try_lock_send_attestation_action(candidate_ref.block_identifier())
                            == ActionLockResult::OkSendAttestation
                        {
                            candidate.guarded(|e| e.own_attestation().clone())
                        } else {
                            has_all_attestations_locked = false;
                            None
                        }
                    } else {
                        prefinalization_proof
                    }
                };
                (Some(candidate.block_identifier().clone()), attestations)
            }
            None => (None, None),
        };

        let attestations_for_ancestors = {
            if block.is_some() {
                vec![]
            } else {
                let Some(ancestor_blocks_finalization_checkpoints) =
                    parent_block.guarded(|e| e.ancestor_blocks_finalization_checkpoints().clone())
                else {
                    tracing::trace!("start_next_round: parent ancestor_blocks_finalization_checkpoints are not set");
                    return OnBlockProducerStalledResult::retry_later(Some(block_height));
                };
                let required_fallback_attestations = {
                    let mut ancestors = Vec::<BlockIdentifier>::from_iter(
                        ancestor_blocks_finalization_checkpoints.fallback().keys().cloned(),
                    );
                    ancestors.retain(|e| {
                        // Keep keys that are in the fallback checkpoints list
                        // and are not in the primary list.
                        // Those are blocks that have missed their primary attestation target.
                        !ancestor_blocks_finalization_checkpoints.primary().contains_key(e)
                    });
                    ancestors
                };
                let required_primary_attestations = Vec::<BlockIdentifier>::from_iter(
                    ancestor_blocks_finalization_checkpoints.primary().iter().filter_map(
                        |(id, _checkpoint)| {
                            // Note:
                            // It is possible to send only attestations for blocks
                            // that must be finalized on the next round.
                            // However it seems that sending attestations for the entire
                            // chain may work better.
                            if !required_fallback_attestations.contains(id) {
                                Some(id.clone())
                            } else {
                                None
                            }
                        },
                    ),
                );
                let mut attestations = vec![];
                for id in required_fallback_attestations.iter() {
                    let ancestor_state = self.block_state_repository.get(id).unwrap();
                    if self.try_lock_send_attestation_action(id)
                        != ActionLockResult::OkSendAttestation
                    {
                        has_all_attestations_locked = false;
                    }

                    let attestation = match ancestor_state
                        .guarded(|e| e.own_fallback_attestation().clone())
                    {
                        Some(attestation) => attestation,
                        None => {
                            if ancestor_state.guarded(|e| e.is_finalized()) {
                                let Ok(attestation) = AttestationSendService::generate_attestation(
                                    self.bls_keys_map.clone(),
                                    &self.node_identifier,
                                    &ancestor_state,
                                    AttestationTargetType::Fallback,
                                ) else {
                                    tracing::trace!("start_next_round: Failed to generate fallback attestation for {:?}", ancestor_state);
                                    return OnBlockProducerStalledResult::retry_later(Some(
                                        block_height,
                                    ));
                                };
                                ancestor_state.guarded_mut(|e| {
                                    if e.own_fallback_attestation().is_none() {
                                        e.set_own_fallback_attestation(attestation)
                                            .expect("failed to set fallback attestation");
                                    }
                                    e.own_fallback_attestation().clone().unwrap()
                                })
                            } else {
                                tracing::trace!("start_next_round: ancestor block does not have own_fallback_attestation set");
                                return OnBlockProducerStalledResult::retry_later(Some(
                                    block_height,
                                ));
                            }
                        }
                    };
                    attestations.push(attestation);
                }
                for id in required_primary_attestations.iter() {
                    let ancestor_state = self.block_state_repository.get(id).unwrap();
                    if self.try_lock_send_attestation_action(id)
                        != ActionLockResult::OkSendAttestation
                    {
                        has_all_attestations_locked = false;
                    }
                    let attestation = match ancestor_state.guarded(|e| e.own_attestation().clone())
                    {
                        Some(attestation) => attestation,
                        None => {
                            if ancestor_state.guarded(|e| e.is_finalized()) {
                                let Ok(attestation) = AttestationSendService::generate_attestation(
                                    self.bls_keys_map.clone(),
                                    &self.node_identifier,
                                    &ancestor_state,
                                    AttestationTargetType::Primary,
                                ) else {
                                    tracing::trace!(
                                        "start_next_round: Failed to generate attestation for {:?}",
                                        ancestor_state
                                    );
                                    return OnBlockProducerStalledResult::retry_later(Some(
                                        block_height,
                                    ));
                                };
                                ancestor_state.guarded_mut(|e| {
                                    if e.own_attestation().is_none() {
                                        e.set_own_attestation(attestation)
                                            .expect("failed to set attestation");
                                    }
                                    e.own_attestation().clone().unwrap()
                                })
                            } else {
                                tracing::trace!(
                                    "start_next_round: ancestor block does not have own_attestation set"
                                );
                                return OnBlockProducerStalledResult::retry_later(Some(
                                    block_height,
                                ));
                            }
                        }
                    };

                    attestations.push(attestation);
                }
                attestations
            }
        };

        if !has_all_attestations_locked {
            tracing::warn!("start_next_round: were not able to lock all ancestor attestations");
            return OnBlockProducerStalledResult::retry_later(Some(block_height));
        }

        let lock = Lock::builder()
            .height(block_height)
            .locked_round(*current_lock_snapshot.locked_round())
            .locked_block(block)
            .next_auth_node_id(next_producer_node_id.clone())
            .parent_block(parent_block.block_identifier().clone())
            .nack_bad_block(current_lock_snapshot.locked_bad_block_nacks().clone())
            .build();
        let secrets = self.bls_keys_map.guarded(|map| map.clone());
        let Ok(lock) =
            Envelope::sealed(&self.node_identifier, &bk_set, &secrets, lock).map_err(|e| {
                tracing::warn!("Failed to sign a lock: {}", e);
            })
        else {
            return OnBlockProducerStalledResult::retry_later(Some(block_height));
        };
        tracing::trace!(
            "sending next round message to {next_producer_node_id}: lock {:?}, attns: {:?}",
            lock.data(),
            attestations,
        );

        if let Some(m) = self.block_repository.get_metrics() {
            m.report_next_round_block_height(*block_height.height(), &self.thread_id);
        }

        let next_round = NextRound::builder()
            .lock(lock)
            .locked_block_attestation(attestations)
            .attestations_for_ancestors(attestations_for_ancestors)
            .build();
        let _ = self.network_direct_tx.send((
            next_producer_node_id.into(),
            NetworkMessage::AuthoritySwitchProtocol(AuthoritySwitch::Request(next_round.clone())),
        ));
        let _ = self.network_direct_tx.send((
            cur_producer_node_id.into(),
            NetworkMessage::AuthoritySwitchProtocol(AuthoritySwitch::Request(next_round.clone())),
        ));

        OnBlockProducerStalledResult {
            next_deadline: Instant::now() + round_remaining_time,
            next_round: Some(next_round),
            block_height: Some(block_height),
        }
    }

    fn get_block(
        &self,
        block_identifier: &BlockIdentifier,
        unprocessed_blocks_cache: &UnfinalizedCandidateBlockCollection,
    ) -> Option<Arc<Envelope<GoshBLS, AckiNackiBlock>>> {
        if let Some(block) = unprocessed_blocks_cache.get_block_by_id(block_identifier) {
            return Some(block);
        }
        self.block_repository.get_finalized_block(block_identifier).unwrap()
    }

    pub fn on_next_round_incoming_request(
        &mut self,
        next_round_message: NextRound,
        sender: Option<std::net::SocketAddr>,
        unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection,
    ) -> OnNextRoundIncomingRequestResult {
        // TODO: ensure that only requests from the last round are collected

        tracing::trace!(
            "on_next_round_incoming_request: next_round_message = {next_round_message:?}"
        );
        let lock = next_round_message.lock().data().clone();
        let signer_index = {
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
            signers[0]
        };
        let block_height = *lock.height();
        let last_finalized_block_height_relative_to_this_thread = {
            let Ok(last_finalized_block_info) =
                self.block_repository.select_thread_last_finalized_block(&self.thread_id)
            else {
                tracing::warn!(
                    "on_next_round_incoming_request: select_thread_last_finalized_block failed"
                );
                return OnNextRoundIncomingRequestResult::DoNothing;
            };

            if let Some((last_finalized_block_id, _seq_no)) = last_finalized_block_info {
                let Ok(last_finalized_block_state) =
                    self.block_state_repository.get(&last_finalized_block_id)
                else {
                    tracing::warn!(
                        "on_next_round_incoming_request: failed to load finalized block state"
                    );
                    return OnNextRoundIncomingRequestResult::DoNothing;
                };
                let Some(height) = last_finalized_block_state.guarded(|e| *e.block_height()) else {
                    tracing::warn!("on_next_round_incoming_request: we have a finalized block with no height information available");
                    return OnNextRoundIncomingRequestResult::DoNothing;
                };
                // just to make sure it stays in this thread.
                // this will be handled with greater vs grater or equal later
                height.next(&self.thread_id)
            } else {
                let thread_root_block_id = self.thread_id.spawning_block_id();
                let Ok(thread_root_block_state) =
                    self.block_state_repository.get(&thread_root_block_id)
                else {
                    tracing::warn!(
                        "on_next_round_incoming_request: failed to load thread root block state"
                    );
                    return OnNextRoundIncomingRequestResult::DoNothing;
                };
                let Some(height) = thread_root_block_state.guarded(|e| *e.block_height()) else {
                    tracing::warn!("on_next_round_incoming_request: we have a finalized thread root block with no height information available");
                    return OnNextRoundIncomingRequestResult::DoNothing;
                };

                // this ensures same heigh calculation logic
                height.next(&self.thread_id)
            }
        };

        if block_height
            .signed_distance_to(&last_finalized_block_height_relative_to_this_thread)
            .map(|e| e > self.max_lookback_block_height_distance as i128)
            .unwrap_or_default()
        {
            if let Some(addr) = sender {
                return OnNextRoundIncomingRequestResult::RejectTooOldRequest(
                    network::DirectReceiver::Addr(addr),
                );
            } else {
                return OnNextRoundIncomingRequestResult::DoNothing;
            }
        }

        let thread_identifier = block_height.thread_identifier();
        let parent_state = self.block_state_repository.get(lock.parent_block()).unwrap();
        // - double check that the parent block prefinalized is the same.
        //  Skipping: if this node didn't have parent prefinalized check the proof and add it for the parent.
        if parent_state.guarded(|e| !e.is_prefinalized() && !e.is_finalized()) {
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
        let Some(node_id) = bk_set.get_by_signer(&signer_index).map(|data| data.node_id()) else {
            tracing::trace!("on_next_round_incoming_request: lock signer is not in the bk set");
            return OnNextRoundIncomingRequestResult::DoNothing;
        };

        // TODO: add signature verification of the request. Low pri: hard to tell what is cheaper for the node: reply to all of malicious request and spam another node or to spend cpu verifying signatures.
        let prefinalized =
            find_next_prefinalized(&parent_state, thread_identifier, &self.block_state_repository);
        if let Some(prefinalized) = prefinalized {
            tracing::trace!(
                "on_next_round_incoming_request: reject, we already have a prefinalized block on this height: {:?} {:?}",
                prefinalized.block_identifier(),
                prefinalized.guarded(|e| *e.block_seq_no()),
            );
            let Some(_ensure_block_is_available) =
                self.get_block(prefinalized.block_identifier(), &unprocessed_blocks_cache)
            else {
                return if prefinalized.guarded(|e| e.is_finalized()) {
                    OnNextRoundIncomingRequestResult::RejectTooOldRequest(node_id.into())
                } else {
                    tracing::trace!(
                        "on_next_round_incoming_request: can't load next prefinalized child"
                    );
                    OnNextRoundIncomingRequestResult::DoNothing
                };
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
                            OnNextRoundIncomingRequestResult::RejectTooOldRequest(node_id.into())
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
                    .prefinalized_block(
                        NetBlock::with_envelope(prefinalized_block.as_ref()).unwrap(),
                    )
                    .proof_of_prefinalization(
                        prefinalized.guarded(|e| e.prefinalization_proof().clone().unwrap()),
                    )
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
        if let Some(current_lock) =
            self.action_lock.get(&block_height, &self.block_state_repository)
        {
            if current_lock.locked_round() > lock.locked_round() {
                tracing::trace!(
                    "on_next_round_incoming_request: reject. BlockRound is lower than our locked"
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

        if next_round_message.lock().data().next_auth_node_id() != &self.node_identifier {
            tracing::trace!(
                "on_next_round_incoming_request: do nothing. request is not for this node"
            );
            return OnNextRoundIncomingRequestResult::DoNothing;
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

        let votes_target = (bk_set.len() + 1).div_ceil(2);
        if collected_voters.len() < votes_target {
            // Less than 50% + 1 of signers yet.
            tracing::trace!(
                "on_next_round_incoming_request: not enough signers ({}), bk_set.len()={}",
                collected_voters.len(),
                bk_set.len()
            );
            return OnNextRoundIncomingRequestResult::DoNothing;
        }
        let mut max_locked_block: Option<Arc<Envelope<GoshBLS, AckiNackiBlock>>> = None;
        let mut max_locked_block_round: Option<BlockRound> = None;
        let mut locked_none_count = 0;
        for e in collected_requests.iter() {
            if let Some(block_id) = e.lock().data().locked_block() {
                let Some(block) = self.get_block(block_id, &unprocessed_blocks_cache) else {
                    // An unknown block id. Skip it.
                    continue;
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
            } else {
                locked_none_count += 1;
            }
        }
        if max_locked_block.is_none() && locked_none_count < votes_target {
            tracing::trace!(
                    "on_next_round_incoming_request: there is a quorum already. However this node has no block to share. (block identifiers only). Not enough votes to produce a new block",
                );
            return OnNextRoundIncomingRequestResult::DoNothing;
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

        // TODO: <closed_round> Must be durable.
        self.closed_round.insert(round_key.0, *round);
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
                .target_type(AttestationTargetType::Primary)
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

            // Produce block for this round AND broadcast it as a NextRoundSuccess
            tracing::trace!("Resend existing locked block from a top round {thread_identifier:?}");
            if let Some(m) = self.block_repository.get_metrics() {
                m.report_authority_switch_direct_resent(thread_identifier);
            }
            // Note: block production is not needed so far.
            // Block producer kicks in when there were no block in a previous round or the majority
            // of the block keepers had no block locked.
            let Ok(envelope) = Envelope::sealed(
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
            ) else {
                start_shutdown();
                tracing::error!("Node does not have valid key to sign message");
                return OnNextRoundIncomingRequestResult::DoNothing;
            };

            return OnNextRoundIncomingRequestResult::Broadcast(envelope);
        }
        let Some(parent_block_time) = parent_state.guarded(|e| *e.block_time_ms()) else {
            tracing::trace!("on_next_round_incoming_request: parent block time is not set");
            return OnNextRoundIncomingRequestResult::DoNothing;
        };
        let now = {
            let start = SystemTime::now();
            let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("time should go forward");
            TryInto::<u64>::try_into(since_the_epoch.as_millis()).unwrap()
        };
        if parent_block_time > now {
            tracing::warn!(
                "on_next_round_incoming_request: local clock and prev BP clock is out of sync"
            );
            return OnNextRoundIncomingRequestResult::DoNothing;
        }
        let duration_from_parent_block = Duration::from_millis(now - parent_block_time);
        let CalculateRoundResult { round: local_round, round_remaining_time } = self
            .round_buckets
            .calculate_round(duration_from_parent_block, bk_set.len().try_into().unwrap());
        // TODO: investigate if the second check is a good guess or not.
        if local_round > *round || round_remaining_time < std::time::Duration::from_millis(330) {
            tracing::warn!(
                "on_next_round_incoming_request: will not have enough time to produce a block before other nodes would be locked in another round"
            );
            return OnNextRoundIncomingRequestResult::DoNothing;
        }
        // Produce block for this round AND broadcast it as a NextRoundSuccess
        match self.block_producers
            .as_ref()
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
            ))  {
                Ok(()) => {},
                Err(e) => {
                    if SHUTDOWN_FLAG.get() != Some(&true) {
                        panic!("Failed to send BP start cmd: {e}");
                    }
                }
            }
        tracing::trace!("on_next_round_incoming_request: StartingBlockProducer: {round}");
        OnNextRoundIncomingRequestResult::StartingBlockProducer
    }

    pub fn on_block_finalized(&mut self, block_height: &BlockHeight) {
        self.action_lock.drop_old_locks(block_height);
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

    pub fn on_next_round_success(&mut self, next_round_success: &NextRoundSuccess) {
        let block_height = next_round_success.block_height();
        let round = *next_round_success.round();
        let Ok(proposed_block) = next_round_success.proposed_block().get_envelope() else {
            tracing::error!("Incoming next round success contains invalid proposed block");
            return;
        };
        let block_round = proposed_block.data().get_common_section().round;
        let parent_id = proposed_block.data().parent();
        let parent_state =
            self.block_state_repository.get(&parent_id).expect("must have parent block state");
        let parent_state_prefinalization_proof = match parent_state
            .guarded(|e| e.prefinalization_proof().clone())
        {
            Some(prefinalization_proof) => Some(prefinalization_proof),
            None => {
                if parent_id == BlockIdentifier::default() {
                    None
                } else {
                    tracing::trace!("Incoming next round success failure: parent block ({parent_id:?}) has no prefinalization proof");
                    return;
                }
            }
        };

        let Some(parent_producer_selector) =
            parent_state.guarded(|e| e.producer_selector_data().clone())
        else {
            tracing::trace!("Incoming next round success failure: parent block ({parent_id:?}) has no producer_selector_data");
            return;
        };

        // TODO: check that proposed block envelope common section contains confirmed bad block nacks
        // let nacked_blocks_bad_block = self
        //     .confirmed_bad_block_nacks
        //     .get(
        //         &SiblingsBlockHeightKey::builder()
        //             .parent_block_identifier(lock.parent_block().clone())
        //             .height(block_height)
        //             .build(),
        //     )
        //     .cloned()
        //     .unwrap_or_default();
        let envelope_nacks = HashSet::from_iter(
            proposed_block
                .data()
                .get_common_section()
                .nacks
                .iter()
                .map(|e| e.data().block_id.clone()),
        );

        // Note: Majority of nodes voted to produce a new block.
        // This means the old block can never ever be accepted by a majority to be finalized.
        let mut to_invalidate = None;
        if let Some(e) = self.action_lock.get_mut(block_height, &self.block_state_repository) {
            if round >= *e.locked_round() {
                e.set_locked_round(round);
                if e.locked_block().is_none() || block_round > e.locked_block().as_ref().unwrap().0
                {
                    to_invalidate = e.locked_block().clone();
                    e.set_locked_block((
                        block_round,
                        BlockRef {
                            block_seq_no: proposed_block.data().seq_no(),
                            block_identifier: proposed_block.data().identifier().clone(),
                        },
                    ));
                }
                e.set_locked_bad_block_nacks(envelope_nacks.clone());
            }
            if *e.locked_round() == round
                && (e.locked_block().is_none()
                    || block_round > e.locked_block().as_ref().unwrap().0)
            {
                let leader = BlockRef {
                    block_seq_no: proposed_block.data().seq_no(),
                    block_identifier: proposed_block.data().identifier().clone(),
                };
                if let Some(prev_locked_block) = e.locked_block().clone() {
                    if prev_locked_block.1.block_identifier != leader.block_identifier {
                        to_invalidate = Some(prev_locked_block);
                    }
                }
                e.set_locked_round(round + 1);
                e.set_locked_block((block_round, leader));
                e.set_locked_bad_block_nacks(envelope_nacks.clone());
            }
        } else {
            let new_lock = ActionLock::builder()
                    .parent_prefinalization_proof(parent_state_prefinalization_proof)
                    .parent_block_producer_selector_data(parent_producer_selector)
                    .parent_block(BlockRef::try_from(&parent_state).unwrap())
                    .locked_round(round)
                    .locked_block(Some((
                        block_round,
                        BlockRef::builder()
                            .block_identifier(proposed_block.data().identifier())
                            .block_seq_no(proposed_block.data().seq_no())
                            .build(),
                    )))
                    .locked_bad_block_nacks(envelope_nacks)
                    //.current_round(round)
                    .build();
            self.action_lock.insert(*block_height, new_lock);
        }
        if let Some(abandoned_by_majority_block_ref) = to_invalidate {
            let abandoned_by_majority_block = self
                .block_state_repository
                .get(&abandoned_by_majority_block_ref.1.block_identifier)
                .unwrap();
            invalidate_branch(
                abandoned_by_majority_block,
                &self.block_state_repository,
                &FilterPrehistoric::builder().block_seq_no(BlockSeqNo::default()).build(),
            );
        }
        // Note:
        // todo!("Push the proposed block for processing");
    }
}
