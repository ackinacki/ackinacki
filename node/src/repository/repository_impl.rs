// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::hash::Hash;
use std::io::Read;
use std::io::Write;
use std::ops::Add;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::ensure;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use database::documents_db::SerializedItem;
use database::sqlite::ArchAccount;
use derive_getters::Getters;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use telemetry_utils::mpsc::InstrumentedSender;
use telemetry_utils::now_ms;
use tvm_block::Augmentation;
use tvm_block::ShardStateUnsplit;
use tvm_types::UInt256;
use typed_builder::TypedBuilder;

use super::accounts::AccountsRepository;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::database::serialize_block::prepare_account_archive_struct;
use crate::helper::get_temp_file_path;
use crate::helper::metrics::BlockProductionMetrics;
use crate::helper::SHUTDOWN_FLAG;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::node::associated_types::AttestationData;
use crate::node::block_state::attestation_target_checkpoints::AncestorBlocksFinalizationCheckpoints;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::block_state::state::AttestationTargets;
use crate::node::block_state::tools::connect;
use crate::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BlockStatistics;
use crate::node::services::sync::ExternalFileSharesBased;
use crate::node::services::sync::StateSyncService;
use crate::node::shared_services::SharedServices;
use crate::node::unprocessed_blocks_collection::UnfinalizedBlocksSnapshot;
use crate::node::unprocessed_blocks_collection::UnfinalizedCandidateBlockCollection;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;
use crate::repository::RepositoryError;
use crate::storage::MessageDBWriterService;
use crate::storage::MessageDurableStorage;
use crate::types::bp_selector::ProducerSelector;
use crate::types::AccountAddress;
use crate::types::AckiNackiBlock;
use crate::types::BlockHeight;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
use crate::utilities::FixedSizeHashMap;
use crate::utilities::FixedSizeHashSet;
use crate::zerostate::ZeroState;

const DEFAULT_OID: &str = "00000000000000000000";
pub const EXT_MESSAGE_STORE_TIMEOUT_SECONDS: i64 = 60;
const _MAX_BLOCK_SEQ_NO_THAT_CAN_BE_BUILT_FROM_ZEROSTATE: u32 = 200;
const MAX_BLOCK_CNT_THAT_CAN_BE_LOADED_TO_PREPARE_STATE: usize = 400;

pub type RepositoryMetadata =
    Arc<Mutex<HashMap<ThreadIdentifier, Arc<Mutex<Metadata<BlockIdentifier, BlockSeqNo>>>>>>;
pub type RepositoryLastExtMessageIndex = Arc<Mutex<HashMap<ThreadIdentifier, u32>>>;

impl AllowGuardedMut for HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockIdentifier>> {}
impl AllowGuardedMut for HashMap<ThreadIdentifier, Arc<OptimisticStateImpl>> {}
impl AllowGuardedMut for HashMap<BlockIdentifier, Arc<OptimisticStateImpl>> {}
impl AllowGuardedMut for HashMap<ThreadIdentifier, UnfinalizedCandidateBlockCollection> {}

pub struct FinalizedBlockStorage {
    per_thread_buffer_size: usize,
    buffer: HashMap<
        ThreadIdentifier,
        FixedSizeHashMap<BlockIdentifier, (BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>)>,
    >,
}

impl std::fmt::Debug for FinalizedBlockStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FinalizedBlockStorage")
    }
}

impl AllowGuardedMut for FinalizedBlockStorage {}

impl FinalizedBlockStorage {
    pub fn new(per_thread_buffer_size: usize) -> Self {
        Self { per_thread_buffer_size, buffer: HashMap::new() }
    }

    pub fn store(&mut self, block_state: BlockState, block: Envelope<GoshBLS, AckiNackiBlock>) {
        let block_id = block.data().identifier();
        let thread_id = block.data().get_common_section().thread_id;
        self.buffer
            .entry(thread_id)
            .and_modify(|e| {
                e.insert(block_id.clone(), (block_state.clone(), Arc::new(block.clone())));
            })
            .or_insert_with(|| {
                let mut thread_buffer = FixedSizeHashMap::new(self.per_thread_buffer_size);
                thread_buffer.insert(block_id.clone(), (block_state.clone(), block.clone().into()));
                thread_buffer
            });
        if let Some(threads_table) = &block.data().get_common_section().threads_table {
            for thread_id in threads_table.list_threads() {
                if thread_id.is_spawning_block(&block_id) {
                    self.buffer
                        .entry(*thread_id)
                        .and_modify(|e| {
                            e.insert(
                                block_id.clone(),
                                (block_state.clone(), Arc::new(block.clone())),
                            );
                        })
                        .or_insert_with(|| {
                            let mut thread_buffer =
                                FixedSizeHashMap::new(self.per_thread_buffer_size);
                            thread_buffer.insert(
                                block_id.clone(),
                                (block_state.clone(), block.clone().into()),
                            );
                            thread_buffer
                        });
                }
            }
        }
    }

    pub fn find(
        &self,
        block_identifier: &BlockIdentifier,
        hint: &[ThreadIdentifier],
    ) -> Option<Arc<Envelope<GoshBLS, AckiNackiBlock>>> {
        for thread_id in hint.iter() {
            if let Some(result) = self.buffer.get(thread_id).and_then(|e| e.get(block_identifier)) {
                return Some(result.1.clone());
            }
        }
        for thread_id in self.buffer.keys() {
            if let Some(result) = self.buffer.get(thread_id).and_then(|e| e.get(block_identifier)) {
                return Some(result.1.clone());
            }
        }
        None
    }

    pub fn buffer(
        &self,
    ) -> &HashMap<
        ThreadIdentifier,
        FixedSizeHashMap<BlockIdentifier, (BlockState, Arc<Envelope<GoshBLS, AckiNackiBlock>>)>,
    > {
        &self.buffer
    }
}

pub struct BkSetUpdate {
    pub seq_no: u32,
    pub current: Option<Arc<BlockKeeperSet>>,
    pub future: Option<Arc<BlockKeeperSet>>,
}

// TODO: divide repository into 2 entities: one for blocks, one for states with weak refs (for not
// to store optimistic states longer than necessary)
pub struct RepositoryImpl {
    data_dir: PathBuf,
    zerostate_path: Option<PathBuf>,
    metadatas: RepositoryMetadata,
    // unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection, -> Node, + all modules that read it
    // change get_block -> get_finalized_block
    saved_states: Arc<Mutex<HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockIdentifier>>>>,
    thread_last_finalized_state: Arc<Mutex<HashMap<ThreadIdentifier, Arc<OptimisticStateImpl>>>>,
    shared_services: SharedServices,
    nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
    block_state_repository: BlockStateRepository,
    optimistic_state: Arc<Mutex<HashMap<BlockIdentifier, Arc<OptimisticStateImpl>>>>,
    accounts: AccountsRepository,
    split_state: bool,
    metrics: Option<BlockProductionMetrics>,
    message_db: MessageDurableStorage,
    message_storage_service: MessageDBWriterService,
    states_cache_size: usize,
    finalized_blocks: Arc<Mutex<FinalizedBlockStorage>>,
    bk_set_update_tx: InstrumentedSender<BkSetUpdate>,
    unfinalized_blocks: Arc<Mutex<HashMap<ThreadIdentifier, UnfinalizedCandidateBlockCollection>>>,
    last_message_for_acc: Arc<Mutex<HashMap<AccountAddress, MessageIdentifier>>>,
}

#[allow(dead_code)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug)]
enum OID {
    ID(String),
    #[allow(dead_code)]
    SingleRow,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct Metadata<TBlockIdentifier: Hash + Eq, TBlockSeqNo> {
    last_finalized_block_id: TBlockIdentifier,
    last_finalized_block_seq_no: TBlockSeqNo,
    pub last_finalized_producer_id: Option<NodeIdentifier>,
}

#[derive(Serialize, Deserialize)]
pub struct WrappedExtMessage<TMessage: Clone> {
    pub index: u32,
    pub message: TMessage,
    pub timestamp: DateTime<Utc>,
}

impl<TMessage: Clone> WrappedExtMessage<TMessage> {
    pub fn new(index: u32, message: TMessage) -> Self {
        Self {
            index,
            message,
            timestamp: Utc::now().add(TimeDelta::seconds(EXT_MESSAGE_STORE_TIMEOUT_SECONDS)),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct ExtMessages<TMessage: Clone> {
    pub queue: Vec<WrappedExtMessage<TMessage>>,
}

#[derive(TypedBuilder, Serialize, Deserialize, Getters)]
pub struct ThreadSnapshot {
    optimistic_state: Vec<u8>,
    cross_thread_ref_data: Vec<CrossThreadRefData>,
    db_messages: Vec<Vec<Arc<WrappedMessage>>>,
    finalized_block: Envelope<GoshBLS, AckiNackiBlock>,
    bk_set: BlockKeeperSet,
    finalized_block_stats: BlockStatistics,
    attestation_target: AttestationTargets,
    producer_selector: ProducerSelector,
    block_height: BlockHeight,
    prefinalization_proof: Envelope<GoshBLS, AttestationData>,
}

impl<TMessage> Default for ExtMessages<TMessage>
where
    TMessage: Clone,
{
    fn default() -> Self {
        Self { queue: vec![] }
    }
}

pub fn load_from_file<T: for<'de> Deserialize<'de>>(
    file_path: &PathBuf,
) -> anyhow::Result<Option<T>> {
    if !file_path.exists() {
        return Ok(None);
    }
    let mut file = File::open(file_path)?;
    let mut buffer = vec![];
    file.read_to_end(&mut buffer)?;
    let data = bincode::deserialize::<T>(&buffer)?;
    Ok(Some(data))
}

pub fn save_to_file<T: Serialize>(
    file_path: &PathBuf,
    data: &T,
    force_sync: bool,
) -> anyhow::Result<()> {
    let buffer = bincode::serialize(&data)?;
    let parent_dir = if let Some(path) = file_path.parent() {
        fs::create_dir_all(path)?;
        path.to_owned()
    } else {
        PathBuf::new()
    };

    let tmp_file_path = get_temp_file_path(&parent_dir);
    let mut file = File::create(&tmp_file_path)?;
    file.write_all(&buffer)?;
    if cfg!(feature = "sync_files") || force_sync {
        file.sync_all()?;
    }
    drop(file);
    std::fs::rename(tmp_file_path, file_path)?;
    tracing::trace!("File saved: {:?}", file_path);
    Ok(())
}

impl Clone for RepositoryImpl {
    // TODO: Repository object can consume great amount of memory, so it's better not to copy it
    fn clone(&self) -> Self {
        Self {
            data_dir: self.data_dir.clone(),
            zerostate_path: self.zerostate_path.clone(),
            metadatas: self.metadatas.clone(),
            saved_states: self.saved_states.clone(),
            thread_last_finalized_state: self.thread_last_finalized_state.clone(),
            shared_services: self.shared_services.clone(),
            nack_set_cache: Arc::clone(&self.nack_set_cache),
            block_state_repository: self.block_state_repository.clone(),
            optimistic_state: self.optimistic_state.clone(),
            accounts: self.accounts.clone(),
            split_state: self.split_state,
            metrics: self.metrics.clone(),
            message_storage_service: self.message_storage_service.clone(),
            message_db: self.message_db.clone(),
            states_cache_size: self.states_cache_size,
            finalized_blocks: Arc::clone(&self.finalized_blocks),
            bk_set_update_tx: self.bk_set_update_tx.clone(),
            unfinalized_blocks: self.unfinalized_blocks.clone(),
            last_message_for_acc: self.last_message_for_acc.clone(),
        }
    }
}

impl RepositoryImpl {
    // TODO: remove option from zerostate_path
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        data_dir: PathBuf,
        zerostate_path: Option<PathBuf>,
        states_cache_size: usize,
        shared_services: SharedServices,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        split_state: bool,
        block_state_repository: BlockStateRepository,
        metrics: Option<BlockProductionMetrics>,
        accounts_repository: AccountsRepository,
        message_db: MessageDurableStorage,
        finalized_blocks: Arc<Mutex<FinalizedBlockStorage>>,
        bk_set_update_tx: InstrumentedSender<BkSetUpdate>,
    ) -> Self {
        if let Err(err) = fs::create_dir_all(data_dir.clone()) {
            tracing::error!("Failed to create data dir {:?}: {}", data_dir, err);
        }

        let message_storage_service =
            MessageDBWriterService::new(message_db.clone(), metrics.clone())
                .expect("Failed to init message storage service");

        let metadata = Arc::new(Mutex::new({
            let metadata = Self::load_metadata(&data_dir)
                .expect("Must be able to create or load repository metadata");
            tracing::trace!("Loaded metadata: {:?}", metadata);
            metadata
        }));

        let mut thread_last_finalized_state = HashMap::new();
        {
            let mut metadatas = metadata.lock();
            if let Some(path) = &zerostate_path {
                let zerostate: ZeroState =
                    ZeroState::load_from_file(path).expect("Failed to load zerostate");
                for thread_id in zerostate.list_threads() {
                    let optimistic_state = zerostate
                        .state(thread_id)
                        .expect("Failed to load state from zerostate")
                        .clone();

                    thread_last_finalized_state.insert(*thread_id, Arc::new(optimistic_state));
                    if !metadatas.contains_key(thread_id) {
                        metadatas.insert(
                            *thread_id,
                            Arc::new(
                                Mutex::new(Metadata::<BlockIdentifier, BlockSeqNo>::default()),
                            ),
                        );
                    }
                }
            };
        }

        let repo_impl = Self {
            data_dir: data_dir.clone(),
            zerostate_path,
            metadatas: metadata,
            saved_states: Default::default(),
            thread_last_finalized_state: Arc::new(Mutex::new(thread_last_finalized_state)),
            shared_services,
            nack_set_cache: Arc::clone(&nack_set_cache),
            block_state_repository: block_state_repository.clone(),
            optimistic_state: Arc::new(Mutex::new(HashMap::new())),
            accounts: accounts_repository,
            split_state,
            metrics,
            message_db: message_db.clone(),
            message_storage_service,
            states_cache_size,
            finalized_blocks,
            bk_set_update_tx,
            unfinalized_blocks: Arc::new(Mutex::new(HashMap::new())),
            last_message_for_acc: Arc::new(Mutex::new(HashMap::new())),
        };

        let optimistic_dir = format!(
            "{}/{}/",
            repo_impl.data_dir.clone().to_str().unwrap(),
            repo_impl.get_optimistic_state_path(),
        );
        if let Ok(paths) = std::fs::read_dir(optimistic_dir) {
            for state_path in paths {
                if let Ok(path) = state_path.map(|de| de.path()) {
                    if let Some(id_str) = path.file_name().and_then(|os_str| os_str.to_str()) {
                        if let Ok(block_id) = BlockIdentifier::from_str(id_str) {
                            tracing::trace!(
                                "RepositoryImpl::new reading optimistic state: {block_id:?}"
                            );
                            if let Ok(state) = OptimisticStateImpl::load_from_file(&path) {
                                let seq_no = state.get_block_info().prev1().unwrap().seq_no;
                                {
                                    let mut all_states = repo_impl.saved_states.lock();
                                    let saved_states =
                                        all_states.entry(state.thread_id).or_default();
                                    saved_states.insert(BlockSeqNo::from(seq_no), block_id);
                                }
                            };
                        }
                    }
                }
            }
        }
        {
            let states = &repo_impl.saved_states.lock();
            tracing::trace!("Repo saved states: {:?}", states);
        }
        let guarded = repo_impl.metadatas.lock();
        for (thread_id, metadata) in guarded.iter() {
            let metadata = metadata.lock();
            if metadata.last_finalized_block_id != BlockIdentifier::default() {
                tracing::trace!("load finalized state {:?}", metadata.last_finalized_block_id);
                let state = repo_impl
                    .get_optimistic_state(&metadata.last_finalized_block_id, thread_id, None)
                    .expect("Failed to get last finalized state")
                    .expect("Failed to load last finalized state");
                repo_impl.thread_last_finalized_state.guarded_mut(|e| e.insert(*thread_id, state));
            }
        }

        // Node repo can contain old blocks which are useless and just consume space
        // and increase metadata size. But we must check that they were successfully stored in the
        // archive
        // tracing::trace!("Start checking old blocks");
        // for (thread_id, metadata) in guarded.iter() {
        //     let metadata = {
        //         let metadata = metadata.lock();
        //         metadata.clone()
        //     };
        //
        //     let last_finalized_seq_no = metadata.last_finalized_block_seq_no;
        //     let sqlite_helper_config = json!({
        //         "data_dir": data_dir
        //     })
        //     .to_string();
        //     let sqlite_helper_raw = Arc::new(
        //         SqliteHelper::from_config(&sqlite_helper_config)
        //             .expect("Failed to create archive DB helper"),
        //     );
        //     for (block_id, block_seq_no) in metadata.block_id_to_seq_no {
        //         if block_seq_no < last_finalized_seq_no {
        //             let markers =
        //                 repo_impl.load_markers(&block_id).expect("Failed to load block markers");
        //             if let Some(true) = markers.is_finalized {
        //                 if repo_impl
        //                     .is_block_present_in_archive(&block_id)
        //                     .expect("Failed to check block")
        //                 {
        //                     let _ = repo_impl.erase_block(&block_id, thread_id);
        //                 } else {
        //                     if let Ok(Some(block)) = repo_impl.get_block(&block_id) {
        //                         if let Ok(Some(state)) = repo_impl.get_optimistic_state(
        //                             &block_id,
        //                             block_keeper_sets.clone(),
        //                             Arc::clone(&nack_set_cache),
        //                         ) {
        //                             write_to_db(
        //                                 sqlite_helper_raw.clone(),
        //                                 block,
        //                                 state.shard_state.shard_state,
        //                                 state.shard_state.shard_state_cell,
        //                             )
        //                             .expect("Failed to store old finalized block in archive");
        //                         }
        //                     }
        //                     let _ = repo_impl.erase_block(&block_id, thread_id);
        //                 }
        //             } else {
        //                 let _ = repo_impl.erase_block(&block_id, thread_id);
        //             }
        //         }
        //     }
        // }
        // tracing::trace!("Finished checking old blocks");

        drop(guarded);
        tracing::trace!("repository init finished");
        repo_impl
    }

    fn is_block_finalized(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>> {
        let markers = self.block_state_repository.get(block_id)?;
        let result = markers.guarded(|e| e.is_finalized());
        Ok(Some(result))
    }

    pub fn nack_set_cache(&self) -> Arc<Mutex<FixedSizeHashSet<UInt256>>> {
        self.nack_set_cache.clone()
    }

    fn get_path(&self, path: &str, oid: String) -> PathBuf {
        PathBuf::from(format!("{}/{}/{}", self.data_dir.clone().to_str().unwrap(), path, oid))
    }

    pub fn finalized_blocks_mut(&mut self) -> &mut Arc<Mutex<FinalizedBlockStorage>> {
        &mut self.finalized_blocks
    }

    #[cfg(test)]
    fn save<T: Serialize>(&self, path: &str, oid: OID, data: &T) -> anyhow::Result<()> {
        let oid = match oid {
            OID::ID(value) => value,
            OID::SingleRow => DEFAULT_OID.to_owned(),
        };
        save_to_file(&self.get_path(path, oid), data, true)?;
        Ok(())
    }

    fn get_metadata_for_thread(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Arc<Mutex<Metadata<BlockIdentifier, BlockSeqNo>>>> {
        self.metadatas
            .lock()
            .get(thread_id)
            .cloned()
            .ok_or(anyhow::format_err!("Failed to get metadata for thread: {thread_id:?}"))
    }

    #[allow(dead_code)]
    fn load<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        oid: OID,
    ) -> anyhow::Result<Option<T>> {
        let oid = match oid {
            OID::ID(value) => value,
            OID::SingleRow => DEFAULT_OID.to_owned(),
        };
        let oid_file = self.get_path(path, oid);
        let data = load_from_file(&oid_file)
            .unwrap_or_else(|_| panic!("Failed to load file: {}", oid_file.display()));
        Ok(data)
    }

    fn _remove(&self, _path: &str, _oid: OID) -> anyhow::Result<()> {
        // let oid = match oid {
        //     OID::ID(value) => value,
        //     OID::SingleRow => DEFAULT_OID.to_owned(),
        // };
        // let _ = std::fs::remove_file(self.get_path(path, oid));
        Ok(())
    }

    #[cfg(test)]
    fn exists(&self, path: &str, oid: OID) -> bool {
        let oid = match oid {
            OID::ID(value) => value,
            OID::SingleRow => DEFAULT_OID.to_owned(),
        };
        let res = std::path::Path::new(&self.get_path(path, oid)).exists();
        res
    }

    // key helpers
    fn get_optimistic_state_path(&self) -> &str {
        "optimistic_state"
    }

    fn get_metadata_path() -> &'static str {
        "metadata"
    }

    fn get_blocks_path() -> &'static str {
        "blocks"
    }

    fn get_attestations_path(&self) -> &str {
        "attestations"
    }

    pub fn get_metrics(&self) -> Option<BlockProductionMetrics> {
        self.metrics.clone()
    }

    pub fn load_metadata(
        data_dir: &Path,
    ) -> anyhow::Result<HashMap<ThreadIdentifier, Arc<Mutex<Metadata<BlockIdentifier, BlockSeqNo>>>>>
    {
        let path = Self::get_metadata_path();
        let path = PathBuf::from(format!(
            "{}/{}/{}",
            data_dir.to_str().unwrap(),
            path,
            DEFAULT_OID.to_owned()
        ));
        let data =
            load_from_file::<HashMap<ThreadIdentifier, Metadata<BlockIdentifier, BlockSeqNo>>>(
                &path,
            )
            .unwrap_or_else(|_| panic!("Failed to load file: {}", path.display()));

        let result = {
            if let Some(metadatas) = data {
                metadatas
                    .into_iter()
                    .map(|(thread, metadata)| (thread, Arc::new(Mutex::new(metadata))))
                    .collect()
            } else {
                HashMap::new()
            }
        };
        Ok(result)
    }

    fn save_metadata(data_dir: &Path, metadatas: RepositoryMetadata) -> anyhow::Result<()> {
        let path = Self::get_metadata_path();
        let path = PathBuf::from(format!(
            "{}/{}/{}",
            data_dir.to_str().unwrap(),
            path,
            DEFAULT_OID.to_owned()
        ));
        let metadatas: HashMap<ThreadIdentifier, Metadata<BlockIdentifier, BlockSeqNo>> =
            { metadatas.lock().clone().into_iter().map(|(k, v)| (k, v.lock().clone())).collect() };
        save_to_file(&path, &metadatas, true)?;
        Ok(())
    }

    pub(crate) fn get_blocks_dir_path(&self) -> PathBuf {
        let mut path = self.data_dir.clone();
        path.push(Self::get_blocks_path());
        path
    }

    fn save_block(
        data_dir: &Path,
        block: &<RepositoryImpl as Repository>::CandidateBlock,
    ) -> anyhow::Result<()> {
        let mut path = data_dir.to_path_buf();
        path.push(block.data().identifier().to_string());
        save_to_file(&path, block, true)?;
        Ok(())
    }

    pub(crate) fn load_block(
        data_dir: &Path,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<Option<<RepositoryImpl as Repository>::CandidateBlock>> {
        let mut path = data_dir.to_path_buf();
        path.push(block_id.to_string());
        load_from_file::<<RepositoryImpl as Repository>::CandidateBlock>(&path)
    }

    fn erase_markers(&self, _block_id: &BlockIdentifier) -> anyhow::Result<()> {
        // let _ = std::fs::remove_file(self.get_path("markers", format!("{block_id:?}")));
        Ok(())
    }

    fn save_accounts(
        &self,
        block_id: BlockIdentifier,
        accounts: HashMap<String, SerializedItem>,
    ) -> anyhow::Result<()> {
        if accounts.keys().len() == 0 {
            return Ok(());
        }
        tracing::trace!("Saving accounts (block: {block_id}) into the repo...");
        Ok(())
    }

    fn load_finalized_candidate_block(
        &self,
        identifier: &BlockIdentifier,
    ) -> anyhow::Result<Option<Arc<<RepositoryImpl as Repository>::CandidateBlock>>> {
        let envelope = self.finalized_blocks.guarded(|e| e.find(identifier, &[]));
        Ok(envelope)
    }

    fn erase_candidate_block(&self, _block_id: &BlockIdentifier) -> anyhow::Result<()> {
        // let path = self.get_path("blocks", format!("{block_id:?}"));
        // let _ = std::fs::remove_file(path);
        // let mut cache = self.blocks_cache.lock();
        // let _ = cache.pop_entry(block_id);
        Ok(())
    }

    fn clear_optimistic_states(&self, thread_id: &ThreadIdentifier) -> anyhow::Result<()> {
        let Some((_, last_finalized_seq_no)) =
            self.select_thread_last_finalized_block(thread_id)?
        else {
            return Ok(());
        };
        const RETAIN: bool = true;
        const REMOVE: bool = false;
        self.optimistic_state.guarded_mut(|states| {
            states.retain(|_, e| {
                if e.get_thread_id() != thread_id {
                    return RETAIN;
                }
                if e.get_block_seq_no() >= &last_finalized_seq_no {
                    return RETAIN;
                }
                REMOVE
            })
        });

        // TODO: refactor and revisit this code
        // CRITICAL! SHOULD NOT BE MERGED TO DEV BEFORE FIXING

        // let mut saved_states =
        //     self.saved_states.guarded(|e| e.get(thread_id).cloned().unwrap_or_default());
        // let mut keys_to_remove = vec![];
        // // retain finalized blocks (fork blocks could save their state)
        // for (block_seq_no, block_id) in saved_states.iter() {
        //     if let Ok(state) = self.block_state_repository.get(block_id) {
        //         if !state.guarded(|e| e.is_finalized()) {
        //             keys_to_remove.push(*block_seq_no);
        //         }
        //     } else {
        //         keys_to_remove.push(*block_seq_no);
        //     }
        // }
        //
        // saved_states.retain(|block_seq_no, _block_id| !keys_to_remove.contains(block_seq_no));
        // // iter saved finalized states
        // for (block_seq_no, block_id) in saved_states.iter() {
        //     if *block_seq_no > last_finalized_seq_no {
        //         break;
        //     }
        //     if !self.is_block_present_in_archive(block_id).unwrap_or(false) {
        //         break;
        //     }
        //     keys_to_remove.push(*block_seq_no);
        // }
        // if keys_to_remove.is_empty() {
        //     return Ok(());
        // }
        // // Leave only the latest state
        // let last = keys_to_remove.pop();
        // // for k in &keys_to_remove {
        // //     let block_id = saved_states.get(k).unwrap();
        // //     tracing::trace!("Clear optimistic state for {k} {block_id:?}");
        // //     let optimistic_state_path = self.get_optimistic_state_path();
        // //     self.remove(optimistic_state_path, OID::ID(block_id.to_string()))?;
        // // }
        // if self.split_state && !keys_to_remove.is_empty() {
        //     if let Some(block_id) = saved_states.get(&last.unwrap()) {
        //         let last = self
        //             .get_optimistic_state(block_id, thread_id, None)?
        //             .ok_or_else(|| anyhow::format_err!("Optimistic state must be present"))?;
        //         let last = Arc::unwrap_or_clone(last);
        //         let block_lt = self
        //             .get_block_from_repo_or_archive(block_id)?
        //             .data()
        //             .tvm_block()
        //             .read_info()
        //             .map_err(|err| anyhow::format_err!("Failed to read block info: {err}"))?
        //             .end_lt();
        //         let accounts = self.accounts.clone();
        //         let last = last.get_shard_state().read_accounts().map_err(|err| {
        //             anyhow::format_err!("Failed to read last shard accounts: {}", err)
        //         })?;
        //         let thread_id = *thread_id;
        //         // TODO: save handler
        //         std::thread::Builder::new()
        //             .name("Clear old accounts".to_owned())
        //             .spawn(move || accounts.clear_old_accounts(&thread_id, &last, block_lt))?;
        //     }
        // }
        // self.saved_states.guarded_mut(|e| {
        //     let thread_states = e.entry(*thread_id).or_default();
        //     thread_states.retain(|seq_no, _| !keys_to_remove.contains(seq_no));
        // });
        // tracing::trace!("repo saved_states={:?}", saved_states);
        Ok(())
    }

    fn try_load_state_from_archive(
        &self,
        block_id: &BlockIdentifier,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        min_state: Option<Arc<<Self as Repository>::OptimisticState>>,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Option<<Self as Repository>::OptimisticState>> {
        tracing::trace!("try_load_state_from_archive: {block_id:?}");

        self.metrics.as_ref().inspect(|m| m.report_load_from_archive_invoke(thread_id));

        let mut available_states: HashSet<BlockIdentifier> = HashSet::from_iter(
            self.saved_states
                .guarded(|e| e.clone())
                .values()
                .flat_map(|v| v.clone().values().cloned().collect::<Vec<_>>()),
        );

        let finalized_state =
            self.thread_last_finalized_state.guarded(|map| map.get(thread_id).cloned());
        if let Some(finalized_state) = finalized_state.as_ref() {
            available_states.insert(finalized_state.block_id.clone());
        }
        if self.zerostate_path.is_some() {
            available_states.insert(BlockIdentifier::default());
        };
        {
            let cache = self.optimistic_state.guarded(|e| e.keys().cloned().collect::<Vec<_>>());
            available_states.extend(cache);
        }
        if let Some(min_state) = min_state.as_ref() {
            available_states.insert(min_state.block_id.clone());
        }
        tracing::trace!("try_load_state_from_archive: available_states: {:?}", available_states);

        let mut blocks = vec![];
        let mut block_id = block_id.clone();

        let state_id = loop {
            let block = self.get_block_from_repo_or_archive(&block_id, thread_id)?;
            tracing::trace!(
                "try_load_state_from_archive: loaded block: {:?} {block_id:?}",
                block.data().seq_no()
            );
            if let Some(min_state) = min_state.as_ref() {
                if block.data().seq_no() < min_state.block_seq_no {
                    anyhow::bail!(RepositoryError::DepthSearchMinStateLimitReached);
                }
            }
            blocks.push(block.data().clone());
            if blocks.len() > MAX_BLOCK_CNT_THAT_CAN_BE_LOADED_TO_PREPARE_STATE {
                anyhow::bail!(RepositoryError::DepthSearchBlockCountLimitReached);
            }
            let parent_id = block.data().parent();
            if available_states.contains(&parent_id) {
                break parent_id;
            }
            block_id = parent_id;
        };
        blocks.reverse();

        let state = if state_id == BlockIdentifier::default() {
            tracing::trace!("Load state from zerostate");
            let mut zero_block = <Self as Repository>::OptimisticState::zero();
            if let Some(path) = &self.zerostate_path {
                let zerostate = ZeroState::load_from_file(path)?;
                zero_block = zerostate.state(thread_id)?.clone();
            };
            Arc::new(zero_block)
        } else if let Some(state) = finalized_state.filter(|s| s.block_id == state_id) {
            state
        } else if let Some(state) = min_state.filter(|s| s.block_id == state_id) {
            state
        } else {
            self.get_optimistic_state(&state_id, thread_id, None)?
                .ok_or(anyhow::format_err!("Optimistic state must be present"))?
        };
        let mut state = Arc::unwrap_or_clone(state);
        tracing::trace!("try_load_state_from_archive start applying blocks");
        for block in blocks {
            tracing::trace!("try_load_state_from_archive apply block {:?}", block.identifier());
            self.metrics.as_ref().inspect(|m| m.report_load_from_archive_apply(thread_id));
            state.apply_block(
                &block,
                &self.shared_services,
                self.block_state_repository.clone(),
                Arc::clone(&nack_set_cache),
                self.accounts.clone(),
                self.message_db.clone(),
            )?;
        }

        tracing::trace!("try_load_state_from_archive return state");
        Ok(Some(state))
    }

    pub fn get_nodes_by_threads(&self) -> HashMap<ThreadIdentifier, Option<NodeIdentifier>> {
        let metadata = self.get_all_metadata();
        let metadata_guarded = metadata.lock();
        let bp_id_for_thread_map: HashMap<ThreadIdentifier, Option<NodeIdentifier>> =
            metadata_guarded
                .iter()
                .map(|(k, v)| {
                    let thread_metadata = v.lock();
                    (*k, thread_metadata.last_finalized_producer_id.clone())
                })
                .collect();
        drop(metadata_guarded);

        bp_id_for_thread_map
    }

    pub fn accounts_repository(&self) -> &AccountsRepository {
        &self.accounts
    }

    #[cfg(test)]
    pub fn stub(
        last_finalized_block_id: BlockIdentifier,
        last_finalized_block_seq_no: BlockSeqNo,
        thread_id: ThreadIdentifier,
        block_state_repository: BlockStateRepository,
    ) -> Self {
        use telemetry_utils::mpsc::instrumented_channel;

        use crate::helper::metrics;
        use crate::multithreading::routing::service::RoutingService;
        use crate::storage::MessageDurableStorage;

        let metadata =
            Metadata { last_finalized_block_id, last_finalized_block_seq_no, ..Default::default() };
        let mut metadatas = HashMap::new();
        metadatas.insert(thread_id, Arc::new(Mutex::new(metadata)));
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut tmp_path = tmp_dir.path().to_owned();
        tmp_path.push("archive_db");
        if !tmp_path.exists() {
            File::create(tmp_path.clone()).unwrap();
        }

        let data_dir = PathBuf::default();
        let message_db = MessageDurableStorage::as_noop();
        let message_service = MessageDBWriterService::new(message_db.clone(), None).unwrap();
        let finalized_blocks =
            crate::repository::repository_impl::tests::finalized_blocks_storage();

        let (bk_set_update_tx, _bk_set_update_rx) = instrumented_channel::<BkSetUpdate>(
            None::<metrics::BlockProductionMetrics>,
            metrics::BK_SET_UPDATE_CHANNEL,
        );
        Self {
            accounts: AccountsRepository::new(data_dir.clone(), None, 1),
            data_dir,
            zerostate_path: None,
            metadatas: Arc::new(Mutex::new(metadatas)),
            saved_states: Arc::new(Mutex::new(HashMap::new())),
            thread_last_finalized_state: Arc::new(Mutex::new(HashMap::new())),
            shared_services: SharedServices::test_start(RoutingService::stub().0, u32::MAX),
            nack_set_cache: Arc::new(Mutex::new(FixedSizeHashSet::new(0))),
            block_state_repository,
            optimistic_state: Arc::new(Mutex::new(HashMap::new())),
            split_state: false,
            metrics: None,
            message_db,
            message_storage_service: message_service,
            states_cache_size: 1,
            finalized_blocks,
            bk_set_update_tx,
            unfinalized_blocks: Arc::new(Mutex::new(HashMap::new())),
            last_message_for_acc: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn finalize_synced_block(
        &mut self,
        thread_snapshot: &ThreadSnapshot,
        _state: &OptimisticStateImpl,
    ) -> anyhow::Result<()> {
        let block = thread_snapshot.finalized_block.clone();
        tracing::trace!("Marked synced block as finalized: {:?}", block);
        let block_id = block.data().identifier();
        let seq_no = block.data().seq_no();
        let thread_id = block.data().get_common_section().thread_id;
        // We have already received the last finalized block end sync
        let parent_block_id = block.data().parent();
        if let Some(metadata) = self.metadatas.guarded(|e| e.get(&thread_id).cloned()) {
            if metadata.guarded(|m| m.last_finalized_block_seq_no) >= seq_no {
                return Ok(());
            }
        } else {
            self.init_thread(&thread_id, &block_id)?;
        }
        self.shared_services.exec(|services| {
            services.threads_tracking.init_thread(
                parent_block_id.clone(),
                HashSet::from_iter(vec![thread_id]),
                &mut (&mut services.load_balancing,),
            );
            // let threads: Vec<ThreadIdentifier> = {
            //     if let Some(threads_table) =
            //         block.data().get_common_section().threads_table.as_ref()
            //     {
            //         threads_table.list_threads().copied().collect()
            //     } else {
            //         state.get_produced_threads_table().list_threads().copied().collect()
            //     }
            // };
            // // TODO: init repo for this thread if not init
            // for thread_identifier in threads {
            services.router.join_thread(thread_id);
            // }
        });

        self.mark_block_as_finalized(
            &block,
            self.block_state_repository.get(&block_id)?,
            None::<Arc<ExternalFileSharesBased>>,
        )?;
        self.shared_services.on_block_finalized(
            block.data(),
            self.get_optimistic_state(&block.data().identifier(), &thread_id, None)?
                .expect("set above"),
        );
        // self.clear_unprocessed_till(&seq_no, &current_thread_id)?;
        self.clear_verification_markers(&seq_no, &thread_id)?;

        Ok(())
    }

    pub fn dump_unfinalized_blocks(&self, blocks: UnfinalizedBlocksSnapshot) {
        let root_path = self.get_blocks_dir_path();
        tracing::trace!("dump_state: save unfinalized blocks len: {}", blocks.blocks().len());
        for (block_state, block) in blocks.blocks().values() {
            let block_id = block.data().identifier();
            let res = RepositoryImpl::save_block(&root_path, block);
            tracing::trace!("dump_state: save unfinalized block res: {block_id:?} {:?}", res);
            let res = block_state.guarded_mut(|e| e.dump());
            tracing::trace!("dump_state: save unfinalized block state res: {block_id:?} {:?}", res);
        }
    }

    pub fn dump_state(&self) {
        tracing::trace!("start dumping state");
        for (_, finalized_state) in self.thread_last_finalized_state.guarded(|e| e.clone()).iter() {
            let res = self.store_optimistic(finalized_state.clone());
            tracing::trace!("dump_state: Store optimistic state res: {:?}", res);
        }

        let res = Self::save_metadata(&self.data_dir, self.metadatas.clone());
        tracing::trace!("dump_state: save metadata res: {:?}", res);

        let root_path = self.get_blocks_dir_path();
        for (_thread, blocks) in self.finalized_blocks.guarded(|e| e.buffer.clone()).iter() {
            for (block_id, (block_state, block)) in blocks.blocks() {
                let res = RepositoryImpl::save_block(&root_path, block.as_ref());
                tracing::trace!("dump_state: save finalized block res: {block_id:?} {:?}", res);
                let res = block_state.guarded_mut(|e| e.dump());
                tracing::trace!(
                    "dump_state: save finalized block state res: {block_id:?} {:?}",
                    res
                );
            }
        }
    }
}

impl Repository for RepositoryImpl {
    type Attestation = Envelope<GoshBLS, AttestationData>;
    type BLS = GoshBLS;
    type CandidateBlock = Envelope<GoshBLS, AckiNackiBlock>;
    type EnvelopeSignerIndex = SignerIndex;
    type NodeIdentifier = NodeIdentifier;
    type OptimisticState = OptimisticStateImpl;
    type StateSnapshot = Vec<u8>;

    fn has_thread_metadata(&self, thread_id: &ThreadIdentifier) -> bool {
        self.metadatas.lock().contains_key(thread_id)
    }

    fn load_sent_attestations(
        &self,
    ) -> anyhow::Result<HashMap<ThreadIdentifier, Vec<(BlockSeqNo, Self::Attestation)>>> {
        let path = self.get_attestations_path();
        let path = self.get_path(path, DEFAULT_OID.to_owned());
        let res = load_from_file(&path)?.unwrap_or_default();
        Ok(res)
    }

    fn init_thread(
        &mut self,
        thread_id: &ThreadIdentifier,
        parent_block_id: &BlockIdentifier,
    ) -> anyhow::Result<()> {
        let mut guarded = self.metadatas.lock();
        if guarded.contains_key(thread_id) {
            let metadata = guarded.get(thread_id).unwrap().lock();
            assert_eq!(&metadata.last_finalized_block_id, parent_block_id);
            return Ok(());
        }

        ensure!(self.is_block_finalized(parent_block_id)? == Some(true));
        let optimistic_state = {
            let optimistic_state = self.get_optimistic_state(parent_block_id, thread_id, None)?;
            ensure!(
                optimistic_state.is_some(),
                "thread parent block must have an optimistic state stored"
            );
            optimistic_state.unwrap()
        };
        let parent_block_seq_no = { *optimistic_state.get_block_seq_no() };
        self.thread_last_finalized_state.guarded_mut(|e| {
            if let Some(cur_state_state_seq_no) = e.get(thread_id).map(|state| state.block_seq_no) {
                if cur_state_state_seq_no <= optimistic_state.block_seq_no {
                    e.insert(*thread_id, optimistic_state);
                }
            } else {
                e.insert(*thread_id, optimistic_state);
            }
        });
        let metadata = Metadata::<BlockIdentifier, BlockSeqNo> {
            last_finalized_block_id: parent_block_id.clone(),
            last_finalized_block_seq_no: parent_block_seq_no,
            ..Default::default()
        };
        guarded.insert(*thread_id, Arc::new(Mutex::new(metadata)));
        drop(guarded);
        Self::save_metadata(&self.data_dir, self.metadatas.clone())?;
        Ok(())
    }

    fn get_finalized_block(
        &self,
        identifier: &BlockIdentifier,
    ) -> anyhow::Result<Option<Arc<Self::CandidateBlock>>> {
        self.load_finalized_candidate_block(identifier)
    }

    fn get_block_from_repo_or_archive(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Arc<<Self as Repository>::CandidateBlock>> {
        let unfinalized_block = self
            .unfinalized_blocks
            .guarded(|e| e.get(thread_id).cloned())
            .and_then(|blocks| blocks.get_block_by_id(block_id));
        Ok(if let Some(block) = unfinalized_block {
            block
        } else if let Some(block) = self.get_finalized_block(block_id)? {
            block
        } else {
            anyhow::bail!(RepositoryError::BlockNotFound(format!(
                "get_block_from_repo_or_archive: failed to load block: {block_id:?}"
            )));
        })
    }

    fn last_finalized_optimistic_state(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> Option<Arc<<Self as Repository>::OptimisticState>> {
        self.thread_last_finalized_state.guarded(|e| e.get(thread_id).cloned())
    }

    fn select_thread_last_finalized_block(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Option<(BlockIdentifier, BlockSeqNo)>> {
        // tracing::trace!("select_thread_last_finalized_block: start");
        // TODO: Critical: This "fast-forward" is already broken.
        // It will not survive thread merge operations.
        // TODO: self.finalized_states can be used here
        if let Ok(metadata) = self.get_metadata_for_thread(thread_id) {
            let guarded = metadata.lock();
            let cursor_id = guarded.last_finalized_block_id.clone();
            let cursor_seq_no = guarded.last_finalized_block_seq_no;
            drop(guarded);
            tracing::trace!("select_thread_last_finalized_block: {cursor_seq_no:?} {cursor_id:?}");
            Ok(Some((cursor_id, cursor_seq_no)))
        } else {
            tracing::trace!("select_thread_last_finalized_block: None");
            Ok(None)
        }
    }

    fn mark_block_as_finalized(
        &mut self,
        block: impl Borrow<<Self as Repository>::CandidateBlock>,
        _block_state: BlockState,
        state_sync_service: Option<Arc<impl StateSyncService<Repository = RepositoryImpl>>>,
    ) -> anyhow::Result<()> {
        if SHUTDOWN_FLAG.get() == Some(&true) {
            // NOTE: to prevent unexpected block finalization during shutdown
            return Ok(());
        }
        tracing::trace!("mark_block_as_finalized: {}", block.borrow());
        let block_id = block.borrow().data().identifier();
        let thread_id = block.borrow().data().get_common_section().thread_id;

        let block_state = self.block_state_repository.get(&block_id)?;
        self.finalized_blocks.guarded_mut(|e| e.store(block_state.clone(), block.borrow().clone()));
        let (block_seq_no, bk_set, future_bk_set, received_ms) =
            block_state.guarded_mut(|block_state_in| {
                if !block_state_in.is_finalized() {
                    block_state_in.set_finalized()?;
                }
                anyhow::Ok((
                    (*block_state_in.block_seq_no()).unwrap(),
                    block_state_in.bk_set().clone(),
                    block_state_in.future_bk_set().clone(),
                    block_state_in.event_timestamps.received_ms,
                ))
            })?;
        if thread_id == ThreadIdentifier::default() {
            if let Some(metrics) = &self.metrics {
                if let (Some(bk_set), Some(future_bk_set)) =
                    (bk_set.as_deref(), future_bk_set.as_deref())
                {
                    metrics.report_bk_set(bk_set.len(), future_bk_set.len(), &thread_id)
                }
            }
            let _ = self.bk_set_update_tx.send(BkSetUpdate {
                seq_no: block_seq_no.into(),
                current: bk_set,
                future: future_bk_set,
            });
        }
        let metadata = self.get_metadata_for_thread(&thread_id)?;
        let mut metadata = metadata.lock();

        if block_seq_no > metadata.last_finalized_block_seq_no {
            metadata.last_finalized_block_seq_no = block_seq_no;
            metadata.last_finalized_block_id = block_id.clone();
            metadata.last_finalized_producer_id =
                Some(block.borrow().data().get_common_section().producer_id.clone());
        }
        drop(metadata);

        if let Some(received_ms) = received_ms {
            if let Some(m) = self.metrics.as_mut() {
                m.report_finalization_time(now_ms().saturating_sub(received_ms), &thread_id);
            }
        }

        if let Err(e) = Self::save_metadata(&self.data_dir, self.metadatas.clone()) {
            tracing::error!("Failed to save metadata: {}", e);
            return Err(e);
        }
        // After sync we mark the incoming block as finalized and this check works
        let state = self
            .thread_last_finalized_state
            .guarded(|e| e.get(&thread_id).cloned())
            .expect("Node should have finalized state for thread");

        // After sync we mark the incoming block as finalized and this check works
        if block_id != state.block_id && block_seq_no > state.block_seq_no {
            // TODO: ask why was it here
            // TODO: self.thread_last_finalized_state is static without this apply block

            let new_state = if let Some(saved_state) =
                self.optimistic_state.guarded(|e| e.get(&block_id).cloned())
            {
                tracing::trace!("Finalized block: load state");
                self.thread_last_finalized_state
                    .guarded_mut(|e| e.insert(thread_id, saved_state.clone()));

                #[cfg(feature = "messages_db")]
                {
                    let mut last_message_guard = self.last_message_for_acc.lock();
                    let btree = &saved_state.messages.messages;
                    let new_messages = extract_new_messages(btree, &last_message_guard)?;
                    self.message_storage_service.write(new_messages.clone())?;
                    for (address, vector) in new_messages {
                        if let Some((m_identifier, _)) = vector.last() {
                            last_message_guard.insert(address, m_identifier.clone());
                        }
                    }
                    drop(last_message_guard);
                }

                Arc::clone(&saved_state)
            } else {
                tracing::trace!("Finalized block: apply state");
                let mut state = Arc::unwrap_or_clone(state);
                let (_, _messages): (
                    _,
                    HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
                ) = state.apply_block(
                    block.borrow().data(),
                    &self.shared_services,
                    self.block_state_repository.clone(),
                    self.nack_set_cache().clone(),
                    self.accounts.clone(),
                    self.message_db.clone(),
                )?;

                let state = Arc::new(state);
                self.thread_last_finalized_state
                    .guarded_mut(|e| e.insert(thread_id, Arc::clone(&state)));
                state
            };

            #[cfg(feature = "monitor-accounts-number")]
            if let Some(metrics) = self.metrics.as_ref() {
                metrics.report_accounts_number(new_state.accounts_number, &thread_id);
            }

            if block
                .borrow()
                .data()
                .get_common_section()
                .directives
                .share_state_resources()
                .is_some()
            {
                if let Some(service) = state_sync_service {
                    let full_state = self
                        .get_full_optimistic_state(&block_id, &thread_id, Some(new_state.clone()))?
                        .expect("Must be accessible");
                    service.save_state_for_sharing(full_state)?;
                }
            }

            if let Some(threads_table) = &block.borrow().data().get_common_section().threads_table {
                for thread_id in threads_table.list_threads() {
                    if thread_id.is_spawning_block(&block_id) {
                        self.optimistic_state.guarded_mut(|e| {
                            e.insert(block_id.clone(), new_state.clone());
                        });
                        self.thread_last_finalized_state.guarded_mut(|e| {
                            if !e.contains_key(thread_id) {
                                e.insert(*thread_id, new_state.clone());
                            }
                        });
                    }
                }
            }
        }

        Ok(())
    }

    fn get_zero_state_for_thread(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Arc<Self::OptimisticState>> {
        let mut state = Self::OptimisticState::zero();
        if let Some(path) = &self.zerostate_path {
            let zerostate = ZeroState::load_from_file(path)?;
            state = zerostate.state(thread_id)?.clone();
        };
        Ok(Arc::new(state))
    }

    fn get_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        min_state: Option<Arc<Self::OptimisticState>>,
    ) -> anyhow::Result<Option<Arc<Self::OptimisticState>>> {
        log::info!("RepositoryImpl: get_optimistic_state: {block_id:?}");
        if let Some(cached) = self.optimistic_state.guarded_mut(|e| e.get(block_id).map(Arc::clone))
        {
            return Ok(Some(cached));
        }
        let zero_block_id = <BlockIdentifier>::default();
        log::info!("RepositoryImpl: get_optimistic_state: load {block_id:?}");
        if let Some(state) = self.thread_last_finalized_state.guarded(|e| {
            e.iter().find(|(_, state)| state.block_id == *block_id).map(|(_, state)| state.clone())
        }) {
            return Ok(Some(state.clone()));
        }
        if block_id == &zero_block_id {
            return Ok(self.get_zero_state_for_thread(thread_id).ok());
        }
        let root_path = self.get_optimistic_state_path();
        let path = self.get_path(root_path, block_id.to_string());
        let state: Option<OptimisticStateImpl> =
            if let Ok(state) = OptimisticStateImpl::load_from_file(&path) {
                Some(state)
            } else {
                self.try_load_state_from_archive(
                    block_id,
                    Arc::clone(&self.nack_set_cache),
                    min_state,
                    thread_id,
                )?
            };
        let state = state.map(Arc::new);
        if let Some(state) = &state {
            self.optimistic_state.guarded_mut(|e| e.insert(block_id.clone(), Arc::clone(state)));
        }
        Ok(state)
    }

    // TODO: rename method to indicate it is very expensive to run.
    fn get_full_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        min_state: Option<Arc<Self::OptimisticState>>,
    ) -> anyhow::Result<Option<Arc<Self::OptimisticState>>> {
        let state = self.get_optimistic_state(block_id, thread_id, min_state)?;
        let Some(state) = state else {
            return Ok(None);
        };
        if !self.split_state {
            return Ok(Some(state));
        }
        // TODO:
        // add flag indicating that this state has all external accounts loaded
        // to skip this iterator.
        let mut state = Arc::unwrap_or_clone(state);
        let mut shard_state = state.get_shard_state().as_ref().clone();
        let mut shard_accounts = shard_state
            .read_accounts()
            .map_err(|e| anyhow::format_err!("Failed to read shard accounts: {e}"))?;
        shard_accounts.clone().iterate_accounts(|tvm_account_id, mut shard_acc, aug| {
            let account_id = AccountAddress(tvm_account_id.clone());
                if shard_acc.is_external() {
                    let acc_root = match state.cached_accounts.get(&account_id) {
                        Some((_, acc_root)) => acc_root.clone(),
                        None => {
                            self.accounts.load_account(&account_id, shard_acc.last_trans_hash(), shard_acc.last_trans_lt()).map_err(|err| tvm_types::error!("{}", err))?
                        }
                    };
                    if acc_root.repr_hash() != shard_acc.account_cell().repr_hash() {
                        return Err(tvm_types::error!("External account {tvm_account_id} cell hash mismatch: required: {}, actual: {}", acc_root.repr_hash(), shard_acc.account_cell().repr_hash()));
                    }
                    shard_acc.set_account_cell(acc_root);
                    shard_accounts.insert_with_aug(&tvm_account_id, &shard_acc, &aug)?;
                }
                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("Failed to load accounts: {e}"))?;
        shard_state
            .write_accounts(&shard_accounts)
            .map_err(|e| anyhow::format_err!("Failed to write shard accounts: {e}"))?;
        state.set_shard_state(Arc::new(shard_state));
        let state = Arc::new(state);
        // TODO: check if we want to keep all coounts loaded in mem.
        // self.optimistic_state
        //    .guarded_mut(|e| e.insert(state.block_id.clone(), Arc::clone(&state));

        Ok(Some(state))
    }

    fn erase_block(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        tracing::trace!("erase_block: {:?}", block_id);
        let metadata = self.get_metadata_for_thread(thread_id)?;
        let metadata = metadata.lock(); // guarded
        if metadata.last_finalized_block_id == *block_id {
            return Ok(());
        }
        // assert_ne!(metadata.last_finalized_block_id, *block_id);
        drop(metadata);

        self.erase_markers(block_id)?;
        self.erase_candidate_block(block_id)?;
        Ok(())
    }

    fn clear_verification_markers(
        &self,
        _excluded_starting_seq_no: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        let _metadata = self.get_metadata_for_thread(thread_id)?;
        // TODO: remove calls
        Ok(())
    }

    fn is_block_already_applied(&self, block_id: &BlockIdentifier) -> anyhow::Result<bool> {
        let result =
            self.block_state_repository.get(block_id)?.guarded(|e| e.is_block_already_applied());
        tracing::trace!(?block_id, "is_block_already_applied: {result}");
        Ok(result)
    }

    fn set_state_from_snapshot(
        &mut self,
        snapshot: Self::StateSnapshot,
        cur_thread_id: &ThreadIdentifier,
        _skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
    ) -> anyhow::Result<()> {
        tracing::debug!("set_state_from_snapshot");
        let thread_snapshot: ThreadSnapshot = bincode::deserialize(&snapshot)
            .map_err(|e| anyhow::format_err!("Failed to deserialize snapshot: {e}"))?;
        tracing::debug!("set_state_from_snapshot1");
        let state = <Self as Repository>::OptimisticState::deserialize_from_buf(
            thread_snapshot.optimistic_state(),
        )
        .map_err(|e| anyhow::format_err!("Failed to deserialize state: {e}"))?;
        tracing::debug!("set_state_from_snapshot2");
        let thread_id = state.thread_id;
        tracing::debug!("set_state_from_snapshot: {:?} {:?}", state.thread_id, state.block_id);
        self.store_optimistic(state.clone())?;
        let seq_no = thread_snapshot.finalized_block.data().seq_no();

        if &thread_id == cur_thread_id {
            // TODO: need to store for other threads
            tracing::trace!(
                "set_state_from_snapshot: fin stats: {:?}",
                thread_snapshot.finalized_block_stats()
            );
        }

        let block_state_repo_clone = self.block_state_repository.clone();
        let update_block_state = || {
            let block_state =
                block_state_repo_clone.get(&thread_snapshot.finalized_block.data().identifier())?;
            let children = block_state
                .guarded_mut(|state| {
                    state.set_thread_identifier(
                        thread_snapshot.finalized_block.data().get_common_section().thread_id,
                    )?;
                    state.set_producer(
                        thread_snapshot
                            .finalized_block
                            .data()
                            .get_common_section()
                            .producer_id
                            .clone(),
                    )?;
                    state.set_block_seq_no(thread_snapshot.finalized_block.data().seq_no())?;
                    state.set_block_time_ms(
                        thread_snapshot.finalized_block.data().time().unwrap(),
                    )?;
                    state.set_common_checks_passed()?;
                    state.set_finalized()?;
                    state.set_applied(Instant::now(), Instant::now())?;
                    state.set_signatures_verified()?;
                    state.set_stored(&thread_snapshot.finalized_block)?;
                    let bk_set = Arc::new(thread_snapshot.bk_set.clone());
                    state.set_bk_set(bk_set.clone())?;
                    state.set_descendant_bk_set(bk_set)?;
                    // TODO: need to sync future BK set
                    state.set_future_bk_set(Arc::new(BlockKeeperSet::new()))?;
                    state.set_descendant_future_bk_set(Arc::new(BlockKeeperSet::new()))?;
                    state.set_block_stats(thread_snapshot.finalized_block_stats.clone())?;
                    state.set_block_height(thread_snapshot.block_height)?;
                    state.set_ancestor_blocks_finalization_checkpoints(
                        AncestorBlocksFinalizationCheckpoints::builder()
                            .primary(HashMap::new())
                            .fallback(HashMap::new())
                            .build(),
                    )?;
                    state.set_attestation_target(thread_snapshot.attestation_target)?;
                    state.set_prefinalized(thread_snapshot.prefinalization_proof.clone())?;
                    // TODO: check with alexander.s if it was some kind of fix
                    // state.set_attestation_target(thread_snapshot.attestation_target)?;
                    state.set_producer_selector_data(thread_snapshot.producer_selector.clone())?;
                    state.set_finalizes_blocks(HashSet::new())?;
                    Ok::<Option<HashSet<_>>, anyhow::Error>(
                        state.known_children(cur_thread_id).cloned(),
                    )
                })?
                .unwrap_or_default();

            for child in children {
                block_state_repo_clone
                    .get(&child)?
                    .guarded_mut(|e| e.set_has_parent_finalized())?;
            }
            let parent =
                block_state_repo_clone.get(&thread_snapshot.finalized_block.data().parent())?;

            connect!(parent = parent, child = block_state, &block_state_repo_clone);
            crate::node::services::block_processor::rules::descendant_bk_set::set_descendant_bk_set(
                &block_state,
                &thread_snapshot.finalized_block,
            );
            Ok::<(), anyhow::Error>(())
        };

        if let Some(finalized_seq_no) = self
            .thread_last_finalized_state
            .guarded(|e| e.get(&thread_id).map(|state| state.block_seq_no))
        {
            tracing::trace!(
                "Repo already has finalized state for the thread, seq_no: {}",
                finalized_seq_no
            );
            if seq_no > finalized_seq_no {
                let shard_state = state.get_shard_state();
                self.thread_last_finalized_state
                    .guarded_mut(|e| e.insert(thread_id, Arc::new(state.clone())));
                self.sync_accounts_from_state(shard_state)?;
                update_block_state()?;
            }
        } else {
            self.thread_last_finalized_state
                .guarded_mut(|e| e.insert(thread_id, Arc::new(state.clone())));

            update_block_state()?;
        }
        {
            let mut all_states = self.saved_states.lock();
            let saved_states = all_states.entry(thread_id).or_default();
            saved_states
                .insert(seq_no, thread_snapshot.finalized_block.data().identifier().clone());
        }
        let db_messages = thread_snapshot
            .db_messages
            .clone()
            .into_iter()
            .filter_map(|v| {
                if v.is_empty() {
                    None
                } else {
                    v[0].message.int_dst_account_id().map(|addr| {
                        (
                            addr.into(),
                            v.into_iter()
                                .map(|msg| {
                                    let id = MessageIdentifier::from(msg.deref());
                                    (id, msg)
                                })
                                .collect(),
                        )
                    })
                }
            })
            .collect();
        self.message_storage_service.write(db_messages)?;

        let mut blocks_with_cross_thread_ref_data_set = vec![];
        self.shared_services.exec(|services| {
            for ref_data in thread_snapshot.cross_thread_ref_data.clone().into_iter() {
                blocks_with_cross_thread_ref_data_set.push(ref_data.block_identifier().clone());
                services
                    .cross_thread_ref_data_service
                    .set_cross_thread_ref_data(ref_data)
                    .expect("Failed to load cross-thread-ref-data");
            }
        });
        for block_id in blocks_with_cross_thread_ref_data_set.into_iter() {
            block_state_repo_clone.get(&block_id).unwrap().guarded_mut(|e| {
                if e.has_cross_thread_ref_data_prepared().is_none() {
                    e.set_has_cross_thread_ref_data_prepared().unwrap();
                }
            });
        }

        self.finalize_synced_block(&thread_snapshot, &state)?;

        Ok(())
    }

    fn is_block_suspicious(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>> {
        let guarded = self.block_state_repository.get(block_id)?;
        let result = !guarded.guarded(|e| e.has_all_nacks_resolved());
        Ok(Some(result))
    }

    fn sync_accounts_from_state(
        &mut self,
        shard_state: Arc<ShardStateUnsplit>,
    ) -> anyhow::Result<()> {
        tracing::debug!("syncing accounts from state...");
        let mut arch_accounts: Vec<ArchAccount> = vec![];
        let accounts = shard_state.read_accounts().map_err(|e| anyhow::format_err!("{e}"))?;
        accounts
            .iterate_accounts(|acc_id, v, _| {
                let last_trans_hash = v.last_trans_hash().clone();
                let account = v.read_account().unwrap().as_struct()?;
                if self.split_state {
                    self.accounts
                        .store_account(
                            &AccountAddress(acc_id),
                            &last_trans_hash,
                            v.last_trans_lt(),
                            v.account_cell(),
                        )
                        .map_err(|err| tvm_types::error!("{}", err))?;
                }
                match prepare_account_archive_struct(
                    account.clone(),
                    None,
                    None,
                    last_trans_hash,
                    v.get_dapp_id().cloned(),
                ) {
                    Ok(acc) => arch_accounts.push(acc),
                    Err(e) => {
                        tracing::error!(
                            "Failed to parse account from state: {e}\naccount: {account}"
                        );
                    }
                }

                Ok(true)
            })
            .map_err(|e| anyhow::format_err!("{e}"))?;

        let cnt_accounts = arch_accounts.len();
        tracing::debug!(
            "syncing accounts from state: {} accounts have been sent to sqlite helper",
            cnt_accounts
        );

        Ok(())
    }

    fn save_account_diffs(
        &self,
        block_id: BlockIdentifier,
        accounts: HashMap<String, SerializedItem>,
    ) -> anyhow::Result<()> {
        // TODO split according to the sqlite limits
        self.save_accounts(block_id, accounts)?;
        Ok(())
    }

    fn store_optimistic_in_cache<T: Into<Arc<Self::OptimisticState>>>(
        &self,
        state: T,
    ) -> anyhow::Result<()> {
        let optimistic = state.into();
        let block_id = optimistic.get_block_id().clone();
        let thread_id = *optimistic.get_thread_id();
        tracing::trace!("save optimistic to cache {block_id:?} {thread_id:?}");
        self.optimistic_state.guarded_mut(|e| {
            e.insert(block_id, optimistic);
        });
        self.clear_optimistic_states(&thread_id)?;
        Ok(())
    }

    fn store_optimistic<T: Into<Arc<Self::OptimisticState>>>(
        &self,
        state: T,
    ) -> anyhow::Result<()> {
        let optimistic = state.into();
        self.store_optimistic_in_cache(optimistic.clone())?;
        let block_id = optimistic.get_block_id().clone();
        tracing::trace!("save optimistic {block_id:?}");
        let block_seq_no = optimistic.block_seq_no;
        let root_path = self.get_optimistic_state_path();
        let path = self.get_path(root_path, block_id.to_string());
        if path.exists() {
            return Ok(());
        }
        let saved_states_clone = self.saved_states.clone();
        let start_save = std::time::Instant::now();
        let thread_id = optimistic.thread_id;
        self.clear_optimistic_states(&optimistic.thread_id)?;

        let mut optimistic = Arc::unwrap_or_clone(optimistic);
        optimistic.make_an_independent_copy();
        if self.split_state
            && (!optimistic.changed_accounts.is_empty() || !optimistic.cached_accounts.is_empty())
        {
            let changed_accounts = std::mem::take(&mut optimistic.changed_accounts);
            let mut shard_state = optimistic.get_shard_state().as_ref().clone();
            let mut accounts =
                shard_state.read_accounts().map_err(|e| anyhow::format_err!("{e}"))?;
            for account_id in changed_accounts.into_keys() {
                if let Some(mut account) = accounts
                    .account(&(&account_id).into())
                    .map_err(|e| anyhow::format_err!("Failed to read account: {e}"))?
                {
                    let aug =
                        account.aug().map_err(|e| anyhow::format_err!("Failed to get aug: {e}"))?;
                    let account_root = account
                        .replace_with_external()
                        .map_err(|e| anyhow::format_err!("Failed to set account external: {e}"))?;
                    self.accounts.store_account(
                        &account_id,
                        account.last_trans_hash(),
                        account.last_trans_lt(),
                        account_root,
                    )?;
                    accounts
                        .insert_with_aug(&account_id.0, &account, &aug)
                        .map_err(|e| anyhow::format_err!("Failed to insert account: {e}"))?;
                }
            }
            for (account_id, (_, account_root)) in &optimistic.cached_accounts {
                if let Some(account) = accounts
                    .account(&account_id.into())
                    .map_err(|e| anyhow::format_err!("Failed to read account: {e}"))?
                {
                    self.accounts.store_account(
                        account_id,
                        account.last_trans_hash(),
                        account.last_trans_lt(),
                        account_root.clone(),
                    )?;
                }
            }
            shard_state.write_accounts(&accounts).map_err(|e| anyhow::format_err!("{e}"))?;
            optimistic.set_shard_state(Arc::new(shard_state));
        }

        // let state_bytes =
        //     OptimisticState::serialize_into_buf(optimistic).expect("Failed to serialize block");
        // let res = save_to_file(&path, &state_bytes, false);
        let res = optimistic.save_to_file(&path);
        tracing::trace!(
            "save optimistic {block_id:?} result: {res:?} {}",
            start_save.elapsed().as_millis()
        );
        res?;
        self.metrics.as_ref().inspect(|m| m.report_saved_state(&thread_id));
        {
            let mut saved_states = saved_states_clone.lock();
            saved_states.entry(thread_id).or_default().insert(block_seq_no, block_id.clone());
            tracing::trace!("repo saved_states={:?}", saved_states);
        }
        Ok(())
    }

    fn get_latest_block_id_with_producer_group_change(
        &self,
        _thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<BlockIdentifier> {
        Ok(BlockIdentifier::default())
    }

    fn get_all_metadata(&self) -> RepositoryMetadata {
        self.metadatas.clone()
    }

    fn get_message_db(&self) -> MessageDurableStorage {
        self.message_db.clone()
    }

    fn unfinalized_blocks(
        &self,
    ) -> Arc<Mutex<HashMap<ThreadIdentifier, UnfinalizedCandidateBlockCollection>>> {
        self.unfinalized_blocks.clone()
    }

    fn get_message_storage_service(&self) -> &MessageDBWriterService {
        &self.message_storage_service
    }
}

#[cfg(feature = "messages_db")]
fn extract_new_messages<T>(
    btree: &BTreeMap<
        AccountAddress,
        account_inbox::range::MessagesRange<MessageIdentifier, Arc<T>>,
    >,
    last_message_guard: &parking_lot::lock_api::MutexGuard<
        '_,
        parking_lot::RawMutex,
        HashMap<AccountAddress, MessageIdentifier>,
    >,
) -> anyhow::Result<HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<T>)>>> {
    let mut messages = HashMap::new();

    for (address, m_range) in btree.iter() {
        let mut tail = m_range.tail_sequence().clone(); // TODO: remove this clone if possible

        if let Some(last_saved_m_id) = last_message_guard.get(address) {
            let found_index = tail.iter().position(|(message_id, _)| message_id == last_saved_m_id);

            if let Some(found_index) = found_index {
                tail = tail.split_off(found_index + 1);
            }
        }
        if !tail.is_empty() {
            let tail_as_vec = tail.into_iter().collect();
            messages.insert(address.clone(), tail_as_vec);
        }
    }
    Ok(messages)
}

#[cfg(test)]
pub mod tests {

    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::Once;

    use parking_lot::Mutex;
    use telemetry_utils::mpsc::instrumented_channel;
    use telemetry_utils::mpsc::InstrumentedSender;

    use super::BkSetUpdate;
    use super::RepositoryImpl;
    use super::OID;
    use crate::helper::metrics;
    use crate::multithreading::routing::service::RoutingService;
    use crate::node::shared_services::SharedServices;
    use crate::repository::accounts::AccountsRepository;
    use crate::repository::repository_impl::BlockStateRepository;
    use crate::repository::repository_impl::FinalizedBlockStorage;
    use crate::storage::MessageDurableStorage;
    use crate::utilities::FixedSizeHashSet;

    const ZEROSTATE: &str = "../config/zerostate";
    const DB_PATH: &str = "./tests-data";
    static START: Once = Once::new();

    fn init_db(db_path: &str) -> anyhow::Result<()> {
        let _ = std::fs::remove_dir_all(db_path);
        let _ = std::fs::create_dir_all(db_path);

        Command::new("../target/debug/migration-tool")
            .arg("-p")
            .arg(db_path)
            .arg("-n")
            .arg("-a")
            .status()?;
        Ok(())
    }

    fn init_tests() {
        init_db(DB_PATH).expect("Failed to init DB");
        init_db(&format!("{DB_PATH}/test_save_load")).expect("Failed to init DB");
        init_db(&format!("{DB_PATH}/test_exists")).expect("Failed to init DB");
        init_db(&format!("{DB_PATH}/test_remove")).expect("Failed to init DB");
        init_db(&format!("{DB_PATH}/test_save_duplication")).expect("Failed to init DB");
    }

    pub fn finalized_blocks_storage() -> Arc<Mutex<FinalizedBlockStorage>> {
        Arc::new(Mutex::new(FinalizedBlockStorage::new(100)))
    }

    fn mock_bk_set_updates_tx() -> InstrumentedSender<BkSetUpdate> {
        let (bk_set_updates_tx, _bk_set_updates_rx) = instrumented_channel::<BkSetUpdate>(
            None::<metrics::BlockProductionMetrics>,
            metrics::BK_SET_UPDATE_CHANNEL,
        );
        bk_set_updates_tx
    }
    #[test]
    #[ignore]
    fn test_save_load() -> anyhow::Result<()> {
        START.call_once(init_tests);

        let block_state_repository =
            BlockStateRepository::test(PathBuf::from("./tests-data/test_save_load/block-state"));
        let accounts_repository =
            AccountsRepository::new(PathBuf::from("./tests-data/test_save_load"), Some(0), 1);
        let message_db = MessageDurableStorage::as_noop();
        let finalized_blocks = finalized_blocks_storage();
        let repository = RepositoryImpl::new(
            PathBuf::from("./tests-data/test_save_load"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
            SharedServices::test_start(RoutingService::stub().0, u32::MAX),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            false,
            block_state_repository,
            None,
            accounts_repository,
            message_db.clone(),
            finalized_blocks,
            mock_bk_set_updates_tx(),
        );

        let path = "block_status";
        let oid = OID::ID(String::from("100"));
        let expected_data: Vec<u8> = vec![0xde, 0xad, 0xbe, 0xef];
        repository.save(path, oid.clone(), &expected_data)?;

        let data = repository.load::<Vec<u8>>(path, oid.clone())?;
        assert_eq!(data, Some(expected_data));
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_clear_big_state() -> anyhow::Result<()> {
        let block_state_repository = BlockStateRepository::test(PathBuf::from(
            "/home/user/GOSH/acki-nacki/server_data/node1/block-state/",
        ));
        let accounts_repository = AccountsRepository::new(
            PathBuf::from("/home/user/GOSH/acki-nacki/server_data/node1/"),
            Some(0),
            1,
        );
        let message_db = MessageDurableStorage::as_noop();
        let finalized_blocks = finalized_blocks_storage();
        let _repository = RepositoryImpl::new(
            PathBuf::from("/home/user/GOSH/acki-nacki/server_data/node1/"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
            SharedServices::test_start(RoutingService::stub().0, u32::MAX),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            false,
            block_state_repository,
            None,
            accounts_repository,
            message_db.clone(),
            finalized_blocks,
            mock_bk_set_updates_tx(),
        );
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_exists() -> anyhow::Result<()> {
        START.call_once(init_tests);
        let block_state_repository =
            BlockStateRepository::test(PathBuf::from("./tests-data/test_exists/block-state"));
        let accounts_repository =
            AccountsRepository::new(PathBuf::from("./tests-data/test_exists"), Some(0), 1);
        let message_db = MessageDurableStorage::as_noop();
        let finalized_blocks = finalized_blocks_storage();

        let repository = RepositoryImpl::new(
            PathBuf::from("./tests-data/test_exists"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
            SharedServices::test_start(RoutingService::stub().0, u32::MAX),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            false,
            block_state_repository,
            None,
            accounts_repository,
            message_db.clone(),
            finalized_blocks,
            mock_bk_set_updates_tx(),
        );

        let path = "metadata";
        let oid = OID::SingleRow;
        let expected_data: Vec<u8> = vec![0xde, 0xad, 0xbe, 0xef];
        repository.save(path, oid.clone(), &expected_data)?;

        assert!(repository.exists(path, oid));
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_remove() -> anyhow::Result<()> {
        START.call_once(init_tests);
        let block_state_repository =
            BlockStateRepository::test(PathBuf::from("./tests-data/test_remove/block-state"));
        let accounts_repository =
            AccountsRepository::new(PathBuf::from("./tests-data/test_remove"), Some(0), 1);
        let message_db = MessageDurableStorage::as_noop();
        let finalized_blocks = finalized_blocks_storage();

        let repository = RepositoryImpl::new(
            PathBuf::from("./tests-data/test_remove"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
            SharedServices::test_start(RoutingService::stub().0, u32::MAX),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            false,
            block_state_repository,
            None,
            accounts_repository,
            message_db.clone(),
            finalized_blocks,
            mock_bk_set_updates_tx(),
        );
        let path = "optimistic_state";
        let oid = OID::ID(String::from("200"));
        let data: Vec<u8> = vec![0xde, 0xad, 0xbe, 0xef];
        repository.save(path, oid.clone(), &data)?;
        assert!(repository.exists(path, oid.clone()));

        repository._remove(path, oid.clone())?;
        assert!(!repository.exists(path, oid.clone()));
        Ok(())
    }
}

#[cfg(all(test, feature = "messages_db"))]
mod extract_new_messages_tests {
    use std::collections::VecDeque;

    use account_inbox::range::MessagesRange;
    use tvm_block::OutMsgQueueKey;

    use super::*;

    fn dummy_message() -> Arc<String> {
        Arc::new(String::default())
    }

    fn gen_message_id() -> MessageIdentifier {
        let key = OutMsgQueueKey { workchain_id: 1, prefix: 1, hash: UInt256::rand() };
        key.into()
    }

    #[test]
    fn test_extract_new_messages_no_last_saved() {
        let addr = AccountAddress(UInt256::default());
        let id1 = gen_message_id();
        let id2 = gen_message_id();

        let mut message_range = MessagesRange::empty();
        message_range.set_tail_sequence(VecDeque::from([
            (id1.clone(), dummy_message()),
            (id2.clone(), dummy_message()),
        ]));

        let mut btree = BTreeMap::new();
        btree.insert(addr.clone(), message_range);

        let m_map = Mutex::new(HashMap::new());

        let result = extract_new_messages(&btree, &m_map.lock()).unwrap();
        let inbox = result.get(&addr).unwrap();

        assert_eq!(inbox.len(), 2);
        assert_eq!(inbox.last().unwrap().0, id2);
    }

    #[test]
    fn test_extract_new_messages_with_last_saved_in_middle() {
        let addr = AccountAddress(UInt256::default());
        let id1 = gen_message_id();
        let id2 = gen_message_id();
        let id3 = gen_message_id();

        let mut message_range = MessagesRange::empty();
        message_range.set_tail_sequence(VecDeque::from([
            (id1.clone(), dummy_message()),
            (id2.clone(), dummy_message()),
            (id3.clone(), dummy_message()),
        ]));

        let mut btree = BTreeMap::new();
        btree.insert(addr.clone(), message_range);

        let mut last_saved = HashMap::new();
        last_saved.insert(addr.clone(), id1.clone());

        let result = extract_new_messages(&btree, &Mutex::new(last_saved).lock()).unwrap();
        let inbox = result.get(&addr).unwrap();

        assert_eq!(inbox.len(), 2); // should skip id1
        assert_eq!(inbox.first().unwrap().0, id2);
        assert_eq!(inbox.last().unwrap().0, id3);
    }

    #[test]
    fn test_extract_new_messages_with_last_saved_latest() {
        let addr = AccountAddress(UInt256::default());
        let id1 = gen_message_id();
        let id2 = gen_message_id();

        let mut message_range = MessagesRange::empty();
        message_range.set_tail_sequence(VecDeque::from([
            (id1.clone(), dummy_message()),
            (id2.clone(), dummy_message()),
        ]));

        let mut btree = BTreeMap::new();
        btree.insert(addr.clone(), message_range);

        let mut last_saved = HashMap::new();
        last_saved.insert(addr.clone(), id2.clone());

        let result = extract_new_messages(&btree, &Mutex::new(last_saved).lock()).unwrap();

        assert!(!result.contains_key(&addr));
    }

    #[test]
    fn test_extract_new_messages_multiple_accounts() {
        let mut btree = BTreeMap::new();
        // Acc 1
        let id_a1_1 = gen_message_id();

        let id_a1_2 = gen_message_id();
        let mut range1 = MessagesRange::empty();
        range1.set_tail_sequence(VecDeque::from([
            (id_a1_1.clone(), dummy_message()),
            (id_a1_2.clone(), dummy_message()),
        ]));
        let addr1 = AccountAddress(UInt256::default());
        btree.insert(addr1.clone(), range1);

        // Acc 2
        let id_a2_1 = gen_message_id();
        let id_a2_2 = gen_message_id();
        let mut range2 = MessagesRange::empty();
        range2.set_tail_sequence(VecDeque::from([
            (id_a2_1.clone(), dummy_message()),
            (id_a2_2.clone(), dummy_message()),
        ]));
        let addr2 = AccountAddress(UInt256::max());
        btree.insert(addr2.clone(), range2);

        // Previous saved ids only for Acc 1
        let mut last_saved = HashMap::new();
        last_saved.insert(addr1.clone(), id_a1_1.clone());

        let result = extract_new_messages(&btree, &Mutex::new(last_saved).lock()).unwrap();

        let inbox1 = result.get(&addr1).unwrap();
        let inbox2 = result.get(&addr2).unwrap();

        assert_eq!(inbox1.len(), 1);
        assert_eq!(inbox1[0].0, id_a1_2);
        assert_eq!(inbox2.len(), 2);
        assert_eq!(inbox2[0].0, id_a2_1);
    }
}
