// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::hash::Hash;
use std::io::Read;
use std::io::Write;
use std::num::NonZeroUsize;
use std::ops::Add;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::ensure;
use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use database::documents_db::DocumentsDb;
use database::documents_db::SerializedItem;
use database::sqlite::sqlite_helper::SqliteHelper;
use database::sqlite::sqlite_helper::SqliteHelperConfig;
use database::sqlite::ArchAccount;
use derive_getters::Getters;
use lru::LruCache;
use parking_lot::Mutex;
use rusqlite::params;
use rusqlite::Connection;
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
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::database::serialize_block::prepare_account_archive_struct;
use crate::helper::get_temp_file_path;
use crate::helper::metrics::BlockProductionMetrics;
use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;
use crate::message_storage::writer_service::MessageDBWriterService;
use crate::message_storage::MessageDurableStorage;
#[cfg(test)]
use crate::multithreading::routing::service::RoutingService;
use crate::node::associated_types::AttestationData;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::block_state::state::AttestationTargets;
use crate::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BlockStatistics;
use crate::node::services::sync::ExternalFileSharesBased;
use crate::node::services::sync::StateSyncService;
use crate::node::shared_services::SharedServices;
use crate::node::unprocessed_blocks_collection::UnfinalizedBlocksSnapshot;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;
use crate::repository::RepositoryError;
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
const REQUIRED_SQLITE_SCHEMA_VERSION: u32 = 2;
const _REQUIRED_SQLITE_ARC_SCHEMA_VERSION: u32 = 2;
const DB_FILE: &str = "node.db";
const DB_ARCHIVE_FILE: &str = "node-archive.db";
pub const EXT_MESSAGE_STORE_TIMEOUT_SECONDS: i64 = 60;
const _MAX_BLOCK_SEQ_NO_THAT_CAN_BE_BUILT_FROM_ZEROSTATE: u32 = 200;
const MAX_BLOCK_CNT_THAT_CAN_BE_LOADED_TO_PREPARE_STATE: usize = 400;

pub type RepositoryMetadata =
    Arc<Mutex<HashMap<ThreadIdentifier, Arc<Mutex<Metadata<BlockIdentifier, BlockSeqNo>>>>>>;
pub type RepositoryLastExtMessageIndex = Arc<Mutex<HashMap<ThreadIdentifier, u32>>>;

impl AllowGuardedMut for HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockIdentifier>> {}
impl AllowGuardedMut for HashMap<ThreadIdentifier, OptimisticStateImpl> {}
impl AllowGuardedMut for HashMap<ThreadIdentifier, LruCache<BlockIdentifier, OptimisticStateImpl>> {}

pub struct FinalizedBlockStorage {
    per_thread_buffer_size: usize,
    buffer: HashMap<
        ThreadIdentifier,
        FixedSizeHashMap<BlockIdentifier, Arc<Envelope<GoshBLS, AckiNackiBlock>>>,
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

    pub fn store(&mut self, block: Envelope<GoshBLS, AckiNackiBlock>) {
        let block_id = block.data().identifier();
        let thread_id = block.data().get_common_section().thread_id;
        self.buffer
            .entry(thread_id)
            .and_modify(|e| {
                e.insert(block_id.clone(), Arc::new(block.clone()));
            })
            .or_insert_with(|| {
                let mut thread_buffer = FixedSizeHashMap::new(self.per_thread_buffer_size);
                thread_buffer.insert(block_id.clone(), block.clone().into());
                thread_buffer
            });
        if let Some(threads_table) = &block.data().get_common_section().threads_table {
            for thread_id in threads_table.list_threads() {
                if thread_id.is_spawning_block(&block_id) {
                    self.buffer
                        .entry(*thread_id)
                        .and_modify(|e| {
                            e.insert(block_id.clone(), Arc::new(block.clone()));
                        })
                        .or_insert_with(|| {
                            let mut thread_buffer =
                                FixedSizeHashMap::new(self.per_thread_buffer_size);
                            thread_buffer.insert(block_id.clone(), block.clone().into());
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
                return Some(result.clone());
            }
        }
        for thread_id in self.buffer.keys() {
            if let Some(result) = self.buffer.get(thread_id).and_then(|e| e.get(block_identifier)) {
                return Some(result.clone());
            }
        }
        None
    }

    pub fn buffer(
        &self,
    ) -> &HashMap<
        ThreadIdentifier,
        FixedSizeHashMap<BlockIdentifier, Arc<Envelope<GoshBLS, AckiNackiBlock>>>,
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
    archive: Option<Connection>,
    data_dir: PathBuf,
    zerostate_path: Option<PathBuf>,
    metadatas: RepositoryMetadata,
    // unprocessed_blocks_cache: UnfinalizedCandidateBlockCollection, -> Node, + all modules that read it
    // change get_block -> get_finalized_block
    saved_states: Arc<Mutex<HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockIdentifier>>>>,
    finalized_optimistic_states: Arc<Mutex<HashMap<ThreadIdentifier, OptimisticStateImpl>>>,
    shared_services: SharedServices,
    nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
    block_state_repository: BlockStateRepository,
    optimistic_states_cache:
        Arc<Mutex<HashMap<ThreadIdentifier, LruCache<BlockIdentifier, OptimisticStateImpl>>>>,
    accounts: AccountsRepository,
    split_state: bool,
    metrics: Option<BlockProductionMetrics>,
    message_db: MessageDurableStorage,
    message_storage_service: MessageDBWriterService,
    states_cache_size: usize,
    finalized_blocks: Arc<Mutex<FinalizedBlockStorage>>,
    bk_set_update_tx: InstrumentedSender<BkSetUpdate>,
}

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
    std::fs::rename(tmp_file_path, file_path)?;
    tracing::trace!("File saved: {:?}", file_path);
    Ok(())
}

impl Clone for RepositoryImpl {
    // TODO: Repository object can consume great amount of memory, so it's better not to copy it
    fn clone(&self) -> Self {
        let db_path = self.data_dir.join(DB_ARCHIVE_FILE);
        let arc_conn = if !cfg!(feature = "disable_db_write") {
            Some(
                SqliteHelper::create_connection_ro(db_path.clone())
                    .unwrap_or_else(|_| panic!("Failed to open {db_path:?}")),
            )
        } else {
            None
        };

        Self {
            archive: arc_conn,
            data_dir: self.data_dir.clone(),
            zerostate_path: self.zerostate_path.clone(),
            metadatas: self.metadatas.clone(),
            saved_states: self.saved_states.clone(),
            finalized_optimistic_states: self.finalized_optimistic_states.clone(),
            shared_services: self.shared_services.clone(),
            nack_set_cache: Arc::clone(&self.nack_set_cache),
            block_state_repository: self.block_state_repository.clone(),
            optimistic_states_cache: self.optimistic_states_cache.clone(),
            accounts: self.accounts.clone(),
            split_state: self.split_state,
            metrics: self.metrics.clone(),
            message_storage_service: self.message_storage_service.clone(),
            message_db: self.message_db.clone(),
            states_cache_size: self.states_cache_size,
            finalized_blocks: Arc::clone(&self.finalized_blocks),
            bk_set_update_tx: self.bk_set_update_tx.clone(),
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

        let archive = if !cfg!(feature = "disable_db_write") {
            let db_path = data_dir.join(DB_FILE);
            let arch_path = data_dir.join(DB_ARCHIVE_FILE);
            let conn = SqliteHelper::create_connection_ro(db_path.clone())
                .unwrap_or_else(|_| panic!("Failed to open {db_path:?}"));
            let schema_version =
                conn.pragma_query_value(None, "user_version", |row| row.get(0)).unwrap_or(0);
            match schema_version.cmp(&REQUIRED_SQLITE_SCHEMA_VERSION) {
                Ordering::Less => {
                    tracing::trace!(
                        r"Error: DB schema must be uprgaded. Current version = {}, required version = {}
                    For migrate DB schema to required version, please use 'migration_tool':
                    $ migration-tool -p {} -n {REQUIRED_SQLITE_SCHEMA_VERSION} -a",
                        schema_version,
                        REQUIRED_SQLITE_SCHEMA_VERSION,
                        data_dir.display()
                    );
                    std::process::exit(2);
                }
                Ordering::Greater => {
                    tracing::trace!(
                        r"The schema of the database version is higher than that required by
                    this instance of the Acki-Nacki node. Please update the app version.",
                    );
                    std::process::exit(3);
                }
                _ => {}
            }

            Some(
                SqliteHelper::create_connection_ro(arch_path.clone()).unwrap_or_else(|_| {
                    panic!("Failed to open an archive DB {}", arch_path.display())
                }),
            )
        } else {
            None
        };

        let metadata = Arc::new(Mutex::new({
            let metadata = Self::load_metadata(&data_dir)
                .expect("Must be able to create or load repository metadata");
            tracing::trace!("Loaded metadata: {:?}", metadata);
            metadata
        }));

        let mut finalized_optimistic_states = HashMap::new();
        {
            let mut metadatas = metadata.lock();
            if let Some(path) = &zerostate_path {
                let zerostate = ZeroState::load_from_file(path).expect("Failed to load zerostate");
                for thread_id in zerostate.list_threads() {
                    finalized_optimistic_states.insert(
                        *thread_id,
                        zerostate
                            .state(thread_id)
                            .expect("Failed to load state from zerostate")
                            .clone(),
                    );
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
        let message_storage_service = MessageDBWriterService::new(message_db.clone())
            .expect("Failed to init message storage service");

        let repo_impl = Self {
            archive,
            data_dir: data_dir.clone(),
            zerostate_path,
            metadatas: metadata,
            saved_states: Default::default(),
            finalized_optimistic_states: Arc::new(Mutex::new(finalized_optimistic_states)),
            shared_services,
            nack_set_cache: Arc::clone(&nack_set_cache),
            block_state_repository: block_state_repository.clone(),
            optimistic_states_cache: Arc::new(Mutex::new(HashMap::new())),
            accounts: accounts_repository,
            split_state,
            metrics,
            message_db: message_db.clone(),
            message_storage_service,
            states_cache_size,
            finalized_blocks,
            bk_set_update_tx,
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
                            if let Some(buffer) = load_from_file::<Vec<u8>>(&path).unwrap() {
                                let state =
                                    <Self as Repository>::OptimisticState::deserialize_from_buf(
                                        &buffer,
                                    )
                                    .unwrap();
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
                repo_impl.finalized_optimistic_states.guarded_mut(|e| e.insert(*thread_id, state));
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
        let result = markers.lock().is_finalized();
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

    fn load_archived_block_by_seq_no(
        &self,
        seq_no: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<<RepositoryImpl as Repository>::CandidateBlock>> {
        tracing::trace!("Loading an archived block (seq_no: {seq_no:?})...");
        let sql = r"SELECT aggregated_signature, signature_occurrences, data
            FROM blocks WHERE seq_no=?1 AND thread_id=?2";

        let mut stmt = self
            .archive
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Internal error: archive is None"))?
            .prepare(sql)?;

        let mut rows = stmt.query(params![seq_no.to_string(), thread_id.to_string()])?;
        let mut envelopes = vec![];

        while let Some(row) = rows.next()? {
            let mut buffer: Vec<u8> = row.get(0)?;
            let aggregated_signature: <GoshBLS as BLSSignatureScheme>::Signature =
                bincode::deserialize(&buffer)?;

            buffer = row.get(1)?;
            let signature_occurrences: HashMap<u16, u16> = bincode::deserialize(&buffer)?;

            buffer = row.get(2)?;
            let block_data: AckiNackiBlock = bincode::deserialize(&buffer)?;

            let envelope = <RepositoryImpl as Repository>::CandidateBlock::create(
                aggregated_signature,
                signature_occurrences,
                block_data,
            );
            envelopes.push(envelope);
        }
        Ok(envelopes)
    }

    fn is_block_present_in_archive(&self, identifier: &BlockIdentifier) -> anyhow::Result<bool> {
        let sql = r"SELECT aggregated_signature, signature_occurrences, data, id
            FROM blocks WHERE id=?1";
        let mut stmt = self
            .archive
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Internal error: archive is None"))?
            .prepare(sql)?;
        let mut rows = stmt.query(params![identifier.to_string()])?;
        let res = rows.next()?.is_some();
        Ok(res)
    }

    pub(crate) fn load_archived_block(
        &self,
        identifier: &Option<BlockIdentifier>,
        thread_identifier: Option<ThreadIdentifier>,
    ) -> anyhow::Result<Option<<RepositoryImpl as Repository>::CandidateBlock>> {
        if cfg!(feature = "disable_db_write") {
            return Ok(None);
        }
        tracing::trace!("Loading an archived block (block_id: {identifier:?})...");
        let add_clause = if let Some(block_id) = identifier {
            format!(" WHERE id={:?}", block_id.to_string())
        } else if let Some(thread_id) = thread_identifier {
            format!(" WHERE thread_id={:?} ORDER BY seq_no DESC LIMIT 1", hex::encode(thread_id))
        } else {
            " ORDER BY seq_no DESC LIMIT 1".to_string()
        };
        let sql = format!(
            "SELECT aggregated_signature, signature_occurrences, data, id FROM blocks {add_clause}",
        );
        let mut stmt = self
            .archive
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Internal error: archive is None"))?
            .prepare(&sql)?;

        let mut rows = stmt.query([])?;
        let envelope = match rows.next()? {
            Some(row) => {
                let mut buffer: Vec<u8> = row.get(0)?;
                let aggregated_signature: <GoshBLS as BLSSignatureScheme>::Signature =
                    bincode::deserialize(&buffer)?;

                buffer = row.get(1)?;
                let signature_occurrences: HashMap<u16, u16> = bincode::deserialize(&buffer)?;

                buffer = row.get(2)?;
                let block_data: AckiNackiBlock = bincode::deserialize(&buffer)?;

                let envelope = <RepositoryImpl as Repository>::CandidateBlock::create(
                    aggregated_signature,
                    signature_occurrences,
                    block_data,
                );
                Some(envelope)
            }
            None => None,
        };

        Ok(envelope)
    }

    fn clear_optimistic_states(&self, thread_id: &ThreadIdentifier) -> anyhow::Result<()> {
        let Some((_, last_finalized_seq_no)) =
            self.select_thread_last_finalized_block(thread_id)?
        else {
            return Ok(());
        };
        let mut saved_states =
            self.saved_states.guarded(|e| e.get(thread_id).cloned().unwrap_or_default());
        let mut keys_to_remove = vec![];
        // retain finalized blocks (fork blocks could save their state)
        for (block_seq_no, block_id) in saved_states.iter() {
            if let Ok(state) = self.block_state_repository.get(block_id) {
                if !state.guarded(|e| e.is_finalized()) {
                    keys_to_remove.push(*block_seq_no);
                }
            } else {
                keys_to_remove.push(*block_seq_no);
            }
        }

        saved_states.retain(|block_seq_no, _block_id| !keys_to_remove.contains(block_seq_no));
        // iter saved finalized states
        for (block_seq_no, block_id) in saved_states.iter() {
            if *block_seq_no > last_finalized_seq_no {
                break;
            }
            if !self.is_block_present_in_archive(block_id).unwrap_or(false) {
                break;
            }
            keys_to_remove.push(*block_seq_no);
        }
        if keys_to_remove.is_empty() {
            return Ok(());
        }
        // Leave only the latest state
        let last = keys_to_remove.pop();
        // for k in &keys_to_remove {
        //     let block_id = saved_states.get(k).unwrap();
        //     tracing::trace!("Clear optimistic state for {k} {block_id:?}");
        //     let optimistic_state_path = self.get_optimistic_state_path();
        //     self.remove(optimistic_state_path, OID::ID(block_id.to_string()))?;
        // }
        if self.split_state && !keys_to_remove.is_empty() {
            if let Some(block_id) = saved_states.get(&last.unwrap()) {
                let mut last = self
                    .get_optimistic_state(block_id, thread_id, None)?
                    .ok_or_else(|| anyhow::format_err!("Optimistic state must be present"))?;
                let block_lt = self
                    .get_block_from_repo_or_archive(block_id)?
                    .data()
                    .tvm_block()
                    .read_info()
                    .map_err(|err| anyhow::format_err!("Failed to read block info: {err}"))?
                    .end_lt();
                let accounts = self.accounts.clone();
                let last = last.get_shard_state().read_accounts().map_err(|err| {
                    anyhow::format_err!("Failed to read last shard accounts: {}", err)
                })?;
                let thread_id = *thread_id;
                // TODO: save handler
                std::thread::Builder::new()
                    .name("Clear old accounts".to_owned())
                    .spawn(move || accounts.clear_old_accounts(&thread_id, &last, block_lt))?;
            }
        }
        self.saved_states.guarded_mut(|e| {
            let thread_states = e.entry(*thread_id).or_default();
            thread_states.retain(|seq_no, _| !keys_to_remove.contains(seq_no));
        });
        tracing::trace!("repo saved_states={:?}", saved_states);
        Ok(())
    }

    fn try_load_state_from_archive(
        &self,
        block_id: &BlockIdentifier,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        min_state: Option<<Self as Repository>::OptimisticState>,
    ) -> anyhow::Result<Option<<Self as Repository>::OptimisticState>> {
        tracing::trace!("try_load_state_from_archive: {block_id:?}");
        // Load last block
        let block = self.get_block_from_repo_or_archive(block_id)?;
        let thread_id = block.data().get_common_section().thread_id;
        self.metrics.as_ref().inspect(|m| m.report_load_from_archive_invoke(&thread_id));

        let mut available_states: HashSet<BlockIdentifier> = HashSet::from_iter(
            self.saved_states
                .guarded(|e| e.clone())
                .values()
                .flat_map(|v| v.clone().values().cloned().collect::<Vec<_>>()),
        );

        let finalized_state =
            self.finalized_optimistic_states.guarded(|map| map.get(&thread_id).cloned());
        if let Some(finalized_state) = finalized_state.as_ref() {
            available_states.insert(finalized_state.block_id.clone());
        }
        if self.zerostate_path.is_some() {
            available_states.insert(BlockIdentifier::default());
        };
        if let Some(cache) = self.optimistic_states_cache.guarded(|e| {
            e.get(&thread_id).map(|v| v.iter().map(|(k, _v)| k.clone()).collect::<Vec<_>>())
        }) {
            available_states.extend(cache);
        }
        if let Some(min_state) = min_state.as_ref() {
            available_states.insert(min_state.block_id.clone());
        }
        tracing::trace!("try_load_state_from_archive: available_states: {:?}", available_states);

        let mut blocks = vec![];
        let mut block_id = block_id.clone();

        let state_id = loop {
            let block = self.get_block_from_repo_or_archive(&block_id)?;
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

        let mut state = if state_id == BlockIdentifier::default() {
            tracing::trace!("Load state from zerostate");
            let mut zero_block = <Self as Repository>::OptimisticState::zero();
            if let Some(path) = &self.zerostate_path {
                let zerostate = ZeroState::load_from_file(path)?;
                zero_block = zerostate.state(&thread_id)?.clone();
            };
            zero_block
        } else if let Some(state) = finalized_state.filter(|s| s.block_id == state_id) {
            state
        } else if let Some(state) = min_state.filter(|s| s.block_id == state_id) {
            state
        } else {
            self.get_optimistic_state(&state_id, &thread_id, None)?
                .ok_or(anyhow::format_err!("Optimistic state must be present"))?
        };
        tracing::trace!("try_load_state_from_archive start applying blocks");
        for block in blocks {
            tracing::trace!("try_load_state_from_archive apply block {:?}", block.identifier());
            self.metrics.as_ref().inspect(|m| m.report_load_from_archive_apply(&thread_id));
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
        let archive = if !cfg!(feature = "disable_db_write") {
            Some(SqliteHelper::create_connection_ro(tmp_path.clone()).unwrap())
        } else {
            None
        };

        let data_dir = PathBuf::default();
        let message_db = MessageDurableStorage::new(tmp_path).unwrap();
        let message_service =
            crate::message_storage::writer_service::MessageDBWriterService::new(message_db.clone())
                .unwrap();
        let finalized_blocks =
            crate::repository::repository_impl::tests::finalized_blocks_storage();

        let (bk_set_update_tx, _bk_set_update_rx) = instrumented_channel::<BkSetUpdate>(
            None::<metrics::BlockProductionMetrics>,
            metrics::BK_SET_UPDATE_CHANNEL,
        );
        Self {
            archive,
            accounts: AccountsRepository::new(data_dir.clone(), None, 1),
            data_dir,
            zerostate_path: None,
            metadatas: Arc::new(Mutex::new(metadatas)),
            saved_states: Arc::new(Mutex::new(HashMap::new())),
            finalized_optimistic_states: Arc::new(Mutex::new(HashMap::new())),
            shared_services: SharedServices::test_start(RoutingService::stub().0, u32::MAX),
            nack_set_cache: Arc::new(Mutex::new(FixedSizeHashSet::new(0))),
            block_state_repository,
            optimistic_states_cache: Arc::new(Mutex::new(HashMap::new())),
            split_state: false,
            metrics: None,
            message_db,
            message_storage_service: message_service,
            states_cache_size: 1,
            finalized_blocks,
            bk_set_update_tx,
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

        self.shared_services.on_block_appended(block.data());
        self.mark_block_as_finalized(
            &block,
            self.block_state_repository.get(&block_id)?,
            None::<Arc<ExternalFileSharesBased>>,
        )?;
        self.shared_services.on_block_finalized(
            block.data(),
            &mut self
                .get_optimistic_state(&block.data().identifier(), &thread_id, None)?
                .expect("set above"),
        );
        // self.clear_unprocessed_till(&seq_no, &current_thread_id)?;
        self.clear_verification_markers(&seq_no, &thread_id)?;

        Ok(())
    }

    pub fn dump_unfinalized_blocks(&self, blocks: UnfinalizedBlocksSnapshot) {
        let root_path = self.get_blocks_dir_path();
        for (_, block) in blocks.values() {
            let block_id = block.data().identifier();
            let res = RepositoryImpl::save_block(&root_path, block);
            tracing::trace!("dump_state: save unfinalized block res: {block_id:?} {:?}", res);
        }
    }

    pub fn dump_state(&self) {
        for (_, finalized_state) in self.finalized_optimistic_states.lock().iter() {
            let res = self.store_optimistic(finalized_state.clone());
            tracing::trace!("dump_state: Store optimistic state res: {:?}", res);
        }

        let res = Self::save_metadata(&self.data_dir, self.metadatas.clone());
        tracing::trace!("dump_state: save metadata res: {:?}", res);

        let root_path = self.get_blocks_dir_path();
        for (_thread, blocks) in self.finalized_blocks.lock().buffer().iter() {
            for (block_id, block) in blocks.blocks() {
                let res = RepositoryImpl::save_block(&root_path, block);
                tracing::trace!("dump_state: save finalized block res: {block_id:?} {:?}", res);
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
        self.finalized_optimistic_states.guarded_mut(|e| {
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
    ) -> anyhow::Result<Arc<<Self as Repository>::CandidateBlock>> {
        Ok(if let Some(block) = self.get_finalized_block(block_id)? {
            block
        } else if let Some(block) = self.load_archived_block(&Some(block_id.clone()), None)? {
            Arc::new(block)
        } else {
            anyhow::bail!(RepositoryError::BlockNotFound(format!(
                "get_block_from_repo_or_archive: failed to load block: {block_id:?}"
            )));
        })
    }

    fn get_block_from_repo_or_archive_by_seq_no(
        &self,
        block_seq_no: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<<Self as Repository>::CandidateBlock>> {
        self.load_archived_block_by_seq_no(block_seq_no, thread_id)
    }

    fn last_finalized_optimistic_state(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> Option<<Self as Repository>::OptimisticState> {
        self.finalized_optimistic_states.guarded(|e| e.get(thread_id).cloned())
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
            Ok(None)
        }
    }

    fn mark_block_as_finalized(
        &mut self,
        block: impl Borrow<<Self as Repository>::CandidateBlock>,
        _block_state: BlockState,
        state_sync_service: Option<Arc<impl StateSyncService<Repository = RepositoryImpl>>>,
    ) -> anyhow::Result<()> {
        tracing::trace!("mark_block_as_finalized: {}", block.borrow());
        let block_id = block.borrow().data().identifier();
        let thread_id = block.borrow().data().get_common_section().thread_id;

        let block_state = self.block_state_repository.get(&block_id)?;
        self.finalized_blocks.guarded_mut(|e| e.store(block.borrow().clone()));
        let mut block_state_in = block_state.lock();
        let block_seq_no = block_state_in.deref().block_seq_no().unwrap();
        if !block_state_in.is_finalized() {
            block_state_in.set_finalized()?;

            if thread_id == ThreadIdentifier::default() {
                let _ = self.bk_set_update_tx.send(BkSetUpdate {
                    seq_no: block_seq_no.into(),
                    current: block_state_in.bk_set().clone(),
                    future: block_state_in.future_bk_set().clone(),
                });
            }
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

        if let Some(received_ms) = block_state_in.event_timestamps.received_ms {
            if let Some(m) = self.metrics.as_mut() {
                m.report_finalization_time(now_ms().saturating_sub(received_ms), &thread_id);
            }
        }
        drop(block_state_in);
        if let Err(e) = Self::save_metadata(&self.data_dir, self.metadatas.clone()) {
            tracing::error!("Failed to save metadata: {}", e);
            return Err(e);
        }
        // After sync we mark the incoming block as finalized and this check works
        let mut state = self
            .finalized_optimistic_states
            .guarded(|e| e.get(&thread_id).cloned())
            .expect("Node should have finalized state for thread");

        // After sync we mark the incoming block as finalized and this check works
        if block_id != state.block_id && block_seq_no > state.block_seq_no {
            // TODO: ask why was it here
            // TODO: self.finalized_optimistic_states is static without this apply block

            let new_state = if let Some(saved_state) = self
                .optimistic_states_cache
                .guarded_mut(|e| e.get_mut(&thread_id).and_then(|map| map.get(&block_id).cloned()))
            {
                tracing::trace!("Finalized block: load state");

                #[cfg(feature = "messages_db")]
                unimplemented!("Need to load messages here");

                self.finalized_optimistic_states
                    .guarded_mut(|e| e.insert(thread_id, saved_state.clone()));
                saved_state
            } else {
                tracing::trace!("Finalized block: apply state");
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

                #[cfg(feature = "messages_db")]
                self.message_storage_service.write(_messages)?;

                self.finalized_optimistic_states
                    .guarded_mut(|e| e.insert(thread_id, state.clone()));
                state
            };
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
                    service.save_state_for_sharing(Arc::new(full_state))?;
                }
            }

            if let Some(threads_table) = &block.borrow().data().get_common_section().threads_table {
                for thread_id in threads_table.list_threads() {
                    if thread_id.is_spawning_block(&block_id) {
                        self.optimistic_states_cache.guarded_mut(|e| {
                            e.entry(*thread_id)
                                .or_insert(LruCache::new(
                                    NonZeroUsize::new(self.states_cache_size).unwrap(),
                                ))
                                .push(block_id.clone(), new_state.clone())
                        });
                        self.finalized_optimistic_states.guarded_mut(|e| {
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
    ) -> anyhow::Result<Self::OptimisticState> {
        let mut state = Self::OptimisticState::zero();
        if let Some(path) = &self.zerostate_path {
            let zerostate = ZeroState::load_from_file(path)?;
            state = zerostate.state(thread_id)?.clone();
        };
        Ok(state)
    }

    fn get_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        min_state: Option<Self::OptimisticState>,
    ) -> anyhow::Result<Option<Self::OptimisticState>> {
        log::info!("RepositoryImpl: get_optimistic_state: {block_id:?}");
        if let Some(cached) = self
            .optimistic_states_cache
            .guarded_mut(|e| e.get_mut(thread_id).and_then(|m| m.get(block_id)).cloned())
        {
            return Ok(Some(cached.clone()));
        }
        let zero_block_id = <BlockIdentifier>::default();
        log::info!("RepositoryImpl: get_optimistic_state: load {block_id:?}");
        if let Some(state) = self.finalized_optimistic_states.guarded(|e| {
            e.iter().find(|(_, state)| state.block_id == *block_id).map(|(_, state)| state.clone())
        }) {
            return Ok(Some(state.clone()));
        }
        if block_id == &zero_block_id {
            return Ok(self.get_zero_state_for_thread(thread_id).ok());
        }
        let path = self.get_optimistic_state_path();
        let state = match self.load::<Vec<u8>>(path, OID::ID(block_id.to_string()))? {
            Some(buffer) => {
                let state = Self::OptimisticState::deserialize_from_buf(&buffer)?;
                Some(state)
            }
            None => self.try_load_state_from_archive(
                block_id,
                Arc::clone(&self.nack_set_cache),
                min_state,
            )?,
        };
        Ok(state.inspect(|state| {
            self.optimistic_states_cache.guarded_mut(|e| {
                e.entry(*thread_id)
                    .or_insert(LruCache::new(NonZeroUsize::new(self.states_cache_size).unwrap()))
                    .push(block_id.clone(), state.clone())
            });
        }))
    }

    fn get_full_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        min_state: Option<Self::OptimisticState>,
    ) -> anyhow::Result<Option<Self::OptimisticState>> {
        let mut state = self.get_optimistic_state(block_id, thread_id, min_state)?;
        if let Some(Some(state)) = self.split_state.then_some(&mut state) {
            let mut shard_state = state.get_shard_state().as_ref().clone();
            let mut shard_accounts = shard_state
                .read_accounts()
                .map_err(|e| anyhow::format_err!("Failed to read shard accounts: {e}"))?;
            shard_accounts.clone().iterate_accounts(|account_id, mut shard_acc, aug| {
                    if shard_acc.is_external() {
                        let acc_root = match state.cached_accounts.get(&account_id) {
                            Some((_, acc_root)) => acc_root.clone(),
                            None => {
                                self.accounts.load_account(&account_id, shard_acc.last_trans_hash(), shard_acc.last_trans_lt()).map_err(|err| tvm_types::error!("{}", err))?
                            }
                        };
                        if acc_root.repr_hash() != shard_acc.account_cell().repr_hash() {
                            return Err(tvm_types::error!("External account {account_id} cell hash mismatch: required: {}, actual: {}", acc_root.repr_hash(), shard_acc.account_cell().repr_hash()));
                        }
                        shard_acc.set_account_cell(acc_root);
                        shard_accounts.insert_with_aug(&account_id, &shard_acc, &aug)?;
                    }
                    Ok(true)
                })
                .map_err(|e| anyhow::format_err!("Failed to load accounts: {e}"))?;
            shard_state
                .write_accounts(&shard_accounts)
                .map_err(|e| anyhow::format_err!("Failed to write shard accounts: {e}"))?;
            state.set_shard_state(Arc::new(shard_state));
        }
        Ok(state)
    }

    fn erase_block_and_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
        _thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        if !self.is_block_present_in_archive(block_id).unwrap_or(false) {
            tracing::trace!("Block was not saved to archive, do not erase it");
            return Ok(());
        }
        tracing::trace!("erase_block: {:?}", block_id);

        self.erase_markers(block_id)?;
        self.erase_candidate_block(block_id)?;
        Ok(())
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
        let result = self.block_state_repository.get(block_id)?.lock().is_block_already_applied();
        tracing::trace!(?block_id, "is_block_already_applied: {result}");
        Ok(result)
    }

    fn set_state_from_snapshot(
        &mut self,
        snapshot: Self::StateSnapshot,
        cur_thread_id: &ThreadIdentifier,
        skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
    ) -> anyhow::Result<()> {
        tracing::debug!("set_state_from_snapshot");
        let thread_snapshot: ThreadSnapshot = bincode::deserialize(&snapshot)?;
        let mut state = <Self as Repository>::OptimisticState::deserialize_from_buf(
            thread_snapshot.optimistic_state(),
        )?;
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
            let mut skipped_attestation_ids = skipped_attestation_ids.lock();
            for block_id in &thread_snapshot.finalized_block_stats().attestations_watched() {
                skipped_attestation_ids.insert(block_id.clone());
            }
        }

        let block_state_repo_clone = self.block_state_repository.clone();
        let update_block_state = || {
            let block_state =
                block_state_repo_clone.get(&thread_snapshot.finalized_block.data().identifier())?;
            block_state.guarded_mut(|state| {
                state.set_thread_identifier(
                    thread_snapshot.finalized_block.data().get_common_section().thread_id,
                )?;
                state
                    .set_parent_block_identifier(thread_snapshot.finalized_block.data().parent())?;
                state.set_producer(
                    thread_snapshot.finalized_block.data().get_common_section().producer_id.clone(),
                )?;
                state.set_block_seq_no(thread_snapshot.finalized_block.data().seq_no())?;
                state.set_block_time_ms(thread_snapshot.finalized_block.data().time().unwrap())?;
                state.set_common_checks_passed()?;
                state.set_prefinalized()?;
                state.set_finalized()?;
                state.set_applied()?;
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
                state.set_ancestor_blocks_finalization_distances(HashMap::new())?;
                state.set_attestation_target(thread_snapshot.attestation_target)?;
                state.set_prefinalization_proof(thread_snapshot.prefinalization_proof.clone())?;
                // TODO: check with alexander.s if it was some kind of fix
                // state.set_attestation_target(thread_snapshot.attestation_target)?;
                state.set_producer_selector_data(thread_snapshot.producer_selector.clone())?;
                Ok::<(), anyhow::Error>(())
            })?;
            crate::node::services::block_processor::rules::descendant_bk_set::set_descendant_bk_set(
                &block_state,
                &thread_snapshot.finalized_block,
            );
            Ok::<(), anyhow::Error>(())
        };

        if let Some(finalized_seq_no) = self
            .finalized_optimistic_states
            .guarded(|e| e.get(&thread_id).map(|state| state.block_seq_no))
        {
            tracing::trace!(
                "Repo already has finalized state for the thread, seq_no: {}",
                finalized_seq_no
            );
            if seq_no > finalized_seq_no {
                let shard_state = state.get_shard_state();
                self.finalized_optimistic_states
                    .guarded_mut(|e| e.insert(thread_id, state.clone()));
                self.sync_accounts_from_state(shard_state)?;
                update_block_state()?;
            }
        } else {
            self.finalized_optimistic_states.guarded_mut(|e| e.insert(thread_id, state.clone()));

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
                            AccountAddress(addr),
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

        self.shared_services.exec(|services| {
            for ref_data in thread_snapshot.cross_thread_ref_data.clone().into_iter() {
                services
                    .cross_thread_ref_data_service
                    .set_cross_thread_ref_data(ref_data)
                    .expect("Failed to load cross-thread-ref-data");
            }
        });
        self.finalize_synced_block(&thread_snapshot, &state)?;

        Ok(())
    }

    fn is_block_suspicious(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>> {
        let guarded = self.block_state_repository.get(block_id)?;
        let result = !guarded.lock().has_all_nacks_resolved();
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
                            &acc_id,
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

        let sqlite_helper_config = SqliteHelperConfig::new(self.data_dir.clone(), None);

        // TODO: _writer_join_handle is not used here.
        // Probably this crate needs to be deeply refactored.
        let (sqlite_helper, _writer_join_handle) = SqliteHelper::from_config(sqlite_helper_config)?;
        let sqlite_helper = Arc::new(sqlite_helper);

        let cnt_accounts = arch_accounts.len();
        sqlite_helper.put_accounts(arch_accounts).map_err(|e| anyhow::format_err!("{e}"))?;
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

    fn store_optimistic_in_cache<T: Into<Self::OptimisticState>>(
        &self,
        state: T,
    ) -> anyhow::Result<()> {
        let optimistic = state.into();
        let block_id = optimistic.get_block_id().clone();
        let thread_id = *optimistic.get_thread_id();
        tracing::trace!("save optimistic to cache {block_id:?} {thread_id:?}");
        self.optimistic_states_cache
            .lock()
            .entry(thread_id)
            .or_insert(LruCache::new(NonZeroUsize::new(self.states_cache_size).unwrap()))
            .push(block_id, optimistic);
        Ok(())
    }

    fn store_optimistic<T: Into<Self::OptimisticState>>(&self, state: T) -> anyhow::Result<()> {
        let mut optimistic = state.into();
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

        if !optimistic.changed_accounts.is_empty() || !optimistic.cached_accounts.is_empty() {
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
                        .insert_with_aug(&account_id, &account, &aug)
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

        let state_bytes =
            OptimisticState::serialize_into_buf(optimistic).expect("Failed to serialize block");
        let res = save_to_file(&path, &state_bytes, false);
        tracing::trace!(
            "save optimistic {block_id:?} result: {res:?} {}",
            start_save.elapsed().as_millis()
        );
        res?;
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
    use crate::message_storage::MessageDurableStorage;
    use crate::multithreading::routing::service::RoutingService;
    use crate::node::shared_services::SharedServices;
    use crate::repository::accounts::AccountsRepository;
    use crate::repository::repository_impl::BlockStateRepository;
    use crate::repository::repository_impl::FinalizedBlockStorage;
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
            BlockStateRepository::new(PathBuf::from("./tests-data/test_save_load/block-state"));
        let accounts_repository =
            AccountsRepository::new(PathBuf::from("./tests-data/test_save_load"), Some(0), 1);
        let message_db = MessageDurableStorage::new(PathBuf::from("./tmp/message_storage1"))?;
        let finalized_blocks = finalized_blocks_storage();
        let repository = RepositoryImpl::new(
            PathBuf::from("./tests-data/test_save_load"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
            SharedServices::test_start(RoutingService::stub().0, u32::MAX),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            true,
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
        let block_state_repository = BlockStateRepository::new(PathBuf::from(
            "/home/user/GOSH/acki-nacki/server_data/node1/block-state/",
        ));
        let accounts_repository = AccountsRepository::new(
            PathBuf::from("/home/user/GOSH/acki-nacki/server_data/node1/"),
            Some(0),
            1,
        );
        let message_db = MessageDurableStorage::new(PathBuf::from("./tmp/message_storage2"))?;
        let finalized_blocks = finalized_blocks_storage();
        let _repository = RepositoryImpl::new(
            PathBuf::from("/home/user/GOSH/acki-nacki/server_data/node1/"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
            SharedServices::test_start(RoutingService::stub().0, u32::MAX),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            true,
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
            BlockStateRepository::new(PathBuf::from("./tests-data/test_exists/block-state"));
        let accounts_repository =
            AccountsRepository::new(PathBuf::from("./tests-data/test_exists"), Some(0), 1);
        let message_db = MessageDurableStorage::new(PathBuf::from("./tmp/message_storage3"))?;
        let finalized_blocks = finalized_blocks_storage();

        let repository = RepositoryImpl::new(
            PathBuf::from("./tests-data/test_exists"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
            SharedServices::test_start(RoutingService::stub().0, u32::MAX),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            true,
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
            BlockStateRepository::new(PathBuf::from("./tests-data/test_remove/block-state"));
        let accounts_repository =
            AccountsRepository::new(PathBuf::from("./tests-data/test_remove"), Some(0), 1);
        let message_db = MessageDurableStorage::new(PathBuf::from("./tmp/message_storage4"))?;
        let finalized_blocks = finalized_blocks_storage();

        let repository = RepositoryImpl::new(
            PathBuf::from("./tests-data/test_remove"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
            SharedServices::test_start(RoutingService::stub().0, u32::MAX),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            true,
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
