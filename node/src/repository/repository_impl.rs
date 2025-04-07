// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

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
use telemetry_utils::now_ms;
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
use crate::external_messages::ExternalMessagesThreadState;
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
use crate::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BlockStatistics;
use crate::node::shared_services::SharedServices;
use crate::node::NodeIdentifier;
use crate::node::SignerIndex;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;
use crate::repository::RepositoryError;
use crate::types::AccountAddress;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::AllowGuardedMut;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;
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

// TODO: divide repository into 2 entities: one for blocks, one for states with weak refs (for not
// to store optimistic states longer than necessary)
pub struct RepositoryImpl {
    archive: Connection,
    data_dir: PathBuf,
    zerostate_path: Option<PathBuf>,
    metadatas: RepositoryMetadata,
    // Remove
    blocks_cache:
        Arc<Mutex<LruCache<BlockIdentifier, (Envelope<GoshBLS, AckiNackiBlock>, BlockMarkers)>>>,
    saved_states: Arc<Mutex<HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockIdentifier>>>>,
    finalized_optimistic_states: Arc<Mutex<HashMap<ThreadIdentifier, OptimisticStateImpl>>>,
    finalized_blocks_buffers:
        Arc<Mutex<HashMap<ThreadIdentifier, Arc<Mutex<Vec<BlockIdentifier>>>>>>,
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
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug)]
enum OID {
    ID(String),
    #[allow(dead_code)]
    SingleRow,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
pub struct BlockMarkers {
    pub is_processed: bool,
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
pub struct WrappedStateSnapshot {
    optimistic_state: Vec<u8>,
    cross_thread_ref_data: Vec<CrossThreadRefData>,
    finalized_block_stats: BlockStatistics,
    bk_set: BlockKeeperSet,
    db_messages: Vec<Vec<Arc<WrappedMessage>>>,
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
    if let Some(path) = file_path.parent() {
        fs::create_dir_all(path)?;
    }
    let mut file = File::create(file_path)?;
    file.write_all(&buffer)?;
    if cfg!(feature = "sync_files") || force_sync {
        file.sync_all()?;
    }
    tracing::trace!("File saved: {:?}", file_path);
    Ok(())
}

impl Clone for RepositoryImpl {
    // TODO: Repository object can consume great amount of memory, so it's better not to copy it
    fn clone(&self) -> Self {
        let db_path = self.data_dir.join(DB_ARCHIVE_FILE);
        let arc_conn = SqliteHelper::create_connection_ro(db_path.clone())
            .unwrap_or_else(|_| panic!("Failed to open {db_path:?}"));

        Self {
            archive: arc_conn,
            data_dir: self.data_dir.clone(),
            zerostate_path: self.zerostate_path.clone(),
            metadatas: self.metadatas.clone(),
            blocks_cache: self.blocks_cache.clone(),
            saved_states: self.saved_states.clone(),
            finalized_optimistic_states: self.finalized_optimistic_states.clone(),
            finalized_blocks_buffers: self.finalized_blocks_buffers.clone(),
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
        }
    }
}

impl RepositoryImpl {
    // TODO: remove option from zerostate_path
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        data_dir: PathBuf,
        zerostate_path: Option<PathBuf>,
        cache_size: usize,
        states_cache_size: usize,
        shared_services: SharedServices,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        split_state: bool,
        block_state_repository: BlockStateRepository,
        metrics: Option<BlockProductionMetrics>,
        message_db: MessageDurableStorage,
    ) -> Self {
        if let Err(err) = fs::create_dir_all(data_dir.clone()) {
            tracing::error!("Failed to create data dir {:?}: {}", data_dir, err);
        }
        let db_path = data_dir.join(DB_FILE);
        let conn = SqliteHelper::create_connection_ro(db_path.clone())
            .unwrap_or_else(|_| panic!("Failed to open {db_path:?}"));

        let arch_path = data_dir.join(DB_ARCHIVE_FILE);
        let archive = SqliteHelper::create_connection_ro(arch_path.clone())
            .unwrap_or_else(|_| panic!("Failed to open an archive DB {}", arch_path.display()));

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
            blocks_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(cache_size).unwrap(),
            ))),
            saved_states: Default::default(),
            finalized_optimistic_states: Arc::new(Mutex::new(finalized_optimistic_states)),
            finalized_blocks_buffers: Arc::new(Mutex::new(HashMap::new())),
            shared_services,
            nack_set_cache: Arc::clone(&nack_set_cache),
            block_state_repository: block_state_repository.clone(),
            optimistic_states_cache: Arc::new(Mutex::new(HashMap::new())),
            accounts: AccountsRepository::new(data_dir.clone()),
            split_state,
            metrics,
            message_db: message_db.clone(),
            message_storage_service,
            states_cache_size,
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
                                        block_id.clone(),
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
                    .get_optimistic_state(
                        &metadata.last_finalized_block_id,
                        thread_id,
                        Arc::clone(&nack_set_cache),
                        None,
                    )
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

    fn get_path(&self, path: &str, oid: String) -> PathBuf {
        PathBuf::from(format!("{}/{}/{}", self.data_dir.clone().to_str().unwrap(), path, oid))
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

    fn save_metadata(
        metadatas: RepositoryMetadata,
        path: PathBuf,
        thread: ThreadIdentifier,
    ) -> anyhow::Result<()> {
        let metadata = metadatas.guarded(|metadatas| {
            metadatas.get(&thread).map(|metadata_arc| {
                let metadata = metadata_arc.lock().deref().clone();
                metadata
            })
        });
        if let Some(metadata) = metadata {
            save_to_file(&path, &metadata, false)?;
        } else {
            return Err(anyhow::anyhow!("Metadata for thread {:?} not found", thread));
        }
        Ok(())
    }

    fn erase_markers(&self, _block_id: &BlockIdentifier) -> anyhow::Result<()> {
        // let _ = std::fs::remove_file(self.get_path("markers", format!("{block_id:?}")));
        Ok(())
    }

    fn load_markers(&self, block_id: &BlockIdentifier) -> anyhow::Result<BlockMarkers> {
        let mut cache = self.blocks_cache.lock();
        let res = if let Some((_, markers)) = cache.get(block_id) {
            markers.clone()
        } else {
            let path = self.get_path("markers", format!("{block_id:?}"));
            load_from_file(&path)?.unwrap_or_default()
        };
        Ok(res)
    }

    fn save_markers(
        &self,
        block_id: &BlockIdentifier,
        markers: &BlockMarkers,
    ) -> anyhow::Result<()> {
        tracing::trace!("save markers {block_id:?} {:?}", markers);
        let path = self.get_path("markers", format!("{block_id:?}"));
        let markers_clone = markers.clone();
        save_to_file(&path, &markers_clone, false)?;
        let mut cache = self.blocks_cache.lock();
        if let Some(entry) = cache.get_mut(block_id) {
            entry.1 = markers.clone();
        }
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

    fn save_candidate_block(
        &self,
        block: <RepositoryImpl as Repository>::CandidateBlock,
        candidate_marker: BlockMarkers,
    ) -> anyhow::Result<()> {
        let block_id = block.data().identifier();
        let path = self.get_path("blocks", format!("{block_id:?}"));

        let block_clone = block.clone();
        save_to_file(&path, &block_clone, false)?;

        let mut cache = self.blocks_cache.lock();
        cache.push(block.data().identifier(), (block, candidate_marker));
        Ok(())
    }

    fn load_candidate_block(
        &self,
        identifier: &BlockIdentifier,
    ) -> anyhow::Result<Option<<RepositoryImpl as Repository>::CandidateBlock>> {
        let mut cache = self.blocks_cache.lock();
        let envelope = if let Some((envelope, _)) = cache.get(identifier) {
            Some(envelope.clone())
        } else {
            let path = self.get_path("blocks", format!("{identifier:?}"));
            load_from_file(&path)?
        };
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
        let mut stmt = self.archive.prepare(sql)?;
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
        let mut stmt = self.archive.prepare(sql)?;
        let mut rows = stmt.query(params![identifier.to_string()])?;
        let res = rows.next()?.is_some();
        Ok(res)
    }

    pub(crate) fn load_archived_block(
        &self,
        identifier: &Option<BlockIdentifier>,
        thread_identifier: Option<ThreadIdentifier>,
    ) -> anyhow::Result<Option<<RepositoryImpl as Repository>::CandidateBlock>> {
        tracing::trace!("Loading an archived block (block_id: {identifier:?})...");
        let add_clause = if let Some(block_id) = identifier {
            format!(" WHERE id={:?}", block_id.to_string())
        } else if let Some(thread_id) = thread_identifier {
            format!(" WHERE thread_id={:?} ORDER BY seq_no DESC LIMIT 1", hex::encode(thread_id))
        } else {
            " ORDER BY seq_no DESC LIMIT 1".to_string()
        };
        let sql = format!(
            "SELECT aggregated_signature, signature_occurrences, data, id FROM blocks {}",
            add_clause,
        );
        let mut stmt = self.archive.prepare(&sql)?;
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
        let (_, last_finalized_seq_no) = self.select_thread_last_finalized_block(thread_id)?;
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
        keys_to_remove.pop();
        // for k in &keys_to_remove {
        //     let block_id = saved_states.get(k).unwrap();
        //     tracing::trace!("Clear optimistic state for {k} {block_id:?}");
        //     let optimistic_state_path = self.get_optimistic_state_path();
        //     self.remove(optimistic_state_path, OID::ID(block_id.to_string()))?;
        // }
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

        let mut available_states: HashSet<BlockIdentifier> = {
            let saved_states =
                self.saved_states.guarded(|e| e.get(&thread_id).cloned().unwrap_or_default());
            let ids: Vec<BlockIdentifier> = saved_states.values().cloned().collect();
            HashSet::from_iter(ids)
        };
        let finalized_state =
            self.finalized_optimistic_states.guarded(|map| map.get(&thread_id).cloned());
        if let Some(finalized_state) = finalized_state.as_ref() {
            available_states.insert(finalized_state.block_id.clone());
        }
        if let Some(path) = &self.zerostate_path {
            let zerostate = ZeroState::load_from_file(path)?;
            if let Ok(state) = zerostate.state(&thread_id) {
                available_states.insert(state.block_id.clone());
            }
        };
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
            self.get_optimistic_state(&state_id, &thread_id, Arc::clone(&nack_set_cache), None)?
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
        let archive = SqliteHelper::create_connection_ro(tmp_path.clone()).unwrap();
        let data_dir = PathBuf::default();
        let message_db = MessageDurableStorage::new(tmp_path).unwrap();
        let message_service =
            crate::message_storage::writer_service::MessageDBWriterService::new(message_db.clone())
                .unwrap();
        Self {
            archive,
            accounts: AccountsRepository::new(data_dir.clone()),
            data_dir,
            zerostate_path: None,
            metadatas: Arc::new(Mutex::new(metadatas)),
            blocks_cache: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1).unwrap()))),
            saved_states: Arc::new(Mutex::new(HashMap::new())),
            finalized_optimistic_states: Arc::new(Mutex::new(HashMap::new())),
            finalized_blocks_buffers: Arc::new(Mutex::new(HashMap::new())),
            shared_services: SharedServices::test_start(RoutingService::stub().0),
            nack_set_cache: Arc::new(Mutex::new(FixedSizeHashSet::new(0))),
            block_state_repository,
            optimistic_states_cache: Arc::new(Mutex::new(HashMap::new())),
            split_state: false,
            metrics: None,
            message_db,
            message_storage_service: message_service,
            states_cache_size: 1,
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
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
    ) -> anyhow::Result<()> {
        let mut guarded = self.metadatas.lock();
        if guarded.contains_key(thread_id) {
            let metadata = guarded.get(thread_id).unwrap().lock();
            assert_eq!(&metadata.last_finalized_block_id, parent_block_id);
            return Ok(());
        }

        tracing::trace!(
            "Checking - {}. Markers - {:?}",
            parent_block_id,
            self.load_markers(parent_block_id)
        );
        ensure!(self.is_block_finalized(parent_block_id)? == Some(true));
        let optimistic_state = {
            let optimistic_state = self.get_optimistic_state(
                parent_block_id,
                thread_id,
                Arc::clone(&nack_set_cache),
                None,
            )?;
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
        let path = self.get_path(Self::get_metadata_path(), format!("{}", thread_id));
        Self::save_metadata(self.metadatas.clone(), path, *thread_id)?;
        Ok(())
    }

    fn get_block(
        &self,
        identifier: &BlockIdentifier,
    ) -> anyhow::Result<Option<Self::CandidateBlock>> {
        self.load_candidate_block(identifier)
    }

    fn get_block_from_repo_or_archive(
        &self,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<<Self as Repository>::CandidateBlock> {
        Ok(if let Some(block) = self.get_block(block_id)? {
            block
        } else if let Some(block) = self.load_archived_block(&Some(block_id.clone()), None)? {
            block
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
    ) -> anyhow::Result<(BlockIdentifier, BlockSeqNo)> {
        // tracing::trace!("select_thread_last_finalized_block: start");
        // TODO: Critical: This "fast-forward" is already broken.
        // It will not survive thread merge operations.
        // TODO: self.finalized_states can be used here
        let metadata = self.get_metadata_for_thread(thread_id)?;
        let guarded = metadata.lock();
        let cursor_id = guarded.last_finalized_block_id.clone();
        let cursor_seq_no = guarded.last_finalized_block_seq_no;
        drop(guarded);
        tracing::trace!("select_thread_last_finalized_block: {cursor_seq_no:?} {cursor_id:?}");
        Ok((cursor_id, cursor_seq_no))
    }

    fn mark_block_as_finalized(
        &mut self,
        block: &<Self as Repository>::CandidateBlock,
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        _block_state: BlockState,
    ) -> anyhow::Result<()> {
        tracing::trace!("mark_block_as_finalized: {}", block);
        let block_id = block.data().identifier();
        let thread_id = block.data().get_common_section().thread_id;

        {
            for (thread, buffer) in self.finalized_blocks_buffers.lock().iter() {
                if *thread != thread_id {
                    let mut locked_buffer = buffer.lock();
                    locked_buffer.push(block_id.clone());
                }
            }
        }

        let block_state = self.block_state_repository.get(&block_id)?;
        let mut block_state_in = block_state.lock();
        if !block_state_in.is_finalized() {
            block_state_in.set_finalized()?;
        }

        let block_seq_no = block_state_in.deref().block_seq_no().unwrap();
        let metadata = self.get_metadata_for_thread(&thread_id)?;
        let mut metadata = metadata.lock();

        if block_seq_no > metadata.last_finalized_block_seq_no {
            metadata.last_finalized_block_seq_no = block_seq_no;
            metadata.last_finalized_block_id = block_id.clone();
            metadata.last_finalized_producer_id =
                Some(block.data().get_common_section().producer_id.clone());
        }
        drop(metadata);

        if let Some(received_ms) = block_state_in.event_timestamps.received_ms {
            if let Some(m) = self.metrics.as_mut() {
                m.report_finalization_time(now_ms().saturating_sub(received_ms), &thread_id);
            }
        }
        drop(block_state_in);
        let metadata = self.metadatas.clone();
        let path = self.get_path(Self::get_metadata_path(), format!("{}", thread_id));
        if let Err(e) = Self::save_metadata(metadata, path, thread_id) {
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

            if let Some(saved_state) = self
                .optimistic_states_cache
                .guarded_mut(|e| e.get_mut(&thread_id).and_then(|map| map.get(&block_id).cloned()))
            {
                tracing::trace!("Finalized block: load state");

                #[cfg(feature = "messages_db")]
                unimplemented!("Need to load messages here");

                self.finalized_optimistic_states.guarded_mut(|e| e.insert(thread_id, saved_state));
            } else {
                tracing::trace!("Finalized block: apply state");
                let (_, _messages): (
                    _,
                    HashMap<AccountAddress, Vec<(MessageIdentifier, Arc<WrappedMessage>)>>,
                ) = state.apply_block(
                    block.data(),
                    &self.shared_services,
                    self.block_state_repository.clone(),
                    nack_set_cache,
                    self.accounts.clone(),
                    self.message_db.clone(),
                )?;

                #[cfg(feature = "messages_db")]
                self.message_storage_service.write(_messages)?;

                self.finalized_optimistic_states.guarded_mut(|e| e.insert(thread_id, state));
            }
        }
        Ok(())
    }

    fn is_block_finalized(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>> {
        let markers = self.block_state_repository.get(block_id)?;
        let result = markers.lock().is_finalized();
        Ok(Some(result))
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
        nack_set_cache: Arc<Mutex<FixedSizeHashSet<UInt256>>>,
        min_state: Option<Self::OptimisticState>,
    ) -> anyhow::Result<Option<Self::OptimisticState>> {
        log::info!("RepositoryImpl: get_optimistic_state: {:?}", block_id);
        if let Some(cached) = self
            .optimistic_states_cache
            .guarded_mut(|e| e.get_mut(thread_id).and_then(|m| m.get(block_id)).cloned())
        {
            return Ok(Some(cached.clone()));
        }
        let zero_block_id = <BlockIdentifier>::default();
        log::info!("RepositoryImpl: get_optimistic_state: load {:?}", block_id);
        if let Some(state) = self.finalized_optimistic_states.guarded(|e| {
            e.iter().find(|(_, state)| state.block_id == *block_id).map(|(_, state)| state.clone())
        }) {
            return Ok(Some(state.clone()));
        }
        if block_id == &zero_block_id {
            // Moved below finalized since it could be that the state was loaded
            // from file
            return Ok(None);
        }
        let path = self.get_optimistic_state_path();
        let state = match self.load::<Vec<u8>>(path, OID::ID(block_id.to_string()))? {
            Some(buffer) => {
                let state = Self::OptimisticState::deserialize_from_buf(&buffer, block_id.clone())?;
                Some(state)
            }
            None => {
                self.try_load_state_from_archive(block_id, Arc::clone(&nack_set_cache), min_state)?
            }
        };
        Ok(state.inspect(|state| {
            self.optimistic_states_cache.guarded_mut(|e| {
                e.entry(*thread_id)
                    .or_insert(LruCache::new(NonZeroUsize::new(self.states_cache_size).unwrap()))
                    .push(block_id.clone(), state.clone())
            });
        }))
    }

    fn store_block<T: Into<Self::CandidateBlock>>(&self, block: T) -> anyhow::Result<()> {
        let candidate_block = block.into();
        let candidate_marker = self.load_markers(&candidate_block.data().identifier())?;
        self.save_candidate_block(candidate_block, candidate_marker)?;
        Ok(())
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

    fn mark_block_as_processed(&self, block_id: &BlockIdentifier) -> anyhow::Result<()> {
        let mut markers = self.load_markers(block_id)?;
        markers.is_processed = true;
        tracing::trace!(?block_id, "mark_block_as_processed");
        self.save_markers(block_id, &markers)
    }

    fn is_block_processed(&self, block_id: &BlockIdentifier) -> anyhow::Result<bool> {
        let is_processed = self.load_markers(block_id).unwrap_or_default().is_processed;
        tracing::trace!(?block_id, "is_block_processed: {is_processed}");
        Ok(is_processed)
    }

    fn is_block_already_applied(&self, block_id: &BlockIdentifier) -> anyhow::Result<bool> {
        let result = self.block_state_repository.get(block_id)?.lock().is_block_already_applied();
        tracing::trace!(?block_id, "is_block_already_applied: {result}");
        Ok(result)
    }

    fn set_state_from_snapshot(
        &mut self,
        block_id: &BlockIdentifier,
        snapshot: Self::StateSnapshot,
        thread_id: &ThreadIdentifier,
        skipped_attestation_ids: Arc<Mutex<HashSet<BlockIdentifier>>>,
        external_messages: ExternalMessagesThreadState,
    ) -> anyhow::Result<Vec<CrossThreadRefData>> {
        tracing::debug!("set_state_from_snapshot()...");
        let wrapped_snapshot: WrappedStateSnapshot = bincode::deserialize(&snapshot)?;
        tracing::trace!("Set state from snapshot: {block_id:?}");
        let mut state = <Self as Repository>::OptimisticState::deserialize_from_buf(
            wrapped_snapshot.optimistic_state(),
            block_id.clone(),
        )?;
        self.store_optimistic(state.clone())?;
        let seq_no = BlockSeqNo::from(
            state
                .get_block_info()
                .prev1()
                .map_err(|e| anyhow::format_err!("Failed to read block info: {e}"))?
                .seq_no,
        );
        {
            tracing::trace!(
                "set_state_from_snapshot: fin stats: {:?}",
                wrapped_snapshot.finalized_block_stats()
            );
            let mut skipped_attestation_ids = skipped_attestation_ids.lock();
            for block_id in &wrapped_snapshot.finalized_block_stats().attestations_watched() {
                skipped_attestation_ids.insert(block_id.clone());
            }
        }

        if let Some(finalized_seq_no) = self
            .finalized_optimistic_states
            .guarded(|e| e.get(thread_id).map(|state| state.block_seq_no))
        {
            if seq_no > finalized_seq_no {
                let shard_state = state.get_shard_state();
                self.finalized_optimistic_states.guarded_mut(|e| e.insert(*thread_id, state));
                self.sync_accounts_from_state(shard_state)?;
                external_messages.set_progress_to_last_known(block_id)?;
                let block_state = self.block_state_repository.get(block_id)?;
                block_state.guarded_mut(|e| {
                    if e.common_checks_passed() != &Some(true) {
                        e.set_common_checks_passed()?;
                    }
                    if !e.is_block_already_applied() {
                        e.set_applied()?;
                    }
                    if e.bk_set().is_none() {
                        e.set_bk_set(Arc::new(wrapped_snapshot.bk_set().clone()))?;
                    }
                    if e.block_stats().is_none() {
                        e.set_block_stats(wrapped_snapshot.finalized_block_stats().clone())?;
                    }
                    Ok::<(), anyhow::Error>(())
                })?;
                crate::node::services::block_processor::rules::descendant_bk_set::set_descendant_bk_set(&block_state, self);
            }
        } else {
            self.finalized_optimistic_states.guarded_mut(|e| e.insert(*thread_id, state));

            external_messages.set_progress_to_last_known(block_id)?;
            let block_state = self.block_state_repository.get(block_id)?;
            block_state.guarded_mut(|e| {
                if e.common_checks_passed() != &Some(true) {
                    e.set_common_checks_passed()?;
                }
                if !e.is_block_already_applied() {
                    e.set_applied()?;
                }
                if e.bk_set().is_none() {
                    e.set_bk_set(Arc::new(wrapped_snapshot.bk_set))?;
                }
                if e.block_stats().is_none() {
                    e.set_block_stats(wrapped_snapshot.finalized_block_stats)?;
                }
                Ok::<(), anyhow::Error>(())
            })?;
            crate::node::services::block_processor::rules::descendant_bk_set::set_descendant_bk_set(
                &block_state,
                self,
            );
        }
        let mut all_states = self.saved_states.lock();
        let saved_states = all_states.entry(*thread_id).or_default();
        saved_states.insert(seq_no, block_id.clone());

        let db_messages = wrapped_snapshot
            .db_messages
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

        Ok(wrapped_snapshot.cross_thread_ref_data)
    }

    fn is_block_suspicious(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>> {
        let guarded = self.block_state_repository.get(block_id)?;
        let result = guarded.lock().has_unresolved_nacks();
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
        self.clear_optimistic_states(&optimistic.thread_id)?;
        let block_seq_no = BlockSeqNo::from(
            optimistic
                .get_block_info()
                .prev1()
                .map_err(|e| anyhow::format_err!("Failed to read block info: {e}"))?
                .seq_no,
        );
        let root_path = self.get_optimistic_state_path();
        let path = self.get_path(root_path, block_id.to_string());
        let saved_states_clone = self.saved_states.clone();
        let start_save = std::time::Instant::now();
        let thread_id = optimistic.thread_id;

        if self.split_state && !optimistic.changed_accounts.is_empty() {
            let changed_accounts = std::mem::take(&mut optimistic.changed_accounts);
            let shard_state = optimistic.get_shard_state();
            let accounts = shard_state.read_accounts().map_err(|e| anyhow::format_err!("{e}"))?;
            for account_id in changed_accounts {
                if let Some(account) = accounts
                    .account(&(&account_id).into())
                    .map_err(|e| anyhow::format_err!("Failed to read account: {e}"))?
                {
                    if account.is_external() {
                        return Err(anyhow::format_err!("External account in changed_accounts"));
                    }
                    self.accounts.store_account(
                        &account_id,
                        account.last_trans_hash(),
                        account.last_trans_lt(),
                        account.account_cell(),
                    )?;
                }
            }
        }

        let state_bytes = OptimisticState::serialize_into_buf(optimistic, self.split_state)
            .expect("Failed to serialize block");
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

    fn add_thread_buffer(&self, thread_id: ThreadIdentifier) -> Arc<Mutex<Vec<BlockIdentifier>>> {
        let buffer = Arc::new(Mutex::new(vec![]));
        self.finalized_blocks_buffers.lock().insert(thread_id, buffer.clone());
        buffer
    }

    fn get_all_metadata(&self) -> RepositoryMetadata {
        self.metadatas.clone()
    }

    fn get_message_db(&self) -> MessageDurableStorage {
        self.message_db.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::Once;

    use parking_lot::lock_api::Mutex;

    use super::RepositoryImpl;
    use super::OID;
    use crate::message_storage::MessageDurableStorage;
    use crate::multithreading::routing::service::RoutingService;
    use crate::node::shared_services::SharedServices;
    use crate::repository::repository_impl::BlockStateRepository;
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

    #[test]
    #[ignore]
    fn test_save_load() -> anyhow::Result<()> {
        START.call_once(init_tests);

        let block_state_repository =
            BlockStateRepository::new(PathBuf::from("./tests-data/test_save_load/block-state"));
        let message_db = MessageDurableStorage::new(PathBuf::from("./tmp/message_storage1"))?;
        let repository = RepositoryImpl::new(
            PathBuf::from("./tests-data/test_save_load"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
            1,
            SharedServices::test_start(RoutingService::stub().0),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            true,
            block_state_repository,
            None,
            message_db.clone(),
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
        let message_db = MessageDurableStorage::new(PathBuf::from("./tmp/message_storage2"))?;
        let _repository = RepositoryImpl::new(
            PathBuf::from("/home/user/GOSH/acki-nacki/server_data/node1/"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
            1,
            SharedServices::test_start(RoutingService::stub().0),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            true,
            block_state_repository,
            None,
            message_db.clone(),
        );
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_exists() -> anyhow::Result<()> {
        START.call_once(init_tests);
        let block_state_repository =
            BlockStateRepository::new(PathBuf::from("./tests-data/test_exists/block-state"));
        let message_db = MessageDurableStorage::new(PathBuf::from("./tmp/message_storage3"))?;

        let repository = RepositoryImpl::new(
            PathBuf::from("./tests-data/test_exists"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
            1,
            SharedServices::test_start(RoutingService::stub().0),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            true,
            block_state_repository,
            None,
            message_db.clone(),
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
        let message_db = MessageDurableStorage::new(PathBuf::from("./tmp/message_storage4"))?;
        let repository = RepositoryImpl::new(
            PathBuf::from("./tests-data/test_remove"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
            1,
            SharedServices::test_start(RoutingService::stub().0),
            Arc::new(Mutex::new(FixedSizeHashSet::new(10))),
            true,
            block_state_repository,
            None,
            message_db.clone(),
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
