// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::hash::Hash;
use std::io::Read;
use std::io::Write;
use std::num::NonZeroUsize;
use std::ops::Add;
use std::ops::Deref;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;

use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use database::documents_db::DocumentsDb;
use database::documents_db::SerializedItem;
use database::sqlite::sqlite_helper::SqliteHelper;
use database::sqlite::ArchAccount;
use lru::LruCache;
use parking_lot::Mutex;
use rusqlite::params;
use rusqlite::Connection;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use tvm_block::HashmapAugType;
use tvm_block::ShardStateUnsplit;

use crate::block::keeper::process::prepare_prev_block_info;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::database::serialize_block::prepare_account_archive_struct;
use crate::database::write_to_db;
use crate::message::WrappedMessage;
use crate::node::associated_types::AttestationData;
use crate::node::NodeIdentifier;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::Repository;
use crate::types::next_seq_no as calc_next_seq_no;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;
use crate::zerostate::ZeroState;

const DEFAULT_OID: &str = "00000000000000000000";
const REQUIRED_SQLITE_SCHEMA_VERSION: u32 = 2;
const _REQUIRED_SQLITE_ARC_SCHEMA_VERSION: u32 = 2;
const DB_FILE: &str = "node.db";
const DB_ARCHIVE_FILE: &str = "node-archive.db";
pub const EXT_MESSAGE_STORE_TIMEOUT_SECONDS: i64 = 60;
const MAX_BLOCK_SEQ_NO_THAT_CAN_BE_BUILT_FROM_ZEROSTATE: u32 = 200;
const MAX_BLOCK_CNT_THAT_CAN_BE_LOADED_TO_PREPARE_STATE: usize = 400;

pub struct RepositoryImpl {
    archive: Connection,
    data_dir: PathBuf,
    zerostate_path: Option<PathBuf>,
    metadatas: HashMap<ThreadIdentifier, Arc<Mutex<Metadata<BlockIdentifier, BlockSeqNo>>>>,
    last_ext_message_index: u32,
    ext_messages_mutex: Arc<Mutex<()>>,
    blocks_cache: Arc<
        Mutex<
            LruCache<BlockIdentifier, (Envelope<GoshBLS, AckiNackiBlock<GoshBLS>>, BlockMarkers)>,
        >,
    >,
    saved_states: Arc<Mutex<HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockIdentifier>>>>,
    finalized_optimistic_states: HashMap<ThreadIdentifier, OptimisticStateImpl>,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug)]
enum OID {
    ID(String),
    SingleRow,
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct BlockMarkers {
    is_finalized: Option<bool>,
    is_main_candidate: Option<bool>,
    last_processed_ext_message: u32,
    is_verified: bool,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
struct Metadata<TBlockIdentifier: Hash + Eq, TBlockSeqNo> {
    last_main_candidate_block_id: TBlockIdentifier,
    last_main_candidate_block_seq_no: TBlockSeqNo,
    last_finalized_block_id: TBlockIdentifier,
    last_finalized_block_seq_no: TBlockSeqNo,
    block_id_to_seq_no: HashMap<TBlockIdentifier, TBlockSeqNo>,
    stored_finalized_blocks: Vec<(TBlockIdentifier, TBlockSeqNo)>,
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

#[derive(Serialize, Deserialize)]
pub struct WrappedStateSnapshot {
    pub optimistic_state: Vec<u8>,
    pub producer_group: HashMap<ThreadIdentifier, Vec<NodeIdentifier>>,
    pub block_keeper_set: HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>,
}

impl<TMessage> Default for ExtMessages<TMessage>
where
    TMessage: Clone,
{
    fn default() -> Self {
        Self { queue: vec![] }
    }
}

type MessageFor<T> = <<T as Repository>::OptimisticState as OptimisticState>::Message;

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

fn save_to_file<T: Serialize>(file_path: &PathBuf, data: &T) -> anyhow::Result<()> {
    let buffer = bincode::serialize(&data)?;
    if let Some(path) = file_path.parent() {
        fs::create_dir_all(path)?;
    }
    let mut file = File::create(file_path)?;
    file.write_all(&buffer)?;
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
            last_ext_message_index: self.last_ext_message_index,
            ext_messages_mutex: self.ext_messages_mutex.clone(),
            blocks_cache: self.blocks_cache.clone(),
            saved_states: self.saved_states.clone(),
            finalized_optimistic_states: self.finalized_optimistic_states.clone(),
        }
    }
}

impl RepositoryImpl {
    // TODO: remove option from zerostate_path
    pub fn new(data_dir: PathBuf, zerostate_path: Option<PathBuf>, cache_size: usize) -> Self {
        if let Err(err) = fs::create_dir_all(data_dir.clone()) {
            tracing::error!("Failed to create data dir {:?}: {}", data_dir, err);
        }
        let db_path = data_dir.join(DB_FILE);
        let conn = SqliteHelper::create_connection_ro(db_path.clone())
            .unwrap_or_else(|_| panic!("Failed to open {db_path:?}"));

        let arch_path = data_dir.join(DB_ARCHIVE_FILE);
        let archive = SqliteHelper::create_connection_ro(arch_path)
            .expect("Failed to open an archive DB {arch_path}");

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

        let mut finalized_optimistic_states = HashMap::new();
        let mut metadatas = HashMap::new();
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
                metadatas.insert(
                    *thread_id,
                    Arc::new(Mutex::new(Metadata::<BlockIdentifier, BlockSeqNo>::default())),
                );
            }
        };

        let mut repo_impl = Self {
            archive,
            data_dir: data_dir.clone(),
            zerostate_path,
            metadatas,
            last_ext_message_index: 0,
            ext_messages_mutex: Arc::new(Mutex::new(())),
            blocks_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(cache_size).unwrap(),
            ))),
            saved_states: Default::default(),
            finalized_optimistic_states,
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

        repo_impl.load_metadata().expect("Failed to load metadata");

        for (thread_id, metadata) in &repo_impl.metadatas {
            let metadata = metadata.lock();
            if metadata.last_finalized_block_id != BlockIdentifier::default() {
                tracing::trace!("load finalized state {:?}", metadata.last_finalized_block_id);
                let state = repo_impl
                    .get_optimistic_state(&metadata.last_finalized_block_id)
                    .expect("Failed to get last finalized state")
                    .expect("Failed to load last finalized state");
                repo_impl.finalized_optimistic_states.insert(*thread_id, state);
            }
        }

        // Node repo can contain old blocks which are useless and just consume space
        // and increase metadata size. But we must check that they were successfully stored in the
        // archive
        tracing::trace!("Start checking old blocks");
        for (thread_id, metadata) in &repo_impl.metadatas {
            let metadata = {
                let metadata = metadata.lock();
                metadata.clone()
            };

            let last_finalized_seq_no = metadata.last_finalized_block_seq_no;
            let sqlite_helper_config = json!({
                "data_dir": data_dir
            })
            .to_string();
            let sqlite_helper_raw = Arc::new(
                SqliteHelper::from_config(&sqlite_helper_config)
                    .expect("Failed to create archive DB helper"),
            );
            for (block_id, block_seq_no) in metadata.block_id_to_seq_no {
                if block_seq_no < last_finalized_seq_no {
                    let markers =
                        repo_impl.load_markers(&block_id).expect("Failed to load block markers");
                    if let Some(true) = markers.is_finalized {
                        if repo_impl
                            .is_block_present_in_archive(&block_id)
                            .expect("Failed to check block")
                        {
                            let _ = repo_impl.erase_block(&block_id, thread_id);
                        } else {
                            if let Ok(Some(block)) = repo_impl.get_block(&block_id) {
                                if let Ok(Some(state)) = repo_impl.get_optimistic_state(&block_id) {
                                    write_to_db(
                                        sqlite_helper_raw.clone(),
                                        block,
                                        state.shard_state.shard_state,
                                        state.shard_state.shard_state_cell,
                                    )
                                    .expect("Failed to store old finalized block in archive");
                                }
                            }
                            let _ = repo_impl.erase_block(&block_id, thread_id);
                        }
                    } else {
                        let _ = repo_impl.erase_block(&block_id, thread_id);
                    }
                }
            }
        }
        tracing::trace!("Finished checking old blocks");

        let msg_queue =
            repo_impl.load_ext_messages_queue().expect("Failed to load external messages queue");
        if msg_queue.queue.is_empty() {
            // TODO: this code worked for single thread, but needs check for multithreaded version
            for metadata in repo_impl.metadatas.values() {
                // If saved queue is empty need to get info from the last block
                let metadata = metadata.lock();
                let mut saved_blocks =
                    metadata.block_id_to_seq_no.clone().into_iter().collect::<Vec<_>>();
                drop(metadata);
                if saved_blocks.is_empty() {
                    repo_impl.last_ext_message_index = 0;
                } else {
                    saved_blocks.sort_by(|a, b| a.1.cmp(&b.1));
                    let markers = repo_impl
                        .load_markers(&saved_blocks.last().unwrap().0)
                        .expect("Failed to load block markers");
                    // Save greates last_processed_ext_messages index
                    if markers.last_processed_ext_message > repo_impl.last_ext_message_index {
                        repo_impl.last_ext_message_index = markers.last_processed_ext_message;
                    }
                }
            }
        } else {
            repo_impl.last_ext_message_index = msg_queue.queue.last().unwrap().index;
        }
        tracing::trace!("repository init finished");
        repo_impl
    }

    fn get_path(&self, path: &str, oid: String) -> PathBuf {
        PathBuf::from(format!("{}/{}/{}", self.data_dir.clone().to_str().unwrap(), path, oid))
    }

    fn save<T: Serialize>(&self, path: &str, oid: OID, data: &T) -> anyhow::Result<()> {
        let oid = match oid {
            OID::ID(value) => value,
            OID::SingleRow => DEFAULT_OID.to_owned(),
        };
        save_to_file(&self.get_path(path, oid), data)?;
        Ok(())
    }

    fn get_metadata_for_thread(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<&Arc<Mutex<Metadata<BlockIdentifier, BlockSeqNo>>>> {
        self.metadatas
            .get(thread_id)
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

    fn remove(&self, path: &str, oid: OID) -> anyhow::Result<()> {
        let oid = match oid {
            OID::ID(value) => value,
            OID::SingleRow => DEFAULT_OID.to_owned(),
        };
        let _ = std::fs::remove_file(self.get_path(path, oid));
        Ok(())
    }

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

    fn get_metadata_path(&self) -> &str {
        "metadata"
    }

    fn get_attestations_path(&self) -> &str {
        "attestations"
    }

    fn get_ext_messages_queue_path(&self) -> &str {
        "ext_messages"
    }

    fn load_metadata(&mut self) -> anyhow::Result<()> {
        let path = self.get_metadata_path();
        if let Some(metadatas) = self.load::<HashMap<
            ThreadIdentifier,
            Metadata<BlockIdentifier, BlockSeqNo>,
        >>(path, OID::SingleRow)?
        {
            self.metadatas = metadatas
                .into_iter()
                .map(|(thread, metadata)| (thread, Arc::new(Mutex::new(metadata))))
                .collect();
        }
        Ok(())
    }

    fn save_metadata(&self) -> anyhow::Result<()> {
        let path = self.get_metadata_path();
        let metadata: HashMap<ThreadIdentifier, Metadata<BlockIdentifier, BlockSeqNo>> = self
            .metadatas
            .clone()
            .into_iter()
            .map(|(thread, metadata)| (thread, metadata.lock().deref().clone()))
            .collect();
        self.save(path, OID::SingleRow, &metadata)
    }

    fn erase_markers(&self, block_id: &BlockIdentifier) -> anyhow::Result<()> {
        let _ = std::fs::remove_file(self.get_path("markers", format!("{block_id:?}")));
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
        let path = self.get_path("markers", format!("{block_id:?}"));
        let markers_clone = markers.clone();
        let _ = thread::Builder::new().name("saving block".to_string()).spawn(move || {
            let _res = save_to_file(&path, &markers_clone);
        })?;
        let mut cache = self.blocks_cache.lock();
        if let Some(entry) = cache.get_mut(block_id) {
            entry.1 = markers.clone();
        }
        Ok(())
    }

    fn save_ext_messages_queue(&self, queue: ExtMessages<MessageFor<Self>>) -> anyhow::Result<()> {
        let lock = self.ext_messages_mutex.lock();
        let path = self.get_ext_messages_queue_path();
        self.save(path, OID::SingleRow, &queue)?;
        drop(lock);
        Ok(())
    }

    pub(crate) fn load_ext_messages_queue(&self) -> anyhow::Result<ExtMessages<MessageFor<Self>>> {
        let lock = self.ext_messages_mutex.lock();
        let path = self.get_ext_messages_queue_path();
        let queue: ExtMessages<WrappedMessage> =
            self.load(path, OID::SingleRow).map(|res| res.unwrap_or_default())?;
        drop(lock);
        Ok(queue)
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
        let _ = thread::Builder::new().name("saving block".to_string()).spawn(move || {
            let _res = save_to_file(&path, &block_clone);
        })?;

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

    fn erase_candidate_block(&self, block_id: &BlockIdentifier) -> anyhow::Result<()> {
        let path = self.get_path("blocks", format!("{block_id:?}"));
        let _ = std::fs::remove_file(path);
        let mut cache = self.blocks_cache.lock();
        let _ = cache.pop_entry(block_id);
        Ok(())
    }

    fn load_archived_block_by_seq_no(
        &self,
        seq_no: &BlockSeqNo,
    ) -> anyhow::Result<Vec<<RepositoryImpl as Repository>::CandidateBlock>> {
        tracing::trace!("Loading an archived block (seq_no: {seq_no:?})...");
        let sql = r"SELECT aggregated_signature, signature_occurrences, data
            FROM blocks WHERE seq_no=?1";
        let mut stmt = self.archive.prepare(sql)?;
        let mut rows = stmt.query(params![seq_no.to_string()])?;
        let mut envelopes = vec![];

        while let Some(row) = rows.next()? {
            let mut buffer: Vec<u8> = row.get(0)?;
            let aggregated_signature: <GoshBLS as BLSSignatureScheme>::Signature =
                bincode::deserialize(&buffer)?;

            buffer = row.get(1)?;
            let signature_occurrences: HashMap<u16, u16> = bincode::deserialize(&buffer)?;

            buffer = row.get(2)?;
            let block_data: AckiNackiBlock<GoshBLS> = bincode::deserialize(&buffer)?;

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
                let block_data: AckiNackiBlock<GoshBLS> = bincode::deserialize(&buffer)?;

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

    fn clear_optimistic_states(&mut self, thread_id: &ThreadIdentifier) -> anyhow::Result<()> {
        let mut all_saved_states = self.saved_states.lock();
        let saved_states = all_saved_states.entry(*thread_id).or_default();
        let (_, last_finalized_seq_no) = self.select_thread_last_finalized_block(thread_id)?;
        let mut keys_to_remove = vec![];
        for (block_seq_no, block_id) in saved_states.iter() {
            if *block_seq_no > last_finalized_seq_no {
                break;
            }
            if !self.is_optimistic_state_present(block_id) {
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
        for k in keys_to_remove {
            let block_id = saved_states.remove(&k).unwrap();
            tracing::trace!("Clear optimistic state for {k} {block_id:?}");
            let optimistic_state_path = self.get_optimistic_state_path();
            self.remove(optimistic_state_path, OID::ID(block_id.to_string()))?;
        }
        tracing::trace!("repo saved_states={:?}", saved_states);
        Ok(())
    }

    fn try_load_state_from_archive(
        &self,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<Option<<Self as Repository>::OptimisticState>> {
        tracing::trace!("try_load_state_from_archive: {block_id:?}");
        // Load last block
        let block = self.get_block_from_repo_or_archive(block_id)?;
        let thread_id = block.data().get_common_section().thread_id;
        // Check that state for this block can be built
        let block_seq_no = block.data().seq_no();
        let mut saved_states =
            { self.saved_states.lock().get(&thread_id).cloned().unwrap_or_default() };
        // TODO: fix for absent finalized state
        let finalized_state = self
            .finalized_optimistic_states
            .get(&thread_id)
            .expect("Failed to load finalized state for thread")
            .clone();
        saved_states.insert(finalized_state.block_seq_no, finalized_state.block_id.clone());
        tracing::trace!("try_load_state_from_archive: saved_state: {:?}", saved_states);
        let mut max_less_or_equal_key = None;
        for key in saved_states.keys() {
            if *key > block_seq_no {
                break;
            }
            max_less_or_equal_key = Some(*key);
        }
        if max_less_or_equal_key.is_none()
            && block_seq_no > BlockSeqNo::from(MAX_BLOCK_SEQ_NO_THAT_CAN_BE_BUILT_FROM_ZEROSTATE)
        {
            anyhow::bail!(
                "Requested block is too old to be built from saved states and too late to be built from zerostate"
            );
        }

        let mut blocks = vec![];
        let mut block_id = block_id.clone();

        let state_id = loop {
            let block = self.get_block_from_repo_or_archive(&block_id)?;
            tracing::trace!(
                "try_load_state_from_archive: loaded block: {:?} {block_id:?}",
                block.data().seq_no()
            );
            blocks.push(block.data().clone());
            if blocks.len() > MAX_BLOCK_CNT_THAT_CAN_BE_LOADED_TO_PREPARE_STATE {
                anyhow::bail!("Failed to load optimistic state: no appropriate state was found during depth search");
            }

            if saved_states.values().any(|id| id == &block.data().parent()) {
                break block.data().parent();
            }
            block_id = block.data().parent();
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
        } else if state_id == finalized_state.block_id {
            finalized_state
        } else {
            self.get_optimistic_state(&state_id)?
                .ok_or(anyhow::format_err!("Optimistic state must be present"))?
        };
        tracing::trace!("try_load_state_from_archive start applying blocks");
        let mut last_processed_message_index = state.last_processed_external_message_index;
        let mut state_cell = state.get_shard_state_as_cell();
        let last_block = blocks.last().unwrap().clone();
        let final_block_info = prepare_prev_block_info(&last_block);
        let final_block_id = last_block.identifier();
        let final_block_seq_no = last_block.seq_no();
        for block in blocks {
            tracing::trace!("try_load_state_from_archive apply block {:?}", block.identifier());
            let state_update = block
                .tvm_block()
                .read_state_update()
                .map_err(|e| anyhow::format_err!("Failed to read block state update: {e}"))?;
            state_cell = state_update
                .apply_for(&state_cell)
                .map_err(|e| anyhow::format_err!("Failed to apply state update: {e}"))?;
            last_processed_message_index += block.processed_ext_messages_cnt() as u32;
        }

        // TODO: single thread implementation. fix.
        let threads_table = ThreadsTable::default();

        tracing::trace!("try_load_state_from_archive return state");
        Ok(Some(
            OptimisticStateImpl::builder()
                .block_seq_no(final_block_seq_no)
                .block_id(final_block_id)
                .shard_state(state_cell)
                .last_processed_external_message_index(last_processed_message_index)
                .threads_table(threads_table)
                .thread_id(thread_id)
                .dapp_id_table(HashMap::new())
                .block_info(final_block_info)
                .build(),
        ))
    }
}

impl Repository for RepositoryImpl {
    type Attestation = Envelope<GoshBLS, AttestationData>;
    type BLS = GoshBLS;
    type CandidateBlock = Envelope<GoshBLS, AckiNackiBlock<Self::BLS>>;
    type EnvelopeSignerIndex = u16;
    type NodeIdentifier = NodeIdentifier;
    type OptimisticState = OptimisticStateImpl;
    type StateSnapshot = Vec<u8>;

    fn dump_sent_attestations(
        &self,
        data: HashMap<ThreadIdentifier, Vec<(BlockSeqNo, Self::Attestation)>>,
    ) -> anyhow::Result<()> {
        let path = self.get_attestations_path();
        let path = self.get_path(path, DEFAULT_OID.to_owned());
        let _ =
            thread::Builder::new().name("saving attestations".to_string()).spawn(move || {
                // let bytes = bincode::serialize(&data).expect("Failed to serialize receipts");
                let res = save_to_file(&path, &data);
                tracing::trace!("save attestations result: {res:?}");
            })?;
        Ok(())
    }

    fn load_sent_attestations(
        &self,
    ) -> anyhow::Result<HashMap<ThreadIdentifier, Vec<(BlockSeqNo, Self::Attestation)>>> {
        let path = self.get_attestations_path();
        let path = self.get_path(path, DEFAULT_OID.to_owned());
        let res = load_from_file(&path)?.unwrap_or_default();
        Ok(res)
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
            anyhow::bail!("get_block_from_repo_or_archive: failed to load block: {block_id:?}");
        })
    }

    fn get_block_from_repo_or_archive_by_seq_no(
        &self,
        block_seq_no: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<<Self as Repository>::CandidateBlock>> {
        let ids = self.get_block_id_by_seq_no(block_seq_no, thread_id)?;
        if !ids.is_empty() {
            let mut res = vec![];
            for id in ids {
                res.push(self.get_block(&id)?.expect("Block must be present"))
            }
            Ok(res)
        } else {
            self.load_archived_block_by_seq_no(block_seq_no)
        }
    }

    fn list_blocks_with_seq_no(
        &self,
        seq_no: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<Self::CandidateBlock>> {
        let metadata = self.get_metadata_for_thread(thread_id)?.lock();
        let mut result = vec![];
        for (block_id, block_seq_no) in metadata.block_id_to_seq_no.iter() {
            if block_seq_no == seq_no {
                if let Some(block) = self.get_block(block_id)? {
                    result.push(block);
                }
            }
        }
        Ok(result)
    }

    fn select_thread_last_finalized_block(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<(BlockIdentifier, BlockSeqNo)> {
        // TODO: self.finalized_states can be used here
        let mut metadata = self.get_metadata_for_thread(thread_id)?.lock();
        let mut cursor_id = metadata.last_finalized_block_id.clone();
        let mut cursor_seq_no = metadata.last_finalized_block_seq_no;
        let mut is_cursor_moved = true;
        let mut is_metadata_update_needed = false;

        while is_cursor_moved {
            is_cursor_moved = false;
            let next_seq_no = calc_next_seq_no(cursor_seq_no);

            for (block_id, block_seq_no) in metadata.block_id_to_seq_no.iter() {
                if next_seq_no == *block_seq_no {
                    if let Ok(Some(true)) = self.is_block_finalized(block_id) {
                        is_metadata_update_needed = true;
                        is_cursor_moved = true;
                        cursor_seq_no = *block_seq_no;
                        cursor_id = block_id.clone();
                        break;
                    }
                }
            }
        }
        if is_metadata_update_needed {
            metadata.last_finalized_block_seq_no = cursor_seq_no;
            metadata.last_finalized_block_id = cursor_id.clone();
            drop(metadata);
            self.save_metadata()?;
        }
        Ok((cursor_id, cursor_seq_no))
    }

    fn select_thread_last_main_candidate_block(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<(BlockIdentifier, BlockSeqNo)> {
        let mut metadata = self.get_metadata_for_thread(thread_id)?.lock();
        let mut cursor_id = metadata.last_main_candidate_block_id.clone();
        let mut cursor_seq_no = metadata.last_main_candidate_block_seq_no;
        let mut is_cursor_moved = true;
        let mut is_metadata_update_needed = false;
        while is_cursor_moved {
            is_cursor_moved = false;
            let next_seq_no = calc_next_seq_no(cursor_seq_no);
            for (block_id, block_seq_no) in metadata.block_id_to_seq_no.iter() {
                if next_seq_no == *block_seq_no {
                    if let Ok(Some(true)) = self.is_block_accepted_as_main_candidate(block_id) {
                        is_metadata_update_needed = true;
                        is_cursor_moved = true;
                        cursor_seq_no = *block_seq_no;
                        cursor_id = block_id.clone();
                        break;
                    }
                }
            }
        }
        if is_metadata_update_needed {
            metadata.last_main_candidate_block_seq_no = cursor_seq_no;
            metadata.last_main_candidate_block_id = cursor_id.clone();
            drop(metadata);
            self.save_metadata()?;
        }
        tracing::info!(
            "select_thread_last_main_candidate_block: block_seq_no: {}, id: {}",
            cursor_seq_no,
            cursor_id
        );
        Ok((cursor_id, cursor_seq_no))
    }

    fn mark_block_as_accepted_as_main_candidate(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        let mut markers = self.load_markers(block_id)?;
        markers.is_main_candidate = Some(true);
        self.save_markers(block_id, &markers)?;
        let mut metadata = self.get_metadata_for_thread(thread_id)?.lock();
        if metadata.block_id_to_seq_no.contains_key(block_id) {
            let block_seq_no = metadata.block_id_to_seq_no.get(block_id).unwrap();
            if *block_seq_no > metadata.last_main_candidate_block_seq_no {
                metadata.last_main_candidate_block_seq_no = *block_seq_no;
                metadata.last_main_candidate_block_id = block_id.clone();
                drop(metadata);
                self.save_metadata()?;
            }
        }
        Ok(())
    }

    fn is_block_accepted_as_main_candidate(
        &self,
        block_id: &BlockIdentifier,
    ) -> anyhow::Result<Option<bool>> {
        let markers = self.load_markers(block_id).unwrap_or_default();
        Ok(markers.is_main_candidate)
    }

    fn mark_block_as_finalized(
        &mut self,
        block: &<Self as Repository>::CandidateBlock,
    ) -> anyhow::Result<()> {
        let block_id = block.data().identifier();
        let thread_id = block.data().get_common_section().thread_id;
        let mut metadata = self.get_metadata_for_thread(&thread_id)?.lock();
        let block_seq_no = *metadata.block_id_to_seq_no.get(&block_id).unwrap();
        let mut markers = self.load_markers(&block_id)?;
        markers.is_finalized = Some(true);
        self.save_markers(&block_id, &markers)?;

        metadata.stored_finalized_blocks.push((block_id.clone(), block_seq_no));
        if block_seq_no > metadata.last_finalized_block_seq_no {
            metadata.last_finalized_block_seq_no = block_seq_no;
            metadata.last_finalized_block_id = block_id.clone();
        }
        drop(metadata);
        self.save_metadata()?;

        if let Some(state_ref) = self.finalized_optimistic_states.get_mut(&thread_id) {
            // After sync we mark the incoming block as finalized and this check works
            if block_id != state_ref.block_id {
                state_ref.apply_block(block.data())?;
            }
        }
        Ok(())
    }

    fn is_block_finalized(&self, block_id: &BlockIdentifier) -> anyhow::Result<Option<bool>> {
        let markers = self.load_markers(block_id).unwrap_or_default();
        Ok(markers.is_finalized)
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
    ) -> anyhow::Result<Option<Self::OptimisticState>> {
        let zero_block_id = <BlockIdentifier>::default();
        if block_id == &zero_block_id {
            return Ok(None);
        }
        log::info!("RepositoryImpl: get_optimistic_state {:?}", block_id);
        if let Some((_, state)) =
            self.finalized_optimistic_states.iter().find(|(_, state)| state.block_id == *block_id)
        {
            return Ok(Some(state.clone()));
        }
        let path = self.get_optimistic_state_path();
        let state = match self.load::<Vec<u8>>(path, OID::ID(block_id.to_string()))? {
            Some(buffer) => {
                let state = Self::OptimisticState::deserialize_from_buf(&buffer, block_id.clone())?;
                Some(state)
            }
            None => self.try_load_state_from_archive(block_id)?,
        };
        let marker = self.load_markers(block_id)?;
        Ok(state.map(|mut state| {
            state.last_processed_external_message_index = marker.last_processed_ext_message;
            state
        }))
    }

    fn is_optimistic_state_present(&self, block_id: &BlockIdentifier) -> bool {
        let zero_block_id = <BlockIdentifier>::default();
        if block_id == &zero_block_id {
            match &self.zerostate_path {
                Some(path) => path.exists(),
                None => false,
            }
        } else if self
            .finalized_optimistic_states
            .iter()
            .any(|(_, state)| state.block_id == *block_id)
        {
            true
        } else {
            let path = self.get_optimistic_state_path();
            self.exists(path, OID::ID(block_id.to_string()))
        }
    }

    fn store_block<T: Into<Self::CandidateBlock>>(&self, block: T) -> anyhow::Result<()> {
        let candidate_block = block.into();
        let thread_id = candidate_block.data().get_common_section().thread_id;
        let mut metadata = self.get_metadata_for_thread(&thread_id)?.lock();
        metadata
            .block_id_to_seq_no
            .insert(candidate_block.data().identifier(), candidate_block.data().seq_no());
        drop(metadata);
        self.save_metadata()?;

        let parent_block_id = candidate_block.data().parent();

        let parent_marker = self.load_markers(&parent_block_id)?;
        let mut candidate_marker = self.load_markers(&candidate_block.data().identifier())?;
        if candidate_marker.last_processed_ext_message == 0 {
            candidate_marker.last_processed_ext_message = parent_marker.last_processed_ext_message
                + (candidate_block.data().processed_ext_messages_cnt() as u32);
            self.save_markers(&candidate_block.data().identifier(), &candidate_marker)?;
        }

        self.save_candidate_block(candidate_block, candidate_marker)?;
        // Get parent state
        // let parent_block_id = candidate_block.data().parent();
        // tracing::info!("RepositoryImpl: store_block: parent {:?}", parent_block_id);
        // if let Some(parent_state) = self.get_optimistic_state(&parent_block_id)? {
        // tracing::info!("RepositoryImpl: store_block: parent found");
        // let mut state = parent_state.clone();
        // todo!();
        // Update over the parent
        // save as is
        // } else {
        // We need to store future state in case parent received later
        // unimplemented!();
        // }
        Ok(())
    }

    fn erase_block_and_optimistic_state(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        if !self.is_block_present_in_archive(block_id).unwrap_or(false) {
            tracing::trace!("Block was not saved to archive, do not erase it");
            return Ok(());
        }
        tracing::trace!("erase_block: {:?}", block_id);
        let mut metadata = self.get_metadata_for_thread(thread_id)?.lock();
        assert_ne!(metadata.last_main_candidate_block_id, *block_id);
        assert_ne!(metadata.last_finalized_block_id, *block_id);
        metadata.block_id_to_seq_no.remove(block_id);
        metadata.stored_finalized_blocks.retain(|e| &e.0 != block_id);
        drop(metadata);
        self.save_metadata()?;

        self.erase_markers(block_id)?;

        // let optimistic_state_path = self.get_optimistic_state_path();
        // self.remove(optimistic_state_path, OID::ID(block_id.to_string()))?;
        self.erase_candidate_block(block_id)?;
        Ok(())
    }

    fn erase_block(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        tracing::trace!("erase_block: {:?}", block_id);
        let mut metadata = self.get_metadata_for_thread(thread_id)?.lock();
        assert_ne!(metadata.last_main_candidate_block_id, *block_id);
        assert_ne!(metadata.last_finalized_block_id, *block_id);
        metadata.block_id_to_seq_no.remove(block_id);
        metadata.stored_finalized_blocks.retain(|e| &e.0 != block_id);
        drop(metadata);
        self.save_metadata()?;

        self.erase_markers(block_id)?;
        self.erase_candidate_block(block_id)?;
        Ok(())
    }

    fn list_stored_thread_finalized_blocks(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<(BlockIdentifier, BlockSeqNo)>> {
        // Note: This implementation assumes that there is only one thread exists.
        Ok(self.get_metadata_for_thread(thread_id)?.lock().stored_finalized_blocks.clone())
    }

    fn delete_external_messages(&self, count: usize) -> anyhow::Result<()> {
        if count == 0 {
            return Ok(());
        }
        let mut queue = self.load_ext_messages_queue()?;
        queue.queue.drain(0..count);
        self.save_ext_messages_queue(queue)
    }

    fn add_external_message<T>(&mut self, messages: Vec<T>) -> anyhow::Result<()>
    where
        T: Into<<Self::OptimisticState as OptimisticState>::Message>,
    {
        let mut queue = self.load_ext_messages_queue()?;
        let cur_last_index = queue.queue.last().map(|el| el.index).unwrap_or(0);
        assert!(self.last_ext_message_index >= cur_last_index);
        for message in messages {
            self.last_ext_message_index += 1;
            let message = message.into();
            queue.queue.push(WrappedExtMessage::new(self.last_ext_message_index, message));
        }
        self.save_ext_messages_queue(queue)
    }

    fn clear_ext_messages_queue_by_time(&self) -> anyhow::Result<()> {
        let mut queue = self.load_ext_messages_queue()?;
        let mut i = 0;
        let now = Utc::now();
        while i < queue.queue.len() {
            if queue.queue[i].timestamp < now {
                break;
            }
            i += 1;
        }
        if i != 0 {
            queue.queue.drain(0..i);
            self.save_ext_messages_queue(queue)?;
        }
        Ok(())
    }

    fn clear_verification_markers(
        &self,
        excluded_starting_seq_no: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<()> {
        let metadata = self.get_metadata_for_thread(thread_id)?.lock();
        let mut cursor_seq_no = *excluded_starting_seq_no;
        let mut is_cursor_moved = true;
        while is_cursor_moved {
            is_cursor_moved = false;
            cursor_seq_no = calc_next_seq_no(cursor_seq_no);
            for (block_id, block_seq_no) in metadata.block_id_to_seq_no.iter() {
                if cursor_seq_no == *block_seq_no {
                    let mut markers = self.load_markers(block_id)?;
                    if !markers.is_verified {
                        break;
                    }
                    markers.is_verified = false;
                    self.save_markers(block_id, &markers)?;
                    is_cursor_moved = true;
                }
            }
            if !is_cursor_moved {
                break;
            }
        }
        Ok(())
    }

    fn mark_block_as_verified(&self, block_id: &BlockIdentifier) -> anyhow::Result<()> {
        let mut markers = self.load_markers(block_id)?;
        markers.is_verified = true;
        self.save_markers(block_id, &markers)
    }

    fn is_block_verified(&self, block_id: &BlockIdentifier) -> anyhow::Result<bool> {
        let markers = self.load_markers(block_id).unwrap_or_default();
        Ok(markers.is_verified)
    }

    fn take_state_snapshot(
        &self,
        block_id: &BlockIdentifier,
        block_producer_groups: HashMap<ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        block_keeper_set: HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>,
    ) -> anyhow::Result<Self::StateSnapshot> {
        let state = match self.get_optimistic_state(block_id)? {
            Some(mut state) => OptimisticState::serialize_into_buf(&mut state)?,
            None => {
                tracing::trace!("State missing: {block_id:?}");
                anyhow::bail!("State missing: {block_id:?}");
            }
        };
        bincode::serialize(&WrappedStateSnapshot {
            optimistic_state: state,
            producer_group: block_producer_groups,
            block_keeper_set,
        })
        .map_err(|e| anyhow::format_err!("Failed to serialize WrappedStateSnapshot: {e}"))
    }

    fn convert_state_data_to_snapshot(
        &self,
        serialized_state: Vec<u8>,
        block_producer_groups: HashMap<ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        block_keeper_set: HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>,
    ) -> anyhow::Result<Self::StateSnapshot> {
        bincode::serialize(&WrappedStateSnapshot {
            optimistic_state: serialized_state,
            producer_group: block_producer_groups,
            block_keeper_set,
        })
        .map_err(|e| anyhow::format_err!("Failed to serialize WrappedStateSnapshot: {e}"))
    }

    fn set_state_from_snapshot(
        &mut self,
        block_id: &BlockIdentifier,
        snapshot: Self::StateSnapshot,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<(
        HashMap<ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>,
    )> {
        tracing::debug!("set_state_from_snapshot()...");
        let wrapper_snapshot: WrappedStateSnapshot = bincode::deserialize(&snapshot)?;
        tracing::trace!("Set state from snapshot: {block_id:?}");
        let mut state = <Self as Repository>::OptimisticState::deserialize_from_buf(
            &wrapper_snapshot.optimistic_state,
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
        if let Some(finalized_seq_no) =
            self.finalized_optimistic_states.get(thread_id).map(|state| state.block_seq_no)
        {
            if seq_no > finalized_seq_no {
                let shard_state = state.get_shard_state();
                self.finalized_optimistic_states.insert(*thread_id, state);
                self.sync_accounts_from_state(shard_state)?;
            }
        } else {
            self.finalized_optimistic_states.insert(*thread_id, state);
        }
        let mut all_states = self.saved_states.lock();
        let saved_states = all_states.entry(*thread_id).or_default();
        saved_states.insert(seq_no, block_id.clone());

        Ok((wrapper_snapshot.producer_group, wrapper_snapshot.block_keeper_set))
    }

    fn sync_accounts_from_state(
        &mut self,
        shard_state: Arc<ShardStateUnsplit>,
    ) -> anyhow::Result<()> {
        tracing::debug!("syncing accounts from state...");
        let mut arch_accounts: Vec<ArchAccount> = vec![];
        let accounts = shard_state.read_accounts().map_err(|e| anyhow::format_err!("{e}"))?;
        accounts
            .iterate_with_keys(|_k, v| {
                let last_trans_hash = v.last_trans_hash().clone();
                let account = v.read_account().unwrap();
                match prepare_account_archive_struct(account.clone(), None, None, last_trans_hash) {
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

        let sqlite_helper_config = json!({ "data_dir": self.data_dir }).to_string();
        let sqlite_helper: Arc<dyn DocumentsDb> =
            Arc::new(SqliteHelper::from_config(&sqlite_helper_config)?);

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

    fn last_stored_block_by_seq_no(
        &self,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<BlockSeqNo> {
        let metadata = self.get_metadata_for_thread(thread_id)?.lock();
        let mut saved_seq_nos = metadata.block_id_to_seq_no.values().collect::<Vec<_>>();
        saved_seq_nos.sort();
        Ok(saved_seq_nos.last().map(|v| **v).unwrap_or_default())
    }

    fn store_optimistic<T: Into<Self::OptimisticState>>(&mut self, state: T) -> anyhow::Result<()> {
        let mut optimistic = state.into();
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
        let _ = thread::Builder::new().name("saving optimistic state".to_string()).spawn(
            move || {
                let start_save = std::time::Instant::now();
                let thread_id = optimistic.thread_id;
                let state_bytes = OptimisticState::serialize_into_buf(&mut optimistic)
                    .expect("Failed to serialize block");
                let res = save_to_file(&path, &state_bytes);
                tracing::trace!(
                    "save optimistic {block_id:?} result: {res:?} {}",
                    start_save.elapsed().as_millis()
                );
                {
                    let mut saved_states = saved_states_clone.lock();
                    saved_states
                        .entry(thread_id)
                        .or_default()
                        .insert(block_seq_no, block_id.clone());
                    tracing::trace!("repo saved_states={:?}", saved_states);
                }
            },
        )?;
        Ok(())
    }

    fn get_block_id_by_seq_no(
        &self,
        block_seq_no: &BlockSeqNo,
        thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<Vec<BlockIdentifier>> {
        Ok(self
            .get_metadata_for_thread(thread_id)?
            .lock()
            .block_id_to_seq_no
            .iter()
            .filter(|(_id, seq_no)| *seq_no == block_seq_no)
            .map(|(id, _seq_no)| id.to_owned())
            .collect())
    }

    fn get_latest_block_id_with_producer_group_change(
        &self,
        _thread_id: &ThreadIdentifier,
    ) -> anyhow::Result<BlockIdentifier> {
        Ok(BlockIdentifier::default())
    }

    fn is_candidate_block_can_be_applied(&self, block: &Self::CandidateBlock) -> bool {
        let mut parent_block_id = block.data().parent();
        let thread_id = block.data().get_common_section().thread_id;
        if let Some(finalized_block_seq_no) =
            self.finalized_optimistic_states.get(&thread_id).map(|state| state.block_seq_no)
        {
            while block.data().seq_no() > finalized_block_seq_no {
                if self.is_optimistic_state_present(&parent_block_id) {
                    return true;
                }
                if let Ok(parent_block) = self.get_block_from_repo_or_archive(&parent_block_id) {
                    parent_block_id = parent_block.data().parent();
                } else {
                    break;
                }
            }
        }
        false
    }

    fn list_finalized_states(
        &self,
    ) -> impl Iterator<Item = (&'_ ThreadIdentifier, &'_ Self::OptimisticState)> {
        self.finalized_optimistic_states.iter()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::Once;

    use super::RepositoryImpl;
    use super::OID;

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

        let repository = RepositoryImpl::new(
            PathBuf::from("./tests-data/test_save_load"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
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
        let _repository = RepositoryImpl::new(
            PathBuf::from("/home/user/GOSH/acki-nacki/server_data/node1/"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
        );
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_exists() -> anyhow::Result<()> {
        START.call_once(init_tests);

        let repository = RepositoryImpl::new(
            PathBuf::from("./tests-data/test_exists"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
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

        let repository = RepositoryImpl::new(
            PathBuf::from("./tests-data/test_remove"),
            Some(PathBuf::from(ZEROSTATE)),
            1,
        );

        let path = "optimistic_state";
        let oid = OID::ID(String::from("200"));
        let data: Vec<u8> = vec![0xde, 0xad, 0xbe, 0xef];
        repository.save(path, oid.clone(), &data)?;
        assert!(repository.exists(path, oid.clone()));

        repository.remove(path, oid.clone())?;
        assert!(!repository.exists(path, oid.clone()));
        Ok(())
    }
}
