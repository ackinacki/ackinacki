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
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;

use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use lru::LruCache;
use parking_lot::Mutex;
use rusqlite::params;
use rusqlite::Connection;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;

use crate::block::keeper::process::prepare_prev_block_info;
use crate::block::Block;
use crate::block::BlockSeqNo;
use crate::block::WrappedBlock;
use crate::block::WrappedUInt256;
use crate::block_keeper_system::BlockKeeperSet;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::database::documents_db::SerializedItem;
use crate::database::sqlite_helper::SqliteHelper;
use crate::database::write_to_db;
use crate::message::WrappedMessage;
use crate::node::associated_types::AttestationData;
use crate::node::NodeIdentifier;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::BlockIdentifierFor;
use crate::repository::BlockSeqNoFor;
use crate::repository::Repository;
use crate::zerostate::ZeroState;

const DEFAULT_OID: &str = "00000000000000000000";
const FAILED_BLOCK_DIR: &str = "fail_dumps";
const REQUIRED_SQLITE_SCHEMA_VERSION: u32 = 2;
const _REQUIRED_SQLITE_ARC_SCHEMA_VERSION: u32 = 2;
const DB_FILE: &str = "node.db";
const DB_ARCHIVE_FILE: &str = "node-archive.db";
pub const EXT_MESSAGE_STORE_TIMEOUT_SECONDS: i64 = 60;
const MAX_BLOCK_SEQ_NO_THAT_CAN_BE_BUILT_FROM_ZEROSTATE: u32 = 200;

pub struct RepositoryImpl {
    archive: Connection,
    data_dir: PathBuf,
    zerostate_path: Option<PathBuf>,
    metadata: Arc<Mutex<MetadataFor<Self>>>,
    last_ext_message_index: u32,
    ext_messages_mutex: Arc<Mutex<()>>,
    blocks_cache:
        Arc<Mutex<LruCache<WrappedUInt256, (Envelope<GoshBLS, WrappedBlock>, BlockMarkers)>>>,
    saved_states: Arc<Mutex<BTreeMap<u32, WrappedUInt256>>>,
    finalized_optimistic_state: OptimisticStateImpl,
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
    pub producer_group: HashMap<u64, Vec<NodeIdentifier>>,
    pub block_keeper_set: BlockKeeperSet,
}

impl<TMessage> Default for ExtMessages<TMessage>
where
    TMessage: Clone,
{
    fn default() -> Self {
        Self { queue: vec![] }
    }
}

type MetadataFor<T> = Metadata<BlockIdentifierFor<T>, BlockSeqNoFor<T>>;
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
    fn clone(&self) -> Self {
        let db_path = self.data_dir.join(DB_ARCHIVE_FILE);
        let arc_conn =
            SqliteHelper::create_connection_ro(db_path).expect("Failed to open {db_path}");

        Self {
            archive: arc_conn,
            data_dir: self.data_dir.clone(),
            zerostate_path: self.zerostate_path.clone(),
            metadata: self.metadata.clone(),
            last_ext_message_index: self.last_ext_message_index,
            ext_messages_mutex: self.ext_messages_mutex.clone(),
            blocks_cache: self.blocks_cache.clone(),
            saved_states: self.saved_states.clone(),
            finalized_optimistic_state: self.finalized_optimistic_state.clone(),
        }
    }
}

impl RepositoryImpl {
    pub fn new(data_dir: PathBuf, zerostate_path: Option<PathBuf>, cache_size: usize) -> Self {
        if let Err(err) = fs::create_dir_all(data_dir.clone()) {
            log::error!("Failed to create data dir {:?}: {}", data_dir, err);
        }
        let db_path = data_dir.join(DB_FILE);
        let conn = SqliteHelper::create_connection(db_path).expect("Failed to open {db_path}");

        let arch_path = data_dir.join(DB_ARCHIVE_FILE);
        let archive = SqliteHelper::create_connection_ro(arch_path)
            .expect("Failed to open an archive DB {arch_path}");

        let schema_version =
            conn.pragma_query_value(None, "user_version", |row| row.get(0)).unwrap_or(0);
        match schema_version.cmp(&REQUIRED_SQLITE_SCHEMA_VERSION) {
            Ordering::Less => {
                log::trace!(
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
                log::trace!(
                    r"The schema of the database version is higher than that required by
                    this instance of the Acki-Nacki node. Please update the app version.",
                );
                std::process::exit(3);
            }
            _ => {}
        }

        let mut finalized_optimistic_state = OptimisticStateImpl::zero();
        if let Some(path) = &zerostate_path {
            let zerostate = ZeroState::load_from_file(path).expect("Failed to load zerostate");
            finalized_optimistic_state.shard_state = Some(Arc::new(zerostate.shard_state));
        };

        let mut repo_impl = Self {
            archive,
            data_dir: data_dir.clone(),
            zerostate_path,
            metadata: Arc::new(Mutex::new(MetadataFor::<Self>::default())),
            last_ext_message_index: 0,
            ext_messages_mutex: Arc::new(Mutex::new(())),
            blocks_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(cache_size).unwrap(),
            ))),
            saved_states: Arc::new(Mutex::new(BTreeMap::new())),
            finalized_optimistic_state,
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
                        if let Ok(block_id) = WrappedUInt256::from_str(id_str) {
                            tracing::trace!(
                                "RepositoryImpl::new reading optimistic state: {block_id:?}"
                            );
                            if let Some(buffer) = load_from_file::<Vec<u8>>(&path).unwrap() {
                                let state = <Self as Repository>::OptimisticState::deserialize(
                                    &buffer,
                                    block_id.clone(),
                                )
                                .unwrap();
                                let seq_no = state.get_block_info().prev1().unwrap().seq_no;
                                {
                                    let mut saved_states = repo_impl.saved_states.lock();
                                    saved_states.insert(seq_no, block_id);
                                }
                            };
                        }
                    }
                }
            }
        }
        {
            tracing::trace!("Repo saved states: {:?}", repo_impl.saved_states.lock());
        }

        repo_impl.load_metadata().expect("Failed to load metadata");

        {
            let metadata = repo_impl.metadata.lock();
            if metadata.last_finalized_block_id != BlockIdentifierFor::<Self>::default() {
                tracing::trace!("load finalized state {:?}", metadata.last_finalized_block_id);
                repo_impl.finalized_optimistic_state = repo_impl
                    .get_optimistic_state(&metadata.last_finalized_block_id)
                    .expect("Failed to get last finalized state")
                    .expect("Failed to load last finalized state");
            }
        }
        // Node repo can contain old blocks which are useless and just consume space
        // and increase metadata size. But we must check that they were successfully stored in the
        // archive
        tracing::trace!("Start checking old blocks");
        {
            let metadata = {
                let metadata = repo_impl.metadata.lock();
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
                            let _ = repo_impl.erase_block(&block_id);
                        } else {
                            let block = repo_impl
                                .get_block(&block_id)
                                .expect("Failed to load block")
                                .expect("Failed to load block");
                            let state = repo_impl
                                .get_optimistic_state(&block_id)
                                .expect("Failed to load optimistic state")
                                .expect("Failed to load optimistic state");
                            write_to_db(
                                sqlite_helper_raw.clone(),
                                block,
                                state.shard_state,
                                state.shard_state_cell,
                            )
                            .expect("Failed to store old finalized block in archive");
                        }
                    } else {
                        let _ = repo_impl.erase_block(&block_id);
                    }
                }
            }
        }
        tracing::trace!("Finished checking old blocks");

        let msg_queue =
            repo_impl.load_ext_messages_queue().expect("Failed to load external messages queue");
        if msg_queue.queue.is_empty() {
            // If saved queue is empty need to get info from the last block
            let metadata = repo_impl.metadata.lock();
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
                repo_impl.last_ext_message_index = markers.last_processed_ext_message;
            }
        } else {
            repo_impl.last_ext_message_index = msg_queue.queue.last().unwrap().index;
        }
        repo_impl
    }

    fn get_path(&self, path: &str, oid: String) -> PathBuf {
        PathBuf::from(format!("{}/{}/{}", self.data_dir.clone().to_str().unwrap(), path, oid))
    }

    fn save<T: Serialize>(&self, path: &str, oid: OID, data: &T) -> anyhow::Result<()> {
        let start = std::time::Instant::now();
        let oid = match oid {
            OID::ID(value) => value,
            OID::SingleRow => DEFAULT_OID.to_owned(),
        };
        save_to_file(&self.get_path(path, oid), data)?;
        tracing::trace!("repository save time: {}", start.elapsed().as_millis());
        Ok(())
    }

    fn load<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        oid: OID,
    ) -> anyhow::Result<Option<T>> {
        let start = std::time::Instant::now();
        let oid = match oid {
            OID::ID(value) => value,
            OID::SingleRow => DEFAULT_OID.to_owned(),
        };
        let oid_file = self.get_path(path, oid);
        let data = load_from_file(&oid_file)
            .unwrap_or_else(|_| panic!("Failed to load file: {}", oid_file.display()));
        tracing::trace!("repository load time: {}", start.elapsed().as_millis());
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
        let start = std::time::Instant::now();
        let oid = match oid {
            OID::ID(value) => value,
            OID::SingleRow => DEFAULT_OID.to_owned(),
        };
        let res = std::path::Path::new(&self.get_path(path, oid)).exists();
        tracing::trace!("repository load time: {}", start.elapsed().as_millis());
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

    fn load_metadata(&self) -> anyhow::Result<()> {
        tracing::trace!("Loading metadata from the repo...");
        let path = self.get_metadata_path();
        if let Some(metadata) = self.load::<MetadataFor<Self>>(path, OID::SingleRow)? {
            tracing::trace!("Metadata was successfully loaded: {metadata:?}");
            let mut inner_ref = self.metadata.lock();
            *inner_ref = metadata;
        }
        Ok(())
    }

    fn save_metadata(&self, metadata: &MetadataFor<Self>) -> anyhow::Result<()> {
        tracing::trace!("Saving metadata into the repo...");
        let path = self.get_metadata_path();
        self.save(path, OID::SingleRow, metadata)
    }

    fn erase_markers(&self, block_id: &BlockIdentifierFor<Self>) -> anyhow::Result<()> {
        tracing::trace!("Removing markers (block_id: {block_id}) from the repo...");
        let _ = std::fs::remove_file(self.get_path("markers", format!("{block_id:?}")));
        Ok(())
    }

    fn load_markers(&self, block_id: &BlockIdentifierFor<Self>) -> anyhow::Result<BlockMarkers> {
        let start = std::time::Instant::now();

        let mut cache = self.blocks_cache.lock();
        let res = if let Some((_, markers)) = cache.get(block_id) {
            tracing::trace!("Loaded markers from cache: {block_id:?}");
            markers.clone()
        } else {
            let path = self.get_path("markers", format!("{block_id:?}"));
            load_from_file(&path)?.unwrap_or_default()
        };

        tracing::trace!("repository load time: {}", start.elapsed().as_millis());
        Ok(res)
    }

    fn save_markers(
        &self,
        block_id: &BlockIdentifierFor<Self>,
        markers: &BlockMarkers,
    ) -> anyhow::Result<()> {
        let start = std::time::Instant::now();
        let path = self.get_path("markers", format!("{block_id:?}"));
        let markers_clone = markers.clone();
        let _ = thread::Builder::new().name("saving block".to_string()).spawn(move || {
            let res = save_to_file(&path, &markers_clone);
            tracing::trace!("save markers result: {res:?}");
        })?;
        let mut cache = self.blocks_cache.lock();
        if let Some(entry) = cache.get_mut(block_id) {
            entry.1 = markers.clone();
        }
        tracing::trace!("repository save time: {}", start.elapsed().as_millis());
        Ok(())
    }

    fn save_ext_messages_queue(&self, queue: ExtMessages<MessageFor<Self>>) -> anyhow::Result<()> {
        tracing::trace!("Saving ext msg queue[{}] into the repo...", queue.queue.len());
        let lock = self.ext_messages_mutex.lock();
        let path = self.get_ext_messages_queue_path();
        self.save(path, OID::SingleRow, &queue)?;
        tracing::trace!("Saving ext msg queue into the repo finished");
        drop(lock);
        Ok(())
    }

    pub(crate) fn load_ext_messages_queue(&self) -> anyhow::Result<ExtMessages<MessageFor<Self>>> {
        tracing::trace!("Loading ext msg queue from repo...");
        let lock = self.ext_messages_mutex.lock();
        let path = self.get_ext_messages_queue_path();
        let queue: ExtMessages<WrappedMessage> =
            self.load(path, OID::SingleRow).map(|res| res.unwrap_or_default())?;
        tracing::trace!("Loaded ext msg queue[{}]", queue.queue.len());
        drop(lock);
        Ok(queue)
    }

    fn save_accounts(
        &self,
        block_id: WrappedUInt256,
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
        let start = std::time::Instant::now();
        let block_id = block.data().identifier();
        let path = self.get_path("blocks", format!("{block_id:?}"));

        let block_clone = block.clone();
        let _ = thread::Builder::new().name("saving block".to_string()).spawn(move || {
            let res = save_to_file(&path, &block_clone);
            tracing::trace!("save block result: {res:?}");
        })?;

        let mut cache = self.blocks_cache.lock();
        cache.push(block.data().identifier(), (block, candidate_marker));
        tracing::trace!("repository save time: {}", start.elapsed().as_millis());
        Ok(())
    }

    fn load_candidate_block(
        &self,
        identifier: &BlockIdentifierFor<RepositoryImpl>,
    ) -> anyhow::Result<Option<<RepositoryImpl as Repository>::CandidateBlock>> {
        let start = std::time::Instant::now();

        let mut cache = self.blocks_cache.lock();
        let envelope = if let Some((envelope, _)) = cache.get(identifier) {
            tracing::trace!("Loaded candidate block from cache: {identifier:?}");
            Some(envelope.clone())
        } else {
            let path = self.get_path("blocks", format!("{identifier:?}"));
            load_from_file(&path)?
        };

        tracing::trace!("repository load time: {}", start.elapsed().as_millis());
        Ok(envelope)
    }

    fn erase_candidate_block(&self, block_id: &BlockIdentifierFor<Self>) -> anyhow::Result<()> {
        let path = self.get_path("blocks", format!("{block_id:?}"));
        let _ = std::fs::remove_file(path);
        let mut cache = self.blocks_cache.lock();
        let _ = cache.pop_entry(block_id);
        Ok(())
    }

    fn load_archived_block_by_seq_no(
        &self,
        seq_no: &BlockSeqNoFor<RepositoryImpl>,
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
            let block_data: WrappedBlock = bincode::deserialize(&buffer)?;

            let envelope = <RepositoryImpl as Repository>::CandidateBlock::create(
                aggregated_signature,
                signature_occurrences,
                block_data,
            );
            envelopes.push(envelope);
        }
        tracing::trace!("load_archived_block_by_seq_no: seq_no: {seq_no}, {envelopes:?}");
        Ok(envelopes)
    }

    fn is_block_present_in_archive(
        &self,
        identifier: &BlockIdentifierFor<RepositoryImpl>,
    ) -> anyhow::Result<bool> {
        tracing::trace!("Check that block presents in archive (block_id: {identifier})");
        let sql = r"SELECT aggregated_signature, signature_occurrences, data, id
            FROM blocks WHERE id=?1";
        let mut stmt = self.archive.prepare(sql)?;
        let mut rows = stmt.query(params![identifier.to_string()])?;
        let res = rows.next()?.is_some();
        tracing::trace!("Does block present in archive: {identifier:?} {res}");
        Ok(res)
    }

    fn load_archived_block(
        &self,
        identifier: &Option<BlockIdentifierFor<RepositoryImpl>>,
    ) -> anyhow::Result<Option<<RepositoryImpl as Repository>::CandidateBlock>> {
        tracing::trace!("Loading an archived block (block_id: {identifier:?})...");
        let add_clause = if let Some(block_id) = identifier {
            format!(" WHERE id={:?}", block_id.to_string())
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
                let block_data: WrappedBlock = bincode::deserialize(&buffer)?;

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

    fn clear_optimistic_states(&mut self) -> anyhow::Result<()> {
        let mut saved_states = self.saved_states.lock();
        let (_, last_finalized_seq_no) = self.select_thread_last_finalized_block(
            &<Self as Repository>::ThreadIdentifier::default(),
        )?;
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
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<<Self as Repository>::OptimisticState>> {
        tracing::trace!("try_load_state_from_archive: {block_id:?}");
        // Load last block
        let block = self.get_block_from_repo_or_archive(block_id)?;
        // Check that state for this block can be built
        let block_seq_no = block.data().seq_no();
        let saved_states = { self.saved_states.lock().clone() };
        let mut max_less_or_equal_key = None;
        for key in saved_states.keys() {
            if *key > block_seq_no {
                break;
            }
            max_less_or_equal_key = Some(*key);
        }
        if max_less_or_equal_key.is_none()
            && block_seq_no > MAX_BLOCK_SEQ_NO_THAT_CAN_BE_BUILT_FROM_ZEROSTATE
        {
            anyhow::bail!(
                "Requested block is too old to be built from saved states and too late to be built from zerostate"
            );
        }
        let (state_seq_no, state_block_id) = match max_less_or_equal_key {
            Some(seq_no) => (seq_no, saved_states.get(&seq_no).unwrap().clone()),
            None => (BlockSeqNoFor::<Self>::default(), BlockIdentifierFor::<Self>::default()),
        };
        tracing::trace!(
            "try_load_state_from_archive use state: {state_seq_no:?} {state_block_id:?}"
        );

        let mut blocks = vec![];
        let mut block_id = block_id.clone();

        loop {
            let block = self.get_block_from_repo_or_archive(&block_id)?;
            tracing::trace!(
                "try_load_state_from_archive: loaded block: {:?} {block_id:?}",
                block.data().seq_no()
            );
            if block.data().seq_no() <= state_seq_no {
                tracing::trace!(
                    "try_load_state_from_archive: requested block is older than last saved state"
                );
                return Ok(None);
            }
            blocks.push(block.data().clone());

            if block.data().parent() == state_block_id {
                break;
            }
            block_id = block.data().parent();
        }
        blocks.reverse();
        let dir_path = self.get_optimistic_state_path();
        tracing::trace!("try_load_state_from_archive loading start state");
        let mut state = if state_block_id == BlockIdentifierFor::<Self>::default() {
            tracing::trace!("Load state from zerostate");
            let mut zero_block = <Self as Repository>::OptimisticState::zero();
            if let Some(path) = &self.zerostate_path {
                let zerostate = ZeroState::load_from_file(path)?;
                zero_block.shard_state = Some(Arc::new(zerostate.shard_state));
            };
            zero_block
        } else {
            match self.load::<Vec<u8>>(dir_path, OID::ID(state_block_id.to_string()))? {
                Some(buffer) => {
                    <Self as Repository>::OptimisticState::deserialize(&buffer, block_id.clone())?
                }
                None => {
                    anyhow::bail!("Failed to load saved optimistic state");
                }
            }
        };
        tracing::trace!("try_load_state_from_archive start applying blocks");
        let mut last_processed_message_index = state.last_processed_external_message_index;
        let mut state_cell = state.get_shard_state_as_cell();
        let last_block = blocks.last().unwrap().clone();
        let final_block_info = prepare_prev_block_info(&last_block);
        let final_block_id = last_block.identifier();
        for block in blocks {
            tracing::trace!("try_load_state_from_archive apply block {:?}", block.identifier());
            let state_update = block
                .block
                .read_state_update()
                .map_err(|e| anyhow::format_err!("Failed to read block state update: {e}"))?;
            state_cell = state_update
                .apply_for(&state_cell)
                .map_err(|e| anyhow::format_err!("Failed to apply state update: {e}"))?;
            last_processed_message_index += block.processed_ext_messages_cnt as u32;
        }

        tracing::trace!("try_load_state_from_archive return state");
        OptimisticStateImpl::from_shard_state_cell_and_block_info(
            final_block_id,
            state_cell,
            final_block_info,
            last_processed_message_index,
        )
        .map(Some)
    }
}

impl Repository for RepositoryImpl {
    type Attestation = Envelope<GoshBLS, AttestationData<WrappedUInt256, u32>>;
    type BLS = GoshBLS;
    type Block = WrappedBlock;
    type CandidateBlock = Envelope<GoshBLS, WrappedBlock>;
    type EnvelopeSignerIndex = u16;
    type NodeIdentifier = NodeIdentifier;
    type OptimisticState = OptimisticStateImpl;
    type StateSnapshot = Vec<u8>;
    type ThreadIdentifier = u64;

    fn saved_optimistic_states(
        &self,
    ) -> BTreeMap<<Self::Block as Block>::BlockSeqNo, <Self::Block as Block>::BlockIdentifier> {
        self.saved_states.lock().clone()
    }

    fn dump_sent_attestations(
        &self,
        data: HashMap<
            Self::ThreadIdentifier,
            Vec<(<Self::Block as Block>::BlockSeqNo, Self::Attestation)>,
        >,
    ) -> anyhow::Result<()> {
        let start = std::time::Instant::now();
        tracing::trace!("saving attestations");
        let path = self.get_attestations_path();
        let path = self.get_path(path, DEFAULT_OID.to_owned());
        let _ =
            thread::Builder::new().name("saving attestations".to_string()).spawn(move || {
                // let bytes = bincode::serialize(&data).expect("Failed to serialize receipts");
                let res = save_to_file(&path, &data);
                tracing::trace!("save attestations result: {res:?}");
            })?;
        tracing::trace!("store attestations: {}ms", start.elapsed().as_millis());
        Ok(())
    }

    fn load_sent_attestations(
        &self,
    ) -> anyhow::Result<
        HashMap<
            Self::ThreadIdentifier,
            Vec<(<Self::Block as Block>::BlockSeqNo, Self::Attestation)>,
        >,
    > {
        let start = std::time::Instant::now();
        tracing::trace!("saving attestations");
        let path = self.get_attestations_path();
        let path = self.get_path(path, DEFAULT_OID.to_owned());
        let res = load_from_file(&path)?.unwrap_or_default();

        tracing::trace!("load attestations: {}ms", start.elapsed().as_millis());
        Ok(res)
    }

    fn get_block(
        &self,
        identifier: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<Self::CandidateBlock>> {
        self.load_candidate_block(identifier)
    }

    fn get_block_from_repo_or_archive(
        &self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<<Self as Repository>::CandidateBlock> {
        Ok(if let Some(block) = self.get_block(block_id)? {
            block
        } else if let Some(block) = self.load_archived_block(&Some(block_id.clone()))? {
            block
        } else {
            anyhow::bail!("get_block_from_repo_or_archive: failed to load block: {block_id:?}");
        })
    }

    fn get_block_from_repo_or_archive_by_seq_no(
        &self,
        block_seq_no: &BlockSeqNoFor<Self>,
    ) -> anyhow::Result<Vec<<Self as Repository>::CandidateBlock>> {
        let ids = self.get_block_id_by_seq_no(block_seq_no);
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
        seq_no: &BlockSeqNoFor<Self>,
    ) -> anyhow::Result<Vec<Self::CandidateBlock>> {
        let metadata = self.metadata.lock();
        let mut result = vec![];
        for (block_id, block_seq_no) in metadata.block_id_to_seq_no.iter() {
            if block_seq_no == seq_no {
                if let Some(block) = self.get_block(block_id)? {
                    result.push(block);
                }
            }
        }
        log::info!("list_blocks_with_seq_no: {} -> {} blocks found.", seq_no, result.len());
        Ok(result)
    }

    fn select_thread_last_finalized_block(
        &self,
        _thread_id: &Self::ThreadIdentifier,
    ) -> anyhow::Result<(BlockIdentifierFor<Self>, BlockSeqNoFor<Self>)> {
        tracing::trace!("Start select_thread_last_finalized_block");
        let mut metadata = self.metadata.lock();
        let mut cursor_id = metadata.last_finalized_block_id.clone();
        let mut cursor_seq_no = metadata.last_finalized_block_seq_no;
        let mut is_cursor_moved = true;
        let mut is_metadata_update_needed = false;

        while is_cursor_moved {
            is_cursor_moved = false;
            let next_seq_no = cursor_seq_no.next();

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
            self.save_metadata(&metadata)?;
        }
        log::info!(
            "select_thread_last_finalized_block: block_seq_no: {}, id: {}",
            cursor_seq_no,
            cursor_id
        );
        Ok((cursor_id, cursor_seq_no))
    }

    fn select_thread_last_main_candidate_block(
        &self,
        _thread_id: &Self::ThreadIdentifier,
    ) -> anyhow::Result<(BlockIdentifierFor<Self>, BlockSeqNoFor<Self>)> {
        let mut metadata = self.metadata.lock();
        let mut cursor_id = metadata.last_main_candidate_block_id.clone();
        let mut cursor_seq_no = metadata.last_main_candidate_block_seq_no;
        let mut is_cursor_moved = true;
        let mut is_metadata_update_needed = false;
        while is_cursor_moved {
            is_cursor_moved = false;
            let next_seq_no = cursor_seq_no.next();
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
            self.save_metadata(&metadata)?;
        }
        log::info!(
            "select_thread_last_main_candidate_block: block_seq_no: {}, id: {}",
            cursor_seq_no,
            cursor_id
        );
        Ok((cursor_id, cursor_seq_no))
    }

    fn mark_block_as_accepted_as_main_candidate(
        &self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<()> {
        tracing::trace!("mark_block_as_accepted_as_main_candidate {block_id:?}");
        let mut markers = self.load_markers(block_id)?;
        markers.is_main_candidate = Some(true);
        self.save_markers(block_id, &markers)?;
        let mut metadata = self.metadata.lock();
        if metadata.block_id_to_seq_no.contains_key(block_id) {
            let block_seq_no = metadata.block_id_to_seq_no.get(block_id).unwrap();
            if *block_seq_no > metadata.last_main_candidate_block_seq_no {
                metadata.last_main_candidate_block_seq_no = *block_seq_no;
                metadata.last_main_candidate_block_id = block_id.clone();
                self.save_metadata(&metadata)?;
            }
        }
        Ok(())
    }

    fn is_block_accepted_as_main_candidate(
        &self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<bool>> {
        let markers = self.load_markers(block_id).unwrap_or_default();
        tracing::trace!(
            "is_block_accepted_as_main_candidate {block_id:?} {:?}",
            markers.is_main_candidate
        );
        Ok(markers.is_main_candidate)
    }

    fn mark_block_as_finalized(
        &mut self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<()> {
        let mut metadata = self.metadata.lock();
        let block_seq_no = *metadata.block_id_to_seq_no.get(block_id).unwrap();
        tracing::trace!("mark_block_as_finalized {block_seq_no:?} {block_id:?}");
        let mut markers = self.load_markers(block_id)?;
        markers.is_finalized = Some(true);
        self.save_markers(block_id, &markers)?;

        metadata.stored_finalized_blocks.push((block_id.clone(), block_seq_no));
        if block_seq_no > metadata.last_finalized_block_seq_no {
            metadata.last_finalized_block_seq_no = block_seq_no;
            metadata.last_finalized_block_id = block_id.clone();
        }
        self.save_metadata(&metadata)?;

        // // After sync we mark the incoming block as finalized and this check works
        // if block_id != &self.finalized_optimistic_state.block_id {
        //     let block = self
        //         .load_candidate_block(block_id)?
        //         .expect("Finalized block should present in the repo");
        //     self.finalized_optimistic_state.apply_block(block.data().clone())?;
        // }
        Ok(())
    }

    fn is_block_finalized(
        &self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<bool>> {
        tracing::trace!("is_block_finalized {:?}", block_id);
        let markers = self.load_markers(block_id).unwrap_or_default();
        tracing::trace!("is_block_finalized {:?}: {:?}", block_id, markers.is_finalized);
        Ok(markers.is_finalized)
    }

    fn get_optimistic_state(
        &self,
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<Option<Self::OptimisticState>> {
        let zero_block_id = <BlockIdentifierFor<Self>>::default();
        if block_id == &zero_block_id {
            let mut zero_block = Self::OptimisticState::zero();
            if let Some(path) = &self.zerostate_path {
                let zerostate = ZeroState::load_from_file(path)?;
                zero_block.shard_state = Some(Arc::new(zerostate.shard_state));
            };
            return Ok(Some(zero_block));
        }
        log::info!("RepositoryImpl: get_optimistic_state {:?}", block_id);
        let path = self.get_optimistic_state_path();
        let state = match self.load::<Vec<u8>>(path, OID::ID(block_id.to_string()))? {
            Some(buffer) => {
                let state = Self::OptimisticState::deserialize(&buffer, block_id.clone())?;
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

    fn is_optimistic_state_present(&self, block_id: &BlockIdentifierFor<Self>) -> bool {
        let zero_block_id = <BlockIdentifierFor<Self>>::default();
        if block_id == &zero_block_id {
            match &self.zerostate_path {
                Some(path) => path.exists(),
                None => false,
            }
        } else {
            let path = self.get_optimistic_state_path();
            self.exists(path, OID::ID(block_id.to_string()))
        }
    }

    fn store_block<T: Into<Self::CandidateBlock>>(&self, block: T) -> anyhow::Result<()> {
        let mut metadata = self.metadata.lock();
        let candidate_block = block.into();
        log::info!("RepositoryImpl: store_block {:?}", candidate_block.data().identifier());
        metadata
            .block_id_to_seq_no
            .insert(candidate_block.data().identifier(), candidate_block.data().seq_no());
        self.save_metadata(&metadata)?;

        let parent_block_id = candidate_block.data().parent();

        let parent_marker = self.load_markers(&parent_block_id)?;
        let mut candidate_marker = self.load_markers(&candidate_block.data().identifier())?;
        if candidate_marker.last_processed_ext_message == 0 {
            candidate_marker.last_processed_ext_message = parent_marker.last_processed_ext_message
                + (candidate_block.data().processed_ext_messages_cnt as u32);
            self.save_markers(&candidate_block.data().identifier(), &candidate_marker)?;
        }

        log::info!(
            "saving block: id: {}, seq_no: {}, signatures: {:?}",
            candidate_block.data().identifier(),
            candidate_block.data().seq_no(),
            candidate_block.clone_signature_occurrences(),
        );

        self.save_candidate_block(candidate_block, candidate_marker)?;
        // Get parent state
        // let parent_block_id = candidate_block.data().parent();
        // log::info!("RepositoryImpl: store_block: parent {:?}", parent_block_id);
        // if let Some(parent_state) = self.get_optimistic_state(&parent_block_id)? {
        // log::info!("RepositoryImpl: store_block: parent found");
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
        block_id: &BlockIdentifierFor<Self>,
    ) -> anyhow::Result<()> {
        tracing::trace!("erase_block_and_optimistic_state: {:?}", block_id);
        if !self.is_block_present_in_archive(block_id).unwrap_or(false) {
            tracing::trace!("Block was not saved to archive, do not erase it");
            return Ok(());
        }
        let mut metadata = self.metadata.lock();
        assert_ne!(metadata.last_main_candidate_block_id, *block_id);
        assert_ne!(metadata.last_finalized_block_id, *block_id);
        metadata.block_id_to_seq_no.remove(block_id);
        metadata.stored_finalized_blocks.retain(|e| &e.0 != block_id);
        self.save_metadata(&metadata)?;

        self.erase_markers(block_id)?;

        // let optimistic_state_path = self.get_optimistic_state_path();
        // self.remove(optimistic_state_path, OID::ID(block_id.to_string()))?;
        self.erase_candidate_block(block_id)?;
        Ok(())
    }

    fn erase_block(&self, block_id: &BlockIdentifierFor<Self>) -> anyhow::Result<()> {
        tracing::trace!("erase_block: {:?}", block_id);
        let mut metadata = self.metadata.lock();
        assert_ne!(metadata.last_main_candidate_block_id, *block_id);
        assert_ne!(metadata.last_finalized_block_id, *block_id);
        metadata.block_id_to_seq_no.remove(block_id);
        metadata.stored_finalized_blocks.retain(|e| &e.0 != block_id);
        self.save_metadata(&metadata)?;

        self.erase_markers(block_id)?;
        self.erase_candidate_block(block_id)?;
        Ok(())
    }

    fn list_stored_thread_finalized_blocks(
        &self,
        _thread_id: &Self::ThreadIdentifier,
    ) -> anyhow::Result<Vec<(BlockIdentifierFor<Self>, BlockSeqNoFor<Self>)>> {
        // Note: This implementation assumes that there is only one thread exists.
        Ok(self.metadata.lock().stored_finalized_blocks.clone())
    }

    fn delete_external_messages(&self, count: usize) -> anyhow::Result<()> {
        tracing::trace!("Removing {count} external message(s) from repo...");
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
        tracing::trace!("Add external message to repo");
        let mut queue = self.load_ext_messages_queue()?;
        let cur_last_index = queue.queue.last().map(|el| el.index).unwrap_or(0);
        assert!(self.last_ext_message_index >= cur_last_index);
        for message in messages {
            self.last_ext_message_index += 1;
            let message = message.into();
            tracing::trace!(
                "Add external message with index {} to repo",
                self.last_ext_message_index
            );
            queue.queue.push(WrappedExtMessage::new(self.last_ext_message_index, message));
        }
        self.save_ext_messages_queue(queue)
    }

    fn clear_ext_messages_queue_by_time(&self) -> anyhow::Result<()> {
        tracing::trace!("Removing old message(s) from repo...");
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
            tracing::trace!("Removed {i} old message(s) from repo...");
            queue.queue.drain(0..i);
            self.save_ext_messages_queue(queue)?;
        }
        Ok(())
    }

    fn clear_verification_markers(
        &self,
        excluded_starting_seq_no: &BlockSeqNoFor<Self>,
    ) -> anyhow::Result<()> {
        tracing::trace!("clear_verification_markers after {excluded_starting_seq_no:?}");
        let metadata = self.metadata.lock();
        let mut cursor_seq_no = *excluded_starting_seq_no;
        let mut is_cursor_moved = true;
        while is_cursor_moved {
            is_cursor_moved = false;
            cursor_seq_no = cursor_seq_no.next();
            for (block_id, block_seq_no) in metadata.block_id_to_seq_no.iter() {
                if cursor_seq_no == *block_seq_no {
                    tracing::trace!("clear_verification_markers {cursor_seq_no:?} {block_id:?}");
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

    fn mark_block_as_verified(&self, block_id: &BlockIdentifierFor<Self>) -> anyhow::Result<()> {
        let mut markers = self.load_markers(block_id)?;
        markers.is_verified = true;
        self.save_markers(block_id, &markers)
    }

    fn is_block_verified(&self, block_id: &BlockIdentifierFor<Self>) -> anyhow::Result<bool> {
        let markers = self.load_markers(block_id).unwrap_or_default();
        Ok(markers.is_verified)
    }

    fn take_state_snapshot(
        &self,
        block_id: &BlockIdentifierFor<Self>,
        block_producer_groups: HashMap<Self::ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        block_keeper_set: BlockKeeperSet,
    ) -> anyhow::Result<Self::StateSnapshot> {
        let state = match self.get_optimistic_state(block_id)? {
            Some(state) => state.serialize()?,
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
        block_producer_groups: HashMap<Self::ThreadIdentifier, Vec<Self::NodeIdentifier>>,
        block_keeper_set: BlockKeeperSet,
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
        block_id: &BlockIdentifierFor<Self>,
        snapshot: Self::StateSnapshot,
    ) -> anyhow::Result<(HashMap<Self::ThreadIdentifier, Vec<Self::NodeIdentifier>>, BlockKeeperSet)>
    {
        tracing::debug!("set_state_from_snapshot()...");
        let wrapper_snapshot: WrappedStateSnapshot = bincode::deserialize(&snapshot)?;
        tracing::trace!("Set state from snapshot: {block_id:?}");
        let state = <Self as Repository>::OptimisticState::deserialize(
            &wrapper_snapshot.optimistic_state,
            block_id.clone(),
        )?;
        self.store_optimistic(state.clone())?;
        let seq_no = state
            .get_block_info()
            .prev1()
            .map_err(|e| anyhow::format_err!("Failed to read block info: {e}"))?
            .seq_no;
        self.finalized_optimistic_state = state;
        let mut saved_states = self.saved_states.lock();
        saved_states.insert(seq_no, block_id.clone());

        Ok((wrapper_snapshot.producer_group, wrapper_snapshot.block_keeper_set))
    }

    fn save_account_diffs(
        &self,
        block_id: WrappedUInt256,
        accounts: HashMap<String, SerializedItem>,
    ) -> anyhow::Result<()> {
        // TODO split according to the sqlite limits
        self.save_accounts(block_id, accounts)?;
        Ok(())
    }

    fn dump_verification_data<
        TBlock: Into<Self::CandidateBlock>,
        TState: Into<Self::OptimisticState>,
    >(
        &self,
        prev_block: TBlock,
        prev_state: TState,
        cur_block: TBlock,
        failed_block: TBlock,
        failed_state: TState,
        incoming_state: TState,
    ) -> anyhow::Result<()> {
        let prev_block = prev_block.into();
        let prev_state = prev_state.into();
        let cur_block = cur_block.into();
        let failed_block = failed_block.into();
        let failed_state = failed_state.into();
        let incoming_state = incoming_state.into();

        let failed_block_id = cur_block.data().identifier().to_string();

        log::info!("RepositoryImpl: Saving verify failure dump: {}", failed_block_id);

        log::info!("RepositoryImpl: store_block {:?}", prev_block.data().identifier());
        let path = self
            .data_dir
            .join(FAILED_BLOCK_DIR)
            .join(&failed_block_id)
            .join("candidate-blocks")
            .join(prev_block.data().identifier().to_string());
        save_to_file(&path, &prev_block)?;

        log::info!("RepositoryImpl: store_block {:?}", cur_block.data().identifier());
        let path = self
            .data_dir
            .join(FAILED_BLOCK_DIR)
            .join(&failed_block_id)
            .join("candidate-blocks")
            .join(cur_block.data().identifier().to_string());
        save_to_file(&path, &cur_block)?;

        log::info!("RepositoryImpl: store_block verify_{:?}", failed_block.data().identifier());
        let path = self
            .data_dir
            .join(FAILED_BLOCK_DIR)
            .join(&failed_block_id)
            .join("candidate-blocks")
            .join(format!("verify_{}", failed_block.data().identifier()));
        save_to_file(&path, &failed_block)?;

        log::info!("RepositoryImpl: store_state {:?}", prev_state.block_id);
        let path = self
            .data_dir
            .join(FAILED_BLOCK_DIR)
            .join(&failed_block_id)
            .join("optimistic-state")
            .join(prev_state.block_id.to_string());
        let state_bytes = prev_state.serialize()?;
        save_to_file(&path, &state_bytes)?;

        log::info!("RepositoryImpl: store_state {:?}", incoming_state.block_id);
        let path = self
            .data_dir
            .join(FAILED_BLOCK_DIR)
            .join(&failed_block_id)
            .join("optimistic-state")
            .join(incoming_state.block_id.to_string());
        let state_bytes = incoming_state.serialize()?;
        save_to_file(&path, &state_bytes)?;

        log::info!("RepositoryImpl: store_state {:?}", failed_state.block_id);
        let path = self
            .data_dir
            .join(FAILED_BLOCK_DIR)
            .join(&failed_block_id)
            .join("optimistic-state")
            .join(format!("verify_{}", failed_state.block_id));
        let state_bytes = failed_state.serialize()?;
        save_to_file(&path, &state_bytes)?;

        Ok(())
    }

    fn last_stored_block_by_seq_no(&self) -> anyhow::Result<BlockSeqNoFor<Self>> {
        let metadata = self.metadata.lock();
        let mut saved_seq_nos = metadata.block_id_to_seq_no.values().collect::<Vec<_>>();
        saved_seq_nos.sort();
        Ok(saved_seq_nos.last().map(|v| **v).unwrap_or_default())
    }

    fn store_optimistic<T: Into<Self::OptimisticState>>(&mut self, state: T) -> anyhow::Result<()> {
        let start = std::time::Instant::now();
        let optimistic = state.into();
        let block_id = optimistic.get_block_id().clone();
        tracing::trace!("save optimistic {block_id:?}");
        self.clear_optimistic_states()?;
        let block_seq_no = optimistic
            .get_block_info()
            .prev1()
            .map_err(|e| anyhow::format_err!("Failed to read block info: {e}"))?
            .seq_no;
        {
            let mut saved_states = self.saved_states.lock();
            saved_states.insert(block_seq_no, block_id.clone());
            tracing::trace!("repo saved_states={:?}", saved_states);
        }
        let root_path = self.get_optimistic_state_path();
        let path = self.get_path(root_path, block_id.to_string());
        let _ = thread::Builder::new().name("saving optimistic state".to_string()).spawn(
            move || {
                let start_save = std::time::Instant::now();
                let state_bytes = optimistic.serialize().expect("Failed to serialize block");
                tracing::trace!(
                    "serialize state {block_id:?}: {}ms",
                    start_save.elapsed().as_millis()
                );
                let res = save_to_file(&path, &state_bytes);
                tracing::trace!(
                    "save optimistic {block_id:?} result: {res:?} {}",
                    start_save.elapsed().as_millis()
                );
            },
        )?;
        tracing::trace!("store optimistic time: {}ms", start.elapsed().as_millis());
        Ok(())
    }

    fn get_block_id_by_seq_no(
        &self,
        block_seq_no: &BlockSeqNoFor<Self>,
    ) -> Vec<BlockIdentifierFor<Self>> {
        self.metadata
            .lock()
            .block_id_to_seq_no
            .iter()
            .filter(|(_id, seq_no)| *seq_no == block_seq_no)
            .map(|(id, _seq_no)| id.to_owned())
            .collect()
    }

    fn get_latest_block_id_with_producer_group_change(
        &self,
        _thread_id: &Self::ThreadIdentifier,
    ) -> anyhow::Result<BlockIdentifierFor<Self>> {
        Ok(BlockIdentifierFor::<Self>::default())
    }

    fn get_finalized_state(&self) -> Self::OptimisticState {
        self.finalized_optimistic_state.clone()
    }

    fn get_finalized_state_as_mut(&mut self) -> &mut Self::OptimisticState {
        tracing::trace!("get_finalized_state_as_mut");
        &mut self.finalized_optimistic_state
    }
}
