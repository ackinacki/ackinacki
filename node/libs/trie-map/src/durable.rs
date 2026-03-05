use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::trie::arena::Arena;
use crate::trie::node::NodeId;
use crate::trie::smt::TrieMapRef;
use crate::trie::smt::TrieMapSnapshot;
use crate::MapHash;
use crate::MapKey;
use crate::MapKeyPath;
use crate::MapRepository;
use crate::MapValue;
use crate::TrieMapRepository;

#[derive(Debug)]
pub struct DurableMapStat {
    pub total_nodes: usize,
    pub reachable_nodes: usize,
    pub values_count: usize,
    pub branch_children_count: usize,
    pub ext_paths_bytes: usize,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct DurableIndexValue<Value> {
    pub root: NodeId,
    pub root_path: MapKeyPath,
    pub value: Value,
}

impl<Value> DurableIndexValue<Value> {
    pub fn new(root: NodeId, root_path: MapKeyPath, value: Value) -> Self {
        Self { root, root_path, value }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum DurableIndexUpdate<Key, Value> {
    Set { key: Key, value: Value },
    Del { key: Key },
}

#[derive(Default)]
pub struct DurableIndex<Key: Clone + Eq + Hash, Value> {
    pub values: HashMap<Key, Value>,
    pub updates: Vec<DurableIndexUpdate<Key, Value>>,
}

impl<Key: Clone + Copy + Eq + Hash, Value: Clone + Copy> DurableIndex<Key, Value> {
    fn new() -> Self {
        Self { values: HashMap::new(), updates: Vec::new() }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self { values: HashMap::with_capacity(capacity), updates: Vec::new() }
    }

    pub fn update(&mut self, key: Key, root: Option<Value>) {
        if let Some(root) = root {
            self.updates.push(DurableIndexUpdate::Set { key, value: root });
            self.values.insert(key, root);
        } else {
            self.updates.push(DurableIndexUpdate::Del { key });
            self.values.remove(&key);
        }
    }

    pub fn read_commited(name: PathBuf, commit: DurableSeqCommit) -> anyhow::Result<Self>
    where
        Key: Clone + Copy + Hash + Eq + DeserializeOwned,
        Value: Clone + Copy + DeserializeOwned,
    {
        let start = Instant::now();
        let f = File::open(&name).with_context(|| format!("open {:?}", name))?;
        let limited = f.take(commit.bytes);
        let mut reader = BufReader::with_capacity(1024 * 1024, limited);
        let mut index = DurableIndex::<Key, Value>::with_capacity(commit.count as usize);
        loop {
            match bincode::deserialize_from::<_, DurableIndexUpdate<Key, Value>>(&mut reader) {
                Ok(DurableIndexUpdate::Set { key, value: root }) => {
                    index.values.insert(key, root);
                }
                Ok(DurableIndexUpdate::Del { key }) => {
                    index.values.remove(&key);
                }
                Err(e) => {
                    if let bincode::ErrorKind::Io(ioe) = &*e {
                        if ioe.kind() == std::io::ErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                    return Err(e).with_context(|| format!("bincode decode failed for {:?}", name));
                }
            }
        }
        println!("read_commited_index {:?} in {:?}", name, start.elapsed());
        Ok(index)
    }
}

pub type DurableMapRef = TrieMapRef;

#[derive(Clone)]
pub struct DurableMapRepository<Value, IndexKey, IndexValue>
where
    Value: MapValue,
    IndexKey: Clone + Copy + Eq + Hash,
    IndexValue: Clone + Copy,
{
    root_path: PathBuf,
    repository: TrieMapRepository<Value>,
    index: Arc<parking_lot::RwLock<DurableIndex<IndexKey, DurableIndexValue<IndexValue>>>>,
    commit: Arc<parking_lot::Mutex<Commit>>,
}

impl<Value, IndexKey, IndexValue> DurableMapRepository<Value, IndexKey, IndexValue>
where
    Value: MapValue,
    IndexKey: Clone + Copy + Eq + Hash + Serialize + DeserializeOwned,
    IndexValue: Clone + Copy + Serialize + DeserializeOwned,
{
    pub fn load(root_path: PathBuf) -> anyhow::Result<Self> {
        let (commit, arena, index) =
            if let Ok(Some(commit)) = read_from_file::<Commit>(&root_path, "commit") {
                let nodes = read_commited(root_path.join("nodes"), commit.nodes)?;
                let values = read_commited(root_path.join("values"), commit.values)?;
                let branch_children =
                    read_commited(root_path.join("branch_children"), commit.branch_children)?;
                let ext_paths = read_commited(root_path.join("ext_paths"), commit.ext_paths)?;
                let index = DurableIndex::read_commited(root_path.join("index"), commit.index)?;
                let arena = Arena::with_raw_parts(nodes, values, branch_children, ext_paths);
                (commit, arena, index)
            } else {
                (Commit::default(), Arena::new(), DurableIndex::new())
            };
        let repository = TrieMapRepository::with_arena(arena);
        Ok(Self {
            root_path,
            repository,
            index: Arc::new(parking_lot::RwLock::new(index)),
            commit: Arc::new(parking_lot::Mutex::new(commit)),
        })
    }

    pub fn get_stat(&self) -> DurableMapStat {
        let index = self.index.read();
        self.repository.get_stat(index.values.values().map(|v| v.root))
    }

    fn next_commit_ranges(
        &self,
        commit: &Commit,
    ) -> (Range<usize>, Range<usize>, Range<usize>, Range<usize>, Range<usize>) {
        let arena = self.repository.arena.read();
        let index = self.index.read();
        (
            (commit.nodes.count as usize)..arena.nodes.len(),
            (commit.values.count as usize)..arena.values.len(),
            (commit.branch_children.count as usize)..arena.branch_children.len(),
            (commit.ext_paths.count as usize)..arena.ext_paths.len(),
            0..index.updates.len(),
        )
    }

    pub fn commit(&self) -> anyhow::Result<()> {
        let mut commit_lock = self.commit.lock();
        let mut commit = *commit_lock;
        let (nodes_range, values_range, branch_children_range, ext_paths_range, index_range) =
            self.next_commit_ranges(&commit);
        if nodes_range.is_empty()
            && values_range.is_empty()
            && branch_children_range.is_empty()
            && ext_paths_range.is_empty()
            && index_range.is_empty()
        {
            return Ok(());
        }

        append_from_commit(self.root_path.join("nodes"), &mut commit.nodes, nodes_range, |r| {
            self.repository.arena.read().nodes[r].to_vec()
        })?;
        append_from_commit(self.root_path.join("values"), &mut commit.values, values_range, |r| {
            self.repository.arena.read().values[r].to_vec()
        })?;
        append_from_commit(
            self.root_path.join("branch_children"),
            &mut commit.branch_children,
            branch_children_range,
            |r| self.repository.arena.read().branch_children[r].to_vec(),
        )?;
        append_from_commit(
            self.root_path.join("ext_paths"),
            &mut commit.ext_paths,
            ext_paths_range,
            |r| self.repository.arena.read().ext_paths[r].to_vec(),
        )?;
        append_from_commit(
            self.root_path.join("index"),
            &mut commit.index,
            index_range.clone(),
            |r| self.index.read().updates[r].to_vec(),
        )?;

        write_commit_to_file(&self.root_path, "commit", &commit)?;

        self.index.write().updates.drain(index_range);
        *commit_lock = commit;
        Ok(())
    }

    pub fn new_map() -> DurableMapRef {
        TrieMapRepository::<Value>::new_map()
    }

    pub fn index_get(&self, root_key: &IndexKey) -> Option<(DurableMapRef, IndexValue)> {
        if let Some(DurableIndexValue { root, root_path, value }) =
            self.index.read().values.get(root_key)
        {
            Some((DurableMapRef::new(*root, *root_path), *value))
        } else {
            None
        }
    }

    pub fn index_set(&self, root_key: &IndexKey, map: &DurableMapRef, index_value: IndexValue) {
        self.index
            .write()
            .update(*root_key, Some(DurableIndexValue::new(map.root, map.root_path, index_value)));
    }

    pub fn map_hash(&self, map: &DurableMapRef) -> MapHash {
        self.repository.map_hash(map)
    }

    pub fn map_get(&self, map: &DurableMapRef, key: &MapKey) -> anyhow::Result<Option<Value>> {
        Ok(self.repository.map_get(map, key))
    }

    pub fn map_update(
        &self,
        map: &DurableMapRef,
        values: &[(MapKey, Option<Value>)],
    ) -> anyhow::Result<DurableMapRef> {
        Ok(self.repository.map_update(map, values))
    }

    pub fn map_split(
        &self,
        map: &DurableMapRef,
        path: MapKeyPath,
    ) -> anyhow::Result<(DurableMapRef, DurableMapRef)> {
        Ok(self.repository.map_split(map, path))
    }

    pub fn merge(&self, a: &DurableMapRef, b: &DurableMapRef) -> anyhow::Result<DurableMapRef> {
        Ok(self.repository.merge(a, b))
    }

    pub fn export_snapshot(&self, map: &DurableMapRef) -> TrieMapSnapshot<Value> {
        self.repository.export_snapshot(map)
    }

    pub fn import_snapshot(&self, snapshot: TrieMapSnapshot<Value>) -> DurableMapRef {
        self.repository.import_snapshot(snapshot)
    }

    pub fn collect_values(&self, map: &DurableMapRef) -> Vec<(MapKey, Value)> {
        self.repository.collect_values(map)
    }
}

#[derive(Default, Clone, Copy, Serialize, Deserialize)]
pub struct DurableSeqCommit {
    pub count: u32,
    pub bytes: u64,
}

#[derive(Default, Clone, Copy, Serialize, Deserialize)]
struct Commit {
    nodes: DurableSeqCommit,
    values: DurableSeqCommit,
    branch_children: DurableSeqCommit,
    ext_paths: DurableSeqCommit,
    index: DurableSeqCommit,
}

fn read_commited<T: DeserializeOwned>(
    name: PathBuf,
    commit: DurableSeqCommit,
) -> anyhow::Result<Vec<T>> {
    let start = Instant::now();
    let f = File::open(&name).with_context(|| format!("open {:?}", name))?;
    let limited = f.take(commit.bytes);
    let mut reader = BufReader::with_capacity(1024 * 1024, limited);
    let mut out = Vec::<T>::with_capacity(commit.count as usize);
    loop {
        match bincode::deserialize_from::<_, T>(&mut reader) {
            Ok(item) => out.push(item),
            Err(e) => {
                if let bincode::ErrorKind::Io(ioe) = &*e {
                    if ioe.kind() == std::io::ErrorKind::UnexpectedEof {
                        break;
                    }
                }
                return Err(e).with_context(|| format!("bincode decode failed for {:?}", name));
            }
        }
    }
    println!("read_commited {:?} in {:?}", name, start.elapsed());
    Ok(out)
}

fn append_from_commit<T: Serialize>(
    path: PathBuf,
    commit: &mut DurableSeqCommit,
    range: Range<usize>,
    get_portion: impl Fn(Range<usize>) -> Vec<T>,
) -> anyhow::Result<()> {
    let f = prepare_fs_commit(path, commit.bytes)?;

    let mut w = BufWriter::with_capacity(256 * 1024, f);
    let mut portion_start = range.start;
    while portion_start < range.end {
        let portion_end = (portion_start + 1000).min(range.end);
        for item in get_portion(portion_start..portion_end) {
            bincode::serialize_into(&mut w, &item)?;
        }
        portion_start = portion_end;
    }

    w.flush()?; // push to OS buffers

    // Get new length
    let mut f = w.into_inner().context("into_inner BufWriter")?;
    let new_len = f.stream_position()?; // current cursor position after writes
    f.sync_data()?; // durability of payload before MANIFEST
    commit.count += range.len() as u32;
    commit.bytes = new_len;
    Ok(())
}

pub fn prepare_fs_commit(path: PathBuf, commit_bytes: u64) -> anyhow::Result<File> {
    let context_path = path.clone();
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .with_context(|| format!("open {:?}", context_path))?;

    let cur_len = f.metadata()?.len();

    if cur_len > commit_bytes {
        f.set_len(commit_bytes)
            .with_context(|| format!("truncate {:?} to {}", context_path, commit_bytes))?;
    } else if cur_len < commit_bytes {
        // Commit points past the file end => corruption or wrong commit file.
        anyhow::ensure!(
            false,
            "file {:?} shorter than committed_bytes (file_len={}, committed_bytes={})",
            context_path,
            cur_len,
            commit_bytes
        );
    }

    f.seek(SeekFrom::Start(commit_bytes))?;
    Ok(f)
}

pub fn write_commit_to_file<T: Serialize>(
    root_path: &PathBuf,
    name: &str,
    commit: &T,
) -> anyhow::Result<()> {
    let tmp_file_path = root_path.join(format!("{name}.new"));
    let commit_file_path = root_path.join(name);

    let buf = bincode::serialize(&commit)?;
    let mut f = File::create(&tmp_file_path)?;
    f.write_all(&buf)?;
    f.sync_all()?;
    drop(f);

    std::fs::rename(&tmp_file_path, &commit_file_path)?;

    // Sync dir
    File::open(root_path)?.sync_all()?;

    Ok(())
}

pub fn read_from_file<T: DeserializeOwned>(
    root_path: impl AsRef<Path>,
    name: impl AsRef<str>,
) -> anyhow::Result<Option<T>> {
    let file_path = root_path.as_ref().join(name.as_ref());
    if !file_path.exists() {
        return Ok(None);
    }
    let t = bincode::deserialize_from(File::open(file_path)?)?;
    Ok(Some(t))
}
