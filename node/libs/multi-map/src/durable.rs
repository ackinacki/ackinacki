use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::BufReader;
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::Serialize;
use trie_map::MapKey;
use trie_map::MapKeyPath;

use crate::repo::MultiMapRef;
use crate::repo::MultiMapRepository;
use crate::MultiMapValue;

/// Per-entry index metadata stored on disk.
#[derive(Clone, Serialize, serde::Deserialize)]
struct IndexRecord<IK, IV> {
    key: IK,
    root_hash: [u8; 32],
    root_path: MapKeyPath,
    index_value: IV,
}

/// In-memory state for an indexed map entry.
struct IndexEntry<V: MultiMapValue, IV> {
    map: MultiMapRef<V>,
    index_value: IV,
}

/// Read-only loader for the old durable format (index.bin + snapshots/).
///
/// Used only for migrating from the old two-directory format to the new
/// self-contained state files. No write machinery — load, read, drop.
pub struct DurableMultiMapRepository<Value, IndexKey, IndexValue>
where
    Value: MultiMapValue,
    IndexKey: Eq + Hash,
{
    repo: MultiMapRepository<Value>,
    index: HashMap<IndexKey, IndexEntry<Value, IndexValue>>,
}

impl<V, IK, IV> DurableMultiMapRepository<V, IK, IV>
where
    V: MultiMapValue,
    IK: Eq + Hash + DeserializeOwned,
    IV: Clone + DeserializeOwned,
{
    /// Load from disk. Reads the index and eagerly loads all map snapshots.
    pub fn load(root_path: PathBuf) -> anyhow::Result<Self> {
        let repo = MultiMapRepository::new();
        let mut index = HashMap::new();

        let index_path = root_path.join("index.bin");
        if index_path.exists() {
            let file = File::open(&index_path)?;
            let records: Vec<IndexRecord<IK, IV>> =
                bincode::deserialize_from(BufReader::new(file))?;

            for rec in records {
                let snap_path =
                    root_path.join("snapshots").join(format!("{}.bin", hex::encode(rec.root_hash)));
                let map = if snap_path.exists() {
                    let f = File::open(&snap_path)?;
                    repo.read_map(&mut BufReader::new(f))?
                } else {
                    MultiMapRef { root: crate::node::Node::Empty, root_path: rec.root_path }
                };
                index.insert(rec.key, IndexEntry { map, index_value: rec.index_value });
            }
        }

        Ok(Self { repo, index })
    }

    /// Get a map and its index value by key.
    pub fn index_get(&self, key: &IK) -> Option<(MultiMapRef<V>, IV)> {
        self.index.get(key).map(|e| (e.map.clone(), e.index_value.clone()))
    }

    /// Collect all key-value pairs from the given map.
    pub fn collect_values(&self, map: &MultiMapRef<V>) -> Vec<(MapKey, V)> {
        self.repo.collect_values(map)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::BufWriter;
    use std::io::Write;

    use blake3::Hasher;
    use node_types::Blake3Hashable;
    use serde::Deserialize;
    use serde::Serialize;
    use tempfile::TempDir;

    use super::*;

    #[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct TV(pub u64);

    impl Blake3Hashable for TV {
        fn update_hasher(&self, hasher: &mut Hasher) {
            hasher.update(&self.0.to_be_bytes());
        }
    }

    fn key(i: usize) -> MapKey {
        let mut h = Hasher::new();
        h.update(b"key:");
        h.update(&(i as u64).to_le_bytes());
        MapKey(*h.finalize().as_bytes())
    }

    /// Helper: write old-format durable data (index.bin + snapshots/) for testing load.
    fn write_old_format(dir: &std::path::Path, entries: &[(u32, Vec<(MapKey, TV)>)]) {
        let repo = MultiMapRepository::<TV>::new();
        let snap_dir = dir.join("snapshots");
        fs::create_dir_all(&snap_dir).unwrap();

        let mut records = Vec::new();
        for (index_key, kvs) in entries {
            let mut map = MultiMapRepository::<TV>::new_map();
            let updates: Vec<_> = kvs.iter().map(|(k, v)| (*k, Some(*v))).collect();
            map = repo.map_update(&map, &updates);
            let root_hash = map.root.hash();

            // Write snapshot
            let snap_path = snap_dir.join(format!("{}.bin", hex::encode(root_hash)));
            if !snap_path.exists() {
                let f = File::create(&snap_path).unwrap();
                let mut w = BufWriter::new(f);
                repo.write_map(&map, &mut w).unwrap();
                w.flush().unwrap();
            }

            records.push(IndexRecord::<u32, ()> {
                key: *index_key,
                root_hash,
                root_path: map.root_path,
                index_value: (),
            });
        }

        // Write index
        let index_path = dir.join("index.bin");
        let f = File::create(&index_path).unwrap();
        let mut w = BufWriter::new(f);
        bincode::serialize_into(&mut w, &records).unwrap();
        w.flush().unwrap();
    }

    #[test]
    fn load_and_read() {
        let dir = TempDir::new().unwrap();
        let kvs: Vec<_> = (0..100).map(|i| (key(i), TV(i as u64))).collect();
        write_old_format(dir.path(), &[(1, kvs)]);

        let loaded =
            DurableMultiMapRepository::<TV, u32, ()>::load(dir.path().to_path_buf()).unwrap();
        let (m, _) = loaded.index_get(&1).unwrap();
        for i in 0..100 {
            let vals = loaded.collect_values(&m);
            assert!(vals.iter().any(|(k, v)| *k == key(i) && *v == TV(i as u64)));
        }
    }

    #[test]
    fn load_missing_index() {
        let dir = TempDir::new().unwrap();
        let loaded =
            DurableMultiMapRepository::<TV, u32, ()>::load(dir.path().to_path_buf()).unwrap();
        assert!(loaded.index_get(&1).is_none());
    }

    #[test]
    fn load_multiple_maps() {
        let dir = TempDir::new().unwrap();
        let kvs1: Vec<_> = (0..50).map(|i| (key(i), TV(i as u64))).collect();
        let kvs2: Vec<_> = (100..150).map(|i| (key(i), TV(i as u64 * 3))).collect();
        write_old_format(dir.path(), &[(1, kvs1), (2, kvs2)]);

        let loaded =
            DurableMultiMapRepository::<TV, u32, ()>::load(dir.path().to_path_buf()).unwrap();

        let (m1, _) = loaded.index_get(&1).unwrap();
        assert_eq!(loaded.collect_values(&m1).len(), 50);

        let (m2, _) = loaded.index_get(&2).unwrap();
        assert_eq!(loaded.collect_values(&m2).len(), 50);

        assert!(loaded.index_get(&999).is_none());
    }
}
