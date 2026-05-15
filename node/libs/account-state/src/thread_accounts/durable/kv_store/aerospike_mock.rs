use std::collections::HashMap;
use std::sync::Arc;

use aerospike2::AerospikeRecord;
use parking_lot::RwLock;

#[derive(Clone, Default)]
pub struct MockAerospikeBackend {
    sets: Arc<RwLock<HashMap<String, HashMap<Vec<u8>, StoredMockRecord>>>>,
}

#[derive(Clone)]
struct StoredMockRecord {
    generation: u64,
    data_epoch: Option<u32>,
    chunk_count: Option<usize>,
    data: Vec<u8>,
}

impl MockAerospikeBackend {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<AerospikeRecord>>> {
        let sets = self.sets.read();
        let set_map = sets.get(set);
        Ok(keys
            .iter()
            .map(|key| {
                set_map.and_then(|map| {
                    map.get(key).map(|stored| AerospikeRecord {
                        key: key.clone(),
                        generation: stored.generation,
                        data_epoch: stored.data_epoch,
                        chunk_count: stored.chunk_count,
                        data: stored.data.clone(),
                    })
                })
            })
            .collect())
    }

    pub fn put(&self, set: &str, records: Vec<AerospikeRecord>, cas: bool) -> anyhow::Result<()> {
        let mut sets = self.sets.write();
        let set_map = sets.entry(set.to_string()).or_default();
        if cas {
            for record in &records {
                match set_map.get(&record.key) {
                    Some(existing) if existing.generation == record.generation => {}
                    None if record.generation == 0 => {}
                    Some(existing) => anyhow::bail!(
                        "CAS mismatch: expected generation {}, found {}",
                        record.generation,
                        existing.generation,
                    ),
                    None => anyhow::bail!(
                        "CAS mismatch: expected generation {}, found missing record",
                        record.generation,
                    ),
                }
            }
        }
        for record in records {
            let new_generation =
                set_map.get(&record.key).map(|stored| stored.generation + 1).unwrap_or(1);
            set_map.insert(
                record.key,
                StoredMockRecord {
                    generation: new_generation,
                    data_epoch: record.data_epoch,
                    chunk_count: record.chunk_count,
                    data: record.data,
                },
            );
        }
        Ok(())
    }

    pub fn delete(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        let mut sets = self.sets.write();
        if let Some(set_map) = sets.get_mut(set) {
            for key in keys {
                set_map.remove(key);
            }
        }
        Ok(())
    }

    pub fn truncate_set(&self, set: &str) -> anyhow::Result<()> {
        let mut sets = self.sets.write();
        if let Some(set_map) = sets.get_mut(set) {
            set_map.clear();
        }
        Ok(())
    }

    pub fn enumerate(
        &self,
        set: &str,
        on_record: &mut dyn FnMut(AerospikeRecord) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let sets = self.sets.read();
        let Some(set_map) = sets.get(set) else {
            return Ok(());
        };
        for (key, stored) in set_map {
            on_record(AerospikeRecord {
                key: key.clone(),
                generation: stored.generation,
                data_epoch: stored.data_epoch,
                chunk_count: stored.chunk_count,
                data: stored.data.clone(),
            })?;
        }
        Ok(())
    }
}
