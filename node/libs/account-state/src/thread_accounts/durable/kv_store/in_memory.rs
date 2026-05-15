use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use super::KVRecord;

#[derive(Clone)]
struct StoredRecord {
    generation: u64,
    data_epoch: Option<u32>,
    data: Vec<u8>,
}

#[derive(Clone, Default)]
pub struct InMemoryKVStore {
    /// set_name → (key_bytes → StoredRecord)
    sets: Arc<RwLock<HashMap<String, HashMap<Vec<u8>, StoredRecord>>>>,
}

impl InMemoryKVStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<KVRecord>>> {
        let sets = self.sets.read();
        let set_map = sets.get(set);
        Ok(keys
            .iter()
            .map(|key| {
                set_map.and_then(|m| {
                    m.get(key).map(|stored| KVRecord {
                        key: key.clone(),
                        generation: stored.generation,
                        data_epoch: stored.data_epoch,
                        data: stored.data.clone(),
                    })
                })
            })
            .collect())
    }

    pub fn put(&self, set: &str, records: Vec<KVRecord>, cas: bool) -> anyhow::Result<()> {
        let mut sets = self.sets.write();
        let set_map = sets.entry(set.to_string()).or_default();
        for record in records {
            if cas {
                if let Some(existing) = set_map.get(&record.key) {
                    if existing.generation != record.generation {
                        anyhow::bail!(
                            "CAS mismatch: expected generation {}, found {}",
                            record.generation,
                            existing.generation
                        );
                    }
                }
            }
            let new_generation = set_map.get(&record.key).map(|s| s.generation + 1).unwrap_or(1);
            set_map.insert(
                record.key,
                StoredRecord {
                    generation: new_generation,
                    data_epoch: record.data_epoch,
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
        on_record: &mut dyn FnMut(KVRecord) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let sets = self.sets.read();
        let Some(set_map) = sets.get(set) else {
            return Ok(());
        };

        for (key, stored) in set_map.iter() {
            on_record(KVRecord {
                key: key.clone(),
                generation: stored.generation,
                data_epoch: stored.data_epoch,
                data: stored.data.clone(),
            })?;
        }
        Ok(())
    }
}
