use std::sync::Arc;

use aerospike::BatchRead;
use aerospike::Bin;
use aerospike::Key;
use cached::Cached;
use cached::SizedCache;
use parking_lot::Mutex;

use crate::storage::BinMap;
use crate::storage::KeyValueStore;

pub trait Cache: Send + Sync {
    fn get(&self, key: &Key) -> Option<BinMap>;
    fn put(&self, key: &Key, bins: BinMap);
    fn invalidate(&self, key: &Key);
}

// ============================
// LruSizedCache
// ============================
#[derive(Clone)]

pub struct LruSizedCache {
    cache: Arc<Mutex<SizedCache<String, BinMap>>>,
}

impl LruSizedCache {
    pub fn new(size: usize) -> Self {
        Self { cache: Arc::new(Mutex::new(SizedCache::with_size(size))) }
    }
}
impl Cache for LruSizedCache {
    fn get(&self, key: &Key) -> Option<BinMap> {
        self.cache.lock().cache_get(&key.to_string()).cloned()
    }

    fn put(&self, key: &Key, bins: BinMap) {
        self.cache.lock().cache_set(key.to_string(), bins);
    }

    fn invalidate(&self, key: &Key) {
        self.cache.lock().cache_remove(&key.to_string());
    }
}

// ============================
// CachedStore
// ============================

fn bins_to_map(bins: &[Bin<'_>]) -> BinMap {
    bins.iter().map(|bin| (bin.name.to_string(), bin.value.clone())).collect()
}
#[derive(Clone)]
pub struct CachedStore<B: KeyValueStore, C: Cache> {
    db: B,
    cache: C,
}

impl<B: KeyValueStore, C: Cache> CachedStore<B, C> {
    pub fn new(db: B, cache: C) -> Self {
        Self { db, cache }
    }
}

impl<B: KeyValueStore, C: Cache> KeyValueStore for CachedStore<B, C> {
    fn get(&self, key: &Key, bins: &[&str], label: &'static str) -> anyhow::Result<Option<BinMap>> {
        if let Some(v) = self.cache.get(key) {
            // tracing::trace!("cache hit: {key}");
            return Ok(Some(v));
        }
        if let Some(v) = self.db.get(key, bins, label)? {
            self.cache.put(key, v.clone());
            return Ok(Some(v));
        }
        Ok(None)
    }

    fn put(
        &self,
        key: &Key,
        bins: &[Bin],
        until_success: bool,
        label: &'static str,
    ) -> anyhow::Result<()> {
        self.db.put(key, bins, until_success, label)?;
        self.cache.put(key, bins_to_map(bins));
        Ok(())
    }

    fn batch_get(&self, reads: Vec<BatchRead>) -> anyhow::Result<Vec<Option<BinMap>>> {
        let mut out: Vec<Option<BinMap>> = vec![None; reads.len()];
        let mut out_idx: Vec<usize> = Vec::new();
        let mut db_reads: Vec<BatchRead> = Vec::new();
        let mut keys_for_cache: Vec<Key> = Vec::new();

        for (i, r) in reads.into_iter().enumerate() {
            let k = r.key.clone();
            if let Some(v) = self.cache.get(&k) {
                out[i] = Some(v);
            } else {
                db_reads.push(r);
                // remember the index in the output vector where we will insert the data obtained from DB
                out_idx.push(i);
                // remember the key for data that will be saved in cache
                keys_for_cache.push(k);
            }
        }

        if !db_reads.is_empty() {
            let results = self.db.batch_get(db_reads)?;
            for (j, res) in results.into_iter().enumerate() {
                if let Some(map) = res {
                    out[out_idx[j]] = Some(map.clone());
                    self.cache.put(&keys_for_cache[j], map);
                }
            }
        }
        Ok(out)
    }
}
