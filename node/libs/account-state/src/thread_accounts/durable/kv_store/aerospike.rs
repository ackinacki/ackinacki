use std::collections::HashMap;
use std::time::Instant;

use aerospike2::Aerospike2Backend;
use aerospike2::Aerospike2BackendConfig;
use aerospike2::AerospikeRecord;

use super::aerospike_mock::MockAerospikeBackend;
use super::aerospike_v1::Aerospike1Backend;
use super::KVRecord;
use crate::StateAccountsMetrics;

const CHUNK_DATA_LIMIT: usize = 1024 * 992;

#[derive(Debug, Default)]
pub struct AerospikeAccountsCacheStat {
    pub cache_len: usize,
    pub pending_len: usize,
}

#[derive(Clone)]
pub struct AerospikeKVConfig {
    pub address: String,
    pub namespace: String,
    pub num_write_threads: usize,
    pub metrics: Option<StateAccountsMetrics>,
}

#[derive(Clone)]
pub enum AerospikeBackend {
    V1(Aerospike1Backend),
    V2(Box<Aerospike2Backend>),
    Mock(MockAerospikeBackend),
}

impl AerospikeBackend {
    fn get(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<AerospikeRecord>>> {
        match self {
            Self::V1(backend) => backend.get(set, keys),
            Self::V2(backend) => backend.get(set, keys),
            Self::Mock(backend) => backend.get(set, keys),
        }
    }

    fn put(&self, set: &str, records: Vec<AerospikeRecord>, cas: bool) -> anyhow::Result<()> {
        debug_assert_unique_raw_keys(&records);
        match self {
            Self::V1(backend) => backend.put(set, records, cas),
            Self::V2(backend) => backend.put(set, records, cas),
            Self::Mock(backend) => backend.put(set, records, cas),
        }
    }

    fn delete(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        match self {
            Self::V1(backend) => backend.delete(set, keys),
            Self::V2(backend) => backend.delete(set, keys),
            Self::Mock(backend) => backend.delete(set, keys),
        }
    }

    /// Server-side wipe of every record in `set`. Implementations may use
    /// constant-time mechanisms (Aerospike `truncate` info command) where
    /// available; mock/in-memory implementations clear the underlying map.
    fn truncate_set(&self, set: &str) -> anyhow::Result<()> {
        match self {
            Self::V1(backend) => backend.truncate_set(set),
            Self::V2(backend) => backend.truncate_set(set),
            Self::Mock(backend) => backend.truncate_set(set),
        }
    }

    fn enumerate(
        &self,
        set: &str,
        on_record: &mut dyn FnMut(AerospikeRecord) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        match self {
            Self::V1(backend) => backend.enumerate(set, on_record),
            Self::V2(backend) => backend.enumerate(set, on_record),
            Self::Mock(backend) => backend.enumerate(set, on_record),
        }
    }
}

#[derive(Clone)]
pub struct AerospikeKVStore {
    backend: AerospikeBackend,
    metrics: Option<StateAccountsMetrics>,
}

impl AerospikeKVStore {
    pub fn new(config: AerospikeKVConfig) -> anyhow::Result<Self> {
        Self::new_v1(config)
    }

    pub fn new_v1(config: AerospikeKVConfig) -> anyhow::Result<Self> {
        Ok(Self {
            backend: AerospikeBackend::V1(Aerospike1Backend::new(config.clone())?),
            metrics: config.metrics,
        })
    }

    pub fn new_v2(config: AerospikeKVConfig) -> anyhow::Result<Self> {
        Ok(Self {
            backend: AerospikeBackend::V2(Box::new(Aerospike2Backend::new(
                Aerospike2BackendConfig { address: config.address, namespace: config.namespace },
            )?)),
            metrics: config.metrics,
        })
    }

    pub fn new_mock() -> Self {
        Self { backend: AerospikeBackend::Mock(MockAerospikeBackend::new()), metrics: None }
    }

    pub fn with_backend(backend: AerospikeBackend, metrics: Option<StateAccountsMetrics>) -> Self {
        Self { backend, metrics }
    }

    pub fn get(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<KVRecord>>> {
        let started = Instant::now();
        let heads = self.backend.get(set, keys)?;
        let mut results = Vec::with_capacity(keys.len());
        for (key, head) in keys.iter().zip(heads) {
            let Some(head) = head else {
                results.push(None);
                continue;
            };
            results.push(Some(self.assemble_logical_record(set, key, head)?));
        }
        if let Some(metrics) = &self.metrics {
            metrics.report_aerospike_read_duration_micros(started.elapsed().as_micros() as u64);
        }
        Ok(results)
    }

    pub fn put(&self, set: &str, records: Vec<KVRecord>, cas: bool) -> anyhow::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        let records_len = records.len();
        let deduped = dedup_keep_last(records);
        let keys = deduped.iter().map(|item| item.record.key.clone()).collect::<Vec<_>>();
        let old_chunk_counts = self
            .backend
            .get(set, &keys)?
            .into_iter()
            .map(|record| record.and_then(|record| record.chunk_count).unwrap_or(1))
            .collect::<Vec<_>>();

        let mut chunk_records = Vec::new();
        let mut head_records = Vec::new();
        let mut new_chunk_counts = HashMap::new();
        for item in deduped {
            let (head, chunks, chunk_count) = split_logical_record(item.record);
            new_chunk_counts.insert(head.key.clone(), chunk_count);
            chunk_records.extend(chunks);
            head_records.push(head);
        }

        let chunk_restore = if cas && !chunk_records.is_empty() {
            let keys = chunk_records.iter().map(|record| record.key.clone()).collect::<Vec<_>>();
            Some((keys.clone(), self.backend.get(set, &keys)?))
        } else {
            None
        };

        if !chunk_records.is_empty() {
            self.backend.put(set, chunk_records, false)?;
        }
        if let Err(err) = self.backend.put(set, head_records, cas) {
            if let Some((keys, previous_chunks)) = chunk_restore {
                let mut restore_records = Vec::new();
                let mut delete_keys = Vec::new();
                for (key, previous) in keys.into_iter().zip(previous_chunks) {
                    match previous {
                        Some(record) => restore_records.push(record),
                        None => delete_keys.push(key),
                    }
                }
                if !restore_records.is_empty() {
                    let _ = self.backend.put(set, restore_records, false);
                }
                if !delete_keys.is_empty() {
                    let _ = self.backend.delete(set, &delete_keys);
                }
            }
            return Err(err);
        }

        let mut stale = Vec::new();
        for (key, old_chunk_count) in keys.iter().zip(old_chunk_counts) {
            let new_chunk_count = *new_chunk_counts.get(key).unwrap_or(&1);
            stale.extend(stale_chunk_keys(key, Some(old_chunk_count), new_chunk_count));
        }
        if !stale.is_empty() {
            self.backend.delete(set, &stale)?;
        }

        tracing::debug!(
            target: "monit",
            "Aerospike wrote {} logical records to {} in {} ms",
            records_len,
            set,
            started.elapsed().as_secs_f64() * 1000.0,
        );
        if let Some(metrics) = &self.metrics {
            metrics
                .report_aerospike_batch_write_duration_micros(started.elapsed().as_micros() as u64);
        }
        Ok(())
    }

    /// Snapshot-import fast path: write fresh records into a set known to be
    /// empty for these keys. Skips the pre-write GET (used to detect old
    /// chunk counts) and the post-write stale-chunk cleanup, since neither
    /// applies on first-time population. Always writes with cas=false.
    pub fn bulk_put_no_overwrite(&self, set: &str, records: Vec<KVRecord>) -> anyhow::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        let records_len = records.len();
        let deduped = dedup_keep_last(records);

        let mut chunk_records = Vec::new();
        let mut head_records = Vec::with_capacity(deduped.len());
        for item in deduped {
            let (head, chunks, _chunk_count) = split_logical_record(item.record);
            chunk_records.extend(chunks);
            head_records.push(head);
        }

        let chunks_len = chunk_records.len();
        let heads_len = head_records.len();
        let chunks_bytes: usize = chunk_records.iter().map(|r| r.data.len()).sum();
        let heads_bytes: usize = head_records.iter().map(|r| r.data.len()).sum();
        let max_head_bytes = head_records.iter().map(|r| r.data.len()).max().unwrap_or(0);
        let max_chunk_bytes = chunk_records.iter().map(|r| r.data.len()).max().unwrap_or(0);
        tracing::debug!(
            target: "monit",
            "bulk_put_no_overwrite set={set} logical={records_len} chunks={chunks_len}/{chunks_bytes}B(max {max_chunk_bytes}) heads={heads_len}/{heads_bytes}B(max {max_head_bytes})",
        );

        if !chunk_records.is_empty() {
            let t = Instant::now();
            self.backend.put(set, chunk_records, false)?;
            tracing::debug!(
                target: "monit",
                "bulk_put_no_overwrite set={set} chunks PUT done in {} ms",
                t.elapsed().as_secs_f64() * 1000.0,
            );
        }
        let t = Instant::now();
        self.backend.put(set, head_records, false)?;
        tracing::debug!(
            target: "monit",
            "bulk_put_no_overwrite set={set} heads PUT done in {} ms",
            t.elapsed().as_secs_f64() * 1000.0,
        );

        tracing::debug!(
            target: "monit",
            "Aerospike bulk_put_no_overwrite wrote {} logical records to {} in {} ms",
            records_len,
            set,
            started.elapsed().as_secs_f64() * 1000.0,
        );
        if let Some(metrics) = &self.metrics {
            metrics
                .report_aerospike_batch_write_duration_micros(started.elapsed().as_micros() as u64);
        }
        Ok(())
    }

    /// Server-side wipe of every record in `set` (head records and any
    /// chunk records associated with them). Use this before re-populating
    /// a set from a snapshot, so stale logical records from a prior epoch
    /// can never be returned by `read_account_operation`.
    pub fn truncate_set(&self, set: &str) -> anyhow::Result<()> {
        self.backend.truncate_set(set)
    }

    pub fn delete(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        let heads = self.backend.get(set, keys)?;
        let mut delete_keys = Vec::new();
        for (key, head) in keys.iter().zip(heads) {
            delete_keys.push(key.clone());
            let chunk_count = head.and_then(|record| record.chunk_count).unwrap_or(1);
            for idx in 1..chunk_count {
                delete_keys.push(chunk_key_bytes(key, idx));
            }
        }
        self.backend.delete(set, &delete_keys)
    }

    pub fn enumerate(
        &self,
        set: &str,
        on_record: &mut dyn FnMut(KVRecord) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.enumerate_checked(set, &|| false, on_record)
    }

    pub fn enumerate_checked(
        &self,
        set: &str,
        should_cancel: &dyn Fn() -> bool,
        on_record: &mut dyn FnMut(KVRecord) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        self.backend.enumerate(set, &mut |head| {
            if should_cancel() {
                anyhow::bail!("snapshot save cancelled");
            }
            if parse_chunk_key(&head.key).is_some() {
                return Ok(());
            }
            let key = head.key.clone();
            on_record(self.assemble_logical_record_checked(set, &key, head, should_cancel)?)
        })
    }

    fn assemble_logical_record(
        &self,
        set: &str,
        key: &[u8],
        head: AerospikeRecord,
    ) -> anyhow::Result<KVRecord> {
        self.assemble_logical_record_checked(set, key, head, &|| false)
    }

    fn assemble_logical_record_checked(
        &self,
        set: &str,
        key: &[u8],
        head: AerospikeRecord,
        should_cancel: &dyn Fn() -> bool,
    ) -> anyhow::Result<KVRecord> {
        let chunk_count = head.chunk_count.unwrap_or(1);
        anyhow::ensure!(chunk_count > 0, "Chunk count must be positive");
        let mut data = head.data;
        if chunk_count > 1 {
            let chunk_keys =
                (1..chunk_count).map(|idx| chunk_key_bytes(key, idx)).collect::<Vec<_>>();
            let chunks = self.backend.get(set, &chunk_keys)?;
            for (idx, chunk) in chunks.into_iter().enumerate() {
                if should_cancel() {
                    anyhow::bail!("snapshot save cancelled");
                }
                let chunk = chunk.ok_or_else(|| {
                    anyhow::anyhow!(
                        "Missing Aerospike chunk: set={set}, key={}, chunk_index={}, chunk_count={}",
                        hex::encode(key),
                        idx + 1,
                        chunk_count,
                    )
                })?;
                data.extend(chunk.data);
            }
        }
        Ok(KVRecord {
            key: key.to_vec(),
            generation: head.generation,
            data_epoch: head.data_epoch,
            data,
        })
    }
}

#[derive(Debug)]
struct IndexedLogicalItem {
    record: KVRecord,
}

fn split_logical_record(record: KVRecord) -> (AerospikeRecord, Vec<AerospikeRecord>, usize) {
    let chunk_count = record.data.len().div_ceil(CHUNK_DATA_LIMIT).max(1);
    let head_end = CHUNK_DATA_LIMIT.min(record.data.len());
    let head_data = record.data[..head_end].to_vec();
    let mut chunks = Vec::new();
    for idx in 1..chunk_count {
        let start = idx * CHUNK_DATA_LIMIT;
        let end = (start + CHUNK_DATA_LIMIT).min(record.data.len());
        chunks.push(chunk_record(
            chunk_key_bytes(&record.key, idx),
            record.data[start..end].to_vec(),
        ));
    }
    (logical_head_record(record, chunk_count, head_data), chunks, chunk_count)
}

fn logical_head_record(record: KVRecord, chunk_count: usize, data: Vec<u8>) -> AerospikeRecord {
    AerospikeRecord {
        key: record.key,
        generation: record.generation,
        data_epoch: record.data_epoch,
        chunk_count: (chunk_count > 1).then_some(chunk_count),
        data,
    }
}

fn chunk_record(key: Vec<u8>, data: Vec<u8>) -> AerospikeRecord {
    AerospikeRecord { key, generation: 0, data_epoch: None, chunk_count: None, data }
}

fn chunk_key_bytes(base: &[u8], idx: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(6 + base.len());
    out.extend_from_slice(&[0xFF, 0xFF]);
    out.extend_from_slice(&(idx as u32).to_be_bytes());
    out.extend_from_slice(base);
    out
}

fn parse_chunk_key(key: &[u8]) -> Option<(usize, &[u8])> {
    if key.len() < 6 || key[..2] != [0xFF, 0xFF] {
        return None;
    }
    let idx = u32::from_be_bytes(key[2..6].try_into().ok()?) as usize;
    if idx == 0 {
        return None;
    }
    Some((idx, &key[6..]))
}

fn stale_chunk_keys(
    base_key: &[u8],
    old_chunk_count: Option<usize>,
    new_chunk_count: usize,
) -> Vec<Vec<u8>> {
    let Some(old_chunk_count) = old_chunk_count else {
        return Vec::new();
    };
    if old_chunk_count <= new_chunk_count {
        return Vec::new();
    }
    (new_chunk_count..old_chunk_count).map(|idx| chunk_key_bytes(base_key, idx)).collect()
}

fn dedup_keep_last(batch: Vec<KVRecord>) -> Vec<IndexedLogicalItem> {
    let mut map: HashMap<Vec<u8>, IndexedLogicalItem> = HashMap::with_capacity(batch.len());
    for record in batch {
        map.insert(record.key.clone(), IndexedLogicalItem { record });
    }
    map.into_values().collect()
}

#[allow(unused)]
fn debug_assert_unique_raw_keys(records: &[AerospikeRecord]) {
    #[cfg(debug_assertions)]
    {
        let mut seen = std::collections::HashSet::with_capacity(records.len());
        for record in records {
            debug_assert!(
                seen.insert(record.key.as_slice()),
                "duplicate raw Aerospike key in backend put: {}",
                hex::encode(&record.key),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(seed: u8, size: usize) -> KVRecord {
        KVRecord {
            key: vec![seed; 8],
            generation: 0,
            data_epoch: Some(seed as u32),
            data: (0..size).map(|idx| seed.wrapping_add(idx as u8)).collect(),
        }
    }

    #[test]
    fn mock_backend_round_trips_mixed_small_and_chunked_records() {
        let store = AerospikeKVStore::new_mock();
        let set = "mock-mixed";
        let small = record(1, 128);
        let large = record(2, CHUNK_DATA_LIMIT + 17);
        store.put(set, vec![small.clone(), large.clone()], false).unwrap();

        let loaded = store.get(set, &[small.key.clone(), large.key.clone()]).unwrap();
        assert_record_body_eq(loaded[0].as_ref().unwrap(), &small);
        assert_record_body_eq(loaded[1].as_ref().unwrap(), &large);

        let mut enumerated = HashMap::new();
        store
            .enumerate(set, &mut |record| {
                enumerated.insert(record.key.clone(), record);
                Ok(())
            })
            .unwrap();
        assert_record_body_eq(enumerated.get(&small.key).unwrap(), &small);
        assert_record_body_eq(enumerated.get(&large.key).unwrap(), &large);
        assert_eq!(enumerated.len(), 2);
    }

    #[test]
    fn mock_backend_large_to_small_overwrite_removes_stale_chunks() {
        let store = AerospikeKVStore::new_mock();
        let set = "mock-overwrite";
        let large = record(3, CHUNK_DATA_LIMIT * 2 + 11);
        store.put(set, vec![large.clone()], false).unwrap();

        let current = store.get(set, std::slice::from_ref(&large.key)).unwrap().remove(0).unwrap();
        let mut small = record(3, 32);
        small.generation = current.generation;
        store.put(set, vec![small.clone()], true).unwrap();
        assert_eq!(
            store.get(set, std::slice::from_ref(&small.key)).unwrap().remove(0),
            Some(KVRecord { generation: small.generation + 1, ..small.clone() })
        );

        let mut physical_count = 0;
        if let AerospikeBackend::Mock(mock) = &store.backend {
            mock.enumerate(set, &mut |_| {
                physical_count += 1;
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(physical_count, 1);
    }

    #[test]
    fn mock_backend_small_to_large_overwrite_adds_chunks() {
        let store = AerospikeKVStore::new_mock();
        let set = "mock-small-to-large";
        let small = record(6, 32);
        store.put(set, vec![small.clone()], false).unwrap();

        let current = store.get(set, std::slice::from_ref(&small.key)).unwrap().remove(0).unwrap();
        let mut large = record(6, CHUNK_DATA_LIMIT + 33);
        large.generation = current.generation;
        store.put(set, vec![large.clone()], true).unwrap();

        let loaded = store.get(set, std::slice::from_ref(&large.key)).unwrap().remove(0).unwrap();
        assert_record_body_eq(&loaded, &large);
        assert_eq!(physical_record_count(&store, set), 2);
    }

    #[test]
    fn mock_backend_large_to_larger_overwrite_updates_and_adds_chunks() {
        let store = AerospikeKVStore::new_mock();
        let set = "mock-large-to-larger";
        let large = record(7, CHUNK_DATA_LIMIT + 11);
        store.put(set, vec![large.clone()], false).unwrap();

        let current = store.get(set, std::slice::from_ref(&large.key)).unwrap().remove(0).unwrap();
        let mut larger = record(7, CHUNK_DATA_LIMIT * 2 + 19);
        larger.generation = current.generation;
        store.put(set, vec![larger.clone()], true).unwrap();

        let loaded = store.get(set, std::slice::from_ref(&larger.key)).unwrap().remove(0).unwrap();
        assert_record_body_eq(&loaded, &larger);
        assert_eq!(physical_record_count(&store, set), 3);
    }

    #[test]
    fn mock_backend_large_to_same_chunk_count_overwrite_updates_chunks() {
        let store = AerospikeKVStore::new_mock();
        let set = "mock-large-same-count";
        let large = record(8, CHUNK_DATA_LIMIT + 11);
        store.put(set, vec![large.clone()], false).unwrap();

        let current = store.get(set, std::slice::from_ref(&large.key)).unwrap().remove(0).unwrap();
        let mut updated = record(9, CHUNK_DATA_LIMIT + 27);
        updated.key = large.key.clone();
        updated.generation = current.generation;
        store.put(set, vec![updated.clone()], true).unwrap();

        let loaded = store.get(set, std::slice::from_ref(&updated.key)).unwrap().remove(0).unwrap();
        assert_record_body_eq(&loaded, &updated);
        assert_eq!(physical_record_count(&store, set), 2);
    }

    #[test]
    fn mock_backend_stale_cas_fails() {
        let store = AerospikeKVStore::new_mock();
        let set = "mock-cas";
        let initial = record(4, 64);
        store.put(set, vec![initial.clone()], false).unwrap();
        let loaded = store.get(set, std::slice::from_ref(&initial.key)).unwrap().remove(0).unwrap();
        let stale = KVRecord { data: vec![9; 64], ..initial };
        assert!(store.put(set, vec![stale], true).is_err());
        assert_eq!(
            store.get(set, std::slice::from_ref(&loaded.key)).unwrap().remove(0),
            Some(loaded)
        );
    }

    #[test]
    fn mock_backend_stale_cas_on_chunked_record_preserves_old_chunks() {
        let store = AerospikeKVStore::new_mock();
        let set = "mock-chunked-cas";
        let initial = record(10, CHUNK_DATA_LIMIT * 2 + 41);
        store.put(set, vec![initial.clone()], false).unwrap();
        let loaded = store.get(set, std::slice::from_ref(&initial.key)).unwrap().remove(0).unwrap();

        let stale = KVRecord { data: vec![99; CHUNK_DATA_LIMIT * 2 + 17], ..initial.clone() };
        assert!(store.put(set, vec![stale], true).is_err());

        let after = store.get(set, std::slice::from_ref(&initial.key)).unwrap().remove(0).unwrap();
        assert_eq!(after, loaded);
        assert_eq!(physical_record_count(&store, set), 3);
    }

    #[test]
    fn mock_backend_delete_removes_head_and_chunks() {
        let store = AerospikeKVStore::new_mock();
        let set = "mock-delete";
        let large = record(5, CHUNK_DATA_LIMIT + 5);
        store.put(set, vec![large.clone()], false).unwrap();
        store.delete(set, std::slice::from_ref(&large.key)).unwrap();
        assert_eq!(store.get(set, std::slice::from_ref(&large.key)).unwrap().remove(0), None);

        let mut physical_count = 0;
        if let AerospikeBackend::Mock(mock) = &store.backend {
            mock.enumerate(set, &mut |_| {
                physical_count += 1;
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(physical_count, 0);
    }

    #[test]
    fn mock_backend_missing_chunk_fails_get_and_enumerate() {
        let store = AerospikeKVStore::new_mock();
        let set = "mock-missing-chunk";
        let large = record(11, CHUNK_DATA_LIMIT + 5);
        store.put(set, vec![large.clone()], false).unwrap();
        if let AerospikeBackend::Mock(mock) = &store.backend {
            mock.delete(set, &[chunk_key_bytes(&large.key, 1)]).unwrap();
        }

        let err = store.get(set, std::slice::from_ref(&large.key)).unwrap_err();
        assert!(err.to_string().contains("Missing Aerospike chunk"), "{err:?}");

        let err = store.enumerate(set, &mut |_| Ok(())).unwrap_err();
        assert!(err.to_string().contains("Missing Aerospike chunk"), "{err:?}");
    }

    #[test]
    fn mock_backend_enumerate_over_chunked_records_returns_logical_records_once() {
        let store = AerospikeKVStore::new_mock();
        let set = "mock-enumerate-chunked";
        let large_a = record(12, CHUNK_DATA_LIMIT + 7);
        let large_b = record(13, CHUNK_DATA_LIMIT * 2 + 9);
        store.put(set, vec![large_a.clone(), large_b.clone()], false).unwrap();

        assert_eq!(physical_record_count(&store, set), 5);

        let mut enumerated = HashMap::new();
        store
            .enumerate(set, &mut |record| {
                assert!(enumerated.insert(record.key.clone(), record).is_none());
                Ok(())
            })
            .unwrap();

        assert_eq!(enumerated.len(), 2);
        assert_record_body_eq(enumerated.get(&large_a.key).unwrap(), &large_a);
        assert_record_body_eq(enumerated.get(&large_b.key).unwrap(), &large_b);
    }

    #[test]
    fn mock_backend_zero_chunk_count_is_rejected() {
        let store = AerospikeKVStore::new_mock();
        let set = "mock-zero-chunk";
        let key = vec![14; 8];
        if let AerospikeBackend::Mock(mock) = &store.backend {
            mock.put(
                set,
                vec![AerospikeRecord {
                    key: key.clone(),
                    generation: 0,
                    data_epoch: None,
                    chunk_count: Some(0),
                    data: Vec::new(),
                }],
                false,
            )
            .unwrap();
        }

        let err = store.get(set, std::slice::from_ref(&key)).unwrap_err();
        assert!(err.to_string().contains("Chunk count must be positive"), "{err:?}");
    }

    #[test]
    fn mock_backend_batch_cas_failure_does_not_commit_other_heads() {
        let store = AerospikeKVStore::new_mock();
        let set = "mock-batch-cas";
        let first = record(15, CHUNK_DATA_LIMIT + 3);
        let second = record(16, CHUNK_DATA_LIMIT + 5);
        store.put(set, vec![first.clone(), second.clone()], false).unwrap();
        let loaded = store.get(set, &[first.key.clone(), second.key.clone()]).unwrap();
        let first_loaded = loaded[0].clone().unwrap();
        let second_loaded = loaded[1].clone().unwrap();

        let mut valid_update = record(15, CHUNK_DATA_LIMIT + 13);
        valid_update.generation = first_loaded.generation;
        let stale_update = KVRecord {
            key: second.key.clone(),
            generation: second_loaded.generation.saturating_sub(1),
            data_epoch: second.data_epoch,
            data: vec![42; CHUNK_DATA_LIMIT + 17],
        };

        assert!(store.put(set, vec![valid_update, stale_update], true).is_err());
        let after = store.get(set, &[first.key.clone(), second.key.clone()]).unwrap();
        assert_eq!(after[0], Some(first_loaded));
        assert_eq!(after[1], Some(second_loaded));
    }

    fn assert_record_body_eq(actual: &KVRecord, expected: &KVRecord) {
        assert_eq!(actual.key, expected.key);
        assert_eq!(actual.data_epoch, expected.data_epoch);
        assert_eq!(actual.data, expected.data);
        assert!(actual.generation > 0);
    }

    fn physical_record_count(store: &AerospikeKVStore, set: &str) -> usize {
        let AerospikeBackend::Mock(mock) = &store.backend else {
            unreachable!("test uses mock backend")
        };
        let mut physical_count = 0;
        mock.enumerate(set, &mut |_| {
            physical_count += 1;
            Ok(())
        })
        .unwrap();
        physical_count
    }
}
