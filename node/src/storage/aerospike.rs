#[cfg(debug_assertions)]
use std::sync::atomic::AtomicUsize;
#[cfg(debug_assertions)]
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use aerospike::errors::ErrorKind;
use aerospike::BatchPolicy;
use aerospike::BatchRead;
use aerospike::Bin;
use aerospike::Bins;
use aerospike::Client;
use aerospike::ClientPolicy;
use aerospike::Error;
use aerospike::Key;
use aerospike::ReadPolicy;
use aerospike::ResultCode;
use aerospike::WritePolicy;

use crate::helper::metrics::BlockProductionMetrics;
use crate::storage::store::KeyValueStore;
use crate::storage::store::ValueMap;
use crate::storage::SplitValueStore;

// Max Aerospike records in LRU cache
pub const DEFAULT_AEROSPIKE_MESSAGE_CACHE_MAX_ENTRIES: usize = 10_000;

pub const BIN_HASH: &str = "hash";
pub const BIN_BLOB: &str = "blob";
pub const BIN_SEQ: &str = "seq";

pub const NAMESPACE: &str = "node";
const WRITE_RETRY_MILLIS: u64 = 5;
// 1MB - 16K (names and headers)

// ============================
// AerospikeStore
// ============================
#[derive(Clone)]
pub struct AerospikeStore {
    client: Arc<Client>,
    rpolicy: ReadPolicy,
    wpolicy: WritePolicy,
    bpolicy: BatchPolicy,
    metrics: Option<BlockProductionMetrics>,
    #[cfg(debug_assertions)]
    stats: Arc<Stats>,
}

impl AerospikeStore {
    pub fn new(
        socket_address: String,
        metrics: Option<BlockProductionMetrics>,
    ) -> anyhow::Result<SplitValueStore<AerospikeStore>> {
        let cpolicy = ClientPolicy::default();
        let client = Client::new(&cpolicy, &socket_address)
            .map_err(|err| anyhow::anyhow!("Failed to connect to Aerospike: {err}"))?;
        Ok(SplitValueStore::new(
            Self {
                client: Arc::new(client),
                rpolicy: ReadPolicy::default(),
                wpolicy: WritePolicy::default(),
                bpolicy: BatchPolicy::default(),
                metrics,
                #[cfg(debug_assertions)]
                stats: Arc::new(Stats::new()),
            },
            1024 * 1020, // 1MB - 16KB (reserved for names and headers)
        ))
    }

    fn put_until_success(&self, key: &Key, bins: &[Bin], object_type: &'static str) {
        let moment = Instant::now();
        let mut first_try = true;
        loop {
            match self.client.put(&self.wpolicy, key, bins) {
                Ok(_) => {
                    if first_try {
                        if let Some(m) = &self.metrics {
                            m.report_aerospike_write(
                                moment.elapsed().as_micros() as f64,
                                object_type,
                            );
                        }
                    }
                    break;
                }
                Err(err) => {
                    log::error!("Write will be retried: {err:?}");
                    if let Some(m) = &self.metrics {
                        m.report_aerospike_write_err(object_type);
                    }
                    std::thread::sleep(Duration::from_millis(WRITE_RETRY_MILLIS));
                    first_try = false;
                }
            }
        }
    }
}

impl KeyValueStore for AerospikeStore {
    fn get(
        &self,
        key: &Key,
        values: &Bins,
        label: &'static str,
    ) -> anyhow::Result<Option<ValueMap>> {
        #[cfg(debug_assertions)]
        self.stats.inc_reads(1);

        let moment = Instant::now();
        match self.client.get(&self.rpolicy, key, values.clone()) {
            Ok(record) => {
                if let Some(m) = &self.metrics {
                    m.report_aerospike_read(moment.elapsed().as_micros() as f64, label);
                }
                Ok(Some(record.bins))
            }
            Err(Error(ErrorKind::ServerError(ResultCode::KeyNotFoundError), _)) => Ok(None),
            Err(err) => {
                if let Some(m) = &self.metrics {
                    m.report_aerospike_read_err(label);
                }
                Err(anyhow::anyhow!("Aerospike get failed: {err}"))
            }
        }
    }

    fn put(
        &self,
        key: &Key,
        bins: &[Bin],
        until_success: bool,
        label: &'static str,
    ) -> anyhow::Result<()> {
        #[cfg(debug_assertions)]
        self.stats.inc_writes();

        if until_success {
            self.put_until_success(key, bins, label);
        } else {
            let moment = Instant::now();
            self.client.put(&self.wpolicy, key, bins).map_err(|e| {
                if let Some(m) = &self.metrics {
                    m.report_aerospike_write_err(label);
                }
                anyhow::anyhow!("Aerospike put failed: {e}")
            })?;
            if let Some(m) = &self.metrics {
                m.report_aerospike_write(moment.elapsed().as_micros() as f64, label);
            }
        }
        Ok(())
    }

    fn batch_get(
        &self,
        gets: Vec<(Key, Bins)>,
        _label: &'static str,
    ) -> anyhow::Result<Vec<Option<ValueMap>>> {
        let reads: Vec<BatchRead<'_>> =
            gets.iter().map(|(key, bins)| BatchRead::new(key.clone(), bins)).collect();

        #[cfg(debug_assertions)]
        self.stats.inc_reads(reads.len());

        self.client
            .batch_get(&self.bpolicy, reads)
            .map_err(|e| anyhow::anyhow!("Batch get failed: {e}"))
            .map(|results| results.into_iter().map(|r| r.record.map(|rec| rec.bins)).collect())
    }

    #[cfg(debug_assertions)]
    fn db_reads(&self) -> usize {
        self.stats.db_reads()
    }

    #[cfg(debug_assertions)]
    fn db_writes(&self) -> usize {
        self.stats.db_writes()
    }
}

// DB reads/writes stats (needed for tests)
#[cfg(debug_assertions)]
pub struct Stats {
    db_reads: AtomicUsize,
    db_writes: AtomicUsize,
}
#[cfg(debug_assertions)]
impl Stats {
    pub fn new() -> Self {
        Self { db_reads: AtomicUsize::new(0), db_writes: AtomicUsize::new(0) }
    }

    pub fn inc_reads(&self, val: usize) {
        self.db_reads.fetch_add(val, Ordering::Relaxed);
    }

    pub fn inc_writes(&self) {
        self.db_writes.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(debug_assertions)]
    pub fn db_reads(&self) -> usize {
        self.db_reads.load(Ordering::Relaxed)
    }

    #[cfg(debug_assertions)]
    pub fn db_writes(&self) -> usize {
        self.db_writes.load(Ordering::Relaxed)
    }
}
#[cfg(debug_assertions)]
impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}
