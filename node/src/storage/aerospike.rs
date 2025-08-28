use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use aerospike::errors::ErrorKind;
use aerospike::BatchPolicy;
use aerospike::BatchRead;
use aerospike::Bin;
use aerospike::Client;
use aerospike::ClientPolicy;
use aerospike::Error;
use aerospike::Key;
use aerospike::ReadPolicy;
use aerospike::ResultCode;
use aerospike::Value;
use aerospike::WritePolicy;

use crate::helper::metrics::BlockProductionMetrics;

// Max Aerospike records in LRU cache
pub const DEFAULT_AEROSPIKE_MESSAGE_CACHE_MAX_ENTRIES: usize = 10_000;

pub const BIN_HASH: &str = "hash";
pub const BIN_BLOB: &str = "blob";
pub const BIN_SEQ: &str = "seq";
pub const NAMESPACE: &str = "node";

const WRITE_RETRY_MILLIS: u64 = 5;

pub type BinMap = HashMap<String, Value>;

pub trait KeyValueStore: Send + Sync {
    fn get(&self, key: &Key, bins: &[&str], label: &'static str) -> anyhow::Result<Option<BinMap>>;
    fn put(
        &self,
        key: &Key,
        bins: &[Bin],
        until_success: bool,
        label: &'static str,
    ) -> anyhow::Result<()>;
    fn batch_get(&self, reads: Vec<BatchRead>) -> anyhow::Result<Vec<Option<BinMap>>>;
}

#[derive(Clone)]
pub struct NoopStore {}
impl KeyValueStore for NoopStore {
    fn get(
        &self,
        _key: &Key,
        _bins: &[&str],
        _label: &'static str,
    ) -> anyhow::Result<Option<BinMap>> {
        Ok(None)
    }

    fn put(
        &self,
        _key: &Key,
        _bins: &[Bin],
        _until: bool,
        _label: &'static str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn batch_get(&self, _reads: Vec<BatchRead>) -> anyhow::Result<Vec<Option<BinMap>>> {
        Ok(vec![None])
    }
}

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
}

impl AerospikeStore {
    pub fn new(
        socket_address: String,
        metrics: Option<BlockProductionMetrics>,
    ) -> anyhow::Result<Self> {
        let cpolicy = ClientPolicy::default();
        let client = Client::new(&cpolicy, &socket_address)
            .map_err(|err| anyhow::anyhow!("Failed to connect to Aerospike: {err}"))?;
        Ok(Self {
            client: Arc::new(client),
            rpolicy: ReadPolicy::default(),
            wpolicy: WritePolicy::default(),
            bpolicy: BatchPolicy::default(),
            metrics,
        })
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
    fn get(&self, key: &Key, bins: &[&str], label: &'static str) -> anyhow::Result<Option<BinMap>> {
        let moment = Instant::now();
        match self.client.get(&self.rpolicy, key, bins) {
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
        unil_success: bool,
        label: &'static str,
    ) -> anyhow::Result<()> {
        if unil_success {
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

    fn batch_get(&self, reads: Vec<BatchRead>) -> anyhow::Result<Vec<Option<BinMap>>> {
        self.client
            .batch_get(&self.bpolicy, reads)
            .map_err(|e| anyhow::anyhow!("Batch get failed: {e}"))
            .map(|results| results.into_iter().map(|r| r.record.map(|rec| rec.bins)).collect())
    }
}
