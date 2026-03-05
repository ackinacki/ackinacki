use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use aerospike::as_bin;
use aerospike::as_key;
use aerospike::Bins;
use aerospike::Client;
use aerospike::ClientPolicy;
use aerospike::ReadPolicy;
use aerospike::Value;
use aerospike::WritePolicy;
use node_types::AccountHash;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;

use crate::thread_accounts::durable::accounts::AccountsRepository;
use crate::ThreadAccount;

pub const DEFAULT_NUM_WRITE_THREADS: usize = 10;

const FLUSH_INTERVAL: Duration = Duration::from_millis(100);

enum FlushCommand {
    /// Flush all pending writes to Aerospike and send acknowledgment.
    Flush(mpsc::SyncSender<()>),
    /// Stop the background flush thread.
    Stop,
}

struct CacheEntry {
    account: Option<ThreadAccount>,
    generation: u64,
}

struct SharedState {
    cache: parking_lot::RwLock<HashMap<AccountHash, CacheEntry>>,
    pending: parking_lot::Mutex<HashMap<AccountHash, (Option<ThreadAccount>, u64)>>,
    next_generation: AtomicU64,
}

struct FlushWorker {
    client: Arc<Client>,
    namespace: String,
    set: String,
    write_policy: WritePolicy,
    write_pool: Option<Arc<rayon::ThreadPool>>,
    shared: Arc<SharedState>,
}

impl FlushWorker {
    fn run(self, flush_rx: mpsc::Receiver<FlushCommand>) {
        loop {
            match flush_rx.recv_timeout(FLUSH_INTERVAL) {
                Ok(FlushCommand::Flush(ack_tx)) => {
                    self.drain_and_write();
                    let _ = ack_tx.send(());
                }
                Ok(FlushCommand::Stop) => {
                    self.drain_and_write();
                    return;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    self.drain_and_write();
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    self.drain_and_write();
                    return;
                }
            }
        }
    }

    fn drain_and_write(&self) {
        let batch: HashMap<AccountHash, (Option<ThreadAccount>, u64)> = {
            let mut pending = self.shared.pending.lock();
            if pending.is_empty() {
                return;
            }
            std::mem::take(&mut *pending)
        };

        let write_entries: Vec<_> = batch.into_iter().collect();

        // Each entry produces Ok((hash, gen)) on success or Err((hash, account, gen)) on failure.
        let results: Vec<Result<(AccountHash, u64), (AccountHash, Option<ThreadAccount>, u64)>> =
            match &self.write_pool {
                Some(pool) => pool.install(|| {
                    write_entries
                        .into_par_iter()
                        .map(|(hash, (account, gen))| match self.write_single(&hash, &account) {
                            Ok(()) => Ok((hash, gen)),
                            Err(e) => {
                                tracing::error!(
                                    "Failed to write account {} to Aerospike: {}",
                                    hash.to_hex_string(),
                                    e
                                );
                                Err((hash, account, gen))
                            }
                        })
                        .collect()
                }),
                None => write_entries
                    .into_iter()
                    .map(|(hash, (account, gen))| match self.write_single(&hash, &account) {
                        Ok(()) => Ok((hash, gen)),
                        Err(e) => {
                            tracing::error!(
                                "Failed to write account {} to Aerospike: {}",
                                hash.to_hex_string(),
                                e
                            );
                            Err((hash, account, gen))
                        }
                    })
                    .collect(),
            };

        // Evict successfully flushed entries from cache (only if generation still matches).
        {
            let mut cache = self.shared.cache.write();
            for (hash, flushed_gen) in results.iter().flatten() {
                if let Some(entry) = cache.get(hash) {
                    if entry.generation == *flushed_gen {
                        cache.remove(hash);
                    }
                }
            }
        }

        // Re-add failed entries to pending (won't overwrite newer entries).
        {
            let mut pending = self.shared.pending.lock();
            for result in results {
                if let Err((hash, account, gen)) = result {
                    pending.entry(hash).or_insert((account, gen));
                }
            }
        }
    }

    fn write_single(
        &self,
        hash: &AccountHash,
        account: &Option<ThreadAccount>,
    ) -> anyhow::Result<()> {
        let acc_key = as_key!(&self.namespace, &self.set, hash.to_hex_string());
        match account {
            Some(acc) => {
                let vec = bincode::serialize(acc)?;
                let bins = [as_bin!("account_bincode", vec)];
                self.client
                    .put(&self.write_policy, &acc_key, &bins)
                    .map_err(|e| anyhow::format_err!("{:?}", e))
            }
            None => self
                .client
                .delete(&self.write_policy, &acc_key)
                .map(|_| ())
                .map_err(|e| anyhow::format_err!("{:?}", e)),
        }
    }
}

struct AerospikeAccountsStoreInner {
    client: Option<Arc<Client>>,
    namespace: String,
    set: String,
    read_policy: ReadPolicy,
    write_policy: WritePolicy,
    shared: Arc<SharedState>,
    flush_tx: mpsc::Sender<FlushCommand>,
    bg_thread: parking_lot::Mutex<Option<thread::JoinHandle<()>>>,
}

impl Drop for AerospikeAccountsStoreInner {
    fn drop(&mut self) {
        let _ = self.flush_tx.send(FlushCommand::Stop);
        if let Some(handle) = self.bg_thread.get_mut().take() {
            let _ = handle.join();
        }
    }
}

#[derive(Clone)]
pub struct AerospikeAccountsStore {
    inner: Arc<AerospikeAccountsStoreInner>,
}

pub struct AerospikeAccountsConfig {
    pub address: String,
    pub namespace: String,
    pub set: String,
    pub num_write_threads: usize,
}

impl AerospikeAccountsStore {
    pub fn new(config: AerospikeAccountsConfig) -> anyhow::Result<Self> {
        tracing::trace!(target: "monit", "Creating Aerospike account repository");
        let client_policy = ClientPolicy::default();
        let client = Client::new(&client_policy, &config.address)
            .map_err(|err| anyhow::anyhow!("Failed to connect to Aerospike: {err}"))?;
        let write_pool = if config.num_write_threads > 0 {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(config.num_write_threads)
                .thread_name(|i| format!("as-account-write-{i}"))
                .start_handler(|_| unsafe {
                    libc::nice(10);
                })
                .build()
                .map_err(|e| anyhow::anyhow!("Failed to build account write pool: {e}"))?;
            Some(Arc::new(pool))
        } else {
            None
        };

        let client_arc = Arc::new(client);
        let shared = Arc::new(SharedState {
            cache: parking_lot::RwLock::new(HashMap::new()),
            pending: parking_lot::Mutex::new(HashMap::new()),
            next_generation: AtomicU64::new(0),
        });

        let (flush_tx, flush_rx) = mpsc::channel();

        let worker = FlushWorker {
            client: Arc::clone(&client_arc),
            namespace: config.namespace.clone(),
            set: config.set.clone(),
            write_policy: WritePolicy::default(),
            write_pool,
            shared: Arc::clone(&shared),
        };

        let bg_handle = thread::Builder::new()
            .name("as-account-flush".to_string())
            .spawn(move || {
                worker.run(flush_rx);
            })
            .map_err(|e| anyhow::anyhow!("Failed to spawn flush thread: {e}"))?;

        let inner = Arc::new(AerospikeAccountsStoreInner {
            client: Some(client_arc),
            namespace: config.namespace,
            set: config.set,
            read_policy: ReadPolicy::default(),
            write_policy: WritePolicy::default(),
            shared,
            flush_tx,
            bg_thread: parking_lot::Mutex::new(Some(bg_handle)),
        });

        Ok(Self { inner })
    }

    pub fn client(&self) -> &Option<Arc<Client>> {
        &self.inner.client
    }

    pub(crate) fn cache_len(&self) -> usize {
        self.inner.shared.cache.read().len()
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.inner.shared.pending.lock().len()
    }

    fn account_key(&self, hash: &AccountHash) -> aerospike::Key {
        as_key!(&self.inner.namespace, &self.inner.set, hash.to_hex_string())
    }

    fn read_from_aerospike(
        &self,
        acc_id: &AccountHash,
    ) -> Result<Option<ThreadAccount>, anyhow::Error> {
        let read_policy = &self.inner.read_policy;
        let write_policy = &self.inner.write_policy;
        let acc_key = self.account_key(acc_id);
        let client = match &self.inner.client {
            Some(c) => c,
            None => return Err(anyhow::format_err!("Aerospike client not configured")),
        };
        if !match client.exists(write_policy, &acc_key) {
            Ok(b) => b,
            Err(e) => return Err(anyhow::format_err!("{:?}", e)),
        } {
            return Ok(None);
        }
        let bins = match client.get(read_policy, &acc_key, Bins::All) {
            Ok(k) => k.bins,
            Err(e) => return Err(anyhow::format_err!("{:?}", e)),
        };
        let vec = value_to_vec_u8(bins.get("account_bincode").cloned())?;

        let account: ThreadAccount = bincode::deserialize(&vec)?;
        Ok(Some(account))
    }
}

impl AccountsRepository for AerospikeAccountsStore {
    fn get(&self, hash: &AccountHash) -> anyhow::Result<Option<ThreadAccount>> {
        // Check cache first
        {
            let cache = self.inner.shared.cache.read();
            if let Some(entry) = cache.get(hash) {
                return Ok(entry.account.clone());
            }
        }
        // Cache miss — read from Aerospike
        self.read_from_aerospike(hash)
    }

    fn iter_rx(
        &self,
        hashes: impl IntoIterator<Item = AccountHash> + Send,
    ) -> std::sync::mpsc::Receiver<anyhow::Result<ThreadAccount>> {
        let (read_tx, read_rx) = std::sync::mpsc::sync_channel(1000);
        let store = self.clone();
        let hashes = hashes.into_iter().collect::<Vec<_>>();
        std::thread::Builder::new()
            .name("account-repository-iter".to_string())
            .spawn(move || {
                for hash in hashes {
                    let cached = {
                        let cache = store.inner.shared.cache.read();
                        cache.get(&hash).map(|e| e.account.clone())
                    };
                    match cached {
                        Some(Some(account)) => {
                            if read_tx.send(Ok(account)).is_err() {
                                break;
                            }
                        }
                        Some(None) => {
                            // Deleted in cache — skip
                        }
                        None => {
                            // Cache miss — read from Aerospike
                            if let Some(result) = store.read_from_aerospike(&hash).transpose() {
                                if read_tx.send(result).is_err() {
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .map_err(|err| {
                anyhow::anyhow!("Failed to spawn account repository iter thread: {}", err)
            })
            .expect("Failed to spawn account repository iter thread");
        read_rx
    }

    fn update(&self, accounts: &[(AccountHash, Option<ThreadAccount>)]) -> anyhow::Result<()> {
        let mut cache = self.inner.shared.cache.write();
        let mut pending = self.inner.shared.pending.lock();
        for (hash, account) in accounts {
            let gen = self.inner.shared.next_generation.fetch_add(1, Ordering::Relaxed);
            cache.insert(*hash, CacheEntry { account: account.clone(), generation: gen });
            pending.insert(*hash, (account.clone(), gen));
        }
        Ok(())
    }

    fn commit(&self) -> anyhow::Result<()> {
        let (ack_tx, ack_rx) = mpsc::sync_channel(1);
        self.inner
            .flush_tx
            .send(FlushCommand::Flush(ack_tx))
            .map_err(|_| anyhow::anyhow!("Flush thread not running"))?;
        ack_rx.recv().map_err(|_| anyhow::anyhow!("Flush thread did not acknowledge"))?;
        Ok(())
    }
}

fn value_to_vec_u8(opt_val: Option<Value>) -> anyhow::Result<Vec<u8>> {
    match opt_val {
        Some(val) => match val {
            Value::Blob(items) => Ok(items),
            Value::List(values) => vec_val_to_vec_u8(values),
            Value::HLL(items) => Ok(items),
            _ => Err(anyhow::format_err!("Invalid data format found in aerospike")),
        },
        None => Ok(vec![]),
    }
}

pub fn vec_val_to_vec_u8(values: Vec<Value>) -> anyhow::Result<Vec<u8>> {
    match values.first() {
        Some(first) => match first {
            Value::Int(_i) => Ok(values
                .into_iter()
                .map(|v| {
                    u8::try_from(match v {
                        Value::Int(k) => k,
                        _ => unreachable!(),
                    })
                    .unwrap()
                })
                .collect()),
            Value::UInt(_u) => Ok(values
                .into_iter()
                .map(|v| {
                    u8::try_from(match v {
                        Value::UInt(k) => k,
                        _ => unreachable!(),
                    })
                    .unwrap()
                })
                .collect()),
            _ => Err(anyhow::format_err!("Invalid data format found in aerospike")),
        },
        None => Ok(vec![]),
    }
}
