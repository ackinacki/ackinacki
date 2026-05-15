use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use std::time::Duration;

use aerospike::as_bin;
use aerospike::BatchPolicy;
use aerospike::BatchRead;
use aerospike::Bins;
use aerospike::Client;
use aerospike::ClientPolicy;
use aerospike::ErrorKind;
use aerospike::GenerationPolicy;
use aerospike::Key;
use aerospike::ResultCode;
use aerospike::Value;
use aerospike::WritePolicy;
use aerospike2::AerospikeRecord;
use aerospike2::EnumeratedKey;
use aerospike2::EnumeratedValue;

use super::aerospike::AerospikeKVConfig;

const SERVER_MEM_RETRY_COUNT: usize = 5;
const SERVER_MEM_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
const BIN_DATA: &str = "data";
const BIN_EPOCH: &str = "epoch";
const BIN_CHUNK_COUNT: &str = "chunks";
const BATCH_GET_LIMIT: usize = 4096;

#[derive(Clone)]
pub struct Aerospike1Backend {
    inner: Arc<Aerospike1BackendInner>,
}

struct Aerospike1BackendInner {
    client: Arc<Client>,
    address: String,
    namespace: String,
    write_policy: WritePolicy,
    workers: Vec<crossbeam_channel::Sender<WorkerMsg>>,
    joins: Vec<std::thread::JoinHandle<()>>,
}

impl Aerospike1Backend {
    pub fn new(config: AerospikeKVConfig) -> anyhow::Result<Self> {
        let client = Arc::new(
            Client::new(&ClientPolicy::default(), &config.address)
                .map_err(|err| anyhow::anyhow!("Failed to connect to Aerospike: {err}"))?,
        );
        let num_write_threads = config.num_write_threads.max(1);
        let mut workers = Vec::with_capacity(num_write_threads);
        let mut joins = Vec::with_capacity(num_write_threads);

        for _ in 0..num_write_threads {
            let (tx, rx) = crossbeam_channel::bounded::<WorkerMsg>(1000);
            let client = Arc::clone(&client);
            let write_policy = WritePolicy { send_key: true, ..WritePolicy::default() };
            let join = std::thread::Builder::new()
                .name("aerospike-write-thread".to_string())
                .spawn(move || worker_loop(client, write_policy, rx))
                .expect("Failed to spawn Aerospike worker thread");
            workers.push(tx);
            joins.push(join);
        }

        Ok(Self {
            inner: Arc::new(Aerospike1BackendInner {
                client,
                address: config.address,
                namespace: config.namespace,
                write_policy: WritePolicy { send_key: true, ..WritePolicy::default() },
                workers,
                joins,
            }),
        })
    }

    fn make_key(&self, set: &str, key_bytes: &[u8]) -> Key {
        make_key(&self.inner.namespace, set, key_bytes)
    }

    pub fn get(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<AerospikeRecord>>> {
        let bins = Bins::All;
        let mut results = Vec::with_capacity(keys.len());
        for key_batch in keys.chunks(BATCH_GET_LIMIT) {
            let batch_reads = key_batch
                .iter()
                .map(|key_bytes| BatchRead::new(self.make_key(set, key_bytes), &bins))
                .collect::<Vec<_>>();
            let records =
                self.inner.client.batch_get(&BatchPolicy::default(), batch_reads).map_err(
                    |err| {
                        anyhow::anyhow!(
                            "Aerospike batch_get failed: set={set}, keys_count={}, err={err}",
                            key_batch.len(),
                        )
                    },
                )?;

            anyhow::ensure!(
                records.len() == key_batch.len(),
                "Aerospike batch_get returned unexpected count: set={set}, expected={}, actual={}",
                key_batch.len(),
                records.len(),
            );

            for (batch_read, key_bytes) in records.into_iter().zip(key_batch) {
                let record = batch_read
                    .record
                    .map(|mut record| {
                        record_from_bins(
                            key_bytes.clone(),
                            record.generation as u64,
                            &mut record.bins,
                        )
                    })
                    .transpose()?;
                results.push(record);
            }
        }
        Ok(results)
    }

    pub fn put(&self, set: &str, records: Vec<AerospikeRecord>, cas: bool) -> anyhow::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut per_worker: Vec<Vec<IndexedRawItem>> =
            (0..self.inner.workers.len()).map(|_| Vec::new()).collect();
        for (original_index, record) in records.into_iter().enumerate() {
            let worker_idx = shard_for_key(&record.key, self.inner.workers.len());
            let item = IndexedRawItem { original_index, record };
            per_worker[worker_idx].push(item);
        }

        let (reply_tx, reply_rx) = crossbeam_channel::unbounded::<WorkerResult>();
        let mut submitted_workers = 0usize;
        for (worker_idx, items) in per_worker.into_iter().enumerate() {
            if items.is_empty() {
                continue;
            }
            self.inner.workers[worker_idx]
                .send(WorkerMsg::Put {
                    namespace: self.inner.namespace.clone(),
                    set: set.to_string(),
                    items,
                    reply_tx: reply_tx.clone(),
                    cas,
                })
                .expect("worker channel closed");
            submitted_workers += 1;
        }
        drop(reply_tx);

        let mut errors = Vec::new();
        for _ in 0..submitted_workers {
            let part = reply_rx.recv().expect("worker reply channel closed");
            errors.extend(part.errors);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            if errors.len() > 10 {
                Err(anyhow::anyhow!(
                    "Failed to write {} records to {}: {:?} ...",
                    errors.len(),
                    set,
                    errors.into_iter().take(10).collect::<Vec<_>>()
                ))
            } else {
                Err(anyhow::anyhow!(
                    "Failed to write {} records to {}: {:?}",
                    errors.len(),
                    set,
                    errors,
                ))
            }
        }
    }

    pub fn delete(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        for key_bytes in keys {
            let key = self.make_key(set, key_bytes);
            let _ = self.inner.client.delete(&self.inner.write_policy, &key);
        }
        Ok(())
    }

    pub fn truncate_set(&self, set: &str) -> anyhow::Result<()> {
        // V1 client doesn't expose a `truncate` info command. Fall back to
        // a scan-and-delete sweep so the contract — "after `truncate_set`
        // the set is empty" — still holds. Slow on large sets, but V1 is
        // legacy; this path is exercised only by `archive.reset()` which
        // happens at most once per snapshot import.
        let mut keys = Vec::new();
        aerospike2::enumerate_set_with(
            &self.inner.address,
            &self.inner.namespace,
            set,
            &mut |record| {
                if let EnumeratedKey::Blob(bytes) = record.user_key {
                    keys.push(bytes);
                }
                Ok(())
            },
        )?;
        self.delete(set, &keys)
    }

    pub fn enumerate(
        &self,
        set: &str,
        on_record: &mut dyn FnMut(AerospikeRecord) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        aerospike2::enumerate_set_with(
            &self.inner.address,
            &self.inner.namespace,
            set,
            &mut |record| {
                let key = match record.user_key {
                    EnumeratedKey::Blob(bytes) => bytes,
                    other => {
                        anyhow::bail!("Expected blob user key during enumeration, got {other:?}")
                    }
                };
                on_record(record_from_enumerated_bins(key, record.generation as u64, &record.bins)?)
            },
        )
    }
}

impl Drop for Aerospike1BackendInner {
    fn drop(&mut self) {
        for tx in &self.workers {
            let _ = tx.send(WorkerMsg::Stop);
        }
        for join in self.joins.drain(..) {
            let _ = join.join();
        }
    }
}

enum WorkerMsg {
    Put {
        namespace: String,
        set: String,
        items: Vec<IndexedRawItem>,
        reply_tx: crossbeam_channel::Sender<WorkerResult>,
        cas: bool,
    },
    Stop,
}

#[derive(Debug)]
struct IndexedRawItem {
    original_index: usize,
    record: AerospikeRecord,
}

#[derive(Debug)]
struct WorkerResult {
    errors: Vec<(usize, aerospike::Error)>,
}

fn worker_loop(
    client: Arc<Client>,
    write_policy: WritePolicy,
    rx: crossbeam_channel::Receiver<WorkerMsg>,
) {
    while let Ok(msg) = rx.recv() {
        match msg {
            WorkerMsg::Put { namespace, set, items, reply_tx, cas } => {
                let mut errors = Vec::new();
                for item in items {
                    let mut wp = write_policy.clone();
                    if cas {
                        wp.generation_policy = GenerationPolicy::ExpectGenEqual;
                        wp.generation = item.record.generation as u32;
                    }

                    let mut last_err = None;
                    for attempt in 0..=SERVER_MEM_RETRY_COUNT {
                        match put_raw_record(&client, &wp, &namespace, &set, &item.record) {
                            Ok(_) => {
                                last_err = None;
                                break;
                            }
                            Err(err) => {
                                if is_retryable_put_error(&err) && attempt < SERVER_MEM_RETRY_COUNT
                                {
                                    let delay = SERVER_MEM_RETRY_BASE_DELAY * (1 << attempt);
                                    std::thread::sleep(delay);
                                    last_err = Some(err);
                                } else {
                                    last_err = Some(err);
                                    break;
                                }
                            }
                        }
                    }
                    if let Some(err) = last_err {
                        errors.push((item.original_index, err));
                    }
                }
                let _ = reply_tx.send(WorkerResult { errors });
            }
            WorkerMsg::Stop => break,
        }
    }
}

fn put_raw_record(
    client: &Client,
    wp: &WritePolicy,
    namespace: &str,
    set: &str,
    record: &AerospikeRecord,
) -> aerospike::Result<()> {
    let key = make_key(namespace, set, &record.key);
    let mut bins = vec![as_bin!(BIN_DATA, record.data.clone())];
    bins.push(match record.chunk_count {
        Some(chunk_count) => as_bin!(BIN_CHUNK_COUNT, chunk_count as i64),
        None => as_bin!(BIN_CHUNK_COUNT, None),
    });
    bins.push(match record.data_epoch {
        Some(epoch) => as_bin!(BIN_EPOCH, epoch as i64),
        None => as_bin!(BIN_EPOCH, None),
    });
    client.put(wp, &key, &bins)
}

fn record_from_bins(
    key: Vec<u8>,
    generation: u64,
    bins: &mut HashMap<String, Value>,
) -> anyhow::Result<AerospikeRecord> {
    let data = value_to_vec_u8(bins.remove(BIN_DATA))?;
    let data_epoch = decode_data_epoch(bins.remove(BIN_EPOCH))?;
    let chunk_count = decode_chunk_count(bins.remove(BIN_CHUNK_COUNT))?;
    Ok(AerospikeRecord { key, generation, data_epoch, chunk_count, data })
}

fn record_from_enumerated_bins(
    key: Vec<u8>,
    generation: u64,
    bins: &HashMap<String, EnumeratedValue>,
) -> anyhow::Result<AerospikeRecord> {
    let data = match bins.get(BIN_DATA) {
        Some(EnumeratedValue::Blob(bytes)) => bytes.clone(),
        Some(other) => anyhow::bail!("Unexpected enumerated data bin type: {other:?}"),
        None => Vec::new(),
    };
    let data_epoch = match bins.get(BIN_EPOCH) {
        Some(EnumeratedValue::Int(value)) => {
            Some(u32::try_from(*value).map_err(|_| anyhow::anyhow!("Invalid epoch: {value}"))?)
        }
        Some(other) => anyhow::bail!("Unexpected enumerated epoch bin type: {other:?}"),
        None => None,
    };
    let chunk_count = match bins.get(BIN_CHUNK_COUNT) {
        Some(EnumeratedValue::Int(value)) => Some(
            usize::try_from(*value).map_err(|_| anyhow::anyhow!("Invalid chunk count: {value}"))?,
        ),
        Some(other) => anyhow::bail!("Unexpected enumerated chunk-count bin type: {other:?}"),
        None => None,
    };
    Ok(AerospikeRecord { key, generation, data_epoch, chunk_count, data })
}

fn decode_chunk_count(value: Option<Value>) -> anyhow::Result<Option<usize>> {
    match value {
        Some(Value::Int(i)) => Ok(Some(
            usize::try_from(i)
                .map_err(|_| anyhow::anyhow!("Negative or too-large chunk count: {i}"))?,
        )),
        Some(Value::UInt(u)) => {
            Ok(Some(usize::try_from(u).map_err(|_| anyhow::anyhow!("Too-large chunk count: {u}"))?))
        }
        Some(other) => anyhow::bail!("Unexpected chunk-count bin type: {other:?}"),
        None => Ok(None),
    }
}

fn decode_data_epoch(value: Option<Value>) -> anyhow::Result<Option<u32>> {
    match value {
        Some(Value::Int(i)) => {
            Ok(Some(u32::try_from(i).map_err(|_| anyhow::anyhow!("Invalid epoch: {i}"))?))
        }
        Some(Value::UInt(u)) => {
            Ok(Some(u32::try_from(u).map_err(|_| anyhow::anyhow!("Invalid epoch: {u}"))?))
        }
        Some(other) => anyhow::bail!("Unexpected epoch bin type: {other:?}"),
        None => Ok(None),
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

fn vec_val_to_vec_u8(values: Vec<Value>) -> anyhow::Result<Vec<u8>> {
    match values.first() {
        Some(first) => match first {
            Value::Int(_) => Ok(values
                .into_iter()
                .map(|v| {
                    u8::try_from(match v {
                        Value::Int(k) => k,
                        _ => unreachable!(),
                    })
                    .unwrap()
                })
                .collect()),
            Value::UInt(_) => Ok(values
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

fn is_retryable_put_error(err: &aerospike::Error) -> bool {
    matches!(
        &err.0,
        ErrorKind::ServerError(ResultCode::ServerMemError)
            | ErrorKind::ServerError(ResultCode::DeviceOverload)
    )
}

fn shard_for_key(key: &[u8], workers: usize) -> usize {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) % workers
}

fn make_key(namespace: &str, set: &str, key_bytes: &[u8]) -> Key {
    Key::new(namespace, set, Value::Blob(key_bytes.to_vec())).unwrap()
}
