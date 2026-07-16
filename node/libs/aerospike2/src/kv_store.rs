use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use aerospike::as_bin;
use aerospike::operations;
use aerospike::AdminPolicy;
use aerospike::BatchOperation;
use aerospike::BatchPolicy;
use aerospike::BatchReadPolicy;
use aerospike::BatchRecord;
use aerospike::BatchWritePolicy;
use aerospike::Bins;
use aerospike::Client;
use aerospike::ClientPolicy;
use aerospike::Error;
use aerospike::GenerationPolicy;
use aerospike::Key;
use aerospike::ResultCode;
use aerospike::Value;
use aerospike::WritePolicy;

use crate::enumerate::enumerate_set_with_client;
use crate::runtime::block_on;
use crate::EnumeratedKey;
use crate::EnumeratedValue;

const RETRYABLE_PUT_ERROR_RETRY_COUNT: usize = 5;
const RETRYABLE_PUT_ERROR_BASE_DELAY: Duration = Duration::from_millis(100);
const BIN_DATA: &str = "data";
const BIN_EPOCH: &str = "epoch";
const BIN_CHUNK_COUNT: &str = "chunks";

// Aerospike's send-buffer is capped at 120 MB. We sub-batch by record count
// AND estimated payload bytes so large logical batches (e.g. snapshot import,
// where a single chunked head record can be ~992 KB) cannot exceed that
// limit. The byte cap is well below 120 MB to leave headroom for protocol
// framing per record (key, bin headers, batch envelope).
const MAX_BATCH_RECORDS: usize = 10_000;
const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;
// Per-record protocol overhead estimate (key + bin headers + per-record batch
// framing). Used when summing payload to decide where to split sub-batches.
const PER_RECORD_OVERHEAD_BYTES: usize = 256;

#[derive(Debug, Clone)]
pub struct Aerospike2BackendConfig {
    pub address: String,
    pub namespace: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AerospikeRecord {
    pub key: Vec<u8>,
    pub generation: u64,
    pub data_epoch: Option<u32>,
    pub chunk_count: Option<usize>,
    pub data: Vec<u8>,
}

#[derive(Debug)]
struct IndexedRecord {
    original_index: usize,
    record: AerospikeRecord,
}

#[derive(Clone)]
pub struct Aerospike2Backend {
    client: Arc<Client>,
    namespace: String,
    write_policy: WritePolicy,
    batch_policy: BatchPolicy,
}

impl Aerospike2Backend {
    pub fn new(config: Aerospike2BackendConfig) -> anyhow::Result<Self> {
        // Build the Client on the shared runtime so the cluster's tend
        // task and TCP connections are anchored to a runtime that exists
        // for the process lifetime (see `runtime.rs`).
        let client = block_on(Client::new(&ClientPolicy::default(), &config.address))?
            .map_err(|err| anyhow::anyhow!("Failed to connect to Aerospike: {err}"))
            .map(Arc::new)?;

        // The default BatchPolicy has total_timeout=1000ms and max_retries=2,
        // which is far too tight for snapshot-import-sized batches (32-64 MB
        // payloads, where a single round trip + server-side batch processing
        // can exceed 1s under any backpressure). When the timeout fires, the
        // client returns Err but the in-flight `aerospike_rt::spawn`-ed batch
        // task becomes orphaned (tokio::spawn doesn't cancel on JoinHandle
        // drop). Repeating that hundreds of times leaks outstanding tasks
        // that hold pool capacity and produce hangs that look deterministic.
        // Use a generous total_timeout, a roomy socket_timeout, and disable
        // the client-side automatic retries — our `put_one_sub_batch` has
        // its own retry loop with proper backoff for retryable server errors.
        let mut batch_policy = BatchPolicy::default();
        batch_policy.base_policy.total_timeout = 30_000;
        batch_policy.base_policy.socket_timeout = 30_000;
        batch_policy.base_policy.max_retries = 0;

        Ok(Self {
            client,
            namespace: config.namespace,
            write_policy: WritePolicy { send_key: true, ..WritePolicy::default() },
            batch_policy,
        })
    }

    pub fn get(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<Vec<Option<AerospikeRecord>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let read_policy = BatchReadPolicy::default();
        let mut output = Vec::with_capacity(keys.len());
        for chunk in chunk_keys_for_get(keys) {
            let batch_ops = chunk
                .iter()
                .map(|key_bytes| {
                    BatchOperation::read(&read_policy, self.make_key(set, key_bytes), Bins::All)
                })
                .collect::<Vec<_>>();

            let results =
                block_on(self.client.batch(&self.batch_policy, &batch_ops))?.map_err(|err| {
                    anyhow::anyhow!("Aerospike2 batch get failed: set={set}, err={err}")
                })?;

            if results.len() != chunk.len() {
                return Err(anyhow::anyhow!(
                    "Aerospike2 batch get returned {} records for {} keys",
                    results.len(),
                    chunk.len(),
                ));
            }

            for (key_bytes, mut result) in chunk.iter().zip(results) {
                match result.result_code {
                    Some(ResultCode::Ok) => match result.record.take() {
                        Some(mut record) => {
                            output.push(Some(record_from_bins(
                                (*key_bytes).clone(),
                                record.generation as u64,
                                &mut record.bins,
                            )?));
                        }
                        None => {
                            return Err(anyhow::anyhow!(
                                "Aerospike2 batch get: ResultCode::Ok but record missing for key={}",
                                hex::encode(key_bytes),
                            ));
                        }
                    },
                    Some(ResultCode::KeyNotFoundError) | None => {
                        output.push(None);
                    }
                    Some(code) => {
                        return Err(anyhow::anyhow!(
                            "Aerospike2 batch get failed: set={set}, key={}, code={code:?}",
                            hex::encode(key_bytes),
                        ));
                    }
                }
            }
        }
        Ok(output)
    }

    pub fn put(&self, set: &str, records: Vec<AerospikeRecord>, cas: bool) -> anyhow::Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        debug_assert_unique_keys(&records);
        let indexed = records
            .into_iter()
            .enumerate()
            .map(|(original_index, record)| IndexedRecord { original_index, record })
            .collect();
        let sub_batches = split_into_sub_batches(indexed);
        let total_sub_batches = sub_batches.len();
        let mut errors: Vec<String> = Vec::new();
        for (idx, sub_batch) in sub_batches.into_iter().enumerate() {
            let bytes: usize =
                sub_batch.iter().map(|item| estimated_record_bytes(&item.record)).sum();
            let count = sub_batch.len();
            let t = std::time::Instant::now();
            tracing::debug!(
                target: "monit",
                "Aerospike2 put set={set} sub_batch={}/{} count={count} est_bytes={bytes} cas={cas} starting",
                idx + 1,
                total_sub_batches,
            );
            self.put_one_sub_batch(set, sub_batch, cas, &mut errors)?;
            tracing::debug!(
                target: "monit",
                "Aerospike2 put set={set} sub_batch={}/{} done in {} ms",
                idx + 1,
                total_sub_batches,
                t.elapsed().as_secs_f64() * 1000.0,
            );
        }
        if errors.is_empty() {
            Ok(())
        } else if errors.len() > 10 {
            Err(anyhow::anyhow!(
                "Failed to write {} records to {}: {} ...",
                errors.len(),
                set,
                errors.into_iter().take(10).collect::<Vec<_>>().join(", "),
            ))
        } else {
            Err(anyhow::anyhow!(
                "Failed to write {} records to {}: {}",
                errors.len(),
                set,
                errors.join(", "),
            ))
        }
    }

    fn put_one_sub_batch(
        &self,
        set: &str,
        mut pending: Vec<IndexedRecord>,
        cas: bool,
        errors: &mut Vec<String>,
    ) -> anyhow::Result<()> {
        for attempt in 0..=RETRYABLE_PUT_ERROR_RETRY_COUNT {
            if pending.is_empty() {
                return Ok(());
            }

            let batch_ops = pending
                .iter()
                .map(|item| self.make_batch_operation(set, item, cas))
                .collect::<Vec<_>>();

            let results = match block_on(self.client.batch(&self.batch_policy, &batch_ops))? {
                Ok(results) => results,
                Err(err) => {
                    if is_retryable_put_error(&err) && attempt < RETRYABLE_PUT_ERROR_RETRY_COUNT {
                        let delay = RETRYABLE_PUT_ERROR_BASE_DELAY * (1 << attempt);
                        tracing::warn!(
                            target: "monit",
                            "Aerospike2 batch retryable error on put: {err}, retry {}/{} after {:?}",
                            attempt + 1,
                            RETRYABLE_PUT_ERROR_RETRY_COUNT,
                            delay,
                        );
                        thread::sleep(delay);
                        continue;
                    }
                    return Err(anyhow::anyhow!("Failed to batch write records to {set}: {err}"));
                }
            };

            let mut retry = Vec::new();
            for (item, result) in pending.into_iter().zip(results) {
                match batch_result_code(&result) {
                    Some(ResultCode::Ok) => {}
                    Some(code)
                        if is_retryable_result_code(code)
                            && attempt < RETRYABLE_PUT_ERROR_RETRY_COUNT =>
                    {
                        retry.push(item);
                    }
                    Some(code) => errors.push(format!(
                        "index={} key={} result_code={code:?} in_doubt={}",
                        item.original_index,
                        hex::encode(&item.record.key),
                        result.in_doubt,
                    )),
                    None => errors.push(format!(
                        "index={} key={} missing result code",
                        item.original_index,
                        hex::encode(&item.record.key),
                    )),
                }
            }

            if retry.is_empty() {
                return Ok(());
            }

            let delay = RETRYABLE_PUT_ERROR_BASE_DELAY * (1 << attempt);
            tracing::warn!(
                target: "monit",
                "Aerospike2 batch returned {} retryable records, retry {}/{} after {:?}",
                retry.len(),
                attempt + 1,
                RETRYABLE_PUT_ERROR_RETRY_COUNT,
                delay,
            );
            thread::sleep(delay);
            pending = retry;
        }

        errors.extend(pending.into_iter().map(|item| {
            format!(
                "index={} key={} exhausted retries",
                item.original_index,
                hex::encode(item.record.key),
            )
        }));
        Ok(())
    }

    pub fn delete(&self, set: &str, keys: &[Vec<u8>]) -> anyhow::Result<()> {
        for key_bytes in keys {
            let as_key = self.make_key(set, key_bytes);
            let _ = block_on(self.client.delete(&self.write_policy, &as_key))?
                .map_err(|e| anyhow::format_err!("{e:?}"));
        }
        Ok(())
    }

    /// Server-side bulk delete of every record in a set. Uses Aerospike's
    /// `truncate` info command — fast (constant-time on the server, just
    /// updates the set's last-update-time threshold) and atomic. Records
    /// written before the call become invisible.
    pub fn truncate_set(&self, set: &str) -> anyhow::Result<()> {
        block_on(self.client.truncate(&AdminPolicy::default(), &self.namespace, set, 0))?
            .map_err(|err| anyhow::anyhow!("Aerospike2 truncate {set} failed: {err}"))
    }

    pub fn enumerate(
        &self,
        set: &str,
        on_record: &mut dyn FnMut(AerospikeRecord) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        enumerate_set_with_client(&self.client, &self.namespace, set, &mut |record| {
            let key = match record.user_key {
                EnumeratedKey::Blob(bytes) => bytes,
                other => {
                    anyhow::bail!("Expected blob user key during enumeration, got {other:?}")
                }
            };
            on_record(record_from_enumerated_bins(key, record.generation as u64, &record.bins)?)
        })
    }

    fn make_key(&self, set: &str, key_bytes: &[u8]) -> Key {
        Key::new(self.namespace.clone(), set.to_string(), Value::Blob(key_bytes.to_vec())).unwrap()
    }

    fn make_batch_operation(&self, set: &str, item: &IndexedRecord, cas: bool) -> BatchOperation {
        let mut policy = BatchWritePolicy { send_key: true, ..BatchWritePolicy::default() };
        if cas {
            policy.generation_policy = GenerationPolicy::ExpectGenEqual;
            policy.generation = item.record.generation as u32;
        }

        let key = self.make_key(set, &item.record.key);
        let mut ops = vec![operations::put(&as_bin!(BIN_DATA, item.record.data.clone()))];
        match item.record.chunk_count {
            Some(chunk_count) => {
                ops.push(operations::put(&as_bin!(BIN_CHUNK_COUNT, chunk_count as i64)));
            }
            None => {
                ops.push(operations::put(&as_bin!(BIN_CHUNK_COUNT, None)));
            }
        }
        match item.record.data_epoch {
            Some(epoch) => {
                ops.push(operations::put(&as_bin!(BIN_EPOCH, epoch as i64)));
            }
            None => {
                ops.push(operations::put(&as_bin!(BIN_EPOCH, None)));
            }
        }
        BatchOperation::write(&policy, key, ops)
    }
}

fn batch_result_code(record: &BatchRecord) -> Option<ResultCode> {
    record.result_code
}

fn is_retryable_result_code(code: ResultCode) -> bool {
    matches!(code, ResultCode::ServerMemError | ResultCode::DeviceOverload)
}

fn is_retryable_put_error(err: &Error) -> bool {
    matches!(
        err,
        Error::ServerError(code, _, _)
            | Error::BatchError(_, code, _, _)
            | Error::BatchLastError(_, code, _, _)
            if is_retryable_result_code(*code)
    )
}

#[allow(unused)]
fn debug_assert_unique_keys(records: &[AerospikeRecord]) {
    #[cfg(debug_assertions)]
    {
        let mut seen = std::collections::HashSet::with_capacity(records.len());
        for record in records {
            debug_assert!(
                seen.insert(record.key.as_slice()),
                "duplicate raw Aerospike2 key in backend put: {}",
                hex::encode(&record.key),
            );
        }
    }
}

fn estimated_record_bytes(record: &AerospikeRecord) -> usize {
    record.key.len() + record.data.len() + PER_RECORD_OVERHEAD_BYTES
}

fn split_into_sub_batches(items: Vec<IndexedRecord>) -> Vec<Vec<IndexedRecord>> {
    if items.is_empty() {
        return Vec::new();
    }
    let mut sub_batches = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;
    for item in items {
        let item_bytes = estimated_record_bytes(&item.record);
        let would_overflow_bytes =
            !current.is_empty() && current_bytes.saturating_add(item_bytes) > MAX_BATCH_BYTES;
        let would_overflow_count = current.len() >= MAX_BATCH_RECORDS;
        if would_overflow_bytes || would_overflow_count {
            sub_batches.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes = current_bytes.saturating_add(item_bytes);
        current.push(item);
    }
    if !current.is_empty() {
        sub_batches.push(current);
    }
    sub_batches
}

fn chunk_keys_for_get(keys: &[Vec<u8>]) -> Vec<&[Vec<u8>]> {
    if keys.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut bytes = 0usize;
    for (i, key) in keys.iter().enumerate() {
        let est = key.len() + PER_RECORD_OVERHEAD_BYTES;
        let count = i - start;
        let would_overflow_bytes = count > 0 && bytes.saturating_add(est) > MAX_BATCH_BYTES;
        let would_overflow_count = count >= MAX_BATCH_RECORDS;
        if would_overflow_bytes || would_overflow_count {
            out.push(&keys[start..i]);
            start = i;
            bytes = 0;
        }
        bytes = bytes.saturating_add(est);
    }
    if start < keys.len() {
        out.push(&keys[start..]);
    }
    out
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
        Some(other) => anyhow::bail!("Unexpected chunk-count bin type: {other:?}"),
        None => Ok(None),
    }
}

fn decode_data_epoch(value: Option<Value>) -> anyhow::Result<Option<u32>> {
    match value {
        Some(Value::Int(i)) => {
            Ok(Some(u32::try_from(i).map_err(|_| anyhow::anyhow!("Invalid epoch: {i}"))?))
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
        Some(Value::Int(_)) => Ok(values
            .into_iter()
            .map(|v| {
                u8::try_from(match v {
                    Value::Int(k) => k,
                    _ => unreachable!(),
                })
                .unwrap()
            })
            .collect()),
        Some(_) => Err(anyhow::format_err!("Invalid data format found in aerospike")),
        None => Ok(vec![]),
    }
}

// Tokio runtime handling moved to the shared `crate::runtime` module so the
// runtime is built once per process instead of once per call. See
// `runtime.rs` for the rationale.
