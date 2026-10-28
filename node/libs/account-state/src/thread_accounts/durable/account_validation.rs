//! Bounded CPU validation shared by legacy and partitioned snapshot import.
//!
//! Reading/framing and the storage sink remain on the caller's thread. Only
//! owned account bodies enter this dedicated pool, never the producer's Rayon
//! pool. Limiting both batch records/bytes and in-flight batches keeps a slow
//! sink from accumulating decoded accounts for the whole snapshot.
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use node_types::AccountRouting;

use super::AccountCapacity;
use super::AccountInfo;
use crate::ThreadAccount;

pub(super) const VALIDATION_BATCH_RECORDS: usize = 1024;
const VALIDATION_BATCH_BYTES: usize = 8 * 1024 * 1024;
type RawAccount = (AccountRouting, Vec<u8>);
type ValidatedAccount = (AccountRouting, Box<[u8]>, AccountInfo, AccountCapacity);

/// Recanonicalize untrusted input once and compute all leaf metadata on the
/// validation worker, not on the serial map/storage thread. Neither format may
/// retain wire bytes as its database version or derive redirects from an old map.
pub(super) fn validate_account((routing, bytes): RawAccount) -> anyhow::Result<ValidatedAccount> {
    let account = ThreadAccount::recanonicalize_tvm_bytes(&bytes)?;
    let info = if account.is_redirect() {
        AccountInfo::Redirect(
            account
                .get_dapp_id()
                .ok_or_else(|| anyhow::anyhow!("Redirect body has no target DApp: {routing}"))?,
        )
    } else {
        AccountInfo::AccountHash(account.account_hash())
    };
    let capacity = AccountCapacity::new(account.canonical_size());
    // Release the decoded TVM tree here as well. Passing it to the serial sink
    // would move expensive cell destruction back onto the import/storage thread.
    Ok((routing, account.into_canonical_bytes(), info, capacity))
}

/// Legacy records may be delivered in batch completion order. Partition records
/// must retain wire order for their incremental bounds/order checks; only that
/// mode buffers out-of-order results, within the same in-flight window.
pub(super) fn with_validated_accounts<T, V, O>(
    entries: &mut dyn Iterator<Item = anyhow::Result<RawAccount>>,
    ordered: bool,
    validate: V,
    consume: impl FnOnce(&mut dyn Iterator<Item = anyhow::Result<T>>) -> anyhow::Result<O>,
) -> anyhow::Result<O>
where
    T: Send + 'static,
    V: Fn(RawAccount) -> anyhow::Result<T> + Send + Sync + 'static,
{
    let workers = super::repository::snapshot_written_hash_worker_count();
    let window = workers.saturating_add(super::repository::snapshot_written_hash_queue_batches());
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .thread_name(|index| format!("snapshot-import-validate-{index}"))
        .build()?;
    let validate = Arc::new(validate);
    let stopped = Arc::new(AtomicBool::new(false));
    let (tx, rx) = crossbeam_channel::bounded(window);
    let mut exhausted = false;
    let mut failed = false;
    let mut pending = 0usize;
    let mut submitted = 0usize;
    let mut next_batch = 0usize;
    let mut buffered = BTreeMap::new();
    let mut ready = Vec::new().into_iter();
    let result = {
        let mut validated = std::iter::from_fn(|| {
            if failed {
                return None;
            }
            let result = (|| -> anyhow::Result<Option<T>> {
                loop {
                    if let Some(account) = ready.next() {
                        return Ok(Some(account));
                    }
                    while !exhausted && pending < window {
                        let mut batch = Vec::with_capacity(VALIDATION_BATCH_RECORDS);
                        let mut bytes = 0usize;
                        while batch.len() < VALIDATION_BATCH_RECORDS
                            && bytes < VALIDATION_BATCH_BYTES
                        {
                            let Some(entry) = entries.next() else {
                                exhausted = true;
                                break;
                            };
                            let entry = entry?;
                            bytes = bytes.saturating_add(entry.1.len());
                            batch.push(entry);
                        }
                        if batch.is_empty() {
                            break;
                        }
                        let (tx, validate, stopped) =
                            (tx.clone(), validate.clone(), stopped.clone());
                        let index = submitted;
                        pool.spawn(move || {
                            // Invalid untrusted bodies return errors through the
                            // result channel. Panics remain fatal, as elsewhere in
                            // the node (production builds use panic = "abort").
                            let result = batch
                                .into_iter()
                                .map(|entry| {
                                    anyhow::ensure!(
                                        !stopped.load(Ordering::Relaxed),
                                        "snapshot validation stopped"
                                    );
                                    validate(entry)
                                })
                                .collect::<anyhow::Result<Vec<T>>>();
                            let _ = tx.send((index, result));
                        });
                        submitted += 1;
                        pending += 1;
                    }
                    if pending == 0 {
                        return Ok(None);
                    }
                    let result = if ordered {
                        loop {
                            if let Some(result) = buffered.remove(&next_batch) {
                                next_batch += 1;
                                break result;
                            }
                            let (index, result) = rx.recv()?;
                            buffered.insert(index, result);
                        }
                    } else {
                        rx.recv()?.1
                    };
                    pending -= 1;
                    ready = result?.into_iter();
                }
            })();
            match result {
                Ok(account) => account.map(Ok),
                Err(error) => {
                    failed = true;
                    Some(Err(error))
                }
            }
        });
        consume(&mut validated)
    };
    // An errored/cancelled sink must not leave queued batches doing expensive
    // validation. Running jobs own their inputs and cannot touch staged storage.
    stopped.store(true, Ordering::Relaxed);
    // At most `window` jobs exist. The result channel has that same capacity,
    // so early exit cannot strand workers on a blocking send.
    result.and_then(|value| {
        anyhow::ensure!(
            !failed && exhausted && pending == 0 && ready.len() == 0,
            "snapshot account sink did not consume all bodies"
        );
        Ok(value)
    })
}

#[cfg(test)]
#[path = "account_validation_tests.rs"]
mod tests;
