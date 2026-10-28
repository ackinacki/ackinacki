//! Bounded, ordered body reads. Does not define a partition wire codec.
use std::collections::VecDeque;

use node_types::AccountRouting;

use super::AccountInfo;
use super::ThreadAccountMap;
use crate::ThreadAccount;

/// Limits apply to each read batch and to the complete pipeline: reading,
/// awaiting validation, validating and awaiting ordered emission combined.
/// A single oversized account is allowed as a batch of its own. Byte limits
/// count canonical payload, not allocator/decoder or storage-envelope overhead.
#[derive(Clone, Copy, Debug)]
pub struct PartitionReadLimits {
    pub accounts_per_batch: usize,
    pub canonical_bytes_per_batch: u64,
    /// Shared credit limit across all stages, including ordered emission.
    pub in_flight_batches: usize,
    pub read_workers: usize,
    pub validation_workers: usize,
}

impl Default for PartitionReadLimits {
    fn default() -> Self {
        Self {
            accounts_per_batch: 256,
            canonical_bytes_per_batch: 8 * 1024 * 1024,
            in_flight_batches: 8,
            read_workers: 4,
            validation_workers: 1,
        }
    }
}

pub(super) struct AccountRead {
    pub routing: AccountRouting,
    pub info: AccountInfo,
    pub size: u64,
}

pub(super) fn read_ordered<F, C, R, V, B, P, O>(
    map: &ThreadAccountMap,
    limits: PartitionReadLimits,
    read_batch: R,
    validate_batch: V,
    prepare_output: O,
    should_cancel: &C,
    mut on_account: F,
) -> anyhow::Result<()>
where
    F: FnMut(AccountRouting, &P) -> anyhow::Result<()>,
    C: Fn() -> bool + Sync,
    R: Fn(&[AccountRead]) -> anyhow::Result<B> + Sync,
    V: Fn(&[AccountRead], B) -> anyhow::Result<Vec<ThreadAccount>> + Sync,
    B: Send,
    P: Send,
    O: Fn(ThreadAccount) -> P + Sync,
{
    anyhow::ensure!(
        limits.accounts_per_batch > 0
            && limits.canonical_bytes_per_batch > 0
            && limits.in_flight_batches > 0
            && limits.read_workers > 0
            && limits.validation_workers > 0,
        "partition read limits must be non-zero"
    );
    let mut entries = map
        .iter_with_augmentation()
        .map(|(routing, info, capacity)| AccountRead {
            routing,
            info: *info,
            size: capacity.bytes(),
        })
        .peekable();
    type BatchResult<P> = anyhow::Result<Vec<P>>;
    let (tasks_tx, tasks_rx) = crossbeam_channel::bounded::<(
        Vec<AccountRead>,
        crossbeam_channel::Sender<BatchResult<P>>,
    )>(limits.in_flight_batches);
    let (validation_tx, validation_rx) = crossbeam_channel::bounded::<(
        Vec<AccountRead>,
        anyhow::Result<B>,
        crossbeam_channel::Sender<BatchResult<P>>,
    )>(limits.in_flight_batches);
    std::thread::scope(|scope| {
        for _ in 0..limits.read_workers.min(limits.in_flight_batches) {
            let tasks_rx = tasks_rx.clone();
            let read_batch = &read_batch;
            let validation_tx = validation_tx.clone();
            scope.spawn(move || {
                for (batch, result_tx) in tasks_rx {
                    let result = if should_cancel() {
                        Err(anyhow::anyhow!("partition account read cancelled"))
                    } else {
                        read_batch(&batch)
                    };
                    if validation_tx.send((batch, result, result_tx)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(validation_tx);
        for _ in 0..limits.validation_workers.min(limits.in_flight_batches) {
            let validation_rx = validation_rx.clone();
            let validate_batch = &validate_batch;
            let prepare_output = &prepare_output;
            scope.spawn(move || {
                for (batch, raw, result_tx) in validation_rx {
                    let result = (|| {
                        anyhow::ensure!(!should_cancel(), "partition account read cancelled");
                        let accounts = validate_batch(&batch, raw?)?;
                        anyhow::ensure!(
                            accounts.len() == batch.len(),
                            "partition batch result length mismatch"
                        );
                        for (entry, account) in batch.iter().zip(&accounts) {
                            anyhow::ensure!(!should_cancel(), "partition account read cancelled");
                            let matches = match entry.info {
                                AccountInfo::Redirect(dapp) => {
                                    account.is_redirect() && account.get_dapp_id() == Some(dapp)
                                }
                                AccountInfo::AccountHash(hash) => {
                                    !account.is_redirect() && account.account_hash() == hash
                                }
                            };
                            anyhow::ensure!(
                                matches,
                                "partition account version mismatch: {}",
                                entry.routing
                            );
                            anyhow::ensure!(
                                account.canonical_size() == entry.size,
                                "partition account canonical size mismatch: {}",
                                entry.routing
                            );
                        }
                        // Strip decoded state here, when requested by the
                        // exporter, rather than dropping cell trees in writer.
                        Ok(accounts.into_iter().map(prepare_output).collect())
                    })();
                    // The receiver may have been dropped after a writer/read error.
                    let _ = result_tx.send(result);
                }
            });
        }
        drop(tasks_rx);
        drop(validation_rx);
        let result = (|| {
            let mut pending = VecDeque::new();
            loop {
                anyhow::ensure!(!should_cancel(), "partition account read cancelled");
                // Credits are returned only after emission, not read completion:
                // a slow first batch cannot grow the reorder buffer unboundedly.
                while pending.len() < limits.in_flight_batches {
                    let mut batch = Vec::new();
                    let mut bytes = 0u64;
                    while let Some(next) = entries.peek() {
                        anyhow::ensure!(!should_cancel(), "partition account read cancelled");
                        if !batch.is_empty()
                            && (batch.len() >= limits.accounts_per_batch
                                || next.size
                                    > limits.canonical_bytes_per_batch.saturating_sub(bytes))
                        {
                            break;
                        }
                        let next = entries.next().unwrap();
                        bytes = bytes
                            .checked_add(next.size)
                            .ok_or_else(|| anyhow::anyhow!("partition batch capacity overflow"))?;
                        batch.push(next);
                    }
                    if batch.is_empty() {
                        break;
                    }
                    let routings: Vec<_> = batch.iter().map(|entry| entry.routing).collect();
                    let (result_tx, result_rx) = crossbeam_channel::bounded(1);
                    tasks_tx
                        .send((batch, result_tx))
                        .map_err(|_| anyhow::anyhow!("partition read workers stopped"))?;
                    pending.push_back((routings, result_rx));
                }
                let Some((routings, result_rx)) = pending.pop_front() else {
                    return Ok(());
                };
                let accounts = result_rx
                    .recv()
                    .map_err(|_| anyhow::anyhow!("partition read worker stopped"))??;
                for (routing, account) in routings.into_iter().zip(accounts) {
                    anyhow::ensure!(!should_cancel(), "partition account read cancelled");
                    on_account(routing, &account)?;
                }
            }
        })();
        // Always close the queue before scoped workers are joined, including
        // cancellation and callback errors. Outstanding bounded reads finish.
        drop(tasks_tx);
        result
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use node_types::AccountIdentifier;
    use node_types::DAppIdentifier;

    use super::*;

    // Most tests isolate scheduling and coverage from body decoding.
    fn read_ordered<F, C, R>(
        map: &ThreadAccountMap,
        limits: PartitionReadLimits,
        read_batch: R,
        should_cancel: &C,
        on_account: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(AccountRouting, &ThreadAccount) -> anyhow::Result<()>,
        C: Fn() -> bool + Sync,
        R: Fn(&[AccountRead]) -> anyhow::Result<Vec<ThreadAccount>> + Sync,
    {
        super::read_ordered(
            map,
            limits,
            read_batch,
            |_, accounts| Ok(accounts),
            std::convert::identity,
            should_cancel,
            on_account,
        )
    }

    fn fixture() -> ThreadAccountMap {
        let account = ThreadAccount::redirect(DAppIdentifier::new([99; 32]));
        let entries: Vec<_> = (0..19u8)
            .map(|i| {
                let routing = AccountRouting::new(
                    DAppIdentifier::new([i / 7; 32]),
                    AccountIdentifier::new([i; 32]),
                );
                (
                    routing,
                    Some((
                        AccountInfo::Redirect(DAppIdentifier::new([99; 32])),
                        super::super::AccountCapacity::new(account.canonical_size()),
                    )),
                )
            })
            .collect();
        ThreadAccountMap::new().update(&entries)
    }

    fn bodies(batch: &[AccountRead]) -> anyhow::Result<Vec<ThreadAccount>> {
        Ok(batch
            .iter()
            .map(|entry| match entry.info {
                AccountInfo::Redirect(dapp) => ThreadAccount::redirect(dapp),
                _ => unreachable!(),
            })
            .collect())
    }

    #[test]
    fn queue_depth_and_worker_counts_are_independent() -> anyhow::Result<()> {
        let map = fixture();
        for (read_workers, validation_workers, in_flight_batches) in
            [(1, 2, 8), (2, 1, 16), (1, 1, 1)]
        {
            let readers = parking_lot::Mutex::new(std::collections::HashSet::new());
            let validators = parking_lot::Mutex::new(std::collections::HashSet::new());
            let loaded = AtomicUsize::new(0);
            let mut output = Vec::new();
            super::read_ordered(
                &map,
                PartitionReadLimits {
                    accounts_per_batch: 1,
                    in_flight_batches,
                    read_workers,
                    validation_workers,
                    ..Default::default()
                },
                |batch| {
                    readers.lock().insert(std::thread::current().id());
                    loaded.fetch_add(batch.len(), Ordering::SeqCst);
                    Ok(())
                },
                |batch, ()| {
                    validators.lock().insert(std::thread::current().id());
                    bodies(batch)
                },
                std::convert::identity,
                &|| false,
                |routing, _| {
                    assert!(loaded.load(Ordering::SeqCst) <= output.len() + in_flight_batches);
                    output.push(routing);
                    Ok(())
                },
            )?;
            assert!(readers.lock().len() <= read_workers);
            assert!(validators.lock().len() <= validation_workers);
            assert_eq!(output, map.iter().map(|(routing, _)| routing).collect::<Vec<_>>());
        }
        Ok(())
    }

    #[test]
    fn canonical_output_is_prepared_in_validation_workers() -> anyhow::Result<()> {
        let map = fixture();
        let writer = std::thread::current().id();
        let mut output = Vec::new();
        super::read_ordered(
            &map,
            PartitionReadLimits::default(),
            bodies,
            |_, accounts| Ok(accounts),
            |account| {
                assert_ne!(std::thread::current().id(), writer);
                account.into_canonical_bytes()
            },
            &|| false,
            |routing, bytes| {
                assert_eq!(std::thread::current().id(), writer);
                assert_eq!(
                    bytes.as_ref(),
                    ThreadAccount::redirect(DAppIdentifier::new([99; 32])).canonical_bytes()
                );
                output.push(routing);
                Ok(())
            },
        )?;
        assert_eq!(output, map.iter().map(|(routing, _)| routing).collect::<Vec<_>>());
        Ok(())
    }

    #[test]
    fn read_and_validation_stages_are_independent_and_share_backpressure() -> anyhow::Result<()> {
        let map = fixture();
        let expected: Vec<_> = map.iter().map(|(routing, _)| routing).collect();
        let loaded = AtomicUsize::new(0);
        let readers = parking_lot::Mutex::new(Vec::new());
        let validators = parking_lot::Mutex::new(Vec::new());
        let mut output = Vec::new();
        super::read_ordered(
            &map,
            PartitionReadLimits {
                accounts_per_batch: 1,
                in_flight_batches: 2,
                ..Default::default()
            },
            |batch| {
                readers.lock().push(std::thread::current().id());
                loaded.fetch_add(batch.len(), Ordering::SeqCst);
                Ok(())
            },
            |batch, ()| {
                validators.lock().push(std::thread::current().id());
                bodies(batch)
            },
            std::convert::identity,
            &|| false,
            |routing, _| {
                assert!(loaded.load(Ordering::SeqCst) <= output.len() + 2);
                output.push(routing);
                Ok(())
            },
        )?;
        assert_eq!(output, expected);
        assert!(readers.lock().iter().all(|id| !validators.lock().contains(id)));
        let result = super::read_ordered(
            &map,
            PartitionReadLimits::default(),
            |_| Ok(()),
            |_, ()| anyhow::bail!("validation failed"),
            std::convert::identity,
            &|| false,
            |_, _| panic!("invalid accounts must not be emitted"),
        );
        assert!(result.unwrap_err().to_string().contains("validation failed"));
        Ok(())
    }

    #[test]
    fn ordered_partition_reads_restore_order_and_bound_the_window() -> anyhow::Result<()> {
        let map = fixture();
        let expected: Vec<_> = map.iter().map(|(routing, _)| routing).collect();
        let loaded = AtomicUsize::new(0);
        let completed = parking_lot::Mutex::new(Vec::new());
        let second_completed = (parking_lot::Mutex::new(false), parking_lot::Condvar::new());
        let mut output = Vec::new();
        let pool = rayon::ThreadPoolBuilder::new().num_threads(2).build()?;
        pool.install(|| {
            read_ordered(
                &map,
                PartitionReadLimits {
                    accounts_per_batch: 1,
                    canonical_bytes_per_batch: u64::MAX,
                    in_flight_batches: 2,
                    ..Default::default()
                },
                |batch| {
                    loaded.fetch_add(batch.len(), Ordering::SeqCst);
                    if batch[0].routing == expected[0] {
                        let mut ready = second_completed.0.lock();
                        second_completed.1.wait_while_for(
                            &mut ready,
                            |ready| !*ready,
                            std::time::Duration::from_secs(5),
                        );
                        assert!(*ready, "second batch must complete before the first");
                    }
                    completed.lock().push(batch[0].routing);
                    if batch[0].routing == expected[1] {
                        *second_completed.0.lock() = true;
                        second_completed.1.notify_one();
                    }
                    bodies(batch)
                },
                &|| false,
                |routing, _| {
                    assert!(loaded.load(Ordering::SeqCst) <= output.len() + 2);
                    output.push(routing);
                    Ok(())
                },
            )
        })?;
        assert_eq!(output, expected);
        assert_eq!(completed.lock()[0], expected[1]);
        Ok(())
    }

    #[test]
    fn ordered_partition_reads_refill_before_the_previous_window_finishes() -> anyhow::Result<()> {
        let map = fixture();
        let expected: Vec<_> = map.iter().map(|(routing, _)| routing).collect();
        let third_started = (parking_lot::Mutex::new(false), parking_lot::Condvar::new());
        let mut output = Vec::new();
        read_ordered(
            &map,
            PartitionReadLimits {
                accounts_per_batch: 1,
                in_flight_batches: 2,
                ..Default::default()
            },
            |batch| {
                if batch[0].routing == expected[1] {
                    let mut started = third_started.0.lock();
                    third_started.1.wait_while_for(
                        &mut started,
                        |started| !*started,
                        std::time::Duration::from_secs(5),
                    );
                    anyhow::ensure!(*started, "third read must start before second completes");
                }
                if batch[0].routing == expected[2] {
                    *third_started.0.lock() = true;
                    third_started.1.notify_one();
                }
                bodies(batch)
            },
            &|| false,
            |routing, _| {
                output.push(routing);
                Ok(())
            },
        )?;
        assert_eq!(output, expected);
        Ok(())
    }

    #[test]
    fn ordered_partition_reads_bound_bytes_and_allow_one_oversized_leaf() -> anyhow::Result<()> {
        let map = fixture();
        let size = map.capacity().bytes() / 19;
        for limit in [1, size, 2 * size] {
            let read = AtomicUsize::new(0);
            read_ordered(
                &map,
                PartitionReadLimits {
                    accounts_per_batch: 3,
                    canonical_bytes_per_batch: limit,
                    in_flight_batches: 2,
                    ..Default::default()
                },
                |batch| {
                    assert!(batch.len() <= 3);
                    assert!(batch.len() == 1 || batch.iter().map(|e| e.size).sum::<u64>() <= limit);
                    read.fetch_add(batch.len(), Ordering::Relaxed);
                    bodies(batch)
                },
                &|| false,
                |_, _| Ok(()),
            )?;
            assert_eq!(read.load(Ordering::Relaxed), 19);
        }
        Ok(())
    }

    #[test]
    fn ordered_partition_reads_stop_on_cancel_and_callback_error() -> anyhow::Result<()> {
        let map = fixture();
        let cancelled = AtomicBool::new(false);
        let emitted = AtomicUsize::new(0);
        let result = read_ordered(
            &map,
            PartitionReadLimits::default(),
            bodies,
            &|| cancelled.load(Ordering::Relaxed),
            |_, _| {
                emitted.fetch_add(1, Ordering::Relaxed);
                cancelled.store(true, Ordering::Relaxed);
                Ok(())
            },
        );
        assert!(result.unwrap_err().to_string().contains("cancelled"));
        assert_eq!(emitted.load(Ordering::Relaxed), 1);
        let result =
            read_ordered(&map, PartitionReadLimits::default(), bodies, &|| false, |_, _| {
                anyhow::bail!("writer failed")
            });
        assert!(result.unwrap_err().to_string().contains("writer failed"));
        Ok(())
    }

    #[test]
    fn ordered_partition_reads_reject_bad_results_and_limits() {
        let map = fixture();
        for limits in [
            PartitionReadLimits { accounts_per_batch: 0, ..Default::default() },
            PartitionReadLimits { canonical_bytes_per_batch: 0, ..Default::default() },
            PartitionReadLimits { in_flight_batches: 0, ..Default::default() },
            PartitionReadLimits { read_workers: 0, ..Default::default() },
            PartitionReadLimits { validation_workers: 0, ..Default::default() },
        ] {
            assert!(read_ordered(
                &map,
                limits,
                |_| panic!("must not read"),
                &|| false,
                |_, _| Ok(())
            )
            .is_err());
        }
        assert!(read_ordered(
            &map,
            PartitionReadLimits::default(),
            |_| Ok(Vec::new()),
            &|| false,
            |_, _| panic!("must not emit")
        )
        .is_err());
        assert!(read_ordered(
            &map,
            PartitionReadLimits::default(),
            |_| anyhow::bail!("DB failed"),
            &|| false,
            |_, _| panic!("must not emit")
        )
        .is_err());
        assert!(read_ordered(
            &map,
            PartitionReadLimits::default(),
            |batch| {
                Ok(batch
                    .iter()
                    .map(|_| ThreadAccount::redirect(DAppIdentifier::new([1; 32])))
                    .collect())
            },
            &|| false,
            |_, _| panic!("must not emit")
        )
        .is_err());
    }
}
