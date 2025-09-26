use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use governor::DefaultKeyedRateLimiter;
use governor::Quota;
use governor::RateLimiter;

use super::NodeIdentifier;
use crate::helper::metrics::BlockProductionMetrics;
use crate::multithreading::load_balancing_service::LoadBalancingService;
use crate::multithreading::routing::service::RoutingService;
use crate::multithreading::thread_synchrinization_service::ThreadSyncService;
use crate::multithreading::threads_tracking_service::ThreadsTrackingService;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::CrossThreadRefDataRepository;
use crate::storage::CrossRefStorage;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::utilities::FixedSizeHashSet;

const DIRTY_HACK_CACHE_SIZE: usize = 10000; // 100 blocks for 100 threads.

#[derive(Clone)]
pub struct SharedServices {
    container: Arc<Mutex<Container>>,
    pub metrics: Option<BlockProductionMetrics>,
    limiter: Arc<DefaultKeyedRateLimiter<NodeIdentifier>>,
    pub last_finalization_timestamp: Arc<AtomicU64>,
}

#[allow(dead_code, non_snake_case)]
pub struct Container {
    // Note: dependency_tracking is replaced with ThreadReferencesState in optimistic states
    // pub dependency_tracking: DependenciesTrackingService,
    pub threads_tracking: ThreadsTrackingService,
    pub thread_sync: ThreadSyncService,
    pub load_balancing: LoadBalancingService,

    // Must be private. But due to extra hacks added it became public :(
    pub router: RoutingService,

    pub cross_thread_ref_data_service: CrossThreadRefDataRepository,

    // This is a dirty solution to fix code that calls the same event
    // multiple times. Since it is not an expected behavior for
    // the inner services these hacks will solve it to some extend.
    dirty_hack__appended_blocks: FixedSizeHashSet<BlockIdentifier>,
    dirty_hack__finalized_blocks: FixedSizeHashSet<BlockIdentifier>,
    dirty_hack__invalidated_blocks: FixedSizeHashSet<BlockIdentifier>,
}

impl SharedServices {
    #[cfg(test)]
    pub fn test_start(router: RoutingService, rate: u32) -> Self {
        // An alias to make a little easier code navigation

        use crate::storage::CrossRefStorage;
        Self::start(
            router,
            PathBuf::from("./data-dir-test"),
            None,
            5000,
            100,
            rate,
            1,
            CrossRefStorage::mem(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn start(
        router: RoutingService,
        data_dir: PathBuf,
        metrics: Option<BlockProductionMetrics>,
        thread_load_threshold: usize,
        thread_load_window_size: usize,
        rate_limit_on_incoming_block_req: u32,
        thread_cnt_soft_limit: usize,
        crossref_db: CrossRefStorage,
    ) -> Self {
        let res = Self {
            container: Arc::new(Mutex::new(Container {
                router,
                thread_sync: ThreadSyncService::start(()),
                threads_tracking: ThreadsTrackingService::start(),
                load_balancing: LoadBalancingService::start(
                    metrics.clone(),
                    thread_load_window_size,
                    thread_load_threshold,
                ),
                cross_thread_ref_data_service: CrossThreadRefDataRepository::new(
                    data_dir,
                    thread_cnt_soft_limit,
                    crossref_db,
                ),
                dirty_hack__appended_blocks: FixedSizeHashSet::new(DIRTY_HACK_CACHE_SIZE),
                dirty_hack__finalized_blocks: FixedSizeHashSet::new(DIRTY_HACK_CACHE_SIZE),
                dirty_hack__invalidated_blocks: FixedSizeHashSet::new(DIRTY_HACK_CACHE_SIZE),
            })),
            metrics,
            // Arc is enough for the rate limiter, since its state lives in AtomicU64
            // https://docs.rs/governor/latest/governor/_guide/index.html#wrapping-the-limiter-in-an-arc
            limiter: Arc::new(RateLimiter::keyed(Quota::per_second(
                NonZeroU32::new(rate_limit_on_incoming_block_req.max(1))
                    .expect("Rate limit is non-zero"),
            ))),
            last_finalization_timestamp: Arc::new(AtomicU64::new(0)),
        };
        res.update_last_finalization_timestamp();
        res
    }

    fn update_last_finalization_timestamp(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("Time from unix epoch can not fail")
            .as_millis() as u64;
        let _ = self.last_finalization_timestamp.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |prev| if prev < now { Some(now) } else { None },
        );
    }

    pub fn duration_since_last_finalization(&self) -> std::time::Duration {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .expect("Time from unix epoch can not fail")
            .as_millis() as u64;
        let last_finalized = self.last_finalization_timestamp.load(Ordering::Relaxed);
        std::time::Duration::from_millis(now.saturating_sub(last_finalized))
    }

    pub fn exec<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Container) -> R,
    {
        #[cfg(feature = "fail_on_long_lock")]
        let start = std::time::Instant::now();
        let mut services = self.container.lock().expect("Can not be poisoned");
        let result = f(&mut services);
        drop(services);
        #[cfg(feature = "fail_on_long_lock")]
        {
            let elapsed = start.elapsed().as_millis();
            tracing::trace!("Shared_services exec time: {:?} ms", elapsed);
            if elapsed > 50 {
                tracing::warn!("too long shared services exec time: {:?}", elapsed);
                tracing::warn!("{:?}", std::backtrace::Backtrace::force_capture());
            }
        }
        result
    }

    pub fn on_block_finalized<TOptimisticState>(
        &mut self,
        block: &AckiNackiBlock,
        state: Arc<TOptimisticState>,
    ) where
        TOptimisticState: OptimisticState,
    {
        let block_identifier: BlockIdentifier = block.identifier();
        // let parent_block_identifier: BlockIdentifier = block.parent();
        let thread_identifier: ThreadIdentifier = block.get_common_section().thread_id;
        let threads_table = state.get_produced_threads_table().clone();
        tracing::trace!("handling on_block_finalized: {:?}", &block_identifier);
        self.update_last_finalization_timestamp();
        self.exec(|services| {
            if services.dirty_hack__finalized_blocks.contains(&block_identifier) {
                return;
            }
            services.dirty_hack__finalized_blocks.insert(block_identifier.clone());

            services
                .thread_sync
                .on_block_finalized(&block_identifier, &thread_identifier)
                .expect("Must work");
            match services.threads_tracking.handle_block_finalized(
                block_identifier.clone(),
                thread_identifier,
                threads_table,
                &mut (&mut services.router, &mut services.load_balancing),
            ) {
                Ok(()) => {}
                Err(e) => {
                    tracing::trace!("threads_tracking handle_block_finalized error: {:?}", e);
                    unimplemented!()
                }
            }
            services.load_balancing.handle_block_finalized(block, state);
        });
    }

    pub fn throttle(&self, node_id: &NodeIdentifier) -> anyhow::Result<()> {
        self.limiter.check_key(node_id).map_err(|error| {
            anyhow::anyhow!("Rate limit exceeded for node {}, until {}", node_id, error)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    #[test]
    fn throttling_one_thread_no_limits() -> anyhow::Result<()> {
        let shared_services = SharedServices::test_start(RoutingService::stub().0, u32::MAX);

        let node_id_0 = NodeIdentifier::from_str("a".repeat(64).as_str())?;
        let node_id_1 = NodeIdentifier::from_str("b".repeat(64).as_str())?;

        for _ in 1..1000 {
            assert!(shared_services.throttle(&node_id_0).is_ok());
            assert!(shared_services.throttle(&node_id_1).is_ok());
        }
        Ok(())
    }

    #[test]
    fn throttling_two_threads_gt_then_limit() -> anyhow::Result<()> {
        let shared_services = SharedServices::test_start(RoutingService::stub().0, 100);

        let node_id_0 = NodeIdentifier::from_str(&"a".repeat(64))?;
        let node_id_1 = NodeIdentifier::from_str(&"b".repeat(64))?;

        // These requests must be throttled
        let shared_services_0 = shared_services.clone();
        let node_id_0_cloned = node_id_0.clone();

        let handle_0 = std::thread::Builder::new()
            .name("t_0".to_string())
            .spawn(move || {
                (0..200).map(|_| shared_services_0.throttle(&node_id_0_cloned)).collect::<Vec<_>>()
            })
            .unwrap();

        // These requests must be all OK
        let shared_services_1 = shared_services.clone();
        let node_id_1_cloned = node_id_1.clone();
        let handle_1 = std::thread::Builder::new()
            .name("t_1".to_string())
            .spawn(move || {
                (0..100).map(|_| shared_services_1.throttle(&node_id_1_cloned)).collect::<Vec<_>>()
            })
            .unwrap();

        let results_0 = handle_0.join().expect("Thread 0 panicked");
        let results_1 = handle_1.join().expect("Thread 1 panicked");

        assert!(results_0.iter().any(|r| r.is_err()));
        assert!(results_1.iter().all(|r| r.is_ok()));

        std::thread::sleep(std::time::Duration::from_secs(1));
        // Now all requests must be OK
        for _ in 1..200 {
            assert!(shared_services.throttle(&node_id_0).is_ok());
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        Ok(())
    }
}
