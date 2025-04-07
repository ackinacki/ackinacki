use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use crate::helper::metrics::BlockProductionMetrics;
use crate::multithreading::load_balancing_service::LoadBalancingService;
use crate::multithreading::routing::service::RoutingService;
use crate::multithreading::thread_synchrinization_service::ThreadSyncService;
use crate::multithreading::threads_tracking_service::ThreadsTrackingService;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::CrossThreadRefDataRepository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::utilities::FixedSizeHashSet;

const DIRTY_HACK__CACHE_SIZE: usize = 10000; // 100 blocks for 100 threads.

#[derive(Clone)]
pub struct SharedServices {
    container: Arc<Mutex<Container>>,
    pub metrics: Option<BlockProductionMetrics>,
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
    // the inner serivces these hacks will solve it to some extend.
    dirty_hack__appended_blocks: FixedSizeHashSet<BlockIdentifier>,
    dirty_hack__finalized_blocks: FixedSizeHashSet<BlockIdentifier>,
    dirty_hack__invalidated_blocks: FixedSizeHashSet<BlockIdentifier>,
}

impl SharedServices {
    #[cfg(test)]
    pub fn test_start(router: RoutingService) -> Self {
        // An alias to make a little easier code navidation
        Self::start(router, PathBuf::from("./data-dir-test"), None, 5000, 100)
    }

    pub fn start(
        router: RoutingService,
        data_dir: PathBuf,
        metrics: Option<BlockProductionMetrics>,
        thread_load_threshold: usize,
        thread_load_window_size: usize,
    ) -> Self {
        Self {
            container: Arc::new(Mutex::new(Container {
                router,
                thread_sync: ThreadSyncService::start(()),
                threads_tracking: ThreadsTrackingService::start(),
                load_balancing: LoadBalancingService::start(
                    metrics.clone(),
                    thread_load_window_size,
                    thread_load_threshold,
                ),
                cross_thread_ref_data_service: CrossThreadRefDataRepository::new(data_dir),
                dirty_hack__appended_blocks: FixedSizeHashSet::new(DIRTY_HACK__CACHE_SIZE),
                dirty_hack__finalized_blocks: FixedSizeHashSet::new(DIRTY_HACK__CACHE_SIZE),
                dirty_hack__invalidated_blocks: FixedSizeHashSet::new(DIRTY_HACK__CACHE_SIZE),
            })),
            metrics,
        }
    }

    pub fn exec<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Container) -> R,
    {
        let mut services = self.container.lock().expect("Can not be poisoned");
        f(&mut services)
    }

    pub fn on_block_appended(&mut self, _block: &AckiNackiBlock) {
        // No actions.
    }

    pub fn on_block_finalized<TOptimisticState>(
        &mut self,
        block: &AckiNackiBlock,
        state: &mut TOptimisticState,
    ) where
        TOptimisticState: OptimisticState,
    {
        let block_identifier: BlockIdentifier = block.identifier();
        // let parent_block_identifier: BlockIdentifier = block.parent();
        let thread_identifier: ThreadIdentifier = block.get_common_section().thread_id;
        let threads_table = state.get_produced_threads_table().clone();
        tracing::trace!("handling on_block_finalized: {:?}", &block_identifier);

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
}
