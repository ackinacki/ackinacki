use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use serde::Deserialize;
use serde::Serialize;

use crate::bls::BLSSignatureScheme;
use crate::multithreading::load_balancing_service::LoadBalancingService;
use crate::multithreading::routing::service::RoutingService;
use crate::multithreading::thread_synchrinization_service::ThreadSyncService;
use crate::multithreading::threads_tracking_service::AppendedBlockData;
use crate::multithreading::threads_tracking_service::ThreadsTrackingService;
use crate::repository::optimistic_state::OptimisticState;
use crate::repository::CrossThreadRefDataRepository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;
use crate::types::ThreadsTable;
use crate::utilities::FixedSizeHashSet;

const DIRTY_HACK__CACHE_SIZE: usize = 10000; // 100 blocks for 100 threads.

#[derive(Clone)]
pub struct SharedServices {
    container: Arc<Mutex<Container>>,
}

#[allow(dead_code, non_snake_case)]
pub struct Container {
    // Note: dependency_tracking is eplaced with ThreadReferencesState in optimistic states
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
        Self::start(router, PathBuf::from("./data-dir-test"))
    }

    pub fn start(router: RoutingService, data_dir: PathBuf) -> Self {
        Self {
            container: Arc::new(Mutex::new(Container {
                router,
                // dependency_tracking: DependenciesTrackingService::start(()),
                thread_sync: ThreadSyncService::start(()),
                threads_tracking: ThreadsTrackingService::start(),
                load_balancing: LoadBalancingService::start(),
                cross_thread_ref_data_service: CrossThreadRefDataRepository::new(data_dir),
                dirty_hack__appended_blocks: FixedSizeHashSet::new(DIRTY_HACK__CACHE_SIZE),
                dirty_hack__finalized_blocks: FixedSizeHashSet::new(DIRTY_HACK__CACHE_SIZE),
                dirty_hack__invalidated_blocks: FixedSizeHashSet::new(DIRTY_HACK__CACHE_SIZE),
            })),
        }
    }

    pub fn exec<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Container) -> R,
    {
        let mut services = self.container.lock().expect("Can not be poisoned");
        f(&mut services)
    }

    pub fn on_block_appended<TBLSSignatureScheme>(
        &mut self,
        block: &AckiNackiBlock<TBLSSignatureScheme>,
    ) where
        TBLSSignatureScheme: BLSSignatureScheme,
        TBLSSignatureScheme::Signature:
            Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
    {
        let block_identifier: BlockIdentifier = block.identifier();
        let parent_block_identifier: BlockIdentifier = block.parent();
        let thread_identifier: ThreadIdentifier = block.get_common_section().thread_id;
        // let block_seq_no: BlockSeqNo = block.seq_no();
        // let refs: Vec<BlockIdentifier> = block.get_common_section().refs.clone();
        let threads_table: Option<ThreadsTable> = block.get_common_section().threads_table.clone();
        self.exec(|services| {
            if services.dirty_hack__appended_blocks.contains(&block_identifier) {
                return;
            }
            services.dirty_hack__appended_blocks.insert(block_identifier.clone());
            /*
            match services.dependency_tracking.append(BlockData {
                parent_block_identifier: parent_block_identifier.clone(),
                block_identifier: block_identifier.clone(),
                block_seq_no,
                thread_identifier,
                refs,
            }) {
                Ok(_events) => {
                    // TODO:
                }
                Err(e) => {
                    tracing::trace!("dependency_tracking append error: {:?}", e);
                    unimplemented!()
                }
            }
            */
            match services.threads_tracking.handle_block_appended(AppendedBlockData {
                parent_block_identifier,
                block_identifier,
                thread_identifier,
                threads_table,
            }) {
                Ok(()) => {}
                Err(e) => {
                    tracing::trace!("threads_tracking handle_block_appended error: {:?}", e);
                    unimplemented!()
                }
            }
        });
    }

    pub fn on_block_finalized<TBLSSignatureScheme, TOptimisticState>(
        &mut self,
        block: &AckiNackiBlock<TBLSSignatureScheme>,
        state: &mut TOptimisticState,
    ) where
        TBLSSignatureScheme: BLSSignatureScheme,
        TBLSSignatureScheme::Signature:
            Serialize + for<'a> Deserialize<'a> + Clone + Send + Sync + 'static,
        TOptimisticState: OptimisticState,
    {
        let block_identifier: BlockIdentifier = block.identifier();
        // let parent_block_identifier: BlockIdentifier = block.parent();
        let thread_identifier: ThreadIdentifier = block.get_common_section().thread_id;

        tracing::trace!("handling on_block_finalized: {:?}", &block_identifier);

        self.exec(|services| {
            if services.dirty_hack__finalized_blocks.contains(&block_identifier) {
                return;
            }
            services.dirty_hack__finalized_blocks.insert(block_identifier.clone());

            /*
            match services.dependency_tracking.on_block_finalized(
                parent_block_identifier.clone(),
                thread_identifier,
                block_identifier.clone(),
            ) {
                Ok(_events) => {
                    // TODO:
                }
                Err(_e) => unimplemented!(),
            }
            */
            services.thread_sync.on_block_finalized(
                &block_identifier,
                &thread_identifier,
            ).expect("Must work");
            match services.threads_tracking.handle_block_finalized(
                block_identifier.clone(),
                thread_identifier,
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
