// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::BTreeMap;
use std::sync::Arc;

use telemetry_utils::mpsc::InstrumentedSender;

use crate::repository::optimistic_state::OptimisticStateImpl;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::BlockIdentifier;
use crate::types::ThreadIdentifier;

pub trait StateSyncService {
    type Repository: Repository;
    // An assumption here:
    // We expect to have an implementation that will be able
    // to convert or reuse TVM hashes to address IPFS resources:
    // ref: https://github.com/multiformats/rust-multihash
    // In the later implementation this function should be modified:
    // - to take state hash that is created internally by the node
    // - accept a data storage as a parameters to be able to take state directly
    //   from there.
    // - use data storage to snapshot state and then publish it on ipfs.
    fn save_state_for_sharing(
        &self,
        block_id: &BlockIdentifier,
        thread_id: &ThreadIdentifier,
        min_state: Option<Arc<OptimisticStateImpl>>,
    ) -> anyhow::Result<()>;

    fn add_load_state_task(
        &mut self,
        resource_address: BTreeMap<ThreadIdentifier, BlockIdentifier>,
        repository: RepositoryImpl,
        output: InstrumentedSender<anyhow::Result<BTreeMap<ThreadIdentifier, BlockIdentifier>>>,
    ) -> anyhow::Result<()>;

    fn reset_sync(&self);
}
