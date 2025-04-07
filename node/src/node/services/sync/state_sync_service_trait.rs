// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use core::fmt::Display;

use serde::Deserialize;
use serde::Serialize;
use telemetry_utils::mpsc::InstrumentedSender;

use crate::block_keeper_system::BlockKeeperSet;
use crate::message_storage::MessageDurableStorage;
use crate::node::services::statistics::median_descendants_chain_length_to_meet_threshold::BlockStatistics;
use crate::repository::CrossThreadRefData;
use crate::repository::Repository;

pub trait StateSyncService {
    type ResourceAddress: Serialize + for<'a> Deserialize<'a> + Clone + PartialEq + Display;
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
    fn add_share_state_task(
        &mut self,
        state: <Self::Repository as Repository>::OptimisticState,
        cross_thread_ref_data: Vec<CrossThreadRefData>,
        finalized_block_stats: BlockStatistics,
        bk_set: BlockKeeperSet,
        message_db: &MessageDurableStorage,
    ) -> anyhow::Result<Self::ResourceAddress>;

    fn add_load_state_task(
        &mut self,
        resource_address: Self::ResourceAddress,
        output: InstrumentedSender<anyhow::Result<(Self::ResourceAddress, Vec<u8>)>>,
    ) -> anyhow::Result<()>;

    fn generate_resource_address(
        &self,
        state: &<Self::Repository as Repository>::OptimisticState,
    ) -> anyhow::Result<Self::ResourceAddress>;
}
