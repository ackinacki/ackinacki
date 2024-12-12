// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use core::fmt::Display;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::mpsc::Sender;

use serde::Deserialize;
use serde::Serialize;

use crate::block_keeper_system::BlockKeeperSet;
use crate::repository::Repository;
use crate::types::BlockSeqNo;
use crate::types::ThreadIdentifier;

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
        block_producer_groups: HashMap<
            ThreadIdentifier,
            Vec<<Self::Repository as Repository>::NodeIdentifier>,
        >,
        block_keeper_set: HashMap<ThreadIdentifier, BTreeMap<BlockSeqNo, BlockKeeperSet>>,
    ) -> anyhow::Result<Self::ResourceAddress>;

    fn add_load_state_task(
        &mut self,
        resource_address: Self::ResourceAddress,
        output: Sender<anyhow::Result<(Self::ResourceAddress, Vec<u8>)>>,
    ) -> anyhow::Result<()>;

    fn generate_resource_address(
        &self,
        state: &<Self::Repository as Repository>::OptimisticState,
    ) -> anyhow::Result<Self::ResourceAddress>;
}
