// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

mod context;
mod fixed_size_hash_set;
mod service;
mod thread_chain;
pub use service::DependenciesTrackingService;

use crate::types::BlockIdentifier;

pub type FinalizedBlocksSet = fixed_size_hash_set::FixedSizeHashSet<BlockIdentifier>;
pub type InvalidatedBlocksSet = fixed_size_hash_set::FixedSizeHashSet<BlockIdentifier>;
