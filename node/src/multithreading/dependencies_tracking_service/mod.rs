// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

mod block_data;
mod errors;
mod events;
pub use block_data::*;
pub use errors::*;
pub use events::*;

mod service;
mod thread_chain;
pub use service::DependenciesTrackingService;

use crate::types::BlockIdentifier;
use crate::utilities::FixedSizeHashSet;

pub type FinalizedBlocksSet = FixedSizeHashSet<BlockIdentifier>;
pub type InvalidatedBlocksSet = FixedSizeHashSet<BlockIdentifier>;
