// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub mod block_subscriber;
pub mod bm_contract_root;
mod bp_resolver;
pub mod cli;
pub mod events;
pub mod executor;
pub mod message_types;
pub mod metrics;
mod rest_api_routes;
pub mod signals;
pub mod state;
pub mod tracing;
pub const DEFAULT_BP_PORT: u16 = 8500;

// 1 process: reads raw_blocks, has seq_no of all threads and for all blocks since start of reading
//  - send event each block processed
// 2 process: download state (lazy dummy implementation)
//  - send even on download complete
// 3 process: "business logic"
//  - listen to events from process 1 and 2
// check if for all accounts state is ready or not
// -- if for all -- yes:
//        we are in sync and can process user requests -> send event to gql_server to start accepting requests
// -- if not -- wait

// TODO:
// 1) new lite_block structure (stub)
//      in block keeper support lite_block structure
//
// 2) process 1 + process 2 + process 3 (only fish)
