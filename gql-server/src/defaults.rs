// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
pub const PATH_TO_DB: &str = "data/bm-archive.db";
pub const LISTEN: &str = "127.0.0.1:3000";

pub const QUERY_BATCH_SIZE: u16 = 50;
pub const MAX_POOL_CONNECTIONS: u32 = 15;
// The number of simultaneously attached databases is limited to SQLITE_MAX_ATTACHED
// which is 10 including `main`
pub const MAX_ATTACHED_DB: u16 = 9;

pub const THREAD_ID_LENGTH: usize = 68;
