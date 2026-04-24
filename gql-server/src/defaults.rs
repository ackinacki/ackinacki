// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
pub const PATH_TO_DB: &str = "data/bm-archive.db";
pub const LISTEN: &str = "127.0.0.1:3000";

pub const QUERY_BATCH_SIZE: u16 = 50;
pub const MAX_POOL_CONNECTIONS: u32 = 20;
// The number of simultaneously attached databases is limited to SQLITE_MAX_ATTACHED
// which is 10 including `main`
pub const MAX_ATTACHED_DB: u16 = 9;

pub const THREAD_ID_LENGTH: usize = 68;

/// SQLite query execution timeout in seconds.
/// Queries exceeding this limit will be interrupted with SQLITE_INTERRUPT.
pub const DEFAULT_SQLITE_QUERY_TIMEOUT_SECS: u64 = 2;

/// Pool acquire timeout in seconds.
/// Requests waiting longer than this for a free connection will fail fast.
pub const DEFAULT_ACQUIRE_TIMEOUT_SECS: u64 = 5;

/// SQLite mmap_size PRAGMA value in bytes (0 = disabled).
pub const DEFAULT_SQLITE_MMAP_SIZE: i64 = 161_061_273_600; // ~150 GB

/// SQLite cache_size PRAGMA value.
/// Negative = size in KiB, positive = number of pages.
pub const DEFAULT_SQLITE_CACHE_SIZE: i64 = -1_000_000; // ~1 GB

// Serde default functions (used by GqlServerConfig deserialization).
pub fn max_pool_connections() -> u32 {
    MAX_POOL_CONNECTIONS
}
pub fn acquire_timeout_secs() -> u64 {
    DEFAULT_ACQUIRE_TIMEOUT_SECS
}
pub fn sqlite_query_timeout_secs() -> u64 {
    DEFAULT_SQLITE_QUERY_TIMEOUT_SECS
}
pub fn max_attached_db() -> u16 {
    MAX_ATTACHED_DB
}
pub fn sqlite_mmap_size() -> i64 {
    DEFAULT_SQLITE_MMAP_SIZE
}
pub fn sqlite_cache_size() -> i64 {
    DEFAULT_SQLITE_CACHE_SIZE
}
