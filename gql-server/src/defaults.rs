// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
pub const PATH_TO_DB: &str = "data/bm-archive.db";
pub const LISTEN: &str = "127.0.0.1:3000";

pub const QUERY_BATCH_SIZE: u16 = 50;
pub const MAX_POOL_CONNECTIONS: u32 = 20;
// SQLite attaches at most SQLITE_MAX_ATTACHED = 10 databases besides `main`.
// Nine go to rotated archives, the tenth to the history proofs store.
pub const MAX_ATTACHED_DB: u16 = 9;

/// File name of the history proofs store that block-manager writes next to
/// the main DB and never rotates. Same value as
/// `database::sqlite::history_proofs::HISTORY_PROOFS_DB`.
pub const HISTORY_PROOFS_DB_FILE: &str = "bm-history-proofs.db";

/// Schema name under which the history proofs store is attached.
pub const HISTORY_PROOFS_DB_ALIAS: &str = "history_proofs_db";

pub const THREAD_ID_LENGTH: usize = 68;

/// Thread 0, the only thread of the history proofs store. Same value as
/// `database::sqlite::history_proofs::THREAD_ZERO_HEX`.
pub const THREAD_ZERO: &str =
    "00000000000000000000000000000000000000000000000000000000000000000000";
const _: () = assert!(THREAD_ZERO.len() == THREAD_ID_LENGTH);

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

#[cfg(test)]
mod tests {
    #[test]
    fn thread_zero_is_the_one_of_the_history_proofs_store() {
        assert_eq!(super::THREAD_ZERO, database::sqlite::history_proofs::THREAD_ZERO_HEX);
    }

    #[test]
    fn history_proofs_file_name_is_the_one_block_manager_writes() {
        assert_eq!(
            super::HISTORY_PROOFS_DB_FILE,
            database::sqlite::history_proofs::HISTORY_PROOFS_DB
        );
    }
}
