// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
//! Test doubles for infrastructure clients, enabling testing without I/O.
//!
//! Use these in tests instead of conditional `dry_run` logic in production code.

pub mod dry_run_s3_client;
pub mod mock_db_client;
pub mod mock_file_system_client;
