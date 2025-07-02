use std::time::Duration;

// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
pub mod attestations_target;
pub mod block_processor;
pub mod db_serializer;
pub mod finalization;
pub mod send_attestations;
pub mod statistics;
pub mod sync;
pub mod validation;

pub(crate) const PULSE_IDLE_TIMEOUT: Duration = Duration::from_millis(50);
