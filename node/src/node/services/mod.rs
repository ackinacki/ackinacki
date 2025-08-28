// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::time::Duration;

pub mod attestations_target;
pub mod authority_switch;
pub mod block_processor;
pub mod finalization;
pub mod send_attestations;
pub mod statistics;
pub mod sync;
pub mod validation;

pub(crate) const PULSE_IDLE_TIMEOUT: Duration = Duration::from_millis(50);
