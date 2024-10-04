// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use tokio::signal::unix::signal;
use tokio::signal::unix::Signal;
use tokio::signal::unix::SignalKind;

/// Setup signals
pub fn setup_signals() -> anyhow::Result<(Signal, Signal)> {
    Ok((
        // force vertical format
        signal(SignalKind::interrupt())?,
        signal(SignalKind::terminate())?,
    ))
}
