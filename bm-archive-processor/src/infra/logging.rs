// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::io::IsTerminal;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn init_tracing() -> anyhow::Result<()> {
    let stdout_is_terminal = std::io::stdout().is_terminal();

    Ok(tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stdout)
                .with_ansi(stdout_is_terminal),
        )
        .try_init()?)
}
