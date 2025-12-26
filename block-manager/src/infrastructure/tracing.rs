// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn init_tracing() {
    // Init tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_line_number(true)
                .with_ansi(false)
                .with_writer(std::io::stderr),
        )
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}
