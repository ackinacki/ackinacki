// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub mod bp_resolver;
pub mod key_handling;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn init_tracing() {
    // Init tracing
    let filter = if std::env::var("NODE_VERBOSE").is_ok() {
        tracing_subscriber::filter::Targets::new()
            .with_target("a7i_gossip", LevelFilter::TRACE)
            .with_target("a7i_http", LevelFilter::TRACE)
            .with_target("a7i_litenode", LevelFilter::TRACE)
            .with_target("node", LevelFilter::TRACE)
            .with_target("node", LevelFilter::TRACE)
            .with_target("executor", LevelFilter::OFF)
            .with_target("network", LevelFilter::TRACE)
            .with_target("tvm", LevelFilter::OFF)
            .with_target("builder", LevelFilter::INFO)
            .with_target("database", LevelFilter::INFO)
            .with_target("sqlite", LevelFilter::TRACE)
            .with_target("message_router", LevelFilter::TRACE)
    } else {
        tracing_subscriber::filter::Targets::new()
            .with_target("a7i_gossip", LevelFilter::INFO)
            .with_target("a7i_http", LevelFilter::INFO)
            .with_target("a7i_litenode", LevelFilter::TRACE)
            .with_target("node", LevelFilter::TRACE)
            .with_target("node", LevelFilter::OFF)
            .with_target("executor", LevelFilter::OFF)
            .with_target("network", LevelFilter::TRACE)
            .with_target("tvm", LevelFilter::OFF)
            .with_target("builder", LevelFilter::OFF)
            .with_target("database", LevelFilter::OFF)
            .with_target("sqlite", LevelFilter::WARN)
            .with_target("message_router", LevelFilter::INFO)
    };
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_thread_ids(true)
                .with_ansi(false)
                .with_writer(std::io::stderr),
        )
        .with(filter)
        .init();
}
