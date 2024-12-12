// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub mod bp_resolver;
pub mod key_handling;

use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub fn init_tracing() {
    // Init tracing
    let (tvm_trace_level, builder_trace_level) = if cfg!(feature = "tvm_tracing") {
        (LevelFilter::TRACE, LevelFilter::TRACE)
    } else {
        (LevelFilter::OFF, LevelFilter::INFO)
    };
    let filter = if std::env::var("NODE_VERBOSE").is_ok() {
        tracing_subscriber::filter::Targets::new()
            .with_target("gossip", LevelFilter::TRACE)
            .with_target("http_server", LevelFilter::TRACE)
            .with_target("block_manager", LevelFilter::TRACE)
            .with_target("node", LevelFilter::TRACE)
            .with_target("executor", tvm_trace_level)
            .with_target("network", LevelFilter::TRACE)
            .with_target("tvm", tvm_trace_level)
            .with_target("builder", builder_trace_level)
            .with_target("database", LevelFilter::INFO)
            .with_target("sqlite", LevelFilter::TRACE)
            .with_target("message_router", LevelFilter::TRACE)
            .with_target("transport_layer", LevelFilter::TRACE)
    } else {
        tracing_subscriber::filter::Targets::new()
            .with_target("gossip", LevelFilter::INFO)
            .with_target("http_server", LevelFilter::INFO)
            .with_target("block_manager", LevelFilter::TRACE)
            .with_target("node", LevelFilter::TRACE)
            .with_target("executor", LevelFilter::OFF)
            .with_target("network", LevelFilter::TRACE)
            .with_target("tvm", LevelFilter::OFF)
            .with_target("builder", LevelFilter::OFF)
            .with_target("database", LevelFilter::OFF)
            .with_target("sqlite", LevelFilter::WARN)
            .with_target("message_router", LevelFilter::INFO)
            .with_target("transport_layer", LevelFilter::TRACE)
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
