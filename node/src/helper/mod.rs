// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub mod bp_resolver;
pub mod key_handling;

use std::str::FromStr;

use opentelemetry::trace::TracerProvider;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
pub const TIMING_TARGET: &str = "timing";

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

    if let Ok(Ok(targets)) =
        std::env::var("TELEMETRY_LOG").map(|x| tracing_subscriber::filter::Targets::from_str(&x))
    {
        let telemetry_layer = tracing_opentelemetry::layer()
            .with_tracer(init_tracer())
            .with_filter(tracing_subscriber::filter::filter_fn(|x| x.is_span()))
            .with_filter(targets);
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .compact()
                    .with_thread_ids(true)
                    .with_ansi(false)
                    .with_writer(std::io::stderr)
                    .with_filter(filter),
            )
            .with(telemetry_layer)
            .init();
    } else {
        let telemetry_layer = tracing_opentelemetry::layer()
            .with_tracer(init_tracer())
            .with_filter(tracing_subscriber::filter::filter_fn(|x| x.is_span()));
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .compact()
                    .with_thread_ids(true)
                    .with_ansi(false)
                    .with_writer(std::io::stderr)
                    .with_filter(filter),
            )
            .with(telemetry_layer)
            .init();
    }
}

pub fn shutdown_tracing() {
    opentelemetry::global::shutdown_tracer_provider();
}

pub fn init_tracer() -> opentelemetry_sdk::trace::Tracer {
    let default_service_name = opentelemetry::KeyValue::new("service.name", "acki-nacki-node");

    let resource = opentelemetry_sdk::Resource::new(vec![default_service_name.clone()])
        .merge(&opentelemetry_sdk::Resource::default());

    let service_name = resource
        .get(default_service_name.key.clone())
        .unwrap_or_else(|| default_service_name.value.clone())
        .to_string();

    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to build OTLP exporter");

    let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(otlp_exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(resource)
        .build();

    tracer_provider.tracer(service_name)
}
