// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub mod bp_resolver;
pub mod key_handling;

use std::str::FromStr;

use opentelemetry::trace::TracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;
pub const TIMING_TARGET: &str = "timing";

fn default_verbose_filter() -> tracing_subscriber::EnvFilter {
    let (tvm_trace_level, builder_trace_level) =
        if cfg!(feature = "tvm_tracing") { ("trace", "trace") } else { ("off", "info") };
    tracing_subscriber::EnvFilter::new(format!(
        "gossip=trace,\
            http_server=trace,\
            block_manager=trace,\
            node=trace,\
            executor={tvm_trace_level},\
            network=trace,\
            tvm={tvm_trace_level},\
            builder={builder_trace_level},\
            database=info,\
            sqlite=trace,\
            message_router=trace,\
            transport_layer=trace"
    ))
}

fn default_non_verbose_filter() -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::new(
        "gossip=info,\
            http_server=info,\
            block_manager=trace,\
            node=trace,\
            executor=off,\
            network=trace,\
            tvm=off,\
            builder=off,\
            database=off,\
            sqlite=warn,\
            message_router=info,\
            transport_layer=trace",
    )
}

fn default_filter() -> tracing_subscriber::EnvFilter {
    if std::env::var("NODE_VERBOSE").is_ok() {
        default_verbose_filter()
    } else {
        default_non_verbose_filter()
    }
}

pub fn init_tracing() {
    let filter = if std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV).is_ok() {
        tracing_subscriber::EnvFilter::from_default_env()
    } else {
        default_filter()
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
