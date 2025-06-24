// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub mod bm_license_loader;
pub mod bp_resolver;
pub mod key_handling;
pub mod metrics;

use std::str::FromStr;
use std::time::Duration;

use opentelemetry::global::ObjectSafeSpan;
use opentelemetry::trace::SpanBuilder;
use opentelemetry::trace::TraceId;
use opentelemetry::trace::Tracer;
use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::runtime::Tokio;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

use crate::helper::metrics::Metrics;
use crate::node::NodeIdentifier;
use crate::types::BlockIdentifier;

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
    // tracing_subscriber::EnvFilter::new(format!(""))
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

pub fn init_tracing() -> Option<Metrics> {
    let filter = if std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV).is_ok() {
        tracing_subscriber::EnvFilter::from_default_env()
    } else {
        default_filter()
    };

    let meter_provider = init_meter_provider();
    opentelemetry::global::set_meter_provider(meter_provider);
    let metrics = Metrics::new(&opentelemetry::global::meter("node"));

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
    Some(metrics)
}

pub fn shutdown_tracing() {
    tracing::trace!("shutting down tracing");
    opentelemetry::global::shutdown_tracer_provider();
}

pub fn init_tracer() -> opentelemetry_sdk::trace::Tracer {
    let default_service_name = KeyValue::new("service.name", "acki-nacki-node");

    let resource = opentelemetry_sdk::Resource::new(vec![default_service_name.clone()])
        .merge(&opentelemetry_sdk::Resource::default());

    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to build OTLP exporter");

    let tracer_provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(otlp_exporter, Tokio)
        .with_resource(resource)
        .build();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    tracer_provider.tracer("node")
}

pub fn init_meter_provider() -> SdkMeterProvider {
    let default_service_name = KeyValue::new("service.name", "acki-nacki-node");

    let resource = opentelemetry_sdk::Resource::new(vec![default_service_name.clone()])
        .merge(&opentelemetry_sdk::Resource::default());

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to build OTLP metrics exporter");

    SdkMeterProvider::builder()
        .with_reader(
            PeriodicReader::builder(metric_exporter, Tokio)
                .with_interval(Duration::from_secs(30))
                .with_timeout(Duration::from_secs(5))
                .build(),
        )
        .with_resource(resource)
        .build()
}

pub fn block_flow_trace<const N: usize>(
    name: impl AsRef<str>,
    block_id: &BlockIdentifier,
    node_id: &NodeIdentifier,
    fields: [(&str, &str); N],
) {
    block_flow_trace_with_time(None, name, block_id, node_id, fields);
}

pub fn block_flow_trace_with_time<const N: usize>(
    time: Option<std::time::SystemTime>,
    name: impl AsRef<str>,
    block_id: &BlockIdentifier,
    node_id: &NodeIdentifier,
    fields: [(&str, &str); N],
) {
    let tracer = opentelemetry::global::tracer_provider().tracer("node");
    let buf = block_id.as_rng_seed();
    let mut trace_id = [0u8; 16];
    trace_id.copy_from_slice(&buf[0..16]);

    let mut attributes = Vec::with_capacity(1 + N);
    attributes.push(KeyValue::new("node", node_id.to_string()));
    if N > 0 {
        for (k, v) in fields.into_iter() {
            attributes.push(KeyValue::new(k.to_string(), v.to_string()));
        }
    }

    let mut builder = SpanBuilder::from_name(format!("block flow: {}", name.as_ref()))
        .with_trace_id(TraceId::from_bytes(trace_id))
        .with_attributes(attributes);
    if let Some(time) = time {
        builder = builder.with_start_time(time);
    }
    let mut span = tracer.build(builder);
    span.end()
}
