// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub mod account_boc_loader;
pub mod bp_resolver;
pub mod key_handling;
pub mod metrics;

use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::OnceLock;

use opentelemetry::global::ObjectSafeSpan;
use opentelemetry::trace::noop::NoopTracer;
use opentelemetry::trace::noop::NoopTracerProvider;
use opentelemetry::trace::SpanBuilder;
use opentelemetry::trace::TraceId;
use opentelemetry::trace::Tracer;
use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::trace;
use opentelemetry_sdk::Resource;
use telemetry_utils::get_metrics_endpoint;
use telemetry_utils::init_meter_provider;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tvm_types::Sha256;

use crate::helper::metrics::Metrics;
use crate::node::NodeIdentifier;
use crate::types::BlockIdentifier;

pub const TIMING_TARGET: &str = "timing";

pub static SHUTDOWN_FINALIZATION_FLAG: OnceLock<bool> = OnceLock::new();
pub static FINALIZATION_LOOPS_COUNTER: AtomicU32 = AtomicU32::new(0);
pub static SHUTDOWN_FLAG: OnceLock<bool> = OnceLock::new();

fn verbose_filter() -> tracing_subscriber::EnvFilter {
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
            transport_layer=trace,\
            transport_layer=trace,\
            monit=trace,\
            ext_messages=trace"
    ))
}

pub fn init_tracing() -> (Option<Metrics>, WorkerGuard) {
    let filter = if std::env::var("NODE_VERBOSE").is_ok() {
        verbose_filter()
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("error,monit=trace"))
    };

    // According to OpenTelemetry Specification:
    // The following environment variables configure the OTLP exporter:
    // `OTEL_EXPORTER_OTLP_ENDPOINT`:
    // The default endpoint used for all OTLP signal types (traces, metrics, logs) if more specific ones are not provided.
    //
    // `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`:
    // Sets the endpoint just for traces.
    //
    // `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`:
    // Sets the endpoint just for metrics.

    let traces_endpoint = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .ok();

    let (non_blocking, guard) = tracing_appender::non_blocking(std::io::stderr());

    let fmt_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_thread_ids(true)
        .with_ansi(false)
        .with_writer(non_blocking)
        .with_filter(filter);

    let registry = tracing_subscriber::registry().with(fmt_layer);

    if let Some(endpoint) = traces_endpoint {
        println!("Using OTLP traces endpoint: {endpoint}");
        let telemetry_layer = tracing_opentelemetry::layer()
            .with_tracer(init_tracer(endpoint))
            .with_filter(tracing_subscriber::filter::filter_fn(|x| x.is_span()));
        registry.with(telemetry_layer).init();
    } else {
        println!("No OTEL exporter endpoint found, using noop tracer.");
        registry.init();
    }

    // Init metrics
    if let Some(endpoint) = get_metrics_endpoint() {
        tracing::info!("Using OTLP metrics endpoint: {endpoint}");
        opentelemetry::global::set_meter_provider(init_meter_provider());
        (Some(Metrics::new(&opentelemetry::global::meter("node"))), guard)
    } else {
        tracing::info!("No OTEL exporter endpoint found, metrics not collected.");
        opentelemetry::global::set_meter_provider(SdkMeterProvider::builder().build());
        (None, guard)
    }
}

pub fn shutdown_tracing(tracing_guard: WorkerGuard) {
    tracing::trace!("shutting down tracing");
    opentelemetry::global::shutdown_tracer_provider();
    drop(tracing_guard);
}

pub fn init_tracer(endpoint: String) -> opentelemetry_sdk::trace::Tracer {
    let default_service_name = KeyValue::new("service.name", "acki-nacki-node");

    let resource = Resource::new(vec![default_service_name.clone()]).merge(&Resource::default());

    tracing::info!("Using OTLP traces endpoint: {endpoint}");
    let otlp_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .expect("Failed to build OTLP exporter");

    let tracer_provider = trace::TracerProvider::builder()
        .with_batch_exporter(otlp_exporter, Tokio)
        .with_resource(resource)
        .build();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    tracer_provider.tracer("node")
}

pub fn init_noop_tracer() -> NoopTracer {
    let noop_tracer_provider = NoopTracerProvider::new();
    opentelemetry::global::set_tracer_provider(noop_tracer_provider.clone());
    noop_tracer_provider.tracer("node")
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

pub fn get_temp_file_path(parent_path: &Path) -> PathBuf {
    let mut path;
    while {
        let tmp_file_name = format!("_{}.tmp", rand::random::<u64>());
        path = parent_path.join(tmp_file_name);
        path.exists()
    } {}
    path
}

pub fn start_shutdown() {
    tracing::info!("starting shutdown");
    SHUTDOWN_FINALIZATION_FLAG.set(true).expect("Failed to set shutdown finalization flag");
    loop {
        {
            let counter = FINALIZATION_LOOPS_COUNTER.load(Ordering::Relaxed);
            tracing::trace!("FINALIZATION_LOOPS_COUNTER = {}", counter);
            if counter == 0 {
                break;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    tracing::info!("set shutdown flag");
    SHUTDOWN_FLAG.set(true).expect("Failed to set shutdown flag");
}

pub fn calc_file_hash<P: AsRef<Path>>(path: P) -> anyhow::Result<String> {
    let bytes = std::fs::read(path.as_ref())?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(hex::encode(hasher.finalize()))
}
