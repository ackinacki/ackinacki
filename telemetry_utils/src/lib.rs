// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use opentelemetry::metrics::Meter;
use opentelemetry::metrics::ObservableCounter;
use opentelemetry::metrics::ObservableGauge;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::PeriodicReader;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::runtime::Tokio;

pub mod instrumented_channel_ext;
pub mod mpsc;

#[macro_export]
macro_rules! out_of_bounds_guard {
    // Version with explicit bounds
    ($value:expr,($low:expr, $high:expr), $metric_name:expr) => {{
        #[allow(unused_comparisons)]
        if !($low..=$high).contains(&$value) {
            tracing::warn!(
                "Metric {}: value {} is out of bounds {}..={}",
                $metric_name,
                $value,
                $low,
                $high
            );
            return;
        }
    }};

    // Version with default bounds.
    ($value:expr, $metric_name:expr) => {
        out_of_bounds_guard!($value, (0, 100_000), $metric_name);
    };
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}
pub fn now_micros() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}
pub fn millis_from_now(start_ms: u64) -> Result<u64, String> {
    let now = now_ms();
    if now >= start_ms {
        Ok(now - start_ms)
    } else if start_ms - now < 5 {
        // we think that a 5ms difference in clock synchronization is acceptable
        Ok(0)
    } else {
        Err("System clock out of sync. Please check NTP or system time settings.".to_string())
    }
}

pub fn get_metrics_endpoint() -> Option<String> {
    std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .ok()
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

#[derive(Clone)]
pub struct TokioMetrics {
    _tokio_num_alive_tasks: ObservableGauge<u64>,
    _tokio_global_queue_depth: ObservableGauge<u64>,
    _tokio_spawned_tasks_count: ObservableCounter<u64>,
}

impl TokioMetrics {
    pub fn new(meter: &Meter) -> Self {
        Self {
            _tokio_num_alive_tasks: meter
                .u64_observable_gauge("node_tokio_num_alive_tasks")
                .with_callback(move |observer| {
                    observer.observe(
                        tokio::runtime::Handle::current().metrics().num_alive_tasks() as u64,
                        &[],
                    )
                })
                .build(),

            _tokio_global_queue_depth: meter
                .u64_observable_gauge("node_tokio_global_queue_depth")
                .with_callback(move |observer| {
                    observer.observe(
                        tokio::runtime::Handle::current().metrics().global_queue_depth() as u64,
                        &[],
                    )
                })
                .build(),

            _tokio_spawned_tasks_count: meter
                .u64_observable_counter("node_tokio_spawned_tasks_count")
                .with_callback(move |observer| {
                    observer.observe(
                        tokio::runtime::Handle::current().metrics().spawned_tasks_count(),
                        &[],
                    );
                })
                .build(),
        }
    }
}
