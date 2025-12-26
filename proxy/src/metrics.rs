use opentelemetry::metrics::Gauge;
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;

pub struct ProxyMetrics {
    build_info: Gauge<u64>,
}
impl ProxyMetrics {
    pub fn new(meter: &Meter) -> Self {
        ProxyMetrics { build_info: meter.u64_gauge("node_build_info").build() }
    }

    pub fn report_build_info(&self) {
        let version = env!("CARGO_PKG_VERSION");
        let commit = env!("BUILD_GIT_COMMIT");
        self.build_info
            .record(1, &[KeyValue::new("version", version), KeyValue::new("commit", commit)]);
    }
}
