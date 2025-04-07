use opentelemetry::metrics::Histogram;
use opentelemetry::metrics::Meter;

#[derive(Clone)]
pub struct RoutingMetrics {
    ext_msg_delivery_duration: Histogram<u64>,
}

impl RoutingMetrics {
    pub fn new(meter: &Meter) -> Self {
        RoutingMetrics {
            ext_msg_delivery_duration: meter
                .u64_histogram("node_ext_msg_delivery_duration")
                .build(),
        }
    }

    pub fn report_ext_msg_delivery_duration(&self, value: u64) {
        self.ext_msg_delivery_duration.record(value, &[]);
    }
}
