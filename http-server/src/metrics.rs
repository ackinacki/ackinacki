use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Histogram;
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use telemetry_utils::out_of_bounds_guard;

#[derive(Clone)]
pub struct RoutingMetrics {
    ext_msg_delivery_duration: Histogram<u64>,
    ext_msg_processing_duration: Histogram<u64>,
    ext_msg_feedback: Counter<u64>,
    boc_by_address_response: Histogram<u64>,
    state_request: Counter<u64>,
}

impl RoutingMetrics {
    pub fn new(meter: &Meter) -> Self {
        let boundaries = vec![
            50.0, 100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0, 1500.0,
            3000.0, 5000.0, 10000.0, 15000.0,
        ];
        RoutingMetrics {
            ext_msg_delivery_duration: meter
                .u64_histogram("node_ext_msg_delivery_duration")
                .build(),
            ext_msg_processing_duration: meter
                .u64_histogram("node_ext_msg_processing_duration")
                .with_boundaries(boundaries.clone())
                .build(),
            ext_msg_feedback: meter.u64_counter("node_ext_msg_feedback").build(),
            boc_by_address_response: meter
                .u64_histogram("node_boc_by_address_response")
                .with_boundaries(boundaries)
                .build(),
            state_request: meter.u64_counter("node_state_req_recv").build(),
        }
    }

    pub fn report_ext_msg_delivery_duration(&self, value: u64) {
        out_of_bounds_guard!(value, "ext_msg_delivery_duration");
        self.ext_msg_delivery_duration.record(value, &[]);
    }

    pub fn report_ext_msg_processing_duration(&self, value: u64, http_code: u16) {
        out_of_bounds_guard!(value, "ext_msg_processing_duration");
        self.ext_msg_processing_duration
            .record(value, &[KeyValue::new("code", http_code.to_string())]);
    }

    pub fn report_boc_by_address_response(&self, value: u64, http_code: u16) {
        out_of_bounds_guard!(value, "boc_by_address_response");
        self.boc_by_address_response.record(value, &[KeyValue::new("code", http_code.to_string())]);
    }

    pub fn report_state_request(&self) {
        self.state_request.add(1, &[]);
    }

    pub fn report_ext_msg_feedback(&self, err_type: &str) {
        self.ext_msg_feedback.add(1, &[KeyValue::new("err", err_type.to_string())]);
    }
}
