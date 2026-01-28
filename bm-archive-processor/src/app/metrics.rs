// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Gauge;
use opentelemetry::metrics::Meter;
use telemetry_utils::init_meter_provider;

const METRIC_PREFIX: &str = "bmap";

#[derive(Clone)]
pub struct Metrics {
    pub anchor_timestamp: Gauge<u64>,
    pub incoming_success: Counter<u64>,
    pub incoming_fail: Counter<u64>,
    pub last_merged_timestamp: Gauge<u64>,
}

impl Metrics {
    pub fn new(meter: &Meter) -> Self {
        let meter_provider = init_meter_provider();
        opentelemetry::global::set_meter_provider(meter_provider);
        Self {
            anchor_timestamp: meter.u64_gauge(prefix("anchor_timestamp")).build(),
            incoming_success: meter.u64_counter(prefix("incoming_success")).build(),
            incoming_fail: meter.u64_counter(prefix("incomming_fail")).build(),
            last_merged_timestamp: meter.u64_gauge(prefix("last_merged_timestamp")).build(),
        }
    }
}

fn prefix(suffix: &str) -> String {
    format!("{METRIC_PREFIX}_{suffix}")
}
