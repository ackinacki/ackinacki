use std::sync::Arc;

use node_types::ThreadIdentifier;
use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Gauge;
use opentelemetry::metrics::Histogram;
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;

#[derive(Clone)]
pub struct StateAccountsMetrics(Arc<StateAccountsMetricsInner>);

struct StateAccountsMetricsInner {
    moved_from_tvm: Counter<u64>,
    aerospike_batch_write_duration: Histogram<u64>,
    aerospike_read_duration: Histogram<u64>,
    accumulator_write_queue_blocks: Gauge<u64>,
    accumulator_deferred_batches: Gauge<u64>,
    merkle_cache_write_duration: Histogram<u64>,
}

impl StateAccountsMetrics {
    pub fn new(meter: &Meter) -> Self {
        Self(Arc::new(StateAccountsMetricsInner {
            moved_from_tvm: meter.u64_counter("node_state_accounts_moved_from_tvm").build(),
            aerospike_batch_write_duration: meter
                .u64_histogram("node_state_accounts_aerospike_batch_write_duration")
                .with_boundaries(vec![
                    100.0,
                    200.0,
                    500.0,
                    1_000.0,
                    2_000.0,
                    5_000.0,
                    10_000.0,
                    20_000.0,
                    50_000.0,
                    100_000.0,
                    500_000.0,
                    1_000_000.0,
                ])
                .build(),
            aerospike_read_duration: meter
                .u64_histogram("node_state_accounts_aerospike_read_duration")
                .with_boundaries(vec![
                    50.0, 100.0, 200.0, 500.0, 1_000.0, 2_000.0, 5_000.0, 10_000.0, 20_000.0,
                    50_000.0, 100_000.0, 500_000.0,
                ])
                .build(),
            accumulator_write_queue_blocks: meter
                .u64_gauge("node_state_accounts_accumulator_write_queue_blocks")
                .build(),
            accumulator_deferred_batches: meter
                .u64_gauge("node_state_accounts_accumulator_deferred_batches")
                .build(),
            merkle_cache_write_duration: meter
                .u64_histogram("node_state_accounts_merkle_cache_write_duration")
                .with_boundaries(vec![
                    1.0, 5.0, 10.0, 20.0, 30.0, 50.0, 80.0, 100.0, 150.0, 200.0, 500.0, 1_000.0,
                    5_000.0,
                ])
                .build(),
        }))
    }

    pub fn report_moved_from_tvm(&self, value: usize, thread_id: &ThreadIdentifier) {
        if value == 0 {
            return;
        }
        self.0.moved_from_tvm.add(value as u64, &[KeyValue::new("thread", thread_id.to_string())]);
    }

    pub fn report_aerospike_batch_write_duration_micros(&self, value: u64) {
        self.0.aerospike_batch_write_duration.record(value, &[]);
    }

    pub fn report_aerospike_read_duration_micros(&self, value: u64) {
        self.0.aerospike_read_duration.record(value, &[]);
    }

    pub fn report_accumulator_write_queue_blocks(&self, value: u64) {
        self.0.accumulator_write_queue_blocks.record(value, &[]);
    }

    pub fn report_accumulator_deferred_batches(&self, value: u64) {
        self.0.accumulator_deferred_batches.record(value, &[]);
    }

    pub fn report_merkle_cache_write_duration_ms(&self, value: u64) {
        self.0.merkle_cache_write_duration.record(value, &[]);
    }
}
