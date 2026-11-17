use std::sync::Arc;
use std::time::Duration;

use opentelemetry::metrics::Gauge;
use opentelemetry::metrics::Histogram;
use opentelemetry::metrics::Meter;

#[derive(Clone)]
pub struct StateAccountsMetrics(Arc<StateAccountsMetricsInner>);

struct StateAccountsMetricsInner {
    aerospike_batch_write_duration: Histogram<u64>,
    aerospike_read_duration: Histogram<u64>,
    batch_accumulation_duration: Histogram<f64>,
    batch_queue_duration: Histogram<f64>,
    batch_write_duration: Histogram<f64>,
    accumulator_write_queue_blocks: Gauge<u64>,
    accumulator_deferred_batches: Gauge<u64>,
    merkle_cache_write_duration: Histogram<u64>,
}

impl StateAccountsMetrics {
    pub fn new(meter: &Meter) -> Self {
        let batch_wait_boundaries = vec![
            0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 20.0, 30.0, 60.0, 120.0,
            300.0, 600.0, 1_800.0, 3_600.0,
        ];
        Self(Arc::new(StateAccountsMetricsInner {
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
                    2_000_000.0,
                    5_000_000.0,
                    10_000_000.0,
                    15_000_000.0,
                    20_000_000.0,
                    30_000_000.0,
                ])
                .build(),
            aerospike_read_duration: meter
                .u64_histogram("node_state_accounts_aerospike_read_duration")
                .with_boundaries(vec![
                    50.0, 100.0, 200.0, 500.0, 1_000.0, 2_000.0, 5_000.0, 10_000.0, 20_000.0,
                    50_000.0, 100_000.0, 500_000.0,
                ])
                .build(),
            batch_accumulation_duration: meter
                .f64_histogram("node_state_accounts_batch_accumulation_duration")
                .with_description("Time from accumulator creation or reset to batch flush")
                .with_unit("s")
                .with_boundaries(batch_wait_boundaries.clone())
                .build(),
            batch_queue_duration: meter
                .f64_histogram("node_state_accounts_batch_queue_duration")
                .with_description(
                    "Time from batch flush to writer processing, including snapshot pin waits",
                )
                .with_unit("s")
                .with_boundaries(batch_wait_boundaries)
                .build(),
            batch_write_duration: meter
                .f64_histogram("node_state_accounts_batch_write_duration")
                .with_description(
                    "Time to process a whole batch, including Merkle maps and archive writes",
                )
                .with_unit("s")
                .with_boundaries(vec![
                    0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 15.0, 20.0, 30.0,
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

    pub fn report_aerospike_batch_write_duration_micros(&self, value: u64) {
        self.0.aerospike_batch_write_duration.record(value, &[]);
    }

    pub fn report_aerospike_read_duration_micros(&self, value: u64) {
        self.0.aerospike_read_duration.record(value, &[]);
    }

    /// Report all stages together, only after a batch has been successfully written.
    pub fn report_batch_durations(&self, accumulation: Duration, queue: Duration, write: Duration) {
        self.0.batch_accumulation_duration.record(accumulation.as_secs_f64(), &[]);
        self.0.batch_queue_duration.record(queue.as_secs_f64(), &[]);
        self.0.batch_write_duration.record(write.as_secs_f64(), &[]);
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
