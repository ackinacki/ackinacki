use opentelemetry::metrics::Gauge;
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use telemetry_utils::TokioMetrics;

#[derive(Clone)]
pub struct BlockManagerMetrics {
    last_finalized_seqno: Gauge<u64>,
}

#[derive(Clone)]
pub struct Metrics {
    pub bm: BlockManagerMetrics,
    pub tokio: TokioMetrics,
}

impl Metrics {
    pub fn new(meter: &Meter) -> Self {
        Self { bm: BlockManagerMetrics::new(meter), tokio: TokioMetrics::new(meter) }
    }
}

impl BlockManagerMetrics {
    pub fn new(meter: &Meter) -> Self {
        Self { last_finalized_seqno: meter.u64_gauge("bm_last_finalized_seqno").build() }
    }

    pub fn report_last_finalized_seqno(&self, seq_no: u32, thread_id: String) {
        self.last_finalized_seqno.record(seq_no as u64, &[KeyValue::new("thread", thread_id)]);
        // Additionally here we can count number of blocks as well
        // self.0.block_finalized.add(1, &[thread_id_attr(thread_id)]);
    }
}
