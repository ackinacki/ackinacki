use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Gauge;
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use telemetry_utils::get_metrics_endpoint;
use telemetry_utils::init_meter_provider;
use telemetry_utils::TokioMetrics;

pub const ERR_UPDATE_BP_RESOLVER: &str = "update_resolver";
pub const ERR_RECORD_SEQNO: &str = "record_seqno";
pub const ERR_STORE_BLOCK: &str = "store_block";
pub const ERR_DESER_BLOCK: &str = "deser_block";
pub const ERR_QUARNTINE_BLOCK: &str = "quarantine";
pub const ERR_ROTATE_DB: &str = "rotate_db";
pub const ERR_GRACEFULL_SHUTDOWN: &str = "gracefull_shutdown";
pub const ERR_RECV_BLOCK: &str = "rcv_block";
pub const ERR_OPEN_CONN: &str = "open_conn";

#[derive(Clone)]
pub struct BlockManagerMetrics {
    last_finalized_seqno: Gauge<u64>,
    build_info: Gauge<u64>,
    errors: Counter<u64>,
    rotation: Counter<u64>,
}

#[derive(Clone)]
pub struct Metrics {
    pub bm: BlockManagerMetrics,
    pub tokio: TokioMetrics,
    pub endpoint: String,
}

impl Metrics {
    pub fn new(name: &'static str) -> Option<Self> {
        if let Some(endpoint) = get_metrics_endpoint() {
            opentelemetry::global::set_meter_provider(init_meter_provider());
            let meter = opentelemetry::global::meter(name);
            let metrics = Metrics {
                bm: BlockManagerMetrics::new(&meter),
                tokio: TokioMetrics::new(&meter),
                endpoint,
            };
            Some(metrics)
        } else {
            None
        }
    }
}

impl BlockManagerMetrics {
    pub fn new(meter: &Meter) -> Self {
        Self {
            last_finalized_seqno: meter.u64_gauge("bm_last_finalized_seqno").build(),
            errors: meter.u64_counter("bm_errors").build(),
            rotation: meter.u64_counter("bm_rotation").build(),
            build_info: meter.u64_gauge("bm_build_info").build(),
        }
    }

    pub fn report_last_finalized_seqno(&self, seq_no: u32, thread_id: String) {
        self.last_finalized_seqno.record(seq_no as u64, &[KeyValue::new("thread", thread_id)]);
        // Additionally here we can count number of blocks as well
        // self.0.block_finalized.add(1, &[thread_id_attr(thread_id)]);
    }

    pub fn report_rotation(&self) {
        self.rotation.add(1, &[]);
    }

    pub fn report_errors(&self, kind: &'static str) {
        self.errors.add(1, &[KeyValue::new("kind", kind)]);
    }

    pub fn report_build_info(&self) {
        let version = env!("CARGO_PKG_VERSION");
        let commit = env!("BUILD_GIT_COMMIT");
        self.build_info
            .record(1, &[KeyValue::new("version", version), KeyValue::new("commit", commit)]);
    }
}
