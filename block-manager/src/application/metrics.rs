use database::sqlite::history_proofs::HistoryProofsCountersSnapshot;
use node_types::ThreadIdentifier;
use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Gauge;
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;
use telemetry_utils::get_metrics_endpoint;
use telemetry_utils::init_meter_provider;
use telemetry_utils::TokioMetrics;

use crate::application::services::history_verifier::StoreGauges;
use crate::application::services::history_verifier::VerifierStats;
use crate::application::services::history_verifier::FAILURE_LABELS;

pub const ERR_UPDATE_BP_RESOLVER: &str = "update_resolver";
pub const ERR_RECORD_SEQNO: &str = "record_seqno";
pub const ERR_STORE_BLOCK: &str = "store_block";
pub const ERR_DESER_BLOCK: &str = "deser_block";
pub const ERR_QUARNTINE_BLOCK: &str = "quarantine";
pub const ERR_ROTATE_DB: &str = "rotate_db";
pub const ERR_GRACEFULL_SHUTDOWN: &str = "gracefull_shutdown";
pub const ERR_RECV_BLOCK: &str = "rcv_block";
pub const ERR_OPEN_CONN: &str = "open_conn";
pub const ERR_BK_SET_POLL: &str = "bk_set_poll";
pub const ERR_BK_SET_INVALID: &str = "bk_set_invalid";
pub const ERR_BK_SET_EVICTED: &str = "bk_set_evicted";
pub const ERR_BK_SET_STALLED: &str = "bk_set_stalled";
pub const ERR_BK_SET_ANCHOR: &str = "bk_set_anchor";
pub const ERR_BK_SET_COMMITMENT: &str = "bk_set_commitment";
pub const ERR_BK_SET_STORE: &str = "bk_set_store";
pub const ERR_BK_SET_RESTORE: &str = "bk_set_restore";

#[derive(Clone)]
pub struct BlockManagerMetrics {
    last_finalized_seqno: Gauge<u64>,
    build_info: Gauge<u64>,
    errors: Counter<u64>,
    rotation: Counter<u64>,
    dedup_skipped: Counter<u64>,
    latest_bk_set_seqno: Gauge<u64>,
    history: HistoryWriteMetrics,
}

/// Counters of history proofs store writes (`bm_history_*`).
#[derive(Clone)]
struct HistoryWriteMetrics {
    rows_written: Counter<u64>,
    conflicts: Counter<u64>,
    /// `kind`: `withdrawal`, `voucher`.
    event_conflicts: Counter<u64>,
    rejected: Counter<u64>,
    seq_no_mismatch: Counter<u64>,
    /// `kind`: `withdrawal`, `voucher`.
    events: Counter<u64>,
    /// `kind`: `withdrawal`, `voucher`.
    events_skipped: Counter<u64>,
    attestation_mismatch: Counter<u64>,
    attestation_missing: Counter<u64>,
    bk_set_updates: Counter<u64>,
    /// `result`: `written`, `replaced`, `rejected`.
    attestations: Counter<u64>,
    missing_attestations: Gauge<u64>,
    verifier_failures: Counter<u64>,
    first_height: Gauge<u64>,
    last_height: Gauge<u64>,
    missing_blocks: Gauge<u64>,
    check_failures: Gauge<u64>,
    unverified: Gauge<u64>,
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
            dedup_skipped: meter.u64_counter("bm_dedup_skipped").build(),
            latest_bk_set_seqno: meter.u64_gauge("bm_latest_bk_set_seqno").build(),
            history: HistoryWriteMetrics {
                rows_written: meter.u64_counter("bm_history_rows_written").build(),
                conflicts: meter.u64_counter("bm_history_conflicts").build(),
                event_conflicts: meter.u64_counter("bm_history_event_conflicts").build(),
                rejected: meter.u64_counter("bm_history_rejected").build(),
                seq_no_mismatch: meter.u64_counter("bm_history_seq_no_mismatch").build(),
                events: meter.u64_counter("bm_history_events").build(),
                events_skipped: meter.u64_counter("bm_history_events_skipped").build(),
                attestation_mismatch: meter.u64_counter("bm_history_attestation_mismatch").build(),
                attestation_missing: meter.u64_counter("bm_history_attestation_missing").build(),
                bk_set_updates: meter.u64_counter("bm_history_bk_set_updates").build(),
                attestations: meter.u64_counter("bm_history_attestations").build(),
                missing_attestations: meter.u64_gauge("bm_history_missing_attestations").build(),
                verifier_failures: meter.u64_counter("bm_history_verifier_failures").build(),
                first_height: meter.u64_gauge("bm_history_first_height").build(),
                last_height: meter.u64_gauge("bm_history_last_height").build(),
                missing_blocks: meter.u64_gauge("bm_history_missing_blocks").build(),
                check_failures: meter.u64_gauge("bm_history_check_failures").build(),
                unverified: meter.u64_gauge("bm_history_unverified").build(),
            },
        }
    }

    /// Gauges of one history verifier pass.
    pub fn report_history_verifier(&self, stats: &VerifierStats) {
        let history = &self.history;
        self.report_history_store(&stats.store);
        for label in FAILURE_LABELS {
            let rows = stats.failures.get(label).copied().unwrap_or(0);
            history.check_failures.record(rows, &[KeyValue::new("check", label)]);
        }
        // Not row checks of `FAILURE_LABELS`: rows of the BK set tables,
        // checked on some passes only; the gauges keep the last value.
        if let Some((updates, attestations)) = stats.inconsistent_bk_set_rows {
            for (label, rows) in [("bk_set_update", updates), ("attestation", attestations)] {
                history.check_failures.record(rows, &[KeyValue::new("check", label)]);
            }
        }
        history.unverified.record(stats.checkable, &[KeyValue::new("state", "checkable")]);
        history.unverified.record(stats.not_checkable, &[KeyValue::new("state", "not_checkable")]);
    }

    /// Gauges of what the store holds; also after a failed verifier pass.
    pub fn report_history_store(&self, gauges: &StoreGauges) {
        let history = &self.history;
        if let Some((first, last)) = gauges.first_last {
            history.first_height.record(first, &[]);
            history.last_height.record(last, &[]);
        }
        history
            .missing_blocks
            .record(gauges.missing_dex_window, &[KeyValue::new("scope", "dex_window")]);
        history.missing_blocks.record(gauges.missing_total, &[KeyValue::new("scope", "total")]);
        history.missing_attestations.record(gauges.missing_attestations, &[]);
    }

    /// A verifier pass failed: its check gauges keep their last values.
    pub fn report_history_verifier_failure(&self) {
        self.history.verifier_failures.add(1, &[]);
    }

    /// Adds the growth of the history proofs store write counters.
    pub fn report_history_counters(&self, delta: &HistoryProofsCountersSnapshot) {
        // Every field by name: a new counter does not build until it has a metric.
        let HistoryProofsCountersSnapshot {
            rows_written,
            // A block sent again after a restart: normal, no metric.
            duplicates: _,
            conflicts,
            rejected,
            seq_no_mismatch,
            withdrawal_events,
            voucher_events,
            withdrawal_events_skipped,
            voucher_events_skipped,
            attestation_mismatch,
            attestation_missing,
            bk_set_updates_written,
            attestations_written,
            attestations_replaced,
            attestations_rejected,
            withdrawal_event_conflicts,
            voucher_event_conflicts,
        } = *delta;
        let history = &self.history;
        for (counter, value) in [
            (&history.rows_written, rows_written),
            (&history.conflicts, conflicts),
            (&history.rejected, rejected),
            (&history.seq_no_mismatch, seq_no_mismatch),
            (&history.attestation_mismatch, attestation_mismatch),
            (&history.attestation_missing, attestation_missing),
            (&history.bk_set_updates, bk_set_updates_written),
        ] {
            if value > 0 {
                counter.add(value, &[]);
            }
        }
        for (counter, kind, value) in [
            (&history.events, "withdrawal", withdrawal_events),
            (&history.events, "voucher", voucher_events),
            (&history.events_skipped, "withdrawal", withdrawal_events_skipped),
            (&history.events_skipped, "voucher", voucher_events_skipped),
            (&history.event_conflicts, "withdrawal", withdrawal_event_conflicts),
            (&history.event_conflicts, "voucher", voucher_event_conflicts),
        ] {
            if value > 0 {
                counter.add(value, &[KeyValue::new("kind", kind)]);
            }
        }
        for (result, value) in [
            ("written", attestations_written),
            ("replaced", attestations_replaced),
            ("rejected", attestations_rejected),
        ] {
            if value > 0 {
                history.attestations.add(value, &[KeyValue::new("result", result)]);
            }
        }
    }

    pub fn report_last_finalized_seqno(&self, seq_no: u32, thread_id: String) {
        self.last_finalized_seqno.record(seq_no as u64, &[KeyValue::new("thread", thread_id)]);
        // Additionally here we can count number of blocks as well
        // self.0.block_finalized.add(1, &[thread_id_attr(thread_id)]);
    }

    /// `thread_id` is the 68-char hex id from the BK node response.
    pub fn report_latest_bk_set_seqno(&self, seq_no: u64, thread_id: &str) {
        self.latest_bk_set_seqno
            .record(seq_no, &[KeyValue::new("thread", thread_label(thread_id))]);
    }

    pub fn report_rotation(&self) {
        self.rotation.add(1, &[]);
    }

    pub fn report_errors(&self, kind: &'static str) {
        self.errors.add(1, &[KeyValue::new("kind", kind)]);
    }

    pub fn report_dedup_skipped(&self) {
        self.dedup_skipped.add(1, &[]);
    }

    pub fn report_build_info(&self) {
        let version = env!("CARGO_PKG_VERSION");
        let commit = env!("BUILD_GIT_COMMIT");
        self.build_info
            .record(1, &[KeyValue::new("version", version), KeyValue::new("commit", commit)]);
    }
}

/// `thread` label in the format `bm_last_finalized_seqno` uses (`ThreadIdentifier`'s `Display`).
/// A value that does not parse as a thread identifier is used as is.
pub fn thread_label(thread_id: &str) -> String {
    ThreadIdentifier::try_from(thread_id.to_string())
        .map(|id| id.to_string())
        .unwrap_or_else(|_| thread_id.to_string())
}

#[cfg(test)]
mod tests {
    use node_types::ThreadIdentifier;

    use super::thread_label;

    #[test]
    fn thread_label_matches_last_finalized_seqno_format() {
        let id = ThreadIdentifier::from([0xab; 34]);
        assert_eq!(thread_label(&hex::encode([0xab; 34])), id.to_string());
        assert_eq!(thread_label(&"ab".repeat(34)), format!("<T:{}>", "ab".repeat(34)));
        assert_eq!(thread_label("not-a-thread"), "not-a-thread");
    }
}
