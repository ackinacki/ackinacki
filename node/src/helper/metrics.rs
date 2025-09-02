use std::sync::Arc;

use http_server::metrics::RoutingMetrics;
use network::metrics::NetMetrics;
use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Gauge;
use opentelemetry::metrics::Histogram;
use opentelemetry::metrics::Meter;
use opentelemetry::metrics::UpDownCounter;
use opentelemetry::KeyValue;
use telemetry_utils::instrumented_channel_ext::XInstrumentedChannelMetrics;
use telemetry_utils::mpsc::InstrumentedChannelMetrics;
use telemetry_utils::out_of_bounds_guard;
use telemetry_utils::TokioMetrics;

use crate::types::ThreadIdentifier;

#[derive(Clone)]
pub struct BlockProductionMetrics(Arc<BlockProductionMetricsInner>);

struct BlockProductionMetricsInner {
    thread_load: Gauge<u64>,
    block_production_time: Histogram<u64>,
    block_production_time_correction: Gauge<i64>,
    block_apply_time: Histogram<u64>,
    finalization_time: Histogram<u64>,
    last_finalized_seqno: Gauge<u64>,
    ext_msg_queue_size: Gauge<u64>,
    int_msg_queue_size: Gauge<u64>,
    block_finalized: Counter<u64>,
    tx_finalized: Counter<u64>,
    tx_aborted: Counter<u64>,
    ext_tx_aborted: Counter<u64>,
    thread_count: UpDownCounter<i64>,
    finalization_gap: Gauge<u64>,
    memento_duration: Histogram<u64>,
    channel_len: UpDownCounter<i64>,
    load_from_archive_invoke: Counter<u64>,
    load_from_archive_apply: Counter<u64>,
    block_received_attestation_sent: Histogram<u64>,
    child_parent_attestation: Histogram<u64>,
    forks_count: Counter<u64>,
    parent_first_attestation_none: Counter<u64>,
    resend: Counter<u64>,
    query_gaps: Counter<u64>,
    store_block_on_disk: Histogram<u64>,
    verify_all_block_signatures: Histogram<u64>,
    calc_consencus_params: Histogram<u64>,
    check_cross_thread_ref_data: Histogram<u64>,
    wait_for_cross_thread_ref_data: Histogram<u64>,
    apply_block_total: Histogram<u64>,
    common_block_checks: Histogram<u64>,
    processing_delay: Histogram<u64>,
    attestation_after_apply_delay: Histogram<u64>,
    attn_target_descendant_generations: Histogram<u64>,
    blocks_requested: Counter<u64>,
    unfinalized_blocks_queue: Gauge<u64>,
    bk_set_size: Gauge<u64>,
    internal_message_queue_length: Gauge<u64>,
    aerospike_messages_write_busy: Counter<u64>,
    aerospike_read: Histogram<f64>,
    aerospike_write: Histogram<f64>,
    aerospike_write_err: Counter<u64>,
    aerospike_read_err: Counter<u64>,
    accounts_number: Gauge<u64>,
    generate_merkle_update_time: Histogram<u64>,
    outbound_accounts: Counter<u64>,
    saved_states_counter: Counter<u64>,
    bk_set: Gauge<u64>,
    future_bk_set: Gauge<u64>,
}

pub const BK_SET_UPDATE_CHANNEL: &str = "bk_set_update";
pub const BLOCK_STATE_CHANNEL: &str = "block_state";
pub const BLOB_SYNC_COMMAND_CHANNEL: &str = "block_sync_command";
pub const EPOCH_BK_DATA_CHANNEL: &str = "epoch_bk_data";
pub const EXT_MSG_FEEDBACK_CHANNEL: &str = "ext_msg_feedback";
pub const INBOUND_EXT_CHANNEL: &str = "inbound_ext";
pub const PRODUCE_CONTROL_CHANNEL: &str = "produce_control";
pub const PRODUCE_THREAD_RESULT_CHANNEL: &str = "produce_thread_result";
pub const RAW_BLOCK_CHANNEL: &str = "raw_block";
pub const ROUTING_COMMAND_CHANNEL: &str = "routing_command";
pub const THREAD_RECEIVER_CHANNEL: &str = "thread_receiver";
pub const AUTHORITY_RECEIVER_CHANNEL: &str = "authority_receiver";
pub const ROUTING_DISPATCHER_CHANNEL: &str = "routing_dispatcher";
pub const STATE_LOAD_RESULT_CHANNEL: &str = "state_load_result";
pub const CROSS_THREAD_REF_DATA_AVAILABILITY_SYNCHRONIZATION_SERVICE_CHANNEL: &str =
    "cross_thread_ref_data_availability_synchronization";
pub const BLOCK_STATE_SAVE_CHANNEL: &str = "block_state_save_channel";
pub const OPTIMISTIC_STATE_SAVE_CHANNEL: &str = "optimistic_state_save_channel";

pub const AEROSPIKE_OBJECT_TYPE_INT_MESSAGES: &str = "int_messages";
pub const AEROSPIKE_OBJECT_TYPE_CROSS_REF_DATA: &str = "cross_ref_data";
pub const AEROSPIKE_OBJECT_TYPE_CROSS_ACTION_LOCK: &str = "action_lock";

#[derive(Clone)]
pub struct Metrics {
    pub net: NetMetrics,
    pub node: BlockProductionMetrics,
    pub routing: RoutingMetrics,
    pub tokio: TokioMetrics,
}

impl Metrics {
    pub fn new(meter: &Meter) -> Self {
        Self {
            net: NetMetrics::new(meter),
            node: BlockProductionMetrics::new(meter),
            routing: RoutingMetrics::new(meter),
            tokio: TokioMetrics::new(meter),
        }
    }
}

impl BlockProductionMetrics {
    pub fn new(meter: &Meter) -> Self {
        Self(Arc::new(BlockProductionMetricsInner {
            thread_load: meter.u64_gauge("node_thread_load").build(),
            block_production_time: meter
                .u64_histogram("node_block_production_time")
                .with_boundaries(vec![
                    0.0, 100.0, 200.0, 300.0, 330.0, 350.0, 370.0, 400.0, 500.0, 700.00, 1000.0,
                    5000.0,
                ])
                .build(),
            block_production_time_correction: meter
                .i64_gauge("node_block_production_time_correction")
                .build(),
            block_apply_time: meter
                .u64_histogram("node_block_apply_time")
                .with_boundaries(vec![
                    0.0, 10.0, 30.0, 50.0, 80.0, 110.0, 150.0, 200.0, 250.0, 300.0, 400.0, 500.0,
                    700.0, 1000.0,
                ])
                .build(),
            generate_merkle_update_time: meter
                .u64_histogram("node_generate_merkle_update_time")
                .with_boundaries(vec![0.0, 5.0, 10.0, 20.0, 30.0, 50.0, 80.0, 100.0, 150.0, 200.0])
                .build(),
            finalization_time: meter
                .u64_histogram("node_finalization_time")
                .with_boundaries(vec![
                    300.0, 450.0, 600.0, 750.0, 900.0, 1050.0, 1200.0, 1350.0, 1500.0, 1650.0,
                    1800.0, 1950.0, 2100.0, 2250.0, 2400.0, 2550.0, 2700.0, 2850.0, 3000.0, 3150.0,
                    3300.0, 3450.0, 3600.0, 3750.0, 3900.0, 4050.0, 4200.0, 4350.0, 4500.0, 4650.0,
                    4800.0, 4950.0, 5100.0, 5250.0, 5400.0, 5550.0, 5700.0, 5850.0, 6000.0, 6150.0,
                    6300.0, 6450.0, 6600.0, 6750.0, 6900.0, 7050.0, 7200.0, 7350.0, 7500.0, 7650.0,
                    7800.0, 7950.0, 8100.0, 8250.0, 8400.0, 8550.0, 8700.0, 8850.0, 9000.0,
                    10000.0,
                ])
                .build(),
            last_finalized_seqno: meter.u64_gauge("node_last_finalized_seqno").build(),
            ext_msg_queue_size: meter.u64_gauge("node_ext_msg_queue_size").build(),
            int_msg_queue_size: meter.u64_gauge("node_int_msg_queue_size").build(),
            block_finalized: meter.u64_counter("node_block_finalized").build(),
            tx_finalized: meter.u64_counter("node_tx_finalized").build(),
            tx_aborted: meter.u64_counter("node_tx_aborted").build(),
            ext_tx_aborted: meter.u64_counter("node_ext_tx_aborted").build(),
            thread_count: meter.i64_up_down_counter("node_thread_count").build(),
            finalization_gap: meter.u64_gauge("node_finalization_gap").build(),
            channel_len: meter.i64_up_down_counter("node_channel_len").build(),
            memento_duration: meter
                .u64_histogram("node_memento_duration")
                .with_boundaries(vec![
                    1.0, 10.0, 20.0, 30.0, 40.0, 50.0, 100.0, 200.0, 300.0, 400.0, 500.0, 600.0,
                    700.0, 800.0, 900.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0, 25000.0,
                ])
                .build(),
            load_from_archive_invoke: meter.u64_counter("node_load_from_archive_invoke").build(),
            load_from_archive_apply: meter.u64_counter("node_load_from_archive_apply").build(),
            block_received_attestation_sent: meter
                .u64_histogram("node_block_received_attestation_sent")
                .with_boundaries(vec![
                    50.0, 100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 1000.0, 3000.0, 5000.0,
                    10000.0,
                ])
                .build(),
            child_parent_attestation: meter
                .u64_histogram("node_child_parent_attestation")
                .with_boundaries(vec![
                    100.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 1000.0, 1200.0, 1500.0,
                    2000.0, 2500.0, 3000.0, 3500.0, 4000.0, 5000.0, 7000.0, 10000.0,
                ])
                .build(),
            forks_count: meter.u64_counter("node_forks_count").build(),
            parent_first_attestation_none: meter
                .u64_counter("node_parent_first_attestation_none")
                .build(),
            resend: meter.u64_counter("node_resend").build(),
            query_gaps: meter.u64_counter("node_query_gaps").build(),
            blocks_requested: meter.u64_counter("node_blocks_requested").build(),

            unfinalized_blocks_queue: meter.u64_gauge("node_unfinalized_blocks_queue").build(),

            bk_set_size: meter.u64_gauge("node_bk_set_size").build(),
            store_block_on_disk: meter
                .u64_histogram("node_store_block_on_disk")
                .with_boundaries(vec![5.0, 10.0, 15.0, 20.0, 50.0, 100.0, 200.0, 500.0])
                .build(),

            verify_all_block_signatures: meter
                .u64_histogram("node_verify_all_block_signatures")
                .build(),

            calc_consencus_params: meter.u64_histogram("node_calc_consencus_params").build(),
            check_cross_thread_ref_data: meter
                .u64_histogram("node_check_cross_thread_ref_data")
                .build(),
            wait_for_cross_thread_ref_data: meter
                .u64_histogram("node_wait_for_cross_thread_ref_data")
                .build(),
            apply_block_total: meter
                .u64_histogram("node_apply_block_total")
                .with_boundaries(vec![
                    0.0, 10.0, 30.0, 50.0, 80.0, 110.0, 150.0, 200.0, 250.0, 300.0, 400.0, 500.0,
                    700.0, 1000.0,
                ])
                .build(),
            common_block_checks: meter.u64_histogram("node_common_block_checks").build(),
            processing_delay: meter
                .u64_histogram("node_processing_delay")
                .with_boundaries(vec![
                    0.0, 10.0, 30.0, 50.0, 80.0, 110.0, 150.0, 200.0, 250.0, 300.0, 400.0, 500.0,
                    700.0, 1000.0,
                ])
                .build(),

            attestation_after_apply_delay: meter
                .u64_histogram("node_attestation_after_apply_delay")
                .with_boundaries(vec![
                    0.0, 10.0, 30.0, 50.0, 80.0, 110.0, 150.0, 200.0, 250.0, 300.0, 400.0, 500.0,
                    700.0, 1000.0,
                ])
                .build(),

            attn_target_descendant_generations: meter
                .u64_histogram("node_attn_target_descendant_generations")
                .with_boundaries((0..=20).map(|x| x as f64).collect())
                .build(),

            #[cfg(feature = "monitor-accounts-number")]
            accounts_number: meter.u64_gauge("node_accounts_number").build(),
            aerospike_messages_write_busy: meter
                .u64_counter("node_aerospike_messages_write_busy")
                .build(),
            internal_message_queue_length: meter
                .u64_gauge("node_internal_message_queue_length")
                .build(),
            aerospike_write: meter
                .f64_histogram("node_aerospike_write")
                .with_boundaries(vec![
                    100.0, 200.0, 400.0, 600.00, 1000.0, 1500.0, 2000.0, 3000.0, 5000.0, 10000.0,
                ])
                .build(),
            aerospike_read: meter
                .f64_histogram("node_aerospike_read")
                .with_boundaries(vec![
                    100.0, 200.0, 400.0, 600.00, 1000.0, 1500.0, 2000.0, 3000.0, 5000.0, 10000.0,
                ])
                .build(),
            aerospike_write_err: meter.u64_counter("node_aerospike_write_err").build(),
            aerospike_read_err: meter.u64_counter("node_aerospike_read_err").build(),
            outbound_accounts: meter.u64_counter("node_outbound_accounts").build(),
            saved_states_counter: meter.u64_counter("node_saved_states_counter").build(),
            bk_set: meter.u64_gauge("node_bk_set").build(),
            future_bk_set: meter.u64_gauge("node_future_bk_set").build(),
        }))
    }

    pub fn report_block_production_time_and_correction(
        &self,
        production_time: u128,
        correction_time: i64,
        thread_id: &ThreadIdentifier,
    ) {
        if production_time < 10_000 {
            self.0
                .block_production_time
                .record(production_time as u64, &[thread_id_attr(thread_id)]);
        } else {
            tracing::warn!(
                "Metric block_production_time: value {production_time} is out of bounds",
            );
        }

        self.0
            .block_production_time_correction
            .record(correction_time, &[thread_id_attr(thread_id)]);
    }

    pub fn report_block_apply_time(&self, value: u64, thread_id: &ThreadIdentifier) {
        out_of_bounds_guard!(value, "block_apply_time");
        self.0.block_apply_time.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_generate_merkle_update_time(&self, value: u64, thread_id: &ThreadIdentifier) {
        self.0.generate_merkle_update_time.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_finalization(&self, seq_no: u32, tx_count: usize, thread_id: &ThreadIdentifier) {
        self.0.block_finalized.add(1, &[thread_id_attr(thread_id)]);
        self.0.last_finalized_seqno.record(seq_no as u64, &[thread_id_attr(thread_id)]);

        self.0.tx_finalized.add(tx_count as u64, &[thread_id_attr(thread_id)]);
    }

    #[cfg(feature = "monitor-accounts-number")]
    pub fn report_accounts_number(&self, accounts_number: u64, thread_id: &ThreadIdentifier) {
        self.0.accounts_number.record(accounts_number, &[thread_id_attr(thread_id)]);
    }

    pub fn report_finalization_time(&self, duration_ms: u64, thread_id: &ThreadIdentifier) {
        out_of_bounds_guard!(duration_ms, "finalization_time");
        self.0.finalization_time.record(duration_ms, &[thread_id_attr(thread_id)]);
    }

    pub fn report_tx_aborted(&self, thread_id: &ThreadIdentifier) {
        self.0.tx_aborted.add(1, &[thread_id_attr(thread_id)]);
    }

    pub fn report_ext_tx_aborted(&self, thread_id: &ThreadIdentifier) {
        self.0.ext_tx_aborted.add(1, &[thread_id_attr(thread_id)]);
    }

    pub fn report_ext_msg_queue_size(&self, value: usize, thread_id: &ThreadIdentifier) {
        self.0
            .ext_msg_queue_size
            .record(value as u64, &[KeyValue::new("thread", Self::thread_label(thread_id))]);
    }

    pub fn report_int_msg_queue_size(&self, value: usize, thread_id: &ThreadIdentifier) {
        self.0.int_msg_queue_size.record(
            value as u64,
            &[KeyValue::new("thread", BlockProductionMetrics::thread_label(thread_id))],
        );
    }

    pub fn report_thread_count(&self) {
        self.0.thread_count.add(1, &[]);
    }

    pub fn report_thread_load(&self, value: usize, thread_id: &ThreadIdentifier) {
        self.0.thread_load.record(value as u64, &[thread_id_attr(thread_id)]);
    }

    pub fn report_finalization_gap(&self, value: u32, thread_id: &ThreadIdentifier) {
        self.0.finalization_gap.record(value as u64, &[thread_id_attr(thread_id)]);
    }

    fn thread_label(thread_id: &ThreadIdentifier) -> String {
        thread_id.to_string()
    }

    pub fn report_memento_duration(&self, value: u128, thread_id: &ThreadIdentifier) {
        out_of_bounds_guard!(value, "memento_duration");
        self.0.memento_duration.record(value as u64, &[thread_id_attr(thread_id)]);
    }

    pub fn report_load_from_archive_invoke(&self, thread_id: &ThreadIdentifier) {
        self.0.load_from_archive_invoke.add(1, &[thread_id_attr(thread_id)]);
    }

    pub fn report_load_from_archive_apply(&self, thread_id: &ThreadIdentifier) {
        self.0.load_from_archive_apply.add(1, &[thread_id_attr(thread_id)]);
    }

    pub fn report_block_received_attestation_sent(&self, value: u64, thread_id: &ThreadIdentifier) {
        out_of_bounds_guard!(value, "block_received_attestation_sent");
        self.0.block_received_attestation_sent.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_child_parent_attestation(&self, value: u64, thread_id: &ThreadIdentifier) {
        out_of_bounds_guard!(value, "child_parent_attestation");
        self.0.child_parent_attestation.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_forks_count(&self, thread_id: &ThreadIdentifier) {
        self.0.forks_count.add(1, &[thread_id_attr(thread_id)]);
    }

    pub fn report_parent_first_attestation_none(&self, thread_id: &ThreadIdentifier) {
        self.0.parent_first_attestation_none.add(1, &[thread_id_attr(thread_id)]);
    }

    pub fn report_resend(&self, thread_id: &ThreadIdentifier) {
        self.0.resend.add(1, &[thread_id_attr(thread_id)]);
    }

    pub fn report_query_gaps(&self, thread_id: &ThreadIdentifier) {
        self.0.query_gaps.add(1, &[thread_id_attr(thread_id)]);
    }

    pub fn report_blocks_requested(&self, value: u64, thread_id: &ThreadIdentifier) {
        self.0.blocks_requested.add(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_unfinalized_blocks_queue(&self, value: u64, thread_id: &ThreadIdentifier) {
        self.0.unfinalized_blocks_queue.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_bk_set_size(&self, value: u64, thread_id: &ThreadIdentifier) {
        self.0.bk_set_size.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_bk_set(&self, bk_set: usize, future_bk_set: usize, thread_id: &ThreadIdentifier) {
        let attrs = &[thread_id_attr(thread_id)];
        self.0.bk_set.record(bk_set as u64, attrs);
        self.0.future_bk_set.record(future_bk_set as u64, attrs);
    }

    pub fn report_store_block_on_disk(&self, value: u64, thread_id: &ThreadIdentifier) {
        out_of_bounds_guard!(value, "store_block_on_disk");
        self.0.store_block_on_disk.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_verify_all_block_signatures(&self, value: u64, thread_id: &ThreadIdentifier) {
        out_of_bounds_guard!(value, "verify_all_block_signatures");
        self.0.verify_all_block_signatures.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_calc_consencus_params(&self, value: u64, thread_id: &ThreadIdentifier) {
        out_of_bounds_guard!(value, "calc_consencus_params");
        self.0.calc_consencus_params.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_check_cross_thread_ref_data(
        &self,
        check_ms: u64,
        wait_ms: u64,
        thread_id: &ThreadIdentifier,
    ) {
        out_of_bounds_guard!(check_ms, "check_cross_thread_ref_data");
        self.0.check_cross_thread_ref_data.record(check_ms, &[thread_id_attr(thread_id)]);

        out_of_bounds_guard!(wait_ms, "wait_for_cross_thread_ref_data");
        self.0.wait_for_cross_thread_ref_data.record(wait_ms, &[thread_id_attr(thread_id)]);
    }

    pub fn report_apply_block_total(&self, value: u64, thread_id: &ThreadIdentifier) {
        out_of_bounds_guard!(value, "apply_block_total");
        self.0.apply_block_total.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_common_block_checks(&self, value: u64, thread_id: &ThreadIdentifier) {
        out_of_bounds_guard!(value, "common_block_checks");
        self.0.common_block_checks.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_processing_delay(&self, value: u64, thread_id: &ThreadIdentifier) {
        out_of_bounds_guard!(value, "processing_delay");
        self.0.processing_delay.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_attestation_after_apply_delay(&self, value: u64, thread_id: &ThreadIdentifier) {
        out_of_bounds_guard!(value, "attestation_after_apply_delay");
        self.0.attestation_after_apply_delay.record(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_attn_target_descendant_generations(
        &self,
        value: usize,
        thread_id: &ThreadIdentifier,
    ) {
        out_of_bounds_guard!(value, "attn_target_descendant_generations");
        self.0
            .attn_target_descendant_generations
            .record(value as u64, &[thread_id_attr(thread_id)]);
    }

    pub fn report_aerospike_messages_write_busy(&self, value: u64) {
        self.0.aerospike_messages_write_busy.add(value, &[]);
    }

    pub fn report_internal_message_queue_length(&self, value: u64) {
        self.0.internal_message_queue_length.record(value, &[]);
    }

    pub fn report_aerospike_write(&self, value: f64, object_type: &'static str) {
        self.0.aerospike_write.record(value, &[KeyValue::new("object_type", object_type)]);
    }

    pub fn report_aerospike_read(&self, value: f64, object_type: &'static str) {
        self.0.aerospike_read.record(value, &[KeyValue::new("object_type", object_type)]);
    }

    pub fn report_aerospike_write_err(&self, object_type: &'static str) {
        self.0.aerospike_write_err.add(1, &[KeyValue::new("object_type", object_type)]);
    }

    pub fn report_aerospike_read_err(&self, object_type: &'static str) {
        self.0.aerospike_read_err.add(1, &[KeyValue::new("object_type", object_type)]);
    }

    pub fn report_outbound_accounts(&self, value: u64, thread_id: &ThreadIdentifier) {
        self.0.outbound_accounts.add(value, &[thread_id_attr(thread_id)]);
    }

    pub fn report_saved_state(&self, thread_id: &ThreadIdentifier) {
        self.0.saved_states_counter.add(1, &[thread_id_attr(thread_id)]);
    }
}

impl InstrumentedChannelMetrics for BlockProductionMetrics {
    fn report_channel(&self, channel: &'static str, delta: isize) {
        self.0.channel_len.add(delta as i64, &[KeyValue::new("channel", channel)]);
    }
}
impl XInstrumentedChannelMetrics for BlockProductionMetrics {
    fn report_channel(&self, channel: &'static str, delta: isize, label: String) {
        self.0
            .channel_len
            .add(delta as i64, &[KeyValue::new("channel", channel), KeyValue::new("tag", label)]);
    }
}
fn thread_id_attr(thread_id: &ThreadIdentifier) -> KeyValue {
    KeyValue::new("thread", BlockProductionMetrics::thread_label(thread_id))
}
