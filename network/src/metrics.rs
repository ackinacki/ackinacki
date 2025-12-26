use std::borrow::Cow;
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Gauge;
use opentelemetry::metrics::Histogram;
use opentelemetry::metrics::Meter;
use opentelemetry::metrics::ObservableGauge;
use opentelemetry::metrics::UpDownCounter;
use opentelemetry::KeyValue;
use telemetry_utils::out_of_bounds_guard;

use crate::pub_sub::connection::ConnectionInfo;
use crate::pub_sub::connection::ConnectionRole;
use crate::transfer::TransportError;
use crate::DeliveryPhase;
use crate::SendMode;

pub const LAGGED: &str = "LAGGED";

#[derive(Clone)]
pub struct NetMetrics {
    incoming_message: Counter<u64>,
    incoming_buffer_duration: Histogram<u64>,
    incoming_message_delivery_duration: Histogram<u64>,
    outgoing_message: Counter<u64>,
    outgoing_buffer_counter: UpDownCounter<i64>,
    outgoing_buffer_duration: Histogram<u64>,
    outgoing_transfer_duration: Histogram<u64>,
    outgoing_transfer_error: Counter<u64>,
    subscriber_count: Gauge<u64>,
    publisher_count: Gauge<u64>,
    planned_publisher_count: Gauge<u64>,
    transfer_after_ser: Histogram<u64>,
    receive_before_deser: Histogram<u64>,
    original_message_size: Histogram<u64>,
    compressed_message_size: Histogram<u64>,
    gossip_peers: Gauge<u64>,      // Nodes only
    gossip_live_nodes: Gauge<u64>, // Nodes + Proxies
    sent_to_outgoing_buffer_bytes: Counter<u64>,
    sent_bytes: Counter<u64>,
    received_bytes: Counter<u64>,
    errors: Counter<u64>,
    warns: Counter<u64>,
    added_connections: Counter<u64>,
    removed_connections: Counter<u64>,

    // It's usual for observable instruments to be prefixed with underscore
    _incoming_buffer_size: ObservableGauge<u64>,
    _outgoing_buffer_size: ObservableGauge<u64>,
    _network_incoming_transfer_inflight: ObservableGauge<u64>,
    _outgoing_transfer_inflight: ObservableGauge<u64>,
    state: Arc<parking_lot::Mutex<NetworkState>>,
}

#[derive(Default)]
struct NetworkState {
    incoming_buffer_count: usize,
    incoming_transfer_count: usize,
    outgoing_buffer_count: usize,
    outgoing_transfer_count: usize,
}

impl NetMetrics {
    pub fn new(meter: &Meter) -> Self {
        let state = Arc::new(parking_lot::Mutex::new(NetworkState::default()));

        let state_clone = state.clone();
        let network_incoming_buffer_size = meter
            .u64_observable_gauge("node_network_incoming_buffer_size")
            .with_callback(move |observer| {
                let state = state_clone.lock();
                observer.observe(state.incoming_buffer_count as u64, &[])
            })
            .build();

        let state_clone = state.clone();
        let network_outgoing_buffer_size = meter
            .u64_observable_gauge("node_network_outgoing_buffer_size")
            .with_callback(move |observer| {
                let state = state_clone.lock();
                observer.observe(state.outgoing_buffer_count as u64, &[])
            })
            .build();

        let state_clone = state.clone();
        let network_incoming_transfer_inflight = meter
            .u64_observable_gauge("node_network_incoming_transfer_inflight")
            .with_callback(move |observer| {
                let state = state_clone.lock();
                observer.observe(state.incoming_transfer_count as u64, &[])
            })
            .build();

        let state_clone = state.clone();
        let network_outgoing_transfer_inflight = meter
            .u64_observable_gauge("node_network_outgoing_transfer_inflight")
            .with_callback(move |observer| {
                let state = state_clone.lock();
                observer.observe(state.outgoing_transfer_count as u64, &[])
            })
            .build();

        // buckets with 50ms interval: from 0 to 1s, 100ms interval from 1s to 2s, 500ms up to 5s
        let mut boundaries_ms = gen_boundaries(0, 50, 5);
        boundaries_ms.extend(gen_boundaries(50, 150, 10));
        boundaries_ms.extend(gen_boundaries(150, 1000, 50));
        boundaries_ms.extend(gen_boundaries(1000, 2000, 100));
        boundaries_ms.extend(gen_boundaries(2000, 5000, 500));
        boundaries_ms.extend(gen_boundaries(5000, 10000, 1000));

        let boundaries_bytes = vec![
            100.0,
            500.0,
            1_000.0,
            2_000.0,
            5_000.0,
            10_000.0,
            50_000.0,
            100_000.0,
            200_000.0,
            400_000.0,
            800_000.0,
            1_200_000.0,
            2_000_000.0,
            5_000_000.0,
        ];
        NetMetrics {
            incoming_message: meter.u64_counter("node_network_incoming_message").build(),
            incoming_buffer_duration: meter
                .u64_histogram("node_network_incoming_buffer_duration")
                .with_boundaries(boundaries_ms.clone())
                .build(),
            incoming_message_delivery_duration: meter
                .u64_histogram("node_network_incoming_message_delivery_duration")
                .with_boundaries(boundaries_ms.clone())
                .build(),
            outgoing_message: meter.u64_counter("node_network_outgoing_message").build(),
            outgoing_buffer_counter: meter
                .i64_up_down_counter("node_network_outgoing_buffer_counter")
                .build(),
            outgoing_buffer_duration: meter
                .u64_histogram("node_network_outgoing_buffer_duration")
                .with_boundaries(boundaries_ms.clone())
                .build(),
            outgoing_transfer_duration: meter
                .u64_histogram("node_network_outgoing_transfer_duration")
                .with_boundaries(boundaries_ms.clone())
                .build(),
            transfer_after_ser: meter
                .u64_histogram("node_network_transfer_after_ser")
                .with_boundaries(boundaries_ms.clone())
                .build(),
            receive_before_deser: meter
                .u64_histogram("node_network_receive_before_deser")
                .with_boundaries(boundaries_ms)
                .build(),
            original_message_size: meter
                .u64_histogram("node_network_original_message_size")
                .with_boundaries(boundaries_bytes.clone())
                .build(),
            compressed_message_size: meter
                .u64_histogram("node_network_compressed_message_size")
                .with_boundaries(boundaries_bytes)
                .build(),
            gossip_peers: meter.u64_gauge("node_network_gossip_peers").build(),
            gossip_live_nodes: meter.u64_gauge("node_network_gossip_live_nodes").build(),
            sent_to_outgoing_buffer_bytes: meter
                .u64_counter("node_network_sent_to_outbuf_bytes")
                .build(),
            sent_bytes: meter.u64_counter("node_network_sent_bytes").build(),
            received_bytes: meter.u64_counter("node_network_received_bytes").build(),
            errors: meter.u64_counter("node_network_errors").build(),
            warns: meter.u64_counter("node_network_warns").build(),
            outgoing_transfer_error: meter
                .u64_counter("node_network_outgoing_transfer_error")
                .build(),
            subscriber_count: meter.u64_gauge("node_network_subscriber_count").build(),
            publisher_count: meter.u64_gauge("node_network_publisher_count").build(),
            planned_publisher_count: meter
                .u64_gauge("node_network_planned_publisher_count")
                .build(),
            added_connections: meter.u64_counter("node_network_added_connections").build(),
            removed_connections: meter.u64_counter("node_network_removed_connections").build(),
            _incoming_buffer_size: network_incoming_buffer_size,
            _outgoing_buffer_size: network_outgoing_buffer_size,
            _network_incoming_transfer_inflight: network_incoming_transfer_inflight,
            _outgoing_transfer_inflight: network_outgoing_transfer_inflight,
            state,
        }
    }

    fn update_delivery_phase_counter(&self, phase: DeliveryPhase, delta: isize) {
        let mut state = self.state.lock();
        let counter = match phase {
            DeliveryPhase::IncomingBuffer => &mut state.incoming_buffer_count,
            DeliveryPhase::IncomingTransfer => &mut state.incoming_transfer_count,
            DeliveryPhase::OutgoingTransfer => &mut state.outgoing_transfer_count,
            DeliveryPhase::OutgoingBuffer => &mut state.outgoing_buffer_count,
        };
        *counter = counter.saturating_add_signed(delta);
    }

    pub fn report_incoming_message_delivery_duration(&self, value: u64, msg_type: &str) {
        out_of_bounds_guard!(value, "incoming_message_delivery_duration");
        self.incoming_message_delivery_duration.record(value, &[msg_type_attr(msg_type)]);
    }

    pub fn report_outgoing_transfer_error(
        &self,
        msg_type: &str,
        send_mode: SendMode,
        error: TransportError,
    ) {
        let attrs = [msg_type_attr(msg_type), send_mode_attr(send_mode), transfer_err_attr(error)];
        self.outgoing_transfer_error.add(1, &attrs);
    }

    pub fn report_added_connection(&self, local_role: ConnectionRole) {
        let attrs = [remote_role_attr(local_role)];
        self.added_connections.add(1, &attrs);
    }

    pub fn report_removed_connection(&self, local_role: ConnectionRole) {
        let attrs = [remote_role_attr(local_role)];
        self.removed_connections.add(1, &attrs);
    }

    pub fn report_connections(&self, subscriber_count: usize, publisher_count: usize) {
        self.subscriber_count.record(subscriber_count as u64, &[]);
        self.publisher_count.record(publisher_count as u64, &[]);
    }

    pub fn report_planned_publisher_count(&self, count: usize) {
        self.planned_publisher_count.record(count as u64, &[]);
    }

    pub fn report_transfer_after_ser(&self, value: u128) {
        out_of_bounds_guard!(value, "transfer_after_ser");
        self.transfer_after_ser.record(value as u64, &[]);
    }

    pub fn report_receive_before_deser(&self, value: u128) {
        out_of_bounds_guard!(value, "receive_before_deser");
        self.receive_before_deser.record(value as u64, &[]);
    }

    pub fn report_message_size(&self, orig_size: usize, compressed_size: usize, msg_type: &str) {
        if orig_size < 10_000_000 {
            self.original_message_size.record(orig_size as u64, &[msg_type_attr(msg_type)]);
        } else {
            tracing::warn!("Metric: original_message_size: value {orig_size} is out of bounds");
        }

        if compressed_size < 10_000_000 {
            self.compressed_message_size.record(compressed_size as u64, &[msg_type_attr(msg_type)]);
        } else {
            tracing::warn!(
                "Metric: compressed_message_size: value {compressed_size} is out of bounds"
            );
        }
    }

    pub fn report_gossip_peers(&self, peers: usize, live_nodes_total: u64) {
        // The terminology here is the opposite of natural, but this is how it's called in our code
        // Only Nodes
        self.gossip_peers.record(peers as u64, &[]);
        // Nodes + Proxies
        self.gossip_live_nodes.record(live_nodes_total, &[]);
    }

    // TBD: This metric could potentially introduce some overhead, after debugging it can be removed
    pub fn report_sent_to_outgoing_buffer_bytes(
        &self,
        bytes: u64,
        msg_type: &str,
        send_mode: SendMode,
    ) {
        self.sent_to_outgoing_buffer_bytes.add(bytes, &attrs(msg_type, send_mode));
    }

    pub fn report_sent_bytes(&self, bytes: usize, msg_type: &str, send_mode: SendMode) {
        self.sent_bytes.add(bytes as u64, &attrs(msg_type, send_mode));
    }

    pub fn report_received_bytes(&self, bytes: usize, msg_type: &str, send_mode: SendMode) {
        self.received_bytes.add(bytes as u64, &attrs(msg_type, send_mode));
    }

    pub fn report_warn(&self, kind: impl Into<Cow<'static, str>>) {
        let kind = kind.into();
        self.warns.add(1, &[KeyValue::new("kind", kind)]);
    }

    pub fn report_error(&self, kind: impl Into<Cow<'static, str>>) {
        let kind = kind.into();
        self.errors.add(1, &[KeyValue::new("kind", kind)]);
    }

    pub fn start_delivery_phase(
        &self,
        phase: DeliveryPhase,
        msg_count: usize,
        msg_type: &str,
        send_mode: SendMode,
    ) {
        self.update_delivery_phase_counter(phase, msg_count as isize);

        if matches!(phase, DeliveryPhase::OutgoingBuffer) {
            self.outgoing_buffer_counter.add(msg_count as i64, &attrs(msg_type, send_mode));
        }
    }

    pub fn start_incoming_phase<PeerId: Debug + Display>(
        &self,
        data_len: usize,
        msg_type: &str,
        info: &ConnectionInfo<PeerId>,
    ) {
        self.report_received_bytes(data_len, msg_type, info.send_mode());
        self.start_delivery_phase(DeliveryPhase::IncomingBuffer, 1, msg_type, info.send_mode());
    }

    pub fn finish_delivery_phase(
        &self,
        phase: DeliveryPhase,
        msg_count: usize,
        msg_type: &str,
        send_mode: SendMode,
        duration: Duration,
    ) {
        self.update_delivery_phase_counter(phase, -(msg_count as isize));

        let duration = duration.as_millis() as u64;
        let attrs = attrs(msg_type, send_mode);
        match phase {
            DeliveryPhase::OutgoingBuffer => {
                self.outgoing_buffer_counter.add(-(msg_count as i64), &attrs);

                out_of_bounds_guard!(duration, "outgoing_buffer_duration");
                self.outgoing_buffer_duration.record(duration, &attrs);
            }
            DeliveryPhase::OutgoingTransfer => {
                self.outgoing_message.add(1, &attrs);

                out_of_bounds_guard!(duration, "outgoing_transfer_duration");
                self.outgoing_transfer_duration.record(duration, &attrs);
            }
            DeliveryPhase::IncomingTransfer => {}
            DeliveryPhase::IncomingBuffer => {
                self.incoming_message.add(1, &[msg_type_attr(msg_type)]);

                out_of_bounds_guard!(duration, "incoming_buffer_duration");
                self.incoming_buffer_duration.record(duration, &attrs);
            }
        }
    }
}

fn remote_role_attr(local_role: ConnectionRole) -> KeyValue {
    KeyValue::new(
        "remote_role",
        match local_role {
            ConnectionRole::Subscriber => "publisher",
            ConnectionRole::Publisher => "subscriber",
            ConnectionRole::DirectReceiver => "direct_sender",
        }
        .to_string(),
    )
}

fn msg_type_attr(msg_type: &str) -> KeyValue {
    KeyValue::new("msg_type", msg_type.to_string())
}

fn send_mode_attr(send_mode: SendMode) -> KeyValue {
    KeyValue::new("broadcast", send_mode.is_broadcast())
}

fn transfer_err_attr(error: TransportError) -> KeyValue {
    KeyValue::new("transfer", error.kind_str())
}
fn attrs(msg_type: &str, send_mode: SendMode) -> [KeyValue; 2] {
    [msg_type_attr(msg_type), send_mode_attr(send_mode)]
}

fn gen_boundaries(low: u32, high: u32, step: u32) -> Vec<f64> {
    let mut result = Vec::new();
    if step > 0 {
        let mut current = low;
        while current < high {
            result.push(current as f64);
            current += step;
        }
    }
    result
}

pub fn to_label_kind<S: Into<Cow<'static, str>>>(input: S) -> String {
    let s: Cow<'static, str> = input.into();
    s.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' => c.to_ascii_lowercase(),
            _ => '_', // replace spaces and invalid chars
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_f64_basic() {
        let result = gen_boundaries(0, 20, 5);
        let expected = [0.0, 5.0, 10.0, 15.0];
        assert_eq!(result.len(), expected.len());

        for (a, b) in result.iter().zip(expected.iter()) {
            assert!((a - b).abs() < 1e-10, "expected {b}, got {a}");
        }
    }

    #[test]
    fn test_empty_result_when_low_ge_high() {
        let result = gen_boundaries(5, 2, 5);
        assert!(result.is_empty());
    }

    #[test]
    fn test_empty_result_when_step_is_not_positive() {
        let result = gen_boundaries(5, 20, 0);
        assert!(result.is_empty());
    }
    #[test]
    fn test_complex_boundaries() {
        let mut boundaries_ms = gen_boundaries(0, 1000, 50);
        boundaries_ms.extend(gen_boundaries(1000, 2000, 100));
        boundaries_ms.extend(gen_boundaries(2000, 5500, 500));
        let expected = [
            0.0, 50.0, 100.0, 150.0, 200.0, 250.0, 300.0, 350.0, 400.0, 450.0, 500.0, 550.0, 600.0,
            650.0, 700.0, 750.0, 800.0, 850.0, 900.0, 950.0, 1000.0, 1100.0, 1200.0, 1300.0,
            1400.0, 1500.0, 1600.0, 1700.0, 1800.0, 1900.0, 2000.0, 2500.0, 3000.0, 3500.0, 4000.0,
            4500.0, 5000.0,
        ];
        assert_eq!(boundaries_ms, expected);
    }
}
