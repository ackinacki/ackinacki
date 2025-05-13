use std::sync::Arc;
use std::time::Duration;

use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Gauge;
use opentelemetry::metrics::Histogram;
use opentelemetry::metrics::Meter;
use opentelemetry::metrics::ObservableGauge;
use opentelemetry::KeyValue;

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
    outgoing_buffer_duration: Histogram<u64>,
    outgoing_transfer_duration: Histogram<u64>,
    outgoing_transfer_error: Counter<u64>,
    subscriber_count: Gauge<u64>,
    transfer_after_ser: Histogram<u64>,
    receive_before_deser: Histogram<u64>,
    original_message_size: Histogram<u64>,
    compressed_message_size: Histogram<u64>,
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

        let boundaries_ms = vec![
            0.0, 10.0, 25.0, 50.0, 150.0, 500.0, 1000.0, 5000.0, 10000.0, 30000.0, 60000.0,
            600000.0,
        ];
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
            outgoing_transfer_error: meter
                .u64_counter("node_network_outgoing_transfer_error")
                .build(),
            subscriber_count: meter.u64_gauge("node_network_subscriber_count").build(),
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

    pub fn report_subscribers_count(&self, value: usize) {
        self.subscriber_count.record(value as u64, &[]);
    }

    pub fn report_transfer_after_ser(&self, value: u128) {
        self.transfer_after_ser.record(value as u64, &[]);
    }

    pub fn report_receive_before_deser(&self, value: u128) {
        self.receive_before_deser.record(value as u64, &[]);
    }

    pub fn report_message_size(&self, orig_size: usize, compressed_size: usize, msg_type: &str) {
        self.original_message_size.record(orig_size as u64, &[msg_type_attr(msg_type)]);
        self.compressed_message_size.record(compressed_size as u64, &[msg_type_attr(msg_type)]);
    }

    pub fn start_delivery_phase(
        &self,
        phase: DeliveryPhase,
        msg_count: usize,
        _msg_type: &str,
        _send_mode: SendMode,
    ) {
        self.update_delivery_phase_counter(phase, msg_count as isize);
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
                self.outgoing_buffer_duration.record(duration, &attrs);
            }
            DeliveryPhase::OutgoingTransfer => {
                self.outgoing_message.add(1, &attrs);
                self.outgoing_transfer_duration.record(duration, &attrs);
            }
            DeliveryPhase::IncomingTransfer => {}
            DeliveryPhase::IncomingBuffer => {
                self.incoming_message.add(1, &[msg_type_attr(msg_type)]);
                self.incoming_buffer_duration.record(duration, &attrs);
            }
        }
    }
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
