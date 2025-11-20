use std::time::Instant;

use thiserror::Error;
use transport_layer::NetConnection;
use wtransport::error::ConnectionError;
use wtransport::error::StreamOpeningError;
use wtransport::error::StreamWriteError;

use crate::detailed;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;

#[derive(Error, Debug, Clone, Eq, PartialEq)]
pub enum TransportError {
    #[error("connection error: {0}")]
    Connection(#[from] ConnectionError),

    #[error("stream opening error: {0}")]
    StreamOpening(#[from] StreamOpeningError),

    #[error("stream write error: {0}")]
    StreamWrite(#[from] StreamWriteError),

    #[error("send error: {0}")]
    Send(String),

    #[error("serialization error: {0}")]
    BincodeSerialization(String),
}
impl TransportError {
    pub fn kind_str(&self) -> &'static str {
        match self {
            TransportError::Connection(err) => match err {
                ConnectionError::ConnectionClosed(_) => "connection_closed",
                ConnectionError::ApplicationClosed(_) => "application_closed",
                ConnectionError::LocallyClosed => "locally_closed",
                ConnectionError::LocalH3Error(_) => "local_h3_error",
                ConnectionError::TimedOut => "timed_out",
                ConnectionError::QuicProto(_) => "quic_proto",
                ConnectionError::CidsExhausted => "cids_exhausted",
            },
            TransportError::StreamOpening(err) => match err {
                StreamOpeningError::NotConnected => "not_connected",
                StreamOpeningError::Refused => "stream_refused",
            },
            TransportError::StreamWrite(err) => match err {
                StreamWriteError::NotConnected => "not_connected",
                StreamWriteError::Closed => "stream_closed",
                StreamWriteError::Stopped(_) => "stream_stopped",
                StreamWriteError::QuicProto => "quic_proto",
            },
            TransportError::BincodeSerialization(_) => "bincode",
            TransportError::Send(_) => "send",
        }
    }
}

// This function returns the number of bytes sent
pub async fn transfer(
    connection: &impl NetConnection,
    net_message: &NetMessage,
    metrics: &Option<NetMetrics>,
) -> Result<usize, TransportError> {
    let data = net_message
        .serialize()
        .map_err(|err| TransportError::BincodeSerialization(err.to_string()))?;

    let moment = Instant::now();
    connection
        .send(&data)
        .await
        .inspect_err(|err| {
            tracing::error!("Failed to send outgoing data: {}", detailed(err));
        })
        .map_err(|err| TransportError::Send(err.to_string()))?;

    metrics.as_ref().inspect(|m| {
        m.report_transfer_after_ser(moment.elapsed().as_millis());
    });
    Ok(data.len())
}
