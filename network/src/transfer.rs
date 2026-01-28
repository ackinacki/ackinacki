use std::time::Instant;

use thiserror::Error;
use transport_layer::NetConnection;

use crate::detailed;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;

#[derive(Error, Debug, Clone, Eq, PartialEq)]
pub enum TransportError {
    #[error("send error: {0}")]
    Send(String),

    #[error("serialization error: {0}")]
    BincodeSerialization(String),
}
impl TransportError {
    pub fn kind_str(&self) -> &'static str {
        match self {
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
