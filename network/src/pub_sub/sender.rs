use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use wtransport::Connection;

use crate::detailed;
use crate::pub_sub::ConnectionWrapper;
use crate::pub_sub::OutgoingMessage;

pub async fn sender(
    connection: Arc<ConnectionWrapper>,
    mut stop_rx: watch::Receiver<bool>,
    mut outgoing_messages_rx: tokio::sync::broadcast::Receiver<OutgoingMessage>,
) -> anyhow::Result<()> {
    'connection: loop {
        let message = tokio::select! {
            result = outgoing_messages_rx.recv() => result.inspect_err(|err| {
                tracing::error!("Outgoing messages recv err: {}", detailed(err));
            })?,
            _ = stop_rx.changed() => if *stop_rx.borrow() {
                break;
            } else {
                continue;
            },
        };

        if !connection.allow_sending(&message) {
            continue;
        }
        tracing::debug!(
            peer = connection.peer_info(),
            len = message.data.len(),
            "Sending broadcast message",
        );

        for attempt_no in 1..4 {
            // 3 attempts to send data
            match handle_send(&connection.connection, &message.data).await {
                Ok(_) => {
                    tracing::debug!(
                        peer = connection.peer_info(),
                        len = message.data.len(),
                        "Sent broadcast message",
                    );
                    continue 'connection;
                }
                Err(err) => {
                    tracing::error!(
                        peer = connection.peer_info(),
                        len = message.data.len(),
                        attempt = attempt_no,
                        "Failed to send broadcast message: {}",
                        detailed(&err),
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }

        anyhow::bail!(
            "Connection with {} closed: Failed to send data (max attempts reached)",
            connection.peer_info()
        );
    }
    tracing::trace!(peer = connection.peer_info(), "Sender loop finished");
    Ok(())
}

async fn handle_send(connection: &Connection, data: &[u8]) -> anyhow::Result<()> {
    let mut send_stream = connection
        .open_uni()
        .await
        .inspect_err(|err| {
            tracing::error!("Failed to open send stream: {}", detailed(err));
        })?
        .await
        .inspect_err(|err| {
            tracing::error!("Failed to accept send stream: {}", detailed(err));
        })?;
    send_stream.write_all(data).await.inspect_err(|err| {
        tracing::error!("Failed to write data to send stream: {}", detailed(err));
    })?;
    send_stream.finish().await.inspect_err(|err| {
        tracing::error!("Failed to write data to send stream: {}", detailed(err));
    })?;
    Ok(())
}
