use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::detailed;
use crate::pub_sub::is_safely_closed;
use crate::pub_sub::ConnectionWrapper;
use crate::pub_sub::IncomingMessage;

pub async fn receiver(
    connection: Arc<ConnectionWrapper>,
    mut stop_rx: watch::Receiver<bool>,
    incoming_messages: mpsc::Sender<IncomingMessage>,
) -> anyhow::Result<()> {
    loop {
        let result = tokio::select! {
            result = handle_recv(&connection) => result,
            _ = stop_rx.changed() => if *stop_rx.borrow() {
                break;
            } else {
                continue;
            },
        };
        match result {
            Ok(None) => {
                tracing::info!(peer = connection.peer_info(), "Connection closed by peer");
                break;
            }
            Ok(Some(data)) => {
                tracing::trace!(
                    len = data.len(),
                    peer = connection.peer_info(),
                    "Received {} message",
                    if connection.self_is_subscriber { "broadcast" } else { "direct" }
                );
                if incoming_messages
                    .send(IncomingMessage {
                        peer: connection.peer_id.clone(),
                        data: Arc::new(data),
                    })
                    .await
                    .is_err()
                {
                    tracing::info!("Finished to receive data");
                    break;
                };
            }
            Err(err) => {
                tracing::error!(
                    peer = connection.peer_info(),
                    "Failed to receive data: {}",
                    detailed(&err)
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    tracing::trace!(peer = connection.peer_info(), "Receiver loop finished");
    Ok(())
}

async fn handle_recv(connection: &ConnectionWrapper) -> anyhow::Result<Option<Vec<u8>>> {
    let mut stream = match connection.connection.accept_uni().await {
        Ok(stream) => stream,
        Err(err) => {
            return if is_safely_closed(&err) {
                Ok(None)
            } else {
                tracing::error!(
                    peer = connection.peer_info(),
                    "Failed to accept uni stream: {}. Closing connection.",
                    detailed(&err)
                );
                Ok(None)
            }
        }
    };
    let mut data = Vec::with_capacity(1024);
    stream.read_to_end(&mut data).await.context("failed to read data from stream")?;
    Ok(Some(data))
}
