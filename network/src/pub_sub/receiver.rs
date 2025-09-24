use std::sync::Arc;
use std::time::Instant;

use transport_layer::NetConnection;

use crate::detailed;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::ConnectionWrapper;
use crate::pub_sub::connection::IncomingMessage;
use crate::pub_sub::IncomingSender;
use crate::DeliveryPhase;

pub async fn receiver<Connection: NetConnection + 'static>(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper<Connection>>,
    receiver_stop_tx: tokio::sync::watch::Sender<bool>,
    mut receiver_stop_rx: tokio::sync::watch::Receiver<bool>,
    incoming_tx: IncomingSender,
) -> anyhow::Result<()> {
    tracing::trace!(
        ident = &connection.connection.local_identity()[..6],
        local = connection.connection.local_addr().to_string(),
        peer = connection.info.remote_info(),
        "Receiver loop started"
    );
    loop {
        tokio::select! {
            sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                break;
            },
            _ = receive_message(metrics.clone(), connection.clone(), incoming_tx.clone(), receiver_stop_tx.clone()) => {
            },
            sender = receiver_stop_rx.changed() => if sender.is_err() || *receiver_stop_rx.borrow() {
                break;
            }
        }
    }
    tracing::trace!(
        ident = &connection.connection.local_identity()[..6],
        local = connection.connection.local_addr().to_string(),
        peer = connection.info.remote_info(),
        "Receiver loop finished"
    );
    Ok(())
}

async fn receive_message<Connection: NetConnection + 'static>(
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper<Connection>>,
    incoming_tx: IncomingSender,
    receiver_stop_tx: tokio::sync::watch::Sender<bool>,
) {
    let info = connection.info.clone();
    match connection.connection.recv().await {
        Ok((data, duration)) => {
            let net_message = match bincode::deserialize::<NetMessage>(&data) {
                Ok(msg) => msg,
                Err(err) => {
                    tracing::error!("Failed to deserialize net message: {}", err);
                    if let Some(metrics) = metrics.as_ref() {
                        metrics.report_error("fail_deser_msg_2");
                    }
                    receiver_stop_tx.send_replace(true);
                    return;
                }
            };

            let msg_type = net_message.label.clone();
            tracing::debug!(
                broadcast = info.is_broadcast(),
                msg_type,
                msg_id = net_message.id,
                peer = info.remote_info(),
                host_id = info.remote_host_id_prefix,
                size = net_message.data.len(),
                duration = duration.as_millis(),
                "Message delivery: incoming transfer finished",
            );
            metrics.as_ref().inspect(|x| {
                x.report_received_bytes(data.len(), &msg_type, info.send_mode());
                x.start_delivery_phase(
                    DeliveryPhase::IncomingBuffer,
                    1,
                    &msg_type,
                    info.send_mode(),
                );
            });
            let duration_after_transfer = Instant::now();
            let incoming = IncomingMessage {
                connection_info: info.clone(),
                message: net_message,
                duration_after_transfer,
            };

            // finish receiver loop if incoming consumer was detached
            if incoming_tx.send(incoming).await.is_err() {
                metrics.as_ref().inspect(|x| {
                    x.finish_delivery_phase(
                        DeliveryPhase::IncomingBuffer,
                        1,
                        &msg_type,
                        info.send_mode(),
                        duration_after_transfer.elapsed(),
                    );
                });
                receiver_stop_tx.send_replace(true);
            }
        }
        Err(err) => {
            tracing::error!(
                broadcast = info.is_broadcast(),
                peer = info.remote_info(),
                "Incoming transfer failed: {}",
                detailed(&err)
            );
            if let Some(metrics) = metrics.as_ref() {
                metrics.report_error("in_transfer_fail");
            }
            // finish the receiver loop because we have a problem with this connection
            receiver_stop_tx.send_replace(true);
        }
    }
}
