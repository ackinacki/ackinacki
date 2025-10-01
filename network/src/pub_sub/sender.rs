use std::sync::Arc;
use std::time::Instant;

use transport_layer::NetConnection;

use crate::detailed;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::ConnectionWrapper;
use crate::pub_sub::connection::OutgoingMessage;
use crate::transfer::transfer;
use crate::DeliveryPhase;
use crate::SendMode;

pub async fn sender<Connection: NetConnection + 'static>(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper<Connection>>,
    stop_tx: tokio::sync::watch::Sender<bool>,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
    mut outgoing_messages_rx: tokio::sync::broadcast::Receiver<OutgoingMessage>,
) -> anyhow::Result<()> {
    tracing::trace!(
        ident = &connection.connection.local_identity()[..6],
        local = connection.connection.local_addr().to_string(),
        peer = connection.info.remote_info(),
        "Sender loop started"
    );
    loop {
        tokio::select! {
            sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                break;
            },
            sender = stop_rx.changed() => if sender.is_err() || *stop_rx.borrow() {
                break;
            },
            _ = connection.connection.watch_close() => {
                break;
            },
            recv_result = outgoing_messages_rx.recv() => {
                match recv_result {
                    Ok(message) => {
                        send_message(metrics.clone(), connection.clone(), message, stop_tx.clone()).await;
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(lagged)) => {
                        tracing::error!(
                            host_id = connection.info.remote_host_id_prefix,
                            addr = connection.info.remote_addr.to_string(),
                            broadcast = true,
                            lagged,
                            "Outgoing sender lagged"
                        );
                        metrics.as_ref().inspect(|x| {
                            x.finish_delivery_phase(
                                DeliveryPhase::OutgoingBuffer,
                                lagged as usize,
                                crate::metrics::LAGGED,
                                SendMode::Broadcast,
                                std::time::Duration::from_millis(0),
                            );
                            x.report_error("out_sender_lagged");
                        });

                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        // Finish sender loop if outgoing messages sender detached
                        break;
                    }
                }
            }
        }
    }
    tracing::trace!(
        ident = &connection.connection.local_identity()[..6],
        local = connection.connection.local_addr().to_string(),
        peer = connection.info.remote_info(),
        "Sender loop finished"
    );
    Ok(())
}

async fn send_message<Connection: NetConnection + 'static>(
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper<Connection>>,
    mut outgoing: OutgoingMessage,
    stop_tx: tokio::sync::watch::Sender<bool>,
) {
    metrics.as_ref().inspect(|x| {
        x.finish_delivery_phase(
            DeliveryPhase::OutgoingBuffer,
            1,
            &outgoing.message.label,
            SendMode::Broadcast,
            outgoing.duration_before_transfer.elapsed(),
        );
    });
    if !connection.allow_sending(&outgoing) {
        return;
    }
    outgoing.message.last_sender_is_proxy = connection.info.local_is_proxy;
    outgoing.message.id.push(':');
    outgoing.message.id.push_str(&connection.info.remote_host_id_prefix);
    let metrics = metrics.clone();
    let connection = connection.clone();
    tracing::debug!(
        host_id = connection.info.remote_host_id_prefix,
        addr = connection.info.remote_addr.to_string(),
        msg_id = outgoing.message.id,
        msg_type = outgoing.message.label,
        broadcast = true,
        "Message delivery: accepted by peer dispatcher"
    );

    metrics.as_ref().inspect(|x| {
        x.start_delivery_phase(
            DeliveryPhase::OutgoingTransfer,
            1,
            &outgoing.message.label,
            SendMode::Broadcast,
        );
    });
    tracing::debug!(
        host_id = connection.info.remote_host_id_prefix,
        addr = connection.info.remote_addr.to_string(),
        msg_id = outgoing.message.id,
        msg_type = outgoing.message.label,
        broadcast = true,
        "Message delivery: outgoing transfer started"
    );
    let transfer_duration = Instant::now();
    let transfer_result = transfer(&connection.connection, &outgoing.message, &metrics).await;
    metrics.as_ref().inspect(|x| {
        x.finish_delivery_phase(
            DeliveryPhase::OutgoingTransfer,
            1,
            &outgoing.message.label,
            SendMode::Broadcast,
            transfer_duration.elapsed(),
        );
    });

    match transfer_result {
        Ok(bytes_sent) => {
            tracing::debug!(
                host_id = connection.info.remote_host_id_prefix,
                addr = connection.info.remote_addr.to_string(),
                msg_id = outgoing.message.id,
                msg_type = outgoing.message.label,
                broadcast = true,
                duration = transfer_duration.elapsed().as_millis(),
                "Message delivery: outgoing transfer finished"
            );
            metrics.as_ref().inspect(|m| {
                m.report_sent_bytes(bytes_sent, &outgoing.message.label, SendMode::Broadcast);
            });
        }
        Err(err) => {
            tracing::error!(
                host_id = connection.info.remote_host_id_prefix,
                addr = connection.info.remote_addr.to_string(),
                msg_id = outgoing.message.id,
                msg_type = outgoing.message.label,
                broadcast = true,
                "Message delivery: outgoing transfer failed: {}",
                detailed(&err),
            );
            metrics.as_ref().inspect(|x| {
                x.report_outgoing_transfer_error(&outgoing.message.label, SendMode::Broadcast, err);
            });
            stop_tx.send_replace(true);
        }
    }
}
