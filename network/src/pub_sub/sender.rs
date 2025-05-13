use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;
use tokio::sync::watch;

use crate::detailed;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::ConnectionWrapper;
use crate::pub_sub::connection::OutgoingMessage;
use crate::transfer::transfer;
use crate::DeliveryPhase;
use crate::SendMode;

pub async fn sender(
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper>,
    mut stop_rx: watch::Receiver<bool>,
    mut outgoing_messages_rx: tokio::sync::broadcast::Receiver<OutgoingMessage>,
) -> anyhow::Result<()> {
    let (transfer_result_tx, mut transfer_result_rx) = mpsc::channel(10);
    loop {
        tokio::select! {
            recv_result = outgoing_messages_rx.recv() => {
                let mut outgoing = match recv_result {
                    Ok(message) => message,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(lagged)) => {
                        tracing::error!(lagged, "Outgoing sender lagged");
                        metrics.as_ref().inspect(|x| {
                            x.finish_delivery_phase(
                                DeliveryPhase::OutgoingBuffer,
                                lagged as usize,
                                crate::metrics::LAGGED,
                                SendMode::Broadcast,
                                std::time::Duration::from_millis(0),
                            );
                        });
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        // Finish sender loop if outgoing messages sender detached or lagged
                        break;
                    }
                };
                if connection.allow_sending(&outgoing) {
                    outgoing.message.id.push(':');
                    outgoing.message.id.push_str(&connection.host_id_prefix);
                    let metrics = metrics.clone();
                    let connection = connection.clone();
                    let transfer_result_tx = transfer_result_tx.clone();
                    tracing::debug!(
                        host_id = connection.host_id_prefix,
                        msg_id = outgoing.message.id,
                        msg_type = outgoing.message.label,
                        broadcast = true,
                        "Message delivery: accepted by peer dispatcher"
                    );

                    // It is not critical task because it serves single transfer
                    // and we do not handle a result
                    tokio::spawn(async move {
                        metrics.as_ref().inspect(|x|{
                            x.finish_delivery_phase(
                                DeliveryPhase::OutgoingBuffer,
                                1,
                                &outgoing.message.label,
                                SendMode::Broadcast,
                                outgoing.duration_before_transfer.elapsed(),
                            );
                            x.start_delivery_phase(
                                DeliveryPhase::OutgoingTransfer,
                                1,
                                &outgoing.message.label,
                                SendMode::Broadcast,
                            );
                        });
                        tracing::debug!(
                            host_id = connection.host_id_prefix,
                            msg_id = outgoing.message.id,
                            msg_type = outgoing.message.label,
                            broadcast = true,
                            "Message delivery: outgoing transfer started"
                        );
                        let transfer_duration = Instant::now();
                        let transfer_result = transfer(&connection.connection, &outgoing.message,  &metrics).await;
                        metrics.as_ref().inspect(|x| {
                            x.finish_delivery_phase(
                                DeliveryPhase::OutgoingTransfer,
                                1,
                                &outgoing.message.label,
                                SendMode::Broadcast,
                                transfer_duration.elapsed(),
                            );
                        });
                        if let Err(err) = transfer_result_tx.send((transfer_result, outgoing.message)).await {
                            tracing::error!("Can not report message delivery result: {}",err);
                        }
                    });
                }
            },
            transfer_result = transfer_result_rx.recv() => {
                match transfer_result.unwrap() {
                    (Ok(()), net_message) => {
                        tracing::debug!(
                            host_id = connection.host_id_prefix,
                            msg_id = net_message.id,
                            msg_type = net_message.label,
                            broadcast = true,
                            "Message delivery: outgoing transfer finished"
                        );
                    },
                    (Err(err), net_message) => {
                        tracing::error!(
                            host_id = connection.host_id_prefix,
                            msg_id = net_message.id,
                            msg_type = net_message.label,
                            broadcast = true,
                            "Message delivery: outgoing transfer failed: {}",
                            detailed(&err),
                        );
                        metrics.as_ref().inspect(|x| {
                            x.report_outgoing_transfer_error(&net_message.label, SendMode::Broadcast, err);
                        });
                        break;
                    },
                }
            },
            _ = stop_rx.changed() => {
                if *stop_rx.borrow_and_update() {
                    break;
                }
            }
        }
    }
    tracing::trace!(peer = connection.peer_info(), "Sender loop finished");
    Ok(())
}
