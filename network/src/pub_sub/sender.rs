use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use transport_layer::NetConnection;

use crate::detailed;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::ConnectionWrapper;
use crate::pub_sub::connection::OutgoingMessage;
use crate::topology::NetTopology;
use crate::transfer::transfer;
use crate::DeliveryPhase;
use crate::SendMode;

pub async fn sender<
    PeerId: Clone + Display + Debug + PartialEq + Eq + Hash + FromStr<Err: Display>,
    Connection: NetConnection + 'static,
>(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    topology_rx: tokio::sync::watch::Receiver<NetTopology<PeerId>>,
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper<PeerId, Connection>>,
    stop_tx: tokio::sync::watch::Sender<bool>,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
    mut outgoing_messages_rx: tokio::sync::broadcast::Receiver<OutgoingMessage<PeerId>>,
) -> anyhow::Result<()> {
    tracing::info!(
        target: "monit",
        ident = &connection.connection.local_identity()[..6],
        local = connection.connection.local_addr().to_string(),
        peer = connection.info.remote_info(),
        "Sender loop started"
    );
    let finish_reason = loop {
        tokio::select! {
            shutdown_changed = shutdown_rx.changed() => if shutdown_changed.is_err() || *shutdown_rx.borrow() {
                break "shutdown signal received";
            },
            stop_changed = stop_rx.changed() => if stop_changed.is_err() || *stop_rx.borrow() {
                break "stop signal received";
            },
            _connection_closed = connection.connection.watch_close() => {
                break "connection closed";
            },
            recv_result = outgoing_messages_rx.recv() => {
                match recv_result {
                    Ok(outgoing) => {
                        metrics.as_ref().inspect(|x| {
                            x.finish_delivery_phase(
                                DeliveryPhase::OutgoingBuffer,
                                1,
                                &outgoing.message.label,
                                SendMode::Broadcast,
                                outgoing.duration_before_transfer.elapsed(),
                            );
                        });
                        if connection.allow_sending(&outgoing, topology_rx.borrow().deref()) {
                            send_message(metrics.clone(), connection.clone(), outgoing, stop_tx.clone()).await;
                        }

                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(lagged)) => {
                        tracing::error!(
                            host_id = connection.info.remote_cert_hash_prefix,
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
                        break "outgoing messages channel closed";
                    }
                }
            }
        }
    };
    let mut unsent_messages = 0usize;
    loop {
        match outgoing_messages_rx.try_recv() {
            Ok(_) => {
                unsent_messages += 1;
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(lagged)) => {
                unsent_messages += lagged as usize;
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Closed)
            | Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {
                break;
            }
        }
    }
    if unsent_messages > 0 {
        metrics.as_ref().inspect(|x| {
            x.finish_delivery_phase(
                DeliveryPhase::OutgoingBuffer,
                unsent_messages,
                crate::metrics::LAGGED,
                SendMode::Broadcast,
                std::time::Duration::from_millis(0),
            );
        });
    }

    tracing::info!(
        target: "monit",
        ident = &connection.connection.local_identity()[..6],
        local = connection.connection.local_addr().to_string(),
        peer = connection.info.remote_info(),
        "Sender loop finished: {finish_reason}"
    );
    Ok(())
}

async fn send_message<PeerId: Debug + Clone + Display, Connection: NetConnection + 'static>(
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper<PeerId, Connection>>,
    mut outgoing: OutgoingMessage<PeerId>,
    stop_tx: tokio::sync::watch::Sender<bool>,
) {
    outgoing.message.last_sender_is_proxy = connection.info.local_is_proxy;
    outgoing.message.id.push(':');
    outgoing.message.id.push_str(&connection.info.remote_cert_hash_prefix);
    let metrics = metrics.clone();
    let connection = connection.clone();
    tracing::debug!(
        host_id = connection.info.remote_cert_hash_prefix,
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
        host_id = connection.info.remote_cert_hash_prefix,
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
                host_id = connection.info.remote_cert_hash_prefix,
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
                host_id = connection.info.remote_cert_hash_prefix,
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
