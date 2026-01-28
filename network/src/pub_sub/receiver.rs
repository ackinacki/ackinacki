use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Instant;

use transport_layer::NetConnection;

use crate::detailed;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::ConnectionWrapper;
use crate::pub_sub::connection::IncomingMessage;
use crate::pub_sub::IncomingSender;
use crate::truncate_chars;

pub async fn receiver<PeerId: Clone + Debug + Display, Connection: NetConnection + 'static>(
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper<PeerId, Connection>>,
    mut receiver_stop_rx: tokio::sync::watch::Receiver<bool>,
    incoming_tx: IncomingSender<PeerId>,
) -> anyhow::Result<()> {
    tracing::info!(
        target: "monit",
        ident = truncate_chars(&connection.connection.local_identity(), 6),
        local = connection.connection.local_addr().to_string(),
        peer = connection.info.remote_info(),
        "Receiver loop started"
    );
    loop {
        tokio::select! {
            shutdown = shutdown_rx.changed() => if shutdown.is_err() || *shutdown_rx.borrow() {
                break;
            },
            recv_incoming_ok = recv_incoming(metrics.clone(), connection.clone(), incoming_tx.clone()) => {
                if !recv_incoming_ok {
                    break;
                }
            },
            stop = receiver_stop_rx.changed() => if stop.is_err() || *receiver_stop_rx.borrow() {
                break;
            }
        }
    }
    tracing::info!(
        target: "monit",
        ident = truncate_chars( &connection.connection.local_identity(), 6),
        local = connection.connection.local_addr().to_string(),
        peer = connection.info.remote_info(),
        "Receiver loop finished"
    );
    Ok(())
}

async fn recv_incoming<PeerId: Debug + Clone + Display, Connection: NetConnection + 'static>(
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper<PeerId, Connection>>,
    incoming_tx: IncomingSender<PeerId>,
) -> bool {
    let info = connection.info.clone();
    match connection.connection.recv().await {
        Ok((data, duration)) => {
            let net_message = match NetMessage::deserialize(&data) {
                Ok(msg) => msg,
                Err(err) => {
                    tracing::error!("Failed to deserialize net message: {}", err);
                    if let Some(metrics) = metrics.as_ref() {
                        metrics.report_error("fail_deser_msg_2");
                    }
                    return false;
                }
            };

            let msg_type = net_message.label.clone();
            tracing::debug!(
                broadcast = info.is_broadcast(),
                msg_type,
                msg_id = net_message.id,
                peer = info.remote_info(),
                host_id = info.remote_cert_hash_prefix,
                addr = info.remote_addr.to_string(),
                size = net_message.data.len(),
                duration = duration.as_millis(),
                "Message delivery: incoming transfer finished",
            );
            metrics.as_ref().inspect(|x| {
                x.start_incoming_phase(data.len(), &msg_type, &info);
            });
            let duration_after_transfer = Instant::now();
            let incoming = IncomingMessage {
                connection_info: info.clone(),
                message: net_message,
                duration_after_transfer,
            };

            // finish receiver loop if incoming consumer was detached
            incoming_tx.send_ok(incoming, &metrics).await
        }
        Err(err) => {
            let err_str = detailed(&err);
            tracing::error!(
                broadcast = info.is_broadcast(),
                peer = info.remote_info(),
                "Incoming transfer failed: {err_str}",
            );

            if let Some(metrics) = metrics.as_ref() {
                let keywords = ["Custom", "ConnectionAborted", "ConnectionLost", "ShutdownByLocal"];
                if keywords.iter().all(|&key| err_str.contains(key)) {
                    metrics.report_error("conn_aborted");
                } else {
                    metrics.report_error("in_transfer_fail");
                }
            }
            // finish the receiver loop because we have a problem with this connection
            false
        }
    }
}
