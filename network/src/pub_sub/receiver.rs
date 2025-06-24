use std::sync::Arc;
use std::time::Instant;

use tokio::sync::watch;
use transport_layer::NetConnection;
use transport_layer::NetRecvRequest;

use crate::detailed;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::ConnectionInfo;
use crate::pub_sub::connection::ConnectionWrapper;
use crate::pub_sub::connection::IncomingMessage;
use crate::pub_sub::IncomingSender;
use crate::DeliveryPhase;

pub async fn receiver<Connection: NetConnection + 'static>(
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper<Connection>>,
    receiver_stop_tx: watch::Sender<bool>,
    mut receiver_stop_rx: watch::Receiver<bool>,
    incoming_tx: IncomingSender,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            _ = receive_message(metrics.clone(), connection.clone(), incoming_tx.clone(), receiver_stop_tx.clone()) => {
            },
            _ = receiver_stop_rx.changed() => if *receiver_stop_rx.borrow() {
                break;
            }
        }
    }
    tracing::trace!(peer = connection.info.remote_info(), "Receiver loop finished");
    Ok(())
}

async fn receive_message<Connection: NetConnection + 'static>(
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper<Connection>>,
    incoming_tx: IncomingSender,
    receiver_stop_tx: watch::Sender<bool>,
) {
    let info = connection.info.clone();
    match connection.connection.accept_recv().await {
        Ok(request) => {
            tokio::spawn(transfer_message(metrics, info, request, incoming_tx, receiver_stop_tx));
        }
        Err(err) => {
            tracing::error!(
                broadcast = info.roles.is_broadcast(),
                peer = info.remote_info(),
                "Incoming transfer failed: {}",
                detailed(&err)
            );
            // finish the receiver loop because we have a problem with this connection
            receiver_stop_tx.send_replace(true);
        }
    }
}

async fn transfer_message(
    metrics: Option<NetMetrics>,
    info: Arc<ConnectionInfo>,
    request: impl NetRecvRequest,
    incoming_tx: IncomingSender,
    receiver_stop_tx: watch::Sender<bool>,
) {
    let start_time = Instant::now();
    let data = match request.recv().await {
        Ok(data) => data,
        Err(err) => {
            tracing::error!(
                broadcast = info.roles.send_mode().is_broadcast(),
                peer = info.remote_info(),
                "Incoming transfer failed: {}",
                detailed(&err)
            );
            // finish the receiver loop because we have a problem with this connection
            receiver_stop_tx.send_replace(true);
            return;
        }
    };

    let net_message = match bincode::deserialize::<NetMessage>(&data) {
        Ok(msg) => msg,
        Err(err) => {
            tracing::error!("Failed to deserialize net message: {}", err);
            receiver_stop_tx.send_replace(true);
            return;
        }
    };

    let msg_type = net_message.label.clone();
    tracing::debug!(
        broadcast = info.roles.is_broadcast(),
        msg_type,
        msg_id = net_message.id,
        peer = info.remote_info(),
        host_id = info.remote_host_id_prefix,
        size = net_message.data.len(),
        duration = start_time.elapsed().as_millis(),
        "Message delivery: incoming transfer finished",
    );
    metrics.as_ref().inspect(|x| {
        x.report_received_bytes(data.len(), &msg_type, info.roles.send_mode());
        x.start_delivery_phase(DeliveryPhase::IncomingBuffer, 1, &msg_type, info.roles.send_mode());
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
                info.roles.send_mode(),
                duration_after_transfer.elapsed(),
            );
        });
        receiver_stop_tx.send_replace(true);
    }
}
