use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use transport_layer::NetConnection;
use transport_layer::NetCredential;
use transport_layer::NetTransport;

use crate::detailed;
use crate::direct_sender::peer_info;
use crate::direct_sender::PeerEvent;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::connection_remote_host_id;
use crate::pub_sub::connection::ConnectionRole;
use crate::pub_sub::connection::ConnectionWrapper;
use crate::pub_sub::connection::IncomingMessage;
use crate::pub_sub::IncomingSender;
use crate::transfer::transfer;
use crate::transfer::TransportError;
use crate::DeliveryPhase;
use crate::SendMode;
use crate::ACKI_NACKI_DIRECT_PROTOCOL;

pub struct PeerSender<PeerId, Transport>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
    Transport: NetTransport,
    Transport::Connection: 'static,
{
    id: PeerId,
    addr: SocketAddr,
    connection: Option<Arc<ConnectionWrapper<Transport::Connection>>>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    transport: Transport,
    metrics: Option<NetMetrics>,
    credential: NetCredential,
    peer_events_tx: tokio::sync::mpsc::UnboundedSender<PeerEvent<PeerId>>,
    transfer_result_tx:
        tokio::sync::mpsc::Sender<(Result<usize, TransportError>, NetMessage, Instant)>,
    transfer_result_rx:
        tokio::sync::mpsc::Receiver<(Result<usize, TransportError>, NetMessage, Instant)>,
    incoming_reply_tx: IncomingSender,
}

impl<PeerId, Transport> PeerSender<PeerId, Transport>
where
    PeerId: Display + Hash + Eq + Clone + Send + Sync + 'static,
    Transport: NetTransport,
    Transport::Connection: 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        id: PeerId,
        addr: SocketAddr,
        shutdown_rx: tokio::sync::watch::Receiver<bool>,
        transport: Transport,
        metrics: Option<NetMetrics>,
        credential: NetCredential,
        peer_events_tx: tokio::sync::mpsc::UnboundedSender<PeerEvent<PeerId>>,
        incoming_reply_tx: IncomingSender,
    ) -> Self {
        let (transfer_result_tx, transfer_result_rx) = tokio::sync::mpsc::channel(10);
        Self {
            id,
            addr,
            connection: None,
            shutdown_rx,
            transport,
            metrics,
            credential,
            peer_events_tx,
            transfer_result_tx,
            transfer_result_rx,
            incoming_reply_tx,
        }
    }

    pub(crate) async fn run(
        &mut self,
        mut outgoing_rx: tokio::sync::mpsc::Receiver<(NetMessage, Instant)>,
        metrics: Option<NetMetrics>,
    ) {
        let peer_info = peer_info(&self.id, self.addr);
        tracing::trace!(peer = peer_info, "Peer sender loop started");
        let mut shutdown_rx = self.shutdown_rx.clone();
        'main: loop {
            tokio::select! {
                sender = shutdown_rx.changed() => if sender.is_err() || *self.shutdown_rx.borrow() {
                    break 'main;
                } else {
                    continue;
                },
                connect = self.connect_to_peer() => match connect {
                    Ok(connection) => {
                        self.connection = Some(Arc::new(connection));
                    },
                    Err(err) => {
                        tracing::error!(
                            peer = peer_info,
                            "Failed to connect to peer: {err}"
                        );
                        if let Some(metrics) = metrics.as_ref() {
                            metrics.report_error("fail_conn_to_peer");
                        }
                        continue;
                    }
                }
            }
            let mut shutdown_rx = self.shutdown_rx.clone();
            loop {
                tokio::select! {
                    sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                        break 'main;
                    },
                    result = outgoing_rx.recv() => {
                        if let Some((net_message, buffer_duration)) = result {
                            self.start_transfer_message(net_message,buffer_duration);
                        } else {
                            break;
                        }
                    },
                    result = receive_message(self.connection.clone(), self.metrics.clone(), self.incoming_reply_tx.clone()) => {
                        if !result {
                            break;
                        }
                    }
                    transfer_result = self.transfer_result_rx.recv() => {
                        if !self.handle_transfer_result(transfer_result.unwrap()).await {
                            break;
                        }
                    }
                }
            }
        }
        tracing::trace!(peer = peer_info, "Peer sender loop finished");
        let _ = self.peer_events_tx.send(PeerEvent::SenderStopped(self.id.clone(), self.addr));
    }

    async fn connect_to_peer(&self) -> anyhow::Result<ConnectionWrapper<Transport::Connection>>
    where
        Transport: NetTransport,
        PeerId: Display,
    {
        // track attempt counter for tracing
        let mut attempt = 0;
        let mut retry_timeout = tokio_retry::strategy::FibonacciBackoff::from_millis(100)
            .max_delay(Duration::from_secs(60 * 60));
        loop {
            match self
                .transport
                .connect(self.addr, &[ACKI_NACKI_DIRECT_PROTOCOL], self.credential.clone())
                .await
            {
                Ok(connection) => {
                    let wrapper = ConnectionWrapper::new(
                        0,
                        false,
                        None,
                        connection_remote_host_id(&connection),
                        false,
                        connection,
                        ConnectionRole::DirectReceiver,
                    )?;
                    tracing::trace!(
                        broadcast = false,
                        host_id = wrapper.info.remote_host_id_prefix,
                        addr = self.addr.to_string(),
                        "Outgoing connection established"
                    );
                    return Ok(wrapper);
                }
                Err(e) => {
                    if let Some(metrics) = self.metrics.as_ref() {
                        metrics.report_warn("fail_create_out_con");
                    }
                    tracing::warn!(
                        broadcast = false,
                        peer = peer_info(&self.id, self.addr),
                        attempt,
                        "Failed to establish outgoing connection: {}",
                        detailed(&e)
                    );
                }
            }

            tokio::time::sleep(retry_timeout.next().unwrap()).await;
            attempt += 1;
        }
    }

    fn start_transfer_message(&self, net_message: NetMessage, buffer_duration: Instant) {
        let Some(connection) = self.connection.clone() else {
            return;
        };
        let metrics = self.metrics.clone();
        let transfer_result_tx = self.transfer_result_tx.clone();
        tokio::spawn(async move {
            metrics.as_ref().inspect(|x| {
                x.finish_delivery_phase(
                    DeliveryPhase::OutgoingBuffer,
                    1,
                    &net_message.label,
                    SendMode::Direct,
                    buffer_duration.elapsed(),
                );
                x.start_delivery_phase(
                    DeliveryPhase::OutgoingTransfer,
                    1,
                    &net_message.label,
                    SendMode::Direct,
                );
            });
            tracing::debug!(
                host_id = connection.info.remote_host_id_prefix,
                msg_id = net_message.id,
                msg_type = net_message.label,
                broadcast = false,
                "Message delivery: outgoing transfer started"
            );
            let transfer_duration = Instant::now();
            let transfer_result = transfer(&connection.connection, &net_message, &metrics).await;
            metrics.as_ref().inspect(|x| {
                x.finish_delivery_phase(
                    DeliveryPhase::OutgoingTransfer,
                    1,
                    &net_message.label,
                    SendMode::Direct,
                    transfer_duration.elapsed(),
                )
            });
            if let Err(err) =
                transfer_result_tx.send((transfer_result, net_message, transfer_duration)).await
            {
                if let Some(metrics) = metrics.as_ref() {
                    metrics.report_error("fail_report_delivery");
                }
                tracing::error!("Can not report message delivery result: {}", err);
            }
        });
    }

    async fn handle_transfer_result(
        &mut self,
        transfer_result: (Result<usize, TransportError>, NetMessage, Instant),
    ) -> bool {
        let Some(connection) = &self.connection else {
            return false;
        };
        let info = &connection.info;
        match transfer_result {
            (Ok(bytes_sent), message, transfer_duration) => {
                tracing::debug!(
                    host_id = info.remote_host_id_prefix,
                    msg_id = message.id,
                    msg_type = message.label,
                    broadcast = false,
                    addr = self.addr.to_string(),
                    duration = transfer_duration.elapsed().as_millis(),
                    "Message delivery: outgoing transfer finished"
                );
                self.metrics.as_ref().inspect(|m| {
                    m.report_sent_bytes(bytes_sent, &message.label, SendMode::Direct);
                });
                true
            }
            (Err(err), net_message, _) => {
                tracing::error!(
                    broadcast = false,
                    msg_type = net_message.label,
                    msg_id = net_message.id,
                    host_id = info.remote_host_id_prefix,
                    addr = self.addr.to_string(),
                    "Message delivery: outgoing transfer failed: {}",
                    detailed(&err)
                );
                self.metrics.as_ref().inspect(|x| {
                    x.report_outgoing_transfer_error(&net_message.label, SendMode::Direct, err);
                });
                connection.connection.close(0).await;
                false
            }
        }
    }
}

async fn receive_message<Connection: NetConnection>(
    connection: Option<Arc<ConnectionWrapper<Connection>>>,
    metrics: Option<NetMetrics>,
    incoming_reply_tx: IncomingSender,
) -> bool {
    let Some(connection) = connection else {
        return false;
    };
    let info = connection.info.clone();
    match connection.connection.recv().await {
        Ok((data, duration)) => {
            let net_message = match bincode::deserialize::<NetMessage>(&data) {
                Ok(msg) => msg,
                Err(err) => {
                    tracing::error!("Failed to deserialize net message: {}", err);
                    if let Some(metrics) = metrics.as_ref() {
                        metrics.report_error("fail_deser_msg_1");
                    }
                    return false;
                }
            };

            let msg_type = net_message.label.clone();
            tracing::debug!(
                broadcast = false,
                msg_type,
                msg_id = net_message.id,
                peer = info.remote_info(),
                host_id = info.remote_host_id_prefix,
                addr = info.remote_addr.to_string(),
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
            if incoming_reply_tx.send(incoming).await.is_err() {
                metrics.as_ref().inspect(|x| {
                    x.finish_delivery_phase(
                        DeliveryPhase::IncomingBuffer,
                        1,
                        &msg_type,
                        info.send_mode(),
                        duration_after_transfer.elapsed(),
                    );
                });
                false
            } else {
                true
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
            false
        }
    }
}
