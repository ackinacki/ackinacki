use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use transport_layer::NetConnection;
use transport_layer::NetCredential;
use transport_layer::NetTransport;

use crate::detailed;
use crate::direct_sender::peer_info;
use crate::direct_sender::PeerCommand;
use crate::direct_sender::PeerEvent;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::ConnectionRole;
use crate::pub_sub::connection::ConnectionWrapper;
use crate::pub_sub::connection::IncomingMessage;
use crate::pub_sub::IncomingSender;
use crate::topology::NetEndpoint;
use crate::topology::NetPeer;
use crate::transfer::transfer;
use crate::transfer::TransportError;
use crate::DeliveryPhase;
use crate::SendMode;
use crate::ACKI_NACKI_DIRECT_PROTOCOL;
const CONNECTION_TIMEOUT_SEC: u64 = 5;

pub struct PeerSender<PeerId, Transport>
where
    PeerId: Display + Debug + Hash + Eq + Clone + Send + Sync + 'static,
    Transport: NetTransport,
    Transport::Connection: 'static,
{
    id: PeerId,
    addr: SocketAddr,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    transport: Transport,
    metrics: Option<NetMetrics>,
    credential: NetCredential,
    peer_events_tx: tokio::sync::mpsc::UnboundedSender<PeerEvent<PeerId>>,
    transfer_result_tx:
        tokio::sync::mpsc::Sender<(Result<usize, TransportError>, NetMessage, Instant)>,
    transfer_result_rx:
        tokio::sync::mpsc::Receiver<(Result<usize, TransportError>, NetMessage, Instant)>,
    incoming_reply_tx: IncomingSender<PeerId>,
}

impl<PeerId, Transport> PeerSender<PeerId, Transport>
where
    PeerId: Display + Debug + Hash + Eq + Clone + Send + Sync + FromStr<Err: Display> + 'static,
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
        incoming_reply_tx: IncomingSender<PeerId>,
    ) -> Self {
        let (transfer_result_tx, transfer_result_rx) = tokio::sync::mpsc::channel(10);
        Self {
            id,
            addr,
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
        mut command_rx: tokio::sync::mpsc::Receiver<PeerCommand>,
        metrics: Option<NetMetrics>,
    ) {
        let peer_info = peer_info(&self.id, self.addr);
        tracing::info!(
            target: "monit",
            peer = peer_info,
            "Peer sender loop started",
        );
        let reason = match self.connect_to_peer().await {
            Ok(connection) => {
                let connection = Arc::new(connection);
                let mut shutdown_rx = self.shutdown_rx.clone();
                loop {
                    tokio::select! {
                        sender = shutdown_rx.changed() => if sender.is_err() || *shutdown_rx.borrow() {
                            break "shutdown signal received";
                        },
                        command = command_rx.recv() => match command {
                            Some(PeerCommand::SendMessage(net_message, buffer_duration)) => {
                                self.start_send_outgoing(connection.clone(), net_message, buffer_duration);
                            },
                            Some(PeerCommand::Stop) => {
                                break "stop signal received";
                            },
                            None => {
                                break "command channel closed";
                            }
                        },
                        result = recv_incoming(connection.clone(), self.metrics.clone(), self.incoming_reply_tx.clone()) => {
                            if !result {
                                break "recv message failed";
                            }
                        },
                        transfer_result = self.transfer_result_rx.recv() => {
                            if let Some(result) = transfer_result {
                                if !self.handle_transfer_result(connection.clone(), result).await {
                                    break "transfer result channel closed";
                                }
                            };
                        }
                    }
                }
            }
            Err(err) => {
                tracing::error!(peer = peer_info, "Failed to direct connect to peer: {err}");
                if let Some(metrics) = metrics.as_ref() {
                    metrics.report_error("fail_conn_to_peer");
                }
                "connection failed"
            }
        };

        tracing::info!(
            target: "monit",
            peer = peer_info,
            "Peer sender loop finished: {reason}",
        );

        // Drain the channel regardless of the stop reason and report the number of messages
        while let Ok(PeerCommand::SendMessage(net_message, buffer_duration)) = command_rx.try_recv()
        {
            if let Some(metrics) = metrics.as_ref() {
                metrics.finish_delivery_phase(
                    DeliveryPhase::OutgoingBuffer,
                    1,
                    &net_message.label,
                    SendMode::Direct,
                    buffer_duration.elapsed(),
                );
            }
        }
        let _ = self.peer_events_tx.send(PeerEvent::SenderStopped(self.id.clone(), self.addr));
    }

    async fn connect_to_peer(
        &mut self,
    ) -> anyhow::Result<ConnectionWrapper<PeerId, Transport::Connection>>
    where
        Transport: NetTransport,
        PeerId: Display + Debug,
    {
        // track attempt counter for tracing
        let mut attempt = 0;
        let mut retry_timeout = tokio_retry::strategy::FibonacciBackoff::from_millis(100)
            .max_delay(Duration::from_secs(60 * 60));
        let deadline = Instant::now() + Duration::from_secs(CONNECTION_TIMEOUT_SEC);
        loop {
            match self
                .transport
                .connect(self.addr, &[ACKI_NACKI_DIRECT_PROTOCOL], self.credential.clone())
                .await
            {
                Ok(connection) => {
                    let endpoint =
                        NetEndpoint::Peer(NetPeer::with_id_and_addr(self.id.clone(), self.addr));
                    let wrapper = ConnectionWrapper::new(
                        0,
                        false,
                        None,
                        endpoint,
                        connection,
                        ConnectionRole::DirectReceiver,
                    )?;
                    tracing::debug!(
                        target: "monit",
                        broadcast = false,
                        host_id = wrapper.info.remote_cert_hash_prefix,
                        addr = self.addr.to_string(),
                        "Outgoing connection established",
                    );
                    return Ok(wrapper);
                }
                Err(e) => {
                    if let Some(metrics) = self.metrics.as_ref() {
                        metrics.report_warn("fail_create_out_con");
                    }
                    tracing::warn!(
                        target: "monit",
                        broadcast = false,
                        peer = peer_info(&self.id, self.addr),
                        attempt,
                        "Failed to establish outgoing connection: {}",
                        detailed(&e),
                    );
                }
            }
            let delay = retry_timeout.next().expect("fibonacci_backoff is an infinite iterator");
            if Instant::now() + delay > deadline {
                return Err(anyhow::anyhow!("Cannot connect for {CONNECTION_TIMEOUT_SEC} seconds"));
            }
            tokio::time::sleep(delay).await;

            attempt += 1;
        }
    }

    fn start_send_outgoing(
        &self,
        connection: Arc<ConnectionWrapper<PeerId, Transport::Connection>>,
        net_message: NetMessage,
        buffer_duration: Instant,
    ) {
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
                host_id = connection.info.remote_cert_hash_prefix,
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
        connection: Arc<ConnectionWrapper<PeerId, Transport::Connection>>,
        transfer_result: (Result<usize, TransportError>, NetMessage, Instant),
    ) -> bool {
        let info = &connection.info;
        match transfer_result {
            (Ok(bytes_sent), message, transfer_duration) => {
                tracing::debug!(
                    host_id = info.remote_cert_hash_prefix,
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
                    host_id = info.remote_cert_hash_prefix,
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

async fn recv_incoming<PeerId: Debug + Clone + Display, Connection: NetConnection>(
    connection: Arc<ConnectionWrapper<PeerId, Connection>>,
    metrics: Option<NetMetrics>,
    incoming_reply_tx: IncomingSender<PeerId>,
) -> bool {
    let info = connection.info.clone();
    match connection.connection.recv().await {
        Ok((data, duration)) => {
            let net_message = match NetMessage::deserialize(&data) {
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
            incoming_reply_tx.send_ok(incoming, &metrics).await
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
