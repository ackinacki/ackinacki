use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use ed25519_dalek::VerifyingKey;
use transport_layer::get_ed_pubkey_from_cert_der;
use transport_layer::CertHash;
use transport_layer::NetConnection;
use transport_layer::NetTransport;

use crate::detailed;
use crate::host_id_prefix;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::receiver;
use crate::pub_sub::sender;
use crate::pub_sub::IncomingSender;
use crate::pub_sub::PubSub;
use crate::DeliveryPhase;
use crate::SendMode;

#[derive(Debug, Default, Copy, Clone)]
pub struct ConnectionRoles {
    pub subscriber: bool,
    pub publisher: bool,
    pub direct_receiver: bool,
    pub direct_sender: bool,
}

impl ConnectionRoles {
    pub fn subscriber() -> Self {
        Self { subscriber: true, ..Default::default() }
    }

    pub fn publisher() -> Self {
        Self { publisher: true, ..Default::default() }
    }

    pub fn direct_sender() -> Self {
        Self { direct_sender: true, ..Default::default() }
    }

    pub fn direct_receiver() -> Self {
        Self { direct_receiver: true, ..Default::default() }
    }

    pub fn is_broadcast(&self) -> bool {
        self.subscriber || self.publisher
    }

    pub fn send_mode(&self) -> SendMode {
        if self.publisher || self.subscriber {
            SendMode::Broadcast
        } else {
            SendMode::Direct
        }
    }
}

#[derive(Debug)]
pub struct ConnectionInfo {
    pub id: u64,
    pub local_is_proxy: bool,
    pub roles: ConnectionRoles,
    pub remote_addr: SocketAddr,
    pub remote_host_id: String,
    pub remote_host_id_prefix: String,
    pub remote_is_proxy: bool,
    pub remote_cert_hash: CertHash,
    pub remote_ed_pubkey: Option<VerifyingKey>,
}

impl ConnectionInfo {
    pub fn remote_info(&self) -> String {
        let mut info = self.remote_addr.to_string();
        if self.roles.subscriber {
            info.push_str(" (publisher)");
        }
        if self.roles.publisher {
            info.push_str(" (subscriber)");
        }
        if self.roles.direct_receiver {
            info.push_str(" (direct sender)");
        }
        if self.roles.direct_sender {
            info.push_str(" (direct receiver)");
        }
        info
    }
}

#[derive(Debug)]
pub struct ConnectionWrapper<Connection: NetConnection> {
    pub info: Arc<ConnectionInfo>,
    pub connection: Connection,
}

pub fn connection_remote_host_id(connection: &impl NetConnection) -> String {
    connection.remote_identity()
}

impl<Connection: NetConnection> ConnectionWrapper<Connection> {
    pub fn new(
        id: u64,
        local_is_proxy: bool,
        remote_addr: Option<SocketAddr>,
        remote_host_id: String,
        remote_is_proxy: bool,
        connection: Connection,
        roles: ConnectionRoles,
    ) -> anyhow::Result<Self> {
        let remote_host_id_prefix = host_id_prefix(&remote_host_id).to_string();
        let cert =
            connection.remote_certificate().ok_or_else(|| anyhow::anyhow!("No certificate"))?;
        Ok(Self {
            info: Arc::new(ConnectionInfo {
                id,
                local_is_proxy,
                remote_addr: if let Some(addr) = remote_addr {
                    addr
                } else {
                    connection.remote_addr()
                },
                remote_host_id,
                remote_host_id_prefix,
                remote_is_proxy,
                remote_cert_hash: CertHash::from(&cert),
                remote_ed_pubkey: get_ed_pubkey_from_cert_der(&cert)?,
                roles,
            }),
            connection,
        })
    }

    pub fn allow_sending(&self, outgoing: &OutgoingMessage) -> bool {
        if outgoing.message.last_sender_is_proxy && self.info.remote_is_proxy {
            return false;
        }
        match &outgoing.delivery {
            MessageDelivery::Broadcast => self.info.roles.publisher,
            MessageDelivery::BroadcastExcluding(excluding) => {
                self.info.roles.publisher && self.info.remote_host_id != excluding.remote_host_id
            }
            MessageDelivery::Addr(addr) => self.info.remote_addr == *addr,
        }
    }
}

pub async fn connection_supervisor<Transport: NetTransport + 'static>(
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    pub_sub: PubSub<Transport>,
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper<Transport::Connection>>,
    incoming_messages_tx: Option<IncomingSender>,
    outgoing_messages_rx: Option<tokio::sync::broadcast::Receiver<OutgoingMessage>>,
    connection_closed_tx: tokio::sync::mpsc::Sender<Arc<ConnectionInfo>>,
) -> anyhow::Result<()> {
    let (sender_stop_tx, sender_stop_rx) = tokio::sync::watch::channel(false);
    let (receiver_stop_tx, receiver_stop_rx) = tokio::sync::watch::channel(false);
    let result = match (incoming_messages_tx, outgoing_messages_rx) {
        (Some(incoming_messages_tx), Some(outgoing_messages_rx)) => {
            tokio::select! {
                result = tokio::spawn(sender::sender(
                    shutdown_rx.clone(),
                    metrics.clone(),
                    connection.clone(),
                    sender_stop_tx.clone(),
                    sender_stop_rx,
                    outgoing_messages_rx)
                ) => trace_connection_task_result(result, "Sender", &connection.info),
                result = tokio::spawn(receiver::receiver(
                    shutdown_rx.clone(),
                    metrics.clone(),
                    connection.clone(),
                    receiver_stop_tx.clone(),
                    receiver_stop_rx,
                    incoming_messages_tx)
                ) => trace_connection_task_result(result, "Receiver", &connection.info)
            }
        }
        (Some(incoming_messages_tx), None) => {
            let receiver = receiver::receiver(
                shutdown_rx.clone(),
                metrics.clone(),
                connection.clone(),
                receiver_stop_tx.clone(),
                receiver_stop_rx,
                incoming_messages_tx,
            );
            trace_connection_task_result(tokio::spawn(receiver).await, "Receiver", &connection.info)
        }
        (None, Some(outgoing_messages_rx)) => {
            let sender = sender::sender(
                shutdown_rx.clone(),
                metrics.clone(),
                connection.clone(),
                sender_stop_tx.clone(),
                sender_stop_rx,
                outgoing_messages_rx,
            );
            trace_connection_task_result(tokio::spawn(sender).await, "Sender", &connection.info)
        }
        (None, None) => Ok(Ok(())),
    };
    pub_sub.remove_connection(&connection.info);
    tracing::trace!(peer = connection.info.remote_info(), "Connection supervisor finished");
    let _ = sender_stop_tx.send_replace(true);
    let _ = receiver_stop_tx.send_replace(true);
    let _ = connection_closed_tx.send(connection.info.clone()).await;
    result?
}

fn trace_connection_task_result(
    result: Result<anyhow::Result<()>, tokio::task::JoinError>,
    name: &str,
    connection_info: &ConnectionInfo,
) -> Result<anyhow::Result<()>, tokio::task::JoinError> {
    match &result {
        Ok(result) => match result {
            Ok(_) => {
                tracing::info!(peer = connection_info.remote_info(), "{name} task finished");
            }
            Err(err) => {
                tracing::error!(
                    peer = connection_info.remote_info(),
                    "{name} task error: {}",
                    detailed(err)
                );
            }
        },
        Err(err) => {
            tracing::error!(
                peer = connection_info.remote_info(),
                "Critical: {name} task panicked: {}",
                detailed(err)
            )
        }
    }
    result
}

#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub connection_info: Arc<ConnectionInfo>,
    pub message: NetMessage,
    pub duration_after_transfer: Instant,
}

impl IncomingMessage {
    pub fn finish<Message>(&self, metrics: &Option<NetMetrics>) -> Option<Message>
    where
        Message: Debug + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
    {
        let _ = metrics.as_ref().inspect(|m| {
            m.finish_delivery_phase(
                DeliveryPhase::IncomingBuffer,
                1,
                &self.message.label,
                self.connection_info.roles.send_mode(),
                self.duration_after_transfer.elapsed(),
            );
        });
        tracing::debug!(
            host_id = self.connection_info.remote_host_id_prefix,
            msg_id = self.message.id,
            msg_type = self.message.label,
            broadcast = self.connection_info.roles.subscriber,
            "Message delivery: decode"
        );
        let (message, decompress_ms, deserialize_ms) = match self.message.decode() {
            Ok(message) => message,
            Err(err) => {
                tracing::error!("Failed decoding incoming message: {}", err);
                tracing::debug!(
                    host_id = self.connection_info.remote_host_id_prefix,
                    msg_id = self.message.id,
                    msg_type = self.message.label,
                    broadcast = self.connection_info.roles.subscriber,
                    "Message delivery: decoding failed"
                );
                return None;
            }
        };
        match self.message.delivery_duration_ms() {
            Ok(delivery_duration_ms) => {
                tracing::info!(
                    "Received incoming {}, peer {}, duration {}",
                    self.message.label,
                    self.connection_info.remote_info(),
                    delivery_duration_ms
                );
                let _ = metrics.as_ref().inspect(|m| {
                    m.report_incoming_message_delivery_duration(
                        delivery_duration_ms,
                        &self.message.label,
                    );
                });
            }
            Err(reason) => {
                tracing::error!("{}", reason);
                tracing::info!(
                    "Received incoming {}, peer {}, duration N/A",
                    self.message.label,
                    self.connection_info.remote_info(),
                );
            }
        }
        tracing::debug!(
            host_id = self.connection_info.remote_host_id_prefix,
            msg_id = self.message.id,
            msg_type = self.message.label,
            broadcast = self.connection_info.roles.subscriber,
            decompress_ms,
            deserialize_ms,
            "Message delivery: finished"
        );
        Some(message)
    }
}

#[derive(Debug, Clone)]
pub enum MessageDelivery {
    Broadcast,
    BroadcastExcluding(Arc<ConnectionInfo>),
    Addr(SocketAddr),
}

#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub delivery: MessageDelivery,
    pub message: NetMessage,
    pub duration_before_transfer: Instant,
}
