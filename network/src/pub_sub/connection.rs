use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use ed25519_dalek::VerifyingKey;
use transport_layer::get_pubkeys_from_cert_der;
use transport_layer::CertHash;
use transport_layer::NetConnection;
use transport_layer::NetTransport;

use crate::detailed;
use crate::host_id_prefix;
use crate::message::NetMessage;
use crate::metrics::to_label_kind;
use crate::metrics::NetMetrics;
use crate::pub_sub::receiver;
use crate::pub_sub::sender;
use crate::pub_sub::IncomingSender;
use crate::pub_sub::PubSub;
use crate::DeliveryPhase;
use crate::SendMode;

#[derive(Debug, Copy, Clone)]
pub enum ConnectionRole {
    Subscriber,
    Publisher,
    DirectReceiver,
}

impl ConnectionRole {
    pub fn is_publisher(&self) -> bool {
        matches!(self, Self::Publisher)
    }

    pub fn is_subscriber(&self) -> bool {
        matches!(self, Self::Subscriber)
    }
}

#[derive(Debug)]
pub struct ConnectionInfo {
    pub id: u64,
    pub local_is_proxy: bool,
    pub role: ConnectionRole,
    pub remote_addr: SocketAddr,
    pub remote_host_id: String,
    pub remote_host_id_prefix: String,
    pub remote_is_proxy: bool,
    pub remote_cert_hash: CertHash,
    pub remote_cert_pubkeys: Vec<VerifyingKey>,
}

impl ConnectionInfo {
    pub fn remote_info(&self) -> String {
        self.remote_addr.to_string()
            + match self.role {
                ConnectionRole::Subscriber => " (publisher)",
                ConnectionRole::Publisher => " (subscriber)",
                ConnectionRole::DirectReceiver => " (direct sender)",
            }
    }

    pub fn is_publisher(&self) -> bool {
        matches!(self.role, ConnectionRole::Publisher)
    }

    pub fn is_subscriber(&self) -> bool {
        matches!(self.role, ConnectionRole::Subscriber)
    }

    pub fn is_broadcast(&self) -> bool {
        matches!(self.role, ConnectionRole::Subscriber | ConnectionRole::Publisher)
    }

    pub fn send_mode(&self) -> SendMode {
        if self.is_broadcast() {
            SendMode::Broadcast
        } else {
            SendMode::Direct
        }
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
        role: ConnectionRole,
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
                remote_cert_pubkeys: get_pubkeys_from_cert_der(&cert)?,
                role,
            }),
            connection,
        })
    }

    pub fn allow_sending(&self, outgoing: &OutgoingMessage) -> bool {
        if outgoing.message.last_sender_is_proxy && self.info.remote_is_proxy {
            return false;
        }
        match &outgoing.delivery {
            MessageDelivery::Broadcast => self.info.is_publisher(),
            MessageDelivery::BroadcastExcluding(excluding) => {
                self.info.is_publisher() && self.info.remote_host_id != excluding.remote_host_id
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
                ) => trace_connection_task_result(result, "Sender", &connection.info, &metrics),
                result = tokio::spawn(receiver::receiver(
                    shutdown_rx.clone(),
                    metrics.clone(),
                    connection.clone(),
                    receiver_stop_tx.clone(),
                    receiver_stop_rx,
                    incoming_messages_tx)
                ) => trace_connection_task_result(result, "Receiver", &connection.info, &metrics)
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
            trace_connection_task_result(
                tokio::spawn(receiver).await,
                "Receiver",
                &connection.info,
                &metrics,
            )
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
            trace_connection_task_result(
                tokio::spawn(sender).await,
                "Sender",
                &connection.info,
                &metrics,
            )
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
    metrics: &Option<NetMetrics>,
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
                if let Some(metrics) = metrics.as_ref() {
                    let kind = to_label_kind(format!("trace_conn_{name}"));
                    metrics.report_error(kind);
                }
            }
        },
        Err(err) => {
            tracing::error!(
                peer = connection_info.remote_info(),
                "Critical: {name} task panicked: {}",
                detailed(err)
            );
            if let Some(metrics) = metrics.as_ref() {
                let kind = to_label_kind(format!("trace_conn_crit_{name}"));
                metrics.report_error(kind);
            }
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
    pub fn finish<Message>(&self, metrics: &Option<NetMetrics>) -> Option<(Message, SocketAddr)>
    where
        Message: Debug + for<'de> serde::Deserialize<'de> + Send + Sync + Clone + 'static,
    {
        let _ = metrics.as_ref().inspect(|m| {
            m.finish_delivery_phase(
                DeliveryPhase::IncomingBuffer,
                1,
                &self.message.label,
                self.connection_info.send_mode(),
                self.duration_after_transfer.elapsed(),
            );
        });
        tracing::debug!(
            host_id = self.connection_info.remote_host_id_prefix,
            msg_id = self.message.id,
            msg_type = self.message.label,
            broadcast = self.connection_info.is_broadcast(),
            "Message delivery: decode"
        );
        let (message, decompress_ms, deserialize_ms) = match self.message.decode() {
            Ok(message) => message,
            Err(err) => {
                tracing::error!("Failed decoding incoming message: {}", err);
                if let Some(metrics) = metrics.as_ref() {
                    metrics.report_error("fail_decode_in_msg");
                }
                tracing::debug!(
                    host_id = self.connection_info.remote_host_id_prefix,
                    msg_id = self.message.id,
                    msg_type = self.message.label,
                    broadcast = self.connection_info.is_broadcast(),
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
                if let Some(metrics) = metrics.as_ref() {
                    metrics.report_error("in_msg_dur_na");
                }
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
            broadcast = self.connection_info.is_broadcast(),
            decompress_ms,
            deserialize_ms,
            "Message delivery: finished"
        );
        Some((message, self.connection_info.remote_addr))
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
