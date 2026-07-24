use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use ed25519_dalek::VerifyingKey;
use transport_layer::get_pubkeys_from_cert_der;
use transport_layer::CertHash;
use transport_layer::NetConnection;
use transport_layer::NetTransport;

use crate::detailed;
use crate::message::NetMessage;
use crate::metrics::to_label_kind;
use crate::metrics::NetMetrics;
use crate::pub_sub::receiver;
use crate::pub_sub::sender;
use crate::pub_sub::IncomingSender;
use crate::pub_sub::PubSub;
use crate::topology::NetEndpoint;
use crate::topology::NetTopology;
use crate::DeliveryPhase;
use crate::SendMode;
use crate::NETWORK_DELIVERY_DETAILED_TARGET;

const SLOW_INCOMING_DELIVERY_LOG_THRESHOLD_MS: u64 = 100;
const INCOMING_DELIVERY_SUMMARY_FLUSH_COUNT: usize = 1000;
const INCOMING_DELIVERY_SUMMARY_FLUSH_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(10);

#[derive(Hash, Eq, PartialEq)]
struct IncomingDeliveryLogKey {
    msg_type: String,
    peer: String,
    broadcast: bool,
}

struct IncomingDeliveryLogStats {
    first_seen: Instant,
    count: usize,
    bytes: u64,
    durations_ms: Vec<u64>,
}

impl Default for IncomingDeliveryLogStats {
    fn default() -> Self {
        Self { first_seen: Instant::now(), count: 0, bytes: 0, durations_ms: Vec::new() }
    }
}

thread_local! {
    static INCOMING_DELIVERY_LOG_STATS: RefCell<HashMap<IncomingDeliveryLogKey, IncomingDeliveryLogStats>> =
        RefCell::new(HashMap::new());
}

fn record_incoming_delivery_summary(
    msg_type: &str,
    peer: &str,
    broadcast: bool,
    bytes: u64,
    duration_ms: u64,
) {
    INCOMING_DELIVERY_LOG_STATS.with_borrow_mut(|stats| {
        let key = IncomingDeliveryLogKey {
            msg_type: msg_type.to_string(),
            peer: peer.to_string(),
            broadcast,
        };
        let item = stats.entry(key).or_default();
        item.count += 1;
        item.bytes += bytes;
        item.durations_ms.push(duration_ms);

        if item.count < INCOMING_DELIVERY_SUMMARY_FLUSH_COUNT
            && item.first_seen.elapsed() < INCOMING_DELIVERY_SUMMARY_FLUSH_INTERVAL
        {
            return;
        }

        let mut durations_ms = std::mem::take(&mut item.durations_ms);
        durations_ms.sort_unstable();
        let p95_index = ((durations_ms.len() * 95).div_ceil(100)).saturating_sub(1);
        let p95_duration_ms = durations_ms.get(p95_index).copied().unwrap_or_default();
        let max_duration_ms = durations_ms.last().copied().unwrap_or_default();
        tracing::debug!(
            "Incoming delivery summary msg_type={} peer={} broadcast={} count={} bytes={} p95_duration_ms={} max_duration_ms={}",
            msg_type,
            peer,
            broadcast,
            item.count,
            item.bytes,
            p95_duration_ms,
            max_duration_ms,
        );
        *item = IncomingDeliveryLogStats::default();
    });
}

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
pub struct ConnectionInfo<PeerId: Debug + Display> {
    pub id: u64,
    pub local_is_proxy: bool,
    pub local_role: ConnectionRole,
    pub remote_addr: SocketAddr,
    pub remote_endpoint: NetEndpoint<PeerId>,
    pub remote_cert_hash: CertHash,
    pub remote_cert_hash_prefix: String,
    pub remote_cert_pubkeys: Vec<VerifyingKey>,
}

impl<PeerId: Debug + Display> ConnectionInfo<PeerId> {
    pub fn remote_info(&self) -> String {
        self.remote_addr.to_string()
            + match self.local_role {
                ConnectionRole::Subscriber => " (publisher)",
                ConnectionRole::Publisher => " (subscriber)",
                ConnectionRole::DirectReceiver => " (direct sender)",
            }
    }

    pub fn remote_is_proxy(&self) -> bool {
        matches!(self.remote_endpoint, NetEndpoint::Proxy(_))
    }

    pub fn local_is_publisher(&self) -> bool {
        matches!(self.local_role, ConnectionRole::Publisher)
    }

    pub fn local_is_subscriber(&self) -> bool {
        matches!(self.local_role, ConnectionRole::Subscriber)
    }

    pub fn is_broadcast(&self) -> bool {
        matches!(self.local_role, ConnectionRole::Subscriber | ConnectionRole::Publisher)
    }

    pub fn is_incoming(&self) -> bool {
        match self.local_role {
            ConnectionRole::Subscriber => false,
            ConnectionRole::Publisher => true,
            ConnectionRole::DirectReceiver => true,
        }
    }

    pub fn send_mode(&self) -> SendMode {
        if self.is_broadcast() {
            SendMode::Broadcast
        } else {
            SendMode::Direct
        }
    }

    pub fn remote_is_same_as(&self, other: &Self) -> bool {
        self.remote_cert_hash == other.remote_cert_hash
    }
}

#[derive(Debug)]
pub struct ConnectionWrapper<PeerId: Debug + Display, Connection: NetConnection> {
    pub info: Arc<ConnectionInfo<PeerId>>,
    pub connection: Connection,
}

impl<
        PeerId: Eq + PartialEq + Clone + Display + Debug + Hash + FromStr,
        Connection: NetConnection,
    > ConnectionWrapper<PeerId, Connection>
{
    pub fn new(
        id: u64,
        local_is_proxy: bool,
        override_remote_addr: Option<SocketAddr>,
        remote_endpoint: NetEndpoint<PeerId>,
        connection: Connection,
        local_role: ConnectionRole,
    ) -> anyhow::Result<Self> {
        let cert =
            connection.remote_certificate().ok_or_else(|| anyhow::anyhow!("No certificate"))?;
        let remote_cert_hash = CertHash::from(&cert);
        let remote_cert_hash_prefix = remote_cert_hash.prefix();
        Ok(Self {
            info: Arc::new(ConnectionInfo {
                id,
                local_is_proxy,
                remote_addr: if let Some(addr) = override_remote_addr {
                    addr
                } else {
                    connection.remote_addr()
                },
                remote_cert_hash,
                remote_cert_hash_prefix,
                remote_endpoint,
                remote_cert_pubkeys: get_pubkeys_from_cert_der(&cert)?,
                local_role,
            }),
            connection,
        })
    }

    pub fn allow_sending(
        &self,
        outgoing: &OutgoingMessage<PeerId>,
        topology: &NetTopology<PeerId>,
    ) -> bool {
        let connection = self.info.deref();
        if outgoing.message.last_sender_is_proxy && connection.remote_is_proxy() {
            return false;
        }
        if !topology.i_am_peer_behind_proxy() {
            if let Some(direct_receiver_peer_id) =
                outgoing.message.direct_receiver_peer_id::<PeerId>()
            {
                let allow = match &connection.remote_endpoint {
                    NetEndpoint::Peer(remote_peer) => remote_peer.id == direct_receiver_peer_id,
                    NetEndpoint::Proxy(addrs) => {
                        topology.proxied_segment_contains_peer(addrs, &direct_receiver_peer_id)
                    }
                };
                if !allow {
                    return false;
                }
            }
        }
        let receiver_is_subscriber = connection.local_is_publisher();
        match &outgoing.delivery {
            MessageDelivery::Broadcast => receiver_is_subscriber,
            MessageDelivery::BroadcastExcludingSender(sender) => {
                receiver_is_subscriber && !connection.remote_is_same_as(sender)
            }
            MessageDelivery::BroadcastToMySegmentPeersExcludingSender(sender) => {
                receiver_is_subscriber
                    && !connection.remote_is_same_as(sender)
                    && topology.endpoint_is_peer_from_my_segment(&connection.remote_endpoint)
            }
            MessageDelivery::Addr(addr) => connection.remote_addr == *addr,
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn connection_supervisor<
    PeerId: Clone + Debug + Display + PartialEq + Hash + Eq + Send + Sync + FromStr<Err: Display> + 'static,
    Transport: NetTransport + 'static,
>(
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    topology_rx: tokio::sync::watch::Receiver<NetTopology<PeerId>>,
    pub_sub: PubSub<PeerId, Transport>,
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper<PeerId, Transport::Connection>>,
    incoming_messages_tx: Option<IncomingSender<PeerId>>,
    outgoing_messages_rx: Option<tokio::sync::broadcast::Receiver<OutgoingMessage<PeerId>>>,
    connection_closed_tx: tokio::sync::mpsc::Sender<Arc<ConnectionInfo<PeerId>>>,
) -> anyhow::Result<()> {
    let (sender_stop_tx, sender_stop_rx) = tokio::sync::watch::channel(false);
    let (receiver_stop_tx, receiver_stop_rx) = tokio::sync::watch::channel(false);
    let result = match (incoming_messages_tx, outgoing_messages_rx) {
        (Some(incoming_messages_tx), Some(outgoing_messages_rx)) => {
            tokio::select! {
                result = tokio::spawn(sender::sender(
                    shutdown_rx.clone(),
                    topology_rx,
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
                topology_rx,
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

fn trace_connection_task_result<PeerId: Debug + Display>(
    result: Result<anyhow::Result<()>, tokio::task::JoinError>,
    name: &str,
    connection_info: &ConnectionInfo<PeerId>,
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
pub struct IncomingMessage<PeerId: Debug + Clone + Display> {
    pub connection_info: Arc<ConnectionInfo<PeerId>>,
    pub message: NetMessage,
    pub duration_after_transfer: Instant,
}

impl<PeerId: Debug + Clone + Display> IncomingMessage<PeerId> {
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
        tracing::trace!(
            target: NETWORK_DELIVERY_DETAILED_TARGET,
            host_id = self.connection_info.remote_cert_hash_prefix,
            addr = self.connection_info.remote_addr.to_string(),
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
                    host_id = self.connection_info.remote_cert_hash_prefix,
                    addr = self.connection_info.remote_addr.to_string(),
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
                let peer = self.connection_info.remote_info();
                record_incoming_delivery_summary(
                    &self.message.label,
                    &peer,
                    self.connection_info.is_broadcast(),
                    self.message.data.len() as u64,
                    delivery_duration_ms,
                );
                if delivery_duration_ms >= SLOW_INCOMING_DELIVERY_LOG_THRESHOLD_MS {
                    tracing::debug!(
                        target: "network_slow_delivery",
                        "Received slow incoming {}, peer {}, duration {}",
                        self.message.label,
                        peer,
                        delivery_duration_ms
                    );
                } else {
                    tracing::trace!(
                        target: NETWORK_DELIVERY_DETAILED_TARGET,
                        "Received incoming {}, peer {}, duration {}",
                        self.message.label,
                        peer,
                        delivery_duration_ms
                    );
                }
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
                tracing::debug!(
                    "Received incoming {}, peer {}, duration N/A",
                    self.message.label,
                    self.connection_info.remote_info(),
                );
            }
        }
        tracing::trace!(
            target: NETWORK_DELIVERY_DETAILED_TARGET,
            host_id = self.connection_info.remote_cert_hash_prefix,
            addr = self.connection_info.remote_addr.to_string(),
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
pub enum MessageDelivery<PeerId: Debug + Clone + Display> {
    Broadcast,
    BroadcastExcludingSender(Arc<ConnectionInfo<PeerId>>),
    BroadcastToMySegmentPeersExcludingSender(Arc<ConnectionInfo<PeerId>>),
    Addr(SocketAddr),
}

#[derive(Debug, Clone)]
pub struct OutgoingMessage<PeerId: Debug + Clone + Display> {
    pub delivery: MessageDelivery<PeerId>,
    pub message: NetMessage,
    pub duration_before_transfer: Instant,
}
