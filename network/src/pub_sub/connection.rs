use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinError;
use url::Url;
use wtransport::Connection;

use crate::detailed;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::receiver;
use crate::pub_sub::sender;
use crate::pub_sub::IncomingSender;
use crate::pub_sub::PubSub;
use crate::SendMode;

#[derive(Debug)]
pub struct ConnectionWrapper {
    pub url: Option<Url>,
    pub peer_id: String,
    pub connection: Connection,
    pub self_is_subscriber: bool,
    pub peer_is_subscriber: bool,
}

impl ConnectionWrapper {
    pub fn new(
        is_debug: bool,
        url: Option<Url>,
        peer_id: String,
        connection: Connection,
        self_is_subscriber: bool,
        peer_is_subscriber: bool,
    ) -> Self {
        let peer_id = if is_debug {
            match connection.remote_address() {
                SocketAddr::V4(addr) => format!("{}:{}", addr.ip(), addr.port()),
                SocketAddr::V6(ipv6) => {
                    if let Some(ipv4_mapped) = ipv6.ip().to_ipv4() {
                        format!("{}:{}", ipv4_mapped, ipv6.port())
                    } else {
                        format!("{}:{}", ipv6.ip(), ipv6.port())
                    }
                }
            }
        } else {
            peer_id
        };
        Self { url, peer_id, connection, self_is_subscriber, peer_is_subscriber }
    }

    pub fn addr(&self) -> String {
        if let Some(url) = &self.url {
            url.to_string()
        } else {
            self.connection.remote_address().to_string()
        }
    }

    pub fn peer_info(&self) -> String {
        let mut info = self.addr();
        if self.self_is_subscriber {
            info.push_str(" (publisher)");
        }
        if self.peer_is_subscriber {
            info.push_str(" (subscriber)");
        }
        info
    }

    pub fn peer_send_mode(&self) -> SendMode {
        if self.self_is_subscriber {
            SendMode::Broadcast
        } else {
            SendMode::Direct
        }
    }

    pub fn allow_sending(&self, message: &OutgoingMessage) -> bool {
        match &message.delivery {
            MessageDelivery::Broadcast => self.peer_is_subscriber,
            MessageDelivery::BroadcastExcluding(excluding) => {
                self.peer_is_subscriber && self.peer_id != excluding.peer_id
            }
            MessageDelivery::Url(url) => self.url.as_ref().map(|x| x == url).unwrap_or_default(),
        }
    }
}

pub async fn connection_supervisor(
    pub_sub: PubSub,
    metrics: Option<NetMetrics>,
    connection: Arc<ConnectionWrapper>,
    incoming_messages_tx: Option<IncomingSender>,
    outgoing_messages_rx: Option<broadcast::Receiver<OutgoingMessage>>,
    connection_closed_tx: mpsc::Sender<Arc<ConnectionWrapper>>,
) -> anyhow::Result<()> {
    let (stop_sender_tx, stop_sender_rx) = watch::channel(false);
    let (stop_receiver_tx, stop_receiver_rx) = watch::channel(false);
    let result = match (incoming_messages_tx, outgoing_messages_rx) {
        (Some(incoming_messages_tx), Some(outgoing_messages_rx)) => {
            tokio::select! {
                result = tokio::spawn(sender::sender(metrics.clone(), connection.clone(), stop_sender_rx, outgoing_messages_rx)) =>
                    trace_connection_task_result(result, "Sender", &connection),
                result = tokio::spawn(receiver::receiver(metrics.clone(), connection.clone(), stop_receiver_rx, incoming_messages_tx)) =>
                    trace_connection_task_result(result, "Receiver", &connection)
            }
        }
        (Some(incoming_messages_tx), None) => {
            let receiver = receiver::receiver(
                metrics.clone(),
                connection.clone(),
                stop_receiver_rx,
                incoming_messages_tx,
            );
            trace_connection_task_result(tokio::spawn(receiver).await, "Receiver", &connection)
        }
        (None, Some(outgoing_messages_rx)) => {
            let sender = sender::sender(
                metrics.clone(),
                connection.clone(),
                stop_sender_rx,
                outgoing_messages_rx,
            );
            trace_connection_task_result(tokio::spawn(sender).await, "Sender", &connection)
        }
        (None, None) => Ok(Ok(())),
    };
    pub_sub.remove_connection(&connection);
    tracing::trace!(peer = connection.peer_info(), "Connection supervisor finished");
    let _ = stop_sender_tx.send_replace(true);
    let _ = stop_receiver_tx.send_replace(true);
    let _ = connection_closed_tx.send(connection).await;
    result?
}

fn trace_connection_task_result(
    result: Result<anyhow::Result<()>, JoinError>,
    name: &str,
    connection: &ConnectionWrapper,
) -> Result<anyhow::Result<()>, JoinError> {
    match &result {
        Ok(result) => match result {
            Ok(_) => {
                tracing::info!(peer = connection.peer_info(), "{name} task finished");
            }
            Err(err) => {
                tracing::error!(
                    peer = connection.peer_info(),
                    "{name} task error: {}",
                    detailed(err)
                );
            }
        },
        Err(err) => {
            tracing::error!(
                peer = connection.peer_info(),
                "Critical: {name} task panicked: {}",
                detailed(err)
            )
        }
    }
    result
}

#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub peer: Arc<ConnectionWrapper>,
    pub message: NetMessage,
    pub duration_after_transfer: Instant,
}

#[derive(Debug, Clone)]
pub enum MessageDelivery {
    Broadcast,
    BroadcastExcluding(Arc<ConnectionWrapper>),
    Url(Url),
}

#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub delivery: MessageDelivery,
    pub message: NetMessage,
    pub duration_before_transfer: Instant,
}
