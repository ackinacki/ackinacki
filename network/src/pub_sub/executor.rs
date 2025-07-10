use std::net::SocketAddr;

use telemetry_utils::mpsc::InstrumentedSender;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use transport_layer::NetTransport;

use super::trace_task_result;
use super::PubSub;
use crate::config::NetworkConfig;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::IncomingMessage;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::server::listen_incoming_connections;
use crate::pub_sub::subscribe::handle_subscriptions;

#[derive(Clone)]
pub enum IncomingSender {
    AsyncUnbounded(mpsc::UnboundedSender<IncomingMessage>),
    SyncUnbounded(InstrumentedSender<IncomingMessage>),
}

impl IncomingSender {
    pub async fn send(&self, message: IncomingMessage) -> anyhow::Result<()> {
        match self {
            IncomingSender::AsyncUnbounded(sender) => {
                sender.send(message).map_err(|_| anyhow::anyhow!("Failed to send message"))
            }
            IncomingSender::SyncUnbounded(sender) => {
                sender.send(message).map_err(|_| anyhow::anyhow!("Failed to send message"))
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run<Transport: NetTransport + 'static>(
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
    transport: Transport,
    is_proxy: bool,
    metrics: Option<NetMetrics>,
    max_connections: usize,
    subscribe_rx: tokio::sync::watch::Receiver<Vec<Vec<SocketAddr>>>,
    // pub sub subscribes to this sender and forward received messages to all network subscribers
    outgoing_tx: broadcast::Sender<OutgoingMessage>,
    // pub sub forwards all received network messages to this sender
    incoming_tx: IncomingSender,
) -> anyhow::Result<()> {
    tracing::info!("Starting server");

    let pub_sub = PubSub::new(transport, is_proxy);

    let (connection_closed_tx, connection_closed_rx) = mpsc::channel(100);
    let listen_incoming_connections_task = tokio::spawn(listen_incoming_connections(
        shutdown_rx.clone(),
        config_rx.clone(),
        pub_sub.clone(),
        metrics.clone(),
        max_connections,
        incoming_tx.clone(),
        outgoing_tx.clone(),
        connection_closed_tx.clone(),
    ));

    let subscriptions_task = tokio::spawn(handle_subscriptions(
        shutdown_rx,
        config_rx,
        pub_sub.clone(),
        metrics.clone(),
        subscribe_rx,
        incoming_tx.clone(),
        outgoing_tx.clone(),
        connection_closed_tx,
        connection_closed_rx,
    ));

    let result = tokio::select! {
        v = listen_incoming_connections_task => trace_task_result(v, "Listen incoming connections"),
        v = subscriptions_task => trace_task_result(v, "Subscribe client")
    };
    result?
}
