use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::str::FromStr;

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
use crate::topology::NetTopology;
use crate::DeliveryPhase;

#[derive(Clone)]
pub enum IncomingSender<PeerId: Clone + Debug + Display> {
    AsyncUnbounded(mpsc::UnboundedSender<IncomingMessage<PeerId>>),
    SyncUnbounded(InstrumentedSender<IncomingMessage<PeerId>>),
}

impl<PeerId: Debug + Clone + Display> IncomingSender<PeerId> {
    pub async fn send(&self, message: IncomingMessage<PeerId>) -> anyhow::Result<()> {
        match self {
            IncomingSender::AsyncUnbounded(sender) => {
                sender.send(message).map_err(|_| anyhow::anyhow!("Failed to send message"))
            }
            IncomingSender::SyncUnbounded(sender) => {
                sender.send(message).map_err(|_| anyhow::anyhow!("Failed to send message"))
            }
        }
    }

    pub(crate) async fn send_ok(
        &self,
        message: IncomingMessage<PeerId>,
        metrics: &Option<NetMetrics>,
    ) -> bool {
        let duration_after_transfer = message.duration_after_transfer;
        let info = message.connection_info.clone();
        let msg_type = message.message.label.clone();
        if self.send(message).await.is_err() {
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
}

#[allow(clippy::too_many_arguments)]
pub async fn run<
    PeerId: Clone + PartialEq + Eq + Debug + Display + Send + Sync + Hash + FromStr<Err: Display> + 'static,
    Transport: NetTransport + 'static,
>(
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
    transport: Transport,
    is_proxy: bool,
    metrics: Option<NetMetrics>,
    max_connections: usize,
    net_topology_rx: tokio::sync::watch::Receiver<NetTopology<PeerId>>,
    // pub sub subscribes to this sender and forward received messages to all network subscribers
    outgoing_tx: broadcast::Sender<OutgoingMessage<PeerId>>,
    // pub sub forwards all received network messages to this sender
    incoming_tx: IncomingSender<PeerId>,
) -> anyhow::Result<()> {
    tracing::info!("Starting server");

    let pub_sub = PubSub::new(transport, is_proxy, metrics.clone());

    let (connection_closed_tx, connection_closed_rx) = mpsc::channel(100);
    let listen_incoming_connections_task = tokio::spawn(listen_incoming_connections(
        shutdown_rx.clone(),
        config_rx.clone(),
        net_topology_rx.clone(),
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
        net_topology_rx,
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
