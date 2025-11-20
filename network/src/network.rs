// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::net::SocketAddr;
use std::str::FromStr;

use chitchat::ChitchatRef;
use serde::Serialize;
use telemetry_utils::mpsc::instrumented_channel;
use telemetry_utils::mpsc::InstrumentedChannelMetrics;
use telemetry_utils::mpsc::InstrumentedReceiver;
use transport_layer::NetTransport;

use crate::channel::NetBroadcastSender;
use crate::channel::NetDirectSender;
use crate::config::NetworkConfig;
use crate::detailed;
use crate::direct_sender;
use crate::direct_sender::DirectReceiver;
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::IncomingMessage;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::spawn_critical_task;
use crate::pub_sub::IncomingSender;
use crate::resolver::watch_hot_reload::watch_gossip;
use crate::resolver::WatchGossipConfig;
use crate::topology::NetTopology;

pub const BROADCAST_RETENTION_CAPACITY: usize = 100;
const DEFAULT_MAX_CONNECTIONS: usize = 1000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerData {
    pub addr: SocketAddr,
    pub bk_api_socket: Option<SocketAddr>,
}

pub struct BasicNetwork<Transport: NetTransport> {
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
    transport: Transport,
}

impl<Transport: NetTransport + 'static> BasicNetwork<Transport> {
    pub fn new(
        shutdown_tx: tokio::sync::watch::Sender<bool>,
        config_rx: tokio::sync::watch::Receiver<NetworkConfig>,
        transport: Transport,
    ) -> Self {
        Self { shutdown_tx, config_rx, transport }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start<PeerId, Message, ChannelMetrics>(
        &self,
        watch_gossip_config_rx: tokio::sync::watch::Receiver<WatchGossipConfig<PeerId>>,
        metrics: Option<NetMetrics>,
        channel_metrics: Option<ChannelMetrics>,
        self_peer_id: PeerId,
        self_addr: SocketAddr,
        is_proxy: bool,
        chitchat_rx: tokio::sync::watch::Receiver<Option<ChitchatRef>>,
    ) -> anyhow::Result<(
        NetDirectSender<PeerId, Message>,
        NetBroadcastSender<PeerId, Message>,
        InstrumentedReceiver<IncomingMessage<PeerId>>,
        tokio::sync::watch::Receiver<NetTopology<PeerId>>,
    )>
    where
        Message:
            Debug + for<'de> serde::Deserialize<'de> + Serialize + Send + Sync + Clone + 'static,
        PeerId: Clone + Debug + Display + Send + Sync + Hash + Eq + FromStr<Err: Display> + 'static,
        ChannelMetrics: InstrumentedChannelMetrics + Send + Sync + 'static,
    {
        tracing::info!("Starting network with configuration: {:?}", *self.config_rx.borrow());

        let (incoming_tx, incoming_rx) =
            instrumented_channel::<IncomingMessage<PeerId>>(channel_metrics, "network_incoming");
        let (outgoing_broadcast_tx, _) =
            tokio::sync::broadcast::channel::<OutgoingMessage<PeerId>>(1000);
        let (outgoing_direct_tx, outgoing_direct_rx) = tokio::sync::mpsc::unbounded_channel::<(
            DirectReceiver<PeerId>,
            NetMessage,
            std::time::Instant,
        )>();

        let (net_topology_tx, net_topology_rx) =
            tokio::sync::watch::channel(NetTopology::default());

        spawn_critical_task(
            "Gossip",
            watch_gossip(
                metrics.clone(),
                self.shutdown_tx.subscribe(),
                watch_gossip_config_rx,
                chitchat_rx.clone(),
                net_topology_tx,
            ),
            metrics.clone(),
        );

        // listen for pub/sub connections
        let metrics_clone = metrics.clone();
        let outgoing_broadcast_tx_clone = outgoing_broadcast_tx.clone();
        let transport_clone = self.transport.clone();
        let shutdown_rx_clone = self.shutdown_tx.subscribe();
        let net_topology_rx_clone = net_topology_rx.clone();
        let config_rx_clone = self.config_rx.clone();
        let incoming_tx_clone = IncomingSender::SyncUnbounded(incoming_tx.clone());
        spawn_critical_task(
            "pub_sub",
            async move {
                let metrics = metrics_clone.clone();
                if let Err(e) = crate::pub_sub::run(
                    shutdown_rx_clone,
                    config_rx_clone,
                    transport_clone,
                    is_proxy,
                    metrics_clone,
                    DEFAULT_MAX_CONNECTIONS,
                    net_topology_rx_clone,
                    outgoing_broadcast_tx_clone,
                    incoming_tx_clone,
                )
                .await
                {
                    if let Some(metrics) = metrics.as_ref() {
                        metrics.report_warn("pub_sub_spawn_crit");
                    }
                    tracing::warn!("pub/sub run task failed: {}", detailed(&e));
                }
            },
            metrics.clone(),
        );

        // listen for outgoing directed messages
        let net_topology_rx_clone = net_topology_rx.clone();
        let metrics_clone = metrics.clone();
        let transport_clone = self.transport.clone();
        let outgoing_reply_tx_clone = outgoing_broadcast_tx.clone();
        let incoming_reply_tx_clone = IncomingSender::SyncUnbounded(incoming_tx.clone());
        spawn_critical_task(
            "Direct sender",
            direct_sender::run_direct_sender(
                self.shutdown_tx.subscribe(),
                self.config_rx.clone(),
                transport_clone,
                metrics_clone,
                outgoing_direct_rx,
                outgoing_reply_tx_clone,
                incoming_reply_tx_clone,
                net_topology_rx_clone,
            ),
            metrics.clone(),
        );

        Ok((
            NetDirectSender::<PeerId, Message>::new(
                outgoing_direct_tx,
                metrics.clone(),
                self_peer_id,
                self_addr,
            ),
            NetBroadcastSender::<PeerId, Message>::new(outgoing_broadcast_tx, metrics.clone()),
            incoming_rx,
            net_topology_rx,
        ))
    }
}
