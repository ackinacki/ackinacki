// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::collections::HashMap;
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
use crate::message::NetMessage;
use crate::metrics::NetMetrics;
use crate::pub_sub::connection::IncomingMessage;
use crate::pub_sub::connection::OutgoingMessage;
use crate::pub_sub::spawn_critical_task;
use crate::pub_sub::IncomingSender;
use crate::resolver::watch_gossip;
use crate::resolver::SubscribeStrategy;

pub const BROADCAST_RETENTION_CAPACITY: usize = 100;
const DEFAULT_MAX_CONNECTIONS: usize = 1000;

#[derive(Clone, Debug)]
pub struct PeerData {
    pub peer_addr: SocketAddr,
    pub bk_api_socket: Option<SocketAddr>,
}

pub struct BasicNetwork<Transport: NetTransport + 'static> {
    transport: Transport,
    config: NetworkConfig,
}

impl<Transport: NetTransport + 'static> BasicNetwork<Transport> {
    pub fn from(transport: Transport, config: NetworkConfig) -> Self {
        Self { transport, config }
    }

    pub async fn start<PeerId, Message, ChannelMetrics>(
        &self,
        metrics: Option<NetMetrics>,
        channel_metrics: Option<ChannelMetrics>,
        self_peer_id: PeerId,
        is_proxy: bool,
        chitchat: ChitchatRef,
    ) -> anyhow::Result<(
        NetDirectSender<PeerId, Message>,
        NetBroadcastSender<Message>,
        InstrumentedReceiver<IncomingMessage>,
        tokio::sync::watch::Receiver<HashMap<PeerId, PeerData>>,
    )>
    where
        Message:
            Debug + for<'de> serde::Deserialize<'de> + Serialize + Send + Sync + Clone + 'static,
        PeerId: Clone + Display + Send + Sync + Hash + Eq + FromStr<Err: Display> + 'static,
        ChannelMetrics: InstrumentedChannelMetrics + Send + Sync + 'static,
    {
        tracing::info!("Starting network with configuration: {:?}", self.config);

        let tls_config = self.config.tls_config();

        let (incoming_tx, incoming_rx) =
            instrumented_channel::<IncomingMessage>(channel_metrics, "network_incoming");
        let (outgoing_broadcast_tx, _) = tokio::sync::broadcast::channel::<OutgoingMessage>(1000);
        let (outgoing_direct_tx, outgoing_direct_rx) =
            tokio::sync::mpsc::unbounded_channel::<(PeerId, NetMessage, std::time::Instant)>();

        let (subscribe_tx, _) = tokio::sync::watch::channel(Vec::new());
        let (peers_tx, peers_rx) = tokio::sync::watch::channel(HashMap::new());
        let (gossip_subscribe_tx, gossip_peers_tx) = if !self.config.subscribe.is_empty() {
            let _ = subscribe_tx.send_replace(self.config.subscribe.clone());
            (None, Some(peers_tx))
        } else if !self.config.proxies.is_empty() {
            let _ = subscribe_tx.send_replace(vec![self.config.proxies.clone()]);
            (None, Some(peers_tx))
        } else {
            (Some(subscribe_tx.clone()), Some(peers_tx))
        };

        spawn_critical_task(
            "Gossip",
            watch_gossip(
                SubscribeStrategy::Peer(self_peer_id.clone()),
                chitchat.clone(),
                gossip_subscribe_tx,
                gossip_peers_tx,
                metrics.clone(),
            ),
        );

        // listen for pub/sub connections
        let bind = self.config.bind;
        let metrics_clone = metrics.clone();
        let outgoing_broadcast_tx_clone = outgoing_broadcast_tx.clone();
        let transport_clone = self.transport.clone();
        spawn_critical_task("Pub/Sub", async move {
            if let Err(e) = crate::pub_sub::run(
                transport_clone,
                is_proxy,
                metrics_clone,
                DEFAULT_MAX_CONNECTIONS,
                bind,
                tls_config,
                subscribe_tx,
                outgoing_broadcast_tx_clone,
                IncomingSender::SyncUnbounded(incoming_tx),
            )
            .await
            {
                tracing::warn!("pub/sub run task failed: {}", detailed(&e));
            }
        });

        // listen for outgoing directed messages
        let config = self.config.tls_config();
        let peers_rx_clone = peers_rx.clone();
        let metrics_clone = metrics.clone();
        let transport_clone = self.transport.clone();
        spawn_critical_task("Direct sender", async move {
            direct_sender::run_direct_sender(
                transport_clone,
                metrics_clone,
                outgoing_direct_rx,
                peers_rx_clone,
                config,
            )
            .await;
        });

        Ok((
            NetDirectSender::<PeerId, Message>::new(
                outgoing_direct_tx,
                metrics.clone(),
                self_peer_id,
            ),
            NetBroadcastSender::<Message>::new(outgoing_broadcast_tx, metrics.clone()),
            incoming_rx,
            peers_rx,
        ))
    }
}
